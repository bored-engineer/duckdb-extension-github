#include "github_client_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <curl/curl.h>

#include <cstdlib>
#include <string>
#include <sstream>

namespace duckdb {

std::map<std::string, std::string> generated_types = {
#include "generated_types.cpp"
};

static string_t LookupRESTType(const string_t &name, bool list, Vector &result) {
	auto it = generated_types.find(name.GetString());
	if (it == generated_types.end()) {
		throw InvalidInputException("Unknown type: %s", name.GetString());
	}
	return StringVector::AddString(result, list ? "[" + it->second + "]" : it->second);
}

inline void GitHubRESTTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
	                                           [&](string_t name) { return LookupRESTType(name, false, result); });
}

inline void GitHubRESTTypeFunctionWithList(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, bool, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](string_t name, bool list) { return LookupRESTType(name, list, result); });
}

// Parses the rel="next' URL from the Link header returned by GitHub API
static std::string ParseLinkNextURL(const std::string &link_header_content) {
	auto split_outer = StringUtil::Split(link_header_content, ',');
	for (auto &split : split_outer) {
		auto split_inner = StringUtil::Split(split, ';');
		if (split_inner.size() != 2) {
			throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", link_header_content);
		}

		StringUtil::Trim(split_inner[1]);
		if (split_inner[1] == "rel=\"next\"") {
			StringUtil::Trim(split_inner[0]);

			if (!StringUtil::StartsWith(split_inner[0], "<") || !StringUtil::EndsWith(split_inner[0], ">")) {
				throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", link_header_content);
			}

			return split_inner[0].substr(1, split_inner[0].size() - 2);
		}
	}

	return "";
}

static size_t CurlWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
	auto *body = static_cast<std::string *>(userdata);
	body->append(ptr, size * nmemb);
	return size * nmemb;
}

struct GitHubResponseHeaders {
	std::string link;
	std::string ratelimit_limit;
	std::string ratelimit_remaining;
	std::string ratelimit_used;
	std::string ratelimit_reset;
	std::string ratelimit_resource;
	vector<pair<std::string, std::string>> all;
};

static size_t CurlHeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
	auto *resp = static_cast<GitHubResponseHeaders *>(userdata);
	std::string header(buffer, size * nitems);

	auto match_prefix = [&](const std::string &prefix, std::string &out) -> bool {
		if (StringUtil::Lower(header.substr(0, prefix.size())) != prefix) {
			return false;
		}
		out = header.substr(prefix.size());
		while (!out.empty() && (out.back() == '\r' || out.back() == '\n')) {
			out.pop_back();
		}
		return true;
	};

	match_prefix("link: ", resp->link) ||
	    match_prefix("x-ratelimit-limit: ", resp->ratelimit_limit) ||
	    match_prefix("x-ratelimit-remaining: ", resp->ratelimit_remaining) ||
	    match_prefix("x-ratelimit-used: ", resp->ratelimit_used) ||
	    match_prefix("x-ratelimit-reset: ", resp->ratelimit_reset) ||
	    match_prefix("x-ratelimit-resource: ", resp->ratelimit_resource);

	// Collect all "name: value" headers (skip status line and blank terminator)
	auto colon = header.find(": ");
	if (colon != std::string::npos) {
		std::string name = StringUtil::Lower(header.substr(0, colon));
		std::string value = header.substr(colon + 2);
		while (!value.empty() && (value.back() == '\r' || value.back() == '\n')) {
			value.pop_back();
		}
		resp->all.emplace_back(std::move(name), std::move(value));
	}

	return size * nitems;
}

// Common request configuration shared by github_rest and github_graphql.
struct GitHubRequestBindData : public TableFunctionData {
	string url;
	string token;
	string user_agent;
	string accept;
	string api_version;
	vector<pair<string, string>> extra_headers;
};

static const char *GitHubUserAgent() {
#ifdef EXT_VERSION_GITHUBCLIENT
	return "duckdb-extension-github/" EXT_VERSION_GITHUBCLIENT " (+https://github.com/bored-engineer/duckdb-extension-github)";
#else
	return "duckdb-extension-github/unknown (+https://github.com/bored-engineer/duckdb-extension-github)";
#endif
}

// Resolves the bearer token: DuckDB secrets take priority, then environment variables.
// When the host is a GitHub Enterprise instance, GH_ENTERPRISE_TOKEN / GITHUB_ENTERPRISE_TOKEN
// are checked before GH_TOKEN / GITHUB_TOKEN.
static std::string ResolveToken(ClientContext &context, const std::string &host, bool is_enterprise) {
	auto try_env = [](const char *name) -> const char * {
		const char *v = std::getenv(name);
		return (v && v[0]) ? v : nullptr;
	};

	std::string token;
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto secret_match = secret_manager.LookupSecret(transaction, host + "/", "http");
	if (secret_match.HasMatch()) {
		auto &secret = secret_match.GetSecret();
		if (secret.GetType() != "http") {
			throw InvalidInputException("Invalid secret type. Expected 'http', got '%s'", secret.GetType());
		}
		const auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&secret);
		if (!kv_secret) {
			throw InvalidInputException("Invalid secret type for GitHub secret");
		}
		Value token_value;
		if (!kv_secret->TryGetValue("bearer_token", token_value)) {
			throw InvalidInputException("'bearer_token' not found for GitHub secret");
		}
		token = token_value.ToString();
	}
	if (token.empty() && is_enterprise) {
		if (const char *v = try_env("GH_ENTERPRISE_TOKEN")) {
			token = v;
		} else if (const char *v = try_env("GITHUB_ENTERPRISE_TOKEN")) {
			token = v;
		}
	}
	if (token.empty()) {
		if (const char *v = try_env("GH_TOKEN")) {
			token = v;
		} else if (const char *v = try_env("GITHUB_TOKEN")) {
			token = v;
		}
	}
	if (token.empty()) {
		throw InvalidInputException("No GitHub token found. Create an 'http' secret with 'CREATE SECRET', or set "
		                            "GH_TOKEN or GITHUB_TOKEN.");
	}
	return token;
}

// Parses the optional 'headers' named parameter (a STRUCT) into name/value pairs.
static vector<pair<string, string>> ParseExtraHeaders(TableFunctionBindInput &input) {
	vector<pair<string, string>> result;
	auto headers_param = input.named_parameters.find("headers");
	if (headers_param != input.named_parameters.end()) {
		auto &hval = headers_param->second;
		if (hval.type().id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("'headers' must be a STRUCT, got %s", hval.type().ToString());
		}
		auto &children = StructValue::GetChildren(hval);
		for (idx_t i = 0; i < children.size(); i++) {
			result.emplace_back(StructType::GetChildName(hval.type(), i), children[i].GetValue<string>());
		}
	}
	return result;
}

// Resolves host, token, accept, api_version, headers and user-agent into the common bind data,
// returning the resolved host (e.g. "https://api.github.com").
static std::string BindCommonRequestData(ClientContext &context, TableFunctionBindInput &input,
                                         GitHubRequestBindData &result) {
	// Resolve host: named parameter > GH_HOST env var > api.github.com
	std::string hostname;
	auto host_param = input.named_parameters.find("host");
	if (host_param != input.named_parameters.end()) {
		hostname = host_param->second.GetValue<string>();
	} else {
		const char *gh_host_env = std::getenv("GH_HOST");
		hostname = (gh_host_env && gh_host_env[0]) ? gh_host_env : "api.github.com";
	}
	bool is_enterprise = hostname != "api.github.com";
	std::string host = "https://" + hostname;

	result.token = ResolveToken(context, host, is_enterprise);

	auto accept_param = input.named_parameters.find("accept");
	result.accept = (accept_param != input.named_parameters.end()) ? accept_param->second.GetValue<string>()
	                                                               : "application/vnd.github+json";

	auto api_version_param = input.named_parameters.find("api_version");
	result.api_version = (api_version_param != input.named_parameters.end())
	                         ? api_version_param->second.GetValue<string>()
	                         : "2026-03-10";

	result.extra_headers = ParseExtraHeaders(input);
	result.user_agent = GitHubUserAgent();

	return host;
}

// Appends the body/headers/ratelimit result columns common to both table functions.
static void AddCommonResultColumns(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("body");
	return_types.emplace_back(LogicalType::JSON());
	names.emplace_back("headers");
	return_types.emplace_back(LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
	names.emplace_back("ratelimit");
	child_list_t<LogicalType> ratelimit_children;
	ratelimit_children.emplace_back("limit", LogicalType::UBIGINT);
	ratelimit_children.emplace_back("remaining", LogicalType::UBIGINT);
	ratelimit_children.emplace_back("used", LogicalType::UBIGINT);
	ratelimit_children.emplace_back("reset", LogicalType::TIMESTAMP_S);
	ratelimit_children.emplace_back("resource", LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::STRUCT(std::move(ratelimit_children)));
}

// Performs the HTTP request and returns the response body and parsed headers.
// A non-null post_body issues a POST (with Content-Type: application/json); otherwise a GET.
static void ExecuteGitHubRequest(const GitHubRequestBindData &data, const std::string *post_body,
                                 std::string &body, GitHubResponseHeaders &resp_headers) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InvalidInputException("Failed to initialize curl");
	}

	char errbuf[CURL_ERROR_SIZE] = {0};

	std::string auth_header = "Authorization: Bearer " + data.token;
	struct curl_slist *headers = nullptr;
	std::string user_agent_header = "User-Agent: " + data.user_agent;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, user_agent_header.c_str());
	std::string accept_header = "Accept: " + data.accept;
	headers = curl_slist_append(headers, accept_header.c_str());
	std::string api_version_header = "X-GitHub-Api-Version: " + data.api_version;
	headers = curl_slist_append(headers, api_version_header.c_str());
	headers = curl_slist_append(headers, "X-Github-Next-Global-ID: 1");
	if (post_body) {
		headers = curl_slist_append(headers, "Content-Type: application/json");
	}
	for (auto &kv : data.extra_headers) {
		std::string header = kv.first + ": " + kv.second;
		headers = curl_slist_append(headers, header.c_str());
	}

	curl_easy_setopt(curl, CURLOPT_URL, data.url.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	if (post_body) {
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_body->c_str());
	}
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
	curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, CurlHeaderCallback);
	curl_easy_setopt(curl, CURLOPT_HEADERDATA, &resp_headers);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 15L);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

	CURLcode res_code = curl_easy_perform(curl);

	long http_status = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	const char *method = post_body ? "POST" : "GET";
	if (res_code != CURLE_OK) {
		std::string err_message = StringUtil::Format("HTTP %s request failed: ", method);
		err_message += errbuf[0] ? errbuf : curl_easy_strerror(res_code);
		throw InvalidInputException(err_message);
	}
	if (http_status != 200) {
		throw InvalidInputException("HTTP %s request failed. Status: %ld, Reason: %s", method, http_status,
		                            body.c_str());
	}
}

// Builds the ratelimit STRUCT value (NULL for any absent headers).
static Value BuildRateLimitValue(const GitHubResponseHeaders &resp_headers) {
	auto parse_ubigint_header = [](const std::string &s) -> Value {
		return s.empty() ? Value(LogicalType::UBIGINT) : Value::UBIGINT(std::stoull(s));
	};
	child_list_t<Value> ratelimit_values;
	ratelimit_values.emplace_back("limit", parse_ubigint_header(resp_headers.ratelimit_limit));
	ratelimit_values.emplace_back("remaining", parse_ubigint_header(resp_headers.ratelimit_remaining));
	ratelimit_values.emplace_back("used", parse_ubigint_header(resp_headers.ratelimit_used));
	ratelimit_values.emplace_back("reset", resp_headers.ratelimit_reset.empty()
	                                           ? Value(LogicalType::TIMESTAMP_S)
	                                           : Value::TIMESTAMPSEC(timestamp_sec_t(
	                                                 std::stoll(resp_headers.ratelimit_reset))));
	ratelimit_values.emplace_back("resource", resp_headers.ratelimit_resource.empty()
	                                               ? Value(LogicalType::VARCHAR)
	                                               : Value(resp_headers.ratelimit_resource));
	return Value::STRUCT(std::move(ratelimit_values));
}

// Builds the MAP(VARCHAR, VARCHAR) value of all response headers.
static Value BuildHeadersMapValue(const GitHubResponseHeaders &resp_headers) {
	vector<Value> header_keys, header_vals;
	for (auto &kv : resp_headers.all) {
		header_keys.emplace_back(kv.first);
		header_vals.emplace_back(kv.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(header_keys), std::move(header_vals));
}

struct GitHubRESTBindData : public GitHubRequestBindData {
	string host;
	bool paginate = true;
};

static unique_ptr<FunctionData> GitHubRESTBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubRESTBindData>();

	// Extract the path from the input — only paths (starting with '/') are accepted
	std::string path = input.inputs[0].GetValue<string>();
	if (!StringUtil::StartsWith(path, "/")) {
		throw InvalidInputException("github_rest expects a path starting with '/', got: %s", path);
	}

	std::string host = BindCommonRequestData(context, input, *result);
	result->host = host;
	result->url = host + path;

	auto paginate_param = input.named_parameters.find("paginate");
	if (paginate_param != input.named_parameters.end()) {
		result->paginate = paginate_param->second.GetValue<bool>();
	}

	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);
	AddCommonResultColumns(return_types, names);

	return std::move(result);
}

static void GitHubRESTFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = const_cast<GitHubRESTBindData &>(data_p.bind_data->Cast<GitHubRESTBindData>());

	// If there's no next page, we're done!
	if (data.url.empty()) {
		return;
	}

	std::string body;
	GitHubResponseHeaders resp_headers;
	ExecuteGitHubRequest(data, nullptr, body, resp_headers);

	// Store the output
	output.SetValue(0, 0, Value(data.url));
	output.SetValue(1, 0, Value(body));
	output.SetValue(2, 0, BuildHeadersMapValue(resp_headers));
	output.SetValue(3, 0, BuildRateLimitValue(resp_headers));
	output.SetCardinality(1);

	// Check for the "Link" header to see if there's a next page (unless pagination is disabled)
	std::string next_url;
	if (data.paginate) {
		next_url = resp_headers.link.empty() ? "" : ParseLinkNextURL(resp_headers.link);
		if (!next_url.empty() && !StringUtil::StartsWith(next_url, data.host + "/")) {
			throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", next_url);
		}
	}
	data.url = next_url;
}

static std::string JSONEncodeString(const std::string &s) {
	std::string out;
	out.reserve(s.size() + 2);
	out += '"';
	for (unsigned char c : s) {
		switch (c) {
		case '"':  out += "\\\""; break;
		case '\\': out += "\\\\"; break;
		case '\b': out += "\\b";  break;
		case '\f': out += "\\f";  break;
		case '\n': out += "\\n";  break;
		case '\r': out += "\\r";  break;
		case '\t': out += "\\t";  break;
		default:
			if (c < 0x20) {
				char buf[7];
				snprintf(buf, sizeof(buf), "\\u%04x", c);
				out += buf;
			} else {
				out += static_cast<char>(c);
			}
		}
	}
	out += '"';
	return out;
}

static std::string ValueToJSON(const Value &v) {
	if (v.IsNull()) {
		return "null";
	}
	switch (v.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return v.GetValue<bool>() ? "true" : "false";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return v.ToString();
	case LogicalTypeId::VARCHAR:
		return JSONEncodeString(v.GetValue<string>());
	case LogicalTypeId::LIST: {
		auto &children = ListValue::GetChildren(v);
		std::string out = "[";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				out += ",";
			}
			out += ValueToJSON(children[i]);
		}
		return out + "]";
	}
	case LogicalTypeId::STRUCT: {
		auto &children = StructValue::GetChildren(v);
		std::string out = "{";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				out += ",";
			}
			out += JSONEncodeString(StructType::GetChildName(v.type(), i));
			out += ":";
			out += ValueToJSON(children[i]);
		}
		return out + "}";
	}
	default:
		throw InvalidInputException("Unsupported type for GraphQL variables: %s", v.type().ToString());
	}
}

// Builds the GraphQL POST body, merging an optional pagination cursor into the variables
// object under the "endCursor" key (so the query can reference it as $endCursor).
static std::string BuildGraphQLBody(const std::string &query, const std::string &variables_json,
                                    const std::string &cursor = "") {
	std::string vars = variables_json;
	if (!cursor.empty()) {
		std::string cursor_entry = "\"endCursor\":" + JSONEncodeString(cursor);
		if (vars.empty() || vars == "{}") {
			vars = "{" + cursor_entry + "}";
		} else {
			vars = vars.substr(0, vars.size() - 1) + "," + cursor_entry + "}";
		}
	}
	std::string body = "{\"query\":" + JSONEncodeString(query);
	if (!vars.empty()) {
		body += ",\"variables\":" + vars;
	}
	return body + "}";
}

static void SkipJSONWhitespace(const std::string &s, size_t &i) {
	while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) {
		i++;
	}
}

// Returns true if the response body contains a "hasNextPage": true entry.
static bool GraphQLHasNextPage(const std::string &body) {
	const std::string key = "\"hasNextPage\"";
	size_t pos = 0;
	while ((pos = body.find(key, pos)) != std::string::npos) {
		size_t i = pos + key.size();
		SkipJSONWhitespace(body, i);
		if (i < body.size() && body[i] == ':') {
			i++;
			SkipJSONWhitespace(body, i);
			if (body.compare(i, 4, "true") == 0) {
				return true;
			}
		}
		pos += key.size();
	}
	return false;
}

// Extracts the first string-valued "endCursor" from the response body, decoding JSON escapes.
static std::string GraphQLEndCursor(const std::string &body) {
	const std::string key = "\"endCursor\"";
	size_t pos = 0;
	while ((pos = body.find(key, pos)) != std::string::npos) {
		size_t i = pos + key.size();
		SkipJSONWhitespace(body, i);
		if (i < body.size() && body[i] == ':') {
			i++;
			SkipJSONWhitespace(body, i);
			if (i < body.size() && body[i] == '"') {
				i++;
				std::string value;
				while (i < body.size() && body[i] != '"') {
					if (body[i] == '\\' && i + 1 < body.size()) {
						switch (body[i + 1]) {
						case '"':  value += '"';  break;
						case '\\': value += '\\'; break;
						case '/':  value += '/';  break;
						case 'b':  value += '\b'; break;
						case 'f':  value += '\f'; break;
						case 'n':  value += '\n'; break;
						case 'r':  value += '\r'; break;
						case 't':  value += '\t'; break;
						default:   value += body[i + 1]; break;
						}
						i += 2;
					} else {
						value += body[i++];
					}
				}
				return value;
			}
			// "endCursor": null — keep looking for a string-valued one
		}
		pos += key.size();
	}
	return "";
}

struct GitHubGraphQLBindData : public GitHubRequestBindData {
	string query;
	string variables_json;
	string cursor;
	bool done = false;
};

static unique_ptr<FunctionData> GitHubGraphQLBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubGraphQLBindData>();

	result->query = input.inputs[0].GetValue<string>();
	auto variables_param = input.named_parameters.find("variables");
	if (variables_param != input.named_parameters.end()) {
		result->variables_json = ValueToJSON(variables_param->second);
	}

	std::string host = BindCommonRequestData(context, input, *result);
	result->url = host + "/graphql";

	AddCommonResultColumns(return_types, names);

	return std::move(result);
}

static void GitHubGraphQLFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = const_cast<GitHubGraphQLBindData &>(data_p.bind_data->Cast<GitHubGraphQLBindData>());

	if (data.done) {
		return;
	}

	std::string post_body = BuildGraphQLBody(data.query, data.variables_json, data.cursor);
	std::string body;
	GitHubResponseHeaders resp_headers;
	ExecuteGitHubRequest(data, &post_body, body, resp_headers);

	output.SetValue(0, 0, Value(body));
	output.SetValue(1, 0, BuildHeadersMapValue(resp_headers));
	output.SetValue(2, 0, BuildRateLimitValue(resp_headers));
	output.SetCardinality(1);

	// Continue paginating while the response reports another page and yields a new cursor.
	std::string next_cursor;
	if (GraphQLHasNextPage(body)) {
		next_cursor = GraphQLEndCursor(body);
	}
	if (next_cursor.empty() || next_cursor == data.cursor) {
		data.done = true;
	} else {
		data.cursor = next_cursor;
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction github_rest_function("github_rest", {LogicalType::VARCHAR}, GitHubRESTFunction, GitHubRESTBind);
	github_rest_function.named_parameters["host"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["accept"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["api_version"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["headers"] = LogicalType::ANY;
	github_rest_function.named_parameters["paginate"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(github_rest_function);
	TableFunction github_graphql_function("github_graphql", {LogicalType::VARCHAR}, GitHubGraphQLFunction,
	                                      GitHubGraphQLBind);
	github_graphql_function.named_parameters["host"] = LogicalType::VARCHAR;
	github_graphql_function.named_parameters["variables"] = LogicalType::ANY;
	github_graphql_function.named_parameters["headers"] = LogicalType::ANY;
	loader.RegisterFunction(github_graphql_function);
	ScalarFunctionSet github_rest_type_set("github_rest_type");
	github_rest_type_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, GitHubRESTTypeFunction));
	github_rest_type_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VARCHAR,
	                                                GitHubRESTTypeFunctionWithList));
	loader.RegisterFunction(github_rest_type_set);
}

void GithubClientExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GithubClientExtension::Name() {
	return "github_client";
}

std::string GithubClientExtension::Version() const {
#ifdef EXT_VERSION_GITHUBCLIENT
	return EXT_VERSION_GITHUBCLIENT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(github_client, loader) {
	duckdb::LoadInternal(loader);
}
}

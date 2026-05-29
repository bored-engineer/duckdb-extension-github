#include "github_extension.hpp"
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
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/connection.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <curl/curl.h>

#include "yyjson.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <sstream>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

std::map<std::string, std::string> generated_types = {
#include "generated_types.cpp"
};

static string_t LookupRESTType(const string_t &name, Vector &result) {
	auto it = generated_types.find(name.GetString());
	if (it == generated_types.end()) {
		throw InvalidInputException("Unknown type: %s", name.GetString());
	}
	return StringVector::AddString(result, it->second);
}

inline void GitHubRESTTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
	                                           [&](string_t name) { return LookupRESTType(name, result); });
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
	std::string request_id;
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

	match_prefix("link: ", resp->link) || match_prefix("x-github-request-id: ", resp->request_id) ||
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
#ifdef EXT_VERSION_GITHUB
	return "duckdb-extension-github/" EXT_VERSION_GITHUB
	       " (+https://github.com/bored-engineer/duckdb-extension-github)";
#else
	return "duckdb-extension-github/unknown (+https://github.com/bored-engineer/duckdb-extension-github)";
#endif
}

// Resolves the bearer token: DuckDB secrets take priority, then environment variables.
// For enterprise hosts only GH_ENTERPRISE_TOKEN / GITHUB_ENTERPRISE_TOKEN are checked.
// For the default host only GH_TOKEN / GITHUB_TOKEN are checked.
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
	if (token.empty()) {
		if (is_enterprise) {
			if (const char *v = try_env("GH_ENTERPRISE_TOKEN")) {
				token = v;
			} else if (const char *v = try_env("GITHUB_ENTERPRISE_TOKEN")) {
				token = v;
			}
		} else {
			if (const char *v = try_env("GH_TOKEN")) {
				token = v;
			} else if (const char *v = try_env("GITHUB_TOKEN")) {
				token = v;
			}
		}
	}
	if (token.empty()) {
		throw InvalidInputException(
		    is_enterprise
		        ? "No GitHub token found. Create an 'http' secret with 'CREATE SECRET', or set GH_ENTERPRISE_TOKEN."
		        : "No GitHub token found. Create an 'http' secret with 'CREATE SECRET', or set GH_TOKEN or "
		          "GITHUB_TOKEN.");
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

// Appends the body/headers/ratelimit/request_id result columns common to both table functions.
static void AddCommonResultColumns(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("data");
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
	names.emplace_back("request_id");
	return_types.emplace_back(LogicalType::VARCHAR);
}

// The X-GitHub-Request-Id header value, or NULL when absent.
static Value RequestIdValue(const GitHubResponseHeaders &resp_headers) {
	return resp_headers.request_id.empty() ? Value(LogicalType::VARCHAR) : Value(resp_headers.request_id);
}

// Performs the HTTP request and returns the response body and parsed headers.
// A non-null post_body issues a POST (with Content-Type: application/json); otherwise a GET.
static void ExecuteGitHubRequest(const GitHubRequestBindData &data, const std::string *post_body, std::string &body,
                                 GitHubResponseHeaders &resp_headers) {
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
	ratelimit_values.emplace_back("reset",
	                              resp_headers.ratelimit_reset.empty()
	                                  ? Value(LogicalType::TIMESTAMP_S)
	                                  : Value::TIMESTAMPSEC(timestamp_sec_t(std::stoll(resp_headers.ratelimit_reset))));
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

	if (data.url.empty()) {
		return;
	}

	std::string body;
	GitHubResponseHeaders resp_headers;
	ExecuteGitHubRequest(data, nullptr, body, resp_headers);

	Value page_url = Value(data.url);
	Value page_headers = BuildHeadersMapValue(resp_headers);
	Value page_ratelimit = BuildRateLimitValue(resp_headers);
	Value page_request_id = RequestIdValue(resp_headers);

	// An array response yields one row per element; anything else yields the whole body.
	// GitHub never returns more than 100 items per page, which fits in one chunk.
	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), 0);
	yyjson_val *root = doc ? yyjson_doc_get_root(doc) : nullptr;
	idx_t count = 0;
	if (root && yyjson_is_arr(root)) {
		yyjson_arr_iter it;
		yyjson_arr_iter_init(root, &it);
		yyjson_val *item;
		while ((item = yyjson_arr_iter_next(&it))) {
			char *item_json = yyjson_val_write(item, 0, nullptr);
			if (item_json) {
				output.SetValue(0, count, page_url);
				output.SetValue(1, count, Value(item_json));
				output.SetValue(2, count, page_headers);
				output.SetValue(3, count, page_ratelimit);
				output.SetValue(4, count, page_request_id);
				free(item_json);
				count++;
			}
		}
	} else {
		output.SetValue(0, count, page_url);
		output.SetValue(1, count, Value(body));
		output.SetValue(2, count, page_headers);
		output.SetValue(3, count, page_ratelimit);
		output.SetValue(4, count, page_request_id);
		count = 1;
	}
	if (doc) {
		yyjson_doc_free(doc);
	}
	output.SetCardinality(count);

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

// Converts a DuckDB Value into a yyjson mutable value owned by doc (used for GraphQL variables).
static yyjson_mut_val *ValueToYYJSON(yyjson_mut_doc *doc, const Value &v) {
	if (v.IsNull()) {
		return yyjson_mut_null(doc);
	}
	switch (v.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return yyjson_mut_bool(doc, v.GetValue<bool>());
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
		return yyjson_mut_sint(doc, v.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(doc, v.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return yyjson_mut_real(doc, v.GetValue<double>());
	case LogicalTypeId::VARCHAR: {
		auto s = v.GetValue<string>();
		return yyjson_mut_strncpy(doc, s.c_str(), s.size());
	}
	case LogicalTypeId::LIST: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &child : ListValue::GetChildren(v)) {
			yyjson_mut_arr_append(arr, ValueToYYJSON(doc, child));
		}
		return arr;
	}
	case LogicalTypeId::STRUCT: {
		auto obj = yyjson_mut_obj(doc);
		auto &children = StructValue::GetChildren(v);
		for (idx_t i = 0; i < children.size(); i++) {
			auto key = StructType::GetChildName(v.type(), i);
			auto key_val = yyjson_mut_strncpy(doc, key.c_str(), key.size());
			yyjson_mut_obj_add(obj, key_val, ValueToYYJSON(doc, children[i]));
		}
		return obj;
	}
	default:
		throw InvalidInputException("Unsupported type for GraphQL variables: %s", v.type().ToString());
	}
}

// Serializes a DuckDB Value to a JSON string.
static std::string ValueToJSON(const Value &v) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_doc_set_root(doc, ValueToYYJSON(doc, v));
	char *json = yyjson_mut_write(doc, 0, nullptr);
	std::string result = json ? json : "";
	free(json);
	yyjson_mut_doc_free(doc);
	return result;
}

// Builds the GraphQL POST body, merging an optional pagination cursor into the variables
// object under the "endCursor" key (so the query can reference it as $endCursor).
static std::string BuildGraphQLBody(const std::string &query, const std::string &variables_json,
                                    const std::string &cursor = "") {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strncpy(doc, root, "query", query.c_str(), query.size());

	// Copy the caller-supplied variables (a JSON object) into the body, if any
	yyjson_mut_val *vars = nullptr;
	if (!variables_json.empty()) {
		yyjson_doc *vdoc = yyjson_read(variables_json.c_str(), variables_json.size(), 0);
		if (vdoc) {
			vars = yyjson_val_mut_copy(doc, yyjson_doc_get_root(vdoc));
			yyjson_doc_free(vdoc);
		}
	}
	if (!cursor.empty()) {
		if (!vars || !yyjson_mut_is_obj(vars)) {
			vars = yyjson_mut_obj(doc);
		}
		yyjson_mut_obj_add_strncpy(doc, vars, "endCursor", cursor.c_str(), cursor.size());
	}
	if (vars) {
		yyjson_mut_obj_add_val(doc, root, "variables", vars);
	}

	char *json = yyjson_mut_write(doc, 0, nullptr);
	std::string body = json ? json : "";
	free(json);
	yyjson_mut_doc_free(doc);
	return body;
}

// Recursively searches a JSON value for the first "pageInfo" key whose value is an object.
static yyjson_val *YYJSONFindPageInfo(yyjson_val *val) {
	if (yyjson_is_obj(val)) {
		yyjson_obj_iter it;
		yyjson_obj_iter_init(val, &it);
		yyjson_val *k;
		while ((k = yyjson_obj_iter_next(&it))) {
			yyjson_val *v = yyjson_obj_iter_get_val(k);
			if (strcmp(yyjson_get_str(k), "pageInfo") == 0 && yyjson_is_obj(v)) {
				return v;
			}
			if (yyjson_val *found = YYJSONFindPageInfo(v)) {
				return found;
			}
		}
	} else if (yyjson_is_arr(val)) {
		yyjson_arr_iter it;
		yyjson_arr_iter_init(val, &it);
		yyjson_val *v;
		while ((v = yyjson_arr_iter_next(&it))) {
			if (yyjson_val *found = YYJSONFindPageInfo(v)) {
				return found;
			}
		}
	}
	return nullptr;
}

// Builds a LIST(JSON) value from a yyjson array, one element per item.
// Returns an empty list when arr is null or not an array.
static Value YYJSONArrayToJSONList(yyjson_val *arr) {
	vector<Value> values;
	if (arr && yyjson_is_arr(arr)) {
		yyjson_arr_iter it;
		yyjson_arr_iter_init(arr, &it);
		yyjson_val *item;
		while ((item = yyjson_arr_iter_next(&it))) {
			char *json = yyjson_val_write(item, 0, nullptr);
			if (json) {
				values.emplace_back(Value(json).DefaultCastAs(LogicalType::JSON()));
				free(json);
			}
		}
	}
	return Value::LIST(LogicalType::JSON(), std::move(values));
}

struct GitHubGraphQLBindData : public GitHubRequestBindData {
	string query;
	string variables_json;
	string cursor;
	bool ignore_errors = false;
	bool paginate = true;
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

	auto ignore_errors_param = input.named_parameters.find("ignore_errors");
	if (ignore_errors_param != input.named_parameters.end()) {
		result->ignore_errors = ignore_errors_param->second.GetValue<bool>();
	}

	auto paginate_param = input.named_parameters.find("paginate");
	if (paginate_param != input.named_parameters.end()) {
		result->paginate = paginate_param->second.GetValue<bool>();
	}

	std::string host = BindCommonRequestData(context, input, *result);
	result->url = host + "/graphql";

	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);
	AddCommonResultColumns(return_types, names);
	names.emplace_back("errors");
	return_types.emplace_back(LogicalType::LIST(LogicalType::JSON()));
	names.emplace_back("warnings");
	return_types.emplace_back(LogicalType::LIST(LogicalType::JSON()));

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

	// Parse the response once and inspect it for errors and pagination info
	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse GraphQL response as JSON");
	}
	yyjson_val *root = yyjson_doc_get_root(doc);

	// Surface any GraphQL errors returned in the response (unless ignore_errors is set)
	yyjson_val *errors = yyjson_is_obj(root) ? yyjson_obj_get(root, "errors") : nullptr;
	if (!data.ignore_errors && errors && yyjson_is_arr(errors) && yyjson_arr_size(errors) > 0) {
		char *errors_json = yyjson_val_write(errors, 0, nullptr);
		std::string message = errors_json ? errors_json : "";
		free(errors_json);
		yyjson_doc_free(doc);
		throw InvalidInputException("GraphQL query returned errors: %s", message);
	}

	// Extract the "data" value, the "errors" list, and the next pagination cursor,
	// while the document is still parsed
	yyjson_val *data_val = yyjson_is_obj(root) ? yyjson_obj_get(root, "data") : nullptr;
	Value data_value(LogicalType::JSON());
	if (data_val) {
		char *data_json = yyjson_val_write(data_val, 0, nullptr);
		if (data_json) {
			data_value = Value(data_json);
			free(data_json);
		}
	}

	Value errors_value = YYJSONArrayToJSONList(errors);

	// Warnings live under extensions.warnings, if present
	yyjson_val *extensions = yyjson_is_obj(root) ? yyjson_obj_get(root, "extensions") : nullptr;
	yyjson_val *warnings = (extensions && yyjson_is_obj(extensions)) ? yyjson_obj_get(extensions, "warnings") : nullptr;
	Value warnings_value = YYJSONArrayToJSONList(warnings);

	std::string next_cursor;
	if (yyjson_val *page_info = YYJSONFindPageInfo(root)) {
		yyjson_val *has_next = yyjson_obj_get(page_info, "hasNextPage");
		yyjson_val *end_cursor = yyjson_obj_get(page_info, "endCursor");
		if (has_next && yyjson_is_true(has_next) && end_cursor && yyjson_is_str(end_cursor)) {
			next_cursor = yyjson_get_str(end_cursor);
		}
	}
	yyjson_doc_free(doc);

	output.SetValue(0, 0, Value(data.url));
	output.SetValue(1, 0, data_value);
	output.SetValue(2, 0, BuildHeadersMapValue(resp_headers));
	output.SetValue(3, 0, BuildRateLimitValue(resp_headers));
	output.SetValue(4, 0, RequestIdValue(resp_headers));
	output.SetValue(5, 0, errors_value);
	output.SetValue(6, 0, warnings_value);
	output.SetCardinality(1);

	// Continue paginating while the response reports another page and yields a new cursor.
	if (!data.paginate || next_cursor.empty() || next_cursor == data.cursor) {
		data.done = true;
	} else {
		data.cursor = next_cursor;
	}
}

struct GitHubContentsRawData : public FunctionData {
	std::string token;
	std::string host;
	std::string user_agent;
	std::string ref; // empty when not provided

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<GitHubContentsRawData>(*this);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<GitHubContentsRawData>();
		return token == other.token && host == other.host && ref == other.ref;
	}
};

static unique_ptr<FunctionData> GitHubContentsRawBind(ClientContext &context, ScalarFunction & /*bound_function*/,
                                                      vector<unique_ptr<Expression>> &arguments) {
	auto result = make_uniq<GitHubContentsRawData>();

	// Extract optional ref (4th arg): empty string or absent means use the repo's default branch.
	if (arguments.size() > 3 && arguments[3]->IsFoldable()) {
		result->ref = ExpressionExecutor::EvaluateScalar(context, *arguments[3]).GetValue<string>();
	}

	// Extract optional host (5th arg): empty string or absent falls back to GH_HOST env var or api.github.com.
	std::string hostname;
	if (arguments.size() > 4 && arguments[4]->IsFoldable()) {
		hostname = ExpressionExecutor::EvaluateScalar(context, *arguments[4]).GetValue<string>();
	}
	if (hostname.empty()) {
		const char *gh_host_env = std::getenv("GH_HOST");
		hostname = (gh_host_env && gh_host_env[0]) ? gh_host_env : "api.github.com";
	}
	bool is_enterprise = hostname != "api.github.com";
	result->host = "https://" + hostname;
	result->user_agent = GitHubUserAgent();
	result->token = ResolveToken(context, result->host, is_enterprise);

	return std::move(result);
}

struct GitHubContentsBindData : public TableFunctionData {
	std::string token;
	std::string host;
	std::string user_agent;
	std::string api_version;
	std::string owner;
	std::string repo;
	std::string path;
	std::string ref;
	bool ignore_incomplete = false;
	bool include_root = true;
	bool done = false;
};

static unique_ptr<FunctionData> GitHubContentsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubContentsBindData>();

	result->owner = input.inputs[0].GetValue<string>();
	result->repo = input.inputs[1].GetValue<string>();
	result->path = input.inputs[2].GetValue<string>();

	auto ref_param = input.named_parameters.find("ref");
	if (ref_param != input.named_parameters.end()) {
		result->ref = ref_param->second.GetValue<string>();
	}

	std::string hostname;
	auto host_param = input.named_parameters.find("host");
	if (host_param != input.named_parameters.end()) {
		hostname = host_param->second.GetValue<string>();
	}
	if (hostname.empty()) {
		const char *gh_host_env = std::getenv("GH_HOST");
		hostname = (gh_host_env && gh_host_env[0]) ? gh_host_env : "api.github.com";
	}
	bool is_enterprise = hostname != "api.github.com";
	result->host = "https://" + hostname;
	result->user_agent = GitHubUserAgent();
	result->token = ResolveToken(context, result->host, is_enterprise);

	auto api_version_param = input.named_parameters.find("api_version");
	result->api_version = (api_version_param != input.named_parameters.end())
	                          ? api_version_param->second.GetValue<string>()
	                          : "2026-03-10";

	auto ignore_incomplete_param = input.named_parameters.find("ignore_incomplete");
	if (ignore_incomplete_param != input.named_parameters.end()) {
		result->ignore_incomplete = ignore_incomplete_param->second.GetValue<bool>();
	}

	auto include_root_param = input.named_parameters.find("include_root");
	if (include_root_param != input.named_parameters.end()) {
		result->include_root = include_root_param->second.GetValue<bool>();
	}

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("size");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("content");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("sha");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("git_url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("download_url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("submodule_git_url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("target");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static void GitHubContentsEmitRow(yyjson_val *obj, DataChunk &output, idx_t row) {
	auto get_str = [&](const char *key) -> Value {
		yyjson_val *v = yyjson_obj_get(obj, key);
		return (v && yyjson_is_str(v)) ? Value(yyjson_get_str(v)) : Value(LogicalType::VARCHAR);
	};

	output.SetValue(0, row, get_str("type"));

	yyjson_val *size_val = yyjson_obj_get(obj, "size");
	output.SetValue(1, row,
	                (size_val && yyjson_is_num(size_val))
	                    ? Value::UBIGINT(static_cast<uint64_t>(yyjson_get_num(size_val)))
	                    : Value(LogicalType::UBIGINT));

	output.SetValue(2, row, get_str("name"));
	output.SetValue(3, row, get_str("path"));

	yyjson_val *content_val = yyjson_obj_get(obj, "content");
	if (content_val && yyjson_is_str(content_val)) {
		// GitHub embeds newlines in base64 content; strip them before decoding.
		const char *encoded = yyjson_get_str(content_val);
		std::string stripped;
		for (const char *p = encoded; *p; ++p) {
			if (*p != '\n' && *p != '\r') {
				stripped += *p;
			}
		}
		output.SetValue(4, row, Value::BLOB(Blob::FromBase64(string_t(stripped))));
	} else {
		output.SetValue(4, row, Value(LogicalType::BLOB));
	}

	output.SetValue(5, row, get_str("sha"));
	output.SetValue(6, row, get_str("url"));
	output.SetValue(7, row, get_str("git_url"));
	output.SetValue(8, row, get_str("download_url"));
	output.SetValue(9, row, get_str("submodule_git_url"));
	output.SetValue(10, row, get_str("target"));
}

static void GitHubContentsFunction(ClientContext & /*context*/, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = const_cast<GitHubContentsBindData &>(data_p.bind_data->Cast<GitHubContentsBindData>());

	if (data.done) {
		return;
	}
	data.done = true;

	std::string url = data.host + "/repos/" + data.owner + "/" + data.repo + "/contents/" + data.path;
	if (!data.ref.empty()) {
		url += "?ref=" + data.ref;
	}

	GitHubRequestBindData req;
	req.url = url;
	req.token = data.token;
	req.user_agent = data.user_agent;
	req.accept = "application/vnd.github.object+json";
	req.api_version = data.api_version;

	std::string body;
	GitHubResponseHeaders resp_headers;
	ExecuteGitHubRequest(req, nullptr, body, resp_headers);

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), 0);
	if (!doc) {
		throw InvalidInputException("github_contents: failed to parse response as JSON");
	}
	yyjson_val *root = yyjson_doc_get_root(doc);

	yyjson_val *entries = yyjson_obj_get(root, "entries");
	if (entries && yyjson_is_arr(entries)) {
		idx_t arr_size = yyjson_arr_size(entries);
		if (arr_size >= 1000 && !data.ignore_incomplete) {
			yyjson_doc_free(doc);
			throw InvalidInputException(
			    "github_contents: directory listing is incomplete (1000 entries returned, GitHub's maximum). "
			    "Use ignore_incomplete=true to suppress this error.");
		}
		idx_t count = 0;
		if (data.include_root) {
			GitHubContentsEmitRow(root, output, count++);
		}
		yyjson_arr_iter it;
		yyjson_arr_iter_init(entries, &it);
		yyjson_val *entry;
		while ((entry = yyjson_arr_iter_next(&it))) {
			GitHubContentsEmitRow(entry, output, count++);
		}
		output.SetCardinality(count);
	} else {
		GitHubContentsEmitRow(root, output, 0);
		output.SetCardinality(1);
	}

	yyjson_doc_free(doc);
}

static void GitHubContentsRawFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<GitHubContentsRawData>();

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](string_t owner, string_t repo, string_t path) -> string_t {
		    std::string url =
		        data.host + "/repos/" + owner.GetString() + "/" + repo.GetString() + "/contents/" + path.GetString();
		    if (!data.ref.empty()) {
			    url += "?ref=" + data.ref;
		    }

		    GitHubRequestBindData req;
		    req.url = url;
		    req.token = data.token;
		    req.user_agent = data.user_agent;
		    req.accept = "application/vnd.github.raw+json";
		    req.api_version = "2026-03-10";

		    std::string body;
		    GitHubResponseHeaders resp_headers;
		    ExecuteGitHubRequest(req, nullptr, body, resp_headers);

		    return StringVector::AddString(result, body);
	    });
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
	github_graphql_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	github_graphql_function.named_parameters["paginate"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(github_graphql_function);
	TableFunction github_contents_function("github_contents",
	                                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       GitHubContentsFunction, GitHubContentsBind);
	github_contents_function.named_parameters["ref"] = LogicalType::VARCHAR;
	github_contents_function.named_parameters["host"] = LogicalType::VARCHAR;
	github_contents_function.named_parameters["api_version"] = LogicalType::VARCHAR;
	github_contents_function.named_parameters["ignore_incomplete"] = LogicalType::BOOLEAN;
	github_contents_function.named_parameters["include_root"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(github_contents_function);
	ScalarFunctionSet github_contents_raw_set("github_contents_raw");
	github_contents_raw_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BLOB,
	                   GitHubContentsRawFunction, GitHubContentsRawBind));
	github_contents_raw_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::BLOB, GitHubContentsRawFunction, GitHubContentsRawBind));
	github_contents_raw_set.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::BLOB, GitHubContentsRawFunction, GitHubContentsRawBind));
	loader.RegisterFunction(github_contents_raw_set);
	loader.RegisterFunction(
	    ScalarFunction("github_rest_type", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GitHubRESTTypeFunction));

	Connection conn(loader.GetDatabaseInstance());
	auto result = conn.Query(
	    "CREATE OR REPLACE MACRO github_repo(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo)"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_repo macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_user(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('public-user')) AS r "
	    "FROM github_rest('/users/' || username)"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_user macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_user_followers(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('simple-user')) AS r "
	    "FROM github_rest('/users/' || username || '/followers?per_page=100')"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_user_followers macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_user_gpg_keys(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('gpg-key')) AS r "
	    "FROM github_rest('/users/' || username || '/gpg_keys?per_page=100')"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_user_gpg_keys macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_user_ssh_keys(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('key')) AS r "
	    "FROM github_rest('/users/' || username || '/keys?per_page=100')"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_user_ssh_keys macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_user_ssh_signing_keys(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('ssh-signing-key')) AS r "
	    "FROM github_rest('/users/' || username || '/ssh_signing_keys?per_page=100')"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_user_ssh_signing_keys macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_org(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('organization-full')) AS r "
	    "FROM github_rest('/orgs/' || org)"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_org macro: %s", result->GetError());
	}
	result = conn.Query(
	    "CREATE OR REPLACE MACRO github_user_social_accounts(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('social-account')) AS r "
	    "FROM github_rest('/users/' || username || '/social_accounts?per_page=100')"
	    ") _");
	if (result->HasError()) {
		throw InvalidInputException("Failed to register github_user_social_accounts macro: %s", result->GetError());
	}
}

void GithubExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GithubExtension::Name() {
	return "github";
}

std::string GithubExtension::Version() const {
#ifdef EXT_VERSION_GITHUB
	return EXT_VERSION_GITHUB;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(github, loader) {
	duckdb::LoadInternal(loader);
}
}

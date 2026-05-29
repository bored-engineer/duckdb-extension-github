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

struct GitHubRESTBindData : public TableFunctionData {
	string url;
	string host;
	string token;
	string user_agent;
	string accept;
	string api_version;
	vector<pair<string, string>> extra_headers;
};

static unique_ptr<FunctionData> GitHubRESTBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubRESTBindData>();

	// Extract the path from the input — only paths (starting with '/') are accepted
	std::string path = input.inputs[0].GetValue<string>();
	if (!StringUtil::StartsWith(path, "/")) {
		throw InvalidInputException("github_rest expects a path starting with '/', got: %s", path);
	}

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

	// Set the URL to the combined host and path
	result->host = host;
	result->url = host + path;

	// Append any extra query parameters from the named 'query' argument (MAP or STRUCT)
	auto query_param = input.named_parameters.find("query");
	if (query_param != input.named_parameters.end()) {
		auto &qval = query_param->second;
		auto type_id = qval.type().id();
		bool has_query = result->url.find('?') != std::string::npos;

		auto append_pair = [&](const std::string &k, const std::string &v) {
			char *ek = curl_easy_escape(nullptr, k.c_str(), k.size());
			char *ev = curl_easy_escape(nullptr, v.c_str(), v.size());
			result->url += (has_query ? "&" : "?");
			result->url += ek;
			result->url += "=";
			result->url += ev;
			curl_free(ek);
			curl_free(ev);
			has_query = true;
		};

		if (type_id == LogicalTypeId::STRUCT) {
			auto &children = StructValue::GetChildren(qval);
			for (idx_t i = 0; i < children.size(); i++) {
				append_pair(StructType::GetChildName(qval.type(), i), children[i].GetValue<string>());
			}
		} else {
			throw InvalidInputException("'query' must be a STRUCT, got %s", qval.type().ToString());
		}
	}

	// Resolve the bearer token: DuckDB secrets take priority, then environment variables.
	// When GH_HOST is set, GH_ENTERPRISE_TOKEN / GITHUB_ENTERPRISE_TOKEN are checked first.
	std::string token;
	auto try_env = [](const char *name) -> const char * {
		const char *v = std::getenv(name);
		return (v && v[0]) ? v : nullptr;
	};

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
	result->token = token;

	auto accept_param = input.named_parameters.find("accept");
	result->accept = (accept_param != input.named_parameters.end())
	                     ? accept_param->second.GetValue<string>()
	                     : "application/vnd.github+json";

	auto api_version_param = input.named_parameters.find("api_version");
	result->api_version = (api_version_param != input.named_parameters.end())
	                          ? api_version_param->second.GetValue<string>()
	                          : "2026-03-10";

	auto headers_param = input.named_parameters.find("headers");
	if (headers_param != input.named_parameters.end()) {
		auto &hval = headers_param->second;
		if (hval.type().id() != LogicalTypeId::STRUCT) {
			throw InvalidInputException("'headers' must be a STRUCT, got %s", hval.type().ToString());
		}
		auto &children = StructValue::GetChildren(hval);
		for (idx_t i = 0; i < children.size(); i++) {
			result->extra_headers.emplace_back(StructType::GetChildName(hval.type(), i),
			                                   children[i].GetValue<string>());
		}
	}

#ifdef EXT_VERSION_GITHUBCLIENT
	result->user_agent = "duckdb-extension-github/" EXT_VERSION_GITHUBCLIENT " (+https://github.com/bored-engineer/duckdb-extension-github)";
#else
	result->user_agent = "duckdb-extension-github/unknown (+https://github.com/bored-engineer/duckdb-extension-github)";
#endif

	// Set the return types and names
	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);
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

	return std::move(result);
}

static void GitHubRESTFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = const_cast<GitHubRESTBindData &>(data_p.bind_data->Cast<GitHubRESTBindData>());

	// If there's no next page, we're done!
	if (data.url.empty()) {
		return;
	}

	CURL *curl = curl_easy_init();
	if (!curl) {
		throw InvalidInputException("Failed to initialize curl");
	}

	std::string body;
	GitHubResponseHeaders resp_headers;
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
	for (auto &kv : data.extra_headers) {
		std::string header = kv.first + ": " + kv.second;
		headers = curl_slist_append(headers, header.c_str());
	}

	curl_easy_setopt(curl, CURLOPT_URL, data.url.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
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

	if (res_code != CURLE_OK) {
		std::string err_message = "HTTP GET request failed: ";
		err_message += errbuf[0] ? errbuf : curl_easy_strerror(res_code);
		throw InvalidInputException(err_message);
	}
	if (http_status != 200) {
		throw InvalidInputException("HTTP GET request failed. Status: %ld, Reason: %s", http_status, body.c_str());
	}

	// Build the ratelimit struct value (NULL for any absent headers)
	auto parse_usmallint_header = [](const std::string &s) -> Value {
		return s.empty() ? Value(LogicalType::UBIGINT) : Value::UBIGINT(std::stoull(s));
	};
	auto parse_reset_header = [](const std::string &s) -> Value {
		return s.empty() ? Value(LogicalType::TIMESTAMP_S)
		                 : Value::TIMESTAMPSEC(timestamp_sec_t(std::stoll(s)));
	};
	child_list_t<Value> ratelimit_values;
	ratelimit_values.emplace_back("limit", parse_usmallint_header(resp_headers.ratelimit_limit));
	ratelimit_values.emplace_back("remaining", parse_usmallint_header(resp_headers.ratelimit_remaining));
	ratelimit_values.emplace_back("used", parse_usmallint_header(resp_headers.ratelimit_used));
	ratelimit_values.emplace_back("reset", parse_reset_header(resp_headers.ratelimit_reset));
	ratelimit_values.emplace_back("resource", resp_headers.ratelimit_resource.empty()
	                                               ? Value(LogicalType::VARCHAR)
	                                               : Value(resp_headers.ratelimit_resource));

	// Build the headers map value
	vector<Value> header_keys, header_vals;
	for (auto &kv : resp_headers.all) {
		header_keys.emplace_back(kv.first);
		header_vals.emplace_back(kv.second);
	}

	// Store the output
	output.SetValue(0, 0, Value(data.url));
	output.SetValue(1, 0, Value(body));
	output.SetValue(2, 0, Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                 std::move(header_keys), std::move(header_vals)));
	output.SetValue(3, 0, Value::STRUCT(std::move(ratelimit_values)));
	output.SetCardinality(1);

	// Check for the "Link" header to see if there's a next page
	std::string next_url = resp_headers.link.empty() ? "" : ParseLinkNextURL(resp_headers.link);
	if (!next_url.empty() && !StringUtil::StartsWith(next_url, data.host + "/")) {
		throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", next_url);
	}
	data.url = next_url;
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction github_rest_function("github_rest", {LogicalType::VARCHAR}, GitHubRESTFunction, GitHubRESTBind);
	github_rest_function.named_parameters["host"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["accept"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["api_version"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["query"] = LogicalType::ANY;
	github_rest_function.named_parameters["headers"] = LogicalType::ANY;
	loader.RegisterFunction(github_rest_function);
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

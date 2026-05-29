#include "github_common.hpp"

#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"

#include <curl/curl.h>
#include <cstdlib>
#include <cstring>

namespace duckdb {

const char *GitHubUserAgent() {
#ifdef EXT_VERSION_GITHUB
	return "duckdb-extension-github/" EXT_VERSION_GITHUB
	       " (+https://github.com/bored-engineer/duckdb-extension-github)";
#else
	return "duckdb-extension-github/unknown (+https://github.com/bored-engineer/duckdb-extension-github)";
#endif
}

std::string ResolveToken(ClientContext &context, const std::string &host, bool is_enterprise) {
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

vector<pair<string, string>> ParseExtraHeaders(TableFunctionBindInput &input) {
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

vector<pair<string, string>> ParseQueryParams(TableFunctionBindInput &input) {
	vector<pair<string, string>> result;
	auto query_param = input.named_parameters.find("query");
	if (query_param == input.named_parameters.end()) {
		return result;
	}
	auto &qval = query_param->second;
	if (qval.type().id() == LogicalTypeId::STRUCT) {
		auto &children = StructValue::GetChildren(qval);
		for (idx_t i = 0; i < children.size(); i++) {
			if (children[i].IsNull()) {
				continue;
			}
			auto val = children[i].GetValue<string>();
			if (val.empty()) {
				continue;
			}
			result.emplace_back(StructType::GetChildName(qval.type(), i), std::move(val));
		}
	} else if (qval.type().id() == LogicalTypeId::MAP) {
		for (auto &entry : MapValue::GetChildren(qval)) {
			auto &kv = StructValue::GetChildren(entry);
			if (kv[1].IsNull()) {
				continue;
			}
			auto val = kv[1].GetValue<string>();
			if (val.empty()) {
				continue;
			}
			result.emplace_back(kv[0].GetValue<string>(), std::move(val));
		}
	} else {
		throw InvalidInputException("'query' must be a STRUCT or MAP(VARCHAR, VARCHAR), got %s",
		                            qval.type().ToString());
	}
	return result;
}

std::string BuildQueryString(const vector<pair<string, string>> &params) {
	std::string result;
	for (auto &kv : params) {
		if (!result.empty()) {
			result += '&';
		}
		char *enc_key = curl_easy_escape(nullptr, kv.first.c_str(), (int)kv.first.size());
		char *enc_val = curl_easy_escape(nullptr, kv.second.c_str(), (int)kv.second.size());
		result += enc_key;
		result += '=';
		result += enc_val;
		curl_free(enc_key);
		curl_free(enc_val);
	}
	return result;
}

std::string BindCommonRequestData(ClientContext &context, TableFunctionBindInput &input,
                                  GitHubRequestBindData &result) {
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

void AddCommonResultColumns(vector<LogicalType> &return_types, vector<string> &names) {
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

Value RequestIdValue(const GitHubResponseHeaders &resp_headers) {
	return resp_headers.request_id.empty() ? Value(LogicalType::VARCHAR) : Value(resp_headers.request_id);
}

static size_t CurlWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
	auto *body = static_cast<std::string *>(userdata);
	body->append(ptr, size * nmemb);
	return size * nmemb;
}

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

void ExecuteGitHubRequest(const GitHubRequestBindData &data, const std::string *post_body, std::string &body,
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

Value BuildRateLimitValue(const GitHubResponseHeaders &resp_headers) {
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

Value BuildHeadersMapValue(const GitHubResponseHeaders &resp_headers) {
	vector<Value> header_keys, header_vals;
	for (auto &kv : resp_headers.all) {
		header_keys.emplace_back(kv.first);
		header_vals.emplace_back(kv.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(header_keys), std::move(header_vals));
}

} // namespace duckdb

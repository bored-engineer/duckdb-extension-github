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

static size_t CurlHeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
	auto *link_header = static_cast<std::string *>(userdata);
	std::string header(buffer, size * nitems);
	const std::string prefix = "link: ";
	std::string lower_header = StringUtil::Lower(header.substr(0, prefix.size()));
	if (lower_header == prefix) {
		*link_header = header.substr(prefix.size());
		while (!link_header->empty() && (link_header->back() == '\r' || link_header->back() == '\n')) {
			link_header->pop_back();
		}
	}
	return size * nitems;
}

struct GitHubRESTBindData : public TableFunctionData {
	string url;
	string host;
	string token;
	string user_agent;
};

static unique_ptr<FunctionData> GitHubRESTBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubRESTBindData>();

	// Extract the path from the input — only paths (starting with '/') are accepted
	std::string path = input.inputs[0].GetValue<string>();
	if (!StringUtil::StartsWith(path, "/")) {
		throw InvalidInputException("github_rest expects a path starting with '/', got: %s", path);
	}

	// Use GH_HOST to allow overriding the host for GitHub Enterprise, defaulting to api.github.com
	const char *gh_host_env = std::getenv("GH_HOST");
	bool is_enterprise = gh_host_env && gh_host_env[0];
	std::string host = "https://";
	host += is_enterprise ? gh_host_env : "api.github.com";

	// Set the URL to the combined host and path
	result->host = host;
	result->url = host + path;

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
	result->user_agent = StringUtil::Format("%s %s", context.db->config.UserAgent(), DuckDB::SourceID());

	// Set the return types and names
	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("body");
	return_types.emplace_back(LogicalType::JSON());

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
	std::string link_header;
	char errbuf[CURL_ERROR_SIZE] = {0};

	std::string auth_header = "Authorization: Bearer " + data.token;
	struct curl_slist *headers = nullptr;
	std::string user_agent_header = "User-Agent: " + data.user_agent;
	headers = curl_slist_append(headers, auth_header.c_str());
	headers = curl_slist_append(headers, user_agent_header.c_str());
	headers = curl_slist_append(headers, "Accept: application/vnd.github+json");
	headers = curl_slist_append(headers, "X-GitHub-Api-Version: 2026-03-10");
	headers = curl_slist_append(headers, "X-Github-Next-Global-ID: 1");

	curl_easy_setopt(curl, CURLOPT_URL, data.url.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
	curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, CurlHeaderCallback);
	curl_easy_setopt(curl, CURLOPT_HEADERDATA, &link_header);
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

	// Store the output
	output.SetValue(0, 0, Value(data.url));
	output.SetValue(1, 0, Value(body));
	output.SetCardinality(1);

	// Check for the "Link" header to see if there's a next page
	std::string next_url = link_header.empty() ? "" : ParseLinkNextURL(link_header);
	if (!next_url.empty() && !StringUtil::StartsWith(next_url, data.host + "/")) {
		throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", next_url);
	}
	data.url = next_url;
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction github_rest_function("github_rest", {LogicalType::VARCHAR}, GitHubRESTFunction, GitHubRESTBind);
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

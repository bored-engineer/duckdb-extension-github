#define DUCKDB_EXTENSION_MAIN
#include "github_client_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#ifdef USE_ZLIB
#define CPPHTTPLIB_ZLIB_SUPPORT
#endif

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <string>
#include <sstream>

namespace duckdb {

inline void GitHubRESTTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name_t) {
            std::string name = name_t.GetString();
#include "generated_types.cpp"
            throw InvalidInputException("Unknown type: %s", name);
        });
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

// Helper function to return the description of one HTTP error.
static std::string GetHttpErrorMessage(const duckdb_httplib_openssl::Result &res, const std::string &request_type) {
    std::string err_message = "HTTP " + request_type + " request failed. ";

    if (res) {
        err_message += "Status: " + std::to_string(res->status) + ", Reason: " + res->body;
    } else {
        switch (res.error()) {
            case duckdb_httplib_openssl::Error::Connection:
                err_message += "Connection error.";
                break;
            case duckdb_httplib_openssl::Error::BindIPAddress:
                err_message += "Failed to bind IP address.";
                break;
            case duckdb_httplib_openssl::Error::Read:
                err_message += "Error reading response.";
                break;
            case duckdb_httplib_openssl::Error::Write:
                err_message += "Error writing request.";
                break;
            case duckdb_httplib_openssl::Error::ExceedRedirectCount:
                err_message += "Too many redirects.";
                break;
            case duckdb_httplib_openssl::Error::Canceled:
                err_message += "Request was canceled.";
                break;
            case duckdb_httplib_openssl::Error::SSLConnection:
                err_message += "SSL connection failed.";
                break;
            case duckdb_httplib_openssl::Error::SSLLoadingCerts:
                err_message += "Failed to load SSL certificates.";
                break;
            case duckdb_httplib_openssl::Error::SSLServerVerification:
                err_message += "SSL server verification failed.";
                break;
            case duckdb_httplib_openssl::Error::UnsupportedMultipartBoundaryChars:
                err_message += "Unsupported characters in multipart boundary.";
                break;
            case duckdb_httplib_openssl::Error::Compression:
                err_message += "Error during compression.";
                break;
            default:
                err_message += "Unknown error.";
                break;
        }
    }
    return err_message;
}

struct GitHubRESTBindData : public TableFunctionData {
    string url;
    string host;
    unique_ptr<duckdb_httplib_openssl::Client> client;
};

static unique_ptr<FunctionData> GitHubRESTBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names
) {
    auto result = make_uniq<GitHubRESTBindData>();

    // Extract the path from the input
    std::string path = input.inputs[0].GetValue<string>();

    // Default to https://api.github.com/, but allow it to be overridden (GitHub Enterprise)
    std::string host = "https://api.github.com";
    if (StringUtil::StartsWith(path, "http")) {
        if (!StringUtil::StartsWith(path, "https://")) {
            throw InvalidInputException("Invalid URL scheme. Only HTTPS is supported.");
        }
        size_t pos = path.find("/", 8);
        if (pos == std::string::npos) {
            throw InvalidInputException("Invalid URL hostname. Expected format: https://api.github.com/<path>");
        }
        host = path.substr(0, pos);
        path = path.substr(pos);
    }

    // Set the URL to the combined host and path
    result->host = host;
    result->url = host + path;

    // Setup the HTTP client to use for each request
    result->client = make_uniq<duckdb_httplib_openssl::Client>(host);
    result->client->set_read_timeout(60, 0); // 60 seconds
    result->client->set_follow_location(true); // Follow redirects

    // Use the SecretManager to find the 'http' bearer token for GitHub
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    auto secret_match = secret_manager.LookupSecret(transaction, host + "/", "http");
    
    if (!secret_match.HasMatch()) {
        throw InvalidInputException("No GitHub secret found. Please create a 'http' secret with 'CREATE SECRET' first.");
    }

    auto &secret = secret_match.GetSecret();
    if (secret.GetType() != "http") {
        throw InvalidInputException("Invalid secret type. Expected 'http', got '%s'", secret.GetType());
    }

    const auto *kv_secret = dynamic_cast<const KeyValueSecret*>(&secret);
    if (!kv_secret) {
        throw InvalidInputException("Invalid secret type for GitHub secret");
    }

    // Attach the bearer token to every request
    Value token_value;
    if (!kv_secret->TryGetValue("bearer_token", token_value)) {
        throw InvalidInputException("'bearer_token' not found for GitHub secret");
    }
    result->client->set_bearer_token_auth(token_value.ToString());

    // Set the return types and names
    names.emplace_back("url");
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("body");
    return_types.emplace_back(LogicalType::JSON());

    return std::move(result);
}

static void GitHubRESTFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output
) {
    auto &data = const_cast<GitHubRESTBindData&>(data_p.bind_data->Cast<GitHubRESTBindData>());

    // If there's no next page, we're done!
    if (data.url.empty()) {
        return;
    }

    // Perform the HTTP GET request
    auto res = data.client->Get(data.url);
    if (!res || res->status != 200) {
        throw InvalidInputException(GetHttpErrorMessage(res, "GET"));
    }

    // Store the output
    output.SetValue(0, 0, Value(data.url));
    output.SetValue(1, 0, Value(res->body));
    output.SetCardinality(1);

    // Check for the "Link" header to see if there's a next page
    std::string next_url = ParseLinkNextURL(res->get_header_value("Link"));
    if (!next_url.empty() && !StringUtil::StartsWith(next_url, data.host + "/")) {
        throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", next_url);
    }
    data.url = next_url;
}

static void LoadInternal(DatabaseInstance &instance) {
    TableFunction github_rest_function("github_rest", {LogicalType::VARCHAR}, GitHubRESTFunction, GitHubRESTBind);
    ExtensionUtil::RegisterFunction(instance, github_rest_function);
    ScalarFunction github_rest_type_function("github_rest_type", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GitHubRESTTypeFunction);
    ExtensionUtil::RegisterFunction(instance, github_rest_type_function);
}

void GithubClientExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
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
DUCKDB_EXTENSION_API void github_client_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::GithubClientExtension>();
}

DUCKDB_EXTENSION_API const char *github_client_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif


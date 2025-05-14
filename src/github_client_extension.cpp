#define DUCKDB_EXTENSION_MAIN
#include "github_client_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#ifdef USE_ZLIB
#define CPPHTTPLIB_ZLIB_SUPPORT
#endif

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include <string>
#include <sstream>

namespace duckdb {

// Helper function to parse URL and setup client
static std::pair<std::string, std::string> ParseUrl(const std::string &url) {
    std::string scheme, domain, path, client_url;
    size_t pos = url.find("://");
    std::string mod_url = url;
    if (pos == std::string::npos) {
        // Default to https://api.github.com if no scheme is provided
        scheme = "https";
        domain = "api.github.com";
        path = mod_url;
    } else {
        scheme = mod_url.substr(0, pos);
        mod_url.erase(0, pos + 3);
        pos = mod_url.find("/");
        if (pos != std::string::npos) {
            domain = mod_url.substr(0, pos);
            path = mod_url.substr(pos);
        } else {
            domain = mod_url;
            path = "/";
        }
    }

    // Construct client url with scheme if specified
    if (scheme.length() > 0) {
        client_url = scheme + "://" + domain;
    } else {
        client_url = domain;
    }

    return std::make_pair(client_url, path);
}

// Helper function to setup client
static duckdb_httplib_openssl::Client SetupHttpClient(const std::string &url) {
    duckdb_httplib_openssl::Client client(url);
    client.set_read_timeout(60, 0); // 60 seconds
    client.set_follow_location(true); // Follow redirects
    return std::move(client);
}

// Extract the rel=next from the "Link" header
std::string extract_next_link(const std::string &link_header) {
    std::string next_link;
    size_t end = link_header.find(">; rel=\"next\"");
    if (end != std::string::npos) {
        size_t start = link_header.rfind("<", end);
        if (start != std::string::npos) {
            next_link = link_header.substr(start + 1, end - start - 1);
        }
    }
    return next_link;
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

static void GithubRestRequestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    D_ASSERT(args.data.size() == 1);

    UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
        std::string url = input.GetString();
        auto client_url_and_path = ParseUrl(input.GetString());
        auto &client_url = client_url_and_path.first;
        auto &path = client_url_and_path.second;
        auto client = SetupHttpClient(client_url);

        // Set some reasonable headers specific to the GitHub API
        /*duckdb_httplib_openssl::Headers header_map = {
            {"User-Agent", "DuckDB"},
            {"X-Github-Api-Version", "2022-11-28"},
            {"X-Github-Next-Global-ID", "1"},
        };*/

        // Perform the first HTTP request
        //auto res = client.Get(path.c_str(), header_map);
        auto res = client.Get(path.c_str());
        if (!res || res->status != 200) {
            throw InvalidInputException(GetHttpErrorMessage(res, "GET"));
        }

        // If there is no "Link" header, we're done, just return the body as JSON.
        std::string next = extract_next_link(res->get_header_value("link"));
        if (next.empty()) {
            return StringVector::AddString(result, res->body);
        }

        // Verify that the response is a JSON array
        if (!(res->body.length() >= 2 && res->body[0] == '[' && res->body[res->body.length() - 1] == ']')) {
            throw InvalidInputException("Expected JSON array, got: " + res->body);
        }

        // We need to paginate until there are no more pages
        while(!next.empty()) {
            // Verify that the link is the same domain
            if (next.find(client_url + "/") != 0) {
                throw InvalidInputException("Invalid rel=\"next\" link: " + next);
            } else {
                path = next.substr(client_url.length());
            }

            // Perform the next HTTP request
            //auto next_res = client.Get(path.c_str(), header_map);
            auto next_res = client.Get(path.c_str());
            if (!next_res || next_res->status != 200) {
                throw InvalidInputException(GetHttpErrorMessage(next_res, "GET"));
            }
            next = extract_next_link(next_res->get_header_value("link"));

            // Verify that the response is a JSON array
            if (!(next_res->body.length() >= 2 && next_res->body[0] == '[' && next_res->body[next_res->body.length() - 1] == ']')) {
                throw InvalidInputException("Expected JSON array, got: " + next_res->body);
            }

            // Replace the final character (']') with a ',' and append the next response body starting after '['
            res->body[res->body.length() - 1] = ',';
            res->body += next_res->body.substr(1);
        }

        return StringVector::AddString(result, res->body);
    });
}

static void LoadInternal(DatabaseInstance &instance) {
    ExtensionUtil::RegisterFunction(instance, ScalarFunction("github_rest", {LogicalType::VARCHAR}, LogicalType::JSON(), GithubRestRequestFunction));
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


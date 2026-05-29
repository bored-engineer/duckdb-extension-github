#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/string_util.hpp"

#include <string>

namespace duckdb {

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

struct GitHubRequestBindData : public TableFunctionData {
	string url;
	string token;
	string host;
	bool is_enterprise = false;
	string user_agent;
	string accept;
	string api_version;
	vector<pair<string, string>> extra_headers;
};

const char *GitHubUserAgent();
std::string GitHubScheme();
std::string ResolveToken(ClientContext &context, const std::string &host, bool is_enterprise);
vector<pair<string, string>> ParseExtraHeaders(TableFunctionBindInput &input);
vector<pair<string, string>> ParseQueryParams(TableFunctionBindInput &input);
std::string BuildQueryString(const vector<pair<string, string>> &params);
std::string BindCommonRequestData(ClientContext &context, TableFunctionBindInput &input, GitHubRequestBindData &result);
void AddCommonResultColumns(vector<LogicalType> &return_types, vector<string> &names);
Value RequestIdValue(const GitHubResponseHeaders &resp_headers);
void ExecuteGitHubRequest(const GitHubRequestBindData &data, const std::string *post_body, std::string &body,
                          GitHubResponseHeaders &resp_headers);
Value BuildRateLimitValue(const GitHubResponseHeaders &resp_headers);
Value BuildHeadersMapValue(const GitHubResponseHeaders &resp_headers);

} // namespace duckdb

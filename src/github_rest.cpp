#include "github_common.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "yyjson.hpp"

#include <string>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

// Parses the rel="next" URL from the Link header returned by GitHub API
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

struct GitHubRESTBindData : public GitHubRequestBindData {
	bool paginate = true;
	string extract;
};

static unique_ptr<FunctionData> GitHubRESTBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubRESTBindData>();

	std::string path = input.inputs[0].GetValue<string>();
	if (!StringUtil::StartsWith(path, "/")) {
		throw InvalidInputException("github_rest expects a path starting with '/', got: %s", path);
	}

	std::string host = BindCommonRequestData(context, input, *result);
	result->host = host;
	result->url = host + path;

	auto query_params = ParseQueryParams(input);
	if (!query_params.empty()) {
		std::string qs = BuildQueryString(query_params);
		result->url += (path.find('?') != std::string::npos ? '&' : '?');
		result->url += qs;
	}

	auto paginate_param = input.named_parameters.find("paginate");
	if (paginate_param != input.named_parameters.end()) {
		result->paginate = paginate_param->second.GetValue<bool>();
	}

	auto extract_param = input.named_parameters.find("array_key");
	if (extract_param != input.named_parameters.end()) {
		result->extract = extract_param->second.GetValue<string>();
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

	if (data.token.empty()) {
		data.token = ResolveToken(context, data.host, data.is_enterprise);
	}

	std::string body;
	GitHubResponseHeaders resp_headers;
	ExecuteGitHubRequest(data, nullptr, body, resp_headers);

	Value page_url = Value(data.url);
	Value page_headers = BuildHeadersMapValue(resp_headers);
	Value page_ratelimit = BuildRateLimitValue(resp_headers);
	Value page_request_id = RequestIdValue(resp_headers);

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), 0);
	yyjson_val *root = doc ? yyjson_doc_get_root(doc) : nullptr;
	idx_t count = 0;
	yyjson_val *arr = root;
	if (!data.extract.empty() && root && yyjson_is_obj(root)) {
		arr = yyjson_obj_get(root, data.extract.c_str());
	}
	if (arr && yyjson_is_arr(arr)) {
		yyjson_arr_iter it;
		yyjson_arr_iter_init(arr, &it);
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

	std::string next_url;
	if (data.paginate) {
		next_url = resp_headers.link.empty() ? "" : ParseLinkNextURL(resp_headers.link);
		if (!next_url.empty() && !StringUtil::StartsWith(next_url, data.host + "/")) {
			throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", next_url);
		}
	}
	data.url = next_url;
}

void RegisterGitHubRESTFunction(ExtensionLoader &loader) {
	TableFunction github_rest_function("github_rest", {LogicalType::VARCHAR}, GitHubRESTFunction, GitHubRESTBind);
	github_rest_function.named_parameters["host"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["accept"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["api_version"] = LogicalType::VARCHAR;
	github_rest_function.named_parameters["headers"] = LogicalType::ANY;
	github_rest_function.named_parameters["query"] = LogicalType::ANY;
	github_rest_function.named_parameters["paginate"] = LogicalType::BOOLEAN;
	github_rest_function.named_parameters["array_key"] = LogicalType::VARCHAR;
	loader.RegisterFunction(github_rest_function);
}

} // namespace duckdb

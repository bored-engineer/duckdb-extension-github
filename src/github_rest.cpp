#include "github_common.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

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
	string query_suffix; // pre-built query string (without leading ? or &)
};

static unique_ptr<FunctionData> GitHubRESTBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GitHubRESTBindData>();

	BindCommonRequestData(context, input, *result);

	auto query_params = ParseQueryParams(input);
	if (!query_params.empty()) {
		result->query_suffix = BuildQueryString(query_params);
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

struct GitHubRESTLocalState : public LocalTableFunctionState {
	idx_t current_input_row = 0;
	bool initialized_row = false;
	string current_url; // current pagination URL; empty means done with this input row
	string token;       // resolved once per local state lifetime
};

static unique_ptr<LocalTableFunctionState> GitHubRESTLocalInit(ExecutionContext &context,
                                                               TableFunctionInitInput & /*input*/,
                                                               GlobalTableFunctionState * /*global_state*/) {
	return make_uniq<GitHubRESTLocalState>();
}

static OperatorResultType GitHubRESTInOutFunction(ExecutionContext &context, TableFunctionInput &data_p,
                                                  DataChunk &input, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<GitHubRESTBindData>();
	auto &state = data_p.local_state->Cast<GitHubRESTLocalState>();

	input.Flatten();

	while (true) {
		if (state.current_input_row >= input.size()) {
			state.current_input_row = 0;
			state.initialized_row = false;
			return OperatorResultType::NEED_MORE_INPUT;
		}

		if (!state.initialized_row) {
			if (FlatVector::IsNull(input.data[0], state.current_input_row)) {
				output.SetCardinality(0);
				state.current_input_row++;
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}

			string path = FlatVector::GetValue<string_t>(input.data[0], state.current_input_row).GetString();
			if (!StringUtil::StartsWith(path, "/")) {
				throw InvalidInputException("github_rest expects a path starting with '/', got: %s", path);
			}

			string url = bind_data.host + path;
			if (!bind_data.query_suffix.empty()) {
				url += (path.find('?') != string::npos ? '&' : '?');
				url += bind_data.query_suffix;
			}

			if (state.token.empty()) {
				state.token = ResolveToken(context.client, bind_data.host, bind_data.is_enterprise);
			}

			state.current_url = url;
			state.initialized_row = true;
		}

		if (state.current_url.empty()) {
			// done with all pages for this input row
			output.SetCardinality(0);
			state.current_input_row++;
			state.initialized_row = false;
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}

		GitHubRequestBindData req;
		req.url = state.current_url;
		req.token = state.token;
		req.host = bind_data.host;
		req.is_enterprise = bind_data.is_enterprise;
		req.user_agent = bind_data.user_agent;
		req.accept = bind_data.accept;
		req.api_version = bind_data.api_version;
		req.extra_headers = bind_data.extra_headers;

		std::string body;
		GitHubResponseHeaders resp_headers;
		ExecuteGitHubRequest(req, nullptr, body, resp_headers);

		Value page_url = Value(state.current_url);
		Value page_headers = BuildHeadersMapValue(resp_headers);
		Value page_ratelimit = BuildRateLimitValue(resp_headers);
		Value page_request_id = RequestIdValue(resp_headers);

		yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), 0);
		yyjson_val *root = doc ? yyjson_doc_get_root(doc) : nullptr;
		idx_t count = 0;
		yyjson_val *arr = root;
		if (!bind_data.extract.empty() && root && yyjson_is_obj(root)) {
			arr = yyjson_obj_get(root, bind_data.extract.c_str());
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
		if (bind_data.paginate) {
			next_url = resp_headers.link.empty() ? "" : ParseLinkNextURL(resp_headers.link);
			if (!next_url.empty() && !StringUtil::StartsWith(next_url, bind_data.host + "/")) {
				throw InvalidInputException("Unexpected Link header for GitHub pagination: %s", next_url);
			}
		}

		if (next_url.empty()) {
			// no more pages; advance to next input row on the next call
			state.current_url = "";
		} else {
			state.current_url = next_url;
		}

		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

void RegisterGitHubRESTFunction(ExtensionLoader &loader) {
	TableFunction github_rest_function("github_rest", {LogicalType::VARCHAR}, nullptr, GitHubRESTBind, nullptr,
	                                   GitHubRESTLocalInit);
	github_rest_function.in_out_function = GitHubRESTInOutFunction;
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

#include "github_common.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "yyjson.hpp"

#include <string>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

struct GitHubContentsRawData : public FunctionData {
	std::string token;
	std::string host;
	std::string user_agent;
	std::string ref;

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

	if (arguments.size() > 3 && arguments[3]->IsFoldable()) {
		result->ref = ExpressionExecutor::EvaluateScalar(context, *arguments[3]).GetValue<string>();
	}

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

void RegisterGitHubContentsFunction(ExtensionLoader &loader) {
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
}

} // namespace duckdb

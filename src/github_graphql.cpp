#include "github_common.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "yyjson.hpp"

#include <string>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

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
	result->host = host;
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

	if (data.token.empty()) {
		data.token = ResolveToken(context, data.host, data.is_enterprise);
	}

	std::string post_body = BuildGraphQLBody(data.query, data.variables_json, data.cursor);
	std::string body;
	GitHubResponseHeaders resp_headers;
	ExecuteGitHubRequest(data, &post_body, body, resp_headers);

	yyjson_doc *doc = yyjson_read(body.c_str(), body.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse GraphQL response as JSON");
	}
	yyjson_val *root = yyjson_doc_get_root(doc);

	yyjson_val *errors = yyjson_is_obj(root) ? yyjson_obj_get(root, "errors") : nullptr;
	if (!data.ignore_errors && errors && yyjson_is_arr(errors) && yyjson_arr_size(errors) > 0) {
		char *errors_json = yyjson_val_write(errors, 0, nullptr);
		std::string message = errors_json ? errors_json : "";
		free(errors_json);
		yyjson_doc_free(doc);
		throw InvalidInputException("GraphQL query returned errors: %s", message);
	}

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

	if (!data.paginate || next_cursor.empty() || next_cursor == data.cursor) {
		data.done = true;
	} else {
		data.cursor = next_cursor;
	}
}

void RegisterGitHubGraphQLFunction(ExtensionLoader &loader) {
	TableFunction github_graphql_function("github_graphql", {LogicalType::VARCHAR}, GitHubGraphQLFunction,
	                                      GitHubGraphQLBind);
	github_graphql_function.named_parameters["host"] = LogicalType::VARCHAR;
	github_graphql_function.named_parameters["variables"] = LogicalType::ANY;
	github_graphql_function.named_parameters["headers"] = LogicalType::ANY;
	github_graphql_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	github_graphql_function.named_parameters["paginate"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(github_graphql_function);
}

} // namespace duckdb

#include "github_common.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

#include <map>
#include <string>

namespace duckdb {

std::map<std::string, std::string> generated_types = {
#include "generated_types.cpp"
};

static string_t LookupRESTType(const string_t &name, Vector &result) {
	auto it = generated_types.find(name.GetString());
	if (it == generated_types.end()) {
		throw InvalidInputException("Unknown type: %s", name.GetString());
	}
	return StringVector::AddString(result, it->second);
}

static void GitHubRESTTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(),
	                                           [&](string_t name) { return LookupRESTType(name, result); });
}

void RegisterGitHubRESTTypeFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    ScalarFunction("github_rest_type", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GitHubRESTTypeFunction));
}

} // namespace duckdb

#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct GithubFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterGithubRequestFunction(db);
	}

private:
	//! Register GitHub functions
	static void RegisterGithubRequestFunction(DatabaseInstance &db);
};

} // namespace duckdb

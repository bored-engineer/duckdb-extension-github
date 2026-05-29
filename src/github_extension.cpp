#include "github_extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void RegisterGitHubRESTFunction(ExtensionLoader &loader);
void RegisterGitHubGraphQLFunction(ExtensionLoader &loader);
void RegisterGitHubContentsFunction(ExtensionLoader &loader);
void RegisterGitHubRESTTypeFunction(ExtensionLoader &loader);
void RegisterGitHubMacros(ExtensionLoader &loader);

static void LoadInternal(ExtensionLoader &loader) {
	RegisterGitHubRESTTypeFunction(loader);
	RegisterGitHubRESTFunction(loader);
	RegisterGitHubGraphQLFunction(loader);
	RegisterGitHubContentsFunction(loader);
	RegisterGitHubMacros(loader);
}

void GithubExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GithubExtension::Name() {
	return "github";
}

std::string GithubExtension::Version() const {
#ifdef EXT_VERSION_GITHUB
	return EXT_VERSION_GITHUB;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(github, loader) {
	duckdb::LoadInternal(loader);
}
}

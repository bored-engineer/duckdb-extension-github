# duckdb-extension-github
A DuckDB extension for querying the [GitHub REST API](https://docs.github.com/en/rest) and [GraphQL API](https://docs.github.com/en/graphql) directly from SQL.

## Installation
> [!WARNING]  
> This does not work yet as the extension needs to be accepted to the community repository.
```sql
INSTALL github FROM community;
LOAD github;
```

## Authentication
Authentication is typically handled via a DuckDB `http` secret, scoped to the GitHub API:
```sql
CREATE SECRET github (
    TYPE http,
    BEARER_TOKEN 'ghp_...',
    SCOPE 'https://api.github.com'
);
```
However, if no secret is found, the value of the `GH_TOKEN` or `GITHUB_TOKEN` environment variable will be checked.

When using a GitHub Enterprise host (via the `host` parameter or `GH_HOST` env var), the `GH_ENTERPRISE_TOKEN` and `GITHUB_ENTERPRISE_TOKEN` environment variables will be checked.

## Functions

### `github_rest(path, ...)`
Makes a GET request to the GitHub REST API. When the response is a JSON array, one row is returned per element. Non-array responses return a single row.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `VARCHAR` | required | API path starting with `/`, e.g. `/repos/owner/repo` |
| `host` | `VARCHAR` | `api.github.com` | Override the API host (GitHub Enterprise) |
| `accept` | `VARCHAR` | `application/vnd.github+json` | `Accept` header value |
| `api_version` | `VARCHAR` | `2026-03-10` | `X-GitHub-Api-Version` header value |
| `headers` | `STRUCT` | — | Additional request headers |
| `query` | `STRUCT` or `MAP(VARCHAR, VARCHAR)` | — | Query string parameters (null/empty values are omitted) |
| `paginate` | `BOOLEAN` | `true` | Follow `Link` pagination headers |
| `array_key` | `VARCHAR` | — | When set, extract this key's array from an object response instead of returning the whole object |

**Returns:**

| Column | Type | Description |
|---|---|---|
| `url` | `VARCHAR` | Request URL |
| `data` | `JSON` | Response body (or single array element when response is an array) |
| `headers` | `MAP(VARCHAR, VARCHAR)` | Response headers (lowercased names) |
| `ratelimit` | `STRUCT(limit UBIGINT, remaining UBIGINT, used UBIGINT, reset TIMESTAMP_S, resource VARCHAR)` | Rate limit info |
| `request_id` | `VARCHAR` | `X-GitHub-Request-Id` header value |

**Examples:**

```sql
-- Get a single repository
SELECT
    data->'owner'->>'login' AS owner,
    data->>'name' AS repository,
    (data->>'stargazers_count')::BIGINT AS stars,
FROM github_rest('/repos/duckdb/duckdb');

-- List all repos for a user (automatically paginates, one row per repo)
SELECT data->>'name', data->>'language'
FROM github_rest('/users/bored-engineer/repos', query := {'per_page': '100'});

-- Fetch a single page only, no pagination
SELECT count(*) FROM github_rest(
    '/users/bored-engineer/repos',
    query := {'per_page': '30'},
    paginate := false
);

-- Extract an array nested inside an object response
SELECT data->>'name'
FROM github_rest('/search/repositories', query := {'q': 'duckdb', 'per_page': '100'}, array_key := 'items');

-- GitHub Enterprise
SELECT data->>'name'
FROM github_rest('/repos/owner/repo', host := 'github.mycompany.com');
```

---

### `github_graphql(query, ...)`

Makes a POST request to the GitHub GraphQL API (`/graphql`). Automatically paginates by injecting the `$endCursor` variable when the response contains a `pageInfo { hasNextPage endCursor }` object.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `query` | `VARCHAR` | required | GraphQL query string |
| `host` | `VARCHAR` | `api.github.com` | Override the API host (GitHub Enterprise) |
| `variables` | `STRUCT` | — | GraphQL variables (encoded as JSON) |
| `headers` | `STRUCT` | — | Additional request headers |
| `ignore_errors` | `BOOLEAN` | `false` | When `true`, return rows even if the response contains errors |
| `paginate` | `BOOLEAN` | `true` | When `false`, return only the first page without following `pageInfo` cursors |

**Returns:**

| Column | Type | Description |
|---|---|---|
| `url` | `VARCHAR` | Request URL |
| `data` | `JSON` | The `data` field from the response |
| `headers` | `MAP(VARCHAR, VARCHAR)` | Response headers (lowercased names) |
| `ratelimit` | `STRUCT(limit UBIGINT, remaining UBIGINT, used UBIGINT, reset TIMESTAMP_S, resource VARCHAR)` | Rate limit info |
| `request_id` | `VARCHAR` | `X-GitHub-Request-Id` header value |
| `errors` | `JSON[]` | GraphQL errors (empty list when none) |
| `warnings` | `JSON[]` | GraphQL warnings from `extensions.warnings` (empty list when none) |

**Pagination:**

Include `pageInfo { hasNextPage endCursor }` in your query and declare `$endCursor: String` as a variable. The extension automatically injects the cursor from each page's `pageInfo` into the next request.

**Examples:**

```sql
-- Get the authenticated user's login
SELECT data->'viewer'->>'login'
FROM github_graphql('query { viewer { login } }');

-- Make a query using a variable
SELECT data->'user'->>'name'
FROM github_graphql(
    'query($login: String!) {
        user(login: $login) {
            name
        }
    }',
    variables := {'login': 'bored-engineer'}
);

-- Automatically paginate through results
SELECT UNNEST(data->>'$.user.repositories.nodes[*].name') AS name
FROM github_graphql(
    'query($login: String!, $endCursor: String) {
        user(login: $login) {
            repositories(first: 100, after: $endCursor) {
                nodes {
                    name
                    stargazerCount
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    }',
    variables := {'login': 'bored-engineer'}
);

-- Fetch only the first page, no pagination
SELECT data->'user'->'repositories'->>'nodes'
FROM github_graphql(
    'query($login: String!, $endCursor: String) {
        user(login: $login) {
            repositories(first: 10, after: $endCursor) {
                nodes { name }
                pageInfo { hasNextPage endCursor }
            }
        }
    }',
    variables := {'login': 'bored-engineer'},
    paginate := false
);

-- Ignore errors and inspect them manually
SELECT data, errors[1]->>'message' AS error
FROM github_graphql(
    'query { viewer { nonexistentField } }',
    ignore_errors := true
);

-- GitHub Enterprise
SELECT data->'viewer'->>'login'
FROM github_graphql(
    'query { viewer { login } }',
    host := 'github.mycompany.com'
);
```

---

### `github_contents(owner, repo, path, ...)`

Lists the contents of a file or directory in a GitHub repository. For directories, returns one row per entry (plus optionally the directory itself). For files, returns a single row with the decoded content.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `owner` | `VARCHAR` | required | Repository owner (user or organisation) |
| `repo` | `VARCHAR` | required | Repository name |
| `path` | `VARCHAR` | required | Path within the repository |
| `ref` | `VARCHAR` | — | Branch, tag, or commit SHA; defaults to the repository's default branch |
| `host` | `VARCHAR` | `api.github.com` | Override the API host (GitHub Enterprise) |
| `api_version` | `VARCHAR` | `2026-03-10` | `X-GitHub-Api-Version` header value |
| `ignore_incomplete` | `BOOLEAN` | `false` | Suppress the error raised when a directory has 1000 entries (GitHub's maximum) |
| `include_root` | `BOOLEAN` | `true` | Include the directory entry itself as the first row when listing a directory |

**Returns:**

| Column | Type | Description |
|---|---|---|
| `type` | `VARCHAR` | Entry type: `file`, `dir`, `symlink`, or `submodule` |
| `size` | `UBIGINT` | File size in bytes |
| `name` | `VARCHAR` | Entry name |
| `path` | `VARCHAR` | Full path within the repository |
| `content` | `BLOB` | Decoded file content (`NULL` for directories) |
| `sha` | `VARCHAR` | Git blob SHA |
| `url` | `VARCHAR` | API URL |
| `git_url` | `VARCHAR` | Git URL |
| `download_url` | `VARCHAR` | Raw download URL |
| `submodule_git_url` | `VARCHAR` | Submodule target URL (submodules only) |
| `target` | `VARCHAR` | Symlink target (symlinks only) |

**Examples:**

```sql
-- List files at the root of a repository
SELECT type, name, size FROM github_contents('duckdb', 'duckdb', '');

-- Read a single file's content
SELECT content::VARCHAR FROM github_contents('duckdb', 'duckdb', 'README.md');

-- List a directory on a specific branch
SELECT name FROM github_contents('duckdb', 'duckdb', 'src', ref := 'main');
```

---

### `github_contents_raw(owner, repo, path[, ref[, host]])`

Fetches the raw contents of a file from a GitHub repository. Makes a GET request to `/repos/{owner}/{repo}/contents/{path}` with `Accept: application/vnd.github.raw+json` and returns the response body as a `BLOB`.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `owner` | `VARCHAR` | required | Repository owner (user or organisation) |
| `repo` | `VARCHAR` | required | Repository name |
| `path` | `VARCHAR` | required | Path to the file within the repository |
| `ref` | `VARCHAR` | `''` | Branch, tag, or commit SHA to read from; empty string uses the repository's default branch |
| `host` | `VARCHAR` | `''` | API hostname (GitHub Enterprise); empty string falls back to `GH_HOST` env var or `api.github.com` |

**Returns:** `BLOB` — raw file contents. Cast to `VARCHAR` if you need text operations (e.g. `::VARCHAR`).

**Examples:**

```sql
-- Read a file from the default branch
SELECT github_contents_raw('bored-engineer', 'duckdb-extension-github', 'README.md');

-- Read a file from a specific branch or tag
SELECT github_contents_raw('bored-engineer', 'duckdb-extension-github', 'README.md', 'main');

-- GitHub Enterprise (empty string ref = default branch)
SELECT github_contents_raw('owner', 'repo', 'path', '', 'github.mycompany.com');

-- Cast to VARCHAR for text operations
SELECT github_contents_raw('bored-engineer', 'duckdb-extension-github', 'README.md')::VARCHAR;
```

---

### `github_rest_type(name)`
Returns the JSON type schema string for a named GitHub API type, for use with DuckDB's `json_transform()` function.

**Examples:**

```sql
-- Transform each REST response row into a typed struct
SELECT json_transform(data, github_rest_type('repository'))
FROM github_rest('/users/bored-engineer/repos', query := {'per_page': '100'});
```

---

## Macros

All macros return typed structs via `json_transform` and paginate automatically where applicable. Optional parameters default to `NULL` (omitted from the request).

### Repositories

| Macro | Description |
|---|---|
| `github_repo(owner, repo)` | Get a repository |
| `github_repo_forks(owner, repo, sort := NULL)` | List forks |
| `github_repo_contributors(owner, repo, anon := NULL)` | List contributors |
| `github_repo_languages(owner, repo)` | List languages and byte counts |
| `github_repo_tags(owner, repo)` | List tags |
| `github_repo_teams(owner, repo)` | List teams with access |
| `github_repo_autolinks(owner, repo)` | List autolink references |
| `github_repo_autolink(owner, repo, autolink_id)` | Get an autolink reference |
| `github_repo_properties(owner, repo)` | List custom property values |
| `github_repo_activity(owner, repo, time_period := NULL, activity_type := NULL, actor := NULL, ref := NULL, direction := NULL)` | List repository activity |
| `github_repo_codeowners_errors(owner, repo, ref := NULL)` | List CODEOWNERS errors |
| `github_repo_deploy_keys(owner, repo)` | List deploy keys |
| `github_repo_events(owner, repo)` | List repository events |
| `github_network_events(owner, repo)` | List network events |

### Branches

| Macro | Description |
|---|---|
| `github_repo_branches(owner, repo, protected := NULL, per_page := NULL)` | List branches |
| `github_repo_branch(owner, repo, branch)` | Get a branch with protection info |
| `github_repo_branch_protection(owner, repo, branch)` | Get branch protection rules |
| `github_repo_rules_for_branch(owner, repo, branch)` | List rules that apply to a branch |

### Commits

| Macro | Description |
|---|---|
| `github_commits(owner, repo, sha := NULL, path := NULL, author := NULL, committer := NULL, since := NULL, until := NULL)` | List commits |
| `github_commit(owner, repo, ref)` | Get a commit |
| `github_commit_pulls(owner, repo, commit_sha)` | List pull requests for a commit |
| `github_commit_comments(owner, repo, commit_sha)` | List comments on a commit |
| `github_repo_commit_comments(owner, repo)` | List all commit comments in a repository |
| `github_commit_status(owner, repo, ref)` | Get the combined commit status |
| `github_commit_statuses(owner, repo, ref)` | List commit statuses |

### Pull Requests

| Macro | Description |
|---|---|
| `github_repo_pulls(owner, repo, state := NULL, head := NULL, base := NULL, sort := NULL, direction := NULL)` | List pull requests |
| `github_repo_pull(owner, repo, pull_number)` | Get a pull request |
| `github_repo_pull_commits(owner, repo, pull_number)` | List commits on a pull request |
| `github_repo_pull_files(owner, repo, pull_number)` | List files changed in a pull request |
| `github_repo_pull_reviews(owner, repo, pull_number)` | List reviews on a pull request |
| `github_repo_pull_review(owner, repo, pull_number, review_id)` | Get a pull request review |
| `github_repo_pull_review_comments(owner, repo, pull_number)` | List review comments on a pull request |

### Issues

| Macro | Description |
|---|---|
| `github_repo_issues(owner, repo, milestone := NULL, state := NULL, assignee := NULL, creator := NULL, mentioned := NULL, labels := NULL, sort := NULL, direction := NULL, since := NULL)` | List issues |
| `github_repo_issue(owner, repo, issue_number)` | Get an issue |
| `github_repo_issue_labels(owner, repo, issue_number)` | List labels on an issue |
| `github_repo_issue_timeline(owner, repo, issue_number)` | List timeline events for an issue |
| `github_repo_issue_assignees(owner, repo, issue_number)` | List assignees of an issue |
| `github_repo_issue_comments(owner, repo, issue_number, since := NULL)` | List comments on an issue |
| `github_repo_issue_comment(owner, repo, comment_id)` | Get an issue comment |
| `github_repo_issue_comments(owner, repo, sort := NULL, direction := NULL, since := NULL)` | List all issue comments in a repository |
| `github_repo_issue_events(owner, repo, issue_number)` | List events for an issue |
| `github_repo_issue_event(owner, repo, event_id)` | Get an issue event |
| `github_repo_issue_events(owner, repo)` | List all issue events in a repository |

### Labels

| Macro | Description |
|---|---|
| `github_repo_labels(owner, repo)` | List labels in a repository |
| `github_repo_label(owner, repo, name)` | Get a label |

### Releases

| Macro | Description |
|---|---|
| `github_repo_releases(owner, repo)` | List releases |
| `github_repo_release(owner, repo, release_id)` | Get a release |
| `github_repo_release_latest(owner, repo)` | Get the latest release |
| `github_repo_release_by_tag(owner, repo, tag)` | Get a release by tag name |

### Webhooks

| Macro | Description |
|---|---|
| `github_repo_webhooks(owner, repo)` | List webhooks |
| `github_repo_webhook(owner, repo, hook_id)` | Get a webhook |
| `github_repo_webhook_config(owner, repo, hook_id)` | Get a webhook's configuration |
| `github_repo_webhook_deliveries(owner, repo, hook_id)` | List webhook deliveries |
| `github_repo_webhook_delivery(owner, repo, hook_id, delivery_id)` | Get a webhook delivery |

### Rulesets

| Macro | Description |
|---|---|
| `github_repo_rulesets(owner, repo, includes_parents := NULL)` | List repository rulesets |
| `github_repo_ruleset(owner, repo, ruleset_id, includes_parents := NULL)` | Get a repository ruleset |
| `github_repo_ruleset_history(owner, repo, ruleset_id)` | List versions of a repository ruleset |
| `github_repo_ruleset_history_version(owner, repo, ruleset_id, version_id)` | Get a specific version of a repository ruleset |
| `github_org_rulesets(org, includes_parents := NULL)` | List organization rulesets |
| `github_org_ruleset(org, ruleset_id, includes_parents := NULL)` | Get an organization ruleset |
| `github_org_ruleset_history(org, ruleset_id)` | List versions of an organization ruleset |
| `github_org_ruleset_history_version(org, ruleset_id, version_id)` | Get a specific version of an organization ruleset |

### Organizations

| Macro | Description |
|---|---|
| `github_org(org)` | Get an organization |
| `github_org_repos(org, type := NULL, sort := NULL, direction := NULL)` | List organization repositories |
| `github_org_members(org, filter := NULL, role := NULL)` | List organization members |
| `github_org_public_members(org)` | List public organization members |
| `github_org_invitations(org)` | List pending organization invitations |
| `github_org_failed_invitations(org)` | List failed organization invitations |
| `github_org_outside_collaborators(org, filter := NULL)` | List outside collaborators |
| `github_org_installations(org)` | List app installations for an organization |
| `github_org_events(org)` | List organization events |
| `github_org_secret_scanning_alerts(org, state := NULL, secret_type := NULL, resolution := NULL, sort := NULL, direction := NULL)` | List secret scanning alerts for an organization |

### Teams

| Macro | Description |
|---|---|
| `github_teams(org)` | List teams in an organization |
| `github_team(org, team_slug)` | Get a team |
| `github_team_repos(org, team_slug)` | List repositories for a team |
| `github_team_teams(org, team_slug)` | List child teams |
| `github_team_members(org, team_slug, role := NULL)` | List team members |
| `github_team_member(org, team_slug, username)` | Get a user's team membership |

### Users

| Macro | Description |
|---|---|
| `github_user(username)` | Get a user |
| `github_user_followers(username)` | List followers |
| `github_user_gpg_keys(username)` | List GPG keys |
| `github_user_ssh_keys(username)` | List SSH keys |
| `github_user_ssh_signing_keys(username)` | List SSH signing keys |
| `github_user_social_accounts(username)` | List social accounts |
| `github_user_orgs(username)` | List organizations |
| `github_user_repos(username, type := NULL, sort := NULL, direction := NULL)` | List repositories |
| `github_user_events(username)` | List events |
| `github_user_gists(username, since := NULL)` | List gists |

### Gists

| Macro | Description |
|---|---|
| `github_gist(gist_id)` | Get a gist |
| `github_gist_forks(gist_id)` | List gist forks |
| `github_gist_commits(gist_id)` | List gist commits |
| `github_gist_revision(gist_id, sha)` | Get a specific gist revision |

### Security

| Macro | Description |
|---|---|
| `github_security_advisories(ghsa_id := NULL, cve_id := NULL, ecosystem := NULL, severity := NULL, cwes := NULL, is_withdrawn := NULL, affects := NULL, published := NULL, updated := NULL, modified := NULL, type := NULL, direction := NULL, sort := NULL)` | List global security advisories |
| `github_security_advisory(ghsa_id)` | Get a global security advisory |
| `github_repo_security_advisories(owner, repo, direction := NULL, sort := NULL, before := NULL, after := NULL, ecosystem := NULL, severity := NULL, cwes := NULL, cve_id := NULL, ghsa_id := NULL, state := NULL)` | List repository security advisories |
| `github_repo_security_advisory(owner, repo, ghsa_id)` | Get a repository security advisory |
| `github_repo_secret_scanning_alerts(owner, repo, state := NULL, secret_type := NULL, resolution := NULL, sort := NULL, direction := NULL)` | List secret scanning alerts for a repository |
| `github_repo_secret_scanning_alert(owner, repo, alert_number)` | Get a secret scanning alert |
| `github_repo_secret_scanning_alert_locations(owner, repo, alert_number)` | List locations for a secret scanning alert |
| `github_repo_secret_scanning_scan_history(owner, repo)` | Get secret scanning scan history |

### Search

| Macro | Description |
|---|---|
| `github_search_code(q, sort := NULL, direction := NULL)` | Search code |
| `github_search_commits(q, sort := NULL, direction := NULL)` | Search commits |
| `github_search_issues(q, sort := NULL, direction := NULL)` | Search issues and pull requests |
| `github_search_labels(q, repository_id, sort := NULL, direction := NULL)` | Search labels |
| `github_search_repos(q, sort := NULL, direction := NULL)` | Search repositories |
| `github_search_topics(q)` | Search topics |
| `github_search_users(q, sort := NULL, direction := NULL)` | Search users |

### Apps & Meta

| Macro | Description |
|---|---|
| `github_app(app_slug)` | Get a GitHub App |
| `github_licenses()` | List commonly used licenses |
| `github_meta()` | Get GitHub API metadata (IP ranges, features, etc.) |
| `github_zen()` | Get a random GitHub design philosophy |
| `github_ratelimit()` | Get current rate limit status for all resources |

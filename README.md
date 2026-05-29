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

The following environment variables control connection behaviour, primarily for GitHub Enterprise instances:

| Variable | Default | Description |
|---|---|---|
| `GH_HOST` | `api.github.com` | Default API hostname |
| `GH_HOST_SSL` | `true` | Set to `false` to use `http://` instead of `https://` |
| `GH_HOST_SSL_VERIFYPEER` | `true` | Set to `false` to disable `CURLOPT_SSL_VERIFYPEER` |
| `GH_HOST_SSL_VERIFYHOST` | `true` | Set to `false` to disable `CURLOPT_SSL_VERIFYHOST` |

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

| Macro | API Endpoint |
|---|---|
| `github_repo(owner, repo)` | [`GET /repos/{owner}/{repo}`](https://docs.github.com/en/rest/repos/repos#get-a-repository) |
| `github_repo_forks(owner, repo, sort := NULL)` | [`GET /repos/{owner}/{repo}/forks`](https://docs.github.com/en/rest/repos/forks#list-forks) |
| `github_repo_contributors(owner, repo, anon := NULL)` | [`GET /repos/{owner}/{repo}/contributors`](https://docs.github.com/en/rest/repos/repos#list-repository-contributors) |
| `github_repo_languages(owner, repo)` | [`GET /repos/{owner}/{repo}/languages`](https://docs.github.com/en/rest/repos/repos#list-repository-languages) |
| `github_repo_tags(owner, repo)` | [`GET /repos/{owner}/{repo}/tags`](https://docs.github.com/en/rest/repos/repos#list-repository-tags) |
| `github_repo_teams(owner, repo)` | [`GET /repos/{owner}/{repo}/teams`](https://docs.github.com/en/rest/repos/repos#list-repository-teams) |
| `github_repo_autolinks(owner, repo)` | [`GET /repos/{owner}/{repo}/autolinks`](https://docs.github.com/en/rest/repos/autolinks#list-all-autolinks-of-a-repository) |
| `github_repo_autolink(owner, repo, autolink_id)` | [`GET /repos/{owner}/{repo}/autolinks/{autolink_id}`](https://docs.github.com/en/rest/repos/autolinks#get-an-autolink-reference-of-a-repository) |
| `github_repo_properties(owner, repo)` | [`GET /repos/{owner}/{repo}/properties/values`](https://docs.github.com/en/rest/repos/custom-properties#get-all-custom-property-values-for-a-repository) |
| `github_repo_activity(owner, repo, time_period := NULL, activity_type := NULL, actor := NULL, ref := NULL, direction := NULL)` | [`GET /repos/{owner}/{repo}/activity`](https://docs.github.com/en/rest/repos/repos#list-repository-activities) |
| `github_repo_codeowners_errors(owner, repo, ref := NULL)` | [`GET /repos/{owner}/{repo}/codeowners/errors`](https://docs.github.com/en/rest/repos/repos#list-codeowners-errors) |
| `github_repo_deploy_keys(owner, repo)` | [`GET /repos/{owner}/{repo}/keys`](https://docs.github.com/en/rest/deploy-keys/deploy-keys#list-deploy-keys) |
| `github_repo_events(owner, repo)` | [`GET /repos/{owner}/{repo}/events`](https://docs.github.com/en/rest/activity/events#list-repository-events) |
| `github_network_events(owner, repo)` | [`GET /networks/{owner}/{repo}/events`](https://docs.github.com/en/rest/activity/events#list-public-events-for-a-network-of-repositories) |

### Branches

| Macro | API Endpoint |
|---|---|
| `github_repo_branches(owner, repo, protected := NULL, per_page := NULL)` | [`GET /repos/{owner}/{repo}/branches`](https://docs.github.com/en/rest/branches/branches#list-branches) |
| `github_repo_branch(owner, repo, branch)` | [`GET /repos/{owner}/{repo}/branches/{branch}`](https://docs.github.com/en/rest/branches/branches#get-a-branch) |
| `github_repo_branch_protection(owner, repo, branch)` | [`GET /repos/{owner}/{repo}/branches/{branch}/protection`](https://docs.github.com/en/rest/branches/branch-protection#get-branch-protection) |
| `github_repo_rules_for_branch(owner, repo, branch)` | [`GET /repos/{owner}/{repo}/rules/branches/{branch}`](https://docs.github.com/en/rest/repos/rules#get-rules-for-a-branch) |

### Commits

| Macro | API Endpoint |
|---|---|
| `github_commits(owner, repo, sha := NULL, path := NULL, author := NULL, committer := NULL, since := NULL, until := NULL)` | [`GET /repos/{owner}/{repo}/commits`](https://docs.github.com/en/rest/commits/commits#list-commits) |
| `github_commit(owner, repo, ref)` | [`GET /repos/{owner}/{repo}/commits/{ref}`](https://docs.github.com/en/rest/commits/commits#get-a-commit) |
| `github_commit_pulls(owner, repo, commit_sha)` | [`GET /repos/{owner}/{repo}/commits/{commit_sha}/pulls`](https://docs.github.com/en/rest/commits/commits#list-pull-requests-associated-with-a-commit) |
| `github_commit_comments(owner, repo, commit_sha)` | [`GET /repos/{owner}/{repo}/commits/{commit_sha}/comments`](https://docs.github.com/en/rest/commits/comments#list-commit-comments) |
| `github_repo_commit_comments(owner, repo)` | [`GET /repos/{owner}/{repo}/comments`](https://docs.github.com/en/rest/commits/comments#list-commit-comments-for-a-repository) |
| `github_commit_status(owner, repo, ref)` | [`GET /repos/{owner}/{repo}/commits/{ref}/status`](https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference) |
| `github_commit_statuses(owner, repo, ref)` | [`GET /repos/{owner}/{repo}/commits/{ref}/statuses`](https://docs.github.com/en/rest/commits/statuses#list-commit-statuses-for-a-reference) |

### Pull Requests

| Macro | API Endpoint |
|---|---|
| `github_repo_pulls(owner, repo, state := NULL, head := NULL, base := NULL, sort := NULL, direction := NULL)` | [`GET /repos/{owner}/{repo}/pulls`](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests) |
| `github_repo_pull(owner, repo, pull_number)` | [`GET /repos/{owner}/{repo}/pulls/{pull_number}`](https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request) |
| `github_repo_pull_commits(owner, repo, pull_number)` | [`GET /repos/{owner}/{repo}/pulls/{pull_number}/commits`](https://docs.github.com/en/rest/pulls/pulls#list-commits-on-a-pull-request) |
| `github_repo_pull_files(owner, repo, pull_number)` | [`GET /repos/{owner}/{repo}/pulls/{pull_number}/files`](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests-files) |
| `github_repo_pull_reviews(owner, repo, pull_number)` | [`GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews`](https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request) |
| `github_repo_pull_review(owner, repo, pull_number, review_id)` | [`GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews/{review_id}`](https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request) |
| `github_repo_pull_review_comments(owner, repo, pull_number)` | [`GET /repos/{owner}/{repo}/pulls/{pull_number}/comments`](https://docs.github.com/en/rest/pulls/comments#list-review-comments-on-a-pull-request) |

### Issues

| Macro | API Endpoint |
|---|---|
| `github_repo_issues(owner, repo, milestone := NULL, state := NULL, assignee := NULL, creator := NULL, mentioned := NULL, labels := NULL, sort := NULL, direction := NULL, since := NULL)` | [`GET /repos/{owner}/{repo}/issues`](https://docs.github.com/en/rest/issues/issues#list-repository-issues) |
| `github_repo_issue(owner, repo, issue_number)` | [`GET /repos/{owner}/{repo}/issues/{issue_number}`](https://docs.github.com/en/rest/issues/issues#get-an-issue) |
| `github_repo_issue_labels(owner, repo, issue_number)` | [`GET /repos/{owner}/{repo}/issues/{issue_number}/labels`](https://docs.github.com/en/rest/issues/labels#list-labels-for-an-issue) |
| `github_repo_issue_timeline(owner, repo, issue_number)` | [`GET /repos/{owner}/{repo}/issues/{issue_number}/timeline`](https://docs.github.com/en/rest/issues/timeline#list-timeline-events-for-an-issue) |
| `github_repo_issue_assignees(owner, repo, issue_number)` | [`GET /repos/{owner}/{repo}/issues/{issue_number}`](https://docs.github.com/en/rest/issues/issues#get-an-issue) |
| `github_repo_issue_comments(owner, repo, issue_number, since := NULL)` | [`GET /repos/{owner}/{repo}/issues/{issue_number}/comments`](https://docs.github.com/en/rest/issues/comments#list-issue-comments) |
| `github_repo_issue_comment(owner, repo, comment_id)` | [`GET /repos/{owner}/{repo}/issues/comments/{comment_id}`](https://docs.github.com/en/rest/issues/comments#get-an-issue-comment) |
| `github_repo_issue_comments(owner, repo, sort := NULL, direction := NULL, since := NULL)` | [`GET /repos/{owner}/{repo}/issues/comments`](https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository) |
| `github_repo_issue_events(owner, repo, issue_number)` | [`GET /repos/{owner}/{repo}/issues/{issue_number}/events`](https://docs.github.com/en/rest/issues/events#list-issue-events) |
| `github_repo_issue_event(owner, repo, event_id)` | [`GET /repos/{owner}/{repo}/issues/events/{event_id}`](https://docs.github.com/en/rest/issues/events#get-an-issue-event) |
| `github_repo_issue_events(owner, repo)` | [`GET /repos/{owner}/{repo}/issues/events`](https://docs.github.com/en/rest/issues/events#list-issue-events-for-a-repository) |

### Labels

| Macro | API Endpoint |
|---|---|
| `github_repo_labels(owner, repo)` | [`GET /repos/{owner}/{repo}/labels`](https://docs.github.com/en/rest/issues/labels#list-labels-for-a-repository) |
| `github_repo_label(owner, repo, name)` | [`GET /repos/{owner}/{repo}/labels/{name}`](https://docs.github.com/en/rest/issues/labels#get-a-label) |

### Releases

| Macro | API Endpoint |
|---|---|
| `github_repo_releases(owner, repo)` | [`GET /repos/{owner}/{repo}/releases`](https://docs.github.com/en/rest/releases/releases#list-releases) |
| `github_repo_release(owner, repo, release_id)` | [`GET /repos/{owner}/{repo}/releases/{release_id}`](https://docs.github.com/en/rest/releases/releases#get-a-release) |
| `github_repo_release_latest(owner, repo)` | [`GET /repos/{owner}/{repo}/releases/latest`](https://docs.github.com/en/rest/releases/releases#get-the-latest-release) |
| `github_repo_release_by_tag(owner, repo, tag)` | [`GET /repos/{owner}/{repo}/releases/tags/{tag}`](https://docs.github.com/en/rest/releases/releases#get-a-release-by-tag-name) |

### Webhooks

| Macro | API Endpoint |
|---|---|
| `github_org_webhooks(org)` | [`GET /orgs/{org}/hooks`](https://docs.github.com/en/rest/orgs/webhooks#list-organization-webhooks) |
| `github_org_webhook(org, hook_id)` | [`GET /orgs/{org}/hooks/{hook_id}`](https://docs.github.com/en/rest/orgs/webhooks#get-an-organization-webhook) |
| `github_org_webhook_config(org, hook_id)` | [`GET /orgs/{org}/hooks/{hook_id}/config`](https://docs.github.com/en/rest/orgs/webhooks#get-a-webhook-configuration-for-an-organization) |
| `github_org_webhook_deliveries(org, hook_id)` | [`GET /orgs/{org}/hooks/{hook_id}/deliveries`](https://docs.github.com/en/rest/orgs/webhooks#list-deliveries-for-an-organization-webhook) |
| `github_org_webhook_delivery(org, hook_id, delivery_id)` | [`GET /orgs/{org}/hooks/{hook_id}/deliveries/{delivery_id}`](https://docs.github.com/en/rest/orgs/webhooks#get-a-webhook-delivery-for-an-organization-webhook) |
| `github_repo_webhooks(owner, repo)` | [`GET /repos/{owner}/{repo}/hooks`](https://docs.github.com/en/rest/repos/webhooks#list-repository-webhooks) |
| `github_repo_webhook(owner, repo, hook_id)` | [`GET /repos/{owner}/{repo}/hooks/{hook_id}`](https://docs.github.com/en/rest/repos/webhooks#get-a-repository-webhook) |
| `github_repo_webhook_config(owner, repo, hook_id)` | [`GET /repos/{owner}/{repo}/hooks/{hook_id}/config`](https://docs.github.com/en/rest/repos/webhooks#get-a-webhook-configuration-for-a-repository) |
| `github_repo_webhook_deliveries(owner, repo, hook_id)` | [`GET /repos/{owner}/{repo}/hooks/{hook_id}/deliveries`](https://docs.github.com/en/rest/repos/webhooks#list-deliveries-for-a-repository-webhook) |
| `github_repo_webhook_delivery(owner, repo, hook_id, delivery_id)` | [`GET /repos/{owner}/{repo}/hooks/{hook_id}/deliveries/{delivery_id}`](https://docs.github.com/en/rest/repos/webhooks#get-a-delivery-for-a-repository-webhook) |

### Rulesets

| Macro | API Endpoint |
|---|---|
| `github_repo_rulesets(owner, repo, includes_parents := NULL)` | [`GET /repos/{owner}/{repo}/rulesets`](https://docs.github.com/en/rest/repos/rules#get-all-repository-rulesets) |
| `github_repo_ruleset(owner, repo, ruleset_id, includes_parents := NULL)` | [`GET /repos/{owner}/{repo}/rulesets/{ruleset_id}`](https://docs.github.com/en/rest/repos/rules#get-a-repository-ruleset) |
| `github_repo_ruleset_history(owner, repo, ruleset_id)` | [`GET /repos/{owner}/{repo}/rulesets/{ruleset_id}/history`](https://docs.github.com/en/rest/repos/rules#list-repository-ruleset-history) |
| `github_repo_ruleset_history_version(owner, repo, ruleset_id, version_id)` | [`GET /repos/{owner}/{repo}/rulesets/{ruleset_id}/history/{version_id}`](https://docs.github.com/en/rest/repos/rules#get-a-repository-ruleset-version) |
| `github_org_rulesets(org, includes_parents := NULL)` | [`GET /orgs/{org}/rulesets`](https://docs.github.com/en/rest/orgs/rules#get-all-organization-rulesets) |
| `github_org_ruleset(org, ruleset_id, includes_parents := NULL)` | [`GET /orgs/{org}/rulesets/{ruleset_id}`](https://docs.github.com/en/rest/orgs/rules#get-an-organization-ruleset) |
| `github_org_ruleset_history(org, ruleset_id)` | [`GET /orgs/{org}/rulesets/{ruleset_id}/history`](https://docs.github.com/en/rest/orgs/rules#list-organization-ruleset-history) |
| `github_org_ruleset_history_version(org, ruleset_id, version_id)` | [`GET /orgs/{org}/rulesets/{ruleset_id}/history/{version_id}`](https://docs.github.com/en/rest/orgs/rules#get-an-organization-ruleset-version) |

### Organizations

| Macro | API Endpoint |
|---|---|
| `github_org(org)` | [`GET /orgs/{org}`](https://docs.github.com/en/rest/orgs/orgs#get-an-organization) |
| `github_org_repos(org, type := NULL, sort := NULL, direction := NULL)` | [`GET /orgs/{org}/repos`](https://docs.github.com/en/rest/repos/repos#list-organization-repositories) |
| `github_org_members(org, filter := NULL, role := NULL)` | [`GET /orgs/{org}/members`](https://docs.github.com/en/rest/orgs/members#list-organization-members) |
| `github_org_public_members(org)` | [`GET /orgs/{org}/public_members`](https://docs.github.com/en/rest/orgs/members#list-public-organization-members) |
| `github_org_invitations(org)` | [`GET /orgs/{org}/invitations`](https://docs.github.com/en/rest/orgs/members#list-pending-organization-invitations) |
| `github_org_failed_invitations(org)` | [`GET /orgs/{org}/failed_invitations`](https://docs.github.com/en/rest/orgs/members#list-failed-organization-invitations) |
| `github_org_outside_collaborators(org, filter := NULL)` | [`GET /orgs/{org}/outside_collaborators`](https://docs.github.com/en/rest/orgs/outside-collaborators#list-outside-collaborators-for-an-organization) |
| `github_org_installations(org)` | [`GET /orgs/{org}/installations`](https://docs.github.com/en/rest/orgs/orgs#list-app-installations-for-an-organization) |
| `github_org_events(org)` | [`GET /orgs/{org}/events`](https://docs.github.com/en/rest/activity/events#list-public-organization-events) |

### Teams

| Macro | API Endpoint |
|---|---|
| `github_teams(org)` | [`GET /orgs/{org}/teams`](https://docs.github.com/en/rest/teams/teams#list-teams) |
| `github_team(org, team_slug)` | [`GET /orgs/{org}/teams/{team_slug}`](https://docs.github.com/en/rest/teams/teams#get-a-team-by-name) |
| `github_team_repos(org, team_slug)` | [`GET /orgs/{org}/teams/{team_slug}/repos`](https://docs.github.com/en/rest/teams/teams#list-team-repositories) |
| `github_team_teams(org, team_slug)` | [`GET /orgs/{org}/teams/{team_slug}/teams`](https://docs.github.com/en/rest/teams/teams#list-child-teams) |
| `github_team_members(org, team_slug, role := NULL)` | [`GET /orgs/{org}/teams/{team_slug}/members`](https://docs.github.com/en/rest/teams/members#list-team-members) |
| `github_team_member(org, team_slug, username)` | [`GET /orgs/{org}/teams/{team_slug}/memberships/{username}`](https://docs.github.com/en/rest/teams/members#get-team-membership-for-a-user) |

### Users

| Macro | API Endpoint |
|---|---|
| `github_user(username)` | [`GET /users/{username}`](https://docs.github.com/en/rest/users/users#get-a-user) |
| `github_user_followers(username)` | [`GET /users/{username}/followers`](https://docs.github.com/en/rest/users/followers#list-followers-of-a-user) |
| `github_user_gpg_keys(username)` | [`GET /users/{username}/gpg_keys`](https://docs.github.com/en/rest/users/gpg-keys#list-gpg-keys-for-a-user) |
| `github_user_ssh_keys(username)` | [`GET /users/{username}/keys`](https://docs.github.com/en/rest/users/keys#list-public-keys-for-a-user) |
| `github_user_ssh_signing_keys(username)` | [`GET /users/{username}/ssh_signing_keys`](https://docs.github.com/en/rest/users/ssh-signing-keys#list-ssh-signing-keys-for-a-user) |
| `github_user_social_accounts(username)` | [`GET /users/{username}/social_accounts`](https://docs.github.com/en/rest/users/social-accounts#list-social-accounts-for-a-user) |
| `github_user_orgs(username)` | [`GET /users/{username}/orgs`](https://docs.github.com/en/rest/orgs/orgs#list-organizations-for-a-user) |
| `github_user_repos(username, type := NULL, sort := NULL, direction := NULL)` | [`GET /users/{username}/repos`](https://docs.github.com/en/rest/repos/repos#list-repositories-for-a-user) |
| `github_user_events(username)` | [`GET /users/{username}/events`](https://docs.github.com/en/rest/activity/events#list-events-for-the-authenticated-user) |
| `github_user_gists(username, since := NULL)` | [`GET /users/{username}/gists`](https://docs.github.com/en/rest/gists/gists#list-gists-for-a-user) |

### Gists

| Macro | API Endpoint |
|---|---|
| `github_gist(gist_id)` | [`GET /gists/{gist_id}`](https://docs.github.com/en/rest/gists/gists#get-a-gist) |
| `github_gist_forks(gist_id)` | [`GET /gists/{gist_id}/forks`](https://docs.github.com/en/rest/gists/gists#list-gist-forks) |
| `github_gist_commits(gist_id)` | [`GET /gists/{gist_id}/commits`](https://docs.github.com/en/rest/gists/gists#list-gist-commits) |
| `github_gist_revision(gist_id, sha)` | [`GET /gists/{gist_id}/{sha}`](https://docs.github.com/en/rest/gists/gists#get-a-gist-revision) |

### Security Advisories

| Macro | API Endpoint |
|---|---|
| `github_security_advisories(ghsa_id := NULL, cve_id := NULL, ecosystem := NULL, severity := NULL, cwes := NULL, is_withdrawn := NULL, affects := NULL, published := NULL, updated := NULL, modified := NULL, type := NULL, direction := NULL, sort := NULL)` | [`GET /advisories`](https://docs.github.com/en/rest/security-advisories/global-advisories#list-global-security-advisories) |
| `github_security_advisory(ghsa_id)` | [`GET /advisories/{ghsa_id}`](https://docs.github.com/en/rest/security-advisories/global-advisories#get-a-global-security-advisory) |
| `github_repo_security_advisories(owner, repo, direction := NULL, sort := NULL, before := NULL, after := NULL, ecosystem := NULL, severity := NULL, cwes := NULL, cve_id := NULL, ghsa_id := NULL, state := NULL)` | [`GET /repos/{owner}/{repo}/security-advisories`](https://docs.github.com/en/rest/security-advisories/repository-advisories#list-repository-security-advisories) |
| `github_repo_security_advisory(owner, repo, ghsa_id)` | [`GET /repos/{owner}/{repo}/security-advisories/{ghsa_id}`](https://docs.github.com/en/rest/security-advisories/repository-advisories#get-a-repository-security-advisory) |

### Secret Scanning

| Macro | API Endpoint |
|---|---|
| `github_org_secret_scanning_alerts(org, state := NULL, secret_type := NULL, resolution := NULL, sort := NULL, direction := NULL)` | [`GET /orgs/{org}/secret-scanning/alerts`](https://docs.github.com/en/rest/secret-scanning/secret-scanning#list-secret-scanning-alerts-for-an-organization) |
| `github_repo_secret_scanning_alerts(owner, repo, state := NULL, secret_type := NULL, resolution := NULL, sort := NULL, direction := NULL)` | [`GET /repos/{owner}/{repo}/secret-scanning/alerts`](https://docs.github.com/en/rest/secret-scanning/secret-scanning#list-secret-scanning-alerts-for-a-repository) |
| `github_repo_secret_scanning_alert(owner, repo, alert_number)` | [`GET /repos/{owner}/{repo}/secret-scanning/alerts/{alert_number}`](https://docs.github.com/en/rest/secret-scanning/secret-scanning#get-a-secret-scanning-alert) |
| `github_repo_secret_scanning_alert_locations(owner, repo, alert_number)` | [`GET /repos/{owner}/{repo}/secret-scanning/alerts/{alert_number}/locations`](https://docs.github.com/en/rest/secret-scanning/secret-scanning#list-locations-for-a-secret-scanning-alert) |
| `github_repo_secret_scanning_scan_history(owner, repo)` | [`GET /repos/{owner}/{repo}/secret-scanning/scans`](https://docs.github.com/en/rest/secret-scanning/secret-scanning#get-secret-scanning-scan-history-for-a-repository) |

### Search

| Macro | API Endpoint |
|---|---|
| `github_search_code(q, sort := NULL, direction := NULL)` | [`GET /search/code`](https://docs.github.com/en/rest/search/search#search-code) |
| `github_search_commits(q, sort := NULL, direction := NULL)` | [`GET /search/commits`](https://docs.github.com/en/rest/search/search#search-commits) |
| `github_search_issues(q, sort := NULL, direction := NULL)` | [`GET /search/issues`](https://docs.github.com/en/rest/search/search#search-issues-and-pull-requests) |
| `github_search_labels(q, repository_id, sort := NULL, direction := NULL)` | [`GET /search/labels`](https://docs.github.com/en/rest/search/search#search-labels) |
| `github_search_repos(q, sort := NULL, direction := NULL)` | [`GET /search/repositories`](https://docs.github.com/en/rest/search/search#search-repositories) |
| `github_search_topics(q)` | [`GET /search/topics`](https://docs.github.com/en/rest/search/search#search-topics) |
| `github_search_users(q, sort := NULL, direction := NULL)` | [`GET /search/users`](https://docs.github.com/en/rest/search/search#search-users) |

### Apps

| Macro | API Endpoint |
|---|---|
| `github_app(app_slug)` | [`GET /apps/{app_slug}`](https://docs.github.com/en/rest/apps/apps#get-an-app) |

### Meta

| Macro | API Endpoint |
|---|---|
| `github_licenses()` | [`GET /licenses`](https://docs.github.com/en/rest/licenses/licenses#get-all-commonly-used-licenses) |
| `github_meta()` | [`GET /meta`](https://docs.github.com/en/rest/meta/meta#get-apiname-meta-information) |
| `github_zen()` | [`GET /zen`](https://docs.github.com/en/rest/meta/meta#get-the-zen-of-github) |
| `github_ratelimit()` | [`GET /rate_limit`](https://docs.github.com/en/rest/rate-limit/rate-limit#get-rate-limit-status-for-the-authenticated-user) |

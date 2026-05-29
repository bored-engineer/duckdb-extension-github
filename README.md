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
| `paginate` | `BOOLEAN` | `true` | Follow `Link` pagination headers |

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
FROM github_rest('/users/bored-engineer/repos?per_page=100');

-- Fetch a single page only, no pagination
SELECT count(*) FROM github_rest(
    '/users/bored-engineer/repos?per_page=30',
    paginate=false
);

-- GitHub Enterprise
SELECT data->>'name'
FROM github_rest('/repos/owner/repo', host='github.mycompany.com');
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
    variables = {'login': 'bored-engineer'}
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
    variables = {'login': 'bored-engineer'}
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
    variables = {'login': 'bored-engineer'},
    paginate = false
);

-- Ignore errors and inspect them manually
SELECT data, errors[1]->>'message' AS error
FROM github_graphql(
    'query { viewer { nonexistentField } }',
    ignore_errors = true
);

-- GitHub Enterprise
SELECT data->'viewer'->>'login'
FROM github_graphql(
    'query { viewer { login } }',
    host = 'github.mycompany.com'
);
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
FROM github_rest('/users/bored-engineer/repos?per_page=100');
```

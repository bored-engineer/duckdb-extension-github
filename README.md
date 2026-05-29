# duckdb-extension-github

A DuckDB extension for querying the GitHub REST and GraphQL APIs directly from SQL.

## Installation

```sql
INSTALL github FROM community;
LOAD github;
```

## Authentication

Tokens are resolved in this order for the default host (`api.github.com`):

1. A DuckDB `http` secret scoped to `https://api.github.com`
2. `GH_TOKEN` environment variable
3. `GITHUB_TOKEN` environment variable

When using a GitHub Enterprise host (via the `host` parameter or `GH_HOST` env var), only enterprise-specific tokens are used — `GH_TOKEN` and `GITHUB_TOKEN` are intentionally ignored:

1. A DuckDB `http` secret scoped to the enterprise host
2. `GH_ENTERPRISE_TOKEN` environment variable
3. `GITHUB_ENTERPRISE_TOKEN` environment variable

**Creating a secret:**
```sql
CREATE SECRET github (
    TYPE http,
    BEARER_TOKEN 'ghp_...',
    SCOPE 'https://api.github.com'
);
```

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
SELECT data->>'$.full_name', data->>'$.stargazers_count'
FROM github_rest('/repos/duckdb/duckdb');

-- List all repos for a user (paginated, one row per repo)
SELECT data->>'$.name', data->>'$.language'
FROM github_rest('/users/bored-engineer/repos?per_page=100');

-- Search issues with a custom header
SELECT data->>'$.title'
FROM github_rest(
    '/search/issues?q=is:open+repo:duckdb/duckdb',
    headers = {'X-Custom-Header': 'value'}
);

-- Single page only, no pagination
SELECT count(*) FROM github_rest(
    '/users/bored-engineer/repos?per_page=100',
    paginate = false
);

-- GitHub Enterprise
SELECT data->>'$.name'
FROM github_rest('/repos/owner/repo', host = 'github.mycompany.com');
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
SELECT data->>'$.viewer.login'
FROM github_graphql('query { viewer { login } }');

-- Paginate through all repositories for a user
SELECT UNNEST(data->>'$.user.repositories.nodes[*].name') AS name
FROM github_graphql(
    'query($login: String!, $endCursor: String) {
        user(login: $login) {
            repositories(first: 100, after: $endCursor) {
                nodes { name stargazerCount }
                pageInfo { hasNextPage endCursor }
            }
        }
    }',
    variables = {'login': 'bored-engineer'}
);

-- Ignore errors and inspect them manually
SELECT data, errors[1]->>'$.message' AS error
FROM github_graphql(
    'query { viewer { nonexistentField } }',
    ignore_errors = true
);

-- GitHub Enterprise
SELECT data->>'$.viewer.login'
FROM github_graphql(
    'query { viewer { login } }',
    host = 'github.mycompany.com'
);
```

---

### `github_rest_type(name)`  /  `github_rest_type(name, list)`

Returns the JSON type schema string for a named GitHub API type, for use with DuckDB's `json_transform()` function. Pass `true` as the second argument to wrap the schema in an array (`[...]`).

**Examples:**

```sql
-- Transform REST response rows into typed structs
SELECT json_transform(data, github_rest_type('repository'))
FROM github_rest('/users/bored-engineer/repos?per_page=100');

-- List form (array response)
SELECT UNNEST(json_transform(data, github_rest_type('repository', true)))
FROM github_rest('/users/bored-engineer/repos?per_page=100', paginate = false);
```

## Rate limits

Every response includes a `ratelimit` struct populated from the `X-RateLimit-*` headers:

```sql
SELECT
    ratelimit.remaining,
    ratelimit.limit,
    ratelimit.reset
FROM github_rest('/meta');
```

## Environment variables

| Variable | Description |
|---|---|
| `GH_HOST` | Default API hostname (overrides `api.github.com`) |
| `GH_TOKEN` | GitHub personal access token |
| `GITHUB_TOKEN` | GitHub personal access token (fallback) |
| `GH_ENTERPRISE_TOKEN` | GitHub Enterprise token (checked first when `GH_HOST` or `host` is set) |
| `GITHUB_ENTERPRISE_TOKEN` | GitHub Enterprise token fallback |

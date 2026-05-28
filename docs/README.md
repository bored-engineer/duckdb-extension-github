# DuckDB GitHub Extension
This repository contains a DuckDB extension for interacting with the [GitHub REST API](https://docs.github.com/en/rest?apiVersion=2022-11-28).

A [GitHub personal access token](https://github.com/settings/tokens) is required. Create an `http` secret scoped to `https://api.github.com` before calling any functions:
```sql
D CREATE SECRET github (
    TYPE http,
    BEARER_TOKEN 'github_pat_...',
    SCOPE 'https://api.github.com'
  );
```
```sql
D SELECT UNNEST(body->>'$.ssh_keys[*]') AS ssh_key FROM github_rest('/meta');
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                          ssh_key                                         │
│                                          varchar                                         │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│ ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl         │
│ ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOm…  │
│ ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUk…  │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```
It will automatically follow the [pagination headers](https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2022-11-28) to fetch multiple pages of results:
```sql
D SELECT url FROM github_rest('/users/bored-engineer/repos?per_page=50');
┌───────────────────────────────────────────────────────────────┐
│                              url                              │
│                            varchar                            │
├───────────────────────────────────────────────────────────────┤
│ https://api.github.com/users/bored-engineer/repos?per_page=50 │
│ https://api.github.com/user/541842/repos?per_page=50&page=2   │
│ https://api.github.com/user/541842/repos?per_page=50&page=3   │
└───────────────────────────────────────────────────────────────┘
```
Use `github_rest_type` and `github_rest_list_type` with [`json_transform`](https://duckdb.org/docs/data/json/json_functions.html#json-transformation) to parse API responses into typed structs with named columns:
```sql
D SELECT r.full_name, r.stargazers_count, r.language
  FROM (
    SELECT UNNEST(json_transform(body, github_rest_list_type('repository'))) AS r
    FROM github_rest('/users/bored-engineer/repos?per_page=100')
  )
  ORDER BY r.stargazers_count DESC LIMIT 5;
┌──────────────────────────────────────────────────┬──────────────────┬─────────────┐
│                    full_name                     │ stargazers_count │  language   │
│                     varchar                      │      int64       │   varchar   │
├──────────────────────────────────────────────────┼──────────────────┼─────────────┤
│ bored-engineer/iOS-sbutils                       │               78 │ Objective-C │
│ bored-engineer/iOS-DataProtection                │               39 │ C           │
│ bored-engineer/bf-lookup                         │               32 │ Go          │
│ bored-engineer/hackeroni-slack-disclosure-bot    │               20 │ Python      │
│ bored-engineer/github-conditional-http-transport │               17 │ Go          │
└──────────────────────────────────────────────────┴──────────────────┴─────────────┘
```

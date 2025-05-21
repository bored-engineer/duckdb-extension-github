# DuckDB GitHub Extension
This repository contains a DuckDB extension for interacting with the [GitHub REST API](https://docs.github.com/en/rest?apiVersion=2022-11-28):
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
D SELECT url FROM github_rest('/user/541842/repos?per_page=50');
┌───────────────────────────────────────────────────────────────┐
│                              url                              │
│                            varchar                            │
├───────────────────────────────────────────────────────────────┤
│ https://api.github.com/users/bored-engineer/repos?per_page=50 │
│ https://api.github.com/user/541842/repos?per_page=50&page=2   │
│ https://api.github.com/user/541842/repos?per_page=50&page=3   │
└───────────────────────────────────────────────────────────────┘
```

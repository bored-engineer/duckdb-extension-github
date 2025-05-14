# DuckDB GitHub Extension
This repository contains a DuckDB extension for interacting with the [GitHub REST API](https://docs.github.com/en/rest?apiVersion=2022-11-28):
```sql
D SELECT github_rest('/meta')->>'ssh_keys' AS keys;
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                        keys                                                        │
│                                                      varchar                                                       │
├────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ ["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl","ecdsa-sha2-nistp256 AAAAE2V…  │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

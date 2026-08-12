# Godis Changelog

## v1.3.4 (2026-08-10)

### Features
- feat: cluster v2 — OSS WAL-based replication, runtime hot-switch, disaster recovery, cluster metrics, HTTP join
  - **Runtime standalone→cluster hot-switch**: turn on `[cluster] enable` while running, godis wraps the live server in a cluster atomically — no data loss, no dropped client connections; auto-generates the `cluster_id` and writes it back to the config file / Nacos
  - **OSS (S3-compatible) WAL sync**: master appends commands to a WAL (same append-only stream as AOF) and uploads gzip-compressed, unencrypted segments to the bucket; replicas pull and replay. Objects are append-only (no DELETE ops → low S3 cost), layout under `{prefix}/{cluster_id}/{wal,state,base}/`
  - **HTTP cluster join**: new `POST /api/cluster/join` and `GET /api/cluster/status`; joining nodes can use `cluster.join_http` + `cluster.join_http_token` instead of the TCP `seed` channel — both transports supported
  - **Failover / disaster recovery**: ≥3 nodes → raft re-elects the master in a background goroutine; <3 nodes → a DR goroutine restores the newest data from OSS state markers and promotes the newest node
  - **Cluster metrics on :9121**: `godis_cluster_enabled/mode/role/master_info/node_count/wal_seq_applied/wal_seq_published/sync_last_success/sync_lag_seconds/wal_upload_ms/wal_download_ms`
  - Node version gap tolerance up to 3 major versions (`cluster_max_version_gap`), heartbeat 15s / 10 failures defaults
- feat: merged remote main history (SSE PubSub, TCP connection pool, Monero mining defense, docs)

## v1.3.3-bugfix (2026-07-24)

### Features
- feat: `--migrate` data migration tool — migrate data from godis to any Redis server
  - `full` subcommand: TUI with source/dest keys side-by-side, progress bar, scroll support
  - `inc` subcommand: continuous polling for new keys, auto-migrate on discovery
  - Supports string, hash, list, set, sorted set data types with TTL preservation
  - Configurable destination host, port, auth, and database index
  - TODO: support arbitrary Redis-to-Redis migration (not just local godis)

### Fixes
- fix: 修复了213个Bug

## v1.3.2 (2026-07-22)

### Features
- feat: HTTP API server with token-based auth (POST /api/auth) and Redis command execution (GET /api/commands)
- feat: X-HEADER-AUTHTOKEN rotating token auth (128-char uppercase, configurable expiry, default 72h)
- feat: comprehensive PRD document (docs/prd.md)
- feat: configurable web dashboard port (WebPort, default 63800, [web] section in TOML)
- feat: port overview in README (TCP 6379, API 63790, Web 63800, Metrics 9121)

## v1.3.1 (2026-07-22)

### Features
- feat: add Helm chart, K8s operator, multi-arch Docker images
- feat(release): build and push multi-arch Docker image to GHCR
- feat: Prometheus metrics with hot/big key monitoring (redis_exporter compatible)
- feat: `--cli` with `--monitor` sub-flag: split-screen TUI (3 monitoring panels + redis-cli)
- feat: `--web` dashboard on :63800 with `-u`, `-h`, `-p`, `-a` flags
- feat: `-u` redis:// URL support in CLI flags
- feat: Entra ID (Azure AD) token-based authentication with JWKS validation

### Fixes
- fix: add riscv64 support to boltdb/bolt via local patch
- fix(release): remove unsupported windows/riscv64 and darwin/riscv64 targets
- fix(release): partial build failure does not block release
- fix(release): upload-checksums job indentation
- fix(release): add packages: write permission for GHCR push
- Fix test.yml: add GOEXPERIMENT=jsonv2 at step level
- fix: suppress spurious "open rdb file failed" error on fresh startup
- fix: error wrapping in loadRdbFile (use %w for errors.Is compatibility)

### Previous Releases

See git log for detailed history.

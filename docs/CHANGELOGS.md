# Godis Changelog

## v1.3.5 (2026-09-14)

### Features

- feat: **vector and hybrid search** — `FT.CREATE` now takes `ON JSON` as well as `ON HASH`, with `AS` aliases and JSON-path fields (`$.features[*]`). Dialect-2 query syntax is parsed properly: pre-filters (`(@category:{Audio})=>[KNN 3 @embedding $vec AS score]`), `@tag:{a|b}`, `@num:[min max]`, `-negation`, `|` union, `SORTBY`, `RETURN` projection, `LIMIT`, `FILTER`, `WITHSCORES`, `NOCONTENT`. Query vectors are accepted as Redis sends them — raw little-endian `FLOAT32` bytes — as well as text.
  - **`FT.HYBRID`** (new): `SEARCH` clause plus `VSIM` clause, fused with Reciprocal Rank Fusion by default, or `COMBINE LINEAR`. `LOAD`, `LIMIT`, `FILTER`, `PARAMS`.
  - `FT.CREATE` backfills documents already in the database, so an index created after the data is immediately queryable.
- feat: **RAG served from the database** — chunk, embed, store and retrieve without a client library, so `redis-cli RAG.QUERY "..."` works directly.
  - `RAG.INGEST collection id text [TITLE t] [SOURCE s] [TAG t ...]` — markdown-aware chunking, embedding, indexing
  - `RAG.QUERY collection text [K n] [TAG t ...] [MINSCORE x] [FORMAT CONTEXT|DETAIL]` — returns an assembled context block with citations
  - `RAG.DEL`, `RAG.DOCS`, `RAG.INFO`
  - Generation intentionally stays client-side: the server returns retrieved context, so no generation API key lives in the database.
- feat: **LLM wiki** — the persistent, interlinked markdown layer an agent maintains, with the bookkeeping done server-side.
  - `WIKI.SET` / `WIKI.GET` / `WIKI.DEL` / `WIKI.LIST`, with `[[Page Name]]` links extracted and indexed automatically
  - `WIKI.LINKS` / `WIKI.BACKLINKS` — the link graph
  - `WIKI.INDEX` (generated `index.md` catalogue) and `WIKI.LOG` / `WIKI.LOG.ADD` (greppable `## [YYYY-MM-DD] kind | title` entries)
  - `WIKI.INGEST` — files an immutable raw source, logs it, and returns a checklist of pages already using related terms
  - `WIKI.SOURCE.ADD` / `GET` / `LIST` — immutable source documents
  - `WIKI.QUERY` — hybrid retrieval (wording + meaning), whole pages
  - `WIKI.LINT` — orphans, broken links, missing summaries, duplicated titles
  - `WIKI.EXPORT` — renders the wiki as front-mattered markdown for git
- feat: **agent memory** — `MEMORY.ADD`, `MEMORY.GET` (recent window, `LIMIT`/`ROLE`), `MEMORY.SEARCH` (older turns by meaning), `MEMORY.CLEAR`, `MEMORY.SESSIONS`, `MEMORY.INFO`
- feat: **semantic answer cache** — `LLMCACHE.SET` / `GET` / `DEL` / `STATS` / `CLEAR`. A reworded prompt hits the stored answer; `TTL` expiry and `THRESHOLD` tuning supported.
- feat: pluggable embedding backend under `[ai]`: a dependency-free pure-Go local embedder (offline, no API key) and any OpenAI-compatible HTTP endpoint (OpenAI, Ollama, vLLM, LM Studio). The schema records the vector dimension and rejects a mismatch rather than mixing incompatible vector spaces.
- feat: Go 1.27 — `go.mod`, CI workflows and Dockerfiles moved from 1.26 to 1.27; `interface{}` replaced with `any` throughout.

### Fixes

- fix: `FT.SEARCH` silently discarded the pre-filter of a KNN query, returning nearest neighbours from the whole index; it now restricts candidates before ranking.
- fix: `PARAMS` only recognised the literal parameter name `query_vec`; any other name produced an empty result. Parameter names are now matched as written, and a missing parameter is an error rather than an empty reply.
- fix: `RETURN` and `WITHSCORES` were parsed and ignored; `FT.SEARCH` now projects the requested fields and uses the RediSearch RESP2 layout.
- fix: `HNSW` in a `VECTOR` schema was accepted but mis-parsed, shifting every following field.
- fix: deleting a key left it searchable. Index cleanup moved into `DB.Remove`, which TTL expiry, `RENAME` and `FLUSH` also route through.
- fix: an AOF rewrite discards every in-memory index; knowledge-base queries now rebuild a missing index from the documents still present.

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

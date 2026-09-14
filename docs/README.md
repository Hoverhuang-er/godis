# Godis v1.3.5

![license](https://img.shields.io/github/license/Hoverhuang-er/godis)
[![Build Status](https://github.com/Hoverhuang-er/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/Hoverhuang-er/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/Hoverhuang-er/godis/badge.svg?branch=master)](https://coveralls.io/github/Hoverhuang-er/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Hoverhuang-er/godis)](https://goreportcard.com/report/github.com/Hoverhuang-er/godis)
[![Go Reference](https://pkg.go.dev/badge/github.com/Hoverhuang-er/godis.svg)](https://pkg.go.dev/github.com/Hoverhuang-er/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[中文版](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_CN.md) | [日本語](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_JA.md) | [Suomi](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_FI.md)

📖 **[Documentation](https://hoverhuang-er.github.io/godis/)**


`Godis` is a golang implementation of Redis Server, which intents to provide an example of writing a high concurrent
middleware using golang.

Key Features:

- Redis 8.8.0 command compatibility
- Support string, list, hash, set, sorted set, bitmap
- HyperLogLog (PFADD, PFCOUNT, PFMERGE)
- Bloom filter (BF.ADD, BF.EXISTS, BF.RESERVE)
- T-Digest (TDIGEST.ADD, TDIGEST.QUANTILE)
- Top-K (TOPK.ADD, TOPK.QUERY, TOPK.LIST)
- Count-min sketch (CMS.INCRBY, CMS.QUERY)
- Geospatial indexes (GEOADD, GEOSEARCH, etc.)
- Bitfield (BITFIELD, BITFIELD_RO)
- RediSearch (FT.CREATE, FT.SEARCH, FT.HYBRID, FT.DROPINDEX, etc.)
- Time Series (TS.CREATE, TS.ADD, TS.GET, TS.RANGE, etc.)
- Redis-Vector (VECTOR fields, KNN and hybrid search, JSON indexes)
- Knowledge base served by the server, queryable from `redis-cli` with no client library:
  - RAG (RAG.INGEST / RAG.QUERY — chunk, embed, retrieve, cite)
  - LLM wiki (WIKI.* — interlinked pages, link graph, generated index and log, lint)
  - Agent memory (MEMORY.ADD / MEMORY.GET / MEMORY.SEARCH)
  - Semantic answer cache (LLMCACHE.GET / LLMCACHE.SET)
- RedisJSON (JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND, JSON.OBJKEYS, etc.)
- Concurrent Core for better performance
- TTL
- Publish/Subscribe
- Streams (XADD, XREAD, XGROUP, etc.)
- GEO
- AOF and AOF Rewrite
- RDB read and write
- Multi Database and `SELECT` command
- Transaction is **Atomic** and Isolated. If any errors are encountered during execution, godis will rollback the executed commands
- Replication
- Server-side Cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster.
- Cluster metadata management based on Raft. Support dynamic expansion, rebalancing and failover.
- `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` command is supported and atomically executed in cluster mode, allow over multi node.
- HTTP API server with token-based auth (POST /api/auth, GET /api/commands, X-HEADER-AUTHTOKEN)
- Web dashboard for browsing and monitoring (launch with `--web` flag, port 63800)
- Data migration tool (`--migrate full` / `--migrate inc`) — migrate data from godis to any Redis server with full and incremental modes
- `MULTI` Commands Transaction is supported within slot in cluster mode

## What's New in v1.3.5

Full-text, vector and hybrid search, plus a knowledge base the server itself serves.

**Search (`FT.*`)**

- `FT.CREATE` takes `ON JSON` as well as `ON HASH`, with `AS` aliases and JSON-path fields such as `$.features[*]`.
- Dialect-2 queries are parsed properly: pre-filters, `@tag:{a|b}`, `@num:[min max]`, negations, unions, `SORTBY`, `RETURN` projection, `LIMIT`, `FILTER`, `WITHSCORES`.
- Query vectors are accepted the way Redis clients send them — raw little-endian `FLOAT32` bytes — as well as as text.
- `FT.HYBRID` is new: a `SEARCH` clause and a `VSIM` clause fused with Reciprocal Rank Fusion, or `COMBINE LINEAR`.
- `FT.CREATE` backfills documents already in the database, so an index created after the data is queryable immediately.

**RAG (`RAG.*`)** — chunking, embedding and retrieval run inside the server, so a knowledge base can be queried with nothing but `redis-cli`:

```
redis> RAG.INGEST kb doc-1 "HNSW builds a layered proximity graph; recall is traded against build time." TITLE "HNSW" SOURCE handbook.md TAG vectors
1
redis> RAG.QUERY kb "how is recall traded against build time?" K 3
# Retrieved context

Query: how is recall traded against build time?

### [1] HNSW
_source: handbook.md · similarity: 0.624 · kb:doc-1 · chunk 0_

HNSW builds a layered proximity graph; recall is traded against build time.
```

Answer generation stays with the caller. The server returns retrieved context with citations, so no generation API key has to live in the database.

**LLM wiki (`WIKI.*`)** — a persistent, interlinked markdown layer an agent maintains, with the bookkeeping done for it: `[[page]]` links become a queryable graph, `WIKI.INDEX` renders the catalogue, `WIKI.LOG` is a greppable history, `WIKI.LINT` finds orphans, broken links and missing summaries, and `WIKI.INGEST` files a raw source and lists the pages it affects.

**Agent memory and caching (`MEMORY.*`, `LLMCACHE.*`)** — `MEMORY.GET` for the recent window, `MEMORY.SEARCH` for relevant older turns, and a semantic answer cache where a reworded prompt hits the answer that was already generated.

**Embedding backend (`[ai]`)** — a dependency-free local embedder (offline, no API key, lexical) by default, or any OpenAI-compatible HTTP endpoint (OpenAI, Ollama, vLLM, LM Studio) for real semantics. The index records the vector dimension and refuses to mix dimensions rather than returning plausible nonsense.

**Go 1.27** — `go.mod`, CI workflows and Dockerfiles moved to Go 1.27, and `interface{}` is replaced by `any` throughout.

See [docs/CHANGELOGS.md](docs/CHANGELOGS.md) for the full list, and the [usage documentation](docs/githubio/index.md) for guides.

If you could read Chinese, you can find more details in [My Blog](https://www.cnblogs.com/Finley/category/1598973.html).

## Release Policy

Godis follows a **tag-only release model**:

- **Only `main` branch** exists permanently. No `release/*` or `fix/*` branches are kept on the remote.
- **All releases are tagged.** Push a semver tag (`v1.2.3`) to trigger the [Release workflow](.github/workflows/release.yml), which builds binaries, Docker images, and publishes Helm charts.
- **Branch protection** is enabled on `main`:
  - Require pull request review (1 approval)
  - Required linear history
  - Enforce admins
  - No force pushes or deletions

```bash
# To cut a release:
git checkout main && git pull
# ... make changes, commit, push to main via PR ...
git tag v1.4.0
git push origin v1.4.0
```

The Release workflow builds for `linux/amd64`, `linux/arm64`, `linux/riscv64`, `windows/amd64`, `windows/arm64`, `darwin/amd64`, `darwin/arm64`.

## Quick Start

### Standalone (Linux / macOS)

```bash
# Download the binary from GitHub Releases
curl -LO https://github.com/Hoverhuang-er/godis/releases/download/v1.3.2/godis_linux_amd64.zip
unzip godis_linux_amd64.zip

# Run with minimal config
CONFIG=config/standalone.toml ./godis

# Or use the default auto-detection (looks for standalone.toml in cwd)
./godis
```

### Standalone (Windows)

```powershell
# Download godis_windows_amd64.zip from Releases, extract
# Run in PowerShell:
$env:CONFIG="config\standalone.toml"
.\godis.exe
```

### Docker Compose (Minimal)

```bash
git clone https://github.com/Hoverhuang-er/godis.git
cd godis
docker compose up -d
redis-cli -p 6379 PING
```

Minimal `docker-compose.yml`:

```yaml
services:
  godis:
    image: ghcr.io/Hoverhuang-er/godis:latest
    ports:
      - "6379:6379"
    volumes:
      - godis-data:/data
      - ./config/standalone.toml:/etc/godis/standalone.toml:ro
    environment:
      - CONFIG=/etc/godis/standalone.toml
    restart: unless-stopped

volumes:
  godis-data:
```

### Docker (Single Container)

```bash
docker run -d --name godis \
  -p 6379:6379 \
  -v godis-data:/data \
  ghcr.io/Hoverhuang-er/godis:latest
```

### Cluster Mode (Multi-Node)

Godis clusters are built with [hashicorp/raft](https://github.com/hashicorp/raft). Two operating modes are used:

- **Master-slave mode (< 3 nodes)**: with fewer than 3 members a raft quorum cannot elect a new leader when a node dies, so nodes follow classic master-slave semantics. Replicas probe their master and promote themselves after the configured number of consecutive failures. Automatic failover is enabled by default.
- **Raft mode (≥ 3 nodes)**: with 3 or more members the raft leader makes all membership, slot-routing and failover decisions.

Health probing is deliberately loose to tolerate high-latency links: nodes probe each other every `cluster_heartbeat_interval` seconds (default **15s**) and a node is considered dead after `cluster_failover_max_failures` consecutive failed probes (default **10**). Both knobs live under the `[cluster]` section.

Node-to-node data replication goes through an **OSS (S3-compatible) bucket** instead of direct port connections (set `cluster_sync_via_oss = true`). The master appends commands to a **WAL (write-ahead log)** — the same append-only stream as AOF — and uploads it to OSS as gzip-compressed, non-encrypted segments; replicas pull and replay those segments. Objects are **append-only** (no DELETE operations, keeping S3 costs low) and nodes tolerate a version gap of up to **3 major versions** (`cluster_max_version_gap`, default 3).

**Hot-switch from standalone to cluster (no data loss, no dropped connections):**

A node can start as a plain standalone server and switch to cluster mode at runtime without losing data or closing client connections:

1. Start standalone: `CONFIG=config/standalone.toml ./godis &`
2. Enable cluster mode by setting `[cluster] enable = true` (and `sync_via_oss = true` with a valid `[oss]` section) in the config file, or by pushing the same change to Nacos. Godis reloads the config, wraps the running server in a cluster, and swaps it in atomically — existing connections keep working, existing data is preserved.
3. The first switched node auto-generates a **cluster_id** and writes it back to the config file (or publishes it to Nacos) under `[cluster] cluster_id`. Later nodes read that id from their own config so joins are validated and all OSS data lands under `{prefix}/{cluster_id}/`.

**Start a 3-node cluster:**

```bash
# node1 — create the cluster (as_seed = true, only for the first node)
CONFIG=config/cluster.toml ./godis &
# node2, node3 — join via any existing node's Redis address
#   seed = "127.0.0.1:6379"
#   raft_listen_address = "0.0.0.0:16667"   # unique per node
#   cluster_id = "<same id as node1>"
CONFIG=config/cluster-node2.toml ./godis &
CONFIG=config/cluster-node3.toml ./godis &
```

**Join over HTTP instead of TCP (both transports are supported):**

A joining node can send its cluster-join handshake through the seed's HTTP API (port `63790`) instead of the seed's Redis port. Every node already exposes `POST /api/cluster/join`, so an operator can also register a node manually:

```bash
# node2 config: join_http = "127.0.0.1:63790" (seed's HTTP API) instead of seed
#   join_http = "127.0.0.1:63790"
#   join_http_token = "<token>"   # only when the seed has a password
CONFIG=config/cluster-node2.toml ./godis &

# or register a node manually (from any machine):
curl -X POST http://127.0.0.1:63790/api/cluster/join \
  -H 'Content-Type: application/json' \
  -d '{"cluster_id":"<id>","redis_addr":"127.0.0.1:6390","raft_addr":"127.0.0.1:16667"}'

# inspect live cluster state:
curl http://127.0.0.1:63790/api/cluster/status
```

The HTTP path performs the exact same raft join as the TCP path — the seed's web server forwards the handshake to its local godis node, so membership, OSS sync and failover behavior are identical.

**Start a 2-node master-slave cluster:**

```bash
# node1 — master (as_seed = true)
CONFIG=config/cluster.toml ./godis &
# node2 — replica of node1
#   seed = "127.0.0.1:6379"
#   master_in_cluster = "127.0.0.1:6379"
CONFIG=config/cluster-node2.toml ./godis &
```

Minimal `[cluster]` / `[oss]` config for cluster mode:

```toml
[cluster]
enable = true
# cluster_id = "..."                 # set on joining nodes (seed writes it back)
# as_seed = true                     # first node only
# seed = "127.0.0.1:6379"            # join address for non-seed nodes
# raft_listen_address = "0.0.0.0:16666"
# master_in_cluster = "127.0.0.1:6379"
sync_via_oss = true

[oss]
enabled = true
endpoint = "127.0.0.1:9000"          # e.g. MinIO endpoint
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
bucket = "godis-cluster"
use_ssl = false
force_path_style = true              # required for MinIO
```

When OSS is unavailable or disabled, nodes fall back to direct TCP replication (legacy behavior).

**Disaster recovery.** A background goroutine watches the OSS state markers (one small object per node, holding the highest WAL seq each node applied). When a master dies in **raft mode (≥ 3 nodes)** the raft leader re-elects a new master. When the cluster shrinks below 3 nodes, survivors pick the node with the newest applied data by comparing marker metadata and restore it as the new master — no manual intervention needed.

**What happens when a node goes down (failover walkthrough):**

- **Raft mode (≥ 3 nodes) — automatic master re-election.** Nodes probe each other every `heartbeat_interval` seconds (default 15s). After `failover_max_failures` consecutive missed probes (default 10, i.e. ~150s) a node is declared dead. The raft leader removes the dead node from the voter set and the surviving members elect a new master: the promoted node starts publishing its own WAL to OSS, every demoted node stops publishing and becomes a replica of the new master. Client connections on the surviving nodes keep working; clients connected to the dead node must reconnect to another node (any node serves the whole dataset).
- **Master-slave mode (< 3 nodes) — OSS disaster recovery.** With fewer than 3 voting members raft cannot elect a leader, so the disaster-recovery goroutine takes over: every 30s each surviving node lists the OSS state markers, sorts them by applied WAL seq (newest data wins, ties broken by marker timestamp), restores the newest segments from OSS into its local database, then promotes itself (or follows the newest node when it is not the self). Because OSS objects are append-only, the dead node's WAL segments remain pullable forever — data published before the crash is never lost.
- **Restarting a dead node.** Bring it back with the same config it joined with (`cluster_id`, `seed`/`join_http`, unique `raft_listen_address`). On startup the node finds it already has raft state on disk, reads the current master from the raft FSM and becomes its replica, catching up via OSS — no manual re-join needed.
- **The node with the newest data wins.** All failover paths (raft re-election and DR promotion) compare OSS state markers, so the node holding the highest applied WAL seq — i.e. the newest committed data — becomes the new master. There is no split-brain: in raft mode quorum rules apply, below quorum the markers act as the single source of truth.

**Scaling the cluster:**

- **Scaling up (add a node).** Start a new node with the shared `cluster_id` plus either `seed = "<any existing node's redis address>"` (TCP join) or `join_http = "<any node's http api address>"` (HTTP join). The new node is added to the raft configuration as a voter. At ≥ 3 nodes the cluster runs raft mode and the leader balances slot ownership across members; below 3 nodes the new node joins as a master-slave replica. The join is dynamic — existing nodes do not need to restart.
- **Scaling down (remove a node).** v1.3.4 keeps the internal raft eviction primitive (`HandleEvict` / `RemoveServer`) but does **not** expose a user-facing remove command or HTTP endpoint yet. To shrink a cluster today, stop the node's godis process; the remaining members detect it via the health probe and (in raft mode) re-elect/keep the master automatically. Its OSS WAL segments are retained (append-only design), so bringing it back later re-syncs cleanly. A dedicated `cluster.evict` command / `DELETE /api/cluster/node` endpoint is planned for a future release.
- **Object layout in OSS.** All cluster data lives under `{prefix}/{cluster_id}/` in the bucket: `wal/{nodeAddr}/` holds each node's append-only gzip-compressed WAL segments, `state/` holds the per-node state markers used by failover/DR, `base/{nodeAddr}/` holds optional base snapshots. Nothing is ever deleted, keeping S3 costs low.

**Cluster metrics on `:9121`.** With `prometheus_enabled = true`, the `/metrics` endpoint exposes `godis_cluster_enabled`, `godis_cluster_mode` (`raft`/`master-slave`), `godis_cluster_role` (`master`/`slave`), `godis_cluster_master_info{master_id, master_addr}`, `godis_cluster_node_count`, `godis_cluster_wal_seq_applied` / `godis_cluster_wal_seq_published`, `godis_cluster_sync_last_success` / `godis_cluster_sync_lag_seconds` and WAL upload/download durations — enough to pinpoint the master and measure replication lag.

Connect to any node to access the full dataset:

```bash
redis-cli -p 6379
```

### Data Migration (`--migrate`)

Godis provides a built-in data migration tool to migrate data from godis to any Redis-compatible server.

```bash
# Full migration: scan all keys, show source/dest side-by-side in TUI
godis --migrate full -dh 192.168.1.100 -dp 6379 -da destpassword -ddb 0

# Incremental migration: watch for new keys and migrate them
godis --migrate inc -dh 192.168.1.100 -dp 6379 -da destpassword

# Optional: source auth (if local godis has requirepass set)
godis --migrate full -dh 192.168.1.100 -dp 6379 -sa srcpassword
```

**Flags:**
| Flag | Description | Default |
|------|-------------|---------|
| `-dh` | Destination host | `127.0.0.1` |
| `-dp` | Destination port | `6379` |
| `-da` | Destination auth password | (none) |
| `-ddb` | Destination database index | `0` |
| `-sa` | Source (godis) auth password | (none) |

**Full mode** shows a TUI with source keys on the left, destination keys on the right, progress bar, and scrollable key list. Press `q` to quit.

**Inc mode** continuously polls the source for new keys every 2 seconds and migrates them to the destination. Press `q` to stop.

> TODO: support Redis-to-Redis migration (arbitrary source, not just local godis).

### Prometheus Monitoring

Godis exposes Prometheus-compatible metrics at `/metrics` on port `9121` (configurable via `monitoring.prometheus_port` in `standalone.toml`). Metrics are **enabled by default** and follow `redis_exporter` naming conventions for compatibility with existing Redis dashboards.

```bash
# Scrape endpoint (default)
curl http://localhost:9121/metrics
```

**Key metrics exposed:**
- `godis_connected_clients` — current active connections
- `godis_commands_total` — total commands processed
- `godis_keyspace_hits_total` / `godis_keyspace_misses_total` — cache hit/miss counters
- `godis_db_keys` — per-database key counts
- `godis_db_avg_ttl_seconds` — average TTL per database
- `godis_slowlog_length` — slow log queue length
- Hot key and big key detection (periodically sampled)

To disable metrics, set `prometheus_enabled = false` in the `[monitoring]` section of your config. All monitoring config changes are hot-reloaded at runtime.

```toml
[monitoring]
prometheus_enabled = true
prometheus_port = 9121
```

### HTTP API Server

Godis provides a lightweight HTTP API server (default port `63790`) for programmatic access. It acts as a bridge between HTTP clients and the godis Redis-compatible engine — each API call is translated into a Redis RESP command and executed against the local godis instance.

> **Source**: [`internal/web/api.go`](internal/web/api.go) — the complete HTTP API implementation with auth, command dispatch, and RESP reply formatting.

#### Configuration

Enable the HTTP API in `standalone.toml`:

```toml
[http_api]
enabled = true
host = "127.0.0.1"   # use 0.0.0.0 for external access
port = 63790
```

#### API Endpoints

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| `POST` | `/api/auth` | Authenticate and get a bearer token | No |
| `GET`  | `/api/commands` | Execute a Redis command | Yes (unless `requirepass` is empty) |

> **Auth bypass**: If `requirepass` is not set in godis config, the `/api/commands` endpoint allows all requests without token authentication.

#### 1. Authentication — `POST /api/auth`

Exchange the godis `requirepass` for a 128-character bearer token. Password validation is done against the godis server's configured `requirepass`.

**Request:**

```json
{
  "password": "your-redis-password",
  "expired": 72
}
```

| Field | Type | Description |
|-------|------|-------------|
| `password` | string | **Required.** Godis `requirepass` (same password used with `redis-cli AUTH`) |
| `expired` | integer | Token lifetime in hours. `0` = permanent (never expires). Default: `72` |

**Response (`200`):**

```json
{
  "token": "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ...",
  "expires_at": "2026-07-27T05:58:19Z",
  "permanent": false
}
```

**cURL example:**

```bash
curl -X POST http://127.0.0.1:63790/api/auth \
  -H "Content-Type: application/json" \
  -d '{"password":"your-redis-password","expired":72}'
```

#### 2. Execute Commands — `GET /api/commands`

Execute any Redis command via query parameters. Parameters are mapped to Redis command arguments and forwarded to the local godis instance.

**Auth:** Pass the token in the `X-HEADER-AUTHTOKEN` header (no "Bearer" prefix).

**Query parameters:**

| Parameter | Required | Redis Role | Example |
|-----------|----------|-----------|---------|
| `type` | ✅ | Command name (lowercase) | `set`, `hget`, `lpush`, `zadd`, `json.set` |
| `key` | varies | Redis key | `mykey`, `user:100` |
| `value` | varies | Value / secondary argument | `bar`, `100` |
| `field` | varies | Hash field name | `name`, `email` |
| `member` | varies | Set / SortedSet member | `member1`, `user:50` |
| `score` | varies | SortedSet score | `1.0`, `100` |
| `args` | no | Extra space-separated arguments | `NX EX 3600` |
| `db` | no | Database index (`0`–`15`) | `0` |

**Response:**

```json
{
  "success": true,
  "result": "bar",
  "raw": "$3\r\nbar\r\n"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `success` | boolean | `true` = command succeeded, `false` = Redis returned an error |
| `result` | string (nullable) | Human-readable Redis reply (OK, value, numbered list, error msg) |
| `raw` | string (nullable) | Raw RESP protocol bytes (same as `redis-cli` would receive) |
| `error` | string (nullable) | Redis error message (present when `success` is `false`) |

**Examples:**

```bash
# String: SET and GET
curl "http://127.0.0.1:63790/api/commands?type=set&key=hello&value=world" \
  -H "X-HEADER-AUTHTOKEN: <token>"
# → {"success":true,"result":"OK","raw":"+OK\r\n"}

curl "http://127.0.0.1:63790/api/commands?type=get&key=hello" \
  -H "X-HEADER-AUTHTOKEN: <token>"
# → {"success":true,"result":"world","raw":"$5\r\nworld\r\n"}

# Hash: HSET and HGET
curl "http://127.0.0.1:63790/api/commands?type=hset&key=user:1&field=name&value=Alice" \
  -H "X-HEADER-AUTHTOKEN: <token>"

curl "http://127.0.0.1:63790/api/commands?type=hget&key=user:1&field=name" \
  -H "X-HEADER-AUTHTOKEN: <token>"

# List: LPUSH and LRANGE
curl "http://127.0.0.1:63790/api/commands?type=lpush&key=tasks&value=task1" \
  -H "X-HEADER-AUTHTOKEN: <token>"

curl "http://127.0.0.1:63790/api/commands?type=lrange&key=tasks&value=0&args=-1" \
  -H "X-HEADER-AUTHTOKEN: <token>"

# Sorted Set: ZADD and ZRANGE
curl "http://127.0.0.1:63790/api/commands?type=zadd&key=leaderboard&score=100&member=player1" \
  -H "X-HEADER-AUTHTOKEN: <token>"

curl "http://127.0.0.1:63790/api/commands?type=zrange&key=leaderboard&value=0&args=-1" \
  -H "X-HEADER-AUTHTOKEN: <token>"

# Set: SADD and SMEMBERS
curl "http://127.0.0.1:63790/api/commands?type=sadd&key=tags&member=golang" \
  -H "X-HEADER-AUTHTOKEN: <token>"

curl "http://127.0.0.1:63790/api/commands?type=smembers&key=tags" \
  -H "X-HEADER-AUTHTOKEN: <token>"

# With expiry: SET NX EX
curl "http://127.0.0.1:63790/api/commands?type=set&key=cache&value=1&args=NX%20EX%203600" \
  -H "X-HEADER-AUTHTOKEN: <token>"

# Server commands
curl "http://127.0.0.1:63790/api/commands?type=ping" \
  -H "X-HEADER-AUTHTOKEN: <token>"

curl "http://127.0.0.1:63790/api/commands?type=dbsize" \
  -H "X-HEADER-AUTHTOKEN: <token>"

# Module commands (pass-through)
curl "http://127.0.0.1:63790/api/commands?type=json.set&key=doc&value=%24&args='{\"name\":\"test\"}'" \
  -H "X-HEADER-AUTHTOKEN: <token>"

curl "http://127.0.0.1:63790/api/commands?type=bf.add&key=bloom&member=item1" \
  -H "X-HEADER-AUTHTOKEN: <token>"
```

#### OpenAPI / Apifox

OpenAPI v3 specification and Apifox project export are available in the `docs/` directory.
The specifications are generated from the `@internal/web/api.go` source code.

| File | Description |
|------|-------------|
| [`docs/openapi.yaml`](docs/openapi.yaml) | OpenAPI 3.0.3 specification — import into Apifox, Postman, or any OpenAPI-compatible tool |
| [`docs/apifox.json`](docs/apifox.json) | Apifox project export with pre-configured environment, auth variables, and 10+ example requests |

**API Source Files:**

| Source | Description |
|--------|-------------|
| [`internal/web/api.go`](internal/web/api.go) | HTTP API server: `POST /api/auth`, `GET /api/commands`, auth middleware, command dispatch, RESP reply formatting |
| [`internal/web/token.go`](internal/web/token.go) | Token engine: password validation, 128-char token generation, expiry management, cleanup goroutine |
| [`internal/web/dashboard.go`](internal/web/dashboard.go) | Web dashboard: embed HTML template, key access tracking, hot/big key detection, ops monitoring |

**Import into Apifox:**

1. Open Apifox → **File** → **Import** → **OpenAPI / Swagger**
2. Select `docs/openapi.yaml` or `docs/apifox.json`
3. The environment `Local Godis` is pre-configured with `{{token}}` variable
4. Run the "01 - 获取 Token" example to auto-fill the token, then execute any command example

#### Supported Commands

The HTTP API supports all Redis commands via the query parameter mapping system:

| Category | Commands |
|----------|----------|
| **Strings** | `get`, `set`, `setnx`, `setex`, `psetex`, `append`, `getset`, `getdel`, `incr`, `incrby`, `incrbyfloat`, `decr`, `decrby`, `strlen` |
| **Hashes** | `hget`, `hset`, `hsetnx`, `hdel`, `hexists`, `hlen`, `hstrlen`, `hkeys`, `hvals`, `hgetall`, `hmget`, `hincrby`, `hincrbyfloat` |
| **Lists** | `lpush`, `rpush`, `lpushx`, `rpushx`, `lpop`, `rpop`, `llen`, `lrange`, `lindex`, `lrem` |
| **Sets** | `sadd`, `srem`, `sismember`, `smove`, `spop`, `scard`, `smembers`, `srandmember` |
| **Sorted Sets** | `zadd`, `zrem`, `zscore`, `zrank`, `zrevrank`, `zcard`, `zcount`, `zrange`, `zrevrange`, `zincrby` |
| **Keys** | `del`, `exists`, `expire`, `expireat`, `pexpire`, `pexpireat`, `ttl`, `pttl`, `persist`, `type`, `rename`, `renamenx`, `copy` |
| **Server** | `ping`, `dbsize`, `flushdb`, `flushall`, `save`, `bgsave`, `info`, `select`, `keys`, `config` |
| **Modules** (passthrough) | `json.set`, `json.get`, `bf.add`, `bf.exists`, `topk.add`, `ts.add`, `ft.search`, and any other Redis module command |

Any unrecognized command type is passed through as-is with positional arguments — this means module commands and future commands work without a server update.

### Port Overview

| Service | Default Port | Config Section | Config Key |
|---------|-------------|----------------|------------|
| Redis TCP | `6379` | `[server]` | `port` |
| HTTP API | `63790` | `[http_api]` | `port` |
| Web Dashboard | `63800` | `[web]` | `port` |
| Prometheus Metrics | `9121` | `[monitoring]` | `prometheus_port` |

### Anti-Monero Mining Protection

Godis includes built-in defenses against Monero cryptojacking attacks, which are
the most common Redis security threat in production. These protections are
**enabled by default** and require no configuration.

#### Threat Model

Monero miners typically exploit Redis through two vectors:

1. **SLAVEOF/REPLICAOF replication takeover** — attacker makes your Redis a
   replica of their malicious instance, injecting crontab entries or mining
   payloads via the replication stream
2. **CONFIG SET + BGSAVE crontab injection** — attacker sets `dbfilename` to
   `/var/spool/cron/crontabs/root` and triggers a save to achieve RCE

#### Built-in Defenses

| Defense | Mechanism | Default |
|---------|-----------|---------|
| **SLAVEOF blocking (standalone)** | `SLAVEOF <host> <port>` rejected in standalone mode. `SLAVEOF NO ONE` still works. Cluster mode allows it (required for replication). | ✅ On |
| **Crontab path detection** | AOF/RDB filenames containing `/var/spool/cron`, `/etc/cron`, `/tmp/`, or `crontab` are silently reset to safe defaults | ✅ On |
| **Command renaming** | `rename-command` support to disable or rename dangerous commands. Defaults rename `FLUSHALL`/`FLUSHDB` to random strings and disable `SLAVEOF`/`REPLICAOF` in standalone mode | ✅ On |
| **Protected commands** | `CONFIG SET` is not implemented in Godis, eliminating the primary crontab-injection vector | ✅ Inherent |

#### Configuration

The `rename-command` mechanism works identically to Redis:

```toml
[security.rename_command]
# Disable a command entirely (set to empty string)
SLAVEOF = ""

# Rename a command (use the new name to invoke it)
FLUSHALL = "b840fc02d52404542994ccf15b82f2c0"
```

To allow `SLAVEOF` in standalone mode (e.g., for manual replication setup),
remove it from `[security.rename_command]` or set it to a new name.

> **Note**: Cluster mode (`cluster.toml`) does **not** disable `SLAVEOF` by
> default, as replication is required for cluster operation. The anti-mining
> SLAVEOF check in `execSlaveOf()` is only active in standalone mode.

## Kubernetes Deployment

### Helm Chart (Recommended)

For production deployments, use the Helm chart:

```bash
# Add repository and install
helm pull oci://ghcr.io/Hoverhuang-er/godis/charts/godis --version 1.3.2
helm install godis ./godis-1.3.2.tgz

# Or install directly
helm install godis oci://ghcr.io/Hoverhuang-er/godis/charts/godis --version 1.3.2

# Cluster mode (3 nodes)
helm install godis-cluster oci://ghcr.io/Hoverhuang-er/godis/charts/godis --version 1.3.2 \
```

See [charts/godis/values.yaml](https://github.com/Hoverhuang-er/godis/blob/main/charts/godis/values.yaml) for all configuration options.

### Kubernetes Operator

The Godis Operator manages GodisCluster custom resources. Deploy with:

```bash
# Install CRD
kubectl apply -f https://raw.githubusercontent.com/Hoverhuang-er/godis/main/config/crd/godisclusters.yaml

# Deploy operator (default: 3 nodes, 0.5 CPU / 1Gi memory per node)
kubectl create deployment godis-operator --image=ghcr.io/Hoverhuang-er/godis/operator:1.3.2

# Create a Godis cluster (standalone)
kubectl apply -f - <<EOF
apiVersion: godis.Hoverhuang-er.io/v1
kind: GodisCluster
metadata:
  name: my-godis
spec:
  mode: standalone
  port: 6379
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
EOF
```

#### Autoscaling

The operator supports HPA, VPA, and KEDA:

```yaml
apiVersion: godis.Hoverhuang-er.io/v1
kind: GodisCluster
metadata:
  name: my-godis-cluster
spec:
  mode: cluster
  replicas: 3
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80
    enableVPA: true
    enableKEDA: true
```

Supported Kubernetes versions: **1.34–1.36** and **k3s** (any supported version).

## Rueidis Client Example

[Rueidis](https://github.com/redis/rueidis) is a high-performance Redis client for Go. Here's how to use it with Godis:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/rueidis"
)

func main() {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{"localhost:6379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// SET/GET example
	err = client.Do(ctx, client.B().Set().Key("foo").Value("bar").Build()).Error()
	if err != nil {
		log.Fatal(err)
	}

	val, err := client.Do(ctx, client.B().Get().Key("foo").Build()).ToString()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GET foo = %s\n", val)

	// RediSearch example
	// Requires FT.CREATE index first
	result, err := client.Do(ctx, client.B().FtSearch().Index("idx").Query("@field:val").Build()).ToArray()
	if err != nil {
		log.Printf("Search note: %v (create index with FT.CREATE first)", err)
	}
	_ = result

	// Time Series example
	err = client.Do(ctx, client.B().TsAdd().Key("ts:temp").Timestamp(1).Value(25.5).Build()).Error()
	if err != nil {
		log.Printf("Time series note: %v", err)
	}
}
```

## Go-Redis Client Example

[go-redis](https://github.com/redis/go-redis) is the official Redis client for Go. Godis is compatible with `go-redis v9` targeting Redis 8.8.0.

### GET / SET

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()

	// SET / GET
	err := rdb.Set(ctx, "foo", "bar", 0).Err()
	if err != nil {
		log.Fatal(err)
	}

	val, err := rdb.Get(ctx, "foo").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GET foo = %s\n", val)

	// Work with RedisJSON
	err = rdb.JSONSet(ctx, "user:1", "$", `{"name":"Alice","age":30}`).Err()
	if err != nil {
		log.Printf("JSON note: %v (RedisJSON module required)", err)
	}

	userJSON, err := rdb.JSONGet(ctx, "user:1", "$.name").Result()
	if err == nil {
		fmt.Printf("JSON user:1 $.name = %s\n", userJSON)
	}
}
```

### Pub/Sub

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()

	// Subscribe
	pubsub := rdb.Subscribe(ctx, "mychannel")
	defer pubsub.Close()

	// Wait for subscription confirmation
	_, err := pubsub.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Publish in a separate goroutine
	go func() {
		err := rdb.Publish(ctx, "mychannel", "hello from godis").Err()
		if err != nil {
			log.Printf("publish error: %v", err)
		}
	}()

	// Receive message
	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Received: %s = %s\n", msg.Channel, msg.Payload)
}
```

## redis-py Client Example

[redis-py](https://github.com/redis/redis-py) is the official Redis client for Python. Godis is compatible with `redis-py >= 5.0`.

### GET / SET

```python
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# SET / GET
r.set("foo", "bar")
val = r.get("foo")
print(f"GET foo = {val}")  # bar

# RedisJSON
try:
    r.json().set("user:1", "$", {"name": "Alice", "age": 30})
    name = r.json().get("user:1", "$.name")
    print(f"JSON user:1 $.name = {name}")
except Exception as e:
    print(f"JSON note: {e} (RedisJSON module required)")
```

### Pub/Sub

```python
import redis
import threading
import time

r = redis.Redis(host="localhost", port=6379, decode_responses=True)


def subscriber():
    pubsub = r.pubsub()
    pubsub.subscribe("mychannel")
    for msg in pubsub.listen():
        if msg["type"] == "message":
            print(f'Received: {msg["channel"]} = {msg["data"]}')
            break


t = threading.Thread(target=subscriber, daemon=True)
t.start()

time.sleep(0.1)  # allow subscription to register
r.publish("mychannel", "hello from godis")
t.join(timeout=2)
```

## HTTP API

> **Full documentation**: See [HTTP API Server](#http-api-server) above for complete endpoint details, auth flow, parameter mappings, response format, and cURL examples.

### API Specification Downloads

OpenAPI v3 and Apifox project files are available for download from the `docs/` directory.
These specifications cover the HTTP API Server (`POST /api/auth`, `GET /api/commands`) and
the Web Dashboard API (`POST /api/query`, `GET /api/monitor`, `GET /api/stats`).

| File | Description |
|------|-------------|
| [`docs/openapi.yaml`](docs/openapi.yaml) | OpenAPI 3.0.3 — import into Apifox, Postman, Swagger UI, or any OpenAPI-compatible tool |
| [`docs/apifox.json`](docs/apifox.json) | Apifox project export with pre-configured environments, assertions, and example requests |

#### Import into Apifox

1. Apifox → **Settings** → **Import Data** → **OpenAPI / Swagger**
2. Select `docs/openapi.yaml` or `docs/apifox.json`
3. Environment `Local Godis` is pre-configured with `{{token}}` variable
4. Run the auth example to auto-fill the token, then execute any API example
## Web Dashboard

Godis includes a built-in web dashboard for browsing and monitoring Redis data. Launch it using the `--web` flag. The dashboard connects to a running godis instance via the standard Redis TCP port.

```bash
# Start the web dashboard (connects to localhost:6379 by default)
./godis --web

# Connect to a remote godis instance
./godis --web -h 192.168.1.100 -p 6379 -a yourpassword
```

The dashboard runs on port **63800** by default (configurable via `[web]` config section):

```toml
[web]
# Web dashboard listen port
port = 63800
```

Open [http://localhost:63800](http://localhost:63800) in your browser to access:
- **Dashboard** — real-time ops/sec, active connections, total commands
- **Query** — execute any Redis command interactively
- **Hot Keys** — most frequently accessed keys
- **Big Keys** — keys with the largest memory/size footprint
- **Monitor** — live command stream

### CLI flags for `--web`

| Flag | Default | Description |
|------|---------|-------------|
| `-h` | `127.0.0.1` | Godis host to connect to |
| `-p` | `6379` | Godis TCP port |
| `-a` | (none) | Auth password (requirepass) |
| `-u` | (none) | Redis URL (`redis://user:pass@host:port`) |

### Dashboard API

The dashboard exposes a REST API used by the frontend. These endpoints run on the
`[web]` port (default **63800**) and require no authentication:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/query` | Execute a read-only Redis command |
| `GET`  | `/api/monitor` | Get hot keys and big keys monitoring data |
| `GET`  | `/api/stats` | Get server runtime statistics |

**Source**: [`internal/web/dashboard.go`](internal/web/dashboard.go)

#### POST /api/query

Execute a read-only Redis command against the connected godis instance.
Write commands (`SET`, `DEL`, etc.) are rejected for safety.

**Request** (form-encoded):

| Field | Type | Description |
|-------|------|-------------|
| `cmd` | string | **Required.** Full Redis command line (e.g. `GET mykey`, `HGETALL user:1`) |

**Response** (`200`):

```json
{
  "success": true,
  "result": "\"bar\"",
  "error": ""
}
```

**Allowed read-only commands**: `GET`, `MGET`, `STRLEN`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `KEYS`, `SCAN`,
`HGET`, `HGETALL`, `HMGET`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HSTRLEN`,
`LRANGE`, `LINDEX`, `LLEN`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SDIFF`, `SINTER`, `SUNION`,
`ZRANGE`, `ZREVRANGE`, `ZRANK`, `ZREVRANK`, `ZSCORE`, `ZCARD`, `ZCOUNT`, `ZRANGEBYSCORE`,
`XRANGE`, `XLEN`, `XREAD`, `GEODIST`, `GEOHASH`, `GEOPOS`,
`INFO`, `PING`, `DBSIZE`, `TIME`,
`FT.SEARCH`, `FT.HYBRID`, `FT.INFO`, `FT._LIST`, `TS.GET`, `TS.INFO`, `TS.RANGE`,
`JSON.GET`, `JSON.TYPE`, `JSON.STRLEN`, `JSON.OBJLEN`, `JSON.OBJKEYS`.

#### GET /api/monitor

Returns current hot key access counts and big key observations.

**Response** (`200`):

```json
{
  "hot_keys": [
    {"key": "user:1001", "count": 1523},
    {"key": "cache:session", "count": 891}
  ],
  "big_keys": [
    {"key": "logs:2026-07", "db": 0, "type": "string", "size": 1, "bytes": 5242880}
  ],
  "db_stats": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `hot_keys` | array | Top 20 most frequently accessed keys (key name + access count) |
| `big_keys` | array | Recently detected large keys (key name, DB index, type, element count, byte size) |
| `db_stats` | null | Reserved for future per-database statistics |

#### GET /api/stats

Returns server runtime statistics and resource usage.

**Response** (`200`):

```json
{
  "uptime": 86400,
  "goroutines": 34,
  "memory": 16777216,
  "connections": 12,
  "commands": 150234,
  "ops_per_sec": 1523
}
```

| Field | Type | Description |
|-------|------|-------------|
| `uptime` | integer | Server uptime in seconds |
| `goroutines` | integer | Number of active goroutines |
| `memory` | integer | Allocated heap memory in bytes |
| `connections` | integer | Active client connections |
| `commands` | integer | Total commands processed |
| `ops_per_sec` | integer | Commands per second (rolling average) |

## Supported Commands

See: [commands.md](https://github.com/Hoverhuang-er/godis/blob/master/commands.md)

## Benchmark

Environment: **Go 1.26.5** (GreenTea GC), macOS 15.7.1, Apple M5 Air, plus a cluster built with 2 DGX Spark nodes

```mermaid
xychart-beta
    title "Godis redis-benchmark — Requests/sec"
    x-axis ["PING", "SET", "GET", "INCR", "LPUSH", "RPUSH", "LPOP", "RPOP", "SADD", "HSET", "SPOP", "ZADD", "ZPOPMIN", "MSET10", "LR100", "LR300", "LR500", "LR600"]
    y-axis "Ops/sec" 0 --> 200000
    bar [179211, 158479, 156986, 164474, 151286, 176678, 177620, 172414, 159490, 175131, 170648, 165289, 185529, 88417, 46512, 21218, 13332, 11153]
```

| Command | Ops/sec | p50 |
|---|---|---|
| PING_INLINE | 179,211 | 1.03 ms |
| PING_MBULK | 173,611 | 1.07 ms |
| SET | 158,479 | 1.54 ms |
| GET | 156,986 | 1.13 ms |
| INCR | 164,474 | 1.06 ms |
| LPUSH | 151,286 | 1.08 ms |
| RPUSH | 176,678 | 1.02 ms |
| LPOP | 177,620 | 1.04 ms |
| RPOP | 172,414 | 1.04 ms |
| SADD | 159,490 | 1.05 ms |
| HSET | 175,131 | 1.03 ms |
| SPOP | 170,648 | 1.03 ms |
| ZADD | 165,289 | 1.04 ms |
| ZPOPMIN | 185,529 | 1.00 ms |
| MSET (10 keys) | 88,417 | 3.69 ms |
| LRANGE_100 | 46,512 | 4.06 ms |
| LRANGE_300 | 21,218 | 9.31 ms |
| LRANGE_500 | 13,332 | 14.41 ms |
| LRANGE_600 | 11,153 | 17.01 ms |

## Read My Code

The project follows the [Go Project Layout](https://github.com/golang-standards/project-layout) standard:

```
godis/
├── cmd/                          # Entry points
│   ├── godis/main.go             # Godis server — standalone or cluster
│   ├── godis/cli.go              # Built-in redis-cli (--cli flag)
│   └── operator/main.go          # Kubernetes operator controller
├── internal/                     # Private application code
│   ├── config/                   # TOML config (viper, hot-reload, embedded default)
│   ├── tcp/                      # TCP server — connection accept, goroutine-per-conn
│   ├── redis/                    # Redis wire protocol
│   │   ├── parser/               # RESP2/RESP3 parser (streaming, zero-copy)
│   │   ├── protocol/             # Reply types (Bulk, MultiBulk, Error, Integer, etc.)
│   │   ├── server/               # Server adapters
│   │   │   ├── std/              #   Standard net.TCP listener
│   │   │   └── gnet/             #   gnet event-loop (higher throughput)
│   │   ├── client/               # Client for inter-node relay in cluster mode
│   │   └── connection/           # Per-connection state (DB index, auth, multi)
│   ├── interface/                # Core interfaces (DB engine, connection, reply)
│   ├── database/                 # Storage engine & command handlers
│   │   ├── server.go             # Multi-database server (AOF, replication, slowlog)
│   │   ├── database.go           # Single database core (data access, TTL, locks)
│   │   ├── router.go             # Command table — register + route
│   │   ├── string.go             # GET, SET, INCR, APPEND, GETBIT, etc.
│   │   ├── hash.go               # HSET, HGET, HDEL, HGETALL, etc.
│   │   ├── list.go               # LPUSH, LRANGE, LINDEX, LTRIM, etc.
│   │   ├── set.go                # SADD, SMEMBERS, SINTER, SUNION, etc.
│   │   ├── sortedset.go          # ZADD, ZRANGE, ZRANK, ZSCORE, etc.
│   │   ├── stream.go             # XADD, XREAD, XGROUP, XACK, etc.
│   │   ├── geo.go                # GEOADD, GEOSEARCH, GEODIST, etc.
│   │   ├── keys.go               # DEL, EXISTS, EXPIRE, TTL, TYPE, etc.
│   │   ├── transaction.go        # MULTI, EXEC, WATCH — atomic & isolated
│   │   ├── persistence.go        # RDB loading from disk
│   │   ├── timeseries.go         # TS.CREATE, TS.ADD, TS.GET, TS.RANGE
│   │   ├── search.go             # FT.CREATE, FT.SEARCH, FT.HYBRID, FT.DROPINDEX, FT.INFO
│   │   ├── rag.go                # RAG.INGEST, RAG.QUERY, RAG.DEL, RAG.DOCS, RAG.INFO
│   │   ├── wiki.go               # WIKI.*: pages, links, index.md, log.md, lint, ingest
│   │   ├── memory.go             # MEMORY.ADD, MEMORY.GET, MEMORY.SEARCH
│   │   ├── llmcache.go           # LLMCACHE.SET, LLMCACHE.GET, LLMCACHE.STATS
│   │   ├── kb.go                 # shared knowledge-base storage and locking
│   │   ├── rag.go                # RAG.INGEST, RAG.QUERY, RAG.DEL, RAG.DOCS, RAG.INFO
│   │   ├── wiki.go               # WIKI.*: pages, links, index.md, log.md, lint, ingest
│   │   ├── memory.go             # MEMORY.ADD, MEMORY.GET, MEMORY.SEARCH
│   │   ├── llmcache.go           # LLMCACHE.SET, LLMCACHE.GET, LLMCACHE.STATS
│   │   ├── kb.go                 # shared knowledge-base storage and locking
│   │   ├── json.go               # JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND, etc.
│   │   ├── bloom.go              # BF.ADD, BF.EXISTS, BF.RESERVE, BF.MADD
│   │   ├── hyperloglog.go        # PFADD, PFCOUNT, PFMERGE
│   │   ├── topk.go               # TOPK.ADD, TOPK.QUERY, TOPK.LIST
│   │   ├── cms.go                # CMS.INCRBY, CMS.QUERY, CMS.MERGE
│   │   ├── tdigest.go            # TDIGEST.ADD, TDIGEST.QUANTILE
│   │   ├── bitfield.go           # BITFIELD, BITFIELD_RO
│   │   └── array.go              # AR.SET, AR.GET, AR.APPEND, AR.POP
│   ├── aof/                      # Append-Only File persistence & rewrite
│   ├── pubsub/                   # Publish/Subscribe channel hub
│   ├── cluster/                  # Cluster mode
│   │   ├── core/                 # Slot routing, TCC transactions, migration
│   │   ├── commands/             # Cluster-aware DEL, MSET, RENAME via TCC
│   │   └── raft/                 # Raft consensus — metadata, failover
│   ├── monitoring/               # Prometheus /metrics endpoint (redis_exporter compatible)
│   ├── auth/entraid/             # Entra ID JWT token validation (Azure AD)
│   ├── datastruct/               # Low-level data structure implementations
│   │   ├── dict/                 # Concurrent hash map (lock-striped)
│   │   ├── list/                 # Quicklist (linked list segments)
│   │   ├── set/                  # Hash set
│   │   ├── sortedset/            # Skip list
│   │   ├── bitmap/               # Bit array
│   │   ├── stream/               # Radix-tree-based stream
│   │   ├── search/               # Inverted index (terms → docs, scoring)
│   │   ├── hyperloglog/          # Probabilistic cardinality (2^14 registers)
│   │   ├── bloom/                # Bloom filter (k-hash, optimal m)
│   │   ├── cms/                  # Count-min sketch (frequency estimation)
│   │   ├── topk/                 # Top-K frequent items
│   │   ├── tdigest/              # T-Digest (quantile estimation)
│   │   ├── timeseries/           # Time series samples
│   │   ├── array/                # Sparse index-addressable array
│   │   └── lock/                 # Key-level read/write lock manager
│   └── lib/                      # Utility libraries
│       ├── logger/               # Structured file logger
│       ├── pool/                 # Generic object pool
│       ├── timewheel/            # Time wheel — expiration & cron
│       ├── wildcard/             # Glob pattern matching
│       ├── consistenthash/       # Consistent hashing ring
│       ├── idgenerator/          # Snowflake ID generator
│       ├── arena/                # Memory arena allocator
│       └── greenteagc/           # GC tuning (GCPercent=40, thread pinning)
├── config/                       # Configuration files
│   ├── standalone.toml           # Standalone server config
│   ├── cluster.toml              # Cluster mode config
│   └── crd/                      # Kubernetes CRD definitions
├── charts/                       # Helm chart for Kubernetes deployment
└── patches/                      # Patched dependencies (boltdb riscv64 support)
```

### Suggested reading order

Start with the **entry point** (`cmd/godis/main.go`) and follow the flow:

1. **`internal/config/`** — how godis loads and hot-reloads its configuration
2. **`internal/tcp/`** + **`internal/redis/parser/`** — how connections are accepted and RESP requests are parsed
3. **`internal/database/`** — the core: `router.go` (dispatch) → `server.go` (multi-db orchestration) → individual command files
4. **`internal/datastruct/`** — the data structures that power each command (dict, skiplist, quicklist, etc.)
5. **`internal/aof/`** + **`internal/database/persistence.go`** — durability: AOF rewrite and RDB loading
6. **`internal/cluster/`** — distributed mode: Raft consensus, slot routing, TCC transactions
7. **`internal/monitoring/`** — observability: Prometheus metrics

# License

This project is licensed under the [GPL license](https://github.com/Hoverhuang-er/godis/blob/master/LICENSE).
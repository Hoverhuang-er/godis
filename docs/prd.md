# Godis Product Requirements Document

> **Version:** 1.3.2
> **Last Updated:** 2026-07-22  
> **Repository:** [github.com/Hoverhuang-er/godis](https://github.com/Hoverhuang-er/godis)

---

## 1. Overview

Godis is a Redis-compatible in-memory data structure server written in Go. It provides a drop-in replacement for Redis with full RESP2/RESP3 protocol support, serving as both a production-grade cache/database and a reference implementation of a high-concurrency Go middleware.

### 1.1 Core Design Goals

- **Redis protocol compatibility**: RESP2/RESP3 wire protocol, supporting standard Redis clients
- **High concurrency**: Go-native concurrent core with goroutine-per-connection model and optional gnet event-loop backend
- **Full data structure support**: All standard Redis types plus Redis Stack modules (JSON, Search, TimeSeries, Bloom, T-Digest, Top-K, CMS)
- **Cluster mode**: Transparent client-side clustering with Raft-based metadata management, dynamic expansion, rebalancing, and failover
- **Persistence**: AOF with RDB preamble (hybrid persistence), RDB read/write, AOF rewrite
- **Replication**: Master-replica with PSYNC
- **Production features**: Prometheus metrics, slow log, TLS, Kubernetes operator

---

## 2. Architecture

### 2.1 Component Map

```
┌──────────────────────────────────────────────────────────┐
│                    Command Line (CLI)                     │
│              --cli  |  --web  |  <daemon>                │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│                    Config Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  TOML Files  │  │  Nacos CDC   │  │  Environment  │  │
│  └──────────────┘  └──────────────┘  └───────────────┘  │
│  (viper + hot-reload)                                    │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│                    Server Layer                           │
│  ┌──────────────────────┐  ┌─────────────────────────┐   │
│  │  TCP Server (std)    │  │  TCP Server (gnet)      │   │
│  │  goroutine-per-conn  │  │  event-loop reactor     │   │
│  └──────────┬───────────┘  └──────────┬──────────────┘   │
│             └──────────┬──────────────┘                   │
│                        ▼                                  │
│  ┌──────────────────────────────────────────────────┐    │
│  │            RESP Parser (v2 + v3)                  │    │
│  └──────────────────────┬───────────────────────────┘    │
└─────────────────────────┼──────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────┐
│                    Database Engine                       │
│  ┌─────────────────────────────────────────────────┐   │
│  │              Router / Executor                    │   │
│  │  ┌──────┐ ┌──────┐ ┌─────┐ ┌────┐ ┌──────┐    │   │
│  │  │String│ │ List │ │Hash │ │Set │ │ZSet  │    │   │
│  │  ├──────┤ ├──────┤ ├─────┤ ├────┤ ├──────┤    │   │
│  │  │Stream│ │ Geo  │ │Bitmap│ │JSON│ │Search│    │   │
│  │  ├──────┤ ├──────┤ ├─────┤ ├────┤ ├──────┤    │   │
│  │  │Bloom │ │TopK  │ │TDigest│ │CMS │ │HLL/TS │    │   │
│  │  └──────┘ └──────┘ └─────┘ └────┘ └──────┘    │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │         Transaction Engine (MULTI/EXEC)          │   │
│  │         Atomic, Isolated, Rollback               │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │         Persistence Layer                        │   │
│  │  ┌──────────────┐  ┌──────────┐  ┌──────────┐  │   │
│  │  │  AOF Writer  │  │ RDB Load │  │ AOF Re-  │  │   │
│  │  │  + Fsync     │  │ + Save   │  │ write    │  │   │
│  │  └──────────────┘  └──────────┘  └──────────┘  │   │
│  └─────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────┐   │
│  │     Replication Layer (Master/Slave + PSYNC)     │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────┬──────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────┐
│                    Cluster Layer                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Slot Manager │  │ Raft Consensus│  │ Migration   │ │
│  │ + TCC        │  │ + FSM        │  │ Engine      │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Replica Mgr  │  │ Node Manager │  │ Cron Tasks   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────┬──────────────────────────────┘
                          │
┌─────────────────────────▼──────────────────────────────┐
│              Support Services                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │Prometheus│ │ Web     │ │ Pub/Sub │ │ SlowLog │ │
│  │Metrics   │ │Dashboard │ │ Hub     │ │ Logger  │ │
│  ├──────────┤ ├──────────┤ ├──────────┤ ├──────────┤ │
│  │ K8s     │ │ HTTP API │ │ Auth    │ │ Entra ID│ │
│  │Operator │ │ (planned)│ │ (Redis) │ │ Auth    │ │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Execution Flow

1. **Client connects** → TCP listener accepts → goroutine spawned
2. **RESP parser** reads commands from wire → `[][]byte` command line
3. **Router** dispatches to registered command executor by name
4. **Authentication gate**: `isAuthenticated()` checks password before any data command
5. **Executor** interacts with the underlying `DB` data structures
6. **AOF logger** records write commands (if append-only enabled)
7. **Response** marshaled back via RESP protocol writer

---

## 3. Features & Capabilities

### 3.1 Data Types & Commands

#### 3.1.1 Core Data Types

| Type | File | Key Commands |
|------|------|-------------|
| **String** | `internal/database/string.go` | SET, GET, GETSET, GETDEL, GETEX, SETNX, SETEX, PSETEX, MSET, MGET, MSETNX, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, STRLEN, APPEND, SETRANGE, GETRANGE, SETBIT, GETBIT, BITCOUNT, BITPOS, GETRANDOMKEY |
| **List** | `internal/database/list.go` | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LPOS, LMOVE, BLMOVE, LPUSHX, RPUSHX, LMPOP, BLMPOP |
| **Hash** | `internal/database/hash.go` | HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HMGET, HMSET, HSTRLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HRANDFIELD, HSCAN, HEXPIRY |
| **Set** | `internal/database/set.go` | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SDIFF, SINTER, SUNION, SDIFFSTORE, SINTERSTORE, SUNIONSTORE, SSCAN, SINTERCARD |
| **Sorted Set** | `internal/database/sortedset.go` | ZADD, ZREM, ZSCORE, ZINCRBY, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREM, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZCARD, ZCOUNT, ZLEXCOUNT, ZPOPMIN, ZPOPMAX, ZMPOP, BZMPOP, ZMSCORE, ZRANDMEMBER, ZDIFF, ZINTER, ZUNION, ZDIFFSTORE, ZINTERSTORE, ZUNIONSTORE, ZINTERCARD, ZSCAN, ZRANK, ZREVRANK |

#### 3.1.2 Module / Extended Data Types

| Module | File | Key Commands |
|--------|------|-------------|
| **Stream** | `internal/database/stream.go` | XADD, XREAD, XRANGE, XREVRANGE, XLEN, XDEL, XGROUP, XREADGROUP, XACK, XTRIM, XINFO, XPENDING, XAUTOCLAIM |
| **GEO** | `internal/database/geo.go` | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH, GEOSEARCHSTORE |
| **Bitmap** | `internal/database/bitfield.go` | BITFIELD, BITFIELD_RO, BITCOUNT, BITPOS, SETBIT, GETBIT |
| **HyperLogLog** | `internal/database/hyperloglog.go` | PFADD, PFCOUNT, PFMERGE |
| **Bloom Filter** | `internal/database/bloom.go` | BF.ADD, BF.EXISTS, BF.RESERVE, BF.INFO, BF.INSERT, BF.MADD, BF.MEXISTS, BF.SCANDUMP, BF.LOADCHUNK |
| **Cuckoo Filter** | `internal/database/cms.go` | (via same framework) |
| **Count-Min Sketch** | `internal/database/cms.go` | CMS.INCRBY, CMS.QUERY, CMS.MERGE, CMS.INFO, CMS.INITBYDIM, CMS.INITBYPROB |
| **T-Digest** | `internal/database/tdigest.go` | TDIGEST.ADD, TDIGEST.QUANTILE, TDIGEST.MERGE, TDIGEST.RESET, TDIGEST.INFO, TDIGEST.MIN, TDIGEST.MAX, TDIGEST.BYRANK, TDIGEST.BYREVRANK, TDIGEST.RANK, TDIGEST.REVRANK, TDIGEST.TRIMMED_MEAN, TDIGEST.CDF |
| **Top-K** | `internal/database/topk.go` | TOPK.ADD, TOPK.QUERY, TOPK.LIST, TOPK.INFO, TOPK.COUNT, TOPK.INCRBY |
| **TimeSeries** | `internal/database/timeseries.go` | TS.CREATE, TS.ADD, TS.GET, TS.MGET, TS.RANGE, TS.REVRANGE, TS.MRANGE, TS.MREVRANGE, TS.ALTER, TS.DELETERULE, TS.DEL, TS.INFO, TS.INCRBY, TS.DECRBY, TS.QUERYINDEX |
| **RedisJSON** | `internal/database/json.go` | JSON.SET, JSON.GET, JSON.DEL, JSON.MGET, JSON.ARRAPPEND, JSON.ARRPOP, JSON.ARRTRIM, JSON.ARRINSERT, JSON.ARRLEN, JSON.OBJKEYS, JSON.OBJLEN, JSON.TYPE, JSON.NUMINCRBY, JSON.STRAPPEND, JSON.STRLEN, JSON.TOGGLE, JSON.CLEAR, JSON.DEBUG, JSON.FORGET, JSON.MERGE, JSON.MSET, JSON.RESP, JSON.SET |
| **RediSearch** | `internal/database/search.go` | FT.CREATE, FT.SEARCH, FT.DROPINDEX, FT.INFO, FT.ADD, FT._LIST, FT.AGGREGATE, FT.ALIASADD, FT.ALIASDEL, FT.ALIASUPDATE, FT.CONFIG, FT.DICTADD, FT.DICTDEL, FT.DICTDUMP, FT.EXPLAIN, FT.EXPLAINCLI, FT.PROFILE, FT.SPELLCHECK, FT.SUGADD, FT.SUGDEL, FT.SUGGET, FT.SUGLEN, FT.SYNADD, FT.SYNDUMP, FT.SYNUPDATE, FT.TAGVALS |

#### 3.1.3 System Commands

| Group | File | Commands |
|-------|------|----------|
| **Connection** | `internal/database/server.go` | PING, AUTH, HELLO, CLIENT (SETNAME, GETNAME, LIST, ID, INFO, KILL), QUIT, ECHO |
| **Server** | `internal/database/systemcmd.go` | INFO, COMMAND, DBSIZE, SLAVEOF, ROLE, TIME, CONFIG (GET, SET, REWRITE), LASTSAVE, MEMORY (STATS, USAGE, DOCTOR, PURGE), MODULE (LIST, LOAD, UNLOAD), DEBUG |
| **Keys** | `internal/database/keys.go` | DEL, EXISTS, EXPIRE, EXPIREAT, EXPIRETIME, PEXPIRE, PEXPIREAT, PEXPIRETIME, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, COPY, FLUSHDB, FLUSHALL, KEYS, SCAN, SORT, RANDOMKEY, TOUCH, MOVE, WAIT, WAITAOF, DUMP, RESTORE, OBJECT |
| **Transactions** | `internal/database/transaction.go` | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| **Pub/Sub** | `internal/database/server.go` + `internal/pubsub/` | SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB (CHANNELS, NUMPAT, NUMSUB) |
| **Persistence** | `internal/database/server.go` | SAVE, BGSAVE, BGREWRITEAOF, REWRITEAOF, LASTSAVE |
| **Replication** | `internal/database/replication_master.go`, `replication_slave.go` | REPLCONF, PSYNC, SLAVEOF, ROLE |
| **Slow Log** | `internal/database/slowlog.go` | SLOWLOG (GET, LEN, RESET) |

### 3.2 Authentication & Security

| Feature | Status | Details |
|---------|--------|---------|
| **Redis AUTH** | ✅ Implemented | `requirepass` in config, `AUTH` command handler, `isAuthenticated()` gate |
| **Master auth** | ✅ Implemented | `masterauth` config for replication auth |
| **Entra ID (Azure AD)** | ✅ Implemented | JWT token validation via JWKS, configurable `enabled`, `tenant_id`, `app_id`, `client_id` |
| **Token-based HTTP API auth** | 🚧 Planned | Rotating tokens via POST /api/auth, 128-char uppercase random string, configurable expiry |

### 3.3 Persistence

| Feature | File | Details |
|---------|------|---------|
| **AOF (Append-Only File)** | `internal/aof/aof.go` | Configurable fsync: always/everysec/no, AOF rewrite, RDB preamble (hybrid) |
| **RDB** | `internal/aof/rdb.go` | RDB loading on startup, BGSAVE, SAVE |
| **AOF Rewrite** | `internal/aof/rewrite.go` | Background rewrite with RDB preamble, configurable trigger |
| **RDB Marshal/Unmarshal** | `internal/aof/marshal.go` | Custom RDB format reader/writer |

### 3.4 Replication

| Feature | File | Details |
|---------|------|---------|
| **Master-Slave** | `internal/database/replication_master.go` | Full and partial resync (PSYNC2) |
| **Replica** | `internal/database/replication_slave.go` | Connect to master, accept replication stream |
| **ReplConf** | `internal/database/replication_master.go` | Heartbeat, offset tracking |
| **Timeout** | Config | `repl_timeout` in seconds |

### 3.5 Clustering

| Feature | File | Details |
|---------|------|---------|
| **Raft Consensus** | `internal/cluster/raft/` | Hashicorp Raft for cluster metadata |
| **Slot Management** | `internal/cluster/core/core.go` | Slot-to-node mapping, 16384 slots |
| **TCC Transactions** | `internal/cluster/core/tcc.go` | Try-Confirm/Cancel for multi-node transactions |
| **Node Management** | `internal/cluster/core/node_manager.go` | Join, leave, failover |
| **Migration** | `internal/cluster/core/migration.go` | Slot migration between nodes |
| **Worker Pool** | Config | `cluster_worker_pool`, `cluster_relay_parallel` |
| **Commands** | `internal/cluster/commands/` | DEL, MSET, RENAME, RENAMENX — atomic across nodes |

### 3.6 Monitoring & Observability

| Feature | File | Details |
|---------|------|---------|
| **Prometheus Metrics** | `internal/monitoring/metrics.go` | Redis-exporter compatible, port 9121 default |
| **Key Metrics** | | Total commands, connections (current/total), keyspace hits/misses, ops/sec, DB stats |
| **Hot Key Tracking** | | Top-N most accessed keys |
| **Big Key Tracking** | | Keys exceeding size thresholds by type |
| **Slow Log** | `internal/database/slowlog.go` | Configurable threshold (μs) and max length |
| **Web Dashboard** | `internal/web/dashboard.go` | HTML dashboard with live stats, queries, monitor, hot/big keys |

### 3.7 Web Dashboard

| Feature | Details |
|---------|---------|
| **URL** | `http://localhost:63808` |
| **Pages** | Dashboard (/) with live stats |
| **API Endpoints** | `/api/query` (POST, read-only queries), `/api/monitor` (SSE), `/api/stats` (JSON) |
| **Auth** | Via `--auth` flag when starting with `--web` |
| **Mode** | Separate process, connects to godis via Redis client |

### 3.8 Data Structures & Internals

| Component | File(s) | Details |
|-----------|---------|---------|
| **Dict (hash table)** | `internal/datastruct/dict/` | Concurrent hash map with sharded locks, simple map |
| **List** | `internal/datastruct/list/` | QuickList (linked list of compressed chunks), linked list |
| **Set** | `internal/datastruct/set/` | Hash-set |
| **Sorted Set** | `internal/datastruct/sortedset/` | SkipList + hash map |
| **Bitmap** | `internal/datastruct/bitmap/` | Bit array operations |
| **Stream** | `internal/datastruct/stream/` | Radix-tree based |
| **Bloom Filter** | `internal/datastruct/bloom/` | Standard bloom with scaling |
| **T-Digest** | `internal/datastruct/tdigest/` | Average-based merging |
| **Top-K** | `internal/datastruct/topk/` | Count-min sketch based |
| **CMS** | `internal/datastruct/cms/` | Count-min sketch |
| **HyperLogLog** | `internal/datastruct/hyperloglog/` | Standard HLL |
| **TimeSeries** | `internal/datastruct/timeseries/` | Compressed time-series |
| **Array** | `internal/datastruct/array/` | Generic array utility |
| **Search** | `internal/datastruct/search/` | Inverted index + vector index |
| **GeoHash** | `internal/lib/geohash/` | Geohash encoding/decoding |
| **Wildcard** | `internal/lib/wildcard/` | Glob-style pattern matching |
| **Lock** | `internal/datastruct/lock/` | Lock map for key-level locking |
| **Time Wheel** | `internal/lib/timewheel/` | Delay queue and time wheel scheduler |
| **ID Generator** | `internal/lib/idgenerator/` | Snowflake ID generator |
| **Connection Pool** | `internal/lib/pool/` | Generic connection pool |
| **Arena Allocator** | `internal/lib/arena/` | Memory arena for reduced GC pressure |
| **Logger (GreenteaGC)** | `internal/lib/greenteagc/` | Custom GC-optimized logger |
| **Utility** | `internal/lib/utils/` | Random string, misc helpers |
| **Sync (WaitGroup, Atomic)** | `internal/lib/sync/` | WaitGroup with timeout, atomic bool |

### 3.9 Deployment Options

| Option | File | Details |
|--------|------|---------|
| **Standalone binary** | `cmd/godis/main.go` | `go build -o godis ./cmd/godis` |
| **Docker** | `Dockerfile` | Multi-stage build, `ghcr.io/Hoverhuang-er/godis` |
| **Docker Compose** | `docker-compose.yml` | Single-node standalone |
| **Kubernetes Operator** | `internal/operator/` | CRD-based `GodisCluster` resource, controller |
| **Helm Charts** | `charts/godis/` | K8s deployment charts |
| **CLI mode** | `cmd/godis/cli.go` | Interactive CLI with `--cli` flag |
| **Web Dashboard mode** | `cmd/godis/main.go` | Standalone dashboard with `--web` flag |

### 3.10 Configuration Reference

| Section | Parameter | Type | Default | Description |
|---------|-----------|------|---------|-------------|
| **server** | bind | string | 0.0.0.0 | Bind address |
| | port | int | 6399 | Listen port |
| | dir | string | /opt/godis | Working directory |
| | maxclients | int | 128 | Max client connections |
| | databases | int | 16 | Number of databases |
| | use_gnet | bool | false | Use gnet event-loop |
| **aof** | appendonly | bool | false | Enable AOF |
| | appendfilename | string | appendonly.aof | AOF filename |
| | appendfsync | string | everysec | Fsync policy |
| | aof_use_rdb_preamble | bool | true | Hybrid persistence |
| | dbfilename | string | test.rdb | RDB filename |
| **security** | requirepass | string | — | Client auth password |
| | masterauth | string | — | Master auth password |
| **replication** | announce_host | string | — | NAT address |
| | slave_announce_port | int | 0 | NAT port |
| | repl_timeout | int | 60 | Timeout in seconds |
| **slowlog** | log_slower_than | int64 | 10000 | Threshold in μs |
| | max_len | int | 128 | Max log length |
| **monitoring** | prometheus_enabled | bool | true | Enable Prometheus |
| | prometheus_port | int | 9121 | Metrics port |
| **cluster** | enable | bool | — | Enable cluster mode |
| | as_seed | bool | — | Bootstrap cluster |
| | seed | string | — | Join address |
| | raft_listen_address | string | — | Raft listen |
| | raft_advertise_address | string | — | Raft advertise |
| | master_in_cluster | string | — | Replica of address |
| | worker_pool | bool | true | Parallel relay pool |
| | relay_parallel | bool | true | Parallel relay |
| **azure** | entra_tenant_id | string | — | Entra ID tenant |
| | entra_app_id | string | — | Entra ID app |

### 3.11 Supported Compilation Targets

| OS | Architectures |
|----|--------------|
| Linux | amd64 (v3), arm64, riscv64 |
| macOS (Darwin) | amd64 (v3), arm64 |
| Windows | amd64, arm64 |

---

## 4. HTTP API Service (Planned Feature)

### 4.1 Purpose

Expose native Redis operations over HTTP for non-RESP clients (browsers, mobile apps, serverless functions, scripting).

### 4.2 Auth Flow

```
POST /api/auth
Body: { "password": "...", "expired": 72 }  // expired in hours, 0=permanent
→ Response: { "token": "A1B2C3...", "expires_at": "..." }
→ Subsequent: X-HEADER-AUTHTOKEN: <token>
```

- Token: 128-character random uppercase ASCII string
- Default TTL: 72 hours (configurable via `expired` parameter)
- `expired: 0` or `expired: "always"` → permanent token
- Tokens are managed in-memory, rotate-able

### 4.3 Command API

```
GET /api/commands?type=string&key=mykey&value=myvalue&px=3600
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `type` | Yes | Command type / Redis data type |
| `key` | Yes* | Key to operate on |
| `value` | No | Value for write operations |
| `field` | No | Hash field |
| `member` | No | Set/SortedSet member |
| `score` | No | SortedSet score |
| `args` | No | Additional arguments (comma/space-separated) |

### 4.4 Security

- Token-based auth header (`X-HEADER-AUTHTOKEN`)
- Respects the same `requirepass` as the TCP Redis protocol
- Tokens cannot be used to bypass the Redis protocol's `requirepass`
- Rate limiting (future enhancement)

---

## 5. HTTP API — Implementation Specification

### 5.1 Token Engine (`internal/web/token.go`)

**Data structures:**
```go
type TokenEntry struct {
    Token     string
    CreatedAt time.Time
    ExpiresAt *time.Time  // nil = permanent
}

type TokenEngine struct {
    mu     sync.RWMutex
    tokens map[string]*TokenEntry  // token → entry
    hasher PasswordHasher          // for auth validation
}
```

**Functions:**
- `NewTokenEngine(requirePass string)` — creates token engine with password reference
- `GenerateToken(expiredHours int) (*TokenEntry, error)` — creates 128-char random uppercase token
- `ValidateToken(token string) bool` — checks if token exists and not expired
- `Authenticate(password string) (*TokenEntry, error)` — validates against `requirepass` and generates token
- `RevokeToken(token string)` — removes a token
- `cleanupLoop()` — periodic expired token cleanup

### 5.2 Auth Handler (`POST /api/auth`)

```go
func (ds *DashboardServer) handleAuth(w http.ResponseWriter, r *http.Request)
```

**Request:**
```json
{
    "password": "your-redis-password",
    "expired": 72
}
```

- `password` (required): The Redis `requirepass`
- `expired` (optional): Token TTL in hours. Default 72. `0` or `"always"` = permanent.

**Response (200):**
```json
{
    "token": "XV7M...9QL2",
    "expires_at": "2026-07-25T12:00:00Z",
    "permanent": false
}
```

**Response (401):**
```json
{
    "error": "invalid password"
}
```

### 5.3 Commands Handler (`GET /api/commands`)

```go
func (ds *DashboardServer) handleCommands(w http.ResponseWriter, r *http.Request)
```

**Authentication:** Check `X-HEADER-AUTHTOKEN` header against the token engine.

**Query parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | Yes | Redis command type (string, hash, list, set, zset, key, server, etc.) |
| `key` | string | Conditional | Key name (required for most operations) |
| `value` | string | No | Value for write operations |
| `field` | string | No | Hash field name |
| `member` | string | No | Set/SortedSet member |
| `score` | number | No | SortedSet score |
| `args` | string | No | Additional space-separated arguments |
| `db` | number | No | Database index (default: 0) |

**Examples:**
```
GET /api/commands?type=string&key=foo&value=bar      → SET foo bar
GET /api/commands?type=string&key=foo                  → GET foo
GET /api/commands?type=hash&key=user:1&field=name     → HGET user:1 name
GET /api/commands?type=key&key=foo                    → EXISTS foo (default TTL check)
```

**Response (200):**
```json
{
    "success": true,
    "result": "bar",
    "raw": "+OK\r\n"
}
```

**Response (401):**
```json
{
    "error": "unauthorized - invalid or expired token"
}
```

### 5.4 Middleware

```go
func authMiddleware(next http.HandlerFunc, engine *TokenEngine) http.HandlerFunc
```

- Reads `X-HEADER-AUTHTOKEN` from request header
- Validates via `engine.ValidateToken()`
- Returns 401 on invalid/missing token
- Sets `r.Context()` with authenticated marker for downstream handlers

### 5.5 Updated Configuration

```toml
[http_api]
# Enable the HTTP API server
api_enabled = false
# HTTP API listen port
api_port = 63809
# Default token expiration in hours (default: 72)
api_token_expiry = 72
```

### 5.6 Integration

- `DashboardServer` gains a `tokenEngine` field
- `NewDashboard()` initializes the token engine with `config.Properties.RequirePass`
- Routes added for `/api/auth` (POST) and `/api/commands` (GET)
- Auth middleware wraps the commands handler
- The existing dashboard and metrics server are independent; the API server runs on a separate port
- Configuration `http_api` section added to `config.ServerProperties`

---

## 6. Future Roadmap

### 6.1 Short-term (v1.4.0)
- HTTP API service (auth + commands)
- Rate limiting for HTTP API
- Swagger/OpenAPI documentation

### 6.2 Medium-term
- ACL (Access Control Lists) — Redis 6+ compatible
- TLS for HTTP API
- WebSocket support for real-time commands
- API key management with persistent storage

### 6.3 Long-term
- Full Redis Stack module API parity
- Improved cluster observability
- Multi-cloud replication
- SQL-like query interface

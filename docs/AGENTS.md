# Repository Guidelines

## Project Overview

**godis** — a Redis-compatible in-memory database server written in Go (`github.com/hdt3213/godis`). Supports standalone, Raft-based cluster, master-replica replication, AOF/RDB persistence, pub/sub, transactions (MULTI/EXEC/WATCH), and RESP2/RESP3 protocols. Runs on std TCP or gnet event-loop transport.

---

## Architecture & Data Flow

```
cmd/godis/main.go
  ├── internal/config (redis.conf parser, ServerProperties)
  ├── internal/cluster or database.Server
  │     ├── internal/database.DB (single DB instance)
  │     │     ├── internal/datastruct/dict (ConcurrentDict, sharded map)
  │     │     ├── internal/datastruct/list, set, sortedset, bitmap, stream, ...
  │     │     ├── internal/lib/timewheel (TTL expiry)
  │     │     └── internal/datastruct/lock (per-key RWMutex table)
  │     ├── internal/pubsub.Hub
  │     └── internal/aof.Persister (AOF + RDB preamble)
  ├── internal/redis/server/std or internal/redis/server/gnet (TCP layer)
  │     ├── internal/redis/parser (RESP parser: ParseStream / ParseV2)
  │     ├── internal/redis/protocol (reply builders)
  │     └── internal/redis/connection (Connection impl, sync.Pool)
  └── internal/interface/ (redis.Reply, redis.Connection, database.DB, tcp.Handler)
```

**Command dispatch:**
1. TCP server receives bytes → RESP parser produces `[][]byte` command line
2. `Server.Exec()` handles auth, replication, pub/sub, system commands directly
3. Normal commands: selects DB by index → `DB.Exec()` handles MULTI/EXEC transaction control
4. `DB.execNormalCommand()`: looks up `cmdTable`, calls `prepare()` for key analysis, acquires per-key `RWLocks` in sorted order, calls executor
5. Executor returns `redis.Reply` → serialized via `ToBytes()` → written to connection

---

## Directory Structure (Go Project Layout)

```
cmd/godis/          — Entry point (main.go)
internal/           — Private application code
  aof/              — Append-Only File persistence + RDB loading
  cluster/          — Raft-based cluster (core/, raft/, commands/)
  config/           — Redis-conf-style config parser
  database/         — Core server logic: DB, Server, per-type command files
  datastruct/       — In-memory data structure implementations
  interface/        — Contract types (redis.Reply, redis.Connection, database.DB, tcp.Handler)
  lib/              — Utility packages (arena, pool, logger, wildcard, timewheel, ...)
  pubsub/           — Channel-based pub/sub hub
  redis/            — Protocol layer (parser/, protocol/, connection/, server/, client/)
  tcp/              — Generic TCP server (goroutine-per-connection accept loop)
config/             — Configuration file templates (standalone.toml, cluster.toml, redis.conf, ...)
scripts/            — Build and release scripts
docs/               — Documentation (README*, CHANGELOGS, AGENTS.md)
testdata/           — Test fixtures (test.rdb)
```

---

## Development Commands

| Command | Purpose |
|---------|---------|
| `go build -o godis ./cmd/godis/` | Build binary |
| `go run ./cmd/godis/` | Run with default config (redis.conf) |
| `CONFIG=my.conf go run ./cmd/godis/` | Run with custom config |
| `go test ./...` | Run all tests |
| `go test -v -coverprofile=profile.cov ./...` | Tests with coverage |
| `go test -run TestSet ./internal/database/` | Run specific test |
| `./scripts/build.sh` | Cross-compile release (7 OS/arch targets) |
| `./scripts/build-all.sh` | Quick cross-compile (5 targets) |

**Build tags** (optional): `-tags greenteagc` enables GC tuning (GCPercent=40, GOMAXPROCS=NumCPU, thread pinning).
**Env**: `GOEXPERIMENT=jsonv2` used in releases.
**CGO**: Disabled for all builds.

---

## Code Conventions & Common Patterns

### Naming

- **Exported types/functions**: PascalCase (`DB`, `Server`, `ConcurrentDict`, `MakeSet`, `NewStream`)
- **Unexported**: camelCase (`makeDB`, `execSet`, `cmdTable`, `addVersion`)
- **Constructors**: `MakeXxx()` for interface-backed types (dict, list, set), `NewXxx()` for concrete types (stream, timeseries, invertedIndex, quickList)
- **Receiver names**: single-letter — `db *DB`, `c *Connection`, `dict *ConcurrentDict`, `server *Server`, `persister *Persister`
- **Alias types**: `type CmdLine = [][]byte` (declared in both `internal/database/` and `internal/aof/`)
- **File per type**: one `.go` file per command category (`string.go`, `hash.go`, `list.go`, `set.go`, `sortedset.go`, `geo.go`, `stream.go`, `timeseries.go`, `search.go`)

### Command Registration Pattern

```go
// In init() of each command file:
func init() {
    registerCommand("set", execSet, prepareSet, undoSet, -3, flagWrite)
}
```

`registerCommand(name, ExecFunc, PreFunc, UndoFunc, arity, flags)`. Arity < 0 means variable-arity; minimum count is the absolute value.

Command executors always follow: `func execXxx(db *DB, args [][]byte) redis.Reply`

### Concurrency

- **Sharded locking**: `ConcurrentDict` uses FNV-32 hash → power-of-2 shards, each with `sync.RWMutex`. The `lock.Locks` package reuses the same hash for per-key RWMutex.
- **Deadlock prevention**: multi-key locks acquired in sorted index order.
- **Command-level locking**: `prepare()` returns write/read key sets; `DB.RWLocks()` acquires all relevant shard locks before executor runs.
- **Self-locking types**: Stream, Array, TimeSeries, InvertedIndex embed `sync.RWMutex` directly.
- **Delegated locking**: Set wraps `dict.Dict` — threadsafe if using `ConcurrentDict`, caller's responsibility if `SimpleDict`.
- **Non-threadsafe types**: SortedSet, LinkedList, QuickList, BitMap — rely on caller's locks.
- **AOF channel**: buffered channel (size `1<<20`), consumed by background goroutine, serialized via `pausingAof` mutex.

### Error Handling

- Every command returns `redis.Reply` (interface with `ToBytes() []byte`). Errors are encoded as protocol `-ERR` replies.
- Typed error replies: `MakeErrReply(msg)`, `MakeArgNumErrReply(cmd)`, `SyntaxErrReply`, `WrongTypeErrReply`, `UnknownErrReply`
- Data structures use Go's standard `(value, bool)` or `int` returns — no custom error types at the data layer.
- `Server.Exec()` has a top-level `recover()` panic catcher returning `UnknownErrReply`.
- AOF errors logged via `slog.Warn`/`slog.Error` — non-blocking.

### Callback Pattern

Traversal universally uses `ForEach(func(key string, val interface{}) bool)` — bool return controls early termination (`true`=continue, `false`=break).

### Logging

Structured JSON logging via `internal/lib/logger` (wraps `zap.Logger`, bridges `slog`). Global `DefaultLogger` with package-level convenience functions. Writes to stdout + `godis.log`.

---

## Important Files

| File | Purpose |
|------|---------|
| `cmd/godis/main.go` | Entry point: config → cluster or standalone → TCP listener |
| `internal/database/database.go` | Core `DB` struct: data store, TTL, version map, command dispatch |
| `internal/database/server.go` | Multi-DB `Server` struct: replication, AOF, pub/sub, system commands |
| `internal/database/router.go` | Command registration table (`cmdTable`), `registerCommand()`, flag definitions |
| `internal/interface/redis/reply.go` | `Reply` interface — single `ToBytes() []byte` method |
| `internal/interface/redis/conn.go` | `Connection` interface — full client contract |
| `internal/interface/database/db.go` | `DB` / `DBEngine` interfaces, `DataEntity` struct |
| `internal/redis/parser/parser.go` | `ParseStream` — channel-based RESP parser |
| `internal/redis/protocol/reply.go` | RESP2 reply builders (`BulkReply`, `MultiBulkReply`, etc.) |
| `internal/redis/protocol/consts.go` | Singleton replies (`PongReply`, `OkReply`, `NullBulkReply`) |
| `internal/redis/connection/conn.go` | Connection implementation (`sync.Pool` recycled) |
| `internal/cluster/cluster.go` | Cluster entry point (alias to `core.Cluster`) |
| `internal/config/config.go` | `ServerProperties` with reflect-based config parsing |
| `internal/aof/aof.go` | AOF persistence — background writer, rewrite, RDB preamble |
| `go.mod` | Module definition (Go 1.26.0) |

---

## Runtime/Tooling Preferences

- **Runtime**: Go 1.26.0+, no other runtime required
- **Module**: `github.com/hdt3213/godis`
- **Package manager**: Go modules (no external tooling)
- **Key deps**: `github.com/hashicorp/raft` + `raft-boltdb` (cluster), `github.com/hdt3213/rdb` (RDB parser), `github.com/panjf2000/gnet/v2` (event-loop transport)
- **Build constraint**: `CGO_ENABLED=0` for all cross-compilation
- **Editor**: `.vscode/launch.json` present for debugging

---

## Testing & QA

- **Framework**: Standard `testing` package only — no external test frameworks
- **Location**: 46 `_test.go` files co-located with source across all packages
- **Test helpers**: `MakeTestDB()` / `NewStandaloneServer()` create in-memory test fixtures; `Exec()` returns `redis.Reply`; assertions compare `ToBytes()` output or use `internal/redis/protocol/asserts` helpers
- **Test patterns**:
  - Database tests: execute command via `Exec()`, compare reply `ToBytes()` output
  - Integration tests: ephemeral ports via `net.Listen("tcp", ":0")`
  - Parser tests: streaming `ParseStream` from `io.Reader` + `TestParseOne` for single messages
- **CI**: GitHub Actions on push/PR to master — `go test -v -coverprofile=profile.cov ./...` with Redis 5, uploads to coveralls
- **Release CI**: GitHub Actions on `v*` tags — matrix build 7 targets, creates release with artifacts and checksums

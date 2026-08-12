# Conf Namespace + Standalone-to-Cluster Join — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Godis starts standalone by default; later nodes join via TCP `--join` or HTTP API; the first node auto-promotes to seed and shares its configuration through a `conf` namespace in db0 that syncs to joiners and propagates between nodes.

**Architecture:** Config lives as granular keys with reserved prefix `conf:` in db0 (persisted/replicated as ordinary keys — no new storage). Node-local settings (bind/port/dir/cluster/raft/OSS) never enter the namespace; everything else is written at startup from the local TOML (conf key wins on later boots). A standalone node receiving `cluster.join` (TCP or via HTTP API forwarding) auto-promotes: generates `cluster_id`, wraps the live DB in a cluster, swaps via the existing `DBSwapper`. `CONFIG GET/SET/REWRITE` are new server commands wired to conf keys; in cluster mode a registered `config` command relays to the conf slot owner (slot 0, pinned to the seed). Master-slave join syncs the whole namespace via existing full sync; raft mode routes conf keys to slot 0.

**Tech Stack:** Go 1.26, stdlib `reflect` for config serialization, existing `hashicorp/raft`, existing `DBSwapper` hot-switch, existing `viper` config layer, std `testing` only.

## Global Constraints

- CGO_ENABLED=0 builds; std `testing` only, no new dependencies.
- Every test uses `connection.NewFakeConn()` + `server.Exec(...)` unless stated otherwise.
- Follow existing naming: `MakeXxx` constructors, single-letter receivers, `execXxx` executors.
- Reserved prefix is exactly `conf:` (constant `ConfNamespacePrefix`); `IsConfKey` is the single source of truth, exported from `internal/database`.
- Node-local params (never in conf namespace): `runid, bind, port, dir, announce-host, slave-announce-port, slave-announce-ip, use-gnet, data-dir, cluster-* (all), raft-* (all), master-in-cluster, oss-* (all), http-api-*, web-port, sentinel-*, sentinel-monitors`.
- Spec: `docs/superpowers/specs/2026-08-12-conf-namespace-cluster-join-design.md`.

---

### Task 1: Conf prefix, write guard, KEYS/SCAN filtering

**Files:**
- Create: `internal/database/namespace.go`
- Modify: `internal/database/database.go:109-140` (execNormalCommand, execWithLock)
- Modify: `internal/database/keys.go:334-345` (execKeys), `internal/database/keys.go:433-470` (execScan)
- Test: `internal/database/namespace_test.go`

**Interfaces:**
- Produces: `const ConfNamespacePrefix = "conf:"`, `func IsConfKey(key string) bool` — used by Tasks 4, 5, 8 and cluster core.

- [ ] **Step 1: Write the failing test**

```go
// internal/database/namespace_test.go
package database

import (
	"testing"

	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

func TestIsConfKey(t *testing.T) {
	if !IsConfKey("conf:requirepass") {
		t.Error("conf:requirepass should be a conf key")
	}
	if IsConfKey("requirepass") || IsConfKey("xconf:y") || IsConfKey("") {
		t.Error("non-conf keys detected as conf keys")
	}
}

func TestConfWriteGuard(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	ret := server.Exec(conn, utils.ToCmdLine("SET", "conf:requirepass", "x"))
	asserts.AssertErrReply(t, ret, "ERR reserved key prefix \"conf:\"")
	ret = server.Exec(conn, utils.ToCmdLine("MSET", "conf:a", "1", "b", "2"))
	asserts.AssertErrReply(t, ret, "ERR reserved key prefix \"conf:\"")
	// normal keys unaffected
	ret = server.Exec(conn, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertNotError(t, ret)
}

func TestConfKeysHiddenFromIteration(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	db := server.mustSelectDB(0)
	db.PutEntity("conf:x", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("user", &database.DataEntity{Data: []byte("v")})

	ret := server.Exec(conn, utils.ToCmdLine("KEYS", "*"))
	asserts.AssertMultiBulkReply(t, ret, []string{"user"})

	ret = server.Exec(conn, utils.ToCmdLine("SCAN", "0"))
	if bulk, ok := ret.(*protocol.MultiBulkReply); ok {
		for _, item := range bulk.Args[1:] {
			if string(item) == "conf:x" {
				t.Fatal("SCAN returned conf key")
			}
		}
	} else {
		t.Fatalf("unexpected SCAN reply type %T", ret)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/database/ -run 'TestIsConfKey|TestConfWriteGuard|TestConfKeysHiddenFromIteration' -v`
Expected: compile error — `IsConfKey` undefined; guard and filters absent.

- [ ] **Step 3: Create the prefix helper**

```go
// internal/database/namespace.go
package database

import "strings"

// ConfNamespacePrefix is the reserved key prefix of the conf namespace in
// db0. Keys under it hold shared configuration; they are filtered from
// user-facing iteration (KEYS/SCAN/DBSIZE/FLUSH*) and protected from client
// writes. Internal config paths (CONFIG SET, bootstrap, replication) write
// them directly and bypass the guard.
const ConfNamespacePrefix = "conf:"

// IsConfKey reports whether key belongs to the conf namespace.
func IsConfKey(key string) bool {
	return strings.HasPrefix(key, ConfNamespacePrefix)
}
```

- [ ] **Step 4: Add the write guard to execNormalCommand and execWithLock**

```go
// internal/database/database.go — inside execNormalCommand, after
// `write, read := prepare(cmdLine[1:])`:
	write, read := prepare(cmdLine[1:])
	for _, k := range write {
		if IsConfKey(k) {
			return protocol.MakeErrReply("ERR reserved key prefix \"conf:\"")
		}
	}
	db.addVersion(write...)
```

```go
// internal/database/database.go — inside execWithLock, after the
// validateArity check:
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	if cmd.prepare != nil {
		write, _ := cmd.prepare(cmdLine[1:])
		for _, k := range write {
			if IsConfKey(k) {
				return protocol.MakeErrReply("ERR reserved key prefix \"conf:\"")
			}
		}
	}
	fun := cmd.executor
```

- [ ] **Step 5: Filter KEYS and SCAN**

```go
// internal/database/keys.go — execKeys, first line of the ForEach body:
	db.data.ForEach(func(key string, val interface{}) bool {
		if IsConfKey(key) {
			return true
		}
		if !pattern.IsMatch(key) {
			return true
		}
```

```go
// internal/database/keys.go — execScan, after DictScan and before the
// scanType filter:
	keysReply, nextCursor := db.data.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}
	filtered := keysReply[:0]
	for _, k := range keysReply {
		if !IsConfKey(string(k)) {
			filtered = append(filtered, k)
		}
	}
	keysReply = filtered
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/database/ -run 'TestIsConfKey|TestConfWriteGuard|TestConfKeysHiddenFromIteration' -v`
Expected: PASS (3 tests).

- [ ] **Step 7: Run the package suite (regression)**

Run: `go test ./internal/database/`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/database/namespace.go internal/database/namespace_test.go internal/database/database.go internal/database/keys.go
git commit -m "feat: conf namespace prefix, write guard, KEYS/SCAN filtering"
```

---

### Task 2: FLUSH preserves conf keys; DBSIZE/INFO exclude conf

**Files:**
- Modify: `internal/database/server.go:497-518` (flushDB, flushAll)
- Modify: `internal/database/server.go:645-649` (GetDBSize) — add GetUserDBSize next to it
- Modify: `internal/database/systemcmd.go:97-100` (DbSize), `internal/database/systemcmd.go:157-165` (INFO keyspace)
- Modify: `internal/cluster/core/command.go:45-48` (cluster dbsize)
- Test: `internal/database/namespace_test.go` (append)

**Interfaces:**
- Produces: `func (server *Server) GetUserDBSize(dbIndex int) (int, int)` — counts non-conf keys.

- [ ] **Step 1: Write the failing tests (append to namespace_test.go)**

```go
func TestFlushDBPreservesConfKeys(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	db := server.mustSelectDB(0)
	db.PutEntity("conf:x", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("user", &database.DataEntity{Data: []byte("v")})

	ret := server.Exec(conn, utils.ToCmdLine("FLUSHDB"))
	asserts.AssertNotError(t, ret)

	if _, ok := server.mustSelectDB(0).GetEntity("conf:x"); !ok {
		t.Fatal("conf key lost by FLUSHDB")
	}
	if _, ok := server.mustSelectDB(0).GetEntity("user"); ok {
		t.Fatal("user key survived FLUSHDB")
	}
}

func TestDBSizeExcludesConfKeys(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	db := server.mustSelectDB(0)
	db.PutEntity("conf:x", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("user", &database.DataEntity{Data: []byte("v")})

	ret := server.Exec(conn, utils.ToCmdLine("DBSIZE"))
	asserts.AssertIntReply(t, ret, 1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/database/ -run 'TestFlushDBPreservesConfKeys|TestDBSizeExcludesConfKeys' -v`
Expected: FAIL — FLUSHDB drops conf key; DBSIZE returns 2.

- [ ] **Step 3: Preserve conf keys in flushDB**

```go
// internal/database/server.go — flushDB:
func (server *Server) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB := makeDB()
	// conf namespace is config, not data: it survives FLUSHDB/FLUSHALL
	oldDB.data.ForEach(func(key string, val interface{}) bool {
		if IsConfKey(key) {
			newDB.PutEntity(key, val.(*database.DataEntity))
			if raw, ok := oldDB.ttlMap.Get(key); ok {
				newDB.Expire(key, raw.(time.Time))
			}
		}
		return true
	})
	server.loadDB(dbIndex, newDB)
	return &protocol.OkReply{}
}
```

- [ ] **Step 4: Add GetUserDBSize and use it in DbSize / INFO keyspace**

```go
// internal/database/server.go — next to GetDBSize:
// GetUserDBSize returns the non-conf key count and ttl count of a database.
// ponytail: O(n) scan to exclude conf keys; switch to an atomic counter if
// DBSIZE ever becomes hot.
func (server *Server) GetUserDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	keys := 0
	db.data.ForEach(func(key string, val interface{}) bool {
		if !IsConfKey(key) {
			keys++
		}
		return true
	})
	return keys, db.ttlMap.Len()
}
```

```go
// internal/database/systemcmd.go — DbSize:
func DbSize(c redis.Connection, db *Server) redis.Reply {
	keys, _ := db.GetUserDBSize(c.GetDBIndex())
	return protocol.MakeIntReply(int64(keys))
}
```

```go
// internal/database/systemcmd.go — INFO keyspace loop:
	for i := 0; i < dbCount; i++ {
		keys, expiresKeys := db.GetUserDBSize(i)
```

- [ ] **Step 5: Fix cluster-mode DBSIZE**

```go
// internal/cluster/core/command.go — dbsize branch:
	if cmdName == "dbsize" {
		if server, ok := cluster.db.(*database.Server); ok {
			dbsize, _ := server.GetUserDBSize(0)
			return protocol.MakeIntReply(int64(dbsize))
		}
		dbsize, _ := cluster.db.GetDBSize(0)
		return protocol.MakeIntReply(int64(dbsize))
	}
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/database/ ./internal/cluster/core/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/database/server.go internal/database/systemcmd.go internal/database/namespace_test.go internal/cluster/core/command.go
git commit -m "feat: FLUSH preserves conf keys, DBSIZE/INFO exclude conf namespace"
```

---

### Task 3: Config reflection helpers (shared entries, setter, rewrite)

**Files:**
- Modify: `internal/config/config.go` (append helpers)
- Test: `internal/config/config_test.go` (append)

**Interfaces:**
- Produces: `func IsNodeLocalParam(name string) bool`, `func SharedConfigEntries() map[string]string`, `func AllParams() map[string]string`, `func SetParameter(name, value string) error`, `func RewriteConfigFile() error` — consumed by Tasks 4 and 5.

- [ ] **Step 1: Write the failing tests (append to config_test.go)**

```go
func TestSharedConfigEntries(t *testing.T) {
	Properties.Bind = "10.0.0.1"
	Properties.AppendOnly = true
	Properties.RequirePass = "s3cret"
	entries := SharedConfigEntries()
	if entries["appendonly"] != "true" {
		t.Errorf("appendonly missing from shared entries: %v", entries)
	}
	if entries["requirepass"] != "s3cret" {
		t.Errorf("requirepass missing from shared entries: %v", entries)
	}
	if _, ok := entries["bind"]; ok {
		t.Error("node-local param bind leaked into shared entries")
	}
	if _, ok := entries["cluster-id"]; ok {
		t.Error("node-local param cluster-id leaked into shared entries")
	}
}

func TestSetParameter(t *testing.T) {
	if err := SetParameter("appendonly", "yes"); err != nil || !Properties.AppendOnly {
		t.Errorf("SetParameter(appendonly, yes) failed: %v", err)
	}
	if err := SetParameter("slowlog-max-len", "42"); err != nil || Properties.SlowLogMaxLen != 42 {
		t.Errorf("SetParameter(slowlog-max-len, 42) failed: %v", err)
	}
	if err := SetParameter("bind", "10.0.0.2"); err == nil {
		t.Error("SetParameter on node-local param must fail")
	}
	if err := SetParameter("no-such-param", "x"); err == nil {
		t.Error("SetParameter on unknown param must fail")
	}
}

func TestRewriteConfigFile(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "godis.toml")
	if err := os.WriteFile(tmp, []byte("[aof]\nappendonly = false\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	configFilePath = tmp
	SetParameter("appendonly", "yes")
	if err := RewriteConfigFile(); err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(tmp)
	if !strings.Contains(string(content), "appendonly = true") {
		t.Errorf("rewrite did not persist new value:\n%s", content)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/ -run 'TestSharedConfigEntries|TestSetParameter|TestRewriteConfigFile' -v`
Expected: compile error — helpers undefined.

- [ ] **Step 3: Implement the helpers**

```go
// internal/config/config.go — append after setTomlKey:

// nodeLocalParams are settings that must stay on the local config file and
// are never replicated through the conf namespace.
var nodeLocalParams = map[string]bool{
	"runid": true, "bind": true, "port": true, "dir": true,
	"announce-host": true, "slave-announce-port": true, "slave-announce-ip": true,
	"use-gnet": true, "data-dir": true,
	"cluster-enable": true, "cluster-as-seed": true, "cluster-seed": true,
	"cluster-join-http": true, "cluster-join-http-token": true, "cluster-id": true,
	"raft-listen-address": true, "raft-advertise-address": true, "master-in-cluster": true,
	"cluster-worker-pool": true, "cluster-relay-parallel": true,
	"cluster-heartbeat-interval": true, "cluster-failover-max-failures": true,
	"cluster-max-version-gap": true, "cluster-sync-via-oss": true, "cluster-snapshot-interval": true,
	"oss-enabled": true, "oss-endpoint": true, "oss-bucket": true,
	"oss-access-key-id": true, "oss-secret-access-key": true, "oss-region": true,
	"oss-prefix": true, "oss-use-ssl": true, "oss-force-path-style": true,
	"http-api-enabled": true, "http-api-port": true, "http-api-host": true,
	"web-port": true,
	"sentinel-enabled": true, "sentinel-port": true, "sentinel-monitors": true,
}

// IsNodeLocalParam reports whether a cfg-tag parameter is node-local.
func IsNodeLocalParam(name string) bool {
	return nodeLocalParams[name]
}

// paramSection maps a cfg-tag parameter name to its TOML section for
// CONFIG REWRITE.
var paramSection = map[string]string{
	"bind": "server", "port": "server", "dir": "server", "databases": "server",
	"maxclients": "server", "runid": "server", "use-gnet": "server", "data-dir": "server",
	"appendonly": "aof", "appendfilename": "aof", "appendfsync": "aof",
	"aof-use-rdb-preamble": "aof", "dbfilename": "aof",
	"requirepass": "security", "masterauth": "security",
	"repl-timeout": "replication", "announce-host": "replication",
	"slave-announce-port": "replication", "slave-announce-ip": "replication",
	"slowlog-log-slower-than": "slowlog", "slowlog-max-len": "slowlog",
	"prometheus-enabled": "monitoring", "prometheus-port": "monitoring",
	"azure-entra-tenant-id": "azure", "azure-entra-app-id": "azure",
	"cluster-enable": "cluster", "cluster-as-seed": "cluster", "cluster-seed": "cluster",
	"cluster-join-http": "cluster", "cluster-join-http-token": "cluster", "cluster-id": "cluster",
	"raft-listen-address": "cluster", "raft-advertise-address": "cluster", "master-in-cluster": "cluster",
	"cluster-worker-pool": "cluster", "cluster-relay-parallel": "cluster",
	"cluster-heartbeat-interval": "cluster", "cluster-failover-max-failures": "cluster",
	"cluster-max-version-gap": "cluster", "cluster-sync-via-oss": "cluster", "cluster-snapshot-interval": "cluster",
	"oss-enabled": "oss", "oss-endpoint": "oss", "oss-bucket": "oss",
	"oss-access-key-id": "oss", "oss-secret-access-key": "oss", "oss-region": "oss",
	"oss-prefix": "oss", "oss-use-ssl": "oss", "oss-force-path-style": "oss",
	"http-api-enabled": "http_api", "http-api-port": "http_api", "http-api-host": "http_api",
	"web-port": "web",
	"sentinel-enabled": "sentinel", "sentinel-port": "sentinel",
}

// allParams returns every cfg-tagged scalar parameter (name -> current value).
func allParams() map[string]string {
	entries := map[string]string{}
	t := reflect.TypeOf(*Properties)
	v := reflect.ValueOf(*Properties)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := f.Tag.Get("cfg")
		if name == "" || name == "-" {
			continue
		}
		if v.Field(i).Kind() == reflect.Slice {
			continue
		}
		entries[name] = fmt.Sprint(v.Field(i).Interface())
	}
	return entries
}

// SharedConfigEntries returns the shared (non node-local) parameters with
// their current values, keyed by cfg-tag name.
func SharedConfigEntries() map[string]string {
	entries := map[string]string{}
	for name, value := range allParams() {
		if !nodeLocalParams[name] {
			entries[name] = value
		}
	}
	return entries
}

// AllParams returns every scalar parameter with its current value,
// including node-local ones.
func AllParams() map[string]string {
	return allParams()
}

// SetParameter updates a shared parameter in Properties by cfg-tag name.
// Node-local and unknown parameters are rejected.
func SetParameter(name, value string) error {
	if nodeLocalParams[name] {
		return fmt.Errorf("CONFIG SET is not supported for node-local parameter '%s'", name)
	}
	t := reflect.TypeOf(*Properties)
	v := reflect.ValueOf(Properties).Elem()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Tag.Get("cfg") != name {
			continue
		}
		field := v.Field(i)
		switch field.Kind() {
		case reflect.String:
			field.SetString(value)
		case reflect.Bool:
			b, err := strconv.ParseBool(value)
			if err != nil {
				if strings.EqualFold(value, "yes") {
					b = true
				} else if strings.EqualFold(value, "no") {
					b = false
				} else {
					return fmt.Errorf("invalid boolean value '%s' for parameter '%s'", value, name)
				}
			}
			field.SetBool(b)
		case reflect.Int, reflect.Int64:
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid integer value '%s' for parameter '%s'", value, name)
			}
			field.SetInt(int64(n))
		default:
			return fmt.Errorf("unsupported parameter type for '%s'", name)
		}
		return nil
	}
	return fmt.Errorf("unknown parameter '%s'", name)
}

// RewriteConfigFile persists the current effective configuration back to
// the active config file, preserving its structure via setTomlKey.
func RewriteConfigFile() error {
	if configFilePath == "" {
		return errors.New("no config file loaded")
	}
	content, err := os.ReadFile(configFilePath)
	if err != nil {
		return err
	}
	t := reflect.TypeOf(*Properties)
	v := reflect.ValueOf(*Properties)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := f.Tag.Get("cfg")
		if name == "" || name == "-" {
			continue
		}
		section, ok := paramSection[name]
		if !ok || v.Field(i).Kind() == reflect.Slice {
			continue
		}
		content = []byte(setTomlKey(string(content), section, f.Tag.Get("toml"), fmt.Sprint(v.Field(i).Interface())))
	}
	return os.WriteFile(configFilePath, content, 0o644)
}
```

Add imports `errors`, `reflect`, `strconv`, `strings` (already imported), `fmt` (already), `os` (already). The new tests in `config_test.go` additionally need `path/filepath` (and `os`, `strings` if not already imported by that file).

- [ ] **Step 4: Run tests**

Run: `go test ./internal/config/ -run 'TestSharedConfigEntries|TestSetParameter|TestRewriteConfigFile' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat: config reflection helpers for conf namespace and CONFIG REWRITE"
```

---

### Task 4: Bootstrap conf namespace at startup

**Files:**
- Create: `internal/database/bootstrap.go`
- Modify: `internal/cluster/core/core.go` (add BootstrapConfNamespace method)
- Modify: `cmd/godis/main.go` (call bootstrap on both server paths)
- Test: `internal/database/namespace_test.go` (append)

**Interfaces:**
- Consumes: `config.SharedConfigEntries()` (Task 3), `ConfNamespacePrefix`/`IsConfKey` (Task 1)
- Produces: `func BootstrapConfNamespace(server *Server)`, `func (c *Cluster) BootstrapConfNamespace()`

- [ ] **Step 1: Write the failing test (append to namespace_test.go)**

```go
func TestBootstrapConfNamespace(t *testing.T) {
	config.Properties.AppendOnly = true
	config.Properties.RequirePass = "pw"
	defer func() {
		config.Properties.AppendOnly = false
		config.Properties.RequirePass = ""
	}()
	server := NewStandaloneServer()
	BootstrapConfNamespace(server)
	db := server.mustSelectDB(0)
	if raw, ok := db.GetEntity("conf:appendonly"); !ok || string(raw.Data.([]byte)) != "true" {
		t.Fatalf("conf:appendonly not bootstrapped: %v %v", ok, raw)
	}
	if raw, ok := db.GetEntity("conf:requirepass"); !ok || string(raw.Data.([]byte)) != "pw" {
		t.Fatalf("conf:requirepass not bootstrapped")
	}
	if _, ok := db.GetEntity("conf:bind"); ok {
		t.Fatal("node-local param bootstrapped into namespace")
	}
	// existing conf keys are never overwritten (conf key wins on later boots)
	db.PutEntity("conf:appendonly", &database.DataEntity{Data: []byte("false")})
	BootstrapConfNamespace(server)
	if raw, _ := db.GetEntity("conf:appendonly"); string(raw.Data.([]byte)) != "false" {
		t.Fatal("bootstrap overwrote existing conf key")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/database/ -run TestBootstrapConfNamespace -v`
Expected: compile error — `BootstrapConfNamespace` undefined.

- [ ] **Step 3: Implement bootstrap**

```go
// internal/database/bootstrap.go
package database

import (
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
)

// BootstrapConfNamespace writes the shared settings from the local config
// into the conf namespace of db0. Keys that already exist (from AOF/RDB
// replay or a previous CONFIG SET) are left untouched — the namespace is
// authoritative on later boots.
func BootstrapConfNamespace(server *Server) {
	db := server.mustSelectDB(0)
	for name, value := range config.SharedConfigEntries() {
		key := ConfNamespacePrefix + name
		if _, exists := db.GetEntity(key); exists {
			continue
		}
		db.PutEntity(key, &database.DataEntity{Data: []byte(value)})
	}
}
```

```go
// internal/cluster/core/core.go — add method:
// BootstrapConfNamespace writes the wrapped server's shared settings into
// the conf namespace (used when the cluster created its own standalone
// server).
func (c *Cluster) BootstrapConfNamespace() {
	if srv, ok := c.db.(*dbimpl.Server); ok {
		dbimpl.BootstrapConfNamespace(srv)
	}
}
```

- [ ] **Step 4: Wire bootstrap into main.go**

```go
// cmd/godis/main.go — add helper:
func bootstrapConfNamespace(db idatabase.DB) {
	switch d := db.(type) {
	case *database.Server:
		database.BootstrapConfNamespace(d)
	case *cluster.Cluster:
		d.BootstrapConfNamespace()
	}
}
```

```go
// cmd/godis/main.go — gnet path, after `db = cluster.MakeCluster(nil)` /
// `db = database.NewStandaloneServer()` and before `srv := gnet.NewGnetServer(db)`:
		bootstrapConfNamespace(db)
```

```go
// cmd/godis/main.go — std path, after `handler := stdserver.MakeHandler()`:
		handler := stdserver.MakeHandler()
		bootstrapConfNamespace(handler.CurrentDB())
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/database/ -run TestBootstrapConfNamespace -v && go build ./cmd/godis/`
Expected: PASS; build succeeds.

- [ ] **Step 6: Commit**

```bash
git add internal/database/bootstrap.go internal/database/namespace_test.go internal/cluster/core/core.go cmd/godis/main.go
git commit -m "feat: bootstrap conf namespace from local config at startup"
```

---

### Task 5: CONFIG GET/SET/REWRITE + cluster forwarding

**Files:**
- Create: `internal/database/config_cmd.go`
- Create: `internal/cluster/core/config_cmd.go`
- Modify: `internal/database/server.go` (route `config` in Exec)
- Test: `internal/database/config_cmd_test.go`
- Test: `internal/cluster/core/config_cmd_test.go`

**Interfaces:**
- Consumes: `config.SetParameter/IsNodeLocalParam/AllParams/RewriteConfigFile` (Task 3), `ConfNamespacePrefix` (Task 1)
- Produces: `execConfig` server handler; cluster command `config` relaying to `PickNode(0)`.

- [ ] **Step 1: Write the failing tests**

```go
// internal/database/config_cmd_test.go
package database

import (
	"testing"

	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

func TestConfigGetSet(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()

	ret := server.Exec(conn, utils.ToCmdLine("CONFIG", "SET", "appendonly", "yes"))
	asserts.AssertNotError(t, ret)
	if !config.Properties.AppendOnly {
		t.Fatal("CONFIG SET did not update Properties")
	}
	raw, ok := server.mustSelectDB(0).GetEntity("conf:appendonly")
	if !ok || string(raw.Data.([]byte)) != "yes" {
		t.Fatal("CONFIG SET did not write conf key")
	}

	ret = server.Exec(conn, utils.ToCmdLine("CONFIG", "GET", "appendonly"))
	asserts.AssertMultiBulkReply(t, ret, []string{"appendonly", "yes"})

	ret = server.Exec(conn, utils.ToCmdLine("CONFIG", "SET", "bind", "10.0.0.1"))
	asserts.AssertErrReply(t, ret, "ERR CONFIG SET is not supported for node-local parameter 'bind'")

	ret = server.Exec(conn, utils.ToCmdLine("CONFIG", "GET", "*"))
	multi, ok := ret.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("CONFIG GET * returned %T", ret)
	}
	found := false
	for _, item := range multi.Args {
		if string(item) == "appendonly" {
			found = true
		}
	}
	if !found {
		t.Fatal("CONFIG GET * missing appendonly")
	}
}
```

```go
// internal/cluster/core/config_cmd_test.go
package core

import (
	"testing"

	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

func TestConfigForwardedToConfSlotOwner(t *testing.T) {
	nodes := MakeTestCluster([]string{"node0", "node1"})
	// pickNodeImpl maps slot % 2 -> node, so slot 0 is owned by node0
	conn := connection.NewFakeConn()
	ret := nodes["node1"].Exec(conn, utils.ToCmdLine("CONFIG", "SET", "requirepass", "hunter2"))
	asserts.AssertNotError(t, ret)

	ownerDB := nodes["node0"].db.(*dbimpl.Server)
	raw, ok := ownerDB.mustSelectDB(0).GetEntity("conf:requirepass")
	if !ok || string(raw.Data.([]byte)) != "hunter2" {
		t.Fatalf("conf key not applied on slot-0 owner: ok=%v raw=%v", ok, raw)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/database/ ./internal/cluster/core/ -run 'TestConfigGetSet|TestConfigForwardedToConfSlotOwner' -v`
Expected: FAIL — `ERR unknown command 'config'` on both.

- [ ] **Step 3: Implement the server-side CONFIG command**

```go
// internal/database/config_cmd.go
package database

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// execConfig handles CONFIG GET/SET/REWRITE. In cluster mode the cluster
// layer forwards it to the conf slot owner before it reaches this handler.
func execConfig(server *Server, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("config")
	}
	sub := strings.ToLower(string(args[0]))
	switch sub {
	case "get":
		if len(args) != 2 {
			return protocol.MakeArgNumErrReply("config|get")
		}
		return execConfigGet(server, args[1])
	case "set":
		if len(args) != 3 {
			return protocol.MakeArgNumErrReply("config|set")
		}
		return execConfigSet(server, string(args[1]), string(args[2]))
	case "rewrite":
		if len(args) != 1 {
			return protocol.MakeArgNumErrReply("config|rewrite")
		}
		if err := config.RewriteConfigFile(); err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		return &protocol.OkReply{}
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'", sub))
	}
}

func execConfigGet(server *Server, param []byte) redis.Reply {
	db := server.mustSelectDB(0)
	pattern := string(param)
	names := make([]string, 0, len(config.AllParams()))
	for name := range config.AllParams() {
		if pattern == "*" || strings.EqualFold(name, pattern) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	result := make([][]byte, 0, len(names)*2)
	for _, name := range names {
		value := config.AllParams()[name]
		if !config.IsNodeLocalParam(name) {
			if raw, ok := db.GetEntity(ConfNamespacePrefix + name); ok {
				if b, ok := raw.Data.([]byte); ok {
					value = string(b)
				}
			}
		}
		result = append(result, []byte(name), []byte(value))
	}
	return protocol.MakeMultiBulkReply(result)
}

func execConfigSet(server *Server, param, value string) redis.Reply {
	if config.IsNodeLocalParam(param) {
		return protocol.MakeErrReply(fmt.Sprintf(
			"ERR CONFIG SET is not supported for node-local parameter '%s'", param))
	}
	if err := config.SetParameter(param, value); err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	server.mustSelectDB(0).PutEntity(ConfNamespacePrefix+param,
		&database.DataEntity{Data: []byte(value)})
	server.AddAof(0, utils.ToCmdLine3("config", "set", param, value))
	return &protocol.OkReply{}
}
```

```go
// internal/database/server.go — in Exec, after the "copy" branch:
	} else if cmdName == "copy" {
		...
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "config" {
		return execConfig(server, c, cmdLine[1:])
	} else if cmdName == "replconf" {
```

- [ ] **Step 4: Implement the cluster forwarding command**

```go
// internal/cluster/core/config_cmd.go
package core

import (
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
)

// confSlotID is the fixed slot that hosts the conf namespace. It is owned
// by the seed and never migrated (see makeRebalancePlan).
const confSlotID uint32 = 0

func init() {
	RegisterCmd("config", execConfigCluster)
}

// execConfigCluster serves CONFIG on the conf slot owner: reads and writes
// are relayed to PickNode(confSlotID) unless this node owns the slot.
func execConfigCluster(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	peer := cluster.PickNode(confSlotID)
	if peer == cluster.SelfID() {
		return cluster.db.Exec(c, cmdLine)
	}
	return cluster.Relay(peer, c, cmdLine)
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/database/ ./internal/cluster/core/ -run 'TestConfigGetSet|TestConfigForwardedToConfSlotOwner' -v`
Expected: PASS.

- [ ] **Step 6: Run full package suites (regression)**

Run: `go test ./internal/database/ ./internal/cluster/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/database/config_cmd.go internal/database/config_cmd_test.go internal/database/server.go internal/cluster/core/config_cmd.go internal/cluster/core/config_cmd_test.go
git commit -m "feat: CONFIG GET/SET/REWRITE wired to conf namespace with cluster forwarding"
```

---

### Task 6: `--join` CLI flag

**Files:**
- Modify: `cmd/godis/main.go`
- Test: Create `cmd/godis/main_test.go`

**Interfaces:**
- Consumes: `config.Properties` fields `ClusterEnable`, `ClusterSeed`, `ClusterJoinHTTP`.
- Produces: `func parseJoinFlag(args []string) string` — returns the join target or "".

- [ ] **Step 1: Write the failing test**

```go
// cmd/godis/main_test.go
package main

import "testing"

func TestParseJoinFlag(t *testing.T) {
	if got := parseJoinFlag([]string{"--cli"}); got != "" {
		t.Errorf("no --join, got %q", got)
	}
	if got := parseJoinFlag([]string{"--join", "127.0.0.1:6379"}); got != "127.0.0.1:6379" {
		t.Errorf("space form failed: %q", got)
	}
	if got := parseJoinFlag([]string{"--join=http://127.0.0.1:63790"}); got != "http://127.0.0.1:63790" {
		t.Errorf("equals form failed: %q", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/godis/ -run TestParseJoinFlag -v`
Expected: compile error — `parseJoinFlag` undefined.

- [ ] **Step 3: Implement flag parsing and wiring**

```go
// cmd/godis/main.go — replace the hidden-flags loop:
	// Check for hidden flags before any other setup
	joinTarget := parseJoinFlag(os.Args[1:])
	for _, arg := range os.Args[1:] {
		if arg == "--cli" {
			runCLI()
			return
		}
		if arg == "--web" {
			startWebDashboard()
			return
		}
		if arg == "--migrate" {
			runMigrate()
			return
		}
	}
```

```go
// cmd/godis/main.go — new function:
// parseJoinFlag extracts the --join <addr> / --join=<addr> target from the
// command line. Returns "" when absent.
func parseJoinFlag(args []string) string {
	for i := 0; i < len(args); i++ {
		if args[i] == "--join" && i+1 < len(args) {
			return args[i+1]
		}
		if len(args[i]) > len("--join=") && args[i][:len("--join=")] == "--join=" {
			return args[i][len("--join="):]
		}
	}
	return ""
}
```

```go
// cmd/godis/main.go — after the config setup block (after the
// `config.Properties = defaultProperties` else-branch):
	// --join: switch this node into cluster mode and join the given seed.
	// host:port uses the TCP join channel; http(s)://host:port uses the
	// seed's HTTP API. The seed auto-promotes on receiving the join.
	if joinTarget != "" {
		config.Properties.ClusterEnable = true
		switch {
		case strings.HasPrefix(joinTarget, "http://"):
			config.Properties.ClusterJoinHTTP = strings.TrimPrefix(joinTarget, "http://")
		case strings.HasPrefix(joinTarget, "https://"):
			// ponytail: httpJoinCluster hardcodes http://; https joins
			// work only if the seed terminates TLS in front of the API.
			config.Properties.ClusterJoinHTTP = strings.TrimPrefix(joinTarget, "https://")
		default:
			config.Properties.ClusterSeed = joinTarget
		}
	}
```

Add `"strings"` to main.go imports.

- [ ] **Step 4: Run test**

Run: `go test ./cmd/godis/ -run TestParseJoinFlag -v && go build ./cmd/godis/`
Expected: PASS; build succeeds.

- [ ] **Step 5: Commit**

```bash
git add cmd/godis/main.go cmd/godis/main_test.go
git commit -m "feat: --join CLI flag for TCP or HTTP cluster join"
```

---

### Task 7: Auto-promote — PromoteToSeed + cluster.join intercept

**Files:**
- Modify: `internal/cluster/hotswitch.go`
- Create: `internal/database/join_hook.go`
- Modify: `internal/database/server.go` (route `cluster.join` in Exec)
- Modify: `cmd/godis/main.go` (register join intercept on both paths)
- Test: `internal/cluster/hotswitch_test.go`

**Interfaces:**
- Consumes: `DBSwapper` (existing), `config.WriteBackClusterID` (existing), `MakeCluster` (existing).
- Produces: `func RegisterJoinIntercept(swapper DBSwapper)`, `func promoteToSeed(swapper DBSwapper, asSeed bool) error`.

- [ ] **Step 1: Write the failing test**

```go
// internal/cluster/hotswitch_test.go
package cluster

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/Hoverhuang-er/godis/internal/config"
	dbimpl "github.com/Hoverhuang-er/godis/internal/database"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

type fakeSwapper struct{ db database.DB }

func (f *fakeSwapper) CurrentDB() database.DB  { return f.db }
func (f *fakeSwapper) SwapDB(db database.DB)   { f.db = db }

func TestPromoteToSeed(t *testing.T) {
	config.Properties.Dir = t.TempDir()
	config.Properties.ClusterHeartbeatInterval = 1 // fast raft election
	config.Properties.ClusterSyncViaOss = false
	swapper := &fakeSwapper{db: dbimpl.NewStandaloneServer()}

	if err := promoteToSeed(swapper, true); err != nil {
		t.Fatal(err)
	}
	if _, ok := swapper.CurrentDB().(*Cluster); !ok {
		t.Fatalf("db not swapped to cluster, got %T", swapper.CurrentDB())
	}
	if config.Properties.ClusterID == "" {
		t.Fatal("cluster_id not generated")
	}
	if err := promoteToSeed(swapper, true); !errors.Is(err, errAlreadyInCluster) {
		t.Fatalf("second promote must fail, got %v", err)
	}
}

func TestJoinInterceptPromotesStandalone(t *testing.T) {
	config.Properties.Dir = t.TempDir()
	config.Properties.ClusterHeartbeatInterval = 1
	config.Properties.ClusterSyncViaOss = false
	config.Properties.ClusterID = ""
	server := dbimpl.NewStandaloneServer()
	swapper := &fakeSwapper{db: server}
	RegisterJoinIntercept(swapper)

	conn := connection.NewFakeConn()
	ret := server.Exec(conn, utils.ToCmdLine("cluster.join", "127.0.0.1:6390", "127.0.0.1:16666"))
	if protocol.IsErrorReply(ret) {
		t.Fatalf("cluster.join failed: %s", ret.ToBytes())
	}
	if Current() == nil {
		t.Fatal("node not promoted to cluster")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cluster/ -run 'TestPromoteToSeed|TestJoinInterceptPromotesStandalone' -v`
Expected: compile error — `promoteToSeed`, `errAlreadyInCluster`, `RegisterJoinIntercept` undefined.

- [ ] **Step 3: Add the join hook to the database package**

```go
// internal/database/join_hook.go
package database

import (
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
)

// clusterJoinHandler is registered by the cluster package to auto-promote a
// standalone node when it receives a cluster.join command (the first join
// of a new cluster). Kept as a callback to avoid an import cycle.
var clusterJoinHandler func(c redis.Connection, cmdLine [][]byte) redis.Reply

// RegisterClusterJoinHandler registers the cluster.join handler used when
// this node runs standalone.
func RegisterClusterJoinHandler(fn func(c redis.Connection, cmdLine [][]byte) redis.Reply) {
	clusterJoinHandler = fn
}
```

```go
// internal/database/server.go — in Exec, after the "replconf"/"psync"
// branches:
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	} else if cmdName == "cluster.join" && clusterJoinHandler != nil {
		return clusterJoinHandler(c, cmdLine)
	}
```

- [ ] **Step 4: Refactor hotswitch.go**

```go
// internal/cluster/hotswitch.go — replace hotSwitch and add promoteToSeed:

// hotSwitched guards against double-switching when the config reload
// triggered by our own cluster_id write-back fires.
var hotSwitched bool

// errAlreadyInCluster is returned when a node is promoted twice.
var errAlreadyInCluster = errors.New("already in cluster")

// promoteMu serializes concurrent promotion attempts (e.g. two joiners
// racing a standalone seed).
var promoteMu sync.Mutex

// promoteToSeed switches a running standalone server into cluster mode.
// asSeed=true makes this node the cluster seed (generates cluster_id and
// bootstraps raft); asSeed=false joins the configured seed instead.
func promoteToSeed(swapper DBSwapper, asSeed bool) error {
	promoteMu.Lock()
	defer promoteMu.Unlock()
	if hotSwitched {
		return errAlreadyInCluster
	}
	config.Properties.ClusterEnable = true
	config.Properties.ClusterAsSeed = asSeed
	// generate and persist the cluster id before building the cluster so
	// join handshakes and the OSS object layout both use it
	if config.Properties.ClusterID == "" {
		config.Properties.ClusterID = utils.RandHexString(16)
		config.WriteBackClusterID(config.Properties.ClusterID)
	}
	current := swapper.CurrentDB()
	server, ok := current.(*dbimpl.Server)
	if !ok {
		return fmt.Errorf("cannot promote: current db is %T, expected standalone server", current)
	}
	logger.Infof("hot-switching standalone node %s to cluster mode, cluster_id=%s",
		config.Properties.AnnounceAddress(), config.Properties.ClusterID)
	cl := MakeCluster(server)
	swapper.SwapDB(cl)
	hotSwitched = true
	logger.Infof("hot-switch complete: node is now a cluster node")
	return nil
}

// hotSwitch is the config-reload-triggered variant of promoteToSeed.
func hotSwitch(swapper DBSwapper) {
	if hotSwitched {
		return
	}
	if !config.Properties.ClusterEnable {
		return
	}
	if err := promoteToSeed(swapper, config.Properties.ClusterAsSeed); err != nil && !errors.Is(err, errAlreadyInCluster) {
		logger.Errorf("hot-switch failed: %v", err)
	}
}

// RegisterJoinIntercept wires the standalone cluster.join intercept: the
// first join request to this node promotes it to seed and hands the command
// to the new cluster.
func RegisterJoinIntercept(swapper DBSwapper) {
	dbimpl.RegisterClusterJoinHandler(func(c redis.Connection, cmdLine [][]byte) redis.Reply {
		if err := promoteToSeed(swapper, true); err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		cl := Current()
		return cl.Exec(c, cmdLine)
	})
}
```

New imports for hotswitch.go: `errors`, `fmt`, `sync`, `github.com/Hoverhuang-er/godis/internal/interface/redis`, `github.com/Hoverhuang-er/godis/internal/redis/protocol`.

- [ ] **Step 5: Wire the intercept into main.go**

```go
// cmd/godis/main.go — std path, next to RegisterHotSwitch:
		cluster.RegisterHotSwitch(handler)
		cluster.RegisterJoinIntercept(handler)
```

```go
// cmd/godis/main.go — gnet path, next to RegisterHotSwitch:
		cluster.RegisterHotSwitch(srv)
		cluster.RegisterJoinIntercept(srv)
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/cluster/ -run 'TestPromoteToSeed|TestJoinInterceptPromotesStandalone' -v`
Expected: PASS. (Raft election with 1s heartbeat may take a few seconds — that is expected.)

- [ ] **Step 7: Run cluster suites (regression)**

Run: `go test ./internal/cluster/... ./internal/database/`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/cluster/hotswitch.go internal/cluster/hotswitch_test.go internal/database/join_hook.go internal/database/server.go cmd/godis/main.go
git commit -m "feat: standalone auto-promote to seed on first cluster join"
```

---

### Task 8: Raft slot-0 pinning for conf keys

**Files:**
- Modify: `internal/cluster/core/utils.go:87-90` (defaultGetSlotImpl)
- Modify: `internal/cluster/core/node_manager.go:206-245` (makeRebalancePlan)
- Test: `internal/cluster/core/utils_test.go` (create)

**Interfaces:**
- Consumes: `database.IsConfKey` (Task 1), `confSlotID` (Task 5).
- Produces: conf keys hash to slot 0; rebalance never migrates slot 0.

- [ ] **Step 1: Write the failing tests**

```go
// internal/cluster/core/utils_test.go
package core

import (
	"testing"

	"github.com/Hoverhuang-er/godis/internal/cluster/raft"
)

func TestConfKeysPinToSlotZero(t *testing.T) {
	cluster := &Cluster{}
	for _, key := range []string{"conf:requirepass", "conf:appendonly"} {
		if got := defaultGetSlotImpl(cluster, key); got != confSlotID {
			t.Errorf("conf key %s routed to slot %d, want %d", key, got, confSlotID)
		}
	}
	// regular keys still hash normally
	if got := defaultGetSlotImpl(cluster, "user-key"); got == confSlotID && got == 0 {
		t.Errorf("suspicious: user key hashed to slot 0")
	}
}

func TestRebalancePlanExcludesConfSlot(t *testing.T) {
	// 8193 slots on n0 (> avgSlot+1 for 2 nodes), none on n1: n0 exports.
	slots := make([]uint32, 0, SlotCount)
	for i := uint32(0); i < SlotCount; i++ {
		slots = append(slots, i)
	}
	fsm := &raft.FSM{
		Node2Slot:    map[string][]uint32{"n0": slots, "n1": {}},
		MasterSlaves: map[string]*raft.MasterSlave{"n0": {}, "n1": {}},
	}
	cluster := &Cluster{raftNode: &raft.Node{FSM: fsm}}
	tasks, err := cluster.makeRebalancePlan()
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		for _, slot := range task.Slots {
			if slot == confSlotID {
				t.Fatalf("rebalance plan migrates conf slot %d", confSlotID)
			}
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cluster/core/ -run 'TestConfKeysPinToSlotZero|TestRebalancePlanExcludesConfSlot' -v`
Expected: FAIL — conf keys hash to arbitrary slots; slot 0 included in plan.

- [ ] **Step 3: Pin conf keys to slot 0**

```go
// internal/cluster/core/utils.go — defaultGetSlotImpl:
func defaultGetSlotImpl(cluster *Cluster, key string) uint32 {
	partitionKey := GetPartitionKey(key)
	if dbimpl.IsConfKey(partitionKey) {
		return confSlotID
	}
	return crc32.ChecksumIEEE([]byte(partitionKey)) % uint32(SlotCount)
}
```

Add `dbimpl "github.com/Hoverhuang-er/godis/internal/database"` to utils.go imports.

- [ ] **Step 4: Exclude slot 0 from rebalance**

```go
// internal/cluster/core/node_manager.go — makeRebalancePlan, replace the
// exportSlots construction inside the loop:
			if len(exportSlots) == 0 {
				exportNodeSlots := fsm.Node2Slot[exportNode]
				exportCount := len(exportNodeSlots) - avgSlot
				for _, s := range exportNodeSlots {
					if s == confSlotID {
						continue // conf namespace slot never migrates
					}
					exportSlots = append(exportSlots, s)
					if len(exportSlots) >= exportCount {
						break
					}
				}
				if len(exportSlots) == 0 {
					exportIndex++
					continue
				}
			}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/cluster/core/ -run 'TestConfKeysPinToSlotZero|TestRebalancePlanExcludesConfSlot' -v`
Expected: PASS.

- [ ] **Step 6: Run cluster core suite (regression)**

Run: `go test ./internal/cluster/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/cluster/core/utils.go internal/cluster/core/utils_test.go internal/cluster/core/node_manager.go
git commit -m "feat: pin conf namespace to slot 0, exclude from rebalance"
```

---

### Task 9: Integration — conf sync across nodes

**Files:**
- Create: `internal/database/conf_sync_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1-8.

- [ ] **Step 1: Write the failing test**

```go
// internal/database/conf_sync_test.go
package database

import (
	"os"
	"testing"
	"time"

	"github.com/Hoverhuang-er/godis/internal/aof"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

// TestConfKeyReplicatesToSlave proves the conf namespace rides the existing
// master-slave full sync: a CONFIG SET on the master lands on the slave.
func TestConfKeyReplicatesToSlave(t *testing.T) {
	config.Properties.AppendOnly = true
	config.Properties.Databases = 16
	master := mockServer()
	defer func() {
		config.Properties.AppendOnly = false
	}()

	aofFile, err := os.CreateTemp(config.GetTmpDir(), "master-*.aof")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(aofFile.Name())
	config.Properties.AppendFilename = aofFile.Name()
	config.Properties.AppendFsync = aof.FsyncAlways
	aofHandler, err := NewPersister(master, config.Properties.AppendFilename, true, config.Properties.AppendFsync)
	if err != nil {
		t.Fatal(err)
	}
	defer aofHandler.Close()

	masterConn := connection.NewFakeConn()
	ret := master.Exec(masterConn, utils.ToCmdLine("CONFIG", "SET", "maxclients", "999"))
	asserts.AssertNotError(t, ret)
	time.Sleep(100 * time.Millisecond) // wait for AOF write

	// slave: fresh server, full sync from master via PSYNC
	slave := mockServer()
	slaveConn := connection.NewFakeConn()
	replConn := connection.NewFakeConn()
	master.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)
	go slave.parsePsyncHandshake()
	// drive the handshake/rdb/aof flow like replication_slave_test.go does;
	// after loadMasterRDB the slave must hold conf:maxclients
	payload := <-masterChan
	if payload.Err != nil {
		t.Fatal(payload.Err)
	}
	slave.loadMasterRDB(0)
	if raw, ok := slave.mustSelectDB(0).GetEntity("conf:maxclients"); !ok || string(raw.Data.([]byte)) != "999" {
		t.Fatalf("conf key missing on slave after full sync: ok=%v", ok)
	}
	_ = slaveConn
}
```

> Note: if `parsePsyncHandshake`'s signature differs from the assumption
> above, follow the exact handshake sequence in
> `internal/database/replication_slave_test.go:257-310` (the authoritative
> pattern for driving a full sync in a test).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/database/ -run TestConfKeyReplicatesToSlave -v`
Expected: FAIL — `ERR unknown command 'config'` (before Task 5) or missing conf key.

- [ ] **Step 3: Make it pass**

The test passes once Tasks 1-5 are complete (CONFIG SET writes the conf key with AOF logging; full sync copies it). Fix any test-harness mismatches against `replication_slave_test.go`'s handshake pattern — the production code under test should not need changes.

- [ ] **Step 4: Run test**

Run: `go test ./internal/database/ -run TestConfKeyReplicatesToSlave -v`
Expected: PASS.

- [ ] **Step 5: Manual E2E smoke (documented, not automated)**

```bash
# terminal 1 — seed (auto-promotes on first join)
CONFIG=config/standalone.toml ./godis &
# terminal 2 — joiner over TCP
./godis --join 127.0.0.1:6379
# verify: seed INFO shows cluster_enabled:1, both nodes serve conf: keys
redis-cli -p 6379 config get maxclients
redis-cli -p 6380 config get maxclients   # second node, same value
```

- [ ] **Step 6: Full regression**

Run: `go test ./...`
Expected: PASS (all suites).

- [ ] **Step 7: Commit**

```bash
git add internal/database/conf_sync_test.go
git commit -m "test: conf namespace replicates across master-slave full sync"
```

---

## Self-Review Notes

- **Spec coverage:** §4.1 (Task 1, 2), §4.2 (Task 3, 4), §4.3 (Task 6, 7), §4.4 (Task 8, 9), §5 components (all), §6 errors (guard in Tasks 1/5, promote races in Task 7, redirect in Task 8), §7 testing (Tasks 1-9). Out-of-scope items untouched.
- **Placeholders:** no TBD/TODO; the one test-harness note in Task 9 points at an existing authoritative file rather than deferring work.
- **Type consistency:** `IsConfKey`/`ConfNamespacePrefix` defined Task 1, used 4/5/8; `confSlotID` defined Task 5, used Task 8 (same package); `promoteToSeed(swapper, asSeed bool)` defined and used within Task 7; `execConfig` wired in Task 5 Step 3, exercised by cluster command in same task. `GetUserDBSize` defined Task 2, used by cluster dbsize in same task. `mockServer` is an existing helper in `internal/database` (used by replication tests).

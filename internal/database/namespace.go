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

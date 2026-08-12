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

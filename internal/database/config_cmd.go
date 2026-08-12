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
	values := make(map[string]string, len(config.AllParams()))
	for name, value := range config.AllParams() {
		if pattern != "*" && !strings.EqualFold(name, pattern) {
			continue
		}
		names = append(names, name)
		values[name] = value
	}
	sort.Strings(names)
	result := make([][]byte, 0, len(names)*2)
	for _, name := range names {
		value := values[name]
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
	server.AddAof(0, utils.ToCmdLine3("config", []byte("set"), []byte(param), []byte(value)))
	return &protocol.OkReply{}
}

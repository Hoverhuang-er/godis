# Godis

![license](https://img.shields.io/github/license/Hoverhuang-er/godis)
[![Build Status](https://github.com/Hoverhuang-er/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/Hoverhuang-er/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/Hoverhuang-er/godis/badge.svg?branch=master)](https://coveralls.io/github/Hoverhuang-er/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Hoverhuang-er/godis)](https://goreportcard.com/report/github.com/Hoverhuang-er/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[English](https://github.com/Hoverhuang-er/godis/blob/master/docs/README.md) | [日本語](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_JA.md) | [Suomi](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_FI.md)

Godis 是一个用 Go 语言实现的 Redis 服务器。本项目旨在为尝试使用 Go 语言开发高并发中间件的朋友提供一些参考。

关键功能:
- Redis 8.8.0 命令兼容
- 支持 string, list, hash, set, sorted set, bitmap 数据结构
- RediSearch 搜索 (FT.CREATE, FT.SEARCH, FT.DROPINDEX 等)
- Time Series 时序数据 (TS.CREATE, TS.ADD, TS.GET, TS.RANGE 等)
- Redis-Vector 向量搜索 (VECTOR 字段类型, KNN 搜索)
- 并行内核，提供更优秀的性能
- 自动过期功能(TTL)
- 发布订阅
- 地理位置
- AOF 持久化、RDB 持久化、aof-use-rdb-preamble 混合持久化
- 主从复制
- Multi 命令开启的事务具有**原子性**和隔离性. 若在执行过程中遇到错误, godis 会回滚已执行的命令
- 内置集群模式. 集群对客户端是透明的, 您可以像使用单机版 redis 一样使用 godis 集群
  - 使用 Raft 算法维护集群元数据。支持动态扩缩容、自动平衡和主从切换。
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX`  命令在集群模式下原子性执行, 允许 key 在集群的不同节点上

可以在[我的博客](https://www.cnblogs.com/Finley/category/1598973.html)了解更多关于
Godis 的信息。

# 运行 Godis

在 GitHub 的 release 页下载 Darwin(MacOS) 和 Linux 版可执行文件。使用命令行启动 Godis 服务器

```bash
./godis-darwin
```

```bash
./godis-linux
```

![](https://i.loli.net/2021/05/15/oQM1yZ6pWm3AIEj.png)
godis 首先会从CONFIG环境变量中读取配置文件路径。若环境变量中未设置配置文件路径，则会尝试读取工作目录中的 standalone.toml（或 redis.conf）文件。 

所有配置项均在 [standalone.toml](./standalone.toml) 和 [example.conf](./example.conf) 中作了说明。

## 集群模式

可以使用 node1.conf 和 node2.conf 配置文件，在本地启动一个双节点集群:

```bash
CONFIG=node1.conf ./godis-darwin &
CONFIG=node2.conf ./godis-darwin &
```

集群模式对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据：

```bash
redis-cli -p 6399
```

更多配置请查阅 [example.conf](./example.conf)

## Prometheus 监控

Godis 默认启用 Prometheus 兼容的指标端点，监听端口 `9121`（可通过 `standalone.toml` 中的 `monitoring.prometheus_port` 配置），路径为 `/metrics`。指标命名兼容 `redis_exporter`，可直接对接现有 Redis 监控面板。

```bash
# 默认指标抓取地址
curl http://localhost:9121/metrics
```

**主要指标：**
- `godis_connected_clients` — 当前活跃连接数
- `godis_commands_total` — 累计处理命令数
- `godis_keyspace_hits_total` / `godis_keyspace_misses_total` — 缓存命中/未命中计数
- `godis_db_keys` — 各数据库的键数量
- `godis_db_avg_ttl_seconds` — 各数据库平均 TTL
- `godis_slowlog_length` — 慢查询日志队列长度
- 热键和大键检测（定期采样）

如需禁用，在配置文件的 `[monitoring]` 段设置 `prometheus_enabled = false`。所有监控配置支持热更新。

```toml
[monitoring]
prometheus_enabled = true
prometheus_port = 9121
```

## Rueidis 客户端示例

[Rueidis](https://github.com/redis/rueidis) 是一个高性能 Go Redis 客户端。以下是在 Godis 中使用 Rueidis 的示例：

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
		InitAddress: []string{"localhost:6399"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// SET/GET 示例
	err = client.Do(ctx, client.B().Set().Key("foo").Value("bar").Build()).Error()
	if err != nil {
		log.Fatal(err)
	}

	val, err := client.Do(ctx, client.B().Get().Key("foo").Build()).ToString()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GET foo = %s\n", val)

	// RediSearch 示例
	// 需要先通过 FT.CREATE 创建索引
	result, err := client.Do(ctx, client.B().FtSearch().Index("idx").Query("@field:val").Build()).ToArray()
	if err != nil {
		log.Printf("搜索说明: %v (请先通过 FT.CREATE 创建索引)", err)
	}
	_ = result

	// Time Series 示例
	err = client.Do(ctx, client.B().TsAdd().Key("ts:temp").Timestamp(1).Value(25.5).Build()).Error()
	if err != nil {
		log.Printf("时序数据说明: %v", err)
	}
}
```

## 支持的命令

请参考 [commands.md](https://github.com/Hoverhuang-er/godis/blob/master/commands.md)

## 性能测试

环境:

Go version: 1.23
System: MacOS Monterey 12.5 M2 Air

redis-benchmark 测试结果:

```
PING_INLINE: 179211.45 requests per second, p50=1.031 msec                    
PING_MBULK: 173611.12 requests per second, p50=1.071 msec                    
SET: 158478.61 requests per second, p50=1.535 msec                    
GET: 156985.86 requests per second, p50=1.127 msec                    
INCR: 164473.69 requests per second, p50=1.063 msec                    
LPUSH: 151285.92 requests per second, p50=1.079 msec                    
RPUSH: 176678.45 requests per second, p50=1.023 msec                    
LPOP: 177619.89 requests per second, p50=1.039 msec                    
RPOP: 172413.80 requests per second, p50=1.039 msec                    
SADD: 159489.64 requests per second, p50=1.047 msec                    
HSET: 175131.36 requests per second, p50=1.031 msec                    
SPOP: 170648.45 requests per second, p50=1.031 msec                    
ZADD: 165289.25 requests per second, p50=1.039 msec                    
ZPOPMIN: 185528.77 requests per second, p50=0.999 msec                    
LPUSH (needed to benchmark LRANGE): 172117.05 requests per second, p50=1.055 msec                    
LRANGE_100 (first 100 elements): 46511.62 requests per second, p50=4.063 msec                   
LRANGE_300 (first 300 elements): 21217.91 requests per second, p50=9.311 msec                     
LRANGE_500 (first 500 elements): 13331.56 requests per second, p50=14.407 msec                    
LRANGE_600 (first 600 elements): 11153.25 requests per second, p50=17.007 msec                    
MSET (10 keys): 88417.33 requests per second, p50=3.687 msec  
```

## 如何阅读源码

项目遵循 [Go Project Layout](https://github.com/golang-standards/project-layout) 标准组织代码：

```
godis/
├── cmd/                          # 程序入口
│   ├── godis/main.go             # Godis 服务器 — 单机/集群模式
│   ├── godis/cli.go              # 内置 redis-cli（--cli 标志）
│   └── operator/main.go          # Kubernetes Operator
├── internal/                     # 内部实现
│   ├── config/                   # TOML 配置（viper 热加载，嵌入默认配置）
│   ├── tcp/                      # TCP 服务器 — 连接管理、每个连接一个 goroutine
│   ├── redis/                    # Redis 协议实现
│   │   ├── parser/               # RESP2/RESP3 解析器（流式、零拷贝）
│   │   ├── protocol/             # 回复类型（Bulk、MultiBulk、Error、Integer 等）
│   │   ├── server/               # 服务器适配器
│   │   │   ├── std/              #   标准 net.TCP listener
│   │   │   └── gnet/             #   gnet 事件循环（更高吞吐）
│   │   ├── client/               # 集群模式下的节点间通信客户端
│   │   └── connection/           # 连接状态（DB 索引、认证、事务）
│   ├── interface/                # 核心接口定义（存储引擎、连接、回复）
│   ├── database/                 # 存储引擎与命令处理器
│   │   ├── server.go             # 多数据库服务器（AOF、复制、慢查询）
│   │   ├── database.go           # 单数据库核心（数据访问、TTL、锁）
│   │   ├── router.go             # 命令表 — 注册与路由
│   │   ├── string.go             # GET、SET、INCR、APPEND、GETBIT 等
│   │   ├── hash.go               # HSET、HGET、HDEL、HGETALL 等
│   │   ├── list.go               # LPUSH、LRANGE、LINDEX、LTRIM 等
│   │   ├── set.go                # SADD、SMEMBERS、SINTER、SUNION 等
│   │   ├── sortedset.go          # ZADD、ZRANGE、ZRANK、ZSCORE 等
│   │   ├── stream.go             # XADD、XREAD、XGROUP、XACK 等
│   │   ├── geo.go                # GEOADD、GEOSEARCH、GEODIST 等
│   │   ├── keys.go               # DEL、EXISTS、EXPIRE、TTL、TYPE 等
│   │   ├── transaction.go        # MULTI、EXEC、WATCH — 原子隔离事务
│   │   ├── persistence.go        # RDB 文件加载
│   │   ├── timeseries.go         # TS.CREATE、TS.ADD、TS.GET、TS.RANGE
│   │   ├── search.go             # FT.CREATE、FT.SEARCH、FT.DROPINDEX、FT.INFO
│   │   ├── json.go               # JSON.SET、JSON.GET、JSON.DEL、JSON.ARRAPPEND 等
│   │   ├── bloom.go              # BF.ADD、BF.EXISTS、BF.RESERVE、BF.MADD
│   │   ├── hyperloglog.go        # PFADD、PFCOUNT、PFMERGE
│   │   ├── topk.go               # TOPK.ADD、TOPK.QUERY、TOPK.LIST
│   │   ├── cms.go                # CMS.INCRBY、CMS.QUERY、CMS.MERGE
│   │   ├── tdigest.go            # TDIGEST.ADD、TDIGEST.QUANTILE
│   │   ├── bitfield.go           # BITFIELD、BITFIELD_RO
│   │   └── array.go              # AR.SET、AR.GET、AR.APPEND、AR.POP
│   ├── aof/                      # AOF 持久化与重写
│   ├── pubsub/                   # 发布/订阅通道管理
│   ├── cluster/                  # 集群模式
│   │   ├── core/                 # 槽路由、TCC 分布式事务、迁移
│   │   ├── commands/             # 集群感知命令（DEL、MSET、RENAME 通过 TCC）
│   │   └── raft/                 # Raft 共识 — 元数据管理、故障切换
│   ├── monitoring/               # Prometheus /metrics 端点（兼容 redis_exporter）
│   ├── auth/entraid/             # Entra ID JWT 令牌验证（Azure AD）
│   ├── datastruct/               # 底层数据结构实现
│   │   ├── dict/                 # 并发哈希表（分段锁）
│   │   ├── list/                 # Quicklist（链表分段）
│   │   ├── set/                  # 哈希集合
│   │   ├── sortedset/            # 跳表
│   │   ├── bitmap/               # 位图
│   │   ├── stream/               # 基于基数树的流
│   │   ├── search/               # 倒排索引（词 → 文档、评分）
│   │   ├── hyperloglog/          # 基数估计（2^14 寄存器）
│   │   ├── bloom/                # 布隆过滤器（k 哈希、最优 m）
│   │   ├── cms/                  # Count-min Sketch（频率估计）
│   │   ├── topk/                 # Top-K 频繁项
│   │   ├── tdigest/              # T-Digest（分位数估计）
│   │   ├── timeseries/           # 时序数据点
│   │   ├── array/                # 稀疏索引数组
│   │   └── lock/                 # 键级读写锁管理器
│   └── lib/                      # 工具库
│       ├── logger/               # 结构化文件日志
│       ├── pool/                 # 通用对象池
│       ├── timewheel/            # 时间轮 — 过期与定时任务
│       ├── wildcard/             # 通配符匹配
│       ├── consistenthash/       # 一致性哈希环
│       ├── idgenerator/          # Snowflake ID 生成器
│       ├── arena/                # 内存分配器
│       └── greenteagc/           # GC 调优（GCPercent=40、线程绑定）
├── config/                       # 配置文件
│   ├── standalone.toml           # 单机模式配置
│   ├── cluster.toml              # 集群模式配置
│   └── crd/                      # Kubernetes CRD 定义
├── charts/                       # Helm Chart
└── patches/                      # 补丁依赖（boltdb riscv64 支持）
```

### 建议阅读顺序

从**入口点**（`cmd/godis/main.go`）开始，沿着数据流阅读：

1. **`internal/config/`** — godis 如何加载和热更新配置
2. **`internal/tcp/`** + **`internal/redis/parser/`** — 连接如何被接受和 RESP 请求如何解析
3. **`internal/database/`** — 核心：`router.go`（路由）→ `server.go`（多库编排）→ 各个命令文件
4. **`internal/datastruct/`** — 支撑各命令的数据结构（dict、skiplist、quicklist 等）
5. **`internal/aof/`** + **`internal/database/persistence.go`** — 持久化：AOF 重写和 RDB 加载
6. **`internal/cluster/`** — 分布式模式：Raft 共识、槽路由、TCC 事务
7. **`internal/monitoring/`** — 可观测性：Prometheus 指标
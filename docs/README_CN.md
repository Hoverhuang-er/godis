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

集群基于 [hashicorp/raft](https://github.com/hashicorp/raft) 构建，支持两种运行模式：

- **主从模式（节点数 < 3）**：节点少于 3 个时，raft 无法在节点宕机后选出新领导者，因此采用经典主从语义。从节点定期探测主节点，连续失败达到配置次数后自动升主，默认带有主备切换。
- **Raft 模式（节点数 ≥ 3）**：节点大于等于 3 个时，由 raft 算法统一决策成员变更、槽位路由与故障切换。

探活间隔刻意放宽以容忍高延迟链路：节点每 `cluster_heartbeat_interval` 秒（默认 **15 秒**）互相探活一次，连续 `cluster_failover_max_failures` 次（默认 **10 次**）失败即判定节点失联，两个配置项均在 `[cluster]` 段。

节点间数据复制通过 **OSS（S3 兼容）桶** 完成，而不是直接连接节点端口（设置 `cluster_sync_via_oss = true`）。主节点将命令追加到 **WAL（预写日志，与 AOF 相同的追加式数据流）** 并上传到 OSS，存储为 gzip 压缩但不加密的段文件；从节点拉取并回放这些段。对象 **只追加、不删除**（避免 S3 删除费用），节点间允许最多 **3 个大版本** 的代差（`cluster_max_version_gap`，默认 3）。

**从单机热切换到集群模式（不丢数据、不断连接）：**

节点可以以普通单机模式启动，运行时再切换为集群模式，全程不丢数据、不断开客户端连接：

1. 以单机模式启动：`CONFIG=config/standalone.toml ./godis &`
2. 在配置文件（或 Nacos）中将 `[cluster] enable` 改为 `true`（同时设置 `sync_via_oss = true` 并配置 `[oss]` 段）。godis 重新加载配置后，会把正在运行的服务器包装成集群并原子替换——现有连接不断开，现有数据不丢失。
3. 首个被切换的节点会自动生成 **cluster_id** 并写回配置文件（或发布到 Nacos）的 `[cluster] cluster_id`。后续节点从自己的配置读取同一 id，加入时校验一致，且所有 OSS 数据都落在 `{prefix}/{cluster_id}/` 目录下。

**启动 3 节点集群：**

```bash
# node1 — 创建集群（as_seed = true，仅首个节点）
CONFIG=config/cluster.toml ./godis &
# node2、node3 — 通过任意已有节点的 Redis 地址加入
#   seed = "127.0.0.1:6379"
#   raft_listen_address = "0.0.0.0:16667"   # 每个节点唯一
#   cluster_id = "<与 node1 相同>"
CONFIG=config/cluster-node2.toml ./godis &
CONFIG=config/cluster-node3.toml ./godis &
```

**通过 HTTP 加入集群（与 TCP 方式并存，两种均支持）：**

加入节点可以走 seed 的 HTTP API（端口 `63790`）发送加入握手，替代 Redis 端口的 TCP 通道。每个节点都暴露 `POST /api/cluster/join`，运维也可以手动注册节点：

```bash
# node2 配置：用 join_http 替代 seed
#   join_http = "127.0.0.1:63790"   # seed 的 HTTP API 地址
#   join_http_token = "<token>"     # 仅当 seed 配置了密码时需要
CONFIG=config/cluster-node2.toml ./godis &

# 或从任意机器手动注册节点：
curl -X POST http://127.0.0.1:63790/api/cluster/join \
  -H 'Content-Type: application/json' \
  -d '{"cluster_id":"<id>","redis_addr":"127.0.0.1:6390","raft_addr":"127.0.0.1:16667"}'

# 查看集群实时状态：
curl http://127.0.0.1:63790/api/cluster/status
```

HTTP 加入与 TCP 加入执行完全相同的 raft 加入流程——seed 的 web 服务器把握手转发给本地 godis 节点，成员管理、OSS 同步与故障转移行为完全一致。

**启动 2 节点主从集群：**

```bash
# node1 — 主节点（as_seed = true）
CONFIG=config/cluster.toml ./godis &
# node2 — node1 的从节点
#   seed = "127.0.0.1:6379"
#   master_in_cluster = "127.0.0.1:6379"
CONFIG=config/cluster-node2.toml ./godis &
```

集群模式所需的最小 `[cluster]` / `[oss]` 配置：

```toml
[cluster]
enable = true
# cluster_id = "..."                 # 加入节点填写（种子节点自动写回）
# as_seed = true                     # 仅首个节点
# seed = "127.0.0.1:6379"            # 非种子节点的加入地址
# raft_listen_address = "0.0.0.0:16666"
# master_in_cluster = "127.0.0.1:6379"
sync_via_oss = true

[oss]
enabled = true
endpoint = "127.0.0.1:9000"          # 例如 MinIO 端点
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
bucket = "godis-cluster"
use_ssl = false
force_path_style = true              # MinIO 必需
```

当 OSS 不可用或未启用时，节点回退到直连 TCP 复制（旧行为）。

**灾难恢复。** 后台 goroutine 持续监控 OSS 上的 state marker（每个节点一个小对象，记录其已应用的最大 WAL 序号）。**Raft 模式（≥ 3 节点）** 下主节点宕机由 raft 重新选出新主；集群缩减到 **3 节点以下** 时，幸存节点对比 marker 元数据，挑选数据最新（WAL 序号最大）的节点恢复为新主，无需人工干预。

**节点掉线后的故障转移（failover 操作指引）：**

- **Raft 模式（≥ 3 节点）—— 自动重新竞选主节点。** 节点每 `heartbeat_interval` 秒（默认 15 秒）互相探活，连续 `failover_max_failures` 次（默认 10 次，约 150 秒）未探到即判定节点失联。raft 主节点将失联节点移出投票集合，幸存成员重新选出新主：被提升的节点立即开始向 OSS 发布自己的 WAL，被降级的节点停止发布并成为新主的从节点。幸存节点上的客户端连接不受影响；连接在失联节点上的客户端需要改连其他节点（任意节点都能访问全部数据）。
- **主从模式（< 3 节点）—— OSS 灾难恢复。** 节点少于 3 个时 raft 无法选出领导者，改由灾难恢复 goroutine 接管：每 30 秒幸存节点列出 OSS 上的 state marker，按已应用 WAL 序号排序（数据最新者胜出，序号相同时按 marker 时间戳），从 OSS 恢复最新段到本地数据库，然后提升自己（若最新数据不在本节点则跟随最新节点）。因为 OSS 对象只追加不删除，失联节点的 WAL 段永远可拉取——宕机前已发布的数据不会丢失。
- **重启失联节点。** 用与原配置相同的 `cluster_id`、`seed`/`join_http` 和唯一的 `raft_listen_address` 重启。启动时节点发现磁盘上已有 raft 状态，从 raft FSM 读取当前主节点并成为其从节点，通过 OSS 追赶数据——无需手动重新加入。
- **数据最新者胜出。** 所有故障转移路径（raft 重选与 DR 提升）都对比 OSS state marker，持有最大已应用 WAL 序号（即最新已提交数据）的节点成为新主。不会出现脑裂：raft 模式下遵循法定人数规则，不足法定人数时以 marker 为唯一事实来源。

**集群扩缩容：**

- **扩容（加节点）。** 用相同的 `cluster_id` 启动新节点，配合 `seed = "<任意已有节点的 Redis 地址>"`（TCP 加入）或 `join_http = "<任意节点的 HTTP API 地址>"`（HTTP 加入）。新节点作为投票成员加入 raft 配置。节点 ≥ 3 时集群运行 raft 模式，主节点会在成员间平衡槽位归属；< 3 时新节点作为主从副本加入。加入是动态的，已有节点无需重启。
- **缩容（移除节点）。** v1.3.4 保留了 raft 内部驱逐原语（`HandleEvict` / `RemoveServer`），但**尚未开放**面向用户的移除命令或 HTTP 端点。当前要缩容：直接停止该节点的 godis 进程即可——其余成员通过探活发现失联，raft 模式下自动重选/维持主节点。其 OSS WAL 段因只追加设计而保留，之后重新拉起可干净地重新同步。面向用户的 `cluster.evict` 命令 / `DELETE /api/cluster/node` 端点计划在未来版本提供。
- **OSS 对象布局。** 集群所有数据位于桶内 `{prefix}/{cluster_id}/` 下：`wal/{nodeAddr}/` 存放各节点只追加的 gzip 压缩 WAL 段，`state/` 存放故障转移/DR 使用的各节点状态 marker，`base/{nodeAddr}/` 存放可选的基础快照。任何对象都不会被删除，保持 S3 低成本。

**集群指标（`:9121`）。** 开启 `prometheus_enabled = true` 后，`/metrics` 端点暴露 `godis_cluster_enabled`、`godis_cluster_mode`（`raft`/`master-slave`）、`godis_cluster_role`（`master`/`slave`）、`godis_cluster_master_info{master_id, master_addr}`、`godis_cluster_node_count`、`godis_cluster_wal_seq_applied` / `godis_cluster_wal_seq_published`、`godis_cluster_sync_last_success` / `godis_cluster_sync_lag_seconds` 以及 WAL 上传/下载耗时——足以精确定位主节点并观测复制延迟。

集群模式对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据：

```bash
redis-cli -p 6379
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
		InitAddress: []string{"localhost:6379"},
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

Go version: 1.26.5 (GreenTea GC)
System: MacOS 15.7.1 M5 Air，以及 2 台 DGX Spark 搭建的集群

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
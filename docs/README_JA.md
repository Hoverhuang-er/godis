# Godis v1.3.1

![license](https://img.shields.io/github/license/Hoverhuang-er/godis)
[![Build Status](https://github.com/Hoverhuang-er/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/Hoverhuang-er/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/Hoverhuang-er/godis/badge.svg?branch=master)](https://coveralls.io/github/Hoverhuang-er/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Hoverhuang-er/godis)](https://goreportcard.com/report/github.com/Hoverhuang-er/godis)
[![Go Reference](https://pkg.go.dev/badge/github.com/Hoverhuang-er/godis.svg)](https://pkg.go.dev/github.com/Hoverhuang-er/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[English](https://github.com/Hoverhuang-er/godis/blob/master/docs/README.md) | [中文版](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_CN.md) | [Suomi](https://github.com/Hoverhuang-er/godis/blob/master/docs/README_FI.md)

`Godis` は Go 言語で実装された Redis サーバーです。Go 言語を使って高並行ミドルウェアを開発するための参考例を提供することを目的としています。

主な機能:

- Redis 8.8.0 コマンド互換性
- string, list, hash, set, sorted set, bitmap データ構造をサポート
- RediSearch (FT.CREATE, FT.SEARCH, FT.DROPINDEX など)
- Time Series (TS.CREATE, TS.ADD, TS.GET, TS.RANGE など)
- Redis-Vector (VECTOR フィールド型, KNN 検索)
- 高パフォーマンスの並行処理コア
- TTL (自動有効期限)
- パブリッシュ/サブスクライブ
- GEO 地理空間機能
- AOF および AOF 書き換え
- RDB 読み書き
- 複数データベースと `SELECT` コマンド
- トランザクションは**アトミック**かつ分離されています。実行中にエラーが発生した場合、godis は実行済みのコマンドをロールバックします
- レプリケーション (主従複製)
- サーバーサイドクラスター。クライアント透過的で、クラスター内のどのノードに接続しても全データにアクセス可能
  - Raft ベースのクラスターメタデータ管理。動的拡張、リバランシング、フェイルオーバーをサポート
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` コマンドはクラスターモードでアトミックに実行され、複数ノードにまたがるキーをサポート
  - `MULTI` トランザクションはスロット内でサポート

詳細は[開発者のブログ](https://www.cnblogs.com/Finley/category/1598973.html)をご覧ください (中国語)。

## はじめ方

このリポジトリのリリースページから実行可能ファイルをダウンロードできます (Linux および Darwin をサポート)。

```bash
./godis-darwin
```

```bash
./godis-linux
```

![](https://i.loli.net/2021/05/15/oQM1yZ6pWm3AIEj.png)

redis-cli または他の Redis クライアントを使用して godis サーバーに接続できます。デフォルトでは 0.0.0.0:6399 でリッスンします。

![](https://i.loli.net/2021/05/15/7WquEgonzY62sZI.png)

プログラムは `CONFIG` 環境変数から設定ファイルのパスを読み取ろうとします。

環境変数が設定されていない場合、プログラムは作業ディレクトリの `standalone.toml` (または `redis.conf`) を読み取ろうとします。

設定の詳細については [standalone.toml](./standalone.toml) および [example.conf](./example.conf) を参照してください。

### クラスターモード

デモ用に node1.conf と node2.conf を用意しています。以下のコマンドで 2 ノードクラスターを起動できます:

```bash
CONFIG=node1.conf ./godis-darwin &
CONFIG=node2.conf ./godis-darwin &
```

クラスター内の任意のノードに接続して、全データにアクセスできます:

```cmd
redis-cli -p 6399
```

クラスター設定の詳細については [example.conf](./example.conf) を参照してください。

### Prometheus モニタリング

Godis は Prometheus 互換のメトリクスを `/metrics` エンドポイント (ポート `9121`、設定ファイルの `monitoring.prometheus_port` で変更可能) で公開します。メトリクスは **デフォルトで有効** で、`redis_exporter` の命名規則に従い、既存の Redis ダッシュボードと互換性があります。

```bash
# デフォルトのスクレイプエンドポイント
curl http://localhost:9121/metrics
```

**主なメトリクス:**
- `godis_connected_clients` — 現在のアクティブ接続数
- `godis_commands_total` — 処理されたコマンドの総数
- `godis_keyspace_hits_total` / `godis_keyspace_misses_total` — キャッシュヒット/ミスカウンター
- `godis_db_keys` — データベースごとのキー数
- `godis_db_avg_ttl_seconds` — データベースごとの平均 TTL
- `godis_slowlog_length` — スローログのキュー長
- ホットキーおよびビッグキーの検出 (定期的にサンプリング)

無効にするには、設定ファイルの `[monitoring]` セクションで `prometheus_enabled = false` を設定します。すべてのモニタリング設定は実行時にホットリロードされます。

```toml
[monitoring]
prometheus_enabled = true
prometheus_port = 9121
```

## Rueidis クライアント例

[Rueidis](https://github.com/redis/rueidis) は Go 向けの高性能 Redis クライアントです。Godis で使用する方法は以下の通りです:

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

	// SET/GET の例
	err = client.Do(ctx, client.B().Set().Key("foo").Value("bar").Build()).Error()
	if err != nil {
		log.Fatal(err)
	}

	val, err := client.Do(ctx, client.B().Get().Key("foo").Build()).ToString()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GET foo = %s\n", val)

	// RediSearch の例
	// 事前に FT.CREATE でインデックスを作成する必要があります
	result, err := client.Do(ctx, client.B().FtSearch().Index("idx").Query("@field:val").Build()).ToArray()
	if err != nil {
		log.Printf("検索メモ: %v (最初に FT.CREATE でインデックスを作成してください)", err)
	}
	_ = result

	// Time Series の例
	err = client.Do(ctx, client.B().TsAdd().Key("ts:temp").Timestamp(1).Value(25.5).Build()).Error()
	if err != nil {
		log.Printf("時系列メモ: %v", err)
	}
}
```

## サポートされているコマンド

[commands.md](https://github.com/Hoverhuang-er/godis/blob/master/commands.md) を参照してください。

## ベンチマーク

環境:

Go version: 1.23
System: MacOS Monterey 12.5 M2 Air

redis-benchmark によるパフォーマンスレポート:

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

## コードの読み方

このプロジェクトは [Go Project Layout](https://github.com/golang-standards/project-layout) に従っています：

```
godis/
├── cmd/                          # エントリポイント
│   ├── godis/main.go             # Godis サーバー（スタンドアロン/クラスター）
│   ├── godis/cli.go              # 組み込み redis-cli（--cli フラグ）
│   └── operator/main.go          # Kubernetes Operator
├── internal/                     # 内部実装
│   ├── config/                   # TOML設定（viper ホットリロード、埋め込みデフォルト）
│   ├── tcp/                      # TCPサーバー（goroutine/コネクション）
│   ├── redis/                    # Redis プロトコル
│   │   ├── parser/               # RESP2/RESP3 パーサー（ストリーミング）
│   │   ├── protocol/             # レスポンス型（Bulk、MultiBulk、Error 等）
│   │   ├── server/               # サーバーアダプター
│   │   │   ├── std/              #   標準 net.TCP
│   │   │   └── gnet/             #   gnet イベントループ
│   │   ├── client/               # クラスターノード間通信用クライアント
│   │   └── connection/           # コネクション状態（DB、認証、トランザクション）
│   ├── interface/                # コアインターフェース定義
│   ├── database/                 # ストレージエンジンとコマンドハンドラー
│   │   ├── server.go             # マルチデータベースサーバー（AOF、レプリケーション）
│   │   ├── database.go           # 単一データベース（データアクセス、TTL、ロック）
│   │   ├── router.go             # コマンド登録とルーティング
│   │   ├── string.go             # GET、SET、INCR、APPEND 等
│   │   ├── hash.go               # HSET、HGET、HDEL 等
│   │   ├── list.go               # LPUSH、LRANGE、LINDEX 等
│   │   ├── set.go                # SADD、SMEMBERS、SINTER 等
│   │   ├── sortedset.go          # ZADD、ZRANGE、ZRANK 等
│   │   ├── stream.go             # XADD、XREAD、XGROUP 等
│   │   ├── geo.go                # GEOADD、GEOSEARCH 等
│   │   ├── keys.go               # DEL、EXISTS、EXPIRE、TTL 等
│   │   ├── transaction.go        # MULTI、EXEC、WATCH
│   │   ├── persistence.go        # RDB 読み込み
│   │   ├── timeseries.go         # TS.CREATE、TS.ADD、TS.GET、TS.RANGE
│   │   ├── search.go             # FT.CREATE、FT.SEARCH、FT.DROPINDEX
│   │   ├── json.go               # JSON.SET、JSON.GET、JSON.DEL 等
│   │   ├── bloom.go              # BF.ADD、BF.EXISTS、BF.RESERVE
│   │   ├── hyperloglog.go        # PFADD、PFCOUNT、PFMERGE
│   │   ├── topk.go               # TOPK.ADD、TOPK.QUERY、TOPK.LIST
│   │   ├── cms.go                # CMS.INCRBY、CMS.QUERY
│   │   ├── tdigest.go            # TDIGEST.ADD、TDIGEST.QUANTILE
│   │   ├── bitfield.go           # BITFIELD、BITFIELD_RO
│   │   └── array.go              # AR.SET、AR.GET、AR.APPEND
│   ├── aof/                      # AOF 永続化とリライト
│   ├── pubsub/                   # パブリッシュ/サブスクライブ
│   ├── cluster/                  # クラスターモード
│   │   ├── core/                 # スロットルーティング、TCC トランザクション
│   │   ├── commands/             # クラスター対応 DEL、MSET、RENAME
│   │   └── raft/                 # Raft 合意形成
│   ├── monitoring/               # Prometheus メトリクス
│   ├── auth/entraid/             # Entra ID JWT 検証（Azure AD）
│   ├── datastruct/               # データ構造実装
│   │   ├── dict/                 # 並行ハッシュマップ（ロックストライピング）
│   │   ├── list/                 # Quicklist
│   │   ├── set/                  # ハッシュセット
│   │   ├── sortedset/            # スキップリスト
│   │   ├── bitmap/               # ビット配列
│   │   ├── stream/               # 基数木ベースのストリーム
│   │   ├── search/               # 転置インデックス
│   │   ├── hyperloglog/          # 確率的基数推定（2^14 レジスタ）
│   │   ├── bloom/                # ブルームフィルター
│   │   ├── cms/                  # Count-min Sketch
│   │   ├── topk/                 # Top-K 頻出項目
│   │   ├── tdigest/              # T-Digest（分位数推定）
│   │   ├── timeseries/           # 時系列データ
│   │   ├── array/                # 疎インデックス配列
│   │   └── lock/                 # キーレベル RW ロック
│   └── lib/                      # ユーティリティ
│       ├── logger/               # 構造化ファイルロガー
│       ├── pool/                 # 汎用オブジェクトプール
│       ├── timewheel/            # タイムホイール（有効期限と cron）
│       ├── wildcard/             # グロブパターンマッチ
│       ├── consistenthash/       # 一貫性ハッシュリング
│       ├── idgenerator/          # Snowflake ID 生成器
│       ├── arena/                # メモリアリーナアロケーター
│       └── greenteagc/           # GC チューニング
├── config/                       # 設定ファイル
│   ├── standalone.toml           # スタンドアロン設定
│   ├── cluster.toml              # クラスター設定
│   └── crd/                      # Kubernetes CRD
├── charts/                       # Helm Chart
└── patches/                      # パッチ依存関係
```

### 推奨読書順序

**エントリポイント**（`cmd/godis/main.go`）から始めて、データフローに沿って読み進めてください：

1. **`internal/config/`** — 設定の読み込みとホットリロード
2. **`internal/tcp/`** + **`internal/redis/parser/`** — 接続受付とRESP解析
3. **`internal/database/`** — コア：ルーティング → マルチDB → 各コマンド
4. **`internal/datastruct/`** — 各コマンドを支えるデータ構造
5. **`internal/aof/`** + **`internal/database/persistence.go`** — 永続化
6. **`internal/cluster/`** — 分散モード：Raft、スロットルーティング、TCC
7. **`internal/monitoring/`** — 可観測性：Prometheus

# ライセンス

このプロジェクトは [GPL ライセンス](https://github.com/Hoverhuang-er/godis/blob/master/LICENSE) の下でライセンスされています。

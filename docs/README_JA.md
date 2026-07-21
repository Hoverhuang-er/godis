# Godis v1.3.1

![license](https://img.shields.io/github/license/Hoverhuang-er/godis)
[![Build Status](https://github.com/Hoverhuang-er/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/Hoverhuang-er/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/Hoverhuang-er/godis/badge.svg?branch=master)](https://coveralls.io/github/Hoverhuang-er/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Hoverhuang-er/godis)](https://goreportcard.com/report/github.com/Hoverhuang-er/godis)
[![Go Reference](https://pkg.go.dev/badge/github.com/Hoverhuang-er/godis.svg)](https://pkg.go.dev/github.com/Hoverhuang-er/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[English](https://github.com/Hoverhuang-er/godis/blob/master/README.md) | [中文版](https://github.com/Hoverhuang-er/godis/blob/master/README_CN.md) | [Suomi](https://github.com/Hoverhuang-er/godis/blob/master/README_FI.md)

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

このリポジトリのコードを読むための簡単なガイドです。

- プロジェクトルート: エントリポイントのみ
- config: 設定パーサー
- interface: インターフェース定義
- lib: ユーティリティ (logger, 同期, ワイルドカードなど)

以下のディレクトリに注目することをお勧めします:

- tcp: TCP サーバー
- redis: Redis プロトコルパーサー
- datastruct: データ構造の実装
    - dict: 並行ハッシュマップ
    - list: リンクリスト
    - lock: スレッドセーフを確保するためのキーロック
    - set: マップベースのハッシュセット
    - sortedset: スキップリストベースのソート済みセット
- database: ストレージエンジンの中核
    - server.go: スタンドアロン Redis サーバー (複数データベース対応)
    - database.go: 単一データベースのデータ構造と基本機能
    - exec.go: データベースのゲートウェイ
    - router.go: コマンドテーブル
    - keys.go: キーコマンドのハンドラー
    - string.go: 文字列コマンドのハンドラー
    - list.go: リストコマンドのハンドラー
    - hash.go: ハッシュコマンドのハンドラー
    - set.go: セットコマンドのハンドラー
    - sortedset.go: ソート済みセットコマンドのハンドラー
    - pubsub.go: パブリッシュ/サブスクライブの実装
    - aof.go: AOF 永続化と書き換えの実装
    - geo.go: 地理空間機能の実装
    - sys.go: 認証およびその他のシステム機能
    - transaction.go: ローカルトランザクション
- cluster: 
    - cluster.go: クラスターモードのエントリ
    - com.go: ノード間通信
    - del.go: クラスターでの `delete` コマンドのアトミック実装
    - keys.go: キーコマンド
    - mset.go: クラスターでの `mset` コマンドのアトミック実装
    - multi.go: 分散トランザクションのエントリ
    - pubsub.go: クラスターでの pub/sub
    - rename.go: クラスターでの `rename` コマンド
    - tcc.go: Try-commit-catch 分散トランザクション実装
- aof: AOF 永続化

# ライセンス

このプロジェクトは [GPL ライセンス](https://github.com/Hoverhuang-er/godis/blob/master/LICENSE) の下でライセンスされています。

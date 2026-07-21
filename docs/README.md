# Godis v1.3.1

![license](https://img.shields.io/github/license/Hoverhuang-er/godis)
[![Build Status](https://github.com/Hoverhuang-er/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/Hoverhuang-er/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/Hoverhuang-er/godis/badge.svg?branch=master)](https://coveralls.io/github/Hoverhuang-er/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Hoverhuang-er/godis)](https://goreportcard.com/report/github.com/Hoverhuang-er/godis)
[![Go Reference](https://pkg.go.dev/badge/github.com/Hoverhuang-er/godis.svg)](https://pkg.go.dev/github.com/Hoverhuang-er/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[中文版](https://github.com/Hoverhuang-er/godis/blob/master/README_CN.md) | [日本語](https://github.com/Hoverhuang-er/godis/blob/master/README_JA.md) | [Suomi](https://github.com/Hoverhuang-er/godis/blob/master/README_FI.md)

`Godis` is a golang implementation of Redis Server, which intents to provide an example of writing a high concurrent
middleware using golang.

Key Features:

- Redis 8.8.0 command compatibility
- Support string, list, hash, set, sorted set, bitmap
- RediSearch (FT.CREATE, FT.SEARCH, FT.DROPINDEX, etc.)
- Time Series (TS.CREATE, TS.ADD, TS.GET, TS.RANGE, etc.)
- Redis-Vector (VECTOR field type, KNN search)
- Concurrent Core for better performance
- TTL
- Publish/Subscribe
- GEO
- AOF and AOF Rewrite
- RDB read and write
- Multi Database and `SELECT` command
- Transaction is **Atomic** and Isolated. If any errors are encountered during execution, godis will rollback the executed commands
- Replication
- Server-side Cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster.
  - Cluster metadata management based on Raft. Support dynamic expansion, rebalancing and failover.
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` command is supported and atomically executed in cluster mode, allow over multi node.
  - `MULTI` Commands Transaction is supported within slot in cluster mode

If you could read Chinese, you can find more details in [My Blog](https://www.cnblogs.com/Finley/category/1598973.html).

## Quick Start

### Standalone (Linux / macOS)

```bash
# Download the binary from GitHub Releases
curl -LO https://github.com/Hoverhuang-er/godis/releases/download/v1.3.1/godis_linux_amd64.zip
unzip godis_linux_amd64.zip

# Run with minimal config
CONFIG=config/standalone.toml ./godis

# Or use the default auto-detection (looks for standalone.toml in cwd)
./godis
```

### Standalone (Windows)

```powershell
# Download godis_windows_amd64.zip from Releases, extract
# Run in PowerShell:
$env:CONFIG="config\standalone.toml"
.\godis.exe
```

### Docker Compose (Minimal)

```bash
git clone https://github.com/Hoverhuang-er/godis.git
cd godis
docker compose up -d
redis-cli -p 6399 PING
```

Minimal `docker-compose.yml`:

```yaml
services:
  godis:
    image: ghcr.io/Hoverhuang-er/godis:latest
    ports:
      - "6399:6399"
    volumes:
      - godis-data:/data
      - ./config/standalone.toml:/etc/godis/standalone.toml:ro
    environment:
      - CONFIG=/etc/godis/standalone.toml
    restart: unless-stopped

volumes:
  godis-data:
```

### Docker (Single Container)

```bash
docker run -d --name godis \
  -p 6399:6399 \
  -v godis-data:/data \
  ghcr.io/Hoverhuang-er/godis:latest
```

### Cluster Mode (Multi-Node)

```bash
# Start a 3-node cluster
CONFIG=config/cluster.toml ./godis &
```

Connect to any node to access the full dataset:

```bash
redis-cli -p 6399
```

## Kubernetes Deployment

### Helm Chart (Recommended)

For production deployments, use the Helm chart:

```bash
# Add repository and install
helm pull oci://ghcr.io/Hoverhuang-er/godis/charts/godis --version 1.3.1
helm install godis ./godis-1.3.1.tgz

# Or install directly
helm install godis oci://ghcr.io/Hoverhuang-er/godis/charts/godis --version 1.3.1

# Cluster mode (3 nodes)
helm install godis-cluster oci://ghcr.io/Hoverhuang-er/godis/charts/godis --version 1.3.1 \
  --set mode=cluster --set replicaCount=3
```

See [charts/godis/values.yaml](https://github.com/Hoverhuang-er/godis/blob/main/charts/godis/values.yaml) for all configuration options.

### Kubernetes Operator

The Godis Operator manages GodisCluster custom resources. Deploy with:

```bash
# Install CRD
kubectl apply -f https://raw.githubusercontent.com/Hoverhuang-er/godis/main/config/crd/godisclusters.yaml

# Deploy operator (default: 3 nodes, 0.5 CPU / 1Gi memory per node)
kubectl create deployment godis-operator --image=ghcr.io/Hoverhuang-er/godis/operator:1.3.1

# Create a Godis cluster (standalone)
kubectl apply -f - <<EOF
apiVersion: godis.Hoverhuang-er.io/v1
kind: GodisCluster
metadata:
  name: my-godis
spec:
  mode: standalone
  port: 6399
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
EOF
```

#### Autoscaling

The operator supports HPA, VPA, and KEDA:

```yaml
apiVersion: godis.Hoverhuang-er.io/v1
kind: GodisCluster
metadata:
  name: my-godis-cluster
spec:
  mode: cluster
  replicas: 3
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
    targetMemoryUtilizationPercentage: 80
    enableVPA: true
    enableKEDA: true
```

Supported Kubernetes versions: **1.34–1.36** and **k3s** (any supported version).

## Rueidis Client Example

[Rueidis](https://github.com/redis/rueidis) is a high-performance Redis client for Go. Here's how to use it with Godis:

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

	// SET/GET example
	err = client.Do(ctx, client.B().Set().Key("foo").Value("bar").Build()).Error()
	if err != nil {
		log.Fatal(err)
	}

	val, err := client.Do(ctx, client.B().Get().Key("foo").Build()).ToString()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GET foo = %s\n", val)

	// RediSearch example
	// Requires FT.CREATE index first
	result, err := client.Do(ctx, client.B().FtSearch().Index("idx").Query("@field:val").Build()).ToArray()
	if err != nil {
		log.Printf("Search note: %v (create index with FT.CREATE first)", err)
	}
	_ = result

	// Time Series example
	err = client.Do(ctx, client.B().TsAdd().Key("ts:temp").Timestamp(1).Value(25.5).Build()).Error()
	if err != nil {
		log.Printf("Time series note: %v", err)
	}
}
```

## Supported Commands

See: [commands.md](https://github.com/Hoverhuang-er/godis/blob/master/commands.md)

## Benchmark

Environment:

Go version：1.23
System: MacOS Monterey 12.5 M2 Air

Performance report by redis-benchmark: 

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

## Read My Code

If you want to read my code in this repository, here is a simple guidance.

- project root: only the entry point
- config: config parser
- interface: some interface definitions
- lib: some utils, such as logger, sync utils and wildcard

I suggest focusing on the following directories:

- tcp: the tcp server
- redis: the redis protocol parser
- datastruct: the implements of data structures
    - dict: a concurrent hash map
    - list: a linked list
    - lock: it is used to lock keys to ensure thread safety
    - set: a hash set based on map
    - sortedset: a sorted set implements based on skiplist
- database: the core of storage engine
    - server.go: a standalone redis server, with multiple database
    - database.go: data structure and base functions of single database
    - exec.go: the gateway of database
    - router.go: the command table
    - keys.go: handlers for keys commands
    - string.go: handlers for string commands
    - list.go: handlers for list commands
    - hash.go: handlers for hash commands
    - set.go: handlers for set commands
    - sortedset.go: handlers for sorted set commands
    - pubsub.go: implements of publish / subscribe
    - aof.go: implements of AOF persistence and rewrite
    - geo.go: implements of geography features
    - sys.go: authentication and other system function
    - transaction.go: local transaction
- cluster: 
    - cluster.go: entrance of cluster mode
    - com.go: communication within nodes
    - del.go: atomic implementation of `delete` command in cluster
    - keys.go: keys command
    - mset.go: atomic implementation of `mset` command in cluster
    - multi.go: entrance of distributed transaction
    - pubsub.go: pub/sub in cluster
    - rename.go: `rename` command in cluster 
    - tcc.go: try-commit-catch distributed transaction implementation
- aof: AOF persistence

# License

This project is licensed under the [GPL license](https://github.com/Hoverhuang-er/godis/blob/master/LICENSE).
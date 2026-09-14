# Cluster

godis runs as a single node or as a cluster. In cluster mode the cluster is
**server-side and transparent**: connect to any node and see all the data. Clients
need no slot map and no cluster-aware driver.

Metadata — which node owns which slot — is maintained by Raft. Nodes can be added,
rebalanced and failed over at runtime.

## Starting a cluster

Node 1 seeds the cluster:

```toml
# node1
[cluster]
enable = true
as_seed = true
cluster_id = ""                     # generated and written back on first start
raft_listen_address = "0.0.0.0:16666"
raft_advertise_address = "10.0.0.1:16666"
```

Nodes 2 and 3 join it:

```toml
# node2
[cluster]
enable = true
seed = "10.0.0.1:6379"              # any existing node's Redis address
raft_listen_address = "0.0.0.0:16667"   # unique per node
raft_advertise_address = "10.0.0.2:16667"
cluster_id = "<same id as node1>"
```

`raft_listen_address` must differ per node. `cluster_id` must match the seed's.

### Joining over HTTP

If the seed has a password, or you would rather not open the TCP join channel, join
through the seed's HTTP API instead:

```toml
[cluster]
join_http = "10.0.0.1:63790"
join_http_token = "<token from POST /api/auth>"
```

Or from the command line:

```bash
./godis --join 10.0.0.1:6379
./godis --join http://10.0.0.1:63790
```

Registering a node manually:

```bash
curl -X POST http://10.0.0.1:63790/api/cluster/join \
  -H "X-HEADER-AUTHTOKEN: $TOKEN" \
  -d '{"addr":"10.0.0.4:6379"}'
```

## Inspecting

```bash
redis-cli CLUSTER INFO
redis-cli CLUSTER NODES
redis-cli -p 6379 cluster.committedindex

curl -H "X-HEADER-AUTHTOKEN: $TOKEN" http://10.0.0.1:63790/api/cluster/status
```

## Replication

A cluster node can also be a replica of another:

```toml
[cluster]
enable = true
seed = "10.0.0.1:6379"
master_in_cluster = "10.0.0.1:6379"
```

## Failover

With three or more voting nodes, Raft elects a new master in the background when
the current one is unresponsive. With fewer than three, a disaster-recovery path
restores the newest data from the object store and promotes the most current node.

Timing is tunable:

```toml
[cluster]
heartbeat_interval = 15         # seconds
failover_max_failures = 10      # consecutive missed probes before failover
```

## Scaling down

To remove a node, stop its godis process. The remaining members detect the loss
through the health probe and, in raft mode, keep or re-elect a master
automatically. Its object-store WAL segments are retained by design, so bringing
the node back later re-synchronises cleanly.

The raft eviction primitive is exposed as `cluster.evict` and
`DELETE /api/cluster/node`:

```bash
redis-cli cluster.evict 10.0.0.4:6379
curl -X DELETE -H "X-HEADER-AUTHTOKEN: $TOKEN" \
  http://10.0.0.1:63790/api/cluster/node -d '{"addr":"10.0.0.4:6379"}'
```

## Multi-key commands

`MSET`, `MSETNX`, `DEL`, `RENAME` and `RENAME` variants execute atomically across
nodes, so keys do not have to share a node. `MULTI`/`EXEC` works within a slot.

## Replication between clusters over object storage

Nodes can synchronise through an S3-compatible bucket instead of direct
connections — useful when nodes cannot reach each other but can both reach a
bucket:

```toml
[cluster]
sync_via_oss = true
snapshot_interval = 60

[oss]
endpoint = "s3.amazonaws.com"
bucket = "godis-cluster"
access_key_id = "..."
secret_access_key = "..."
region = "us-east-1"
```

The master appends commands to a WAL and uploads compressed segments; replicas
pull and replay them. Objects are append-only, so there are no delete operations
and storage cost stays low.

## Notes on the knowledge base in cluster mode

Knowledge-base documents are ordinary hashes, so they shard, replicate and fail
over like any other key. Indexes are per-process and rebuild from the documents
present on that node — a query reaches the data that node holds. For a corpus that
must be entirely queryable from one connection, run the knowledge base on a single
node and scale the search workload by replicas.

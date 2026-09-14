# Operations

## Ports

| Port | Service | Config |
|---|---|---|
| 6379 | Redis protocol | `[server] port` |
| 63790 | HTTP API | `[http_api] port` |
| 63800 | Web dashboard | `[web] port` |
| 9121 | Prometheus metrics | `[monitoring] prometheus_port` |

## Persistence

godis supports AOF (with rewrite), RDB read and write, and a hybrid mode where the
AOF preamble is RDB.

```toml
[aof]
appendonly = true
appendfilename = "appendonly.aof"
appendfsync = "everysec"        # always | everysec | no
aof_use_rdb_preamble = true
dbfilename = "test.rdb"
```

```bash
redis-cli BGREWRITEAOF
redis-cli BGSAVE
redis-cli SAVE
```

`everysec` is the usual choice: at most one second of writes is at risk, at a
fraction of the cost of `always`.

### Indexes and restarts

Search indexes are **in-memory and not persisted** — they are derived data. After a
restart, an index is rebuilt from the documents that are still present, on first
access. For `FT.*` indexes you created by hand, re-run `FT.CREATE` after a restart;
knowledge-base collections (`RAG.*`, `WIKI.*`, `MEMORY.*`, `LLMCACHE.*`) rebuild
themselves, because their schema is fixed by the command family.

Because embeddings are stored as document fields, a rebuild never re-embeds and
never needs the embedding backend to be reachable.

### Backups

Documents are ordinary Redis data. `BGSAVE`, AOF, and `--migrate` all work, and
the [wiki can be exported as markdown](llm-wiki#exporting) for a
human-readable, diffable copy.

## Monitoring

`PROMETHEUS_ENABLED=true` serves metrics on :9121.

```bash
curl -s localhost:9121/metrics | grep godis_cluster
redis-cli INFO
```

`INFO` includes a `replication` section with sentinel-compatible field names.

The dashboard on :63800 browses keys and runs read-only commands including
`FT.SEARCH`, `FT.HYBRID`, `RAG.QUERY`, `WIKI.QUERY`, `MEMORY.SEARCH` and
`LLMCACHE.STATS`.

The HTTP API on :63790 executes commands:

```bash
TOKEN=$(curl -s -X POST localhost:63790/api/auth | jq -r .token)
curl -H "X-HEADER-AUTHTOKEN: $TOKEN" \
  "localhost:63790/api/commands?cmd=RAG.QUERY%20handbook%20%22how%20do%20I%20tune%20recall%22"
```

## Slow log

```toml
[slowlog]
log_slower_than = 10000     # microseconds
max_len = 128
```

```bash
redis-cli SLOWLOG GET 10
```

Knowledge-base writes are the operations most likely to show up here, because they
embed before storing. Chunking and embedding dominate that cost; if ingest latency
matters, raise `chunk_size` so a document produces fewer chunks, or use a remote
embedder with a larger batch size.

## Capacity

Rough sizing for a knowledge base:

- Each chunk stores its text, its metadata, and its vector.
- A vector costs `4 × dim` bytes on disk and roughly twice that in memory, since
  the hash copy and the index copy are both held.
- A 768-dimension embedding is therefore about 3 KB of disk per chunk and 6 KB of
  RAM, on top of the text.

At 800-character chunks, 1 GB of source text is roughly 1.3M chunks and, at 768
dimensions, on the order of 8 GB of RAM. Size accordingly, or keep vectors shorter.

## Metrics worth alerting on

| Symptom | Likely cause |
|---|---|
| `LLMCACHE.STATS` hit rate collapses | threshold too strict for the embedder, or the model changed |
| `RAG.INGEST` slow | large chunks, or a remote embedder without batching |
| `RAG.QUERY` returns only weak similarities | the collection does not cover the question — ingest more |
| Retrieval got worse after a config change | the embedding model changed; re-ingest |

## Troubleshooting

**`ERR no such collection 'kb'`** — nothing has been ingested into that collection,
or the name is misspelled. The name is case-sensitive.

**`vector dimension mismatch`** — the index was built with a different
`embed_dim` or `embed_model`. Re-ingest the collection.

**`embedding failed`** — the HTTP embedding endpoint is unreachable or returned an
error. `RAG.QUERY`/`RAG.INGEST` fail rather than returning empty results, so a
broken embedder cannot be mistaken for an empty knowledge base.

**Search returns nothing after a restart** — a hand-made `FT.*` index is not
rebuilt automatically. Re-run `FT.CREATE`; it backfills from the documents present.

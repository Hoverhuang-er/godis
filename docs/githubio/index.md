# godis

A Redis-compatible in-memory database written in Go, with vector search, hybrid
search, and a knowledge base the server itself serves.

```bash
docker run -p 6379:6379 ghcr.io/hoverhuang-er/godis:latest
redis-cli FT.CREATE idx ON JSON PREFIX 1 product: SCHEMA \
  '$.name' AS name TEXT \
  '$.embedding' AS embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE
```

## Start here

| Guide | What it covers |
|---|---|
| [Getting started](getting-started) | Install, run, and issue your first commands |
| [Configuration](configuration) | Every `[ai]` and server setting that matters |
| [Vector and hybrid search](search) | `FT.CREATE`, `FT.SEARCH`, `FT.HYBRID` |
| [RAG](rag) | `RAG.*`: chunk, embed, retrieve with citations |
| [LLM wiki](llm-wiki) | `WIKI.*`: the persistent knowledge layer an agent maintains |
| [Agent memory](agent-memory) | `MEMORY.*`: what to remember, and how to recall it |
| [Semantic cache](semantic-cache) | `LLMCACHE.*`: reuse an answer to a reworded question |
| [Cluster](cluster) | Multi-node operation, joins, failover |
| [Operations](operations) | Persistence, ports, monitoring, backups |
| [Clients and agents](clients) | Connecting from a client library or an LLM agent |

## What makes godis different

Most Redis-compatible servers stop at data structures. godis also answers
questions.

**Retrieval happens in the server.** Embedding runs inside the database process,
so a knowledge-base query needs nothing from the client side:

```
redis> RAG.QUERY handbook "how is recall traded against build time?" K 3
# Retrieved context

Query: how is recall traded against build time?

### [1] HNSW
_source: handbook.md · similarity: 0.624 · kb:hnsw · chunk 0_

HNSW builds a multi-layer proximity graph. Recall is traded against build time
through efConstruction and efRuntime.
```

No Python, no client library, no embedding code in your application.

**The knowledge layer is an artifact, not a pipeline.** The wiki pattern that
godis implements keeps a persistent, interlinked set of pages between your raw
sources and your questions. Cross-references, the catalogue and the history are
maintained by the server, so knowledge accumulates instead of being re-derived on
every query.

**Generation stays yours.** godis retrieves; it does not call an LLM. Your agent
is already a model, so it gets assembled context with citations and generates the
answer itself. No generation API key lives inside the database.

## Compatibility

- Redis 8.8.0 command surface, RESP2 and RESP3
- Data structures: string, list, hash, set, sorted set, bitmap, stream, GeoJSON/JSON
- Modules: RediSearch (`FT.*`), RedisJSON (`JSON.*`), TimeSeries (`TS.*`), Bloom,
  HyperLogLog, Top-K, Count-min sketch, T-Digest
- Persistence: AOF with rewrite, RDB read/write, `aof-use-rdb-preamble`
- Replication and a server-side cluster managed by Raft

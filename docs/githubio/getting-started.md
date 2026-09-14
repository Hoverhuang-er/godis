# Getting started

## Run with Docker

```bash
docker run --rm -p 6379:6379 ghcr.io/hoverhuang-er/godis:latest
```

The image starts with a working default configuration, including a local
embedding backend, so the knowledge-base commands work with no further setup.

## Run a release binary

Download the archive for your platform from
[Releases](https://github.com/Hoverhuang-er/godis/releases), then:

```bash
unzip godis_linux_amd64.zip
./godis                    # writes standalone.toml on first run and loads it
# or point at your own config
CONFIG=standalone.toml ./godis
```

Ports, once running:

| Port | Service |
|---|---|
| 6379 | Redis protocol (`redis-cli`) |
| 63790 | HTTP API |
| 63800 | Web dashboard |
| 9121 | Prometheus metrics |

## First commands

```bash
redis-cli PING                     # PONG
redis-cli SET hello world
redis-cli GET hello                # "world"
redis-cli INFO server | head       # version and uptime
```

## Your first vector index

godis indexes JSON documents as well as hashes. Write documents first — `FT.CREATE`
backfills what is already in the database.

```bash
redis-cli JSON.SET doc:1 $ '{"title":"Tuning recall","body":"Recall is traded against build time.","embedding":[0.9,0.1,0,0]}'
redis-cli JSON.SET doc:2 $ '{"title":"Baking bread","body":"Sourdough needs a starter.","embedding":[0,0,1,0]}'

redis-cli FT.CREATE idx ON JSON PREFIX 1 doc: SCHEMA \
  '$.title' AS title TEXT \
  '$.body' AS body TEXT \
  '$.embedding' AS embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC COSINE
# OK

redis-cli FT.SEARCH idx '(*)=>[KNN 1 @embedding $vec AS score]' \
  PARAMS 2 vec "1.0 0.0 0.0 0.0" \
  SORTBY score ASC RETURN 2 score title DIALECT 2
```

The nearest document is `doc:1`, the one whose vector points the same way.

Note the two ways a query vector can be sent: `redis-cli` sends the text
`"1.0 0.0 0.0 0.0"`, while client libraries send raw little-endian `FLOAT32`
bytes. godis accepts both.

## Your first knowledge-base query

`RAG.*` is the same machinery with the chunking and embedding done for you.

```bash
redis-cli RAG.INGEST handbook hnsw \
  "HNSW builds a multi-layer proximity graph. Recall is traded against build time through efConstruction." \
  TITLE "HNSW index" SOURCE handbook.md TAG vectors

redis-cli RAG.QUERY handbook "how is recall traded against build time?" K 3
```

You get an assembled context block with a similarity per passage and the source
to cite. Feed it to your model; generate the answer there.

## Next

- Tune the embedding backend: [Configuration](configuration)
- Learn the query syntax: [Vector and hybrid search](search)
- Build a knowledge base: [RAG](rag), [LLM wiki](llm-wiki)
- Give an agent memory: [Agent memory](agent-memory)
- Wire it into an agent: [Clients and agents](clients)

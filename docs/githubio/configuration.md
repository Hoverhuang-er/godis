# Configuration

godis reads a TOML file. It looks for `standalone.toml` (or `cluster.toml`) in the
working directory, or wherever `CONFIG` points.

## The `[ai]` section

This section controls the embedding backend used by `RAG.*`, `WIKI.*`,
`MEMORY.*` and `LLMCACHE.*`. It runs inside the server, which is what lets
`redis-cli` query a knowledge base without embedding anything client-side.

```toml
[ai]
# "local" or "http"
embed_provider = "local"

# Vector length of the local embedder.
embed_local_dim = 512

# --- HTTP provider settings (only when embed_provider = "http") ---
# embed_base_url = "https://api.openai.com/v1"
# embed_api_key  = "sk-..."
# embed_model    = "text-embedding-3-small"
# embed_dim      = 1536
# embed_timeout  = 30

# Chunking bounds for RAG.INGEST, in characters.
chunk_size = 800
chunk_overlap = 120
```

### Choosing a provider

**`local`** — a dependency-free pure-Go embedder. Offline, no API key, no network
call, deterministic across restarts. It is **lexical, not semantic**: it hashes
word unigrams and character trigrams, so it handles word forms and typos, but it
will not connect "headphones" to "earbuds". It is the right default for a wiki or
a notes corpus where the vocabulary is stable, and for anything that must work
without network access.

**`http`** — any OpenAI-compatible `/embeddings` endpoint: OpenAI, Azure OpenAI,
Ollama, vLLM, LM Studio, or a hosted gateway. This gives real semantic matching
and is what you want for retrieval over prose you did not write.

```toml
# Local Ollama
[ai]
embed_provider = "http"
embed_base_url = "http://localhost:11434/v1"
embed_model    = "nomic-embed-text"
embed_dim      = 768
```

**A misconfigured HTTP provider is a startup error**, not a silent fallback to
the local embedder. The two produce vectors in unrelated spaces, and quietly
mixing them would corrupt an existing index rather than fail loudly.

### Dimension mismatches

Every index records the vector dimension it was built with. If a later request
disagrees — because you changed `embed_model` or `embed_dim` — the command fails
with a dimension error instead of returning plausible nonsense. **Changing the
embedding model means re-ingesting.** Existing vectors cannot be compared with
new ones.

### Changing the chunking bounds

`chunk_size` and `chunk_overlap` apply to `RAG.INGEST`. Chunks are built on
paragraph boundaries with the heading breadcrumb prefixed, so a chunk lifted out
of a long document still carries its section title. Raise `chunk_size` if your
retrieved passages are too fragmented to be useful; lower it if a chunk contains
several unrelated topics.

## Server settings

```toml
[server]
bind = "0.0.0.0"
port = 6379
databases = 16
maxclients = 128
# dir = "/var/lib/godis"     # where AOF/RDB files live
```

## Persistence

```toml
[aof]
appendonly = false
appendfilename = "appendonly.aof"
appendfsync = "everysec"        # always | everysec | no
aof_use_rdb_preamble = true
dbfilename = "test.rdb"
```

godis RDB files are produced by AOF rewriting. Knowledge-base documents are
ordinary hashes, so they persist through the same path as any other data, and
indexes rebuild themselves from the documents on access.

## Security

```toml
[security]
requirepass = "yourpassword"
```

The default configuration also renames or disables `FLUSHALL`, `FLUSHDB`,
`SLAVEOF` and `REPLICAOF`, which are the commands used in cryptojacking attacks.
Rename them back deliberately if you need them.

## Monitoring and HTTP

```toml
[monitoring]
prometheus_enabled = true
prometheus_port = 9121

[http_api]
enabled = true
host = "127.0.0.1"
port = 63790

[web]
port = 63800
```

See [Operations](operations) for the endpoints these expose.

## Hot reload

The config file is watched. Editing it reloads settings without a restart.
Embedding settings are read at startup, so a change to `[ai]` needs a restart.

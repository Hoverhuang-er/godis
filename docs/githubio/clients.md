# Clients and agents

godis speaks the Redis protocol. Any Redis client works, and so does a shell.

## From any Redis client

Knowledge-base commands are ordinary commands, so the usual clients work as-is:

```python
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Retrieval: the server embeds, so the client embeds nothing
context = r.execute_command("RAG.QUERY", "handbook", "how do I tune vector recall?", "K", 3)

# Store an answer so the next caller does not pay for it
r.execute_command("LLMCACHE.SET", "how do I tune vector recall?", answer, "TTL", 86400)
hit, cached, similarity, key = r.execute_command("LLMCACHE.GET", "how do I tune vector recall?")
```

```go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

// Vector search with a binary query vector, as clients normally send it
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

vec := make([]byte, 4*len(embedding))
for i, f := range embedding {
    binary.LittleEndian.PutUint32(vec[i*4:], math.Float32bits(f))
}
res, err := rdb.Do(ctx, "FT.SEARCH", "idx:catalog",
    "(*)=>[KNN 3 @embedding $vec AS score]",
    "PARAMS", 2, "vec", vec,
    "SORTBY", "score", "ASC", "RETURN", 2, "score", "name", "DIALECT", 2).Result()
```

Because embedding happens server-side, a client never needs an embedding model,
an embedding API key, or a library to talk to one.

## From redis-cli

Everything is reachable from a shell, which is what makes godis usable by an agent
with no tooling beyond a terminal:

```bash
redis-cli RAG.QUERY handbook "how is recall traded against build time?" K 3
redis-cli WIKI.INDEX
redis-cli LLMCACHE.GET "how do I tune vector recall?"
```

## For an LLM agent

A knowledge base is only useful to an agent if the agent knows how to reach it and
when to trust it. Both of those are conventions, not code — so they belong in the
agent's instructions.

godis ships one: [`docs/skills/knowledge-base/SKILL.md`](https://github.com/Hoverhuang-er/godis/blob/main/docs/skills/knowledge-base/SKILL.md).
It covers connecting, choosing the right command for a request, reading
similarities before trusting a passage, and the maintenance loop for a wiki.

Point your agent at that file, or copy its rules into your own prompt. The parts
that matter most:

**Check the collection exists before querying it.** `RAG.INFO` answers "is this
populated, and with what model?" in one call.

**Do not present a weak passage as an answer.** The scan always returns `K`
neighbours. When every similarity is low, the right reply is "the knowledge base
does not cover this", not a confident synthesis of unrelated text. That single
habit is the difference between an agent you can trust and one you cannot.

**File good answers back.** An answer produced this session — a comparison, a
decision, a connection — is knowledge. `WIKI.SET` it so the next session starts
from it instead of re-deriving it.

**Remember decisions, not narration.** `MEMORY.ADD` for what a future session
needs: choices, preferences, constraints, environment facts.

## Setting up an agent

```bash
# 1. Run godis where the agent can reach it
docker run -d --name godis -p 6379:6379 ghcr.io/hoverhuang-er/godis:latest

# 2. Point it at a real embedding model for semantic retrieval
#    (edit standalone.toml, then restart)
[ai]
embed_provider = "http"
embed_base_url = "http://localhost:11434/v1"
embed_model    = "nomic-embed-text"
embed_dim      = 768

# 3. Load the corpus
for f in notes/*.md; do
  redis-cli RAG.INGEST kb "$(basename "$f" .md)" "$(cat "$f")" SOURCE "$f" TAG notes
done

# 4. Verify before trusting it
redis-cli RAG.INFO kb
redis-cli RAG.QUERY kb "a question you know the answer to" K 3
```

Step 4 matters. Run a question whose answer you know, and look at the
similarities. If the right passage is not near the top, fix the corpus or the
embedder before you build anything on top of it.

## Auth and TLS

```bash
redis-cli -h host -p 6379 -a "$PASSWORD"
redis://:password@host:6379/0
```

`X-HEADER-AUTHTOKEN` rotates on the HTTP API — fetch it from `POST /api/auth`.

## Compatibility notes

- RESP2 and RESP3 are both supported; `HELLO 3` switches.
- Search replies use the RediSearch RESP2 layout:
  `[total, key, [field, value ...], ...]`.
- `FT.HYBRID` follows the Redis 8.8 command shape.
- Cluster mode is transparent — no slot map, no cluster-aware driver needed.

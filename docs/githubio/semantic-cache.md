# Semantic cache

Most LLM caches are exact-match: the same prompt string returns the same answer.
That misses the common case. "How do I tune vector recall?" and "how do I tune
vector recall for a large index?" are the same question, and only one of them is
in the cache.

A semantic cache matches on meaning. godis embeds the incoming prompt, finds the
closest cached prompt, and returns its answer if the similarity clears a
threshold.

## Storing an answer

```
LLMCACHE.SET prompt response [TTL seconds] [TAG tag ...]
```

```bash
redis-cli LLMCACHE.SET "how do I tune vector recall?" \
  "Raise the candidate window, or move to a graph index for large collections." \
  TTL 86400 TAG vectors
```

`TTL` is a real Redis expiry. When it elapses, the entry stops being served **and**
is evicted from the index, so an expired answer can never come back as a hit.

## Looking up

```
LLMCACHE.GET prompt [THRESHOLD similarity] [TAG tag ...]
```

```
redis> LLMCACHE.GET "how do I tune vector recall?"
HIT
Raise the candidate window, or move to a graph index for large collections.
1.000000
llmc:0a6a9cfe6daf6b76d4cba20c518f9582
```

The reply is always four elements — `HIT` or `MISS`, the answer, the similarity,
and the cache key — so a caller branches on one round trip with no extra
bookkeeping.

A reworded prompt hits the same entry:

```
redis> LLMCACHE.GET "how do I tune vector recall for a large index?"
HIT
Raise the candidate window, or move to a graph index for large collections.
0.781736
llmc:0a6a9cfe6daf6b76d4cba20c518f9582
```

An unrelated prompt misses, and reports how far off it was:

```
redis> LLMCACHE.GET "how do I bake sourdough bread?"
MISS

0.000000
```

### Thresholds

`THRESHOLD` defaults to **0.75**. Lower it to accept looser rewordings and raise it
to insist on a close match:

```bash
redis-cli LLMCACHE.GET "..." THRESHOLD 0.9    # strict
```

The right value depends on your embedder. 0.75 sits in the measured gap the
built-in local embedder leaves between a reworded prompt (~0.78) and an unrelated
one (below 0.2). A remote model separates the two much more sharply, so raise it
towards 0.85–0.9 when using one — a threshold set for the local embedder is too
permissive there.

A threshold that is too low is worse than a cache miss, because it returns a
confident answer to a different question. When in doubt, raise it.

### Tag filtering

Tags scope a lookup to a family of prompts, so two domains cannot cross-hit:

```bash
redis-cli LLMCACHE.SET "what is the rate limit?" "100 rps" TAG api
redis-cli LLMCACHE.GET "what is the rate limit?" TAG billing     # MISS
```

## Measuring

```bash
redis-cli LLMCACHE.STATS
```

```
hits
42
misses
17
hit_rate
0.7119
entries
23
```

Watch `hit_rate` after changing the threshold or the embedding model. A rate that
collapses means the threshold is too strict for the new embedder; a rate that
approaches 1.0 while answers feel wrong means it is too loose.

## Maintenance

```bash
redis-cli LLMCACHE.DEL llmc:0a6a9cfe...    # one entry (a bare prompt works too)
redis-cli LLMCACHE.CLEAR                   # every entry and the counters
```

Clear the cache whenever you change the embedding model: the stored vectors and
the new ones are not comparable, so every lookup would be a meaningless miss.

## What to cache

| Cache | Do not cache |
|---|---|
| stable, general answers | anything time-sensitive |
| expensive retrievals you would repeat | user-specific or session-specific data |
| reference explanations | answers derived from this turn's context |

Always set a `TTL` for anything that could age. A cache that outlives its truth is
worse than no cache, because it is confidently wrong and fast about it.

## Wiring it into a loop

```
1. LLMCACHE.GET prompt
2. HIT  -> return the stored answer, done
3. MISS -> retrieve (RAG.QUERY), generate with your model,
           LLMCACHE.SET prompt answer TTL 86400
4. Record the exchange in MEMORY.ADD if it belongs to this session
```

Step 1 costs one round trip; step 3 costs a model call. On a corpus where users
ask similar questions with different words, the semantic match is what makes the
cache actually work.

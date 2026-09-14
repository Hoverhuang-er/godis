# RAG, LLM Wiki, Agent Memory and Semantic Cache — Design

Date: 2026-09-14
Status: approved (embedder = pluggable local + HTTP; generation stays client-side)
Builds on: `2026-09-14-vector-hybrid-search-design.md`

## Goal

Make godis serve a knowledge base over the Redis protocol, so an agent reaches it
with nothing but `redis-cli`:

1. **RAG** — chunk, embed, retrieve and return citable context from the server.
2. **LLM wiki** — the persistent, interlinked markdown layer an agent maintains,
   with the link graph, catalogue, log and lint that nobody wants to do by hand.
3. **Agent memory** — recent and relevant turns of a session.
4. **Semantic cache** — a reworded prompt reuses a stored answer.

Sources: the Redis RAG quickstart (Redis's role is vector store + semantic cache +
session manager; chunking and embedding are the client's job unless the server
takes them on) and Karpathy's *LLM Wiki* gist (three layers, three operations,
`index.md` and `log.md`).

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Where embedding runs | In the server | `redis-cli RAG.QUERY "..."` must work with no client-side embedding. This is the constraint everything else follows from. |
| Embedding backend | `Embedder` interface: pure-Go local default, OpenAI-compatible HTTP optional | The server has to work offline with zero config, and has to reach real semantics when configured. A misconfigured HTTP provider is fatal, never a silent fallback, because the two vector spaces are incomparable. |
| Generation | Not in the server | The caller is an LLM. Returning assembled context avoids holding a generation key, outbound generation calls and their cost. `LLMCACHE` still stores answers — the agent writes them. |
| Storage | Ordinary Redis hashes under reserved prefixes, indexed by ordinary FT indexes | Persistence, replication and reindexing come from machinery that already exists. The wiki is dumpable, replicable and exportable as files. |
| Embedding on replay | The vector is stored as a field, so AOF replay never re-embeds | Replay must be deterministic — a remote embedder would otherwise give different vectors, or fail offline. |
| Model drift | Schema records dimension; `EnsureIndex` rejects a mismatch | Mixing two vector spaces produces plausible, wrong results. |
| Local embedder quality | Stated plainly in code and docs as lexical, not semantic | It is a hashing vectorizer over unigrams and character trigrams. Calling it "semantic" would be a lie the thresholds then hide. |
| Wiki retrieval | Hybrid: text ranking and vector ranking fused with the same RRF as `FT.HYBRID` | A page that uses the words and a page that means the same thing are both useful. |
| Wiki page granularity | Whole pages, not fragments | A wiki page is already the compiled synthesis; half of one is worse than the whole thing. |

## Architecture

```
internal/ai/                     server-side AI primitives
  embed.go     Embedder, LocalEmbedder (FNV-1a hashing vectorizer, unigrams +
               character trigrams, stop words dropped, L2-normalised)
  http.go      HTTPEmbedder, OpenAI-compatible /embeddings
  chunk.go     markdown-aware chunking; process-wide embedder and chunk options
  config.go    [ai] config -> Options -> Embedder

internal/database/
  kb.go        shared plumbing: hash writes + AOF, index recovery, locking,
               vector encoding, tag filters, markdown rendering
  rag.go       RAG.INGEST / RAG.QUERY / RAG.DEL / RAG.DOCS / RAG.INFO
  wiki.go      WIKI.*  (16 commands)
  memory.go    MEMORY.*
  llmcache.go  LLMCACHE.*
```

### Keys

| Family | Document keys | Metadata | Index prefix |
|---|---|---|---|
| RAG | `rag:{collection}:{id}:{n}` | `ragmeta:{collection}` (id → chunk count) | `rag:{collection}:` |
| Wiki | `wiki:page:{name}` | `wiki:raw:{name}`, `wiki:log:{seq}`, `wiki:logseq` | `wiki:page:` |
| Memory | `mem:{session}:{seq}` | `memseq:{session}` | `mem:` |
| Cache | `llmc:{sha256(prompt)}` | `llmcache_stats` | `llmc:` |

Metadata keys sit deliberately outside their index prefix, or they would show up
as search hits.

### Data flow

```
RAG.INGEST   chunk -> embed (outside the lock) -> HMSET + reindex -> AOF hmset
RAG.QUERY    embed -> KNN over the collection, tag filter, MINSCORE -> context
WIKI.SET     extract [[links]] -> embed -> HMSET (links become a TAG field)
WIKI.QUERY   TermsQuery(text) + KNN -> RRF -> whole pages
WIKI.INDEX   read the index -> group by category -> index.md
WIKI.LINT    orphans, broken links, missing summaries, duplicate titles -> log
MEMORY.GET   walk the seq counter backwards for the recent window
MEMORY.SEARCH KNN filtered by the session tag
LLMCACHE.GET embed -> KNN k=1 -> confirm the key still exists -> HIT/MISS
```

## Two failures this design had to fix at the root

**AOF rewriting discards every index.** `aof/marshal.go` regenerates the log from
data entities alone, so the `FT.CREATE` that built an index never survives a
rewrite. Every knowledge-base schema is fixed by its command family, so
`indexFor` rebuilds the index from the keys already present when it finds it
missing. Without this, a knowledge base comes back after `BGREWRITEAOF` with its
documents intact and its search permanently broken.

**Expiring a key left it searchable.** `db.Remove` did not touch the indexes;
only `DEL` did. A cached answer whose TTL elapsed stayed a KNN candidate, so
`LLMCACHE.GET` could return an answer the database no longer held. `db.Remove` is
the single choke point every removal routes through, so the index cleanup moved
there.

## The local embedder, measured

Similarity bands (512 dims, cosine), which every threshold depends on:

| Pair | Similarity |
|---|---|
| identical prompt | 1.00 |
| reworded prompt | 0.78 |
| question vs passage that answers it | 0.50 |
| short query vs passage sharing one word | 0.22 |
| unrelated question vs passage | 0.00–0.12 |

The weakest related pair (0.22) still clears the strongest unrelated pair (0.12),
and a test pins that gap: remove the stop-word filtering and unrelated text jumps
back to ~0.54, overlapping the related band and making every threshold
unusable. The bands are narrow because the model is lexical, which is why
`MINSCORE` defaults to 0 — a floor tuned for the local embedder would silently
drop legitimate hits, and a floor tuned for a remote model would be far too low
here. Defaults that depend on the provider are documented at each site.

`LLMCACHE`'s threshold is the exception, since a cache needs a decision: 0.75
sits in the measured gap, with a note to raise it towards 0.85–0.9 for a remote
model.

## Error handling

- No matches is a *result*, not an error: `RAG.QUERY` returns "No passages
  matched the query", `WIKI.QUERY` "No wiki page matched the question".
- A missing collection, a misconfigured embedder, a dimension mismatch and a
  malformed argument are command errors with actionable text.
- Embedding failure fails the command rather than returning an empty result,
  which a caller would read as "no matches".
- `WIKI.SOURCE.ADD` refuses to overwrite an existing source: immutability is the
  property that makes raw sources ground truth.

## Testing

- `internal/ai/ai_test.go` — determinism across instances (the property that
  makes stored vectors reusable), morphology, chunk sizes, the heading
  breadcrumb, oversized paragraphs, and the related/unrelated separation bands.
- `internal/database/kb_test.go` — RAG ingest/query/re-ingest/delete, the wiki
  lifecycle (links, backlinks, index, log, lint, sources, export), memory, the
  cache including TTL expiry, AOF ordering (index creation before documents), and
  recovery after every index is lost.

## Known limits

- Chunk embeddings are stored both in the hash and in the index's in-memory field
  map, so a collection costs roughly twice its vector bytes in RAM.
- One lock per command family: writes serialise across all collections of a
  family. Embedding happens outside the lock.
- The wiki's vector is built from title, summary and the first ~2000 characters,
  so retrieval is driven by a page's opening rather than its whole body.
- Contradiction detection — the one lint check worth having — needs an LLM and is
  not attempted; `WIKI.LINT` covers only what is provable from the data.

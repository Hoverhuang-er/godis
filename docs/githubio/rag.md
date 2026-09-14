# RAG

Retrieval-augmented generation in three steps: **retrieve** relevant passages,
**augment** a prompt with them, **generate** an answer.

godis does the first two. It chunks your documents, embeds them, stores them, and
returns the passages that match a question — with the source to cite. Generation
stays with your model, because your model is already running and no generation API
key belongs inside a database.

## Why the server does the embedding

The usual RAG pipeline lives in the client: chunk, embed, upsert, query. That
means every consumer needs an embedding library, a model, and credentials.

godis embeds inside the server process instead, so a knowledge base is reachable
with nothing but the Redis protocol:

```
redis> RAG.QUERY handbook "how is recall traded against build time?" K 3
```

This is what makes the knowledge base usable from `redis-cli`, from any Redis
client in any language, and from an LLM agent with no tooling beyond a shell.

## Ingesting documents

```
RAG.INGEST collection id text [TITLE title] [SOURCE source] [TAG tag ...]
```

```bash
redis-cli RAG.INGEST handbook hnsw \
  "$(cat hnsw.md)" \
  TITLE "HNSW index" SOURCE handbook.md TAG vectors engineering
```

- `collection` groups documents so you can query, filter and delete them
  together.
- `id` identifies the document. Ingesting the same id again **replaces** it,
  including removing chunks the shorter version no longer has.
- `TITLE` and `SOURCE` are carried onto every chunk, so retrieved passages are
  self-describing.
- `TAG` values are how you restrict a later query to a subset.

The reply is the number of chunks stored.

### How text is chunked

`RAG.INGEST` is markdown-aware. It packs paragraphs up to `chunk_size`
characters, overlaps consecutive chunks by `chunk_overlap`, and prefixes each
chunk with its heading breadcrumb — so a passage lifted out of a long document
still carries its section title.

Tune `chunk_size` and `chunk_overlap` in the `[ai]` config section
(see [Configuration](configuration)).

## Querying

```
RAG.QUERY collection text [K n] [TAG tag ...] [MINSCORE score] [FORMAT CONTEXT|DETAIL]
```

```bash
redis-cli RAG.QUERY handbook "how is recall traded against build time?" K 3
```

```
# Retrieved context

Query: how is recall traded against build time?

### [1] HNSW index
_source: handbook.md · similarity: 0.624 · hnsw · chunk 0_

HNSW builds a multi-layer proximity graph. Recall is traded against build time
through efConstruction and efRuntime.
```

Pass that block to your model as context. It already carries the query, the
passages, the similarity of each, and the source to cite.

- `K` — how many passages to retrieve. Default 5.
- `TAG` — restrict to documents carrying a tag.
- `MINSCORE` — drop passages below a similarity floor.
- `FORMAT DETAIL` — a structured reply (`[total, key, score, fields, ...]`)
  instead of the markdown context block, for programmatic use.

### Reading similarities

The scan always returns `K` neighbours, however far away they are, so `K` alone
does not mean the collection covers the question. Check the similarities:

| Similarity | Reading |
|---|---|
| above ~0.5 (remote model) or ~0.2 (local embedder) | the passage is about the question |
| below that | the collection probably does not cover it |

When every hit is weak, **say so** rather than synthesising an answer from
unrelated text. An agent that reports "the knowledge base does not cover this" is
far more useful than one that confabulates.

Set a floor to make that automatic:

```bash
redis-cli RAG.QUERY handbook "how is recall traded against build time?" MINSCORE 0.2
# No passages matched the query.
```

The thresholds differ between the local embedder and a remote model, so
`MINSCORE` defaults to 0 and the floor is yours to set. See
[Configuration](configuration) for why the local embedder's scores run low.

## Inspecting and maintaining

```bash
redis-cli RAG.INFO handbook     # documents, chunks, dim, distance metric, embedder
redis-cli RAG.DOCS handbook     # document ids and their chunk counts
redis-cli RAG.DEL handbook hnsw # remove a document and all of its chunks
```

`RAG.INFO` is the fastest way to answer "is this collection populated, and with
what embedding model?" before trusting a query against it.

## A complete example

```bash
# 1. Ingest a corpus
for f in docs/*.md; do
  redis-cli RAG.INGEST handbook "$(basename "$f" .md)" "$(cat "$f")" \
    TITLE "$(basename "$f" .md)" SOURCE "$f" TAG engineering
done

# 2. Check it landed
redis-cli RAG.INFO handbook

# 3. Retrieve
redis-cli RAG.QUERY handbook "how do I tune vector recall?" K 5 MINSCORE 0.2
```

## Cache it

Retrieval over a large corpus is the expensive path. Once your model has produced
an answer, store it so the same question is cheap next time — including when it is
asked differently. See [Semantic cache](semantic-cache).

## Relationship to the wiki

RAG retrieves from what you ingested. The [LLM wiki](llm-wiki) keeps a maintained
synthesis on top of it. Use RAG for source material, the wiki for conclusions —
and file good answers back into the wiki so they compound.

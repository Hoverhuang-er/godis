---
name: knowledge-base
description: "Query and maintain a godis-backed knowledge base from an agent: retrieval-augmented context (RAG.QUERY), a maintained markdown wiki (WIKI.*), long-term agent memory (MEMORY.*), and a semantic answer cache (LLMCACHE.*). Use when the user asks to look something up in a knowledge base, to remember or recall something across sessions, to ingest a document into a knowledge base, or to maintain a wiki of notes."
---

# Knowledge base on godis

godis serves a knowledge base over the ordinary Redis protocol. Embedding runs
inside the server, so a plain `redis-cli` call retrieves by meaning — no client
library, no embedding code, no API key in the agent.

## Connecting

```bash
R="redis-cli -p ${GODIS_PORT:-6379}"          # add -a "$GODIS_PASSWORD" if set
$R PING                                       # expect PONG
```

If `PING` fails, godis is not running. Say so rather than inventing an answer.

## Pick the right command

| The user wants | Use |
|---|---|
| an answer grounded in ingested documents | `RAG.QUERY` |
| an answer from the maintained wiki | `WIKI.QUERY` |
| to add a document to the knowledge base | `RAG.INGEST` |
| to record a source and see what it affects | `WIKI.INGEST` |
| to write or revise a wiki page | `WIKI.SET` |
| to check the wiki's health | `WIKI.LINT` |
| what was said earlier in this session | `MEMORY.GET` / `MEMORY.SEARCH` |
| to remember this for later sessions | `MEMORY.ADD` |
| to avoid regenerating a known answer | `LLMCACHE.GET` / `LLMCACHE.SET` |

## Retrieval

`RAG.QUERY` returns a markdown context block with a similarity per passage. Read
it, then answer from it. Cite the `source:` lines you relied on.

```bash
$R RAG.QUERY kb "how is recall traded against build time?" K 5
$R RAG.QUERY kb "exact recall" TAG vectors          # restrict to a tag
$R RAG.QUERY kb "index types" MINSCORE 0.2          # drop weak matches
```

`MINSCORE` defaults to 0, and the scan always returns `K` neighbours however far
away they are. So inspect the similarities: if every hit is low (below roughly
0.2 with the built-in embedder, below 0.5 with a remote model), the collection
does not cover the question. **Say that** instead of synthesising an answer from
unrelated passages.

Check what exists before querying an unfamiliar name:

```bash
$R RAG.INFO kb          # documents, chunks, dim, embedder
$R RAG.DOCS kb          # document ids and their chunk counts
```

## Ingesting a document

```bash
$R RAG.INGEST kb proj-notes "$(cat notes.md)" TITLE "Project notes" SOURCE notes.md TAG engineering
```

The server chunks the text, embeds each chunk, and indexes it. Re-ingesting the
same id replaces the document, including removing chunks the shorter version no
longer has. `TAG` values are how you restrict later queries.

## The wiki

A wiki is a set of interlinked markdown pages the agent maintains. The server
keeps the link graph, the catalogue and the log; you write the prose.

**Ingest a source** — file it, then revise the pages it touches:

```bash
$R WIKI.INGEST llm-wiki "$(cat article.md)" TITLE "The LLM wiki pattern"
# -> source stored, log entry written, and a checklist of pages already using
#    related terms. Revise those pages with WIKI.SET.
```

**Write a page** — `[[Page Name]]` links are extracted automatically:

```bash
$R WIKI.SET hnsw 'A layered proximity graph, used by [[vector-search]].
Recall is traded against build time via efConstruction.' \
  TITLE "HNSW" CATEGORY search TAG vectors SUMMARY "Graph index with tunable recall"
```

Always pass `SUMMARY`: it is what the catalogue shows and what retrieval surfaces.

**Read, navigate, maintain:**

```bash
$R WIKI.QUERY "how is recall traded against build time" K 3   # wording + meaning
$R WIKI.QUERY "index types" K 5 FORMAT FULL                   # whole page bodies
$R WIKI.GET hnsw
$R WIKI.LINKS hnsw             # what this page points at
$R WIKI.BACKLINKS hnsw         # what points at this page
$R WIKI.INDEX                  # the catalogue: read this first on an unfamiliar wiki
$R WIKI.LIST CATEGORY search
$R WIKI.LINT                   # orphans, broken links, missing summaries, duplicate titles
$R WIKI.LOG 10                 # recent history, newest first
```

**Answering from the wiki:** read `WIKI.INDEX` first when you do not know the
wiki, then `WIKI.QUERY`, then `WIKI.GET` the pages you need. File a good answer
back as a new page with `WIKI.SET` — that is what makes the wiki compound instead
of being re-derived every session.

**Raw sources are immutable.** `WIKI.SOURCE.ADD` refuses to overwrite; that
refusal is the point, since the sources are the ground truth. Pass `FORCE` only
when the user explicitly wants a source replaced.

**Exporting for git:**

```bash
$R WIKI.EXPORT PREFIX wiki/ > wiki-dump.md
```

## Agent memory

```bash
$R MEMORY.ADD session-42 user "the deploy key rotates every 90 days"
$R MEMORY.ADD session-42 assistant "noted" TAG ops
$R MEMORY.GET    session-42 LIMIT 10          # recent turns, conversation order
$R MEMORY.GET    session-42 ROLE assistant    # only one side
$R MEMORY.SEARCH session-42 "when does the deploy key rotate" K 3   # relevant turns, best first
```

`MEMORY.GET` is the recent window; `MEMORY.SEARCH` reaches back further by
meaning. Record decisions, user preferences and facts the user will expect you to
remember — not play-by-play narration.

## Semantic answer cache

Check before generating an expensive answer, store after:

```bash
$R LLMCACHE.GET "how do I tune vector recall?"
# HIT  <answer>  0.912  llmc:...
# or
# MISS (empty)   0.031  (empty)

$R LLMCACHE.SET "how do I tune vector recall?" "<answer>" TTL 86400 TAG vectors
$R LLMCACHE.STATS
```

A `HIT` means a prompt that *means the same thing* was answered before, so the
stored answer is reusable. A `MISS` gives you the closest similarity. Cache only
answers that are stable — not anything time-sensitive, user-specific, or derived
from this turn's context. Use `TTL` so stale answers expire.

## Rules

- Never present a low-similarity passage as if it answered the question.
- Never invent an `RAG.INFO`, `WIKI.INDEX` or `LLMCACHE.STATS` result; run it.
- The wiki's prose is yours to write, but its bookkeeping is the server's — do not
  hand-maintain `WIKI.INDEX` or the log.
- If a command returns an error, report the error text. Do not silently fall back
  to answering from your own memory as though it came from the knowledge base.

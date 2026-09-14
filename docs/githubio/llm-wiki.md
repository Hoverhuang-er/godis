# LLM wiki

RAG retrieves fragments of your sources on every question. A wiki does something
different: an LLM **incrementally builds and maintains a persistent, interlinked
set of pages** sitting between your raw sources and your questions. Knowledge is
compiled once and kept current, instead of being rediscovered on every query.

godis implements the pattern. The pages are markdown; the bookkeeping — the link
graph, the catalogue, the history, the health checks — is done by the server, so
the agent only has to write prose.

## Three layers

| Layer | Who owns it | Where it lives |
|---|---|---|
| Raw sources | immutable, never edited | `WIKI.SOURCE.ADD` |
| The wiki | written and maintained by the agent | `WIKI.SET` |
| The schema | the conventions you agree on with your agent | your own `AGENTS.md` / prompt |

## Writing pages

```
WIKI.SET page content [TITLE title] [SUMMARY summary] [CATEGORY category] [TAG tag ...]
```

```bash
redis-cli WIKI.SET vector-search \
  '## Overview
Vector search matches on meaning rather than exact words. It depends on [[embeddings]]
and, for large collections, on [[hnsw]].

## Metrics
Cosine distance decides what "near" means.' \
  TITLE "Vector search" CATEGORY search TAG retrieval \
  SUMMARY "Matching documents by meaning instead of exact words"
```

`[[Page Name]]` links are extracted automatically and become a queryable graph.
Page names are normalised — `[[Vector Search]]`, `[[vector-search]]` and
`vector_search` all refer to the same page.

Pass `SUMMARY`: it is what the catalogue displays and what retrieval surfaces.

## Reading and navigating

```bash
redis-cli WIKI.GET vector-search            # the page body
redis-cli WIKI.GET vector-search FIELD summary
redis-cli WIKI.LIST                         # every page
redis-cli WIKI.LIST CATEGORY search         # narrowed to a category
redis-cli WIKI.LINKS vector-search          # pages this one points at
redis-cli WIKI.BACKLINKS vector-search      # pages pointing at this one
redis-cli WIKI.DEL vector-search
```

## Generating index.md

`index.md` is the catalogue: every page with a one-line summary, grouped by
category. It is the pattern's first retrieval step, and at moderate scale — a few
hundred pages — it is often the *only* one you need.

```bash
redis-cli WIKI.INDEX
```

```markdown
# Wiki index

12 pages in 3 categories.

## search

- [[hnsw]] — A layered proximity graph with tunable recall
- [[vector-search]] — Matching documents by meaning instead of exact words
```

The summary is the one you passed to `WIKI.SET`, or the first line of prose in
the page.

## The log

The log is append-only and chronological. Entries start with a consistent prefix
so the exported file stays greppable with ordinary tools:

```bash
redis-cli WIKI.LOG 10
redis-cli WIKI.LOG.ADD query "Why is recall low on the large index?" BODY "Led to [[hnsw]]."
```

```markdown
## [2026-09-14] ingest | Attention Is All You Need

source: `attention-paper`
```

Once exported, `grep "^## \[" log.md | tail -5` gives you the last five entries.

## Ingesting a source

```
WIKI.INGEST name content [TITLE title]
```

Ingesting is a write to *several* pages at once, so the writing stays with the
agent. What the server does is the part that gets forgotten: file the immutable
source, append the log entry, and surface the pages already using related terms.

```bash
redis-cli WIKI.INGEST attention-paper "$(cat attention.md)" TITLE "Attention Is All You Need"
```

```
# Ingest checklist for `attention-paper`

Raw source stored. The log entry is written.

Pages already using related terms; review each and update or contradict it:

- [[embeddings]]
- [[vector-search]]
```

That checklist is the difference between a wiki that stays current and one that
rots: every ingest tells you which existing conclusions to revisit.

## Raw sources

Sources are immutable, and re-adding one is refused:

```bash
redis-cli WIKI.SOURCE.ADD paper "the source text"   # OK
redis-cli WIKI.SOURCE.ADD paper "revised"           # ERR already exists
redis-cli WIKI.SOURCE.ADD paper "revised" FORCE     # replace deliberately
redis-cli WIKI.SOURCE.GET paper
redis-cli WIKI.SOURCE.LIST
```

That refusal is the point. The sources are your ground truth, and a knowledge base
whose ground truth can be silently rewritten cannot be trusted.

## Querying

```
WIKI.QUERY text [K n] [MINSCORE score] [CATEGORY category] [FORMAT CONTEXT|FULL]
```

```bash
redis-cli WIKI.QUERY "how is recall traded against build time" K 3
redis-cli WIKI.QUERY "recall" K 2 FORMAT FULL     # whole page bodies
```

Retrieval is hybrid — the words a page uses *and* what it means — and whole pages
come back rather than fragments, because a wiki page is already the compiled
synthesis.

Read `WIKI.INDEX` first when you do not know a wiki, then `WIKI.QUERY`, then
`WIKI.GET` the pages you need.

## Filing answers back

The most valuable habit in this pattern: when a question produces a good answer —
a comparison, an analysis, a connection you only just noticed — write it back as a
page.

```bash
redis-cli WIKI.SET recall-vs-latency \
  'Built while comparing [[hnsw]] and [[flat-index]] for the catalog service.' \
  TITLE "Recall vs latency" CATEGORY decisions SUMMARY "What we chose and why"
```

Otherwise it disappears into a chat log, and the next session re-derives it.

## Linting

```bash
redis-cli WIKI.LINT
```

Reports, with a count for each:

- **Orphan pages** — nothing links to them. Link them in or merge them.
- **Broken links** — `[[links]]` pointing at pages that do not exist.
- **Missing summaries** — pages with no `SUMMARY`.
- **Duplicated titles** — two pages with the same title, candidates for merging.

Each pass appends a log entry, so you can see whether the wiki is getting healthier.

Lint cannot check the one thing that matters most — whether a page *contradicts*
another. That needs judgement, so it is the agent's job, not the server's:
after an ingest, read the pages the checklist named.

## Exporting

The wiki is markdown. Export it and keep it in git for version history, diffs and
review:

```bash
redis-cli WIKI.EXPORT PREFIX wiki/ > wiki-dump.md
```

Output is front-mattered files, one per page, with `title`, `category`, `tags` and
`updated`:

```markdown
----- wiki/hnsw.md
---
title: HNSW
category: search
tags: retrieval
updated: 1789385767
---

A layered proximity graph...
```

## A working loop

```bash
# 1. Read the catalogue
redis-cli WIKI.INDEX

# 2. Ask
redis-cli WIKI.QUERY "how is recall traded against build time" K 3 FORMAT FULL

# 3. Answer, then file the answer back
redis-cli WIKI.SET recall-tradeoffs '...' TITLE "Recall tradeoffs" CATEGORY search

# 4. Health check
redis-cli WIKI.LINT

# 5. Export for git
redis-cli WIKI.EXPORT PREFIX wiki/ > wiki-dump.md
```

## Why this beats plain RAG

The tedious part of a knowledge base is not the reading or the thinking — it is
the bookkeeping. Keeping cross-references current, noticing that a new source
contradicts an old claim, making sure nothing has drifted. Humans abandon wikis
because the maintenance burden grows faster than the value.

The [Agent memory](agent-memory) commands cover the other half of an agent's
state: what happened in *this* conversation.

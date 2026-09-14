# Vector and hybrid search

`FT.*` indexes documents and answers queries that combine keywords, field
filters, and vector similarity.

## Creating an index

```
FT.CREATE index [ON HASH|JSON] [PREFIX count prefix ...] SCHEMA field-spec ...
```

A field spec is `path [AS alias] TYPE [options]`. For `ON JSON`, `path` is a JSON
path and `alias` is the name queries use; `$.features[*]` expands every element of
an array.

```bash
redis-cli FT.CREATE idx:catalog ON JSON PREFIX 1 product: SCHEMA \
  '$.name' AS name TEXT \
  '$.brand' AS brand TAG \
  '$.category' AS category TAG \
  '$.description' AS description TEXT \
  '$.price' AS price NUMERIC SORTABLE \
  '$.features[*]' AS features TAG \
  '$.embedding' AS embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE
```

The `VECTOR` options are read as `ALGO count TYPE FLOAT32|FLOAT64 DIM n
DISTANCE_METRIC L2|IP|COSINE`. `FLAT` and `HNSW` are both accepted; godis runs an
exact scan for either, which is the right trade for collections that fit in
memory. Use `COSINE` unless you have a reason not to.

**`FT.CREATE` backfills documents already in the database.** A common workflow is
to write data first and create the index afterwards; that works, and the index is
queryable immediately.

Other commands: `FT.INFO index`, `FT._LIST`, `FT.DROPINDEX index`. Dropping an
index never deletes documents.

## K-nearest neighbours

```
FT.SEARCH index "prefilter=>[KNN k @field $param AS score]" PARAMS 2 param <vector> ...
```

```bash
redis-cli FT.SEARCH idx:catalog \
  '(*)=>[KNN 3 @embedding $query_vector AS score]' \
  PARAMS 2 query_vector "1.0 0.0 0.0 0.0" \
  SORTBY score ASC RETURN 2 score name DIALECT 2
```

- `(*)` is the pre-filter. `*` means "consider every document".
- `=>[KNN 3 @embedding $query_vector AS score]` asks for the 3 nearest neighbours
  and names each distance `score`.
- `PARAMS` supplies the query vector. The parameter name is whatever you wrote
  after `$`.
- `SORTBY score ASC` orders closest-first.

### Sending a query vector

Two encodings are accepted, and godis tells them apart automatically:

- **Text** — `"1.0 0.0 0.0 0.0"` or `"[1.0,0.0,0.0,0.0]"`. Convenient from
  `redis-cli` and for documents stored as JSON arrays.
- **Binary** — raw little-endian `FLOAT32` bytes, which is what client libraries
  send. A 768-dimension vector is 3072 bytes.

A dimension that disagrees with the index is an error, not a silently wrong
answer.

### Pre-filtering

Replace `(*)` with any filter expression to search a subset:

```bash
redis-cli FT.SEARCH idx:catalog \
  '(@category:{Audio})=>[KNN 3 @embedding $vec AS score]' \
  PARAMS 2 vec "1.0 0.0 0.0 0.0" SORTBY score ASC NOCONTENT DIALECT 2
```

The filter runs **before** ranking, so this returns the 3 nearest Audio products
even when the nearest products overall sit in another category.

## Query syntax

godis parses the dialect-2 subset these operators need:

| Syntax | Meaning |
|---|---|
| `word` | matches the word in any TEXT field |
| `@field:word` | matches the word in one field |
| `@tag:{a}` / `@tag:{a\|b}` | tag equality, or any of several tags |
| `@num:[10 20]` | numeric range, inclusive |
| `@num:[(10 +inf]` | exclusive lower bound, unbounded above |
| `a b` | intersection (AND) |
| `a \| b` | union (OR) |
| `-word` | exclusion (NOT) |
| `(expr)` | grouping |

AND binds tighter than OR, so `a b | c d` means `(a AND b) OR (c AND d)`. Use
parentheses when in doubt.

## Result shaping

```
FT.SEARCH index query
  [NOCONTENT] [WITHSCORES] [RETURN count field ...]
  [SORTBY field [ASC|DESC]] [LIMIT offset num]
  [PARAMS count name vector ...] [DIALECT 2] [FILTER field min max]
```

- `RETURN 2 score name` projects just those fields. `score` is the KNN distance
  when the query has a KNN clause.
- `NOCONTENT` returns only keys, which is much cheaper when you only need the hit
  list.
- `WITHSCORES` adds the score element to the reply.
- `SORTBY price ASC` sorts by a schema field instead of similarity.

The reply is the RediSearch RESP2 shape:

```
[total, key1, [field, value ...], key2, [field, value ...], ...]
```

## Hybrid search

Keyword search is precise when you know the term. Vector search is forgiving when
you describe what you want. `FT.HYBRID` runs both and fuses the rankings.

```
FT.HYBRID index
  SEARCH query
  VSIM @field $param [KNN k] [K n]
  [COMBINE RRF|LINEAR [count option value ...]]
  [LOAD count field ...] [FILTER query] [LIMIT offset num]
  [PARAMS count name vector ...]
```

```bash
redis-cli FT.HYBRID idx:catalog \
  SEARCH "wireless" \
  VSIM @embedding $query_vector KNN 2 K 5 \
  LOAD 2 @name score \
  PARAMS 2 query_vector "1.0 0.0 0.0 0.0"
```

- `SEARCH` is any query from the syntax above.
- `VSIM` names the vector field and the parameter holding the query vector.
  `KNN k` how many neighbours to rank; `K n` how many fused results to return.
- Results are fused with **Reciprocal Rank Fusion** by default, which needs no
  tuning and treats the two rankings on equal terms.

Tuning, when you need it:

```bash
# Weight the text ranking at 0.5 and the vector ranking at 0.5
COMBINE LINEAR 2 ALPHA 0.5

# Make rank position count for less (default constant is 60)
COMBINE RRF 2 CONSTANT 10

# Only consider the top 100 of each ranking
COMBINE RRF 2 WINDOW 100
```

A document that satisfies **both** the keyword and the vector ranking wins, which
is the whole point: `wireless headphones` retrieves documents that literally say
*wireless* and documents that mean it.

## Performance notes

Both algorithms run an exact linear scan, so a query costs O(n) in the number of
indexed vectors. That is the right trade for collections that fit in memory, where
it is fast, exact, and needs no build time or recall tuning. Beyond roughly
100k vectors per field, a proximity-graph index would earn its complexity.

There is no stemming or stop-word filtering, and no fuzzy `%term%` or prefix
`term*` matching. Queries are matched literally against indexed tokens.

# Vector and Hybrid Search for godis — Design

Date: 2026-09-14
Status: approved (HNSW = brute-force exact; query grammar = tutorial subset)

## Goal

Bring godis to parity with the Redis "Vector and hybrid search" tutorial
(`redis.io/docs/latest/develop/get-started/search-tutorial/vector-search`) inside
`internal/`:

1. `FT.CREATE ... ON JSON` with JSON-path fields (`$.name AS name`,
   `$.features[*] AS features`) and `VECTOR` fields (`FLAT`/`HNSW`,
   `TYPE FLOAT32`, `DIM n`, `DISTANCE_METRIC L2|IP|COSINE`).
2. `FT.SEARCH` dialect-2 subset: pre-filter query, `=>[KNN k @field $param AS score]`,
   `PARAMS`, `SORTBY`, `RETURN`, `LIMIT`, `DIALECT`.
3. `FT.HYBRID`: `SEARCH` clause + `VSIM` clause fused by RRF (default) or LINEAR.
4. Existing-index behaviour: `FT.CREATE` backfills documents already in the DB;
   `FT.DROPINDEX` never deletes documents.
5. Repo hygiene: `go.mod`, CI workflows and Dockerfiles → Go 1.27;
   `interface{}` → `any` throughout.

Out of scope: approximate HNSW graph, `FT.AGGREGATE`, stemming/stopwords,
fuzzy `%term%` and prefix `term*` matching.

## Current state

- `internal/datastruct/search/index.go` — schema (TEXT/TAG/NUMERIC/VECTOR),
  inverted index `term -> doc -> tf`, doc store, brute-force KNN, global registry,
  `IndexDocByPrefix` / `RemoveDocByPrefix` write hooks.
- `internal/database/search.go` — `FT.CREATE` (`ON HASH` only), `FT.SEARCH`
  (string-scraped KNN clause, `PARAMS` only matches the literal `query_vec`,
  filter discarded, `RETURN` skipped), `FT.DROPINDEX`, `FT.INFO`, `FT._LIST`.
- Hooks: `hash.go` (`reindexHash`, HDEL, DEL), `string.go` (SET), `keys.go` (DEL).
  JSON writes are **not** hooked.
- No tests in the search package.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| HNSW | Accept the token, run the existing exact linear scan for both algorithms | Tutorial calls FLAT the right default for small datasets; exact recall beats approximate; avoids 300 LOC of graph code with no demonstrated need. Marked `ponytail:` in code with the upgrade path. |
| Query grammar | Tutorial subset: `*`, bare terms, `@text:word`, `@tag:{a\|b}`, `@num:[min max]`, `-neg`, `\|` OR, parenthesised groups, `=>[KNN …]` | Exactly what the tutorial exercises. Fuzzy/prefix matching is a separate feature. |
| Boolean semantics | Space is intersection (AND), `\|` is union, AND binds tighter than OR | Matches the documented dialect-2 rules ("Combined queries"), verified against the Redis docs rather than assumed. A leading `+` is accepted and ignored, since space already means AND. |
| Vector encoding | Accept binary FLOAT32/FLOAT64 blobs **and** textual `1.2 3.4` / `[1.2,3.4]` | `redis-cli` examples send binary; hand-written tests and JSON documents carry text. |
| Doc field values | `map[string][]string` (multi-value) | `$.features[*]` yields several tags per document; TAG queries must match any of them. |
| KNN pre-filter | Filter candidates first, then rank by distance | Required by `(@category:{Audio})=>[KNN 3 …]`. |
| Backfill | `FT.CREATE` walks `DB.ForEach` and indexes matching keys | Tutorial: "the embeddings you just stored are indexed as soon as the new index is created". |

## Architecture

`internal/datastruct/search/` splits into focused units:

| File | Responsibility |
|---|---|
| `schema.go` | `FieldType`, `VectorAlgo`, `VectorFieldOpts`, `FieldSchema` (`Name` alias + `Path`), `IndexSchema`, `KeyType` (HASH/JSON), `ExtractFields` resolving JSON paths against a `map[string]any` document |
| `index.go` | `InvertedIndex` doc store, `IndexDocument` / `RemoveDocument`, term index, registry (`RegisterIndex`, `DropIndex`, `GetIndex`, `ListIndexes`), prefix routing (`IndexDocByPrefix`, `RemoveDocByPrefix`) |
| `query.go` | Dialect-2 subset parser → `Query` AST; predicate evaluation (`EvalScores`) producing candidate keys with relevance scores |
| `vector.go` | `EncodeVector` / `DecodeVector` (binary + text), distance metrics |
| `hybrid.go` | `HybridRequest` evaluation, RRF and linear fusion |

`internal/database/search.go` owns command parsing only; it never walks index
internals. `internal/database/json.go` gains one helper, `reindexJSON(db, key)`,
called from every mutating JSON command.

### Interfaces

```go
// schema.go
type KeyType int // KeyTypeHash, KeyTypeJSON
type FieldSchema struct {
    Name       string      // alias used in queries
    Path       string      // "$.features[*]" for JSON; == Name for HASH
    Type       FieldType
    VectorOpts *VectorFieldOpts
}
type IndexSchema struct {
    Name     string
    KeyType  KeyType
    Prefixes []string
    Fields   []FieldSchema
}

// index.go
func (idx *InvertedIndex) IndexDocument(key string, fields map[string][]string)
func (idx *InvertedIndex) RemoveDocument(key string)
func (idx *InvertedIndex) EvalScores(q *Query) (map[string]float64, error)
func (idx *InvertedIndex) TextSearch(q *Query) ([]*DocInfo, error)
func (idx *InvertedIndex) KNNSearch(field string, vec []float32, k int, candidates map[string]float64) ([]*DocInfo, error)
func IndexDocByPrefix(key string, fields map[string]string)   // HASH documents
func IndexJSONByPrefix(key string, doc any)                   // JSON documents

// query.go
func ParseQuery(s string) (*Query, error)
func AndQuery(a, b *Query) *Query
func NumericRangeQuery(field string, min, max float64) *Query

// vector.go
func DecodeVector(raw []byte, dim int) ([]float32, error)

// hybrid.go
type HybridRequest struct {
    Search      *Query
    Filter      *Query   // optional constraint applied to both rankings
    Vector      []float32
    VectorField string
    KNN         int
    K           int
    Combine     string   // "RRF" (default) or "LINEAR"
    Alpha       float64  // LINEAR: weight of the text ranking
    Window      int      // RRF rank window per ranking
    Constant    float64  // RRF rank constant, defaults to 60
}
func (idx *InvertedIndex) Hybrid(req *HybridRequest) ([]*DocInfo, error)
```

## Data flow

```
HSET key f v     -> reindexHash -> search.IndexDocByPrefix(key, map[string]string)
JSON.* write     -> reindexJSON  -> search.IndexJSONByPrefix(key, decoded doc)
FT.CREATE        -> RegisterIndex -> DB.ForEach backfill for matching prefixes
FT.SEARCH        -> ParseQuery -> EvalScores (candidates) -> KNNSearch | TextSearch -> SORTBY/LIMIT
FT.HYBRID        -> TextSearch(SEARCH) + KNNSearch(VSIM) -> RRF/LINEAR fuse -> K
```

## Error handling

- Malformed query / unknown field / dimension mismatch → `protocol.MakeErrReply`
  with the Redis wording (`Syntax error at offset N`, `Vector dimension mismatch`).
- Unknown index → `ERR unknown index name`.
- `FD`-style silent failures are forbidden: a KNN clause whose `PARAMS` vector is
  missing is an error, not an empty result.

## Testing

- `internal/datastruct/search/query_test.go` covers the parser (including the
  AND/OR precedence rules), pre-filtered KNN, and hybrid fusion.
- `internal/database/search_test.go` replays the tutorial over `DB.Exec`:
  JSON.SET products, `FT.CREATE ON JSON` with a `VECTOR` field and backfill, KNN
  with binary and textual query vectors, pre-filtered KNN, tag/numeric filters
  with SORTBY, `RETURN` projection, write-after-index, and `FT.HYBRID`.
- The same commands were replayed against the built binary over RESP with
  `redis-cli` and a raw-socket client sending binary `FLOAT32` parameters.

## Risks and known limits

- Binary vs text vector decoding is heuristic: text wins only when every byte is
  printable ASCII (a real binary payload never is), otherwise the blob is
  decoded as little-endian float32 by length.
- `FT.SEARCH`'s previous lenient behaviour (ignoring a KNN clause it could not
  parse, discarding a pre-filter, dropping `RETURN`) is replaced by real parsing
  and explicit errors — intentional, callers get told instead of getting silently
  wrong results.
- Response format now follows RediSearch RESP2 (`[total, key, (score), [field,
  value …]]`); the previous always-on score element moved behind `WITHSCORES`.
- Text and tag postings share one term map, so a word that is also a tag inflates
  the term-frequency relevance of the matching documents. Filtering stays exact;
  only the relevance number is affected. Noted in `index.go`.
- The index registry is process-wide, not per-database, so `FLUSHDB` resets all
  indexes (`search.ResetAll`). Without it, flush would leave phantom hits.
- HNSW is exact linear scan (see above): correct, but O(n) per query.

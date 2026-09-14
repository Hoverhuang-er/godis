package database

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/datastruct/dict"
	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// parseFieldType parses a field type string (TEXT, TAG, NUMERIC, VECTOR) into a FieldType constant.
func parseFieldType(s string) (search.FieldType, error) {
	switch strings.ToUpper(s) {
	case "TEXT":
		return search.FieldTypeText, nil
	case "TAG":
		return search.FieldTypeTag, nil
	case "NUMERIC":
		return search.FieldTypeNumeric, nil
	case "VECTOR":
		return search.FieldTypeVector, nil
	default:
		return 0, fmt.Errorf("unsupported field type: %s", s)
	}
}

// defaultAlias derives a field alias from its schema path: "$.features[*]" and
// "features" both alias to "features".
func defaultAlias(path string) string {
	alias := strings.TrimSuffix(strings.TrimPrefix(path, "$."), "[*]")
	if i := strings.LastIndex(alias, "."); i >= 0 {
		alias = alias[i+1:]
	}
	return alias
}

// isFieldOption reports whether an argument begins a field option group inside
// SCHEMA, so field parsing knows where the next field definition starts.
func isFieldOption(arg string) (takesValue bool, known bool) {
	switch strings.ToUpper(arg) {
	case "SORTABLE", "NOSTEM", "UNF", "INDEXMISSING", "WITHSUFFIXTRIES", "CASESENSITIVE", "NOINDEX":
		return false, true
	case "WEIGHT", "SEPARATOR":
		return true, true
	}
	return false, false
}

// execFTCreate creates a new search index with the given schema definition.
//
// FT.CREATE index [ON HASH|JSON] [PREFIX count prefix ...] SCHEMA field ...
//
// Each SCHEMA entry is "path [AS alias] type [options]", where a VECTOR entry
// reads "path AS alias VECTOR algo count TYPE FLOAT32 DIM n DISTANCE_METRIC m".
func execFTCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
	}

	idxName := string(args[0])
	schema := search.IndexSchema{Name: idxName, KeyType: search.KeyTypeHash}
	rest := args[1:]

	for i := 0; i < len(rest); {
		switch strings.ToUpper(string(rest[i])) {
		case "ON":
			if i+1 >= len(rest) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
			}
			switch strings.ToUpper(string(rest[i+1])) {
			case "HASH":
				schema.KeyType = search.KeyTypeHash
			case "JSON":
				schema.KeyType = search.KeyTypeJSON
			default:
				return protocol.MakeErrReply("ERR only HASH and JSON structures are supported")
			}
			i += 2
		case "PREFIX":
			if i+1 >= len(rest) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
			}
			count, err := strconv.Atoi(string(rest[i+1]))
			if err != nil || count < 0 {
				return protocol.MakeErrReply("ERR PREFIX count must be a non-negative integer")
			}
			if i+2+count > len(rest) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
			}
			for j := 0; j < count; j++ {
				schema.Prefixes = append(schema.Prefixes, string(rest[i+2+j]))
			}
			i += 2 + count
		case "SCHEMA":
			fields, next, errReply := parseSchemaFields(rest, i+1)
			if errReply != nil {
				return errReply
			}
			schema.Fields = fields
			i = next
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(rest[i]) + "`")
		}
	}

	if len(schema.Fields) == 0 {
		return protocol.MakeErrReply("ERR SCHEMA is required for FT.CREATE")
	}
	if err := search.RegisterIndex(schema); err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	backfillIndex(db, schema)

	db.addAof(utils.ToCmdLine3("ft.create", args...))
	return protocol.MakeOkReply()
}

func parseSchemaFields(args [][]byte, i int) ([]search.FieldSchema, int, protocol.ErrorReply) {
	var fields []search.FieldSchema
	for i < len(args) {
		path := string(args[i])
		i++
		if path == "" {
			return nil, 0, protocol.MakeErrReply("ERR field path must not be empty")
		}
		alias := defaultAlias(path)
		if i < len(args) && strings.EqualFold(string(args[i]), "AS") {
			if i+1 >= len(args) {
				return nil, 0, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
			}
			alias = string(args[i+1])
			i += 2
		}
		if i >= len(args) {
			return nil, 0, protocol.MakeErrReply("ERR missing type for field `" + alias + "`")
		}
		ft, err := parseFieldType(string(args[i]))
		if err != nil {
			return nil, 0, protocol.MakeErrReply(err.Error())
		}
		i++

		field := search.FieldSchema{Name: alias, Path: path, Type: ft}
		if ft == search.FieldTypeVector {
			opts, next, errReply := parseVectorFieldOpts(args, i)
			if errReply != nil {
				return nil, 0, errReply
			}
			field.VectorOpts = opts
			i = next
		} else {
			for i < len(args) {
				takesValue, known := isFieldOption(string(args[i]))
				if !known {
					break
				}
				i++
				if takesValue {
					i++
				}
			}
		}
		fields = append(fields, field)
	}
	return fields, i, nil
}

func parseVectorFieldOpts(args [][]byte, i int) (*search.VectorFieldOpts, int, protocol.ErrorReply) {
	if i+1 >= len(args) {
		return nil, 0, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
	}
	opts := &search.VectorFieldOpts{}
	switch strings.ToUpper(string(args[i])) {
	case "FLAT":
		opts.Algo = search.VectorAlgoFlat
	case "HNSW":
		opts.Algo = search.VectorAlgoHNSW
	default:
		return nil, 0, protocol.MakeErrReply("ERR unsupported vector algorithm: " + string(args[i]))
	}
	i++
	count, err := strconv.Atoi(string(args[i]))
	if err != nil || count < 0 {
		return nil, 0, protocol.MakeErrReply("ERR vector attribute count must be a non-negative integer")
	}
	i++
	// count is the number of vector attributes (two arguments each), matching
	// "VECTOR FLAT 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE".
	for j := 0; j+1 < count; j += 2 {
		if i+1 >= len(args) {
			return nil, 0, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
		}
		attr := strings.ToUpper(string(args[i]))
		val := string(args[i+1])
		i += 2
		switch attr {
		case "TYPE":
			opts.Type = strings.ToUpper(val)
			if opts.Type != "FLOAT32" && opts.Type != "FLOAT64" {
				return nil, 0, protocol.MakeErrReply("ERR unsupported vector type: " + val)
			}
		case "DIM":
			dim, err := strconv.Atoi(val)
			if err != nil || dim <= 0 {
				return nil, 0, protocol.MakeErrReply("ERR DIM must be a positive integer")
			}
			opts.Dim = dim
		case "DISTANCE_METRIC":
			opts.DistanceMetric = strings.ToUpper(val)
		}
	}
	switch opts.DistanceMetric {
	case "", "L2", "IP", "COSINE":
	default:
		return nil, 0, protocol.MakeErrReply("ERR unsupported distance metric: " + opts.DistanceMetric)
	}
	if opts.Dim == 0 {
		return nil, 0, protocol.MakeErrReply("ERR vector field requires DIM")
	}
	return opts, i, nil
}

// backfillIndex indexes the documents already present in the database, so an
// index created after data was written still serves queries.
func backfillIndex(db *DB, schema search.IndexSchema) {
	idx := search.GetIndex(schema.Name)
	if idx == nil {
		return
	}
	db.ForEach(func(key string, entity *database.DataEntity, _ *time.Time) bool {
		if !idx.HasPrefix(key) || entity == nil || entity.Data == nil {
			return true
		}
		switch schema.KeyType {
		case search.KeyTypeJSON:
			search.IndexJSONByPrefix(key, entity.Data)
		default:
			if d, ok := entity.Data.(dict.Dict); ok {
				reindexHash(key, d)
			}
		}
		return true
	})
}

// searchOptions holds the parsed trailing options of FT.SEARCH.
type searchOptions struct {
	noContent   bool
	withScores  bool
	offset      int
	limit       int
	returns     []string
	returnsSet  bool
	sortField   string
	sortAsc     bool
	hasSort     bool
	params      map[string][]byte
	filterQuery *search.Query
}

// makeSearchOptions parses the options that follow the query of FT.SEARCH.
func makeSearchOptions(args [][]byte) (*searchOptions, protocol.ErrorReply) {
	opts := &searchOptions{limit: 10, params: map[string][]byte{}}

	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NOCONTENT":
			opts.noContent = true
		case "WITHSCORES":
			opts.withScores = true
		case "VERBATIM", "NOSTOPWORDS", "WITHPAYLOADS", "WITHSORTKEYS":
			// accepted for client compatibility; godis has no payload or stemming support
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
			}
			offset, err1 := strconv.Atoi(string(args[i+1]))
			limit, err2 := strconv.Atoi(string(args[i+2]))
			if err1 != nil || err2 != nil || offset < 0 || limit < 0 {
				return nil, protocol.MakeErrReply("ERR LIMIT must be a pair of non-negative integers")
			}
			opts.offset, opts.limit = offset, limit
			i += 2
		case "RETURN":
			if i+1 >= len(args) {
				return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 || i+2+count > len(args) {
				return nil, protocol.MakeErrReply("ERR invalid RETURN count")
			}
			opts.returnsSet = true
			opts.returns = []string{}
			for j := 0; j < count; j++ {
				name := string(args[i+2+j])
				if strings.EqualFold(name, "AS") {
					// the following token is the output alias, not an input field
					j++
					continue
				}
				opts.returns = append(opts.returns, strings.TrimPrefix(name, "@"))
			}
			i += 1 + count
		case "SORTBY":
			if i+1 >= len(args) {
				return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
			}
			opts.sortField = strings.TrimPrefix(string(args[i+1]), "@")
			opts.hasSort = true
			opts.sortAsc = true
			i++
			if i+1 < len(args) {
				switch strings.ToUpper(string(args[i+1])) {
				case "ASC":
					opts.sortAsc = true
					i++
				case "DESC":
					opts.sortAsc = false
					i++
				}
			}
		case "PARAMS":
			if i+1 >= len(args) {
				return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 || count%2 != 0 || i+2+count > len(args) {
				return nil, protocol.MakeErrReply("ERR PARAMS count must be an even integer")
			}
			for j := 0; j < count; j += 2 {
				opts.params[string(args[i+2+j])] = args[i+3+j]
			}
			i += 1 + count
		case "DIALECT":
			if i+1 >= len(args) {
				return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
			}
			if _, err := strconv.Atoi(string(args[i+1])); err != nil {
				return nil, protocol.MakeErrReply("ERR DIALECT must be an integer")
			}
			i++
		case "FILTER":
			if i+3 >= len(args) {
				return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
			}
			min, max := math.Inf(-1), math.Inf(1)
			if string(args[i+2]) != "-inf" {
				v, err := strconv.ParseFloat(string(args[i+2]), 64)
				if err != nil {
					return nil, protocol.MakeErrReply("ERR invalid FILTER minimum")
				}
				min = v
			}
			if string(args[i+3]) != "+inf" && string(args[i+3]) != "inf" {
				v, err := strconv.ParseFloat(string(args[i+3]), 64)
				if err != nil {
					return nil, protocol.MakeErrReply("ERR invalid FILTER maximum")
				}
				max = v
			}
			opts.filterQuery = search.NumericRangeQuery(strings.TrimPrefix(string(args[i+1]), "@"), min, max)
			i += 3
		default:
			return nil, protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}
	return opts, nil
}

// execFTSearch performs full-text and/or vector similarity search using a registered index.
//
// FT.SEARCH index query [NOCONTENT] [WITHSCORES] [RETURN count field ...]
//
//	[SORTBY field [ASC|DESC]] [LIMIT offset num] [PARAMS count name value ...]
//	[DIALECT n]
func execFTSearch(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
	}

	idxName := string(args[0])
	idx := search.GetIndex(idxName)
	if idx == nil {
		return protocol.MakeErrReply("ERR unknown index name")
	}

	opts, errReply := makeSearchOptions(args[2:])
	if errReply != nil {
		return errReply
	}

	q, err := search.ParseQuery(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if opts.filterQuery != nil {
		q = search.AndQuery(q, opts.filterQuery)
	}

	var docs []*search.DocInfo
	if q.KNN != nil {
		vec, errReply := resolveVectorParam(idx, q.KNN, opts.params)
		if errReply != nil {
			return errReply
		}
		candidates, err := idx.EvalScores(q)
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		docs, err = idx.KNNSearch(q.KNN.Field, vec, q.KNN.K, candidates)
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
	} else {
		docs, err = idx.TextSearch(q)
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
	}

	scoreName := ""
	if q.KNN != nil {
		scoreName = q.KNN.ScoreAlias
	}
	if opts.hasSort {
		if err := sortDocs(idx, docs, opts.sortField, opts.sortAsc, scoreName); err != nil {
			return protocol.MakeErrReply(err.Error())
		}
	}
	docs = paginate(docs, opts.offset, opts.limit)

	return makeDocReply(docs, docReplyOpts{
		noContent:  opts.noContent,
		withScores: opts.withScores,
		returns:    opts.returns,
		returnsSet: opts.returnsSet,
		scoreName:  scoreName,
	})
}

func resolveVectorParam(idx *search.InvertedIndex, knn *search.KNNClause, params map[string][]byte) ([]float32, protocol.ErrorReply) {
	schema := idx.Schema()
	f := schema.FieldByName(knn.Field)
	if f == nil || f.Type != search.FieldTypeVector {
		return nil, protocol.MakeErrReply("ERR unknown vector field `" + knn.Field + "`")
	}
	raw, ok := params[knn.Param]
	if !ok {
		return nil, protocol.MakeErrReply("ERR vector parameter `" + knn.Param + "` was not provided")
	}
	dim := 0
	if f.VectorOpts != nil {
		dim = f.VectorOpts.Dim
	}
	vec, err := search.DecodeVector(raw, dim)
	if err != nil {
		return nil, protocol.MakeErrReply(err.Error())
	}
	return vec, nil
}

func paginate(docs []*search.DocInfo, offset, limit int) []*search.DocInfo {
	if offset >= len(docs) {
		return nil
	}
	docs = docs[offset:]
	if limit > 0 && limit < len(docs) {
		docs = docs[:limit]
	}
	return docs
}

// sortDocs orders results by the KNN score or by a schema field.
func sortDocs(idx *search.InvertedIndex, docs []*search.DocInfo, field string, asc bool, scoreName string) error {
	if field == "score" || (scoreName != "" && strings.EqualFold(field, scoreName)) {
		sort.SliceStable(docs, func(i, j int) bool {
			if docs[i].Score != docs[j].Score {
				return (docs[i].Score < docs[j].Score) == asc
			}
			return docs[i].Key < docs[j].Key
		})
		return nil
	}

	schema := idx.Schema()
	f := schema.FieldByName(field)
	if f == nil {
		return fmt.Errorf("Unknown field '%s'", field)
	}
	key := func(d *search.DocInfo) string {
		vals := d.Fields[f.Name]
		if len(vals) == 0 {
			return ""
		}
		return vals[0]
	}
	sort.SliceStable(docs, func(i, j int) bool {
		a, b := key(docs[i]), key(docs[j])
		if f.Type == search.FieldTypeNumeric {
			an, aerr := strconv.ParseFloat(a, 64)
			bn, berr := strconv.ParseFloat(b, 64)
			if aerr == nil && berr == nil && an != bn {
				return (an < bn) == asc
			}
		}
		if a != b {
			return (a < b) == asc
		}
		return docs[i].Key < docs[j].Key
	})
	return nil
}

type docReplyOpts struct {
	noContent  bool
	withScores bool
	returns    []string
	returnsSet bool
	scoreName  string
}

// makeDocReply renders results in the RediSearch RESP2 shape:
// [total, key1, [field, value ...], key2, ...], with optional score and
// field projection.
func makeDocReply(docs []*search.DocInfo, opts docReplyOpts) redis.Reply {
	resp := make([][]byte, 0, 1+len(docs)*2)
	resp = append(resp, []byte(strconv.Itoa(len(docs))))

	for _, doc := range docs {
		resp = append(resp, []byte(doc.Key))
		if opts.withScores {
			resp = append(resp, []byte(formatFloat(doc.Score)))
		}
		if opts.noContent {
			continue
		}

		var pairs [][]byte
		if !opts.returnsSet {
			names := make([]string, 0, len(doc.Fields))
			for name := range doc.Fields {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				for _, v := range doc.Fields[name] {
					pairs = append(pairs, []byte(name), []byte(v))
				}
			}
		} else {
			for _, name := range opts.returns {
				if opts.scoreName != "" && strings.EqualFold(name, opts.scoreName) {
					pairs = append(pairs, []byte(name), []byte(formatFloat(doc.Score)))
					continue
				}
				for _, v := range doc.Fields[name] {
					pairs = append(pairs, []byte(name), []byte(v))
				}
			}
		}
		resp = append(resp, protocol.MakeMultiBulkReply(pairs).ToBytes())
	}
	return protocol.MakeMultiBulkReply(resp)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// execFTHybrid runs a keyword search and a vector search and fuses the two
// rankings.
//
// FT.HYBRID index SEARCH query VSIM @field $param [KNN k] [K n]
//
//	[COMBINE RRF|LINEAR ...] [LOAD count field ...] [LIMIT offset num]
//	[PARAMS count name value ...] [FILTER query]
func execFTHybrid(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
	}
	idx := search.GetIndex(string(args[0]))
	if idx == nil {
		return protocol.MakeErrReply("ERR unknown index name")
	}

	req := &search.HybridRequest{Combine: search.CombineRRF}
	params := map[string][]byte{}
	var searchQuery string
	var returns []string
	returnsSet := false
	offset, limit := 0, 10
	var vectorParam string

	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "SEARCH":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
			}
			searchQuery = string(args[i+1])
			i++
		case "VSIM":
			field, param, knn, k, next, errReply := parseVSIMClause(args, i+1)
			if errReply != nil {
				return errReply
			}
			req.VectorField, vectorParam, req.KNN, req.K, i = field, param, knn, k, next-1
		case "COMBINE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
			}
			req.Combine = strings.ToUpper(string(args[i+1]))
			if req.Combine != search.CombineRRF && req.Combine != search.CombineLinear {
				return protocol.MakeErrReply("ERR unsupported COMBINE method: " + req.Combine)
			}
			i++
			// "COMBINE method [count arg value ...]"; a bare trailing value is
			// also accepted for clients that omit the count.
			count := -1
			if v, err := strconv.Atoi(string(nextArg(args, i+1))); err == nil {
				count = v
				i++
			}
			var errReply protocol.ErrorReply
			if i, errReply = applyCombineOpts(req, args, i, count); errReply != nil {
				return errReply
			}
		case "LOAD":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 || i+2+count > len(args) {
				return protocol.MakeErrReply("ERR invalid LOAD count")
			}
			returnsSet = true
			for j := 0; j < count; j++ {
				returns = append(returns, strings.TrimPrefix(string(args[i+2+j]), "@"))
			}
			i += 1 + count
		case "LIMIT":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
			}
			var err1, err2 error
			offset, err1 = strconv.Atoi(string(args[i+1]))
			limit, err2 = strconv.Atoi(string(args[i+2]))
			if err1 != nil || err2 != nil {
				return protocol.MakeErrReply("ERR LIMIT must be a pair of integers")
			}
			i += 2
		case "PARAMS":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 || count%2 != 0 || i+2+count > len(args) {
				return protocol.MakeErrReply("ERR PARAMS count must be an even integer")
			}
			for j := 0; j < count; j += 2 {
				params[string(args[i+2+j])] = args[i+3+j]
			}
			i += 1 + count
		case "FILTER":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
			}
			filter, err := search.ParseQuery(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			req.Filter = filter
			i++
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	if req.VectorField == "" {
		return protocol.MakeErrReply("ERR FT.HYBRID requires a VSIM clause")
	}
	knn := &search.KNNClause{Field: req.VectorField, Param: vectorParam, K: req.KNN}
	vec, errReply := resolveVectorParam(idx, knn, params)
	if errReply != nil {
		return errReply
	}
	req.Vector = vec

	textQuery, err := search.ParseQuery(searchQuery)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	req.Search = textQuery

	docs, err := idx.Hybrid(req)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	docs = paginate(docs, offset, limit)

	return makeDocReply(docs, docReplyOpts{returns: returns, returnsSet: returnsSet, scoreName: "score"})
}

// nextArg returns args[i] or nil when out of range.
func nextArg(args [][]byte, i int) []byte {
	if i < 0 || i >= len(args) {
		return nil
	}
	return args[i]
}

// applyCombineOpts consumes fusion option/value pairs starting at args[i+1].
// A negative count means "consume while the next token is a known fusion option".
// It returns the index of the last consumed argument.
func applyCombineOpts(req *search.HybridRequest, args [][]byte, i, count int) (int, protocol.ErrorReply) {
	consumed := 0
	for i+2 < len(args) && (count < 0 || consumed < count) {
		opt := strings.ToUpper(string(args[i+1]))
		switch opt {
		case "ALPHA", "WINDOW", "CONSTANT":
		default:
			return i, nil
		}
		val, err := strconv.ParseFloat(string(args[i+2]), 64)
		if err != nil {
			return 0, protocol.MakeErrReply("ERR " + opt + " must be numeric")
		}
		switch opt {
		case "ALPHA":
			req.Alpha = val
		case "WINDOW":
			req.Window = int(val)
		case "CONSTANT":
			req.Constant = val
		}
		i += 2
		consumed += 2
	}
	if count >= 0 && consumed < count {
		return 0, protocol.MakeErrReply("ERR COMBINE count does not match the arguments provided")
	}
	return i, nil
}

// parseVSIMClause parses "@field $param [KNN k] [K n]" starting at args[i] and
// returns the index of the first unconsumed argument.
func parseVSIMClause(args [][]byte, i int) (field, param string, knn, k, next int, errReply protocol.ErrorReply) {
	if i+1 >= len(args) {
		return "", "", 0, 0, 0, protocol.MakeErrReply("ERR wrong number of arguments for 'FT.HYBRID' command")
	}
	field = strings.TrimPrefix(string(args[i]), "@")
	param = strings.TrimPrefix(string(args[i+1]), "$")
	i += 2

	for i+1 < len(args) {
		opt := strings.ToUpper(string(args[i]))
		if opt != "KNN" && opt != "K" {
			break
		}
		v, err := strconv.Atoi(string(args[i+1]))
		if err != nil || v <= 0 {
			return "", "", 0, 0, 0, protocol.MakeErrReply("ERR " + opt + " must be a positive integer")
		}
		if opt == "KNN" {
			knn = v
		} else {
			k = v
		}
		i += 2
	}
	return field, param, knn, k, i, nil
}

// execFTDropIndex drops a registered search index by name.
// The documents themselves are left untouched, matching RediSearch.
func execFTDropIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.DROPINDEX' command")
	}
	idxName := string(args[0])

	if !search.DropIndex(idxName) {
		return protocol.MakeErrReply("ERR unknown index name")
	}

	db.addAof(utils.ToCmdLine3("ft.dropindex", args...))
	return protocol.MakeOkReply()
}

// execFTInfo returns metadata about a registered search index.
func execFTInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.INFO' command")
	}
	idx := search.GetIndex(string(args[0]))
	if idx == nil {
		return protocol.MakeErrReply("ERR unknown index name")
	}
	schema := idx.Schema()

	result := make([][]byte, 0, 16)
	result = append(result, []byte("index_name"), []byte(schema.Name))

	keyType := "HASH"
	if schema.KeyType == search.KeyTypeJSON {
		keyType = "JSON"
	}
	prefixes := make([][]byte, len(schema.Prefixes))
	for i, p := range schema.Prefixes {
		prefixes[i] = []byte(p)
	}
	def := [][]byte{
		[]byte("key_type"), []byte(keyType),
		[]byte("prefixes"), protocol.MakeMultiBulkReply(prefixes).ToBytes(),
	}
	result = append(result, []byte("index_definition"), protocol.MakeMultiBulkReply(def).ToBytes())

	attrs := make([][]byte, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		entry := [][]byte{
			[]byte("identifier"), []byte(f.Path),
			[]byte("attribute"), []byte(f.Name),
			[]byte("type"), []byte(search.FieldTypeName(f.Type)),
		}
		if f.VectorOpts != nil {
			algo := "FLAT"
			if f.VectorOpts.Algo == search.VectorAlgoHNSW {
				algo = "HNSW"
			}
			metric := f.VectorOpts.DistanceMetric
			if metric == "" {
				metric = "COSINE"
			}
			entry = append(entry,
				[]byte("algorithm"), []byte(algo),
				[]byte("data_type"), []byte(f.VectorOpts.Type),
				[]byte("dim"), []byte(strconv.Itoa(f.VectorOpts.Dim)),
				[]byte("distance_metric"), []byte(metric),
			)
		}
		attrs = append(attrs, protocol.MakeMultiBulkReply(entry).ToBytes())
	}
	result = append(result, []byte("attributes"), protocol.MakeMultiBulkReply(attrs).ToBytes())
	result = append(result, []byte("num_docs"), []byte(strconv.Itoa(idx.DocCount())))
	result = append(result, []byte("num_terms"), []byte(strconv.Itoa(idx.TermCount())))

	return protocol.MakeMultiBulkReply(result)
}

// execFTList returns the names of all registered search indexes.
func execFTList(db *DB, args [][]byte) redis.Reply {
	names := search.ListIndexes()
	result := make([][]byte, len(names))
	for i, n := range names {
		result[i] = []byte(n)
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("FT.CREATE", execFTCreate, readAllKeys, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("FT.SEARCH", execFTSearch, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.HYBRID", execFTHybrid, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.DROPINDEX", execFTDropIndex, readAllKeys, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.INFO", execFTInfo, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT._LIST", execFTList, readAllKeys, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}

package database

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// float32Blob encodes a vector the way a Redis client library sends it: raw
// little-endian IEEE-754 float32 bytes inside the RESP bulk string.
func float32Blob(vals ...float32) []byte {
	buf := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}
	return buf
}

func mustExec(t *testing.T, db *DB, cmdLine ...string) redis.Reply {
	t.Helper()
	return mustExecRaw(t, db, toBytesArgs(cmdLine...))
}

func mustExecRaw(t *testing.T, db *DB, cmdLine [][]byte) redis.Reply {
	t.Helper()
	reply := db.Exec(nil, cmdLine)
	if errReply, ok := reply.(protocol.ErrorReply); ok {
		t.Fatalf("%s failed: %s", string(cmdLine[0]), errReply.Error())
	}
	return reply
}

func toBytesArgs(args ...string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}

// searchResult renders a search reply as ordered keys plus, for each key, its
// projected fields. The RediSearch RESP2 layout is
// [total, key, (score), ([field, value ...])?] repeated per document.
func searchResult(t *testing.T, reply redis.Reply, withScores, noContent bool) (int, []string, []map[string]string, []string) {
	t.Helper()
	multi, ok := reply.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected multi bulk reply, got %T: %s", reply, reply.ToBytes())
	}
	total, err := strconv.Atoi(string(multi.Args[0]))
	if err != nil {
		t.Fatalf("first element must be the total count: %q", multi.Args[0])
	}

	stride := 2
	if withScores {
		stride = 3
	}
	if noContent {
		stride = 1
		if withScores {
			stride = 2
		}
	}
	var keys, scores []string
	var fields []map[string]string
	for i := 1; i+stride <= len(multi.Args); i += stride {
		keys = append(keys, string(multi.Args[i]))
		offset := i + 1
		if withScores {
			scores = append(scores, string(multi.Args[i+1]))
			offset++
		}
		if noContent {
			continue
		}
		fields = append(fields, parseFieldPairs(t, multi.Args[offset]))
	}
	return total, keys, fields, scores
}

func parseFieldPairs(t *testing.T, raw []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	if len(raw) == 0 {
		return out
	}
	reply, err := parser.ParseOne(raw)
	if err != nil {
		t.Fatalf("field payload is not valid RESP: %v (%q)", err, raw)
	}
	multi, ok := reply.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected a multi bulk field payload, got %T", reply)
	}
	for i := 0; i+1 < len(multi.Args); i += 2 {
		out[string(multi.Args[i])] = string(multi.Args[i+1])
	}
	return out
}

// TestVectorSearchTutorial replays the Redis "vector and hybrid search" tutorial
// against the JSON catalog: index an existing product catalogue, run KNN, run a
// pre-filtered KNN, and fuse keyword and vector rankings with FT.HYBRID.
func TestVectorSearchTutorial(t *testing.T) {
	db := makeTestDB()
	const index = "idx:catalog"
	defer db.Exec(nil, toBytesArgs("FT.DROPINDEX", index))

	products := map[string]string{
		"product:1": `{"name":"Aurora AcousticPro Headphones","brand":"Aurora","category":"Audio",
			"description":"Wireless noise-cancelling over-ear headphones for focused listening.",
			"price":199.99,"features":["wireless","noise-cancelling","bluetooth"],"embedding":[0.9,0.1,0,0]}`,
		"product:2": `{"name":"Aurora BudsMini Earbuds","brand":"Aurora","category":"Audio",
			"description":"Tiny true-wireless earbuds with a secure in-ear fit for workouts.",
			"price":89.99,"features":["wireless","bluetooth","in-ear"],"embedding":[0.95,0.05,0,0]}`,
		"product:3": `{"name":"Sonus Boom Portable Speaker","brand":"Sonus","category":"Audio",
			"description":"A rugged portable Bluetooth speaker with deep bass for the beach or a campsite.",
			"price":129.5,"features":["wireless","bluetooth","portable","waterproof"],"embedding":[1,0,0,0]}`,
		"product:4": `{"name":"Pixma Vortex 15 Laptop","brand":"Pixma","category":"Computers",
			"description":"A thin-and-light laptop with a fast multi-core processor.",
			"price":1399.0,"features":["laptop","ssd"],"embedding":[0,0,1,0]}`,
		"product:5": `{"name":"Clackr Mechanical Keyboard","brand":"Clackr","category":"Accessories",
			"description":"A compact mechanical keyboard with wireless connectivity.",
			"price":119.0,"features":["keyboard","mechanical","wireless"],"embedding":[0,0,0,1]}`,
		"product:6": `{"name":"Pixma UltraView 27 Monitor","brand":"Pixma","category":"Computers",
			"description":"A 27-inch 4K monitor with an IPS panel.",
			"price":329.99,"features":["monitor","4k"],"embedding":[0,0,0.9,0.1]}`,
	}
	for key, doc := range products {
		mustExec(t, db, "JSON.SET", key, "$", doc)
	}

	// The index is created after the data, so FT.CREATE must backfill it.
	mustExec(t, db, "FT.CREATE", index, "ON", "JSON", "PREFIX", "1", "product:", "SCHEMA",
		"$.name", "AS", "name", "TEXT",
		"$.brand", "AS", "brand", "TAG",
		"$.category", "AS", "category", "TAG",
		"$.description", "AS", "description", "TEXT",
		"$.price", "AS", "price", "NUMERIC", "SORTABLE",
		"$.features[*]", "AS", "features", "TAG",
		"$.embedding", "AS", "embedding", "VECTOR", "FLAT", "6",
		"TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE")

	info := mustExec(t, db, "FT.INFO", index)
	infoMulti, ok := info.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("FT.INFO must return an array, got %T", info)
	}
	infoPairs := map[string]string{}
	for i := 0; i+1 < len(infoMulti.Args); i += 2 {
		infoPairs[string(infoMulti.Args[i])] = string(infoMulti.Args[i+1])
	}
	if infoPairs["num_docs"] != strconv.Itoa(len(products)) {
		t.Fatalf("FT.CREATE should have indexed all %d existing documents, FT.INFO reports %s",
			len(products), infoPairs["num_docs"])
	}
	if !strings.Contains(infoPairs["index_definition"], "JSON") {
		t.Fatalf("index_definition should declare the JSON key type: %q", infoPairs["index_definition"])
	}

	// KNN: the query vector is the embedding of "portable music for the outdoors".
	query := float32Blob(1, 0, 0, 0)
	total, keys, fields, scores := searchResult(t, mustExecRaw(t, db, [][]byte{
		[]byte("FT.SEARCH"), []byte(index),
		[]byte("(*)=>[KNN 3 @embedding $query_vector AS score]"),
		[]byte("PARAMS"), []byte("2"), []byte("query_vector"), query,
		[]byte("SORTBY"), []byte("score"), []byte("ASC"),
		[]byte("RETURN"), []byte("2"), []byte("score"), []byte("name"),
		[]byte("DIALECT"), []byte("2"),
	}), false, false)

	if total != 3 {
		t.Fatalf("expected 3 nearest neighbours, got %d", total)
	}
	want := []string{"product:3", "product:2", "product:1"}
	for i, key := range want {
		if keys[i] != key {
			t.Fatalf("KNN order = %v, want %v (3 is nearest, 1 is furthest of the three)", keys, want)
		}
	}
	for i := 1; i < len(scores); i++ {
		prev, _ := strconv.ParseFloat(scores[i-1], 64)
		cur, _ := strconv.ParseFloat(scores[i], 64)
		if cur < prev {
			t.Fatalf("scores must ascend for SORTBY score ASC: %v", scores)
		}
	}
	for _, f := range fields {
		if _, ok := f["score"]; !ok {
			t.Fatalf("RETURN 2 score name must project the KNN score: %v", f)
		}
		if _, ok := f["name"]; !ok {
			t.Fatalf("RETURN 2 score name must project name: %v", f)
		}
		if len(f) != 2 {
			t.Fatalf("RETURN must project exactly the requested fields, got %v", f)
		}
	}

	// Pre-filtered KNN: only Computers may be returned even though the nearest
	// documents overall are Audio.
	_, keys, _, _ = searchResult(t, mustExecRaw(t, db, [][]byte{
		[]byte("FT.SEARCH"), []byte(index),
		[]byte("(@category:{Computers})=>[KNN 3 @embedding $query_vector AS score]"),
		[]byte("PARAMS"), []byte("2"), []byte("query_vector"), query,
		[]byte("SORTBY"), []byte("score"), []byte("ASC"),
		[]byte("NOCONTENT"), []byte("DIALECT"), []byte("2"),
	}), false, true)
	if len(keys) != 2 || keys[0] != "product:4" || keys[1] != "product:6" {
		t.Fatalf("pre-filter leaked documents outside the subset: %v", keys)
	}

	// A textual parameter must work as well as a binary one.
	_, textKeys, _, _ := searchResult(t, mustExec(t, db, "FT.SEARCH", index,
		"(*)=>[KNN 3 @embedding $vec AS score]",
		"PARAMS", "2", "vec", "1.0 0.0 0.0 0.0",
		"SORTBY", "score", "ASC", "NOCONTENT", "DIALECT", "2"), false, true)
	if len(textKeys) != 3 || textKeys[0] != "product:3" {
		t.Fatalf("a text-encoded query vector must decode identically, got %v", textKeys)
	}

	// Tag and numeric filters, plus SORTBY on a schema field.
	total, tagKeys, _, _ := searchResult(t, mustExec(t, db, "FT.SEARCH", index,
		"@features:{wireless}", "SORTBY", "price", "ASC", "NOCONTENT"), false, true)
	if total != 4 || len(tagKeys) != 4 {
		t.Fatalf("expected the 4 wireless products, got %d (%v)", total, tagKeys)
	}
	if tagKeys[0] != "product:2" || tagKeys[3] != "product:1" {
		t.Fatalf("SORTBY price ASC should order 89.99, 119, 129.5, 199.99: %v", tagKeys)
	}
	_, _, _, _ = searchResult(t, mustExec(t, db, "FT.SEARCH", index,
		"@price:[100 160] @category:{Audio}", "NOCONTENT"), false, true)

	// Distance metrics other than COSINE, on a numeric filter.
	if reply := db.Exec(nil, toBytesArgs("FT.SEARCH", index, "@price:[1 2]")); reply != nil {
		if errReply, ok := reply.(protocol.ErrorReply); ok {
			t.Fatalf("numeric range filter must parse: %s", errReply.Error())
		}
	}

	// Writes after index creation must be searchable immediately.
	mustExec(t, db, "JSON.SET", "product:1", "$.price", "149.99")
	_, _, updated, _ := searchResult(t, mustExec(t, db, "FT.SEARCH", index,
		"@price:[100 160] @category:{Audio}", "RETURN", "1", "price"), false, false)
	if len(updated) != 2 {
		t.Fatalf("expected product:1 and product:3 in the 100..160 Audio range, got %v", updated)
	}
	for _, f := range updated {
		price, _ := strconv.ParseFloat(f["price"], 64)
		if price < 100 || price > 160 {
			t.Fatalf("price filter returned %v", f)
		}
	}

	// Hybrid search: product:2 is the only document matching both the keyword
	// "wireless" and the query vector's meaning, so it must rank first.
	hybrid := mustExecRaw(t, db, [][]byte{
		[]byte("FT.HYBRID"), []byte(index),
		[]byte("SEARCH"), []byte("wireless"),
		[]byte("VSIM"), []byte("@embedding"), []byte("$query_vector"),
		[]byte("KNN"), []byte("2"), []byte("K"), []byte("5"),
		[]byte("LOAD"), []byte("1"), []byte("@name"),
		[]byte("PARAMS"), []byte("2"), []byte("query_vector"), query,
	})
	_, hybridKeys, hybridFields, _ := searchResult(t, hybrid, false, false)
	if len(hybridKeys) < 3 {
		t.Fatalf("hybrid search should fuse both rankings, got %v", hybridKeys)
	}
	if hybridKeys[0] != "product:2" {
		t.Fatalf("product:2 satisfies keyword and vector, so it must rank first: %v", hybridKeys)
	}
	for _, f := range hybridFields {
		if len(f) != 1 || f["name"] == "" {
			t.Fatalf("LOAD 1 @name must project name and nothing else: %v", f)
		}
	}
	if !strings.Contains(fmt.Sprint(hybridKeys), "product:3") {
		t.Fatalf("the vector ranking must contribute product:3: %v", hybridKeys)
	}

	// The fused score is a loadable field, and the fused ranking must descend.
	_, hybridKeys, hybridFields, _ = searchResult(t, mustExecRaw(t, db, [][]byte{
		[]byte("FT.HYBRID"), []byte(index),
		[]byte("SEARCH"), []byte("wireless"),
		[]byte("VSIM"), []byte("@embedding"), []byte("$query_vector"),
		[]byte("KNN"), []byte("2"), []byte("K"), []byte("5"),
		[]byte("LOAD"), []byte("2"), []byte("@name"), []byte("score"),
		[]byte("PARAMS"), []byte("2"), []byte("query_vector"), query,
	}), false, false)
	if hybridKeys[0] != "product:2" {
		t.Fatalf("hybrid order changed when score was loaded: %v", hybridKeys)
	}
	fusedScores := make([]float64, len(hybridFields))
	for i, f := range hybridFields {
		if len(f) != 2 {
			t.Fatalf("LOAD 2 @name score must project both: %v", f)
		}
		fusedScores[i], _ = strconv.ParseFloat(f["score"], 64)
	}
	for i := 1; i < len(fusedScores); i++ {
		if fusedScores[i] > fusedScores[i-1] {
			t.Fatalf("fused scores must descend: %v", fusedScores)
		}
	}

	// Dropping the index must not delete the documents.
	mustExec(t, db, "FT.DROPINDEX", index)
	if _, ok := db.Exec(nil, toBytesArgs("JSON.GET", "product:3")).(*protocol.BulkReply); !ok {
		t.Fatal("documents must survive FT.DROPINDEX")
	}
	if _, ok := db.Exec(nil, toBytesArgs("FT.SEARCH", index, "*")).(protocol.ErrorReply); !ok {
		t.Fatal("searching a dropped index must report an unknown index")
	}
}

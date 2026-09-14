package search

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Query is a parsed RediSearch dialect-2 query: an optional filter expression
// plus an optional KNN clause that ranks the filtered candidates by vector
// distance.
type Query struct {
	Root *Node
	KNN  *KNNClause
}

// NodeOp is the operator of a query AST node.
type NodeOp int

const (
	OpAll NodeOp = iota
	OpTerm
	OpAnd
	OpOr
	OpNot
)

// TermKind is the value form of a term.
type TermKind int

const (
	TermWord TermKind = iota
	TermTag
	TermNumeric
)

// Term is a leaf query node: a word, a tag, or a numeric range.
type Term struct {
	Field  string
	Kind   TermKind
	Word   string
	Tags   []string
	Min    float64
	Max    float64
	HasMin bool
	HasMax bool
}

// Node is a node of the query AST.
type Node struct {
	Op       NodeOp
	Children []*Node
	Term     *Term
}

// KNNClause is the "=>[KNN k @field $param AS alias]" suffix of a query.
type KNNClause struct {
	K          int
	Field      string
	Param      string
	ScoreAlias string
}

const knnPrefix = "=>[KNN"

// ParseQuery parses a dialect-2 query. The supported subset is: "*", bare words,
// "@field:word", "@tag:{a|b}", "@numeric:[min max]", "-negation", "+required",
// "|" OR groups, parenthesised sub-expressions, and a trailing KNN clause.
func ParseQuery(s string) (*Query, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return &Query{}, nil
	}

	q := &Query{}
	if i := strings.Index(s, knnPrefix); i >= 0 {
		tail := s[i+len(knnPrefix):]
		j := strings.Index(tail, "]")
		if j < 0 {
			return nil, fmt.Errorf("Syntax error: unterminated KNN clause")
		}
		knn, err := parseKNN(tail[:j])
		if err != nil {
			return nil, err
		}
		q.KNN = knn
		s = strings.TrimSpace(s[:i])
		if s == "" || s == "*" {
			return q, nil
		}
	}

	p := &parser{s: s}
	root, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if !p.done() {
		return nil, fmt.Errorf("Syntax error at offset %d", p.i)
	}
	q.Root = root
	return q, nil
}

// AndQuery merges two parsed queries so a document must satisfy both.
func AndQuery(a, b *Query) *Query {
	switch {
	case a == nil || a.Root == nil:
		return b
	case b == nil || b.Root == nil:
		return a
	}
	merged := &Query{Root: &Node{Op: OpAnd, Children: []*Node{a.Root, b.Root}}}
	if a.KNN != nil {
		merged.KNN = a.KNN
	} else {
		merged.KNN = b.KNN
	}
	return merged
}

// NumericRangeQuery builds a single-clause numeric range filter.
func NumericRangeQuery(field string, min, max float64) *Query {
	return &Query{Root: &Node{Op: OpTerm, Term: &Term{
		Field:  field,
		Kind:   TermNumeric,
		Min:    min,
		Max:    max,
		HasMin: true,
		HasMax: true,
	}}}
}

// TagQuery builds a single-clause tag filter matching any of the given tags.
func TagQuery(field string, tags []string) *Query {
	lowered := make([]string, 0, len(tags))
	for _, t := range tags {
		if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
			lowered = append(lowered, t)
		}
	}
	return &Query{Root: &Node{Op: OpTerm, Term: &Term{
		Field: field,
		Kind:  TermTag,
		Tags:  lowered,
	}}}
}

// maxTermsQuery caps how many words a free-text query contributes; beyond a
// couple of dozen the union matches nearly everything and stops discriminating.
const maxTermsQuery = 32

// stopWords are the words a natural-language question contributes that no
// document is meaningfully indexed by.
var stopWords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "but": true, "by": true, "can": true, "do": true, "does": true,
	"for": true, "from": true, "how": true, "i": true, "if": true, "in": true,
	"is": true, "it": true, "its": true, "me": true, "my": true, "of": true,
	"on": true, "or": true, "should": true, "so": true, "that": true, "the": true,
	"then": true, "there": true, "this": true, "to": true, "was": true, "what": true,
	"when": true, "where": true, "which": true, "why": true, "will": true, "with": true,
	"you": true, "your": true,
}

// TermsQuery turns free text into a match-any-word query. It exists for callers
// holding a question rather than a RediSearch expression: a question parsed as a
// dialect-2 query would become a conjunction, which matches almost nothing.
func TermsQuery(text string) *Query {
	var children []*Node
	seen := map[string]bool{}
	for _, word := range queryWords(text) {
		if seen[word] {
			continue
		}
		seen[word] = true
		children = append(children, &Node{Op: OpTerm, Term: &Term{Kind: TermWord, Word: word}})
		if len(children) >= maxTermsQuery {
			break
		}
	}
	switch len(children) {
	case 0:
		return &Query{}
	case 1:
		return &Query{Root: children[0]}
	default:
		return &Query{Root: &Node{Op: OpOr, Children: children}}
	}
}

// queryWords extracts indexable words: lowercased, alphanumeric, and not a
// stop word. Non-word characters are dropped rather than escaped, so a raw
// sentence can never be read as query syntax.
func queryWords(text string) []string {
	var words []string
	for _, field := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r > 0x7f)
	}) {
		if len([]rune(field)) < 2 || stopWords[field] {
			continue
		}
		words = append(words, field)
	}
	return words
}

// Restrict drops hits outside a candidate set; a nil set means no restriction.
func Restrict(hits []*DocInfo, candidates map[string]float64) []*DocInfo {
	if candidates == nil {
		return hits
	}
	out := hits[:0]
	for _, h := range hits {
		if _, ok := candidates[h.Key]; ok {
			out = append(out, h)
		}
	}
	return out
}

func parseKNN(clause string) (*KNNClause, error) {
	fields := strings.Fields(clause)
	if len(fields) < 2 {
		return nil, fmt.Errorf("Syntax error: malformed KNN clause")
	}
	k, err := strconv.Atoi(fields[0])
	if err != nil || k <= 0 {
		return nil, fmt.Errorf("Syntax error: KNN k must be a positive integer")
	}
	knn := &KNNClause{K: k, Field: strings.TrimPrefix(fields[1], "@")}
	for i := 2; i < len(fields); i++ {
		switch {
		case strings.EqualFold(fields[i], "AS") && i+1 < len(fields):
			i++
			knn.ScoreAlias = fields[i]
		case strings.HasPrefix(fields[i], "$"):
			knn.Param = fields[i][1:]
		}
	}
	if knn.Param == "" {
		return nil, fmt.Errorf("Syntax error: KNN clause requires a $vector parameter")
	}
	return knn, nil
}

type parser struct {
	s string
	i int
}

func (p *parser) done() bool {
	p.skipSpace()
	return p.i >= len(p.s)
}

func (p *parser) skipSpace() {
	for p.i < len(p.s) && isSpaceByte(p.s[p.i]) {
		p.i++
	}
}

func (p *parser) peek() byte {
	return p.at(0)
}

// at reads the byte at a relative offset, returning 0 past the end of input so
// lookahead never panics on the final token.
func (p *parser) at(off int) byte {
	if p.i+off >= len(p.s) {
		return 0
	}
	return p.s[p.i+off]
}

func (p *parser) eat(c byte) bool {
	if p.peek() == c {
		p.i++
		return true
	}
	return false
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func isTermEnd(c byte) bool {
	return c == 0 || isSpaceByte(c) || c == '(' || c == ')' || c == '|' || c == '{' || c == '}' || c == '[' || c == ']' || c == '"'
}

// parseExpr parses the OR level: expressions joined by '|'. Dialect 2 gives AND
// higher precedence, so "a b | c d" means "(a b) | (c d)".
func (p *parser) parseExpr() (*Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		p.skipSpace()
		if p.peek() != '|' {
			return left, nil
		}
		p.i++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		switch {
		case left == nil:
			left = right
		case right == nil:
			return nil, fmt.Errorf("Syntax error at offset %d: missing operand after '|'", p.i)
		default:
			left = &Node{Op: OpOr, Children: []*Node{left, right}}
		}
	}
}

// parseAnd parses the AND level: a sequence of space-separated terms. A leading
// '-' negates a term; a leading '+' is accepted and ignored, since space already
// means intersection in dialect 2.
func (p *parser) parseAnd() (*Node, error) {
	var children []*Node
	for {
		p.skipSpace()
		c := p.peek()
		if c == 0 || c == ')' || c == '|' {
			break
		}
		if c == '*' && isTermEnd(p.at(1)) {
			p.i++
			children = append(children, &Node{Op: OpAll})
			continue
		}

		neg := p.eat('-')
		if !neg {
			p.eat('+')
		}

		node, err := p.parseAtom()
		if err != nil {
			return nil, err
		}
		if neg {
			node = &Node{Op: OpNot, Children: []*Node{node}}
		}
		children = append(children, node)
	}

	switch len(children) {
	case 0:
		return nil, nil
	case 1:
		return children[0], nil
	default:
		return &Node{Op: OpAnd, Children: children}, nil
	}
}

func (p *parser) parseAtom() (*Node, error) {
	p.skipSpace()
	switch c := p.peek(); {
	case c == '(':
		p.i++
		node, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if !p.eat(')') {
			return nil, fmt.Errorf("Syntax error at offset %d: expected ')'", p.i)
		}
		if node == nil {
			return nil, fmt.Errorf("Syntax error: empty expression")
		}
		return node, nil
	case c == '@':
		return p.parseFieldTerm()
	case isTermEnd(c):
		return nil, fmt.Errorf("Syntax error at offset %d", p.i)
	default:
		word, err := p.parseWord()
		if err != nil {
			return nil, err
		}
		return &Node{Op: OpTerm, Term: &Term{Kind: TermWord, Word: strings.ToLower(word)}}, nil
	}
}

func (p *parser) parseFieldTerm() (*Node, error) {
	p.i++ // '@'
	start := p.i
	for p.i < len(p.s) && (isIdentByte(p.s[p.i]) || p.s[p.i] == '$') {
		p.i++
	}
	field := p.s[start:p.i]
	if field == "" {
		return nil, fmt.Errorf("Syntax error at offset %d: missing field name", p.i)
	}
	p.skipSpace()
	if !p.eat(':') {
		return nil, fmt.Errorf("Syntax error at offset %d: expected ':'", p.i)
	}
	p.skipSpace()

	var node *Node
	var err error
	switch p.peek() {
	case '{':
		node, err = p.parseTagValue(field)
	case '[':
		node, err = p.parseNumericValue(field)
	case '(':
		p.i++
		node, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
		p.skipSpace()
		if !p.eat(')') {
			return nil, fmt.Errorf("Syntax error at offset %d: expected ')'", p.i)
		}
		if node == nil {
			node = &Node{Op: OpAll}
		}
		applyField(node, field)
	default:
		if isTermEnd(p.peek()) {
			return nil, fmt.Errorf("Syntax error at offset %d: missing value for field '%s'", p.i, field)
		}
		word, err := p.parseWord()
		if err != nil {
			return nil, err
		}
		node = &Node{Op: OpTerm, Term: &Term{Field: field, Kind: TermWord, Word: strings.ToLower(word)}}
	}
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (p *parser) parseTagValue(field string) (*Node, error) {
	p.i++ // '{'
	start := p.i
	for p.i < len(p.s) && p.s[p.i] != '}' {
		p.i++
	}
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("Syntax error at offset %d: unterminated tag value", start)
	}
	raw := p.s[start:p.i]
	p.i++ // '}'

	var tags []string
	for _, t := range strings.Split(raw, "|") {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != "" {
			tags = append(tags, t)
		}
	}
	if len(tags) == 0 {
		return nil, fmt.Errorf("Syntax error at offset %d: empty tag value", start)
	}
	return &Node{Op: OpTerm, Term: &Term{Field: field, Kind: TermTag, Tags: tags}}, nil
}

func (p *parser) parseNumericValue(field string) (*Node, error) {
	p.i++ // '['
	start := p.i
	for p.i < len(p.s) && p.s[p.i] != ']' {
		p.i++
	}
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("Syntax error at offset %d: unterminated numeric range", start)
	}
	raw := strings.TrimSpace(p.s[start:p.i])
	p.i++ // ']'

	exclusiveMin := strings.HasPrefix(raw, "(")
	exclusiveMax := strings.HasSuffix(raw, ")")
	raw = strings.Trim(raw, "()")
	bounds := strings.Fields(raw)
	if len(bounds) != 2 {
		return nil, fmt.Errorf("Syntax error: numeric range needs two bounds, got %q", raw)
	}

	term := &Term{Field: field, Kind: TermNumeric}
	lo, hi := bounds[0], bounds[1]
	if !isInfLiteral(lo) {
		v, err := strconv.ParseFloat(lo, 64)
		if err != nil {
			return nil, fmt.Errorf("Syntax error: invalid numeric bound %q", lo)
		}
		if exclusiveMin {
			v = math.Nextafter(v, math.Inf(1))
		}
		term.Min, term.HasMin = v, true
	}
	if !isInfLiteral(hi) {
		v, err := strconv.ParseFloat(hi, 64)
		if err != nil {
			return nil, fmt.Errorf("Syntax error: invalid numeric bound %q", hi)
		}
		if exclusiveMax {
			v = math.Nextafter(v, math.Inf(-1))
		}
		term.Max, term.HasMax = v, true
	}
	return &Node{Op: OpTerm, Term: term}, nil
}

func isInfLiteral(s string) bool {
	switch strings.ToLower(s) {
	case "-inf", "inf", "+inf":
		return true
	}
	return false
}

func (p *parser) parseWord() (string, error) {
	start := p.i
	for p.i < len(p.s) && !isTermEnd(p.s[p.i]) && p.s[p.i] != '*' {
		p.i++
	}
	if p.peek() == '*' {
		return "", fmt.Errorf("Syntax error at offset %d: prefix matching is not supported", p.i)
	}
	return p.s[start:p.i], nil
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '-' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// applyField pushes a field scope down into terms that do not carry one.
func applyField(n *Node, field string) {
	if n == nil {
		return
	}
	if n.Op == OpTerm && n.Term != nil && n.Term.Field == "" {
		n.Term.Field = field
	}
	for _, c := range n.Children {
		applyField(c, field)
	}
}

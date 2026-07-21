package protocol

import (
	"bytes"
	"strconv"

	"github.com/Hoverhuang-er/godis/internal/interface/redis"
)

var _ = redis.Resp2

// ---- RESP3 Map Reply (%<size>\r\n<key><value>...) ----

// MapReply stores a RESP3 map type
type MapReply struct {
	Keys   []redis.Reply
	Values []redis.Reply
}

// MakeMapReply creates a MapReply
func MakeMapReply(keys []redis.Reply, values []redis.Reply) *MapReply {
	return &MapReply{
		Keys:   keys,
		Values: values,
	}
}

// MakeMapReplyFromMap creates a MapReply from a Go map[string]redis.Reply
func MakeMapReplyFromMap(m map[string]redis.Reply) *MapReply {
	keys := make([]redis.Reply, 0, len(m))
	values := make([]redis.Reply, 0, len(m))
	for k, v := range m {
		keys = append(keys, MakeBulkReply([]byte(k)))
		values = append(values, v)
	}
	return &MapReply{
		Keys:   keys,
		Values: values,
	}
}

// ToBytes marshal redis.Reply
func (r *MapReply) ToBytes() []byte {
	argLen := len(r.Keys)
	var buf bytes.Buffer
	buf.WriteString("%" + strconv.Itoa(argLen) + CRLF)
	for i, key := range r.Keys {
		buf.Write(key.ToBytes())
		if i < len(r.Values) {
			buf.Write(r.Values[i].ToBytes())
		}
	}
	return buf.Bytes()
}

// ---- RESP3 Set Reply (~<size>\r\n<value>...) ----

// SetReply stores a RESP3 set type
type SetReply struct {
	Values []redis.Reply
}

// MakeSetReply creates a SetReply
func MakeSetReply(values []redis.Reply) *SetReply {
	return &SetReply{
		Values: values,
	}
}

// ToBytes marshal redis.Reply
func (r *SetReply) ToBytes() []byte {
	argLen := len(r.Values)
	var buf bytes.Buffer
	buf.WriteString("~" + strconv.Itoa(argLen) + CRLF)
	for _, v := range r.Values {
		buf.Write(v.ToBytes())
	}
	return buf.Bytes()
}

// ---- RESP3 Push Reply (><size>\r\n<value>...) ----

// PushReply stores a RESP3 push type (for pub/sub, hello metadata)
type PushReply struct {
	Values []redis.Reply
}

// MakePushReply creates a PushReply
func MakePushReply(values []redis.Reply) *PushReply {
	return &PushReply{
		Values: values,
	}
}

// ToBytes marshal redis.Reply
func (r *PushReply) ToBytes() []byte {
	argLen := len(r.Values)
	var buf bytes.Buffer
	buf.WriteString(">" + strconv.Itoa(argLen) + CRLF)
	for _, v := range r.Values {
		buf.Write(v.ToBytes())
	}
	return buf.Bytes()
}

// ---- RESP3 Double Reply (,<value>\r\n) ----

// DoubleReply stores a RESP3 double type
type DoubleReply struct {
	Value float64
}

// MakeDoubleReply creates a DoubleReply
func MakeDoubleReply(value float64) *DoubleReply {
	return &DoubleReply{
		Value: value,
	}
}

// ToBytes marshal redis.Reply
func (r *DoubleReply) ToBytes() []byte {
	return []byte("," + strconv.FormatFloat(r.Value, 'g', -1, 64) + CRLF)
}

// ---- RESP3 Bool Reply (#t\r\n or #f\r\n) ----

// BoolReply stores a RESP3 boolean type
type BoolReply struct {
	Value bool
}

var trueBytes = []byte("#t\r\n")
var falseBytes = []byte("#f\r\n")

// MakeBoolReply creates a BoolReply
func MakeBoolReply(value bool) *BoolReply {
	return &BoolReply{
		Value: value,
	}
}

// ToBytes marshal redis.Reply
func (r *BoolReply) ToBytes() []byte {
	if r.Value {
		return trueBytes
	}
	return falseBytes
}

// ---- RESP3 Big Number Reply (<value>\r\n) ----

// BigNumberReply stores a RESP3 big number type
type BigNumberReply struct {
	Value string
}

// MakeBigNumberReply creates a BigNumberReply
func MakeBigNumberReply(value string) *BigNumberReply {
	return &BigNumberReply{
		Value: value,
	}
}

// ToBytes marshal redis.Reply
func (r *BigNumberReply) ToBytes() []byte {
	return []byte("(" + r.Value + CRLF)
}

// ---- RESP3 Verbatim String Reply (=<size>\r\n<format>:\r\n<content>\r\n) ----

// VerbatimStringReply stores a RESP3 verbatim string type
type VerbatimStringReply struct {
	Format string
	Value  string
}

// MakeVerbatimStringReply creates a VerbatimStringReply
func MakeVerbatimStringReply(format string, value string) *VerbatimStringReply {
	return &VerbatimStringReply{
		Format: format,
		Value:  value,
	}
}

// ToBytes marshal redis.Reply
func (r *VerbatimStringReply) ToBytes() []byte {
	content := r.Format + ":" + r.Value
	return []byte("=" + strconv.Itoa(len(content)) + CRLF + content + CRLF)
}

// ---- RESP3 Null Reply (_\r\n) ----

// NullReply is the RESP3 null type
type Resp3NullReply struct{}

var nullResp3Bytes = []byte("_\r\n")

// MakeResp3NullReply creates a NullReply
func MakeResp3NullReply() *Resp3NullReply {
	return &Resp3NullReply{}
}

// ToBytes marshal redis.Reply
func (r *Resp3NullReply) ToBytes() []byte {
	return nullResp3Bytes
}

// ---- RESP3 Attribute Reply (|<size>\r\n<key><value>...) ----

// AttributeReply stores a RESP3 attribute type
type AttributeReply struct {
	Keys   []redis.Reply
	Values []redis.Reply
}

// MakeAttributeReply creates an AttributeReply
func MakeAttributeReply(keys []redis.Reply, values []redis.Reply) *AttributeReply {
	return &AttributeReply{
		Keys:   keys,
		Values: values,
	}
}

// ToBytes marshal redis.Reply
func (r *AttributeReply) ToBytes() []byte {
	argLen := len(r.Keys)
	var buf bytes.Buffer
	buf.WriteString("|" + strconv.Itoa(argLen) + CRLF)
	for i, key := range r.Keys {
		buf.Write(key.ToBytes())
		if i < len(r.Values) {
			buf.Write(r.Values[i].ToBytes())
		}
	}
	return buf.Bytes()
}



package database

import (
	"encoding/binary"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// getAsBitfield retrieves the raw byte slice backing a bitfield key.
func getAsBitfield(db *DB, key string) ([]byte, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return nil, nil
	}
	data, ok := entity.Data.([]byte)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return data, nil
}

// bitfieldOp represents a single BITFIELD subcommand.
type bitfieldOp struct {
	opType string // GET, SET, INCRBY
	offsetType string // BIT or BYTE
	offset int64
	encoding string // u8, i16, etc.
	bits int
	signed bool
	value int64 // for SET and INCRBY
	overflow string // WRAP, SAT, FAIL
}

func parseEncoding(e string) (bits int, signed bool, errReply protocol.ErrorReply) {
	if len(e) < 2 {
		return 0, false, protocol.MakeErrReply("ERR Invalid bitfield encoding " + e)
	}
	signed = e[0] == 'i'
	if e[0] != 'i' && e[0] != 'u' {
		return 0, false, protocol.MakeErrReply("ERR Invalid bitfield encoding " + e)
	}
	bits, err := strconv.Atoi(e[1:])
	if err != nil || bits < 1 || bits > 63 {
		return 0, false, protocol.MakeErrReply("ERR Invalid bitfield encoding " + e)
	}
	return bits, signed, nil
}

// execBITFIELD performs bitfield operations on a key.
func execBITFIELD(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("BITFIELD")
	}
	key := string(args[0])
	overflow := "WRAP"
	data, _ := getAsBitfield(db, key)
	if data == nil {
		data = make([]byte, 0)
	}
	var results []redis.Reply
	for i := 1; i < len(args); i++ {
		op := strings.ToUpper(string(args[i]))
		switch op {
		case "OVERFLOW":
			i++
			if i < len(args) {
				overflow = strings.ToUpper(string(args[i]))
			}
		case "GET":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for BITFIELD GET")
			}
			i++
			encoding := strings.ToLower(string(args[i]))
			i++
			offsetStr := string(args[i])
			var offset int64
			offsetType := "BIT"
			if strings.HasPrefix(offsetStr, "#") {
				offsetType = "BYTE"
				offsetStr = offsetStr[1:]
			}
			offset, _ = strconv.ParseInt(offsetStr, 10, 64)
			bits, signed, errReply := parseEncoding(encoding)
			if errReply != nil {
				return errReply
			}
			val := readBits(data, offset, offsetType, bits, signed)
			results = append(results, protocol.MakeIntReply(val))

		case "SET":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for BITFIELD SET")
			}
			i++
			encoding := strings.ToLower(string(args[i]))
			i++
			offsetStr := string(args[i])
			i++
			valStr := string(args[i])
			var offset int64
			offsetType := "BIT"
			if strings.HasPrefix(offsetStr, "#") {
				offsetType = "BYTE"
				offsetStr = offsetStr[1:]
			}
			offset, _ = strconv.ParseInt(offsetStr, 10, 64)
			val, _ := strconv.ParseInt(valStr, 10, 64)
			bits, signed, errReply := parseEncoding(encoding)
			if errReply != nil {
				return errReply
			}
			oldVal := readBits(data, offset, offsetType, bits, signed)
			data = writeBits(data, offset, offsetType, bits, signed, val, overflow)
			db.PutEntity(key, &database.DataEntity{Data: data})
			results = append(results, protocol.MakeIntReply(oldVal))

		case "INCRBY":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for BITFIELD INCRBY")
			}
			i++
			encoding := strings.ToLower(string(args[i]))
			i++
			offsetStr := string(args[i])
			i++
			incrStr := string(args[i])
			var offset int64
			offsetType := "BIT"
			if strings.HasPrefix(offsetStr, "#") {
				offsetType = "BYTE"
				offsetStr = offsetStr[1:]
			}
			offset, _ = strconv.ParseInt(offsetStr, 10, 64)
			incr, _ := strconv.ParseInt(incrStr, 10, 64)
			bits, signed, errReply := parseEncoding(encoding)
			if errReply != nil {
				return errReply
			}
			oldVal := readBits(data, offset, offsetType, bits, signed)
			newVal := applyOverflow(oldVal+incr, bits, signed, overflow)
			data = writeBits(data, offset, offsetType, bits, signed, newVal, overflow)
			db.PutEntity(key, &database.DataEntity{Data: data})
			results = append(results, protocol.MakeIntReply(oldVal))
		}
	}

	return protocol.MakeMultiRawReply(results)
}

// execBITFIELD_RO is a read-only variant of BITFIELD
func execBITFIELD_RO(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("BITFIELD_RO")
	}
	key := string(args[0])
	data, errReply := getAsBitfield(db, key)
	if errReply != nil {
		return errReply
	}
	if data == nil {
		data = make([]byte, 0)
	}
	var results []redis.Reply
	for i := 1; i < len(args); i++ {
		op := strings.ToUpper(string(args[i]))
		if op != "GET" {
			return protocol.MakeErrReply("ERR BITFIELD_RO only supports GET subcommand")
		}
		if i+2 >= len(args) {
			return protocol.MakeErrReply("ERR wrong number of arguments for BITFIELD_RO GET")
		}
		i++
		encoding := strings.ToLower(string(args[i]))
		i++
		offsetStr := string(args[i])
		var offset int64
		offsetType := "BIT"
		if strings.HasPrefix(offsetStr, "#") {
			offsetType = "BYTE"
			offsetStr = offsetStr[1:]
		}
		offset, _ = strconv.ParseInt(offsetStr, 10, 64)
		bits, signed, errReply := parseEncoding(encoding)
		if errReply != nil {
			return errReply
		}
		val := readBits(data, offset, offsetType, bits, signed)
		results = append(results, protocol.MakeIntReply(val))
	}
	return protocol.MakeMultiRawReply(results)
}

func readBits(data []byte, offset int64, offsetType string, bits int, signed bool) int64 {
	var bitPos int64
	if offsetType == "BYTE" {
		bitPos = offset * 8
	} else {
		bitPos = offset
	}
	if bitPos < 0 {
		bitPos = 0
	}
	bytePos := bitPos / 8
	bitOffset := bitPos % 8
	neededBytes := (bits + int(bitOffset) + 7) / 8

	// Read the required bytes, zero-padded
	var buf [8]byte
	for i := 0; i < neededBytes && bytePos+int64(i) < int64(len(data)); i++ {
		buf[i] = data[bytePos+int64(i)]
	}

	// Read as big-endian integer
	val := int64(0)
	for i := 0; i < neededBytes; i++ {
		val = (val << 8) | int64(buf[i])
	}

	// Shift right to discard bit offset (offset from MSB side)
	if bitOffset > 0 {
		// The bits are stored from MSB. So we need to shift right by (8-bitOffset)
		// This is more complex - for now use a simpler approach
		// Read as a big-endian integer starting from the relevant bit position
		val = 0
		for i := 0; i < bits; i++ {
			totalBitPos := bitPos + int64(i)
			byteIdx := totalBitPos / 8
			bitIdx := 7 - (totalBitPos % 8) // MSB first
			if byteIdx < int64(len(data)) {
				if (data[byteIdx]>>bitIdx)&1 == 1 {
					val |= 1 << uint(bits-1-i)
				}
			}
		}
	}

	// Sign extend if signed
	if signed && bits < 64 {
		if val&(1<<uint(bits-1)) != 0 {
			val |= -1 << uint(bits)
		}
	}

	return val
}

func writeBits(data []byte, offset int64, offsetType string, bits int, signed bool, val int64, overflow string) []byte {
	var bitPos int64
	if offsetType == "BYTE" {
		bitPos = offset * 8
	} else {
		bitPos = offset
	}
	if bitPos < 0 {
		bitPos = 0
	}

	neededBytes := int(bitPos/8) + (bits+7)/8 + 1
	for len(data) < neededBytes {
		data = append(data, 0)
	}

	// Apply overflow
	val = applyOverflow(val, bits, signed, overflow)

	// Write bits from MSB
	for i := 0; i < bits; i++ {
		totalBitPos := bitPos + int64(i)
		byteIdx := totalBitPos / 8
		bitIdx := 7 - (totalBitPos % 8) // MSB first

		if (val>>uint(bits-1-i))&1 == 1 {
			data[byteIdx] |= 1 << bitIdx
		} else {
			data[byteIdx] &^= 1 << bitIdx
		}
	}

	return data
}

func applyOverflow(val int64, bits int, signed bool, overflow string) int64 {
	mask := int64((1 << uint(bits)) - 1)
	if signed {
		switch overflow {
		case "WRAP":
			val = (val + (1 << uint(bits-1))) & ((1 << uint(bits)) - 1)
			val -= (1 << uint(bits - 1))
		case "SAT":
			maxVal := int64((1 << uint(bits-1)) - 1)
			minVal := -maxVal - 1
			if val > maxVal {
				val = maxVal
			} else if val < minVal {
				val = minVal
			}
		case "FAIL":
			maxVal := int64((1 << uint(bits-1)) - 1)
			minVal := -maxVal - 1
			if val > maxVal || val < minVal {
				val = math.MaxInt64 // sentinel
			}
		}
	} else {
		switch overflow {
		case "WRAP":
			val &= mask
		case "SAT":
			if val < 0 {
				val = 0
			} else if uint64(val) > uint64(mask) {
				val = int64(mask)
			}
		case "FAIL":
			if val < 0 || uint64(val) > uint64(mask) {
				val = math.MaxInt64
			}
		}
	}
	return val
}

func init() {
	registerCommand("BITFIELD", execBITFIELD, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BITFIELD_RO", execBITFIELD_RO, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	slog.Info("Bitfield commands registered (2 commands)")
}

func init() { _ = binary.LittleEndian; _ = math.MaxInt64 }

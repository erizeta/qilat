package h2

import (
	"strconv"

	"github.com/erizeta/qilat/internal/bytesconv"
	"golang.org/x/net/http2/hpack"
)

// staticNames maps a header name to its first index in the HPACK static table (RFC 7541 Appendix A).
var staticNames = func() map[string]uint8 {
	names := [...]string{
		":authority", ":method", ":method", ":path", ":path", ":scheme", ":scheme", ":status", ":status",
		":status", ":status", ":status", ":status", ":status", "accept-charset", "accept-encoding",
		"accept-language", "accept-ranges", "accept", "access-control-allow-origin", "age", "allow",
		"authorization", "cache-control", "content-disposition", "content-encoding", "content-language",
		"content-length", "content-location", "content-range", "content-type", "cookie", "date", "etag",
		"expect", "expires", "from", "host", "if-match", "if-modified-since", "if-none-match", "if-range",
		"if-unmodified-since", "last-modified", "link", "location", "max-forwards", "proxy-authenticate",
		"proxy-authorization", "range", "referer", "refresh", "retry-after", "server", "set-cookie",
		"strict-transport-security", "transfer-encoding", "user-agent", "vary", "via", "www-authenticate",
	}
	m := make(map[string]uint8, len(names))
	for i, n := range names {
		if _, ok := m[n]; !ok {
			m[n] = uint8(i + 1)
		}
	}
	return m
}()

// staticStatus holds the static-table index of the :status values it contains.
var staticStatus = map[int]uint8{200: 8, 204: 9, 206: 10, 304: 11, 400: 12, 404: 13, 500: 14}

// encoder is an HPACK encoder with a dynamic table (RFC 7541). It is only used under the connection's write
// lock, so blocks are encoded in the order they are sent. Table entries own copies of their bytes, and the
// buffers of evicted entries are reused, so steady-state encoding does not allocate.
type encoder struct {
	limit   uint32 // the largest table we use, whatever the peer allows
	maxSize uint32 // current table capacity
	size    uint32
	// A capacity change must be announced at the start of the next block, with the smallest value in between.
	pending    bool
	pendingMin uint32

	entries  []hentry // oldest first, from head
	head     int
	inserted uint64            // total insertions; an entry's id is its insertion number
	fields   map[uint64]uint64 // hash(name, value) -> id of the newest such entry
	names    map[uint64]uint64 // hash(name) -> id of the newest entry with that name
	free     [][]byte
}

type hentry struct {
	buf           []byte // name followed by value
	nameLen       int
	id            uint64
	hField, hName uint64
}

func newEncoder(limit uint32) *encoder {
	return &encoder{limit: limit, maxSize: min(limit, defaultHeaderTable),
		fields: make(map[uint64]uint64), names: make(map[uint64]uint64)}
}

// setMaxSize applies the peer's SETTINGS_HEADER_TABLE_SIZE.
func (e *encoder) setMaxSize(v uint32) {
	v = min(v, e.limit)
	if !e.pending || v < e.pendingMin {
		e.pendingMin = v
	}
	e.pending = true
	e.maxSize = v
	e.evict(0)
}

// startBlock writes any pending table size update, which must lead the next header block.
func (e *encoder) startBlock(dst []byte) []byte {
	if !e.pending {
		return dst
	}
	e.pending = false
	if e.pendingMin < e.maxSize {
		dst = appendInt(dst, 0x20, 5, uint64(e.pendingMin))
	}
	return appendInt(dst, 0x20, 5, uint64(e.maxSize))
}

func (e *encoder) appendStatus(dst []byte, status int) []byte {
	if i, ok := staticStatus[status]; ok {
		return appendInt(dst, 0x80, 7, uint64(i))
	}
	var b [3]byte
	return e.appendField(dst, statusName, strconv.AppendInt(b[:0], int64(status), 10))
}

var statusName = []byte(":status")

// appendField encodes one field, indexing it unless its values are typically unique or sensitive.
func (e *encoder) appendField(dst, name, value []byte) []byte {
	hn := hashBytes(fnvOffset, name)
	hf := hashBytes(hashBytes(hn, []byte{0}), value)
	if id, ok := e.fields[hf]; ok {
		if en := e.entry(id); en != nil && string(en.buf[:en.nameLen]) == string(name) && string(en.buf[en.nameLen:]) == string(value) {
			return appendInt(dst, 0x80, 7, e.index(id))
		}
	}
	nameIdx, _ := staticNames[bytesconv.B2S(name)]
	idx := uint64(nameIdx)
	if idx == 0 {
		if id, ok := e.names[hn]; ok {
			if en := e.entry(id); en != nil && string(en.buf[:en.nameLen]) == string(name) {
				idx = e.index(id)
			}
		}
	}
	size := uint32(len(name) + len(value) + 32)
	index := size <= e.maxSize && !noIndex(name)
	if index {
		dst = appendInt(dst, 0x40, 6, idx)
	} else {
		dst = appendInt(dst, 0x00, 4, idx)
	}
	if idx == 0 {
		dst = appendString(dst, name)
	}
	dst = appendString(dst, value)
	if index {
		e.insert(name, value, size, hf, hn)
	}
	return dst
}

func (e *encoder) insert(name, value []byte, size uint32, hf, hn uint64) {
	e.evict(size)
	var buf []byte
	if n := len(e.free); n > 0 && cap(e.free[n-1]) >= len(name)+len(value) {
		buf = e.free[n-1][:0]
		e.free = e.free[:n-1]
	}
	buf = append(append(buf, name...), value...)
	id := e.inserted
	e.inserted++
	e.entries = append(e.entries, hentry{buf: buf, nameLen: len(name), id: id, hField: hf, hName: hn})
	e.size += size
	e.fields[hf] = id
	e.names[hn] = id
}

// evict drops the oldest entries until room more bytes fit.
func (e *encoder) evict(room uint32) {
	for e.size+room > e.maxSize && e.head < len(e.entries) {
		en := &e.entries[e.head]
		e.size -= uint32(len(en.buf) + 32)
		if e.fields[en.hField] == en.id {
			delete(e.fields, en.hField)
		}
		if e.names[en.hName] == en.id {
			delete(e.names, en.hName)
		}
		if len(e.free) < 16 {
			e.free = append(e.free, en.buf)
		}
		*en = hentry{}
		e.head++
	}
	if e.head > 32 && e.head*2 > len(e.entries) {
		n := copy(e.entries, e.entries[e.head:])
		clear(e.entries[n:])
		e.entries = e.entries[:n]
		e.head = 0
	}
}

func (e *encoder) entry(id uint64) *hentry {
	if e.head == len(e.entries) || id < e.entries[e.head].id {
		return nil
	}
	return &e.entries[e.head+int(id-e.entries[e.head].id)]
}

// index converts an entry id to its HPACK index: the newest entry is 62, just past the static table.
func (e *encoder) index(id uint64) uint64 { return 62 + (e.inserted - 1 - id) }

// noIndex reports fields whose values rarely repeat or should not be kept in the table.
func noIndex(name []byte) bool {
	switch string(name) {
	case "etag", "last-modified", "location", "content-range", "age", "set-cookie",
		"authorization", "proxy-authenticate", "www-authenticate":
		return true
	}
	return false
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

func hashBytes(h uint64, b []byte) uint64 {
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime
	}
	return h
}

// appendString writes a string literal, Huffman-coded when that is shorter.
func appendString(dst, s []byte) []byte {
	str := bytesconv.B2S(s)
	if n := hpack.HuffmanEncodeLength(str); n < uint64(len(s)) {
		dst = appendInt(dst, 0x80, 7, n)
		return hpack.AppendHuffmanString(dst, str)
	}
	dst = appendInt(dst, 0x00, 7, uint64(len(s)))
	return append(dst, s...)
}

// appendInt writes an HPACK integer with an n-bit prefix (RFC 7541 section 5.1); first holds the other bits.
func appendInt(dst []byte, first byte, n uint, v uint64) []byte {
	limit := uint64(1)<<n - 1
	if v < limit {
		return append(dst, first|byte(v))
	}
	dst = append(dst, first|byte(limit))
	v -= limit
	for v >= 128 {
		dst = append(dst, byte(v&0x7f|0x80))
		v >>= 7
	}
	return append(dst, byte(v))
}

// newDecoder returns an HPACK decoder whose strings are capped at maxString bytes.
func newDecoder(tableSize uint32, maxString int, emit func(hpack.HeaderField)) *hpack.Decoder {
	d := hpack.NewDecoder(tableSize, emit)
	d.SetMaxStringLength(maxString)
	return d
}

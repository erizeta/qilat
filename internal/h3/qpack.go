package h3

import (
	"errors"
	"strconv"

	"github.com/erizeta/qilat/internal/bytesconv"
	"github.com/erizeta/qilat/internal/httpfields"
	"golang.org/x/net/http2/hpack"
)

// QPACK (RFC 9204) restricted to the static table: the server advertises a dynamic table capacity of 0,
// so a peer must never reference dynamic entries, and the server never creates any. That needs no encoder
// or decoder streams and no blocked streams, and lets decoding fill requests without allocating.

var errQPACK = errors.New("h3: invalid QPACK field section")

// staticIndex maps a field to its static-table index; staticNameIndex maps a name to its first index.
var (
	staticIndex     = make(map[[2]string]uint64, len(staticTable))
	staticNameIndex = make(map[string]uint64, len(staticTable))
)

func init() {
	for i, f := range staticTable {
		staticIndex[[2]string{f.name, f.value}] = uint64(i)
		if _, ok := staticNameIndex[f.name]; !ok {
			staticNameIndex[f.name] = uint64(i)
		}
	}
}

// decoder decodes field sections. Its buffers are reused, so a decoder serves one section at a time.
type decoder struct {
	name  huffBuf
	value huffBuf
}

// huffBuf collects Huffman-decoded bytes.
type huffBuf struct{ b []byte }

func (h *huffBuf) Write(p []byte) (int, error) {
	h.b = append(h.b, p...)
	return len(p), nil
}

// fieldSink receives decoded fields; *httpfields.Request is the one used outside tests.
type fieldSink interface {
	Field(name, value []byte)
}

var _ fieldSink = (*httpfields.Request)(nil)

// decode decodes a field section into req. It fails if the section references the dynamic table or is malformed,
// which is a connection error (QPACK_DECOMPRESSION_FAILED).
func (d *decoder) decode(p []byte, req fieldSink) error {
	ric, p, err := readInt(p, 8)
	if err != nil || ric != 0 {
		return errQPACK
	}
	// Without dynamic entries the Delta Base must be 0; the sign bit is ignored, as quic-go/qpack does.
	var base uint64
	if base, p, err = readInt(p, 7); err != nil || base != 0 {
		return errQPACK
	}
	for len(p) > 0 {
		var name, value []byte
		b := p[0]
		switch {
		case b&0x80 != 0: // indexed field line
			if b&0x40 == 0 {
				return errQPACK
			}
			var idx uint64
			if idx, p, err = readInt(p, 6); err != nil || idx >= uint64(len(staticTable)) {
				return errQPACK
			}
			f := &staticTable[idx]
			name, value = bytesconv.S2B(f.name), bytesconv.S2B(f.value)
		case b&0x40 != 0: // literal field line with name reference
			if b&0x10 == 0 {
				return errQPACK
			}
			var idx uint64
			if idx, p, err = readInt(p, 4); err != nil || idx >= uint64(len(staticTable)) {
				return errQPACK
			}
			name = bytesconv.S2B(staticTable[idx].name)
			if value, p, err = d.readString(&d.value, p, 7); err != nil {
				return err
			}
		case b&0x20 != 0: // literal field line with literal name
			if name, p, err = d.readString(&d.name, p, 3); err != nil {
				return err
			}
			if value, p, err = d.readString(&d.value, p, 7); err != nil {
				return err
			}
		default: // post-base forms reference the dynamic table
			return errQPACK
		}
		req.Field(name, value)
	}
	return nil
}

// readString reads a string literal whose length has an n-bit prefix and whose Huffman flag is the bit above it.
func (d *decoder) readString(buf *huffBuf, p []byte, n uint) ([]byte, []byte, error) {
	if len(p) == 0 {
		return nil, nil, errQPACK
	}
	huffman := p[0]&(1<<n) != 0
	length, p, err := readInt(p, n)
	if err != nil || length > uint64(len(p)) {
		return nil, nil, errQPACK
	}
	s, p := p[:length], p[length:]
	if !huffman {
		return s, p, nil
	}
	buf.b = buf.b[:0]
	if _, err := hpack.HuffmanDecode(buf, s); err != nil {
		return nil, nil, errQPACK
	}
	return buf.b, p, nil
}

// readInt reads an integer with an n-bit prefix (RFC 7541 section 5.1).
func readInt(p []byte, n uint) (uint64, []byte, error) {
	if len(p) == 0 {
		return 0, nil, errQPACK
	}
	limit := uint64(1)<<n - 1
	v := uint64(p[0]) & limit
	p = p[1:]
	if v < limit {
		return v, p, nil
	}
	for shift := uint(0); ; shift += 7 {
		if len(p) == 0 || shift > 56 {
			return 0, nil, errQPACK
		}
		b := p[0]
		p = p[1:]
		v += uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return v, p, nil
		}
	}
}

// appendSectionPrefix starts a field section: Required Insert Count 0 and Base 0.
func appendSectionPrefix(b []byte) []byte { return append(b, 0, 0) }

func appendStatus(b []byte, status int) []byte {
	var s [3]byte
	return appendField(b, statusName, strconv.AppendInt(s[:0], int64(status), 10))
}

var statusName = []byte(":status")

// appendField encodes a field as a static reference, or a literal with a static name reference, or a literal.
func appendField(b, name, value []byte) []byte {
	if idx, ok := staticIndex[[2]string{bytesconv.B2S(name), bytesconv.B2S(value)}]; ok {
		return appendInt(b, 0xc0, 6, idx)
	}
	if idx, ok := staticNameIndex[bytesconv.B2S(name)]; ok {
		b = appendInt(b, 0x50, 4, idx)
	} else {
		b = appendString(b, 0x20, 3, name)
	}
	return appendString(b, 0x00, 7, value)
}

// appendString writes a string literal with an n-bit length prefix, Huffman-coded when shorter.
func appendString(b []byte, first byte, n uint, s []byte) []byte {
	str := bytesconv.B2S(s)
	if hl := hpack.HuffmanEncodeLength(str); hl < uint64(len(s)) {
		b = appendInt(b, first|1<<n, n, hl)
		return hpack.AppendHuffmanString(b, str)
	}
	b = appendInt(b, first, n, uint64(len(s)))
	return append(b, s...)
}

// appendInt writes an integer with an n-bit prefix; first holds the bits above the prefix.
func appendInt(b []byte, first byte, n uint, v uint64) []byte {
	limit := uint64(1)<<n - 1
	if v < limit {
		return append(b, first|byte(v))
	}
	b = append(b, first|byte(limit))
	v -= limit
	for v >= 128 {
		b = append(b, byte(v&0x7f|0x80))
		v >>= 7
	}
	return append(b, byte(v))
}

// Package h3 implements the server side of HTTP/3 (RFC 9114) on raw QUIC streams from quic-go. It is the
// only package that imports quic-go, so changes in its pre-1.0 API stay here.
package h3

import (
	"bufio"
	"errors"
	"io"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

const (
	frameData        = 0x0
	frameHeaders     = 0x1
	frameCancelPush  = 0x3
	frameSettings    = 0x4
	framePushPromise = 0x5
	frameGoAway      = 0x7
	frameMaxPushID   = 0xd
)

// isHTTP2Frame reports frame types reserved because HTTP/2 used them; receiving one is an error (RFC 9114 section 7.2.8).
func isHTTP2Frame(t uint64) bool { return t == 0x2 || t == 0x6 || t == 0x8 || t == 0x9 }

const (
	streamControl      = 0x00
	streamPush         = 0x01
	streamQPACKEncoder = 0x02
	streamQPACKDecoder = 0x03
)

const (
	settingQPACKMaxTableCapacity = 0x1
	settingMaxFieldSectionSize   = 0x6
	settingQPACKBlockedStreams   = 0x7
)

// HTTP/3 and QPACK error codes (RFC 9114 section 8.1, RFC 9204 section 6).
const (
	errNoError              = 0x100
	errGeneralProtocol      = 0x101
	errInternal             = 0x102
	errStreamCreation       = 0x103
	errClosedCriticalStream = 0x104
	errFrameUnexpected      = 0x105
	errFrame                = 0x106
	errExcessiveLoad        = 0x107
	errID                   = 0x108
	errSettings             = 0x109
	errMissingSettings      = 0x10a
	errRequestRejected      = 0x10b
	errRequestCancelled     = 0x10c
	errRequestIncomplete    = 0x10d
	errMessage              = 0x10e
	errQPACKDecompression   = 0x200
	errQPACKEncoderStream   = 0x201
)

// connError closes the whole connection with an HTTP/3 error code.
type connError struct {
	code   quic.ApplicationErrorCode
	reason string
}

func (e *connError) Error() string { return "h3: connection error: " + e.reason }

func newConnError(code quic.ApplicationErrorCode, reason string) error {
	return &connError{code, reason}
}

// readFrameHeader reads a frame type and length.
func readFrameHeader(r io.ByteReader) (typ, length uint64, err error) {
	if typ, err = quicvarint.Read(r); err != nil {
		return 0, 0, err
	}
	if length, err = quicvarint.Read(r); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, 0, err
	}
	return typ, length, nil
}

// skip discards n bytes of an ignored frame.
func skip(br *bufio.Reader, n uint64) error {
	for n > 0 {
		m, err := br.Discard(int(min(n, 1<<20)))
		n -= uint64(m)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return nil
}

func appendFrameHeader(b []byte, typ, length uint64) []byte {
	return quicvarint.Append(quicvarint.Append(b, typ), length)
}

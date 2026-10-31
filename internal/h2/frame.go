// Package h2 implements the server side of HTTP/2 (RFC 9113) on top of internal/exchange.
package h2

import (
	"errors"
	"fmt"
	"os"
)

// ClientPreface is what every HTTP/2 client sends first (RFC 9113 section 3.4).
const ClientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

type frameType uint8

const (
	frameData           frameType = 0x0
	frameHeaders        frameType = 0x1
	framePriority       frameType = 0x2
	frameRSTStream      frameType = 0x3
	frameSettings       frameType = 0x4
	framePushPromise    frameType = 0x5
	framePing           frameType = 0x6
	frameGoAway         frameType = 0x7
	frameWindowUpdate   frameType = 0x8
	frameContinuation   frameType = 0x9
	framePriorityUpdate frameType = 0x10 // RFC 9218
)

const (
	flagEndStream  uint8 = 0x1
	flagAck        uint8 = 0x1
	flagEndHeaders uint8 = 0x4
	flagPadded     uint8 = 0x8
	flagPriority   uint8 = 0x20
)

const (
	frameHeaderLen     = 9
	minMaxFrameSize    = 1 << 14
	maxMaxFrameSize    = 1<<24 - 1
	maxWindow          = 1<<31 - 1
	defaultWindow      = 65535
	maxStreamID        = 1<<31 - 1
	defaultHeaderTable = 4096
)

type settingID uint16

const (
	settingHeaderTableSize      settingID = 0x1
	settingEnablePush           settingID = 0x2
	settingMaxConcurrentStreams settingID = 0x3
	settingInitialWindowSize    settingID = 0x4
	settingMaxFrameSize         settingID = 0x5
	settingMaxHeaderListSize    settingID = 0x6
	settingNoRFC7540Priorities  settingID = 0x9 // RFC 9218 section 2.1
)

// ErrCode is an HTTP/2 error code (RFC 9113 section 7).
type ErrCode uint32

// HTTP/2 error codes.
const (
	ErrCodeNo                 ErrCode = 0x0
	ErrCodeProtocol           ErrCode = 0x1
	ErrCodeInternal           ErrCode = 0x2
	ErrCodeFlowControl        ErrCode = 0x3
	ErrCodeSettingsTimeout    ErrCode = 0x4
	ErrCodeStreamClosed       ErrCode = 0x5
	ErrCodeFrameSize          ErrCode = 0x6
	ErrCodeRefusedStream      ErrCode = 0x7
	ErrCodeCancel             ErrCode = 0x8
	ErrCodeCompression        ErrCode = 0x9
	ErrCodeConnect            ErrCode = 0xa
	ErrCodeEnhanceYourCalm    ErrCode = 0xb
	ErrCodeInadequateSecurity ErrCode = 0xc
	ErrCodeHTTP11Required     ErrCode = 0xd
)

var errCodeNames = [...]string{
	"NO_ERROR", "PROTOCOL_ERROR", "INTERNAL_ERROR", "FLOW_CONTROL_ERROR", "SETTINGS_TIMEOUT", "STREAM_CLOSED",
	"FRAME_SIZE_ERROR", "REFUSED_STREAM", "CANCEL", "COMPRESSION_ERROR", "CONNECT_ERROR", "ENHANCE_YOUR_CALM",
	"INADEQUATE_SECURITY", "HTTP_1_1_REQUIRED",
}

func (e ErrCode) String() string {
	if int(e) < len(errCodeNames) {
		return errCodeNames[e]
	}
	return fmt.Sprintf("ERR_0x%x", uint32(e))
}

// connError ends the whole connection with GOAWAY.
type connError struct {
	code   ErrCode
	reason string
}

func (e connError) Error() string { return "h2: connection error " + e.code.String() + ": " + e.reason }

// streamError resets one stream with RST_STREAM.
type streamError struct {
	id     uint32
	code   ErrCode
	reason string
}

func (e streamError) Error() string {
	return fmt.Sprintf("h2: stream %d error %s: %s", e.id, e.code, e.reason)
}

var (
	// errStreamReset is returned to a handler whose stream was reset by the client or the server.
	errStreamReset = errors.New("qilat: HTTP/2 stream reset")
	// errConnClosed is returned to a handler whose connection has gone away.
	errConnClosed = errors.New("qilat: HTTP/2 connection closed")
	// errStreamStalled is returned to a handler whose client stopped sending the body or accepting the response.
	errStreamStalled = fmt.Errorf("qilat: HTTP/2 stream stalled: %w", os.ErrDeadlineExceeded)
)

// frameHeader is the fixed 9-byte header that starts every frame.
type frameHeader struct {
	length   uint32
	typ      frameType
	flags    uint8
	streamID uint32
}

func (h frameHeader) has(f uint8) bool { return h.flags&f != 0 }

func appendFrameHeader(dst []byte, length int, typ frameType, flags uint8, streamID uint32) []byte {
	return append(dst, byte(length>>16), byte(length>>8), byte(length), byte(typ), flags,
		byte(streamID>>24)&0x7f, byte(streamID>>16), byte(streamID>>8), byte(streamID))
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func appendBE32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

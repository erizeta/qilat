package h2

import (
	"io"
)

// framer reads frames into a reusable buffer. The payload it returns is valid until the next readFrame.
type framer struct {
	r       io.Reader
	maxRead uint32 // our SETTINGS_MAX_FRAME_SIZE
	hdr     [frameHeaderLen]byte
	buf     []byte
}

func (fr *framer) readFrame() (frameHeader, []byte, error) {
	if _, err := io.ReadFull(fr.r, fr.hdr[:]); err != nil {
		return frameHeader{}, nil, err
	}
	h := frameHeader{
		length:   uint32(fr.hdr[0])<<16 | uint32(fr.hdr[1])<<8 | uint32(fr.hdr[2]),
		typ:      frameType(fr.hdr[3]),
		flags:    fr.hdr[4],
		streamID: be32(fr.hdr[5:]) & maxStreamID,
	}
	if h.length > fr.maxRead {
		return h, nil, connError{ErrCodeFrameSize, "frame larger than SETTINGS_MAX_FRAME_SIZE"}
	}
	if int(h.length) > cap(fr.buf) {
		fr.buf = make([]byte, h.length, max(h.length, minMaxFrameSize))
	}
	p := fr.buf[:h.length]
	if _, err := io.ReadFull(fr.r, p); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return h, nil, err
	}
	return h, p, nil
}

// stripPadding removes the Pad Length field and padding of a PADDED frame, reporting the padding size
// (including the length byte) so it can be credited back to flow control.
func stripPadding(h frameHeader, p []byte) ([]byte, uint32, error) {
	if !h.has(flagPadded) {
		return p, 0, nil
	}
	if len(p) == 0 {
		return nil, 0, connError{ErrCodeFrameSize, "padded frame without Pad Length"}
	}
	pad := int(p[0])
	if pad >= len(p) {
		return nil, 0, connError{ErrCodeProtocol, "padding longer than frame payload"}
	}
	return p[1 : len(p)-pad], uint32(pad) + 1, nil
}

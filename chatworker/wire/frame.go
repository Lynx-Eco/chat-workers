// SPDX-License-Identifier: AGPL-3.0-or-later

// Package wire is the channel contract's framing and the worker's end of the
// connection.
//
// A frame is the 12-byte v2 header (magic u32 BE, opcode u16 BE, codec u8,
// compression u8, payload length u32 BE) followed by one protobuf-encoded
// HostFrame (opcode 0x0100) or WorkerFrame (opcode 0x0101). Channel frames are
// always protobuf (codec 2) and uncompressed (compression 0), and at most
// MaxFrameBytes long. This mirrors crates/ingest_protocol/src/v2/channel.rs.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"lynx.invalid/ingest/chatworker/contract"
)

const (
	// Magic is the v2 frame magic, "IPGR".
	Magic uint32 = 0x49504752
	// HostOpcode marks a frame from host to worker.
	HostOpcode uint16 = 0x0100
	// WorkerOpcode marks a frame from worker to host.
	WorkerOpcode uint16 = 0x0101
	// CodecProtobuf is the v2 codec byte for protobuf (codec.rs: Protobuf = 2).
	CodecProtobuf byte = 2
	// CompressionNone is the v2 compression byte for no compression.
	CompressionNone byte = 0
	// HeaderLen is the length of the v2 header.
	HeaderLen = 12
	// MaxFrameBytes is the largest payload in either direction.
	MaxFrameBytes = 4 * 1024 * 1024
)

// Kind classifies a wire error.
type Kind int

const (
	KindIO Kind = iota + 1
	KindTruncated
	KindBadMagic
	KindUnexpectedOpcode
	KindUnsupportedCodec
	KindUnsupportedCompression
	KindFrameTooLarge
	KindDecode
	KindEncode
	KindHandshake
	KindClosedDuringHandshake
	KindUnsupportedContractVersion
	KindHandshakeTimeout
	KindClosed
)

var kindNames = map[Kind]string{
	KindIO:                         "io",
	KindTruncated:                  "truncated",
	KindBadMagic:                   "bad_magic",
	KindUnexpectedOpcode:           "unexpected_opcode",
	KindUnsupportedCodec:           "unsupported_codec",
	KindUnsupportedCompression:     "unsupported_compression",
	KindFrameTooLarge:              "frame_too_large",
	KindDecode:                     "decode",
	KindEncode:                     "encode",
	KindHandshake:                  "handshake",
	KindClosedDuringHandshake:      "closed_during_handshake",
	KindUnsupportedContractVersion: "unsupported_contract_version",
	KindHandshakeTimeout:           "handshake_timeout",
	KindClosed:                     "closed",
}

// String is a fixed code, safe to log.
func (k Kind) String() string {
	if name, ok := kindNames[k]; ok {
		return name
	}
	return "unknown"
}

// Error is every error this package returns. Its message names sizes,
// opcodes and causes only, never frame content.
type Error struct {
	Kind     Kind
	Found    uint64
	Expected uint64
	Err      error
}

func (e *Error) Error() string {
	switch e.Kind {
	case KindTruncated:
		return fmt.Sprintf("channel peer closed mid-frame (%d of %d bytes received)", e.Found, e.Expected)
	case KindBadMagic:
		return fmt.Sprintf("invalid frame magic %#010x", e.Found)
	case KindUnexpectedOpcode:
		return fmt.Sprintf("unexpected opcode %#06x on a channel connection (expected %#06x)", e.Found, e.Expected)
	case KindUnsupportedCodec:
		return fmt.Sprintf("unsupported codec %d on a channel connection", e.Found)
	case KindUnsupportedCompression:
		return fmt.Sprintf("unsupported compression %d on a channel connection", e.Found)
	case KindFrameTooLarge:
		return fmt.Sprintf("channel frame of %d bytes exceeds the %d byte cap", e.Found, e.Expected)
	case KindUnsupportedContractVersion:
		return fmt.Sprintf("channel contract version %d is not supported", e.Found)
	}
	if e.Err != nil {
		return "channel " + e.Kind.String() + ": " + e.Err.Error()
	}
	return "channel " + e.Kind.String()
}

func (e *Error) Unwrap() error { return e.Err }

// Is matches another *Error of the same Kind, so errors.Is(err, ErrClosed)
// works for every closed error.
func (e *Error) Is(target error) bool {
	var t *Error
	if errors.As(target, &t) {
		return t.Kind == e.Kind
	}
	return false
}

// Sentinels for errors.Is.
var (
	ErrClosed        = &Error{Kind: KindClosed}
	ErrFrameTooLarge = &Error{Kind: KindFrameTooLarge}
)

// KindOf returns the Kind of err, or 0 when it is not a wire error.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return 0
}

func header(opcode uint16, length int) []byte {
	h := make([]byte, HeaderLen, HeaderLen+length)
	binary.BigEndian.PutUint32(h[0:4], Magic)
	binary.BigEndian.PutUint16(h[4:6], opcode)
	h[6] = CodecProtobuf
	h[7] = CompressionNone
	binary.BigEndian.PutUint32(h[8:12], uint32(length))
	return h
}

// EncodeFrame returns one complete frame: header and deterministic payload.
func EncodeFrame(opcode uint16, m proto.Message) ([]byte, error) {
	payload, err := contract.Marshal(m)
	if err != nil {
		return nil, &Error{Kind: KindEncode, Err: err}
	}
	return framePayload(opcode, payload)
}

func framePayload(opcode uint16, payload []byte) ([]byte, error) {
	if len(payload) > MaxFrameBytes {
		return nil, &Error{Kind: KindFrameTooLarge, Found: uint64(len(payload)), Expected: MaxFrameBytes}
	}
	return append(header(opcode, len(payload)), payload...), nil
}

// EncodeQueuedEvent frames WorkerFrame{event: QueuedEvent{seq, event}} where
// event is an already deterministic encoding of a ChatEvent, without decoding
// and re-encoding it. The bytes equal EncodeFrame of the same value.
func EncodeQueuedEvent(seq uint64, event []byte) ([]byte, error) {
	var queued []byte
	if seq != 0 {
		queued = protowire.AppendTag(queued, 1, protowire.VarintType)
		queued = protowire.AppendVarint(queued, seq)
	}
	queued = protowire.AppendTag(queued, 2, protowire.BytesType)
	queued = protowire.AppendBytes(queued, event)
	var payload []byte
	payload = protowire.AppendTag(payload, 4, protowire.BytesType)
	payload = protowire.AppendBytes(payload, queued)
	return framePayload(WorkerOpcode, payload)
}

// ReadFrame reads one frame with the given opcode into m. It returns io.EOF
// when the peer closed cleanly between frames. A header announcing more than
// MaxFrameBytes fails before anything of that size is allocated.
//
// The payload is read into a buffer of ReadFrame's own, which is zeroed
// before it returns, decoded or not: m holds copies of what it decoded
// (protobuf-go copies bytes and string fields), so the frame's bytes, Init's
// store key among them, do not stay behind in a buffer nobody owns. What
// the operating system held of them (the socket's buffers) is outside Go.
func ReadFrame(r io.Reader, opcode uint16, m proto.Message) error {
	var h [HeaderLen]byte
	n, err := io.ReadFull(r, h[:])
	switch {
	case n == 0 && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)):
		return io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF):
		return &Error{Kind: KindTruncated, Found: uint64(n), Expected: HeaderLen}
	case err != nil:
		return &Error{Kind: KindIO, Err: err}
	}
	length, err := ParseHeader(h, opcode)
	if err != nil {
		return err
	}
	payload := make([]byte, length)
	defer clear(payload)
	n, err = io.ReadFull(r, payload)
	switch {
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		return &Error{Kind: KindTruncated, Found: uint64(HeaderLen + n), Expected: uint64(HeaderLen + length)}
	case err != nil:
		return &Error{Kind: KindIO, Err: err}
	}
	if err := contract.Unmarshal(payload, m); err != nil {
		// What was decoded before the error is in m: zeroed too.
		zeroBytes(m.ProtoReflect())
		return &Error{Kind: KindDecode, Err: err}
	}
	return nil
}

// zeroBytes zeroes every bytes field of m and of the messages in it (Init's
// store key among them). Strings cannot be zeroed in Go.
func zeroBytes(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				zeroValue(fd, l.Get(i))
			}
		case fd.IsMap():
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				zeroValue(fd.MapValue(), mv)
				return true
			})
		default:
			zeroValue(fd, v)
		}
		return true
	})
}

func zeroValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) {
	switch fd.Kind() {
	case protoreflect.BytesKind:
		clear(v.Bytes())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		zeroBytes(v.Message())
	}
}

// ParseHeader checks a header against the expected opcode and returns the
// payload length.
func ParseHeader(h [HeaderLen]byte, opcode uint16) (int, error) {
	if magic := binary.BigEndian.Uint32(h[0:4]); magic != Magic {
		return 0, &Error{Kind: KindBadMagic, Found: uint64(magic)}
	}
	if op := binary.BigEndian.Uint16(h[4:6]); op != opcode {
		return 0, &Error{Kind: KindUnexpectedOpcode, Found: uint64(op), Expected: uint64(opcode)}
	}
	if h[6] != CodecProtobuf {
		return 0, &Error{Kind: KindUnsupportedCodec, Found: uint64(h[6])}
	}
	if h[7] != CompressionNone {
		return 0, &Error{Kind: KindUnsupportedCompression, Found: uint64(h[7])}
	}
	length := binary.BigEndian.Uint32(h[8:12])
	if length > MaxFrameBytes {
		return 0, &Error{Kind: KindFrameTooLarge, Found: uint64(length), Expected: MaxFrameBytes}
	}
	return int(length), nil
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package wire_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/hosttest"
	"lynx.invalid/ingest/chatworker/wire"
)

const framedDir = "../../../crates/ingest_protocol/testdata/golden-vectors/channel_v1"
const payloadDir = "../../../crates/ingest_models/tests/vectors/channel_v1"

func readHex(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skipf("vector %s not found (go/ checked out without the rest of the repository)", path)
	}
	if err != nil {
		t.Fatal(err)
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestFramedGoldenVectors reads each framed vector, checks that its payload is
// the matching payload vector, and re-frames it to the same bytes.
func TestFramedGoldenVectors(t *testing.T) {
	cases := []struct {
		framed, payload string
		host            bool
	}{
		{"framed_host_init", "host_init", true},
		{"framed_host_ping", "host_ping", true},
		{"framed_worker_hello", "worker_hello", false},
		{"framed_worker_reply_sent", "reply_sent", false},
	}
	for _, c := range cases {
		t.Run(c.framed, func(t *testing.T) {
			framed := readHex(t, filepath.Join(framedDir, c.framed+".hex"))
			payload := readHex(t, filepath.Join(payloadDir, c.payload+".hex"))
			if !bytes.Equal(framed[wire.HeaderLen:], payload) {
				t.Fatalf("framed payload differs from the payload vector")
			}
			var msg proto.Message
			op := wire.WorkerOpcode
			if c.host {
				msg, op = &contract.HostFrame{}, wire.HostOpcode
			} else {
				msg = &contract.WorkerFrame{}
			}
			r := bytes.NewReader(framed)
			if err := wire.ReadFrame(r, op, msg); err != nil {
				t.Fatalf("read: %v", err)
			}
			if r.Len() != 0 {
				t.Fatalf("trailing bytes")
			}
			again, err := wire.EncodeFrame(op, msg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(again, framed) {
				t.Fatalf("re-framed bytes differ\n got %x\nwant %x", again, framed)
			}
			if err := wire.ReadFrame(r, op, msg); !errors.Is(err, io.EOF) {
				t.Fatalf("clean end: got %v, want io.EOF", err)
			}
		})
	}
}

func TestEncodeQueuedEventMatchesEncodeFrame(t *testing.T) {
	ev := &contract.ChatEvent{EventId: "msg:c:m:", EmittedAtMs: 5, Kind: &contract.ChatEvent_ChatRemoved{ChatRemoved: &contract.ChatRemoved{ChatId: "c"}}}
	evb, err := contract.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	got, err := wire.EncodeQueuedEvent(42, evb)
	if err != nil {
		t.Fatal(err)
	}
	want, err := wire.EncodeFrame(wire.WorkerOpcode, &contract.WorkerFrame{Body: &contract.WorkerFrame_Event{Event: &contract.QueuedEvent{Seq: 42, Event: ev}}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("hand-built frame differs\n got %x\nwant %x", got, want)
	}
}

func TestHeaderErrorsAreTyped(t *testing.T) {
	good, err := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Ping{Ping: &contract.Ping{Nonce: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f func(b []byte)) []byte {
		b := append([]byte(nil), good...)
		f(b)
		return b
	}
	cases := map[string]struct {
		in   []byte
		kind wire.Kind
	}{
		"bad magic":   {mutate(func(b []byte) { b[0] = 0 }), wire.KindBadMagic},
		"run opcode":  {mutate(func(b []byte) { binary.BigEndian.PutUint16(b[4:6], 1) }), wire.KindUnexpectedOpcode},
		"json codec":  {mutate(func(b []byte) { b[6] = 0 }), wire.KindUnsupportedCodec},
		"zstd":        {mutate(func(b []byte) { b[7] = 1 }), wire.KindUnsupportedCompression},
		"over cap":    {mutate(func(b []byte) { binary.BigEndian.PutUint32(b[8:12], wire.MaxFrameBytes+1) }), wire.KindFrameTooLarge},
		"short head":  {good[:5], wire.KindTruncated},
		"short body":  {good[:len(good)-1], wire.KindTruncated},
		"bad payload": {append(append([]byte(nil), good[:8]...), 0, 0, 0, 2, 0xff, 0xff), wire.KindDecode},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var f contract.HostFrame
			err := wire.ReadFrame(bytes.NewReader(c.in), wire.HostOpcode, &f)
			if got := wire.KindOf(err); got != c.kind {
				t.Fatalf("kind %v (%v), want %v", got, err, c.kind)
			}
		})
	}
	big := &contract.WorkerFrame{Body: &contract.WorkerFrame_Reply{Reply: &contract.CommandReply{Code: strings.Repeat("x", wire.MaxFrameBytes)}}}
	if _, err := wire.EncodeFrame(wire.WorkerOpcode, big); !errors.Is(err, wire.ErrFrameTooLarge) {
		t.Fatalf("oversized encode: %v", err)
	}
}

// runEcho is a minimal worker on the endpoint: it answers commands with an
// OK reply and Shutdown with ShutdownAck.
func runEcho(t *testing.T, ctx context.Context, path string, done chan<- error) {
	init, hs, err := wire.Dial(ctx, path, wire.Config{HandshakeTimeout: 5 * time.Second})
	if err != nil {
		done <- err
		return
	}
	if init.GetConnectionId() != "conn-1" {
		done <- errors.New("init not as sent")
		return
	}
	ep, err := hs.Hello(&contract.Hello{ContractVersion: contract.Version, WorkerName: "echo", LastUnackedSeq: 7})
	if err != nil {
		done <- err
		return
	}
	for {
		select {
		case <-ctx.Done():
			done <- ep.Close(time.Second)
			return
		case f, ok := <-ep.Inbound():
			if !ok {
				done <- ep.ReadErr()
				return
			}
			switch b := f.Body.(type) {
			case *contract.HostFrame_Command:
				_ = ep.SendReply(ctx, &contract.CommandReply{CommandId: b.Command.GetCommandId(), Outcome: contract.Outcome_OUTCOME_OK})
				evb, _ := contract.Marshal(&contract.ChatEvent{EventId: "e:" + b.Command.GetCommandId()})
				fr, _ := wire.EncodeQueuedEvent(8, evb)
				_ = ep.SendEvent(ctx, fr)
			case *contract.HostFrame_Shutdown:
				_ = ep.SendControl(ctx, &contract.WorkerFrame{Body: &contract.WorkerFrame_ShutdownAck{ShutdownAck: &contract.ShutdownAck{}}})
				done <- ep.Close(time.Second)
				return
			}
		}
	}
}

func TestHandshakePingCommandShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go runEcho(t, ctx, ln.Path, done)
	s, hello, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version, ConnectionId: "conn-1", StoreKey: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	if hello.GetWorkerName() != "echo" || hello.GetLastUnackedSeq() != 7 {
		t.Fatalf("hello: %v", hello)
	}
	for nonce := uint64(1); nonce <= 3; nonce++ {
		if err := s.Ping(ctx, nonce); err != nil {
			t.Fatalf("ping %d: %v", nonce, err)
		}
	}
	reply, err := s.Command(ctx, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if err != nil || reply.GetOutcome() != contract.Outcome_OUTCOME_OK {
		t.Fatalf("command: %v %v", reply, err)
	}
	if _, err := s.WaitEvent(ctx, func(e *contract.QueuedEvent) bool { return e.GetSeq() == 8 }); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(ctx, time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("worker side: %v", err)
	}
}

func TestHandshakeRefusesOtherContractVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	errc := make(chan error, 1)
	go func() {
		_, _, err := wire.Dial(ctx, ln.Path, wire.Config{})
		errc <- err
	}()
	_, _, _ = ln.Accept(ctx, &contract.Init{ContractVersion: 99})
	if err := <-errc; wire.KindOf(err) != wire.KindUnsupportedContractVersion {
		t.Fatalf("got %v", err)
	}
}

func TestHandshakeTimeoutAndWrongFirstFrame(t *testing.T) {
	dir := hosttest.SocketDir(t)
	path := filepath.Join(dir, "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		// Says nothing: the worker times out.
		time.Sleep(time.Second)
		c.Close()
	}()
	_, _, err = wire.Dial(context.Background(), path, wire.Config{HandshakeTimeout: 200 * time.Millisecond})
	if wire.KindOf(err) != wire.KindHandshakeTimeout {
		t.Fatalf("got %v, want handshake timeout", err)
	}
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		b, _ := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Ping{Ping: &contract.Ping{}}})
		_, _ = c.Write(b)
		time.Sleep(200 * time.Millisecond)
		c.Close()
	}()
	_, _, err = wire.Dial(context.Background(), path, wire.Config{HandshakeTimeout: 2 * time.Second})
	if wire.KindOf(err) != wire.KindHandshake {
		t.Fatalf("got %v, want handshake error", err)
	}
}

// TestOversizedHostFrameEndsReader: a frame header over the cap ends the
// connection with a typed error and nothing of that size is allocated.
func TestOversizedHostFrameEndsReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	epc := make(chan *wire.Endpoint, 1)
	go func() {
		_, hs, err := wire.Dial(ctx, ln.Path, wire.Config{})
		if err != nil {
			epc <- nil
			return
		}
		ep, _ := hs.Hello(&contract.Hello{ContractVersion: contract.Version})
		epc <- ep
	}()
	s, _, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version})
	if err != nil {
		t.Fatal(err)
	}
	ep := <-epc
	if ep == nil {
		t.Fatal("dial failed")
	}
	var h [wire.HeaderLen]byte
	binary.BigEndian.PutUint32(h[0:4], wire.Magic)
	binary.BigEndian.PutUint16(h[4:6], wire.HostOpcode)
	h[6] = wire.CodecProtobuf
	binary.BigEndian.PutUint32(h[8:12], 1<<31)
	if err := writeRaw(s, h[:]); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-ep.Inbound():
		if ok {
			t.Fatal("frame delivered")
		}
	case <-ctx.Done():
		t.Fatal("reader did not stop")
	}
	if !errors.Is(ep.ReadErr(), wire.ErrFrameTooLarge) {
		t.Fatalf("read error %v", ep.ReadErr())
	}
	_ = ep.Close(time.Second)
}

// writeRaw sends raw bytes through the session's socket (for malformed input).
func writeRaw(s *hosttest.Session, b []byte) error { return s.SendRaw(b) }

// TestCloseFlushesQueuedFrames: frames queued before Close are written.
func TestCloseFlushesQueuedFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ln, err := hosttest.Listen(hosttest.SocketDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		_, hs, err := wire.Dial(ctx, ln.Path, wire.Config{QueueDepth: 256})
		if err != nil {
			return
		}
		ep, err := hs.Hello(&contract.Hello{ContractVersion: contract.Version})
		if err != nil {
			return
		}
		for i := uint64(1); i <= 100; i++ {
			evb, _ := contract.Marshal(&contract.ChatEvent{EventId: "e"})
			fr, _ := wire.EncodeQueuedEvent(i, evb)
			_ = ep.SendEvent(ctx, fr)
		}
		_ = ep.Close(2 * time.Second)
	}()
	s, _, err := ln.Accept(ctx, &contract.Init{ContractVersion: contract.Version})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Closed():
	case <-ctx.Done():
		t.Fatal("not closed")
	}
	evs := s.Events()
	if len(evs) != 100 {
		t.Fatalf("got %d events, want 100", len(evs))
	}
	for i, e := range evs {
		if e.GetSeq() != uint64(i+1) {
			t.Fatalf("event %d has seq %d: out of order", i, e.GetSeq())
		}
	}
}

// TestContextCancelStopsDial: a cancelled context ends a pending handshake.
func TestContextCancelStopsDial(t *testing.T) {
	dir := hosttest.SocketDir(t)
	path := filepath.Join(dir, "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err == nil {
			time.Sleep(2 * time.Second)
			c.Close()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	start := time.Now()
	_, _, err = wire.Dial(ctx, path, wire.Config{HandshakeTimeout: 10 * time.Second})
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("dial not cut short: %v after %v", err, time.Since(start))
	}
	// Ended by the caller: not reported as the handshake's own time limit.
	if !errors.Is(err, context.Canceled) || wire.KindOf(err) == wire.KindHandshakeTimeout {
		t.Fatalf("got %v (kind %v), want context.Canceled", err, wire.KindOf(err))
	}
}

// TestCancelAfterHandshakeLeavesNoDeadline: the caller's ctx ends after Init
// was read. The connection must not be left with a read deadline in the past.
func TestCancelAfterHandshakeLeavesNoDeadline(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		frame, _ := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Init{Init: &contract.Init{ContractVersion: contract.Version}}})
		_, _ = b.Write(frame)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	if _, _, err := wire.Connect(ctx, a, wire.Config{HandshakeTimeout: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(50 * time.Millisecond)
	go func() { time.Sleep(100 * time.Millisecond); _, _ = b.Write([]byte{1}) }()
	buf := make([]byte, 1)
	if _, err := a.Read(buf); err != nil {
		t.Fatalf("read after a late cancel: %v", err)
	}
}

// keepingReader hands out its bytes like a socket and keeps every buffer it
// was asked to fill, so the test can look into ReadFrame's own buffers after
// it returned.
type keepingReader struct {
	r    io.Reader
	bufs [][]byte
}

func (k *keepingReader) Read(p []byte) (int, error) {
	n, err := k.r.Read(p)
	k.bufs = append(k.bufs, p[:n])
	return n, err
}

// TestReadFrameZeroesPayload: the buffer ReadFrame reads Init into is zeroed
// when it returns, so the store key's only copy left by the read is the
// decoded Init's (which Run zeroes after it took its own); also when the
// payload does not decode. The decoded key is a copy, not the zeroed buffer.
func TestReadFrameZeroesPayload(t *testing.T) {
	key := bytes.Repeat([]byte{0xa5}, 32)
	frame, err := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Init{Init: &contract.Init{
		ContractVersion: contract.Version, StoreKey: key}}})
	if err != nil {
		t.Fatal(err)
	}
	// A payload that does not decode (field 1's length runs on into the
	// key), behind a valid header.
	bad := append([]byte{0x0a, 0xff, 0xff}, key...)
	broken := binary.BigEndian.AppendUint32(nil, wire.Magic)
	broken = binary.BigEndian.AppendUint16(broken, wire.HostOpcode)
	broken = append(broken, wire.CodecProtobuf, wire.CompressionNone)
	broken = binary.BigEndian.AppendUint32(broken, uint32(len(bad)))
	broken = append(broken, bad...)
	for _, c := range []struct {
		name    string
		frame   []byte
		decodes bool
	}{{"init", frame, true}, {"undecodable", broken, false}} {
		kr := &keepingReader{r: bytes.NewReader(c.frame)}
		var got contract.HostFrame
		err := wire.ReadFrame(kr, wire.HostOpcode, &got)
		if c.decodes != (err == nil) {
			t.Fatalf("%s: %v", c.name, err)
		}
		seen := 0
		for _, b := range kr.bufs {
			seen += len(b)
			if bytes.Contains(b, key[:8]) {
				t.Fatalf("%s: the key is still in the buffer ReadFrame read the payload into", c.name)
			}
		}
		if seen != len(c.frame) {
			t.Fatalf("%s: read %d bytes, want %d", c.name, seen, len(c.frame))
		}
		if c.decodes && !bytes.Equal(got.GetInit().GetStoreKey(), key) {
			t.Fatalf("%s: decoded key %x, want a copy of %x", c.name, got.GetInit().GetStoreKey(), key)
		}
	}
}

// TestReadFrameZeroesHalfDecoded: an Init followed by bytes that do not
// decode fails as KindDecode, and the key the decoder had already put in the
// message before the error is zeroed: Connect, which drops the message on
// that error, leaves no live copy of it.
func TestReadFrameZeroesHalfDecoded(t *testing.T) {
	key := bytes.Repeat([]byte{0xa5}, 32)
	good, err := contract.Marshal(&contract.HostFrame{Body: &contract.HostFrame_Init{Init: &contract.Init{
		ContractVersion: contract.Version, StoreKey: key}}})
	if err != nil {
		t.Fatal(err)
	}
	payload := append(good, 0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)
	frame := binary.BigEndian.AppendUint32(nil, wire.Magic)
	frame = binary.BigEndian.AppendUint16(frame, wire.HostOpcode)
	frame = append(frame, wire.CodecProtobuf, wire.CompressionNone)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	var got contract.HostFrame
	err = wire.ReadFrame(bytes.NewReader(frame), wire.HostOpcode, &got)
	if wire.KindOf(err) != wire.KindDecode {
		t.Fatalf("got %v, want a decode error", err)
	}
	k := got.GetInit().GetStoreKey()
	if len(k) == 0 {
		t.Fatal("the decoder kept nothing of the Init: the case this guards is not exercised")
	}
	if !bytes.Equal(k, make([]byte, len(k))) {
		t.Fatalf("the half-decoded Init still holds the key %x", k)
	}
}

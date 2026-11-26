// SPDX-License-Identifier: AGPL-3.0-or-later

// Package hosttest is an in-process fake of the engine's side of the channel
// contract, for tests: it listens on a Unix socket, sends Init, reads Hello,
// records every event, answers with acks and sends commands. It is test
// support only and is never linked into a worker binary.
package hosttest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/wire"
)

// Listener is the host's socket.
type Listener struct {
	Path string
	ln   *net.UnixListener
}

// SocketDir makes a short directory for a test's socket and removes it when
// the test ends. t.TempDir() is not used for sockets: under macOS's default
// TMPDIR its path is longer than a Unix socket address may be (104 bytes).
func SocketDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "cw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// Listen binds a fresh socket in dir. Socket paths are short-lived and
// limited in length, so dir should be short (a t.TempDir() is fine).
func Listen(dir string) (*Listener, error) {
	path := filepath.Join(dir, "h.sock")
	_ = os.Remove(path)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	return &Listener{Path: path, ln: ln}, nil
}

// Close stops listening.
func (l *Listener) Close() error { return l.ln.Close() }

// Accept waits for one worker, sends init, reads its Hello and starts
// recording.
func (l *Listener) Accept(ctx context.Context, init *contract.Init) (*Session, *contract.Hello, error) {
	type result struct {
		conn *net.UnixConn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		conn, err := l.ln.AcceptUnix()
		ch <- result{conn, err}
	}()
	var conn *net.UnixConn
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, nil, r.err
		}
		conn = r.conn
	case <-ctx.Done():
		_ = l.ln.SetDeadline(time.Now())
		r := <-ch
		if r.conn != nil {
			_ = r.conn.Close()
		}
		_ = l.ln.SetDeadline(time.Time{})
		return nil, nil, ctx.Err()
	}
	frame, err := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Init{Init: init}})
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if _, err := conn.Write(frame); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(d)
	}
	var first contract.WorkerFrame
	if err := wire.ReadFrame(conn, wire.WorkerOpcode, &first); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("reading hello: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	hello := first.GetHello()
	if hello == nil {
		_ = conn.Close()
		return nil, nil, errors.New("first worker frame is not hello")
	}
	s := &Session{
		conn:    conn,
		replies: map[string]chan *contract.CommandReply{},
		changed: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	s.AutoAck.Store(true)
	go s.read()
	return s, hello, nil
}

// Session is one connected worker as the host sees it.
type Session struct {
	conn *net.UnixConn
	wmu  sync.Mutex

	// AutoAck acknowledges every event as it arrives, just before it is
	// recorded (the engine acknowledges after storing; here nothing can
	// fail in between, and the ack must precede what a test sends once it
	// saw the event: read).
	AutoAck atomic.Bool

	mu           sync.Mutex
	events       []*contract.QueuedEvent
	replies      map[string]chan *contract.CommandReply
	orphans      []*contract.CommandReply
	pongs        []uint64
	shutdownAck  bool
	changed      chan struct{}
	pauseUntil   time.Time
	readErr      error
	closed       chan struct{}
	nextCommand  atomic.Uint64
	highestAcked atomic.Uint64
}

func (s *Session) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Session) read() {
	defer close(s.closed)
	for {
		s.mu.Lock()
		pause := time.Until(s.pauseUntil)
		s.mu.Unlock()
		if pause > 0 {
			time.Sleep(pause)
		}
		var f contract.WorkerFrame
		err := wire.ReadFrame(s.conn, wire.WorkerOpcode, &f)
		if err != nil {
			s.mu.Lock()
			if !errors.Is(err, io.EOF) {
				s.readErr = err
			}
			for _, ch := range s.replies {
				close(ch)
			}
			s.replies = map[string]chan *contract.CommandReply{}
			s.notify()
			s.mu.Unlock()
			return
		}
		// An event is acknowledged before it is recorded (and a waiter
		// sees it): whatever a test sends once it saw the event (a
		// Shutdown above all) is written after the ack, and the worker,
		// which reads its frames in order, deletes the event from its
		// outbox first. Acknowledged after, the ack could follow the
		// Shutdown, which the worker does not read past: the event stayed
		// in the outbox and came again, first, at the next start, where a
		// test waiting for an event like it took the old one
		// (TestChannelTurned).
		if ev, ok := f.Body.(*contract.WorkerFrame_Event); ok && s.AutoAck.Load() {
			_ = s.Ack(ev.Event.GetSeq())
		}
		s.mu.Lock()
		switch body := f.Body.(type) {
		case *contract.WorkerFrame_Event:
			s.events = append(s.events, body.Event)
		case *contract.WorkerFrame_Reply:
			id := body.Reply.GetCommandId()
			if ch, ok := s.replies[id]; ok {
				ch <- body.Reply
				delete(s.replies, id)
			} else {
				s.orphans = append(s.orphans, body.Reply)
			}
		case *contract.WorkerFrame_Pong:
			s.pongs = append(s.pongs, body.Pong.GetNonce())
		case *contract.WorkerFrame_ShutdownAck:
			s.shutdownAck = true
		}
		s.notify()
		s.mu.Unlock()
	}
}

// Send writes one host frame.
func (s *Session) Send(frame *contract.HostFrame) error {
	b, err := wire.EncodeFrame(wire.HostOpcode, frame)
	if err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, err = s.conn.Write(b)
	return err
}

// Ack acknowledges every event up to seq.
func (s *Session) Ack(seq uint64) error {
	for {
		cur := s.highestAcked.Load()
		if seq <= cur {
			break
		}
		if s.highestAcked.CompareAndSwap(cur, seq) {
			break
		}
	}
	return s.Send(&contract.HostFrame{Body: &contract.HostFrame_Ack{Ack: &contract.Ack{UpToSeq: seq}}})
}

// Command sends cmd (assigning a command id when it has none) and waits for
// its reply.
func (s *Session) Command(ctx context.Context, cmd *contract.Command) (*contract.CommandReply, error) {
	if cmd.CommandId == "" {
		cmd.CommandId = "cmd-" + strconv.FormatUint(s.nextCommand.Add(1), 10)
	}
	ch := make(chan *contract.CommandReply, 1)
	s.mu.Lock()
	for i, orphan := range s.orphans {
		if orphan.GetCommandId() == cmd.CommandId {
			s.orphans = append(s.orphans[:i], s.orphans[i+1:]...)
			s.mu.Unlock()
			return orphan, nil
		}
	}
	s.replies[cmd.CommandId] = ch
	s.mu.Unlock()
	if err := s.Send(&contract.HostFrame{Body: &contract.HostFrame_Command{Command: cmd}}); err != nil {
		return nil, err
	}
	select {
	case r, ok := <-ch:
		if !ok {
			return nil, errors.New("worker closed the connection before replying")
		}
		return r, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.replies, cmd.CommandId)
		s.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Ping sends a ping and waits for its pong.
func (s *Session) Ping(ctx context.Context, nonce uint64) error {
	if err := s.Send(&contract.HostFrame{Body: &contract.HostFrame_Ping{Ping: &contract.Ping{Nonce: nonce}}}); err != nil {
		return err
	}
	return s.waitFor(ctx, func() bool {
		for _, n := range s.pongs {
			if n == nonce {
				return true
			}
		}
		return false
	})
}

// Shutdown asks the worker to stop and waits for its ShutdownAck and for the
// connection to close.
func (s *Session) Shutdown(ctx context.Context, grace time.Duration) error {
	if err := s.Send(&contract.HostFrame{Body: &contract.HostFrame_Shutdown{Shutdown: &contract.Shutdown{GraceMs: uint32(grace.Milliseconds())}}}); err != nil {
		return err
	}
	if err := s.waitFor(ctx, func() bool { return s.shutdownAck }); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ShutdownAcked tells whether a ShutdownAck arrived.
func (s *Session) ShutdownAcked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownAck
}

// PauseReading stops reading from the worker for d (a slow host).
func (s *Session) PauseReading(d time.Duration) {
	s.mu.Lock()
	s.pauseUntil = time.Now().Add(d)
	s.mu.Unlock()
}

// Events is a copy of every event received so far, repeats included.
func (s *Session) Events() []*contract.QueuedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*contract.QueuedEvent(nil), s.events...)
}

// Orphans are replies nobody waited for.
func (s *Session) Orphans() []*contract.CommandReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*contract.CommandReply(nil), s.orphans...)
}

// WaitEvent waits until a received event (from the first one on) satisfies
// match, and returns it.
func (s *Session) WaitEvent(ctx context.Context, match func(*contract.QueuedEvent) bool) (*contract.QueuedEvent, error) {
	var found *contract.QueuedEvent
	err := s.waitFor(ctx, func() bool {
		for _, e := range s.events {
			if match(e) {
				found = e
				return true
			}
		}
		return false
	})
	return found, err
}

// WaitQuiet waits until no frame has arrived for quiet, or ctx ends.
func (s *Session) WaitQuiet(ctx context.Context, quiet time.Duration) {
	for {
		s.mu.Lock()
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
			continue
		case <-time.After(quiet):
			return
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		}
	}
}

// waitFor runs cond under the session lock whenever something arrives.
func (s *Session) waitFor(ctx context.Context, cond func() bool) error {
	for {
		s.mu.Lock()
		ok := cond()
		ch := s.changed
		s.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closed:
			s.mu.Lock()
			ok := cond()
			s.mu.Unlock()
			if ok {
				return nil
			}
			return errors.New("worker connection closed")
		}
	}
}

// Closed is closed when the worker's side of the connection ends.
func (s *Session) Closed() <-chan struct{} { return s.closed }

// ReadErr is the error that ended reading, nil after a clean close.
func (s *Session) ReadErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readErr
}

// Close drops the connection.
func (s *Session) Close() error { return s.conn.Close() }

// SendRaw writes raw bytes, for tests of malformed input.
func (s *Session) SendRaw(b []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, err := s.conn.Write(b)
	return err
}

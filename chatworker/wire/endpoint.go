// SPDX-License-Identifier: AGPL-3.0-or-later

package wire

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
)

// Config holds the tunables of one connection.
type Config struct {
	// QueueDepth is how many frames each outgoing queue and the inbound
	// queue hold before a sender (or the reader) waits.
	QueueDepth int
	// HandshakeTimeout bounds reading Init and writing Hello.
	HandshakeTimeout time.Duration
}

// DefaultConfig matches the Rust side's defaults.
func DefaultConfig() Config {
	return Config{QueueDepth: 64, HandshakeTimeout: 30 * time.Second}
}

func (c Config) normalized() Config {
	if c.QueueDepth <= 0 {
		c.QueueDepth = 64
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 30 * time.Second
	}
	return c
}

// Handshake is a connection that has read Init and owes the host its Hello.
// The two steps are apart because Hello needs what Init names (the state
// directory holds last_unacked_seq).
type Handshake struct {
	conn net.Conn
	cfg  Config
}

// Dial connects to the host's socket and reads Init.
func Dial(ctx context.Context, socketPath string, cfg Config) (*contract.Init, *Handshake, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, nil, &Error{Kind: KindIO, Err: err}
	}
	init, hs, err := Connect(ctx, conn, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return init, hs, nil
}

// Connect reads Init from an established connection.
func Connect(ctx context.Context, conn net.Conn, cfg Config) (*contract.Init, *Handshake, error) {
	return connect(ctx, conn, cfg, &contract.HostFrame{})
}

// connect is Connect reading the first frame into first, which a test in the
// package keeps to see what an error path left in the Init it dropped.
func connect(ctx context.Context, conn net.Conn, cfg Config, first *contract.HostFrame) (*contract.Init, *Handshake, error) {
	cfg = cfg.normalized()
	deadline := time.Now().Add(cfg.HandshakeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetReadDeadline(deadline)
	// A cancelled ctx ends the read by moving the deadline to now. Once the
	// read is over the deadline is cleared under the same lock, so a cancel
	// that comes later cannot leave a deadline on the connection.
	var mu sync.Mutex
	reading := true
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()
		if reading {
			_ = conn.SetReadDeadline(time.Now())
		}
	})
	err := ReadFrame(conn, HostOpcode, first)
	stop()
	mu.Lock()
	reading = false
	_ = conn.SetReadDeadline(time.Time{})
	mu.Unlock()
	if err != nil {
		// A frame that did not decode may have given Init its key before
		// the error (ReadFrame zeroes its bytes fields too).
		clear(first.GetInit().GetStoreKey())
	}
	if errors.Is(err, io.EOF) {
		return nil, nil, &Error{Kind: KindClosedDuringHandshake}
	}
	if err != nil {
		// Ended by the caller, not by the handshake's own time limit.
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(ctxErr, context.DeadlineExceeded) {
			return nil, nil, ctxErr
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil, nil, &Error{Kind: KindHandshakeTimeout, Err: err}
		}
		return nil, nil, err
	}
	init := first.GetInit()
	if init == nil {
		return nil, nil, &Error{Kind: KindHandshake, Err: errors.New("expected init")}
	}
	if init.GetContractVersion() != contract.Version {
		// The caller never sees this Init: its key is zeroed here.
		clear(init.GetStoreKey())
		return nil, nil, &Error{Kind: KindUnsupportedContractVersion, Found: uint64(init.GetContractVersion())}
	}
	return init, &Handshake{conn: conn, cfg: cfg}, nil
}

// Close abandons the handshake.
func (h *Handshake) Close() error { return h.conn.Close() }

// Hello sends Hello and starts the connection's reader and writer.
func (h *Handshake) Hello(hello *contract.Hello) (*Endpoint, error) {
	frame, err := EncodeFrame(WorkerOpcode, &contract.WorkerFrame{Body: &contract.WorkerFrame_Hello{Hello: hello}})
	if err != nil {
		return nil, err
	}
	_ = h.conn.SetWriteDeadline(time.Now().Add(h.cfg.HandshakeTimeout))
	_, err = h.conn.Write(frame)
	_ = h.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return nil, &Error{Kind: KindIO, Err: err}
	}
	return start(h.conn, h.cfg), nil
}

// Endpoint is the worker's end of an established connection. One writer
// goroutine owns the write side, so frames never interleave; one reader
// goroutine owns the read side and never waits on the writer. Pings are
// answered by the endpoint itself.
type Endpoint struct {
	conn    net.Conn
	pongs   chan []byte
	ctrl    chan []byte
	events  chan []byte
	inbound chan *contract.HostFrame

	closing    chan struct{}
	closeOnce  sync.Once
	writerDone chan struct{}
	readerDone chan struct{}

	errMu    sync.Mutex
	readErr  error
	writeErr error
}

func start(conn net.Conn, cfg Config) *Endpoint {
	e := &Endpoint{
		conn:       conn,
		pongs:      make(chan []byte, 16),
		ctrl:       make(chan []byte, cfg.QueueDepth),
		events:     make(chan []byte, cfg.QueueDepth),
		inbound:    make(chan *contract.HostFrame, cfg.QueueDepth),
		closing:    make(chan struct{}),
		writerDone: make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go e.writer()
	go e.reader()
	return e
}

// Inbound yields every host frame except pings, in order. It is closed when
// the reader stops; ReadErr then tells why (nil for a clean close).
func (e *Endpoint) Inbound() <-chan *contract.HostFrame { return e.inbound }

// ReadErr is the error that stopped the reader, nil after a clean close.
func (e *Endpoint) ReadErr() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()
	return e.readErr
}

// WriteErr is the error that stopped the writer, if any.
func (e *Endpoint) WriteErr() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()
	return e.writeErr
}

// WriterDone is closed when the writer has stopped.
func (e *Endpoint) WriterDone() <-chan struct{} { return e.writerDone }

// SendReply queues a command reply ahead of queued events.
func (e *Endpoint) SendReply(ctx context.Context, reply *contract.CommandReply) error {
	return e.SendControl(ctx, &contract.WorkerFrame{Body: &contract.WorkerFrame_Reply{Reply: reply}})
}

// SendControl queues a frame ahead of queued events (replies, ShutdownAck).
func (e *Endpoint) SendControl(ctx context.Context, frame *contract.WorkerFrame) error {
	b, err := EncodeFrame(WorkerOpcode, frame)
	if err != nil {
		return err
	}
	return e.enqueue(ctx, e.ctrl, b)
}

// SendOrdered queues a frame behind every event already queued (a
// ShutdownAck must follow the events it flushes).
func (e *Endpoint) SendOrdered(ctx context.Context, frame *contract.WorkerFrame) error {
	b, err := EncodeFrame(WorkerOpcode, frame)
	if err != nil {
		return err
	}
	return e.enqueue(ctx, e.events, b)
}

// SendEvent queues one already-framed QueuedEvent (see EncodeQueuedEvent).
// It waits while the event queue is full, until ctx ends.
func (e *Endpoint) SendEvent(ctx context.Context, framed []byte) error {
	return e.enqueue(ctx, e.events, framed)
}

func (e *Endpoint) enqueue(ctx context.Context, q chan []byte, b []byte) error {
	select {
	case <-e.closing:
		return ErrClosed
	case <-e.writerDone:
		return ErrClosed
	default:
	}
	select {
	case q <- b:
		// Both this case and a close may have been ready. The writer's last
		// drain after a close may or may not pick such a frame up, so it is
		// not reported as queued: ErrClosed here means "maybe written". A
		// caller must treat it as not sent (the emitter keeps the event in
		// the outbox, and a repeat is dropped by the host by seq). nil means
		// queued, not written: only the host's acknowledgement says an
		// event arrived.
		select {
		case <-e.closing:
			return ErrClosed
		case <-e.writerDone:
			return ErrClosed
		default:
			return nil
		}
	case <-e.closing:
		return ErrClosed
	case <-e.writerDone:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Endpoint) write(b []byte) bool {
	if _, err := e.conn.Write(b); err != nil {
		e.errMu.Lock()
		if e.writeErr == nil {
			e.writeErr = &Error{Kind: KindIO, Err: err}
		}
		e.errMu.Unlock()
		return false
	}
	return true
}

func (e *Endpoint) writer() {
	defer close(e.writerDone)
	for {
		// Pongs first, then replies, then events.
		select {
		case b := <-e.pongs:
			if !e.write(b) {
				return
			}
			continue
		default:
		}
		select {
		case b := <-e.ctrl:
			if !e.write(b) {
				return
			}
			continue
		default:
		}
		select {
		case b := <-e.pongs:
			if !e.write(b) {
				return
			}
		case b := <-e.ctrl:
			if !e.write(b) {
				return
			}
		case b := <-e.events:
			if !e.write(b) {
				return
			}
		case <-e.closing:
			// Write what was queued before the close, without waiting for more.
			for _, q := range []chan []byte{e.pongs, e.ctrl, e.events} {
				for {
					select {
					case b := <-q:
						if !e.write(b) {
							return
						}
						continue
					default:
					}
					break
				}
			}
			if uc, ok := e.conn.(*net.UnixConn); ok {
				_ = uc.CloseWrite()
			}
			return
		}
	}
}

func (e *Endpoint) reader() {
	defer close(e.readerDone)
	defer close(e.inbound)
	for {
		var frame contract.HostFrame
		err := ReadFrame(e.conn, HostOpcode, &frame)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				select {
				case <-e.closing:
					// Our own close ended the read.
				default:
					e.errMu.Lock()
					e.readErr = err
					e.errMu.Unlock()
				}
			}
			return
		}
		if frame.Body == nil {
			// Empty, or a kind this build does not know: skipped.
			continue
		}
		if ping := frame.GetPing(); ping != nil {
			pong, err := EncodeFrame(WorkerOpcode, &contract.WorkerFrame{
				Body: &contract.WorkerFrame_Pong{Pong: &contract.Pong{Nonce: ping.GetNonce()}},
			})
			if err == nil {
				select {
				case e.pongs <- pong:
				default:
					// The host is not reading; its ping times out, as it should.
				}
			}
			continue
		}
		select {
		case e.inbound <- &frame:
		case <-e.closing:
			return
		}
	}
}

// Close writes what is queued (waiting at most limit), closes the write
// side, and stops reading.
func (e *Endpoint) Close(limit time.Duration) error {
	e.closeOnce.Do(func() { close(e.closing) })
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case <-e.writerDone:
	case <-timer.C:
		_ = e.conn.SetWriteDeadline(time.Now())
		<-e.writerDone
	}
	_ = e.conn.SetReadDeadline(time.Now())
	<-e.readerDone
	err := e.conn.Close()
	if we := e.WriteErr(); we != nil {
		return we
	}
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return &Error{Kind: KindIO, Err: err}
	}
	return nil
}

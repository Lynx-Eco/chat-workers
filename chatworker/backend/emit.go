// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"sync"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
	"lynx.invalid/ingest/chatworker/wire"
)

// Emitter is the upward queue. Emit writes the event to lynx_outbox in one
// small statement and returns; a single pump goroutine hands queued events to
// the connection's writer strictly in seq order, waiting as long as the host
// is slow. A framework call that emits therefore never waits on the host,
// beyond a short, bounded pause when the backlog is very large.
//
// Events still in the outbox at start (not acknowledged before the last
// stop) are handed over first, in order, with their original seq.
type Emitter struct {
	st  *store.Store
	log logx.Logger

	// HighWater and MaxWait bound the only wait Emit ever does: when more
	// than HighWater events are queued and not yet handed to the writer, Emit
	// waits up to MaxWait for the backlog to shrink, then returns anyway.
	HighWater int
	MaxWait   time.Duration

	mu    sync.Mutex
	queue []store.QueuedEvent
	// gen counts Resets. The pump takes queue[0] and hands it over without
	// the lock (the handover waits as long as the host does not read); it
	// removes it afterwards only when no Reset came in between, since after
	// one queue[0] is another event (the ENDED a logout queued after its
	// wipe), which has not been sent.
	gen uint64
	// congested is set once a wait timed out: no further waits until the
	// backlog has halved, so a slow host costs the network one pause per
	// episode, not one per event.
	congested bool
	changed   chan struct{}
	now       func() time.Time
}

// NewEmitter starts with the events still pending from before.
func NewEmitter(st *store.Store, log logx.Logger, pending []store.QueuedEvent) *Emitter {
	return &Emitter{
		st:        st,
		log:       log,
		HighWater: 512,
		MaxWait:   250 * time.Millisecond,
		queue:     append([]store.QueuedEvent(nil), pending...),
		changed:   make(chan struct{}),
		now:       time.Now,
	}
}

func (e *Emitter) signal() {
	close(e.changed)
	e.changed = make(chan struct{})
}

// Emit stores one event and queues it for the host. It returns the seq, or 0
// when the event could not be stored (the failure is logged by code; the
// event is lost, as it would be if the worker had crashed before it).
func (e *Emitter) Emit(eventID string, kind isChatEventKind) uint64 {
	return e.EmitSince(e.st.Epoch(), eventID, kind)
}

// EmitSince is Emit for an event made before the store's epoch moved on
// from epoch (store.Epoch, read when the event's making began): after a
// wipe (a logout) it is neither stored nor queued, and 0 is returned. The
// wipe and the reset of the queue that follows it (Reset) are two steps; an
// event of the old login that waited on the outbox lock while the wipe ran
// would otherwise be stored in the emptied outbox and then dropped from the
// queue by the reset, never sent and left on disk.
func (e *Emitter) EmitSince(epoch uint64, eventID string, kind isChatEventKind) uint64 {
	ev := &contract.ChatEvent{EventId: eventID, EmittedAtMs: e.now().UnixMilli()}
	setKind(ev, kind)
	b, err := contract.Marshal(ev)
	if err != nil {
		e.log.Error("event_encode_failed")
		return 0
	}
	seq, err := e.st.AppendEventSince(context.Background(), epoch, b, func(seq uint64) {
		e.enqueue(store.QueuedEvent{Seq: seq, Event: b})
	})
	if errors.Is(err, store.ErrWiped) {
		return 0
	}
	if err != nil {
		e.log.Error("outbox_insert_failed")
		return 0
	}
	e.backpressure()
	return seq
}

// enqueue adds one stored event to the queue.
func (e *Emitter) enqueue(ev store.QueuedEvent) {
	e.mu.Lock()
	e.queue = append(e.queue, ev)
	e.signal()
	e.mu.Unlock()
}

func (e *Emitter) backpressure() {
	deadline := time.Now().Add(e.MaxWait)
	for {
		e.mu.Lock()
		n := len(e.queue)
		ch := e.changed
		congested := e.congested
		e.mu.Unlock()
		if n <= e.HighWater || congested {
			return
		}
		left := time.Until(deadline)
		if left <= 0 {
			e.mu.Lock()
			e.congested = true
			e.mu.Unlock()
			e.log.Warn("host_slow", logx.N("backlog", int64(n)))
			return
		}
		select {
		case <-ch:
		case <-time.After(left):
		}
	}
}

// Backlog is how many events are queued and not yet handed to the writer.
func (e *Emitter) Backlog() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.queue)
}

// Pump hands queued events to ep until ctx ends or the connection closes.
func (e *Emitter) Pump(ctx context.Context, ep *wire.Endpoint) {
	for {
		e.mu.Lock()
		if len(e.queue) == 0 {
			ch := e.changed
			e.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return
			}
		}
		next, gen := e.queue[0], e.gen
		e.mu.Unlock()
		framed, err := wire.EncodeQueuedEvent(next.Seq, next.Event)
		if err != nil {
			// Too large to frame: it can never be delivered. It stays in the
			// outbox; the host sees a gap in seq and the log says why.
			e.log.Error("event_too_large", logx.N("seq", int64(next.Seq)))
		} else if err := ep.SendEvent(ctx, framed); err != nil {
			return
		}
		e.mu.Lock()
		if e.gen == gen {
			e.queue = e.queue[1:]
		}
		if e.congested && len(e.queue) <= e.HighWater/2 {
			e.congested = false
		}
		e.signal()
		e.mu.Unlock()
	}
}

// Flush waits until every queued event has been handed to the writer, or
// until deadline. It reports whether the queue is empty.
func (e *Emitter) Flush(deadline time.Time) bool {
	for {
		e.mu.Lock()
		n := len(e.queue)
		ch := e.changed
		e.mu.Unlock()
		if n == 0 {
			return true
		}
		left := time.Until(deadline)
		if left <= 0 {
			return false
		}
		select {
		case <-ch:
		case <-time.After(left):
			return false
		}
	}
}

// Ack deletes acknowledged events from the outbox.
func (e *Emitter) Ack(ctx context.Context, upTo uint64) {
	if _, err := e.st.Ack(ctx, upTo); err != nil {
		e.log.Error("outbox_ack_failed")
	}
}

// Reset drops the in-memory queue (after the outbox was wiped). An event the
// pump is handing over at that moment still goes out (it was taken before);
// every event queued after Reset is handed over once the host reads again.
func (e *Emitter) Reset() {
	e.mu.Lock()
	e.queue = nil
	e.gen++
	e.signal()
	e.mu.Unlock()
}

// isChatEventKind is the generated oneof interface of ChatEvent.kind.
type isChatEventKind interface{}

func setKind(ev *contract.ChatEvent, kind isChatEventKind) {
	switch k := kind.(type) {
	case *contract.Message:
		ev.Kind = &contract.ChatEvent_Message{Message: k}
	case *contract.Edit:
		ev.Kind = &contract.ChatEvent_Edit{Edit: k}
	case *contract.Reaction:
		ev.Kind = &contract.ChatEvent_Reaction{Reaction: k}
	case *contract.MessageDeleted:
		ev.Kind = &contract.ChatEvent_MessageDeleted{MessageDeleted: k}
	case *contract.ReadUpTo:
		ev.Kind = &contract.ChatEvent_ReadUpTo{ReadUpTo: k}
	case *contract.Delivered:
		ev.Kind = &contract.ChatEvent_Delivered{Delivered: k}
	case *contract.ChatInfo:
		ev.Kind = &contract.ChatEvent_ChatInfo{ChatInfo: k}
	case *contract.ChatRemoved:
		ev.Kind = &contract.ChatEvent_ChatRemoved{ChatRemoved: k}
	case *contract.PersonInfo:
		ev.Kind = &contract.ChatEvent_PersonInfo{PersonInfo: k}
	case *contract.HistoryBatch:
		ev.Kind = &contract.ChatEvent_HistoryBatch{HistoryBatch: k}
	case *contract.ConnectionState:
		ev.Kind = &contract.ChatEvent_ConnectionState{ConnectionState: k}
	case *contract.ChatAbilities:
		ev.Kind = &contract.ChatEvent_ChatAbilities{ChatAbilities: k}
	case *contract.SignInStep:
		ev.Kind = &contract.ChatEvent_SignInStep{SignInStep: k}
	case *contract.SendResolved:
		ev.Kind = &contract.ChatEvent_SendResolved{SendResolved: k}
	}
}

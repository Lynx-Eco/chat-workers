// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
	"lynx.invalid/ingest/chatworker/wire"
)

// pipeEndpoint is a worker endpoint whose host end the test reads (or does
// not read) itself: net.Pipe has no buffer, so a host that does not read
// holds the writer at once.
func pipeEndpoint(t *testing.T) (*wire.Endpoint, net.Conn) {
	t.Helper()
	worker, host := net.Pipe()
	t.Cleanup(func() { _ = worker.Close(); _ = host.Close() })
	go func() {
		f, _ := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Init{Init: &contract.Init{ContractVersion: contract.Version}}})
		_, _ = host.Write(f)
		var hello contract.WorkerFrame
		_ = wire.ReadFrame(host, wire.WorkerOpcode, &hello)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, hs, err := wire.Connect(ctx, worker, wire.Config{QueueDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	ep, err := hs.Hello(&contract.Hello{ContractVersion: contract.Version})
	if err != nil {
		t.Fatal(err)
	}
	return ep, host
}

func testEvent(t *testing.T, seq uint64, code string) store.QueuedEvent {
	t.Helper()
	ev := &contract.ChatEvent{EventId: code}
	setKind(ev, &contract.ConnectionState{State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: code})
	b, err := contract.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return store.QueuedEvent{Seq: seq, Event: b}
}

// TestResetWhileHostNotReading: a logout wipes the outbox and resets the
// queue while the pump is handing an event to a host that does not read,
// then queues its ENDED. Once the host reads again it gets the event that
// was being handed over and then the ENDED: the pump does not take the
// ENDED, which was queued after the reset, for the event it had sent.
func TestResetWhileHostNotReading(t *testing.T) {
	ep, host := pipeEndpoint(t)
	log := logx.New(io.Discard, "c")
	e := NewEmitter(nil, log, []store.QueuedEvent{testEvent(t, 1, "a"), testEvent(t, 2, "b"), testEvent(t, 3, "c")})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Pump(ctx, ep)
	// The writer holds seq 1 (the host does not read), the endpoint's queue
	// seq 2, and the pump waits to hand over seq 3, which stays queue[0].
	deadline := time.Now().Add(10 * time.Second)
	for e.Backlog() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("backlog %d, want 1", e.Backlog())
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	// The logout: outbox wiped, queue reset, ENDED stored and queued.
	e.Reset()
	e.enqueue(testEvent(t, 4, "logged_out"))

	var got []uint64
	_ = host.SetReadDeadline(time.Now().Add(5 * time.Second))
	for len(got) < 4 {
		var f contract.WorkerFrame
		if err := wire.ReadFrame(host, wire.WorkerOpcode, &f); err != nil {
			t.Fatalf("after seqs %v: %v (the ENDED queued after the reset was not sent)", got, err)
		}
		if q := f.GetEvent(); q != nil {
			got = append(got, q.GetSeq())
		}
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 || got[3] != 4 {
		t.Fatalf("seqs %v, want [1 2 3 4]", got)
	}
	if !e.Flush(time.Now().Add(2 * time.Second)) {
		t.Fatalf("backlog %d after the host read everything", e.Backlog())
	}
}

// TestEmitBetweenWipeAndReset: a logout wipes the store and then resets the
// queue (resetAll), two steps. An event of the old login that got past the
// silence switch before the logout set it, and waited on the outbox lock
// while the wipe ran, gets the lock between the two: it must not be stored
// in the emptied outbox (where Reset would drop it from the queue, so that
// it is never sent and stays on disk after the wipe, to be replayed after
// the ENDED at the next start). Only the ENDED queued after the reset is in
// the outbox and reaches the host.
func TestEmitBetweenWipeAndReset(t *testing.T) {
	b, st := testBackend(t)
	ep, host := pipeEndpoint(t)
	e := NewEmitter(st, b.log, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Pump(ctx, ep)
	epoch := st.Epoch() // the old login's event begins (Backend.emit)
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	// It had waited on the outbox lock while Wipe ran, and gets it now.
	late := e.EmitSince(epoch, "late", &contract.ConnectionState{Code: "late"})
	e.Reset()
	end := e.Emit("ended", &contract.ConnectionState{State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: "logged_out"})
	if end == 0 {
		t.Fatal("the ENDED was not stored")
	}
	var got []uint64
	_ = host.SetReadDeadline(time.Now().Add(5 * time.Second))
	for len(got) == 0 || got[len(got)-1] != end {
		var f contract.WorkerFrame
		if err := wire.ReadFrame(host, wire.WorkerOpcode, &f); err != nil {
			t.Fatalf("after seqs %v: %v (the ENDED %d was not sent)", got, err, end)
		}
		if q := f.GetEvent(); q != nil {
			got = append(got, q.GetSeq())
		}
	}
	pending, err := st.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var onDisk []uint64
	for _, p := range pending {
		onDisk = append(onDisk, p.Seq)
	}
	if late != 0 || len(got) != 1 || len(onDisk) != 1 || onDisk[0] != end {
		t.Fatalf("late seq %d, ended seq %d, sent %v, in the outbox after the wipe %v: want late 0, sent and stored only the ENDED", late, end, got, onDisk)
	}
}

// TestEmitEpochBeforeSilence: Backend.emit reads the store's epoch before
// it checks the silence switch. An event of the old login checks the switch
// while a logout has not set it yet; the logout then silences, wipes (a new
// epoch), resets the queue and lifts the switch. Had emit read the epoch
// after its check, it would read the new epoch and store the old login's
// event in the emptied outbox, to be sent after the wipe. Read before, the
// epoch is the old one and the event is dropped (EmitSince).
func TestEmitEpochBeforeSilence(t *testing.T) {
	b, st := testBackend(t)
	ctx := t.Context()
	logouts := 0
	b.emitBetween = func() {
		logouts++
		b.silenced.Store(true)
		if err := st.Wipe(ctx); err != nil {
			t.Error(err)
		}
		b.out.Reset()
		b.silenced.Store(false)
	}
	b.emit("late", &contract.ConnectionState{Code: "late"})
	b.emitBetween = nil
	if logouts != 1 {
		t.Fatalf("the logout ran %d times inside emit, want 1", logouts)
	}
	pending, err := st.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 || b.out.Backlog() != 0 {
		t.Fatalf("after the wipe: %d events in the outbox, %d queued; want none (the old login's event was stored after the wipe)", len(pending), b.out.Backlog())
	}
	// The same call with no logout in between is stored: the drop above is
	// the epoch's doing, not emit's.
	b.emit("after", &contract.ConnectionState{Code: "after"})
	if pending, err = st.Pending(ctx); err != nil || len(pending) != 1 {
		t.Fatalf("an event after the logout: %d in the outbox (%v), want 1", len(pending), err)
	}
}

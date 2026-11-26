// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
)

func contentIDs(w *worker) []string { return contentIDsAfter(w, 0) }

// contentIDsAfter lists the content event ids with a seq above after
// (replays of unacknowledged events have their old seq).
func contentIDsAfter(w *worker, after uint64) []string {
	var ids []string
	for _, q := range w.s.Events() {
		if q.GetSeq() <= after {
			continue
		}
		id := q.GetEvent().GetEventId()
		for _, p := range []string{"msg:", "edit:", "react:", "del:", "read:", "delivered:", "chat:", "person:", "abilities:", "signin:"} {
			if strings.HasPrefix(id, p) {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	return ids
}

// generatedKinds begin the ids of the events the worker generates
// (backend genID): the store's nonce is their first component after the
// kind. So is the echo of an edit command (edit:<nonce>:...), which shares
// its kind with the network's edits and is not listed here. Every other id
// is the network's, or carries the host's command id.
var generatedKinds = []string{"self:", "state:", "chat:", "person:", "abilities:", "chat-removed:", "signin:"}

func generatedID(id string) bool {
	for _, p := range generatedKinds {
		if strings.HasPrefix(id, p) {
			return true
		}
	}
	return false
}

// nonceOf is the store nonce a generated id carries.
func nonceOf(id string) string {
	_, rest, _ := strings.Cut(id, ":")
	nonce, _, _ := strings.Cut(rest, ":")
	return nonce
}

// withoutNonce drops the store's nonce from a generated id.
func withoutNonce(id string) string {
	if !generatedID(id) {
		return id
	}
	kind, rest, _ := strings.Cut(id, ":")
	_, rest, _ = strings.Cut(rest, ":")
	return kind + ":" + rest
}

// acrossRuns makes content event ids comparable between two runs of the same
// script: the store's nonce is dropped from the ids the worker generates. A chat's info is reported whenever the stored room state differs
// from what was last reported ("chat:<chat>:<generation>"), and how many
// intermediate states are seen while a chat is being created depends on
// timing: for those ids the generation is dropped and the chat counted once.
// Every other id is made from the network's own ids and is compared as it
// is, repeats included.
func acrossRuns(ids []string) []string {
	chats := map[string]bool{}
	var out []string
	for _, id := range ids {
		id = withoutNonce(id)
		if strings.HasPrefix(id, "chat:") {
			id = id[:strings.LastIndex(id, ":")]
			if chats[id] {
				continue
			}
			chats[id] = true
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// TestStableEventIDs: the same script gives the same event ids (see
// acrossRuns for a chat's info), and a restart against the same store
// repeats none of them.
func TestStableEventIDs(t *testing.T) {
	a := start(t, newState(t), opts{})
	a.signIn()
	a.shutdown()
	b := start(t, newState(t), opts{})
	b.signIn()
	lastB := b.s.Events()[len(b.s.Events())-1].GetSeq()
	b.shutdown()
	ia, ib := acrossRuns(contentIDs(a)), acrossRuns(contentIDs(b))
	t.Logf("%d content ids compared", len(ia))
	if len(ia) < 30 || strings.Join(ia, "\n") != strings.Join(ib, "\n") {
		t.Fatalf("event ids differ between runs of the same script (%d vs %d)\n%v\n%v", len(ia), len(ib), ia, ib)
	}
	c := start(t, b.state, opts{})
	c.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	c.shutdown()
	if n := len(contentIDsAfter(c, lastB)); n != 0 {
		t.Fatalf("restart produced %d content events again: %v", n, contentIDsAfter(c, lastB))
	}
}

// TestOutboxReplayAfterKill: events queued and not acknowledged when the
// worker is killed are sent again once, with the same seq; nothing that was
// acknowledged comes again.
func TestOutboxReplayAfterKill(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{noAutoAck: true})
	w.signIn()
	before := w.s.Events()
	w.proc.Kill()
	if len(before) < 20 {
		t.Fatalf("only %d events", len(before))
	}
	maxSeq := before[len(before)-1].GetSeq()

	w2 := start(t, state, opts{})
	if w2.hello.GetLastUnackedSeq() != maxSeq {
		t.Fatalf("last_unacked_seq %d, want %d", w2.hello.GetLastUnackedSeq(), maxSeq)
	}
	w2.wait("replay of the last event", func(e *contract.ChatEvent) bool {
		return e.GetEventId() == before[len(before)-1].GetEvent().GetEventId()
	})
	seen := map[uint64]int{}
	for _, q := range w2.s.Events() {
		if q.GetSeq() <= maxSeq {
			seen[q.GetSeq()]++
		}
	}
	for _, q := range before {
		if seen[q.GetSeq()] != 1 {
			t.Fatalf("seq %d replayed %d times", q.GetSeq(), seen[q.GetSeq()])
		}
	}
	for i, q := range w2.s.Events()[:len(before)] {
		if q.GetSeq() != before[i].GetSeq() || q.GetEvent().GetEventId() != before[i].GetEvent().GetEventId() {
			t.Fatalf("replay %d differs: seq %d id %q, want seq %d id %q", i, q.GetSeq(), q.GetEvent().GetEventId(), before[i].GetSeq(), before[i].GetEvent().GetEventId())
		}
	}
	ctx, cancel := w2.ctx(3 * time.Second)
	w2.s.WaitQuiet(ctx, 300*time.Millisecond)
	cancel()
	last := w2.s.Events()[len(w2.s.Events())-1].GetSeq()
	w2.shutdown()

	w3 := start(t, state, opts{})
	if n := w3.hello.GetLastUnackedSeq(); n != 0 && n <= last {
		t.Fatalf("after acks last_unacked_seq %d", w3.hello.GetLastUnackedSeq())
	}
	w3.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	for _, q := range w3.s.Events() {
		if q.GetSeq() <= last {
			t.Fatalf("acknowledged seq %d sent again", q.GetSeq())
		}
	}
	w3.shutdown()
}

// TestHostStoredSeqFloor: a worker whose store is new (its queue and
// numbering lost) while the host has stored up to some seq numbers every
// new event above that mark; a later start with a lower mark never lowers
// the numbering again.
func TestHostStoredSeqFloor(t *testing.T) {
	const mark = 500
	state := newState(t)
	w := start(t, state, opts{hostStoredSeq: mark})
	if n := w.hello.GetLastUnackedSeq(); n != 0 {
		t.Fatalf("a new store reports last_unacked_seq %d", n)
	}
	w.signIn()
	evs := w.s.Events()
	if len(evs) < 20 {
		t.Fatalf("only %d events", len(evs))
	}
	if first := evs[0].GetSeq(); first != mark+1 {
		t.Fatalf("first seq %d, want %d", first, mark+1)
	}
	for _, q := range evs {
		if q.GetSeq() <= mark {
			t.Fatalf("event %q has seq %d at or below the host's mark %d", q.GetEvent().GetEventId(), q.GetSeq(), mark)
		}
	}
	last := evs[len(evs)-1].GetSeq()
	w.shutdown()

	w2 := start(t, state, opts{hostStoredSeq: 3})
	w2.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	for _, q := range w2.s.Events() {
		if q.GetSeq() <= last {
			t.Fatalf("after a restart with a lower mark, seq %d is not above %d", q.GetSeq(), last)
		}
	}
	w2.shutdown()
}

// TestGeneratedIDsAfterStoreLoss: the ids of the events the worker
// generates carry its store's nonce, so after the store file is lost (the
// host keeps what it landed, and lands each event id once) none of them
// names an event of the lost store, while the network's events synced again
// keep their ids; a restart on the same store keeps the nonce.
func TestGeneratedIDsAfterStoreLoss(t *testing.T) {
	state := newState(t)
	a := start(t, state, opts{})
	a.signIn()
	before := a.s.Events()
	last := before[len(before)-1].GetSeq()
	a.shutdown()
	for _, name := range []string{"worker.db", "worker.db-wal", "worker.db-shm"} {
		if err := os.Remove(filepath.Join(state, name)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	b := start(t, state, opts{hostStoredSeq: last})
	b.signIn()
	after := b.s.Events()
	b.shutdown()

	ids := func(evs []*contract.QueuedEvent) (all map[string]bool, gen []string, network map[string]bool) {
		all, network = map[string]bool{}, map[string]bool{}
		for _, q := range evs {
			id := q.GetEvent().GetEventId()
			all[id] = true
			if generatedID(id) {
				gen = append(gen, id)
			} else if strings.HasPrefix(id, "msg:") {
				network[id] = true
			}
		}
		return all, gen, network
	}
	allA, genA, netA := ids(before)
	_, genB, netB := ids(after)
	kinds := func(gen []string) map[string]bool {
		k := map[string]bool{}
		for _, id := range gen {
			kind, _, _ := strings.Cut(id, ":")
			k[kind] = true
		}
		return k
	}
	for _, k := range []string{"self", "state", "chat", "person", "signin"} {
		if !kinds(genA)[k] || !kinds(genB)[k] {
			t.Fatalf("no %s event in one of the runs", k)
		}
	}
	nonceA, nonceB := nonceOf(genA[0]), nonceOf(genB[0])
	if nonceA == nonceB {
		t.Fatal("the new store has the lost store's nonce")
	}
	same := 0
	for _, id := range genB {
		if allA[id] {
			t.Fatalf("generated id %q names an event of the lost store", id)
		}
		if nonceOf(id) != nonceB {
			t.Fatalf("generated id %q does not carry the store's nonce", id)
		}
		for _, old := range genA {
			if withoutNonce(old) == withoutNonce(id) {
				same++
				break
			}
		}
	}
	for _, id := range genA {
		if nonceOf(id) != nonceA {
			t.Fatalf("generated id %q does not carry the store's nonce", id)
		}
	}
	// Without the nonce many would have repeated: that is what it is for.
	if same == 0 {
		t.Fatal("no generated id would have repeated without the nonce")
	}
	if len(netA) < 5 || len(netA) != len(netB) {
		t.Fatalf("network message ids: %d before, %d after", len(netA), len(netB))
	}
	for id := range netA {
		if !netB[id] {
			t.Fatalf("the network's message %q has another id after the store loss", id)
		}
	}
	t.Logf("%d generated ids after the loss, %d would have repeated without the nonce", len(genB), same)

	c := start(t, state, opts{hostStoredSeq: after[len(after)-1].GetSeq()})
	c.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	c.shutdown()
	for _, q := range c.s.Events() {
		if id := q.GetEvent().GetEventId(); generatedID(id) && nonceOf(id) != nonceB {
			t.Fatalf("a restart on the same store changed the nonce: %q", id)
		}
	}
}

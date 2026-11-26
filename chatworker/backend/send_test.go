// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
)

func testBackend(t *testing.T) (*Backend, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	db, _, err := store.OpenDB(filepath.Join(dir, "worker.db"),
		store.OpenOptions{Key: bytes.Repeat([]byte{7}, store.KeyLen), DevPlain: true}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	st, err := store.Open(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	log := logx.New(io.Discard, "c")
	b := New(&contract.Init{StateDir: dir}, st, NewEmitter(st, log, nil), log, Hooks{}, 1)
	return b, st
}

func resolvedEvents(t *testing.T, st *store.Store) []*contract.SendResolved {
	t.Helper()
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []*contract.SendResolved
	for _, p := range pending {
		var ev contract.ChatEvent
		if err := contract.Unmarshal(p.Event, &ev); err != nil {
			t.Fatal(err)
		}
		if r := ev.GetSendResolved(); r != nil {
			out = append(out, r)
		}
	}
	return out
}

// TestStatusBeforeAnswered: a send whose framework result is not final is
// answered unknown; a final status that arrives after the result was read
// and before the send is marked answered (recordStatus then does not
// resolve it) must still resolve it, once.
func TestStatusBeforeAnswered(t *testing.T) {
	ctx := context.Background()
	refused := &bridgev2.MessageStatus{Status: event.MessageStatusFail, IsCertain: true, ErrorReason: "refused_by_test"}
	for _, c := range []struct {
		name string
		// taken: the status finalize read the result with; recorded: one
		// that arrived after that.
		taken, recorded *bridgev2.MessageStatus
	}{
		{"recorded in the window", nil, refused},
		{"taken with a backgrounded result", refused, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, st := testBackend(t)
			evtID := id.EventID("$e-" + c.name)
			if _, err := st.JournalBegin(ctx, store.JournalRow{Key: "k1", ChatID: "c", Kind: "send", Boot: 1, CommandID: "cmd-1"}); err != nil {
				t.Fatal(err)
			}
			ps := &pendingSend{commandID: "cmd-1", key: "k1", receiver: "l", sync: true}
			b.statusMu.Lock()
			b.waiting[evtID] = ps
			b.statusMu.Unlock()
			if c.recorded != nil {
				b.recordStatus(st.Epoch(), evtID, c.recorded)
				if n := len(resolvedEvents(t, st)); n != 0 {
					t.Fatalf("resolved before the command was answered: %d", n)
				}
			}
			b.answeredPending(evtID, ps, c.taken)
			if n := len(resolvedEvents(t, st)); n != 1 {
				t.Fatalf("%d SendResolved once answered, want 1", n)
			}
			// A status repeated later changes nothing.
			b.recordStatus(st.Epoch(), evtID, refused)
			res := resolvedEvents(t, st)
			if len(res) != 1 || res[0].GetCommandId() != "cmd-1" || res[0].GetOutcome() != contract.Outcome_OUTCOME_REJECTED || res[0].GetCode() != "refused_by_test" {
				t.Fatalf("resolved events: %v", res)
			}
			row, _ := st.JournalGet(ctx, "k1")
			if row.State != store.Rejected {
				t.Fatalf("journal %+v", row)
			}
		})
	}
}

// TestNoSendWriteAfterWipe: a send begun before a wipe (a logout while it
// waited) writes and reports nothing after it, even when its result comes
// after it passed every other check (forgetSends had not marked it): no
// SendResolved is queued and no journal row comes back.
func TestNoSendWriteAfterWipe(t *testing.T) {
	b, st := testBackend(t)
	ctx := context.Background()
	if _, err := st.JournalBegin(ctx, store.JournalRow{Key: "k-wiped", ChatID: "c", Kind: "send", Boot: 1}); err != nil {
		t.Fatal(err)
	}
	ps := &pendingSend{commandID: "cmd-wiped", key: "k-wiped", answered: true, epoch: st.Epoch()}
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	b.finishSend(ctx, id.EventID("$wiped"), ps, contract.Outcome_OUTCOME_OK, "", "remote-1")
	if r := resolvedEvents(t, st); len(r) != 0 {
		t.Fatalf("SendResolved after the wipe: %v", r)
	}
	// The same send begun after the wipe is reported.
	ps = &pendingSend{commandID: "cmd-new", key: "k-new", answered: true, epoch: st.Epoch()}
	b.finishSend(ctx, id.EventID("$new"), ps, contract.Outcome_OUTCOME_OK, "", "remote-2")
	if r := resolvedEvents(t, st); len(r) != 1 || r[0].GetCommandId() != "cmd-new" {
		t.Fatalf("SendResolved of a send after the wipe: %v", r)
	}
}

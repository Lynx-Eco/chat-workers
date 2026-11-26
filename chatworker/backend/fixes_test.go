// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// outboxEvents are the events in the outbox, oldest first.
func outboxEvents(t *testing.T, st *store.Store) []*contract.ChatEvent {
	t.Helper()
	pending, err := st.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []*contract.ChatEvent
	for _, p := range pending {
		var ev contract.ChatEvent
		if err := contract.Unmarshal(p.Event, &ev); err != nil {
			t.Fatal(err)
		}
		out = append(out, &ev)
	}
	return out
}

// TestStaleStatusSettlesNothing: a send's event id depends only on its key,
// so after a logout and a sign-in again the host's resend of key K has the
// id of the old one. A late status of the stopped instance, failure or
// success, settles nothing of the new send: neither as it comes (the send
// waits, answered unknown), nor kept from before the wipe and read when the
// new send is answered. The new instance's status still settles it.
func TestStaleStatusSettlesNothing(t *testing.T) {
	refused := &bridgev2.MessageStatus{Status: event.MessageStatusFail, IsCertain: true, ErrorReason: "refused_by_test"}
	sent := &bridgev2.MessageStatus{Status: event.MessageStatusSuccess}
	for _, c := range []struct {
		name   string
		status *bridgev2.MessageStatus
		// kept: the old status came before the wipe, with no send waiting.
		kept bool
	}{
		{"late failure", refused, false},
		{"late success", sent, false},
		{"kept failure", refused, true},
		{"kept success", sent, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			b, st := testBackend(t)
			const key = "k-same"
			evtID := b.txnEventID(key)
			info := &bridgev2.MessageStatusEventInfo{SourceEventID: evtID}
			old := b.newInstance()
			if c.kept {
				old.SendMessageStatus(ctx, c.status, info)
			}
			if err := st.Wipe(ctx); err != nil {
				t.Fatal(err)
			}
			fresh := b.newInstance()
			if _, err := st.JournalBegin(ctx, store.JournalRow{Key: key, ChatID: "c", Kind: "send", Boot: 1, CommandID: "cmd-new"}); err != nil {
				t.Fatal(err)
			}
			ps := &pendingSend{commandID: "cmd-new", key: key, receiver: "acct", sync: true, epoch: st.Epoch()}
			b.statusMu.Lock()
			b.waiting[evtID] = ps
			b.statusMu.Unlock()
			b.answeredPending(evtID, ps, nil)
			if !c.kept {
				old.SendMessageStatus(ctx, c.status, info)
			}
			if r := resolvedEvents(t, st); len(r) != 0 {
				t.Fatalf("the stopped instance's status settled the new send: %v", r)
			}
			fresh.SendMessageStatus(ctx, refused, info)
			r := resolvedEvents(t, st)
			if len(r) != 1 || r[0].GetCommandId() != "cmd-new" || r[0].GetCode() != "refused_by_test" {
				t.Fatalf("the new instance's status: %v, want the new send settled", r)
			}
		})
	}
}

// TestStaleStatusKeepsNewOne: the new instance reports the status of its
// own send of key K while the send still waits for the framework's result
// (the status is kept for it); then a late status of the instance a logout
// stopped comes for the same event id (the same key). The new send still
// reads its own status. Kept under the event id alone, the late one
// replaced it, and the send read none (takeStatus drops another epoch's).
func TestStaleStatusKeepsNewOne(t *testing.T) {
	ctx := context.Background()
	b, st := testBackend(t)
	const key = "k-same"
	evtID := b.txnEventID(key)
	info := &bridgev2.MessageStatusEventInfo{SourceEventID: evtID}
	old := b.newInstance()
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := b.newInstance()
	refused := &bridgev2.MessageStatus{Status: event.MessageStatusFail, IsCertain: true, ErrorReason: "refused_by_test"}
	fresh.SendMessageStatus(ctx, refused, info)
	old.SendMessageStatus(ctx, &bridgev2.MessageStatus{Status: event.MessageStatusSuccess}, info)
	got := b.takeStatus(fresh.epoch, evtID)
	if got == nil || got.ErrorReason != "refused_by_test" {
		t.Fatalf("the new send read %v, want its own instance's status", got)
	}
	if left := b.takeStatus(old.epoch, evtID); left != nil {
		t.Fatalf("the stopped instance's status was kept: %v", left)
	}
}

func stateCounter(t *testing.T, st *store.Store) int {
	t.Helper()
	return countRows(t, st, `SELECT COALESCE((SELECT value FROM lynx_kv WHERE name = 'state_event'), 0)`)
}

// TestStaleStateMovesNothing: a bridge state of a stopped instance moves
// no state counter, stores nothing and logs nothing as reported; the new
// instance's does all three.
func TestStaleStateMovesNothing(t *testing.T) {
	ctx := context.Background()
	b, st, logs := logBackend(t)
	old := b.newInstance()
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	state := &status.BridgeState{StateEvent: status.StateConnected, RemoteID: "acct"}
	_ = old.SendBridgeStatus(ctx, state)
	if n := stateCounter(t, st); n != 0 {
		t.Fatalf("the stopped instance's state moved the counter to %d", n)
	}
	if n := outboxLen(t, st); n != 0 {
		t.Fatalf("the stopped instance's state stored %d events", n)
	}
	if strings.Contains(logs.String(), `"code":"connection_state"`) {
		t.Fatalf("the stopped instance's state was logged as reported:\n%s", logs)
	}
	_ = b.newInstance().SendBridgeStatus(ctx, state)
	if n := stateCounter(t, st); n != 1 || outboxLen(t, st) != 1 || strings.Count(logs.String(), `"code":"connection_state"`) != 1 {
		t.Fatalf("the new instance's state: counter %d, %d events, log:\n%s", n, outboxLen(t, st), logs)
	}
	// A state not stored (events silenced, as during a logout) is not
	// logged as reported either.
	b.silenced.Store(true)
	b.emitState(&contract.ConnectionState{State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED})
	b.silenced.Store(false)
	if n := strings.Count(logs.String(), `"code":"connection_state"`); n != 1 || outboxLen(t, st) != 1 {
		t.Fatalf("a silenced state: %d log lines, %d events; want 1 and 1", n, outboxLen(t, st))
	}
}

// TestSelfRetriedAfterOutboxFailure: when the account's own PersonInfo
// cannot be stored (the outbox refuses it), the chat event it was to
// precede is refused too, and the next chat event stores the PersonInfo
// first: the outbox never holds a chat event before it.
func TestSelfRetriedAfterOutboxFailure(t *testing.T) {
	ctx := context.Background()
	b, st, _ := logBackend(t)
	fw := b.newInstance()
	// The outbox refuses every event whose bytes hold the self event id.
	if _, err := st.DB().Exec(ctx, `CREATE TRIGGER test_fail_self BEFORE INSERT ON lynx_outbox
		WHEN instr(NEW.event, CAST('self:' AS BLOB)) > 0 BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if seq := fw.emitChat("acct", "chat:x:1", &contract.ChatInfo{ChatId: "x"}); seq != 0 {
		t.Fatalf("a chat event was stored (seq %d) while its account's own PersonInfo was not", seq)
	}
	if n := outboxLen(t, st); n != 0 {
		t.Fatalf("%d events stored while the own PersonInfo failed", n)
	}
	if _, err := st.DB().Exec(ctx, `DROP TRIGGER test_fail_self`); err != nil {
		t.Fatal(err)
	}
	if seq := fw.emitChat("acct", "chat:x:2", &contract.ChatInfo{ChatId: "x"}); seq == 0 {
		t.Fatal("the chat event after the outbox recovered was not stored")
	}
	evs := outboxEvents(t, st)
	if len(evs) != 2 || !evs[0].GetPersonInfo().GetIsSelf() || evs[0].GetPersonInfo().GetPersonId() != "acct" || evs[1].GetChatInfo() == nil {
		t.Fatalf("outbox %v, want the own PersonInfo then the chat", evs)
	}
}

// TestGateAnnouncesNothing: the gate lets a new login through before the
// framework keeps it (NewLogin inserts it after): nothing of the account
// goes upward from there.
func TestGateAnnouncesNothing(t *testing.T) {
	b, st, _ := logBackend(t)
	if _, err := st.DB().Exec(t.Context(), `CREATE TABLE user_login (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	fw := b.newInstance()
	g := b.gate(&recordingNet{}, fw)
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-mine", RemoteName: "Mine"}}
	if err := g.LoadUserLogin(t.Context(), login); err != nil {
		t.Fatal(err)
	}
	if evs := outboxEvents(t, st); len(evs) != 0 {
		t.Fatalf("the gate reported %v for a login not kept yet", evs)
	}
}

// TestStaleSendMessageRefused: after a wipe the new instance makes again
// the room of the same chat, with the same id. A message the stopped
// instance's intent still sends into it finds the room (the new epoch's
// row) and passes every check but the epoch: it must not be stored. The
// same message from the new instance is.
func TestStaleSendMessageRefused(t *testing.T) {
	ctx := context.Background()
	b, st, _ := logBackend(t)
	key := networkid.PortalKey{ID: "chat-1", Receiver: "acct"}
	old := b.newInstance()
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := b.newInstance()
	room := fresh.GenerateDeterministicRoomID(key)
	if _, err := fresh.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{BeeperLocalRoomID: room}); err != nil {
		t.Fatal(err)
	}
	send := func(fw *instance, msg string) {
		_, err := fw.GhostIntent("u-a").SendMessage(ctx, room, event.EventMessage,
			&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: msg}},
			&bridgev2.MatrixSendExtra{Timestamp: time.Now(), MessageMeta: &database.Message{ID: networkid.MessageID(msg), SenderID: "u-a"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	messagesOf := func() []string {
		var ids []string
		for _, e := range outboxEvents(t, st) {
			if m := e.GetMessage(); m != nil {
				ids = append(ids, m.GetMessageId())
			}
		}
		return ids
	}
	send(old, "m-old")
	if m := messagesOf(); len(m) != 0 {
		t.Fatalf("the stopped instance's message was stored: %v", m)
	}
	send(fresh, "m-new")
	if m := messagesOf(); len(m) != 1 || m[0] != "m-new" {
		t.Fatalf("messages %v, want the new instance's", m)
	}
}

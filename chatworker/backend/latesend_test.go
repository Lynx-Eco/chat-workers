// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// lateSend is a send the connector answered pending, as runKeyedFresh
// leaves it: journal row in flight, waiting for its status, answered
// unknown (answeredPending), with lateOK armed the way setLateOK arms it
// for cmdSend (echoSend of the stored row with the command's content).
type lateSend struct {
	b     *Backend
	st    *store.Store
	bdb   *database.Database
	key   networkid.PortalKey
	room  id.RoomID
	evtID id.EventID
	ps    *pendingSend
	logs  *lockedBuffer
}

const lateKey = "k-late"

func newLateSend(t *testing.T, armed bool) *lateSend {
	t.Helper()
	l := newArmedSend(t, armed)
	l.b.answeredPending(l.evtID, l.ps, nil)
	return l
}

// newArmedSend is newLateSend before the send was answered: still waiting
// for its framework result (sync), as runKeyedFresh registers it, with
// lateOK armed when armed.
func newArmedSend(t *testing.T, armed bool) *lateSend {
	t.Helper()
	ctx := context.Background()
	b, st, logs, bdb := frameworkBackend(t)
	key := networkid.PortalKey{ID: "chat:1", Receiver: "acct"}
	room := madeRoom(t, b.newInstance(), bdb, key, "Chat", formatGhost("a"))
	if _, err := st.JournalBegin(ctx, store.JournalRow{Key: lateKey, ChatID: "chat:1", Kind: "send", Boot: 1, CommandID: "cmd-late"}); err != nil {
		t.Fatal(err)
	}
	evtID := b.txnEventID(lateKey)
	ps := &pendingSend{commandID: "cmd-late", key: lateKey, receiver: "acct", sync: true, epoch: st.Epoch()}
	if armed {
		portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key, MXID: room}, Bridge: b.bridge()}
		content := &event.MessageEventContent{MsgType: event.MsgText, Body: "sent from Lynx"}
		ps.lateOK = func(ctx context.Context, part *database.Message) {
			b.echoSend(ctx, portal, nil, part, content, lateKey)
		}
	}
	b.statusMu.Lock()
	b.waiting[evtID] = ps
	b.statusMu.Unlock()
	return &lateSend{b: b, st: st, bdb: bdb, key: key, room: room, evtID: evtID, ps: ps, logs: logs}
}

// kept tells whether the send is kept for a late echo (keepForLateEchoLocked).
func (l *lateSend) kept() bool {
	l.b.statusMu.Lock()
	defer l.b.statusMu.Unlock()
	return l.b.lateEchoes[l.evtID] != nil
}

// held tells whether the send is kept and its hold has not ended
// (releaseLateEcho).
func (l *lateSend) held() bool {
	l.b.statusMu.Lock()
	defer l.b.statusMu.Unlock()
	k := l.b.lateEchoes[l.evtID]
	return k != nil && k.held
}

// echoed stores the network's message row the way the framework does when
// the network echoes a pending send (portal.checkPendingMessage): the send's
// transaction id on it, the network's id and the deterministic event id.
func (l *lateSend) echoed(t *testing.T, msg networkid.MessageID) {
	t.Helper()
	if err := l.bdb.Message.Insert(context.Background(), &database.Message{
		ID: msg, MXID: messageEventID(l.key, msg, ""), Room: l.key, SenderID: "a", SenderMXID: OwnerMXID,
		Timestamp: time.Unix(1795000000, 0), SendTxnID: l.b.txnFor(lateKey),
	}); err != nil {
		t.Fatal(err)
	}
}

func (l *lateSend) status(ms *bridgev2.MessageStatus) {
	l.b.recordStatus(l.st.Epoch(), l.evtID, ms)
}

// ownMessages are the Message events stored, and the index in the outbox of
// the first SendResolved (-1: none).
func ownMessages(t *testing.T, st *store.Store) (msgs []*contract.Message, resolvedAt int, firstMsgAt int) {
	t.Helper()
	resolvedAt, firstMsgAt = -1, -1
	for i, e := range outboxEvents(t, st) {
		if m := e.GetMessage(); m != nil {
			if firstMsgAt < 0 {
				firstMsgAt = i
			}
			msgs = append(msgs, m)
		}
		if e.GetSendResolved() != nil && resolvedAt < 0 {
			resolvedAt = i
		}
	}
	return msgs, resolvedAt, firstMsgAt
}

var (
	lateSent     = &bridgev2.MessageStatus{Status: event.MessageStatusSuccess}
	lateRefused  = &bridgev2.MessageStatus{Status: event.MessageStatusFail, IsCertain: true, ErrorReason: event.MessageStatusUnsupported}
	lateTimedOut = &bridgev2.MessageStatus{Status: event.MessageStatusRetriable, ErrorReason: event.MessageStatusTooOld}
)

// TestLateSuccessReportsOwnMessage: a send the connector answered pending
// (mautrix-gmessages: every send) that a late status resolves OK reports the
// account's own message, as a synchronous success does (echoSend): one
// Message of the stored row, from_me, with the send's key, the network's
// message id and the chat's id, before its SendResolved OK with that id; a
// status repeated later reports nothing more. WHY: catches the user's own
// sent message never reaching the thread for a worker whose sends are all
// asynchronous, and a second copy of it at a repeated status.
func TestLateSuccessReportsOwnMessage(t *testing.T) {
	ctx := context.Background()
	l := newLateSend(t, true)
	if msgs, at, _ := ownMessages(t, l.st); len(msgs) != 0 || at >= 0 {
		t.Fatalf("before the status: %d messages, resolved at %d", len(msgs), at)
	}
	l.echoed(t, "net-1")
	l.status(lateSent)
	msgs, resolvedAt, firstMsgAt := ownMessages(t, l.st)
	room, _ := l.st.Room(ctx, string(l.room))
	if len(msgs) != 1 {
		t.Fatalf("%d own messages, want 1", len(msgs))
	}
	m := msgs[0]
	if !m.GetFromMe() || m.GetSendKey() != lateKey || m.GetMessageId() != "net-1" || room == nil || m.GetChatId() != room.ChatID ||
		m.GetContent().GetText() != "sent from Lynx" {
		t.Fatalf("own message %v (room %+v)", m, room)
	}
	res := resolvedEvents(t, l.st)
	if len(res) != 1 || res[0].GetOutcome() != contract.Outcome_OUTCOME_OK || res[0].GetRemoteMessageId() != "net-1" || res[0].GetSendKey() != lateKey {
		t.Fatalf("resolved %v", res)
	}
	if firstMsgAt > resolvedAt {
		t.Fatalf("the message (at %d) came after its SendResolved (at %d)", firstMsgAt, resolvedAt)
	}
	row, _ := l.st.JournalGet(ctx, lateKey)
	if row == nil || row.State != store.Sent || row.RemoteID != "net-1" {
		t.Fatalf("journal %+v", row)
	}
	// The connector's later statuses of the same message (delivered, read)
	// change nothing.
	l.status(lateSent)
	l.b.answeredPending(l.evtID, l.ps, lateSent)
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 1 {
		t.Fatalf("%d own messages after repeated statuses, want 1", len(msgs))
	}
	if n := len(resolvedEvents(t, l.st)); n != 1 {
		t.Fatalf("%d SendResolved after repeated statuses, want 1", n)
	}
}

// TestLateSuccessTakenAtAnswer: the status already came while the
// framework handled the send (the network echoed fast), and is taken when
// the send is answered: the own message is reported once there.
func TestLateSuccessTakenAtAnswer(t *testing.T) {
	b, st, _, bdb := frameworkBackend(t)
	ctx := context.Background()
	key := networkid.PortalKey{ID: "chat:1", Receiver: "acct"}
	room := madeRoom(t, b.newInstance(), bdb, key, "Chat", formatGhost("a"))
	if _, err := st.JournalBegin(ctx, store.JournalRow{Key: lateKey, ChatID: "chat:1", Kind: "send", Boot: 1, CommandID: "cmd-late"}); err != nil {
		t.Fatal(err)
	}
	evtID := b.txnEventID(lateKey)
	calls := 0
	ps := &pendingSend{commandID: "cmd-late", key: lateKey, receiver: "acct", sync: true, epoch: st.Epoch()}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key, MXID: room}, Bridge: b.bridge()}
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: "fast"}
	ps.lateOK = func(ctx context.Context, part *database.Message) {
		calls++
		b.echoSend(ctx, portal, nil, part, content, lateKey)
	}
	if err := bdb.Message.Insert(ctx, &database.Message{ID: "net-2", MXID: messageEventID(key, "net-2", ""), Room: key, SenderID: "a",
		SenderMXID: OwnerMXID, Timestamp: time.Unix(1795000000, 0), SendTxnID: b.txnFor(lateKey)}); err != nil {
		t.Fatal(err)
	}
	b.statusMu.Lock()
	b.waiting[evtID] = ps
	b.statusMu.Unlock()
	// Recorded while the send was still synchronous: kept, not resolved.
	b.recordStatus(st.Epoch(), evtID, lateSent)
	if msgs, _, _ := ownMessages(t, st); len(msgs) != 0 {
		t.Fatalf("%d own messages before the answer", len(msgs))
	}
	b.answeredPending(evtID, ps, nil)
	b.recordStatus(st.Epoch(), evtID, lateSent)
	if msgs, _, _ := ownMessages(t, st); len(msgs) != 1 || calls != 1 {
		t.Fatalf("%d own messages, %d reports, want 1 and 1", len(msgs), calls)
	}
}

// TestLateResultWithoutOwnMessage: no own message is reported for a late
// result that is REJECTED (the phone refused it), for one still UNKNOWN (the
// minute's echo timeout) of a send whose lateOK is not armed, for an OK
// whose row is not found, nor for an OK of a send whose lateOK is not armed
// (a synchronous connector, a backgrounded handler, a command other than
// Send); each still gets its SendResolved, and a later success finds the
// send finished. WHY: catches a message reported for a send that did not
// go, or may not have, and an echo added to what a synchronous connector's
// late result reports. (An armed send's UNKNOWN end and its late echo:
// TestLateEchoAfterUnknown.)
func TestLateResultWithoutOwnMessage(t *testing.T) {
	for _, c := range []struct {
		name    string
		armed   bool
		row     bool
		status  *bridgev2.MessageStatus
		outcome contract.Outcome
	}{
		{"rejected", true, true, lateRefused, contract.Outcome_OUTCOME_REJECTED},
		{"echo timeout, not armed", false, true, lateTimedOut, contract.Outcome_OUTCOME_UNKNOWN},
		{"no row", true, false, lateSent, contract.Outcome_OUTCOME_OK},
		{"not armed", false, true, lateSent, contract.Outcome_OUTCOME_OK},
	} {
		t.Run(c.name, func(t *testing.T) {
			l := newLateSend(t, c.armed)
			if c.row {
				l.echoed(t, "net-3")
			}
			l.status(c.status)
			if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 {
				t.Fatalf("%d own messages, want none", len(msgs))
			}
			res := resolvedEvents(t, l.st)
			if len(res) != 1 || res[0].GetOutcome() != c.outcome {
				t.Fatalf("resolved %v, want one %v", res, c.outcome)
			}
			// After the timeout, the phone's success finds the send
			// finished: nothing more is reported.
			l.status(lateSent)
			if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 {
				t.Fatalf("%d own messages after a later success, want none", len(msgs))
			}
			if n := len(resolvedEvents(t, l.st)); n != 1 {
				t.Fatalf("%d SendResolved after a later success, want 1", n)
			}
			if l.kept() {
				t.Fatal("a send not armed, or not UNKNOWN, kept for a late echo")
			}
		})
	}
}

// TestLateEchoAfterUnknown: a send whose lateOK is armed and that finished
// UNKNOWN is kept for the network's late echo, both when a late status
// ended it (answered: its SendResolved UNKNOWN) and when its framework
// result did (mautrix-gmessages' phone that did not answer: the command's
// own reply was UNKNOWN, no SendResolved). The network's later success
// reports its message once, after that SendResolved, with no second
// SendResolved, and the journal row becomes Sent with the network's id; a
// repeated success reports nothing more. WHY: catches the user's message
// lost when the phone sends it after the worker answered UNKNOWN (the
// framework saves its row and makes no event of it), a send reported
// resolved twice, and an echo reported twice.
func TestLateEchoAfterUnknown(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name     string
		answered bool
	}{
		{"answered, then an echo timeout", true},
		{"a final UNKNOWN result", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var l *lateSend
			if c.answered {
				l = newLateSend(t, true)
				l.status(lateTimedOut)
			} else {
				l = newArmedSend(t, true)
				l.b.finishSend(ctx, l.evtID, l.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
			}
			wantResolved := 0
			if c.answered {
				wantResolved = 1
			}
			if res := resolvedEvents(t, l.st); len(res) != wantResolved || (c.answered && res[0].GetOutcome() != contract.Outcome_OUTCOME_UNKNOWN) {
				t.Fatalf("resolved %v, want %d UNKNOWN", res, wantResolved)
			}
			if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 || !l.kept() {
				t.Fatalf("after the UNKNOWN end: %d messages, kept %v", len(msgs), l.kept())
			}
			// The phone came back: the framework saved the row and sent a
			// success status for the send's event.
			l.echoed(t, "net-8")
			l.status(lateSent)
			msgs, resolvedAt, firstMsgAt := ownMessages(t, l.st)
			if len(msgs) != 1 {
				t.Fatalf("%d own messages, want 1", len(msgs))
			}
			if m := msgs[0]; !m.GetFromMe() || m.GetSendKey() != lateKey || m.GetMessageId() != "net-8" || m.GetContent().GetText() != "sent from Lynx" {
				t.Fatalf("own message %v", m)
			}
			if c.answered && firstMsgAt < resolvedAt {
				t.Fatalf("the message (at %d) came before the SendResolved UNKNOWN (at %d)", firstMsgAt, resolvedAt)
			}
			if n := len(resolvedEvents(t, l.st)); n != wantResolved {
				t.Fatalf("%d SendResolved after the echo, want %d", n, wantResolved)
			}
			row, _ := l.st.JournalGet(ctx, lateKey)
			if row == nil || row.State != store.Sent || row.RemoteID != "net-8" {
				t.Fatalf("journal %+v", row)
			}
			if l.kept() {
				t.Fatal("still kept after its echo")
			}
			l.status(lateSent)
			if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 1 {
				t.Fatalf("%d own messages after a repeated success, want 1", len(msgs))
			}
		})
	}
}

// TestLateEchoBounded: a send kept for a late echo is kept for
// lateEchoLimit, and none is kept after a logout (forgetSends) or a wipe
// since the send began; a success after that reports nothing, and a status
// of another epoch never does. WHY: catches a kept send that is never let
// go (one a send whose phone never comes back), and a message reported
// after a logout.
func TestLateEchoBounded(t *testing.T) {
	ctx := context.Background()
	t.Run("limit", func(t *testing.T) {
		orig := lateEchoLimit
		lateEchoLimit = 20 * time.Millisecond
		t.Cleanup(func() { lateEchoLimit = orig })
		l := newLateSend(t, true)
		l.status(lateTimedOut)
		deadline := time.Now().Add(5 * time.Second)
		for l.kept() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if l.kept() {
			t.Fatal("still kept after lateEchoLimit")
		}
		l.echoed(t, "net-9")
		l.status(lateSent)
		if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 {
			t.Fatalf("%d own messages after the limit, want none", len(msgs))
		}
	})
	t.Run("logout", func(t *testing.T) {
		l := newLateSend(t, true)
		l.status(lateTimedOut)
		l.b.forgetSends()
		if l.kept() {
			t.Fatal("kept after forgetSends")
		}
		l.echoed(t, "net-10")
		l.status(lateSent)
		if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 {
			t.Fatalf("%d own messages after the logout, want none", len(msgs))
		}
	})
	t.Run("another epoch", func(t *testing.T) {
		l := newLateSend(t, true)
		l.status(lateTimedOut)
		l.echoed(t, "net-11")
		l.b.recordStatus(l.ps.epoch+1, l.evtID, lateSent)
		if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 || !l.kept() {
			t.Fatalf("a status of another epoch: %d messages, kept %v", len(msgs), l.kept())
		}
	})
	t.Run("wiped before the end", func(t *testing.T) {
		l := newArmedSend(t, true)
		if err := l.st.Wipe(ctx); err != nil {
			t.Fatal(err)
		}
		l.b.finishSend(ctx, l.evtID, l.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
		if l.kept() {
			t.Fatal("kept after a wipe since the send began")
		}
	})
}

// TestFinishSendNeverEchoes: a send finished by its framework result (the
// synchronous path, and the deadline path whose finalize already ran onOK
// and its echoSend) does not run lateOK, armed or not: only a status's
// resolution does. WHY: catches the own message reported twice when a
// command's deadline passed before a synchronous success.
func TestFinishSendNeverEchoes(t *testing.T) {
	b, st := testBackend(t)
	calls := 0
	ps := &pendingSend{commandID: "cmd-sync", key: "k-sync", answered: true, epoch: st.Epoch(),
		lateOK: func(context.Context, *database.Message) { calls++ }}
	b.finishSend(context.Background(), id.EventID("$sync"), ps, contract.Outcome_OUTCOME_OK, "", "net-4")
	if calls != 0 {
		t.Fatalf("lateOK ran %d times from finishSend", calls)
	}
	if r := resolvedEvents(t, st); len(r) != 1 || r[0].GetRemoteMessageId() != "net-4" {
		t.Fatalf("resolved %v", r)
	}
}

// TestFinishSendEchoOnce: the echo finishSendEcho is given runs once, by
// the call that finishes the send, only for an OK of a send already
// answered, and not after a wipe since the send began (a logout): then
// neither the message nor the SendResolved is reported.
func TestFinishSendEchoOnce(t *testing.T) {
	ctx := context.Background()
	b, st := testBackend(t)
	calls := 0
	echo := func() { calls++ }

	wiped := &pendingSend{commandID: "cmd-w", key: "k-w", answered: true, epoch: st.Epoch()}
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	b.finishSendEcho(ctx, id.EventID("$w"), wiped, contract.Outcome_OUTCOME_OK, "", "net-5", echo)
	if calls != 0 || len(outboxEvents(t, st)) != 0 {
		t.Fatalf("after the wipe: %d echoes, %d events", calls, len(outboxEvents(t, st)))
	}

	ps := &pendingSend{commandID: "cmd-1", key: "k-1", answered: true, epoch: st.Epoch()}
	b.finishSendEcho(ctx, id.EventID("$1"), ps, contract.Outcome_OUTCOME_OK, "", "net-6", echo)
	b.finishSendEcho(ctx, id.EventID("$1"), ps, contract.Outcome_OUTCOME_OK, "", "net-6", echo)
	if calls != 1 {
		t.Fatalf("%d echoes of one send, want 1", calls)
	}
	notAnswered := &pendingSend{commandID: "cmd-2", key: "k-2", epoch: st.Epoch()}
	b.finishSendEcho(ctx, id.EventID("$2"), notAnswered, contract.Outcome_OUTCOME_OK, "", "net-7", echo)
	refused := &pendingSend{commandID: "cmd-3", key: "k-3", answered: true, epoch: st.Epoch()}
	b.finishSendEcho(ctx, id.EventID("$3"), refused, contract.Outcome_OUTCOME_REJECTED, "refused", "", echo)
	unknown := &pendingSend{commandID: "cmd-4", key: "k-4", answered: true, epoch: st.Epoch()}
	b.finishSendEcho(ctx, id.EventID("$4"), unknown, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "", echo)
	if calls != 1 {
		t.Fatalf("%d echoes, want only the first send's", calls)
	}
}

// TestConnectorPending: only the connector's own pending answer arms the
// late report of a send's message: not a synchronous success (every
// connector but mautrix-gmessages, and mautrix-slack but for a file share
// whose message Slack names later), not a handler the framework backgrounded
// because it took too long, not an ignored or failed event. WHY: catches a
// change of what a synchronous connector's sends report.
func TestConnectorPending(t *testing.T) {
	for _, c := range []struct {
		name string
		res  bridgev2.EventHandlingResult
		want bool
	}{
		{"pending (Queued)", bridgev2.EventHandlingResultQueued, true},
		{"synchronous success", bridgev2.EventHandlingResultSuccess, false},
		{"success with an event id", bridgev2.EventHandlingResultSuccess.WithEventID("$x"), false},
		{"backgrounded", bridgev2.EventHandlingResult{Queued: true, Success: true, Error: bridgev2.ErrHandlerBackgrounded}, false},
		{"ignored", bridgev2.EventHandlingResultIgnored, false},
		{"failed", bridgev2.EventHandlingResultFailed.WithError(errors.New("x")), false},
		{"queued but failed", bridgev2.EventHandlingResult{Queued: true, Error: errors.New("x")}, false},
	} {
		if got := connectorPending(c.res); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestLateEchoKeptAfterReply: a send not answered yet whose framework result
// is a final UNKNOWN (answered by the reply that follows) is kept for a late
// echo at once but held: a success before its reply is out is noted, and
// reported once that reply has been sent (afterReply, run by serve after
// SendReply), once; after a wipe between, or forgetSends between (a
// logout's order, before its wipe), nothing is kept; an answered send (its
// SendResolved UNKNOWN already queued) is kept with no hold. WHY: catches a
// late echo's message reported before the UNKNOWN reply of its own command,
// one lost in that gap, and a send kept again after a logout.
func TestLateEchoKeptAfterReply(t *testing.T) {
	ctx := context.Background()
	l := newArmedSend(t, true)
	ar := &afterReply{}
	l.b.finishSend(withAfterReply(ctx, ar), l.evtID, l.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
	if !l.held() {
		t.Fatal("not kept, held, before its reply was sent")
	}
	// The network echoes it before the reply is out: noted, not reported.
	l.echoed(t, "net-12")
	l.status(lateSent)
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 || !l.held() {
		t.Fatalf("before the reply: %d own messages, held %v; want none, held", len(msgs), l.held())
	}
	ar.run()
	if l.kept() {
		t.Fatal("still kept once its noted echo was reported")
	}
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 1 || msgs[0].GetMessageId() != "net-12" {
		t.Fatalf("%d own messages after the reply, want 1", len(msgs))
	}
	l.status(lateSent)
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 1 {
		t.Fatalf("%d own messages after a repeated success, want 1", len(msgs))
	}
	if whenReplied(withAfterReply(ctx, ar), func() {}) || whenReplied(ctx, func() {}) {
		t.Fatal("whenReplied deferred to a reply already sent, or to none")
	}

	wiped := newArmedSend(t, true)
	ar = &afterReply{}
	wiped.b.finishSend(withAfterReply(ctx, ar), wiped.evtID, wiped.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
	if err := wiped.st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	ar.run()
	if wiped.kept() {
		t.Fatal("kept after a wipe between its end and its reply")
	}

	// A logout's order: forgetSends, then the wipe. A reply sent between the
	// two does not keep again what forgetSends let go.
	between := newArmedSend(t, true)
	ar = &afterReply{}
	between.b.finishSend(withAfterReply(ctx, ar), between.evtID, between.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
	between.b.forgetSends()
	ar.run()
	if between.kept() {
		t.Fatal("kept again by a reply sent between forgetSends and the wipe")
	}
	if err := between.st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}

	answered := newLateSend(t, true)
	ar = &afterReply{}
	answered.b.finishSend(withAfterReply(ctx, ar), answered.evtID, answered.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
	if !answered.kept() || answered.held() {
		t.Fatal("an answered send not kept, its hold ended, at once")
	}
}

// TestLateEchoNoGapAtTheEnd: a success that came for the send after the
// framework's final UNKNOWN result was read and before the send was let go
// from waiting (kept as a status, as for a send still running) is taken
// over by the keep and reported once its reply is out; and a success that
// found the send waiting while it was being finished (resolveByStatus
// finished nothing) is reported from the keep. WHY: catches the gap the
// keep left between the result and the reply (B), where such a success was
// dropped with the journal row left in flight.
func TestLateEchoNoGapAtTheEnd(t *testing.T) {
	ctx := context.Background()
	// Recorded while the send still waited for its result (sync): kept.
	l := newArmedSend(t, true)
	l.echoed(t, "net-14")
	l.status(lateSent)
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 {
		t.Fatalf("%d own messages before the end, want none", len(msgs))
	}
	ar := &afterReply{}
	l.b.finishSend(withAfterReply(ctx, ar), l.evtID, l.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 {
		t.Fatalf("%d own messages before the reply, want none", len(msgs))
	}
	ar.run()
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 1 || msgs[0].GetMessageId() != "net-14" {
		t.Fatalf("%d own messages after the reply, want 1", len(msgs))
	}
	if row, _ := l.st.JournalGet(ctx, lateKey); row == nil || row.State != store.Sent {
		t.Fatalf("journal %+v, want sent", row)
	}

	// A success that resolves nothing because another call finished the
	// send first: the send was kept by that call, and the success reports
	// from the keep.
	l = newLateSend(t, true)
	l.b.finishSend(ctx, l.evtID, l.ps, contract.Outcome_OUTCOME_UNKNOWN, "m.event_too_old", "")
	l.echoed(t, "net-15")
	if l.b.resolveByStatus(l.evtID, l.ps, lateSent) {
		t.Fatal("a finished send finished again")
	}
	l.b.lateSuccess(l.evtID, l.ps.epoch)
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 1 || msgs[0].GetMessageId() != "net-15" {
		t.Fatalf("%d own messages, want 1", len(msgs))
	}
}

// TestLateEchoTimerStopped: the timer that lets a kept send go is stopped
// when the send goes sooner, by a logout (forgetSends) or by its echo, so
// that nothing holds the send past them. WHY: catches a logout that leaves
// every kept send (with its command's content) held for a day.
func TestLateEchoTimerStopped(t *testing.T) {
	timer := func(l *lateSend) *time.Timer {
		l.b.statusMu.Lock()
		defer l.b.statusMu.Unlock()
		if k := l.b.lateEchoes[l.evtID]; k != nil {
			return k.timer
		}
		return nil
	}
	l := newLateSend(t, true)
	l.status(lateTimedOut)
	tm := timer(l)
	if tm == nil {
		t.Fatal("not kept")
	}
	l.b.forgetSends()
	if tm.Stop() {
		t.Fatal("the timer still ran after forgetSends")
	}

	l = newLateSend(t, true)
	l.status(lateTimedOut)
	tm = timer(l)
	l.echoed(t, "net-13")
	l.status(lateSent)
	if tm == nil || tm.Stop() {
		t.Fatal("the timer still ran after the echo")
	}
}

// TestLateEchoNoRow: a success for a kept send whose row is not found
// reports no message, lets the send go, leaves its journal row in flight,
// and says so by a fixed code (late_echo_no_row). WHY: catches the drop
// going unseen in the log, and a message or a journal state made up with
// no row.
func TestLateEchoNoRow(t *testing.T) {
	ctx := context.Background()
	l := newLateSend(t, true)
	l.status(lateTimedOut)
	l.status(lateSent)
	if msgs, _, _ := ownMessages(t, l.st); len(msgs) != 0 || l.kept() {
		t.Fatalf("%d messages, kept %v; want none, let go", len(msgs), l.kept())
	}
	if !strings.Contains(l.logs.String(), `"late_echo_no_row"`) {
		t.Fatalf("log %q: want late_echo_no_row", l.logs.String())
	}
	if row, _ := l.st.JournalGet(ctx, lateKey); row == nil || row.State == store.Sent {
		t.Fatalf("journal %+v, want the row still in flight", row)
	}
}

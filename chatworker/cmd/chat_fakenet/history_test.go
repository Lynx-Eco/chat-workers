// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

// existingHistory is how many messages each chat of the account holds before
// it signs in: more than the default import, so the bound shows.
const existingHistory = 80

// unaskedHistory is the messages of chat in the HistoryBatch events that
// answer no command, in the order delivered, and the batches' event ids.
func unaskedHistory(evs []*contract.ChatEvent, chat string) (msgs []*contract.Message, ids []string) {
	for _, e := range evs {
		hb := e.GetHistoryBatch()
		if hb == nil || hb.GetChatId() != chat || hb.GetCommandId() != "" {
			continue
		}
		msgs = append(msgs, hb.GetMessages()...)
		ids = append(ids, e.GetEventId())
	}
	return msgs, ids
}

// fetches is the fake network's log of history fetches of the chats of
// Script.ExistingHistory ("fetch" records: chat, direction and count).
func fetches(t *testing.T, state string) []fakenet.Record {
	t.Helper()
	recs, err := fakenet.ReadNetworkLog(state)
	if err != nil {
		t.Fatal(err)
	}
	var out []fakenet.Record
	for _, r := range recs {
		if r.Kind == "fetch" {
			out = append(out, r)
		}
	}
	return out
}

// assertImported: of chat the host got exactly the newest want messages
// the network holds, oldest first, each with its own id, time, sender and
// text, in batches that answer no command and are not forward (the host
// stores a chat's first import as history, not as a replay of a gap after
// a disconnect, which later handling may act on), whose ids are the network's
// (history:<chat>:...) and within the batch bound.
func assertImported(t *testing.T, evs []*contract.ChatEvent, chat string, want int) {
	t.Helper()
	msgs, ids := unaskedHistory(evs, chat)
	if len(msgs) != want {
		t.Fatalf("%s: %d messages imported at sign-in, want %d", chat, len(msgs), want)
	}
	for _, id := range ids {
		if !strings.HasPrefix(id, "history:"+chat+":") {
			t.Errorf("%s: an unasked batch has the id %q, want the chat's and its messages' ids", chat, id)
		}
	}
	for _, e := range evs {
		if hb := e.GetHistoryBatch(); hb != nil && hb.GetChatId() == chat &&
			(hb.GetForward() || hb.GetCommandId() != "" || len(hb.GetMessages()) > 100) {
			t.Errorf("%s: batch forward=%v command %q with %d messages, want not forward, no command, at most 100",
				chat, hb.GetForward(), hb.GetCommandId(), len(hb.GetMessages()))
		}
	}
	for k, m := range msgs {
		i := existingHistory - want + k
		sender := fakenet.OldSender(chat, i, fakenet.Me)
		if m.GetMessageId() != fakenet.OldHistoryID(chat, i) || m.GetChatId() != chat ||
			m.GetTimestampMs() != fakenet.OldTime(i).UnixMilli() || m.GetSenderId() != string(sender) ||
			m.GetFromMe() != (sender == fakenet.Me) || m.GetContent().GetText() != fakenet.OldText(chat, i) {
			t.Fatalf("%s: imported message %d is %v, want %s by %s at %v", chat, k, m, fakenet.OldHistoryID(chat, i), sender, fakenet.OldTime(i))
		}
	}
}

// TestHistoryOnSignIn is the test that would have caught an empty Telegram
// sign-in: the worker's backfill was on with no automatic history
// ("history comes only on FetchHistory"), nothing in the engine or the
// product sends FetchHistory, and a real account's chats all landed with no
// message. Here the account already has chats with messages when it signs
// in; the host signs in by steps and sends nothing else, and each chat's
// newest messages arrive by themselves, bounded by the default, in order,
// with their own times and senders. A broadcast channel's are fetched by
// the framework (one request, logged by the network) and dropped by the
// worker, as every event of a channel is. The newest message, which the
// network also sends live after the fetch, arrives once. A restart does
// not fetch or deliver them again.
func TestHistoryOnSignIn(t *testing.T) {
	state := newState(t)
	script := &fakenet.Script{ExistingHistory: existingHistory}
	w := start(t, state, opts{script: script})
	w.signIn()
	evs := w.events()

	for _, chat := range []string{fakenet.OldDM, fakenet.OldGroup} {
		assertImported(t, evs, chat, backend.DefaultInitialMessages)
	}

	// The channel: its ChatInfo, kind CHANNEL, and nothing of its posts.
	sawChan := false
	for _, e := range evs {
		if ci := e.GetChatInfo(); ci != nil && ci.GetChatId() == fakenet.OldChan {
			sawChan = sawChan || ci.GetKind() == contract.ChatKind_CHAT_KIND_CHANNEL
			continue
		}
		if s := fmt.Sprint(e); strings.Contains(s, fakenet.OldMark) || strings.Contains(s, fakenet.OldChan) {
			t.Errorf("an event of the channel was delivered: %v", e)
		}
	}
	if !sawChan {
		t.Errorf("no ChatInfo of kind CHANNEL for %s", fakenet.OldChan)
	}
	// The channel's posts did reach the worker (it has no live post): what
	// kept them from the host is the worker's channel rule, not a fetch that
	// never happened.
	if !strings.Contains(stderrOf(t, w), dropCode) {
		t.Errorf("no %s line: the channel's history never reached the worker, so its absence proves nothing", dropCode)
	}

	// The newest message came in the import and again live: once to the
	// host. The live message after it comes as a Message.
	newest := fakenet.OldHistoryID(fakenet.OldDM, existingHistory-1)
	n := len(messages(evs, fakenet.OldDM, newest))
	for _, e := range evs {
		for _, m := range e.GetHistoryBatch().GetMessages() {
			if m.GetMessageId() == newest {
				n++
			}
		}
	}
	if n != 1 {
		t.Errorf("the newest message reached the host %d times, want once", n)
	}
	if live := messages(evs, fakenet.OldDM, fakenet.OldLiveMsg); len(live) != 1 {
		t.Errorf("the live message after the import: %v", live)
	}

	// One forward fetch of the default size per chat made, the channel's
	// included: that is what a sign-in costs the network.
	got := fetches(t, state)
	if len(got) != 3 {
		t.Fatalf("fetches at sign-in: %v, want one per chat", got)
	}
	for _, r := range got {
		if r.Target != fmt.Sprintf("forward:%d", backend.DefaultInitialMessages) {
			t.Errorf("fetch %v, want forward:%d", r, backend.DefaultInitialMessages)
		}
	}

	// A restart: the rooms exist, so nothing is fetched, and no history
	// event lands that the host did not have (an unacknowledged one may be
	// sent again under its own id, which the host takes for a repeat).
	seen := map[string]bool{}
	for _, e := range evs {
		seen[e.GetEventId()] = true
	}
	w.shutdown()
	w = start(t, state, opts{script: script})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	ctx, cancel := w.ctx(3 * time.Second)
	defer cancel()
	w.s.WaitQuiet(ctx, 500*time.Millisecond)
	for _, e := range w.events() {
		if e.GetHistoryBatch() != nil && !seen[e.GetEventId()] {
			t.Errorf("a restart delivered history again: %v", e)
		}
	}
	if again := fetches(t, state); len(again) != len(got) {
		t.Errorf("a restart fetched history again: %v", again[len(got):])
	}
	w.shutdown()
}

// TestHistoryOnSignInSetting: the host bounds the import per chat with
// max_initial_messages, and 0 turns it off (the framework then asks the
// network for nothing); a value out of bounds refuses the start, so a typo
// neither silently turns history off nor floods the network.
func TestHistoryOnSignInSetting(t *testing.T) {
	for _, want := range []int{7, 0} {
		t.Run(fmt.Sprint(want), func(t *testing.T) {
			state := newState(t)
			w := start(t, state, opts{
				script:   &fakenet.Script{ExistingHistory: existingHistory},
				settings: map[string]string{backend.SettingInitialMessages: fmt.Sprint(want)},
			})
			w.signIn()
			evs := w.events()
			for _, chat := range []string{fakenet.OldDM, fakenet.OldGroup} {
				assertImported(t, evs, chat, want)
			}
			if got := fetches(t, state); (want == 0 && len(got) != 0) || (want > 0 && len(got) != 3) {
				t.Errorf("fetches with %d: %v", want, got)
			}
			// Without the import the newest message comes live, once.
			if want == 0 && len(messages(evs, fakenet.OldDM, fakenet.OldHistoryID(fakenet.OldDM, existingHistory-1))) != 1 {
				t.Errorf("with the import off the live message did not come")
			}
			w.shutdown()
		})
	}
	w := start(t, newState(t), opts{settings: map[string]string{backend.SettingInitialMessages: "501"}})
	w.wait("ERROR bad_settings", func(e *contract.ChatEvent) bool {
		cs := e.GetConnectionState()
		return cs.GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR && cs.GetCode() == "bad_settings"
	})
}

// TestHistoryCatchUp: a chat the worker already has, whose network then
// holds messages the update stream did not replay (a reconnect after a gap:
// mautrix-telegram's channel gap too long to replay resyncs the chat), is
// caught up by the framework from the newest message it stored, up to
// max_catchup_messages, with no command from the host. Those batches are
// forward, the one class the host stores as a replay of a gap; the same
// chat's first import at sign-in was not.
func TestHistoryCatchUp(t *testing.T) {
	const gap = 5
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{ExistingHistory: existingHistory}})
	w.signIn()
	assertImported(t, w.events(), fakenet.OldGroup, backend.DefaultInitialMessages)
	w.shutdown()
	before := len(fetches(t, state))

	w = start(t, state, opts{script: &fakenet.Script{ExistingHistory: existingHistory, HistoryGap: gap}})
	last := fakenet.OldHistoryID(fakenet.OldGroup, existingHistory+gap-1)
	w.wait("the caught-up gap", func(e *contract.ChatEvent) bool {
		ms := e.GetHistoryBatch().GetMessages()
		return len(ms) > 0 && ms[len(ms)-1].GetMessageId() == last
	})
	var got []*contract.Message
	for _, e := range w.events() {
		hb := e.GetHistoryBatch()
		if hb == nil || hb.GetChatId() != fakenet.OldGroup {
			continue
		}
		if !hb.GetForward() || hb.GetCommandId() != "" || !strings.HasPrefix(e.GetEventId(), "history:"+fakenet.OldGroup+":") {
			t.Errorf("a catch-up batch: forward=%v command %q id %q, want forward, no command, the network's id",
				hb.GetForward(), hb.GetCommandId(), e.GetEventId())
		}
		got = append(got, hb.GetMessages()...)
	}
	if len(got) != gap {
		t.Fatalf("%d messages caught up, want the %d of the gap: %v", len(got), gap, got)
	}
	for k, m := range got {
		i := existingHistory + k
		if m.GetMessageId() != fakenet.OldHistoryID(fakenet.OldGroup, i) || m.GetTimestampMs() != fakenet.OldTime(i).UnixMilli() ||
			m.GetSenderId() != string(fakenet.OldSender(fakenet.OldGroup, i, fakenet.Me)) {
			t.Fatalf("caught-up message %d is %v, want %s", k, m, fakenet.OldHistoryID(fakenet.OldGroup, i))
		}
	}
	recs := fetches(t, state)[before:]
	if len(recs) != 1 || recs[0].Chat != fakenet.OldGroup || recs[0].Target != fmt.Sprintf("forward:%d", backend.DefaultCatchupMessages) {
		t.Errorf("fetches of the reconnect: %v, want one forward:%d of %s", recs, backend.DefaultCatchupMessages, fakenet.OldGroup)
	}
	w.shutdown()
}

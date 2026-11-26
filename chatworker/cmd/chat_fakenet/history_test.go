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

// fetchGroup is a FetchHistory of count messages of the group from cursor
// (empty: its newest), with the deadline given (zero: none).
func fetchGroup(count uint32, cursor string, deadline time.Time) *contract.Command {
	cmd := &contract.Command{Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: fakenet.Group, Count: count, Cursor: cursor}}}
	if !deadline.IsZero() {
		cmd.DeadlineMs = deadline.UnixMilli()
	}
	return cmd
}

// expectPage: r answered cmd OK, and cmd's page of want messages arrived
// as its HistoryBatch.
func (w *worker) expectPage(cmd *contract.Command, r *contract.CommandReply, want int) {
	w.t.Helper()
	expect(w.t, r, contract.Outcome_OUTCOME_OK, "")
	batch := w.wait("the page of "+cmd.GetCommandId(), func(e *contract.ChatEvent) bool {
		return e.GetHistoryBatch().GetCommandId() == cmd.GetCommandId()
	}).GetHistoryBatch()
	if len(batch.GetMessages()) != want {
		w.t.Fatalf("the page of %s has %d messages, want %d", cmd.GetCommandId(), len(batch.GetMessages()), want)
	}
}

// expectRateLimited: r is rate_limited with the wait retryAfter, answered
// within took of the command (not after the network's wait), and no page
// of cmd arrived.
func (w *worker) expectRateLimited(cmd *contract.Command, r *contract.CommandReply, retryAfter uint32, took, within time.Duration) {
	w.t.Helper()
	expect(w.t, r, contract.Outcome_OUTCOME_REJECTED, "rate_limited")
	if r.GetRetryAfterS() != retryAfter {
		w.t.Errorf("%s: retry_after_s %d, want %d", cmd.GetCommandId(), r.GetRetryAfterS(), retryAfter)
	}
	if took > within {
		w.t.Errorf("%s: rate_limited after %v, want within %v (the wait was slept out)", cmd.GetCommandId(), took, within)
	}
	for _, e := range w.events() {
		if e.GetHistoryBatch().GetCommandId() == cmd.GetCommandId() {
			w.t.Errorf("%s: a page arrived for a rate-limited fetch: %v", cmd.GetCommandId(), e)
		}
	}
}

// TestHistoryRateLimited: a history fetch the network rate-limits is
// answered OUTCOME_REJECTED rate_limited with the wait (retry_after_s: its
// seconds rounded up, and one more), at once, not after sleeping the wait
// out inside the worker, which held the worker's history lock (and every
// other fetch) for minutes before, or failing as a bare fetch_failed:
//   - a wait the connector does not sleep out (Telegram: 5 minutes or more)
//     comes back as its error;
//   - a wait the connector is about to sleep out but that is longer than
//     the worker lets it (maxInWorkerRateWait, 15 s) ends the fetch at once;
//   - so does a short one that would end after the command's deadline;
//   - a short one within the deadline is slept out, and the page is served.
//
// After each, the lock is free: the next fetch reaches the network, and the
// last is served.
func TestHistoryRateLimited(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{HistoryRateLimits: []fakenet.RateLimit{
		{WaitMs: 600_000},
		{WaitMs: 60_000, Sleep: true},
		{WaitMs: 3_000, Sleep: true},
		{WaitMs: 300, Sleep: true},
	}}})
	w.signIn()

	// Each rate-limited fetch, then a fetch that is served.
	for _, c := range []struct {
		name       string
		deadline   time.Duration
		retryAfter uint32
	}{
		{"not slept out", 0, 601},
		{"too long to sleep out", 0, 61},
		{"would end after the deadline", 2 * time.Second, 4},
	} {
		var deadline time.Time
		if c.deadline > 0 {
			deadline = time.Now().Add(c.deadline)
		}
		cmd := fetchGroup(10, "", deadline)
		started := time.Now()
		r := w.command(cmd)
		w.expectRateLimited(cmd, r, c.retryAfter, time.Since(started), time.Second)
		if t.Failed() {
			t.Fatalf("%s: %v", c.name, r)
		}
	}
	// A short wait within the deadline: slept out, and the page served.
	cmd := fetchGroup(10, "", time.Now().Add(20*time.Second))
	started := time.Now()
	r := w.command(cmd)
	w.expectPage(cmd, r, 10)
	if took := time.Since(started); took < 300*time.Millisecond {
		t.Errorf("the short wait was not slept out: answered after %v", took)
	}
	// The lock is free after all of them: the next page is served.
	cmd = fetchGroup(5, r.GetCursor(), time.Time{})
	w.expectPage(cmd, w.command(cmd), 5)

	// Each fetch reached the network (none was refused worker_busy: the
	// lock was free after the one before), and each wait was met once.
	var waits []string
	recs, err := fakenet.ReadNetworkLog(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.Kind == "rate_limited" {
			waits = append(waits, r.Target)
		}
	}
	if fmt.Sprint(waits) != "[600000 60000 3000 300]" {
		t.Errorf("the network rate-limited %v, want each entry once, in order", waits)
	}
	if log := stderrOf(t, w); !strings.Contains(log, "history_rate_limited") {
		t.Errorf("no history_rate_limited line in the worker's log")
	}
	w.shutdown()
}

// TestHistoryTimedOutAndBusy: a network that does not answer holds a fetch
// only until shortly before the command's deadline: the worker ends the
// request and answers fetch_timed_out (OUTCOME_REJECTED, nothing bridged)
// while the host still waits, where it went on fetching after the host's
// deadline before. And one fetch runs at a time: another that comes while
// one runs (one whose host gave up on it included) is refused worker_busy
// at once, where it waited on the history lock, possibly past its own
// deadline, and then fetched for a host that was no longer waiting.
func TestHistoryTimedOutAndBusy(t *testing.T) {
	const hang = 3 * time.Second
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{HistoryHangMs: int(hang / time.Millisecond)}})
	w.signIn()

	deadline := time.Now().Add(1500 * time.Millisecond)
	cmd := fetchGroup(10, "", deadline)
	r := w.command(cmd)
	answered := time.Now()
	expect(t, r, contract.Outcome_OUTCOME_REJECTED, "fetch_timed_out")
	if !answered.Before(deadline) {
		t.Errorf("fetch_timed_out after the deadline (%v late)", answered.Sub(deadline))
	}
	if r.GetRetryAfterS() != 0 {
		t.Errorf("fetch_timed_out with retry_after_s %d", r.GetRetryAfterS())
	}

	slow := fetchGroup(10, "", time.Time{})
	done := make(chan *contract.CommandReply, 1)
	go func() {
		ctx, cancel := w.ctx(30 * time.Second)
		defer cancel()
		r, err := w.s.Command(ctx, slow)
		if err != nil {
			t.Errorf("the slow fetch: %v", err)
		}
		done <- r
	}()
	time.Sleep(300 * time.Millisecond)
	busy := fetchGroup(10, "", time.Time{})
	started := time.Now()
	r = w.command(busy)
	expect(t, r, contract.Outcome_OUTCOME_REJECTED, "worker_busy")
	if took := time.Since(started); took > time.Second {
		t.Errorf("worker_busy after %v, want at once", took)
	}
	select {
	case r := <-done:
		t.Fatalf("the slow fetch was answered before its network did: %v", r)
	default:
	}
	r = <-done
	w.expectPage(slow, r, 10)
	// Answered, it gave the lock back: the next page is served.
	cmd = fetchGroup(5, r.GetCursor(), time.Time{})
	w.expectPage(cmd, w.command(cmd), 5)
	w.shutdown()
}

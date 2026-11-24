// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"fmt"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

func TestChatRemoved(t *testing.T) {
	w := start(t, newState(t), opts{})
	w.signIn()
	w.wait("chat removed", func(e *contract.ChatEvent) bool { return e.GetChatRemoved().GetChatId() == fakenet.DMCarol })
	w.shutdown()
}

func TestNetworkEndsSession(t *testing.T) {
	for _, c := range []struct {
		knob  string
		state contract.ConnectionStateKind
		code  string
	}{
		{"bad_credentials", contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER, "fake-bad-credentials"},
		{"logged_out", contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, "fake-logged-out"},
	} {
		t.Run(c.knob, func(t *testing.T) {
			w := start(t, newState(t), opts{script: &fakenet.Script{AfterSync: c.knob}})
			w.signIn()
			w.wait(c.knob, func(e *contract.ChatEvent) bool {
				cs := e.GetConnectionState()
				return cs.GetState() == c.state && cs.GetCode() == c.code && cs.GetPrincipal() == string(fakenet.Me)
			})
			w.shutdown()
		})
	}
}

// tickRun signs in on a fresh state, then restarts with the scripted ticker
// of n messages while the host stops reading for pause. It returns when the
// worker started and when the last tick was stored (its emitted_at).
func tickRun(t *testing.T, n int, pause time.Duration) (began, lastStored time.Time) {
	t.Helper()
	state := newState(t)
	// dev_plain_store lets the same test time the -tags plainstore build;
	// the encrypted build ignores it.
	dev := map[string]string{"dev_plain_store": "1"}
	w := start(t, state, opts{settings: dev})
	w.signIn()
	w.shutdown()
	script := &fakenet.Script{}
	script.Ticker.Count, script.Ticker.IntervalMs, script.Ticker.DelayMs, script.Ticker.PadBytes = n, 1, 300, 1000
	w2 := start(t, state, opts{script: script, settings: dev})
	began = time.Now()
	if pause > 0 {
		w2.s.PauseReading(pause)
	}
	last := fmt.Sprintf("t%03d", n-1)
	w2.wait("last tick", func(e *contract.ChatEvent) bool { return e.GetMessage().GetMessageId() == last })
	got := map[string]int64{}
	for _, e := range w2.events() {
		if m := e.GetMessage(); m != nil && len(m.GetMessageId()) == 4 && m.GetMessageId()[0] == 't' {
			got[m.GetMessageId()] = e.GetEmittedAtMs()
		}
	}
	if len(got) != n {
		t.Fatalf("%d of %d ticks arrived", len(got), n)
	}
	w2.shutdown()
	return began, time.UnixMilli(got[last])
}

// TestSlowHostDoesNotStallNetwork: while the host does not read, the
// network keeps delivering in both direct chats (every message is stored
// upward while the host is paused) and nothing is lost. The pause is at
// least 3 s and at least twice what this host takes to store the ticks
// when the host reads at once (measured first), so that a loaded machine
// does not fail it for being slow rather than for waiting on the host.
func TestSlowHostDoesNotStallNetwork(t *testing.T) {
	const n = 600
	began, stored := tickRun(t, n, 0)
	took := stored.Sub(began)
	pause := max(3*time.Second, 2*took+500*time.Millisecond)
	paused, stored := tickRun(t, n, pause)
	resumed := paused.Add(pause)
	if !stored.Before(resumed) {
		t.Fatalf("the network was held up by the slow host: last tick stored %v after the host resumed (pause %v, unpaused run %v)", stored.Sub(resumed), pause, took)
	}
	t.Logf("unpaused run stored the ticks in %v; with the host paused %v, the last tick was stored %v before it resumed", took, pause, resumed.Sub(stored))
}

// TestMeasureRSS logs the worker's resident memory idle after the sync and
// after importing the group's 120 older messages, and the import's wall time
// from the first FetchHistory to the last batch received (numbers for the
// report). With -tags plainstore it runs the development plain store
// (dev_plain_store is ignored by the encrypted build).
func TestMeasureRSS(t *testing.T) {
	w := start(t, newState(t), opts{settings: map[string]string{"dev_plain_store": "1"}})
	w.signIn()
	time.Sleep(time.Second)
	if rss, err := w.proc.RSSKiB(); err == nil {
		t.Logf("chat_fakenet VmRSS idle after sync: %d KiB (store encrypted: %v)", rss, w.hello.GetStoreEncrypted())
	}
	began := time.Now()
	cursor := ""
	got := 0
	for {
		cmd := &contract.Command{Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: fakenet.Group, Cursor: cursor, Count: 50}}}
		r := w.command(cmd)
		if r.GetOutcome() != contract.Outcome_OUTCOME_OK {
			t.Fatalf("history: %v", r)
		}
		batch := w.wait("history batch", func(e *contract.ChatEvent) bool {
			return e.GetHistoryBatch().GetCommandId() == cmd.GetCommandId()
		}).GetHistoryBatch()
		got += len(batch.GetMessages())
		if !r.GetHasMore() {
			break
		}
		cursor = r.GetCursor()
	}
	took := time.Since(began)
	if got != fakenet.HistoryCount {
		t.Fatalf("imported %d messages", got)
	}
	t.Logf("history import of %d messages: %v (store encrypted: %v)", got, took.Round(time.Millisecond), w.hello.GetStoreEncrypted())
	time.Sleep(time.Second)
	if rss, err := w.proc.RSSKiB(); err == nil {
		t.Logf("chat_fakenet VmRSS after the 120-message import: %d KiB", rss)
	}
	w.shutdown()
}

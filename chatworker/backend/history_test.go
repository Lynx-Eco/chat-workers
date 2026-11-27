// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"lynx.invalid/ingest/chatworker/contract"
)

// TestHistoryBatchIDCarriesCommand: the host lands an event id once. Two
// FetchHistory commands that get the same page (the host asks for it
// again, the worker's store was lost and the framework bridges it again)
// are two events, each naming its command, so each has an id of its own:
// the second is not taken for a repeat of the first, and its command gets
// its page. The messages in both keep the network's ids. The same command's
// page emitted again (a replay) has the same id, and a page the framework
// sends unasked has the id of its chat and messages alone, the same every
// time. Before the fix every page of the same messages had one id.
func TestHistoryBatchIDCarriesCommand(t *testing.T) {
	ctx := context.Background()
	b, st, _, bdb := frameworkBackend(t)
	fw := b.newInstance()
	roomID := madeRoom(t, fw, bdb, oldKey5, "Group", formatGhost("a"))
	room, err := st.Room(ctx, string(roomID))
	if err != nil || room == nil {
		t.Fatalf("the room's row %+v %v", room, err)
	}
	page := []*contract.Message{
		{ChatId: "chat:5", MessageId: "5.1", SenderId: "a"},
		{ChatId: "chat:5", MessageId: "5.2", SenderId: "a"},
	}
	// emitted emits the page for hc and is its one HistoryBatch event.
	emitted := func(hc *historyCtx) *contract.ChatEvent {
		t.Helper()
		n := len(outboxEvents(t, st))
		fw.emitBatches(room, page, hc, true)
		var got []*contract.ChatEvent
		for _, e := range outboxEvents(t, st)[n:] {
			if e.GetHistoryBatch() != nil {
				got = append(got, e)
			}
		}
		if len(got) != 1 {
			t.Fatalf("%d history batches for one page, want 1", len(got))
		}
		return got[0]
	}
	first := emitted(&historyCtx{commandID: "cmd-1"})
	replay := emitted(&historyCtx{commandID: "cmd-1"})
	second := emitted(&historyCtx{commandID: "cmd-2"})
	unasked := emitted(nil)
	unaskedAgain := emitted(nil)

	if first.GetEventId() == second.GetEventId() {
		t.Errorf("two commands' pages share the id %q: the second is landed as a repeat", first.GetEventId())
	}
	if first.GetEventId() != replay.GetEventId() {
		t.Errorf("one command's page has the ids %q and %q", first.GetEventId(), replay.GetEventId())
	}
	if unasked.GetEventId() != unaskedAgain.GetEventId() || strings.Contains(unasked.GetEventId(), "cmd-") {
		t.Errorf("an unasked page's ids %q, %q: want the network's, the same each time", unasked.GetEventId(), unaskedAgain.GetEventId())
	}
	// An unasked page carries the forward flag it is given (here a
	// catch-up's, unaskedForward) and no command.
	if hb := unasked.GetHistoryBatch(); !hb.GetForward() || hb.GetCommandId() != "" {
		t.Errorf("an unasked page: forward %v, command %q; want forward, no command", hb.GetForward(), hb.GetCommandId())
	}
	for _, id := range []string{first.GetEventId(), second.GetEventId()} {
		if id == unasked.GetEventId() {
			t.Errorf("a command's page has the unasked page's id %q", id)
		}
	}
	for _, e := range []*contract.ChatEvent{first, second} {
		hb := e.GetHistoryBatch()
		if !strings.Contains(e.GetEventId(), hb.GetCommandId()) {
			t.Errorf("the page of %s has the id %q", hb.GetCommandId(), e.GetEventId())
		}
		if len(hb.GetMessages()) != 2 || hb.GetMessages()[0].GetMessageId() != "5.1" || hb.GetMessages()[1].GetMessageId() != "5.2" {
			t.Errorf("the page of %s: %v, want the messages with their own ids", hb.GetCommandId(), hb.GetMessages())
		}
	}
}

// TestUnaskedForward: the host stores a batch's forward flag as how its
// messages arrived (forward: a replay of a gap after a disconnect, which
// later handling may act on; not forward: history). The framework's
// request is forward both for the initial import of a chat whose room it
// just made and for a catch-up; only a catch-up, where the chat held
// messages before the batch, is forward. A backward page never is.
func TestUnaskedForward(t *testing.T) {
	for _, c := range []struct{ reqForward, held, want bool }{
		{true, false, false}, // the initial import of a new room
		{true, true, true},   // a catch-up
		{false, false, false},
		{false, true, false},
	} {
		if got := unaskedForward(c.reqForward, c.held); got != c.want {
			t.Errorf("unaskedForward(%v, %v) = %v, want %v", c.reqForward, c.held, got, c.want)
		}
	}
}

// TestFetchBound: a fetch ends before the command's deadline, with room for
// the answer to reach the host (2 s, or a quarter of what is left when that
// is less), never at an instant already past; a command with no deadline is
// bounded all the same. Mutants: the margin dropped (the answer comes after
// the host gave up); the margin taken whole from a short deadline (a 1 s
// command is ended before it starts); no bound without a deadline.
func TestFetchBound(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	for _, c := range []struct {
		deadline time.Time
		want     time.Time
		ok       bool
	}{
		{time.Time{}, now.Add(historyFetchBound), true},
		{now.Add(120 * time.Second), now.Add(118 * time.Second), true},
		{now.Add(8 * time.Second), now.Add(6 * time.Second), true},
		{now.Add(time.Second), now.Add(750 * time.Millisecond), true},
		{now, time.Time{}, false},
		{now.Add(-time.Second), time.Time{}, false},
	} {
		got, ok := fetchBound(now, c.deadline)
		if ok != c.ok || !got.Equal(c.want) {
			t.Errorf("fetchBound(deadline %v) = %v, %v; want %v, %v", c.deadline.Sub(now), got.Sub(now), ok, c.want.Sub(now), c.ok)
		}
	}
}

// TestRateWatch: the watch lets the connector sleep out a wait of at most
// maxInWorkerRateWait that ends (with the connector's second more) before
// the fetch's bound, and ends the fetch at once for any other, keeping the
// first such wait; after end, a wait changes nothing. Mutants: >= for > at
// the 15 s bound; the connector's extra second not counted; a later wait
// overwriting the one that ended the fetch; a wait after end cutting.
func TestRateWatch(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	watch := func(bound time.Duration) (*rateWatch, *bool) {
		cancelled := new(bool)
		return &rateWatch{bound: now.Add(bound), cancel: func() { *cancelled = true }, now: func() time.Time { return now }}, cancelled
	}
	for _, c := range []struct {
		wait, bound time.Duration
		cut         bool
	}{
		{maxInWorkerRateWait, time.Minute, false},
		{maxInWorkerRateWait + time.Millisecond, time.Minute, true},
		{3 * time.Second, 4 * time.Second, false},
		{3 * time.Second, 4*time.Second - time.Millisecond, true},
		{0, time.Minute, false},
	} {
		w, cancelled := watch(c.bound)
		w.onWait(c.wait)
		if *cancelled != c.cut {
			t.Errorf("wait %v, bound %v: cancelled %v, want %v", c.wait, c.bound, *cancelled, c.cut)
		}
		got := w.end()
		if got.cut != c.cut || (c.cut && got.wait != c.wait) {
			t.Errorf("wait %v, bound %v: end %+v", c.wait, c.bound, got)
		}
	}
	w, _ := watch(time.Minute)
	w.onWait(time.Minute)
	w.onWait(2 * time.Minute)
	if got := w.end(); !got.cut || got.wait != time.Minute {
		t.Errorf("two waits: %+v, want the first, which ended the fetch", got)
	}
	w, cancelled := watch(time.Minute)
	w.end()
	*cancelled = false
	w.onWait(time.Hour)
	if *cancelled || w.cut {
		t.Errorf("a wait after end cut the fetch")
	}
}

// TestFetchFailure: what a failed fetch is answered: rate_limited with the
// wait when a wait ended it or its error is the network's rate limit (a
// wait the network did not name: rate_limited with none), fetch_timed_out
// when the worker's own bound ended it, fetch_failed otherwise; the wait in
// whole seconds rounded up, and one more. Mutants: the cut ignored; the
// error's wait not read; a timeout taken for a rate limit or the other way
// round; the wait rounded down.
func TestFetchFailure(t *testing.T) {
	limited := errors.New("limited")
	rl := func(err error) (time.Duration, bool) {
		if errors.Is(err, limited) {
			return 600 * time.Second, true
		}
		if err.Error() == "unnamed" {
			return 0, true
		}
		return 0, false
	}
	other := errors.New("other")
	for _, c := range []struct {
		err      error
		cut      rateCut
		rl       func(error) (time.Duration, bool)
		timedOut bool
		code     string
		wait     uint32
	}{
		{context.Canceled, rateCut{cut: true, wait: 60 * time.Second}, rl, false, "rate_limited", 61},
		{context.DeadlineExceeded, rateCut{cut: true, wait: 2500 * time.Millisecond}, rl, true, "rate_limited", 4},
		{fmt.Errorf("x: %w", limited), rateCut{}, rl, false, "rate_limited", 601},
		{errors.New("unnamed"), rateCut{}, rl, false, "rate_limited", 0},
		{context.DeadlineExceeded, rateCut{}, rl, true, "fetch_timed_out", 0},
		{other, rateCut{}, rl, false, "fetch_failed", 0},
		{limited, rateCut{}, nil, false, "fetch_failed", 0},
	} {
		code, wait := fetchFailure(c.err, c.cut, c.rl, c.timedOut)
		if code != c.code || wait != c.wait {
			t.Errorf("fetchFailure(%v, %+v, timedOut %v) = %q, %d; want %q, %d", c.err, c.cut, c.timedOut, code, wait, c.code, c.wait)
		}
	}
	for _, c := range []struct {
		wait time.Duration
		want uint32
	}{
		{0, 1}, {time.Second, 2}, {time.Second + time.Nanosecond, 3}, {-time.Second, 1},
		{time.Duration(math.MaxInt64), math.MaxUint32},
	} {
		if got := retryAfter(c.wait); got != c.want {
			t.Errorf("retryAfter(%v) = %d, want %d", c.wait, got, c.want)
		}
	}
}

// TestWatchLogWaits: the watch reads the wait of a warning with the given
// message, as zerolog encodes a duration, from the logger the context
// carries, and nothing else (another message, an info line, a line with no
// such field). Mutants: the message not compared; the duration unit not
// applied; the level not bounded (an info line counted).
func TestWatchLogWaits(t *testing.T) {
	var got []time.Duration
	ctx := WatchLogWaits(context.Background(), "Waiting on flood wait", "duration", func(d time.Duration) { got = append(got, d) })
	log := zerolog.Ctx(ctx)
	log.Warn().Dur("duration", 1500*time.Millisecond).Msg("Waiting on flood wait")
	log.Warn().Dur("duration", time.Hour).Msg("Something else")
	log.Info().Dur("duration", time.Hour).Msg("Waiting on flood wait")
	log.Warn().Msg("Waiting on flood wait")
	sub := log.With().Str("method", "FetchMessages").Logger()
	sub.Error().Dur("duration", 3*time.Second).Msg("Waiting on flood wait")
	if fmt.Sprint(got) != "[1.5s 3s]" {
		t.Errorf("waits read %v, want [1.5s 3s]", got)
	}
	if _, ok := parseWaitLine([]byte(`{"level":"warn","duration":-5,"message":"Waiting on flood wait"}`), "Waiting on flood wait", "duration"); ok {
		t.Errorf("a negative wait was read")
	}
}

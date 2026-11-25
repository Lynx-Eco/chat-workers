// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"strings"
	"testing"

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
		fw.emitBatches(room, page, hc)
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

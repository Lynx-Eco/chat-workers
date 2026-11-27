// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"fmt"
	"strings"
	"testing"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

const dropCode = `"code":"channel_event_dropped"`

// renamed is the group's rename, which the sync plays after the channels.
func renamed(e *contract.ChatEvent) bool { return e.GetChatInfo().GetName() == "Renamed club" }

// afterMsg is the group's message every later connect sends after the
// channels' posts (fakenet.AfterMsg).
func afterMsg(e *contract.ChatEvent) bool {
	return e.GetMessage().GetChatId() == fakenet.Group && strings.HasPrefix(e.GetMessage().GetMessageId(), fakenet.AfterMsg+"-")
}

// upTo is the delivered events up to the first that matches marker, which
// the network sends after the events under test: the framework handles
// events one at a time (backend.BridgeConfig) and the outbox keeps their
// order, so what those events gave is in the list. It fails when the marker
// has not come.
func upTo(t *testing.T, w *worker, marker func(*contract.ChatEvent) bool) []*contract.ChatEvent {
	t.Helper()
	evs := w.events()
	for i, e := range evs {
		if marker(e) {
			return evs[:i]
		}
	}
	t.Fatal("the marker event was not delivered")
	return nil
}

// assertChannelSilent: of the channel only its ChatInfo (kind CHANNEL) was
// delivered before the marker: no message, reaction, abilities, history,
// nor the PersonInfo of its poster or of its own ghost, who are met
// nowhere else. It returns whether the ChatInfo came.
func assertChannelSilent(t *testing.T, w *worker, marker func(*contract.ChatEvent) bool) bool {
	t.Helper()
	info := false
	for _, e := range upTo(t, w, marker) {
		if ci := e.GetChatInfo(); ci != nil && ci.GetChatId() == fakenet.Chan {
			if ci.GetKind() != contract.ChatKind_CHAT_KIND_CHANNEL {
				t.Errorf("the channel's info has kind %v", ci.GetKind())
			}
			info = true
			continue
		}
		s := fmt.Sprint(e)
		if strings.Contains(s, fakenet.Chan) || strings.Contains(s, fakenet.ChanMark) ||
			strings.Contains(s, string(fakenet.ChanPoster)) || strings.Contains(s, string(fakenet.ChanGhost)) {
			t.Errorf("an event of the channel was delivered: %v", e)
		}
	}
	return info
}

// TestChannelNotSent: a broadcast channel (a space room, CHAT_KIND_CHANNEL)
// is reported by its ChatInfo and nothing else: none of its posts, its
// reaction, its uploaded attachment (no spool row), its poster's
// PersonInfo; the group beside it is reported in full. Commands that target
// it are refused. After a restart a new post in it is dropped too. Each
// run logs the drops once for the room.
func TestChannelNotSent(t *testing.T) {
	state := newState(t)
	script := &fakenet.Script{Channel: true}
	w := start(t, state, opts{script: script})
	w.signIn()
	if !assertChannelSilent(t, w, renamed) {
		t.Fatal("the channel's ChatInfo was not delivered")
	}
	evs := w.events()
	if m3 := messages(evs, fakenet.Group, "m3"); len(m3) != 1 {
		t.Errorf("the group beside the channel: m3 %v", m3)
	}
	group := false
	for _, e := range evs {
		group = group || (e.GetChatInfo().GetChatId() == fakenet.Group && e.GetChatInfo().GetKind() == contract.ChatKind_CHAT_KIND_GROUP)
	}
	if !group {
		t.Error("the group's info was not delivered")
	}
	for _, c := range []*contract.Command{
		sendCmd(fakenet.Chan, "chan-send", "hello channel", 0),
		{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.Chan, "cp1", ""), SendKey: "chan-react", Emoji: "👍"}}},
		{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: fakenet.Chan, UpToMessageId: "cp1"}}},
		{Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: fakenet.Chan}}},
	} {
		expect(t, w.command(c), contract.Outcome_OUTCOME_REJECTED, "channel_not_ingested")
	}
	w.shutdown()
	if n := strings.Count(stderrOf(t, w), dropCode); n != 1 {
		t.Errorf("%d lines %s in the first run, want 1 (one room)", n, dropCode)
	}
	for _, row := range storeSnapshot(t, state)["lynx_spool"] {
		if strings.Contains(row, fakenet.ChanMark) {
			t.Errorf("the spool keeps the channel's attachment: %s", row)
		}
	}
	recs, _ := fakenet.ReadNetworkLog(state)
	for _, r := range recs {
		if r.Chat == fakenet.Chan {
			t.Errorf("a command reached the network for the channel: %v", r)
		}
	}

	w = start(t, state, opts{script: script})
	w.wait("the group's message after the channel's post", afterMsg)
	assertChannelSilent(t, w, afterMsg)
	w.shutdown()
	if n := strings.Count(stderrOf(t, w), dropCode); n != 2 {
		t.Errorf("%d lines %s in the two runs, want 2 (one room each)", n, dropCode)
	}
}

// TestChannelPeopleWithoutEvent: people whose profiles come without any
// event (as mautrix-telegram updates every user of a network update, and a
// channel's own ghost) are reported only once they are a member or a
// sender in a room that is sent: the channel's own ghost, which posts in
// it, and the channel's poster never are; Frank, met only in the group,
// is reported, with his stored name, before his first message there.
func TestChannelPeopleWithoutEvent(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{Channel: true, GhostUpdates: true}})
	w.signIn()
	if !assertChannelSilent(t, w, renamed) {
		t.Fatal("the channel's ChatInfo was not delivered")
	}
	info, msg := -1, -1
	for i, e := range upTo(t, w, renamed) {
		if p := e.GetPersonInfo(); p != nil && p.GetPersonId() == string(fakenet.Frank) {
			if info < 0 {
				info = i
			}
			if p.GetName() != "Frank Example" {
				t.Errorf("Frank's PersonInfo has name %q", p.GetName())
			}
		}
		if m := e.GetMessage(); m != nil && m.GetChatId() == fakenet.Group && m.GetMessageId() == fakenet.FrankMsg {
			msg = i
		}
	}
	if msg < 0 {
		t.Fatal("Frank's message in the group was not delivered")
	}
	if info < 0 || info > msg {
		t.Errorf("Frank's PersonInfo at %d, his message at %d: want it first", info, msg)
	}
	w.shutdown()
}

// TestChannelTurned: a room of the default type is a group until the
// connector's stored metadata says broadcast (Hooks.IsChannel), then a
// channel. Its first ChatInfo is GROUP and its post then is delivered; once
// the metadata says broadcast, its next event brings a ChatInfo with kind
// CHANNEL (nothing else changed in the room), and its posts and reaction
// are dropped; commands on it are refused. A person first met in it
// (TurnPoster) is not reported there; their first message in the group
// reports their PersonInfo, with the stored name, before it. The group
// beside it is unaffected. After a restart the room stays a channel; when
// its metadata says it is not one any more (at a later start, before the
// new instance looked at the room), its next event brings a GROUP ChatInfo
// and is sent.
func TestChannelTurned(t *testing.T) {
	state := newState(t)
	script := &fakenet.Script{TurnedChannel: true}
	w := start(t, state, opts{script: script})
	w.signIn()
	erinSent := func(e *contract.ChatEvent) bool {
		return e.GetMessage().GetChatId() == fakenet.Group && e.GetMessage().GetMessageId() == fakenet.TurnGroupMsg
	}
	w.wait("the erin message", erinSent)
	evs := upTo(t, w, erinSent)
	var kinds []contract.ChatKind
	before, erinInfo := -1, -1
	for i, e := range evs {
		if ci := e.GetChatInfo(); ci != nil && ci.GetChatId() == fakenet.TurnChan {
			kinds = append(kinds, ci.GetKind())
		}
		if m := e.GetMessage(); m != nil && m.GetChatId() == fakenet.TurnChan && m.GetMessageId() == fakenet.TurnBefore {
			before = i
		}
		if p := e.GetPersonInfo(); p != nil && p.GetPersonId() == string(fakenet.TurnPoster) {
			if erinInfo < 0 {
				erinInfo = i
			}
			if p.GetName() != "Erin Example" {
				t.Errorf("the poster's PersonInfo has name %q", p.GetName())
			}
		}
		s := fmt.Sprint(e)
		if strings.Contains(s, fakenet.TurnMark) || (e.GetReaction() != nil && e.GetReaction().GetTarget().GetChatId() == fakenet.TurnChan) {
			t.Errorf("an event of the channel was delivered: %v", e)
		}
	}
	if len(kinds) < 2 || kinds[0] != contract.ChatKind_CHAT_KIND_GROUP || kinds[len(kinds)-1] != contract.ChatKind_CHAT_KIND_CHANNEL {
		t.Errorf("the room's ChatInfo kinds %v, want GROUP first and CHANNEL last", kinds)
	}
	if before < 0 {
		t.Error("the post made while the room was a group was not delivered")
	}
	if erinInfo < 0 {
		t.Error("the poster's PersonInfo did not come before their group message")
	}
	if m3 := messages(w.events(), fakenet.Group, "m3"); len(m3) != 1 {
		t.Errorf("the group beside the channel: m3 %v", m3)
	}
	for _, c := range []*contract.Command{
		sendCmd(fakenet.TurnChan, "turn-send", "hello channel", 0),
		{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: fakenet.TurnChan, UpToMessageId: fakenet.TurnBefore}}},
		{Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: fakenet.TurnChan}}},
	} {
		expect(t, w.command(c), contract.Outcome_OUTCOME_REJECTED, "channel_not_ingested")
	}
	expect(t, w.command(sendCmd(fakenet.Group, "turn-group-send", "hello group", 0)), contract.Outcome_OUTCOME_OK, "")
	w.shutdown()
	if n := strings.Count(stderrOf(t, w), dropCode); n != 1 {
		t.Errorf("%d lines %s in the first run, want 1 (one room)", n, dropCode)
	}

	w = start(t, state, opts{script: script})
	w.wait("the group's message after the channel's post", afterMsg)
	for _, e := range upTo(t, w, afterMsg) {
		if ci := e.GetChatInfo(); ci != nil && ci.GetChatId() == fakenet.TurnChan && ci.GetKind() != contract.ChatKind_CHAT_KIND_CHANNEL {
			t.Errorf("after the restart the room's ChatInfo has kind %v", ci.GetKind())
		}
		if strings.Contains(fmt.Sprint(e), fakenet.TurnMark) {
			t.Errorf("after the restart an event of the channel was delivered: %v", e)
		}
	}
	w.shutdown()

	w = start(t, state, opts{script: &fakenet.Script{TurnedChannel: true, TurnBack: true}})
	w.wait("the group's message after the room's post", afterMsg)
	group, post := -1, -1
	for i, e := range upTo(t, w, afterMsg) {
		if ci := e.GetChatInfo(); ci != nil && ci.GetChatId() == fakenet.TurnChan && ci.GetKind() == contract.ChatKind_CHAT_KIND_GROUP && group < 0 {
			group = i
		}
		if m := e.GetMessage(); m != nil && m.GetChatId() == fakenet.TurnChan && strings.HasPrefix(m.GetMessageId(), fakenet.TurnPostAgain+"-") {
			post = i
		}
	}
	if post < 0 {
		t.Error("the room's post once it is a group again was not delivered")
	}
	if group < 0 || group > post {
		t.Errorf("the room's GROUP ChatInfo at %d, its post at %d: want the ChatInfo first", group, post)
	}
	w.shutdown()
}

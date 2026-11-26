// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"fmt"
	"strings"
	"testing"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

// chatIDs are the chat ids an event names.
func chatIDs(e *contract.ChatEvent) []string {
	var out []string
	add := func(s string) {
		if s != "" {
			out = append(out, s)
		}
	}
	add(e.GetChatInfo().GetChatId())
	add(e.GetChatAbilities().GetChatId())
	add(e.GetChatRemoved().GetChatId())
	add(e.GetMessage().GetChatId())
	add(e.GetMessage().GetReplyTo().GetChatId())
	add(e.GetEdit().GetTarget().GetChatId())
	add(e.GetReaction().GetTarget().GetChatId())
	add(e.GetMessageDeleted().GetTarget().GetChatId())
	add(e.GetReadUpTo().GetChatId())
	add(e.GetDelivered().GetTarget().GetChatId())
	add(e.GetHistoryBatch().GetChatId())
	for _, m := range e.GetHistoryBatch().GetMessages() {
		add(m.GetChatId())
	}
	return out
}

// assertNeverNamed: no event names one of the chat ids.
func assertNeverNamed(t *testing.T, w *worker, ids ...string) {
	t.Helper()
	for _, e := range w.events() {
		for _, c := range chatIDs(e) {
			if contains(ids, c) {
				t.Errorf("an event names the chat id %q, which the host was never given: %v", c, e)
			}
		}
	}
}

// networkRecord is the network log's last record of kind for txn (or
// target), nil when there is none.
func networkRecord(t *testing.T, state, kind, txn string) *fakenet.Record {
	t.Helper()
	recs, err := fakenet.ReadNetworkLog(state)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(recs) - 1; i >= 0; i-- {
		if r := recs[i]; r.Kind == kind && (txn == "" || r.Txn == txn || r.Target == txn) {
			return &recs[i]
		}
	}
	return nil
}

// TestReIDKeepsChatID: the network re-IDs a portal after its room was made
// as mautrix-telegram does with a basic group upgraded to a supergroup (the
// framework's own Bridge.ReIDPortal, then a chat-info sync under the new
// key: fakenet Script.ReID). For the host the chat keeps the id it was
// first reported under: the message after the re-ID, a read by stream
// position (looked up by the portal's key), the ChatInfo of the
// sync after it (kind GROUP, the new name and members) and the echoes of
// send, react, mark read and fetch history on the old id carry it; those
// commands reach the portal under its new key (the network gets them for
// the new id); a StartChat that resolves to the portal answers the old id;
// the new raw id, which the host was never given, is unknown_chat and no
// event names it. A portal re-IDed onto one that has a room of its own is
// merged into it: its room is reported removed under its own id, which is
// then unknown, and the target stays the chat it was. After a restart all
// of it holds.
func TestReIDKeepsChatID(t *testing.T) {
	state := newState(t)
	script := &fakenet.Script{ReID: true}
	w := start(t, state, opts{script: script})
	w.signIn()
	w.wait("the merged room removed", func(e *contract.ChatEvent) bool { return e.GetChatRemoved().GetChatId() == fakenet.MergeFrom })
	evs := w.events()
	if m := messages(evs, fakenet.MiniGroup, fakenet.SuperMsg); len(m) != 1 || m[0].GetSenderId() != string(fakenet.Alice) {
		t.Fatalf("the message after the re-ID: %v, want one under %s", m, fakenet.MiniGroup)
	}
	if m := messages(evs, fakenet.MiniGroup, fakenet.MiniMsg); len(m) != 1 {
		t.Fatalf("the message before the re-ID: %v", m)
	}
	// A read by stream position looks the portal's messages up by its key:
	// the current one.
	read := false
	for _, e := range evs {
		r := e.GetReadUpTo()
		read = read || (r.GetChatId() == fakenet.MiniGroup && r.GetReaderId() == string(fakenet.Alice) && r.GetUpToMessageId() == fakenet.SuperMsg)
	}
	if !read {
		t.Error("Alice's read by stream position after the re-ID was not reported under the chat's id")
	}
	var last *contract.ChatInfo
	for _, e := range evs {
		if ci := e.GetChatInfo(); ci.GetChatId() == fakenet.MiniGroup {
			last = ci
		}
	}
	if last == nil || last.GetKind() != contract.ChatKind_CHAT_KIND_GROUP || last.GetName() != "Super group "+fakenet.SuperGroup || len(last.GetMembers()) != 3 {
		t.Fatalf("the chat's info after the re-ID: %v, want kind GROUP with the supergroup's name and members", last)
	}
	for _, kind := range []string{fakenet.SuperGroup + ":source re-ID'd", fakenet.MergeInto + ":source tombstoned into target"} {
		found := false
		recs, _ := fakenet.ReadNetworkLog(state)
		for _, r := range recs {
			found = found || (r.Kind == "reid" && r.Target == kind)
		}
		if !found {
			t.Fatalf("the network log has no re-ID %q: the case is not exercised", kind)
		}
	}

	commands := func(round string) {
		t.Helper()
		key := "reid-send-" + round
		expect(t, w.command(sendCmd(fakenet.MiniGroup, key, "to the supergroup "+round, 0)), contract.Outcome_OUTCOME_OK, "")
		echo := w.wait("send echo", func(e *contract.ChatEvent) bool { return e.GetMessage().GetSendKey() == key }).GetMessage()
		if echo.GetChatId() != fakenet.MiniGroup {
			t.Errorf("send echo under %q, want %q", echo.GetChatId(), fakenet.MiniGroup)
		}
		if rec := networkRecord(t, state, "deliver", key); rec == nil || rec.Chat != fakenet.SuperGroup {
			t.Errorf("the network got the send as %+v, want it in %s", rec, fakenet.SuperGroup)
		}
		rkey := "reid-react-" + round
		expect(t, w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.MiniGroup, fakenet.SuperMsg, ""), SendKey: rkey, Emoji: "🎉"}}}),
			contract.Outcome_OUTCOME_OK, "")
		react := w.wait("react echo", func(e *contract.ChatEvent) bool {
			return e.GetReaction().GetFromMe() && e.GetReaction().GetTarget().GetMessageId() == fakenet.SuperMsg && e.GetReaction().GetEmoji() == "🎉"
		}).GetReaction()
		if react.GetTarget().GetChatId() != fakenet.MiniGroup {
			t.Errorf("react echo under %q", react.GetTarget().GetChatId())
		}
		expect(t, w.command(&contract.Command{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: fakenet.MiniGroup, UpToMessageId: fakenet.SuperMsg}}}),
			contract.Outcome_OUTCOME_OK, "")
		read := w.wait("own read", func(e *contract.ChatEvent) bool {
			return e.GetReadUpTo().GetFromMe() && e.GetReadUpTo().GetUpToMessageId() == fakenet.SuperMsg
		}).GetReadUpTo()
		if read.GetChatId() != fakenet.MiniGroup {
			t.Errorf("read echo under %q", read.GetChatId())
		}
		fh := &contract.Command{CommandId: "reid-history-" + round, Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: fakenet.MiniGroup, Count: 10}}}
		expect(t, w.command(fh), contract.Outcome_OUTCOME_OK, "")
		batch := w.wait("history", func(e *contract.ChatEvent) bool { return e.GetHistoryBatch().GetCommandId() == fh.GetCommandId() }).GetHistoryBatch()
		if batch.GetChatId() != fakenet.MiniGroup {
			t.Errorf("history under %q", batch.GetChatId())
		}
		if round == "1" && len(batch.GetMessages()) != fakenet.SuperHistoryN {
			// The network serves the history only for the portal under
			// its new key (fakenet FetchMessages).
			t.Errorf("history of %d messages, want %d", len(batch.GetMessages()), fakenet.SuperHistoryN)
		}
		for _, m := range batch.GetMessages() {
			if m.GetChatId() != fakenet.MiniGroup {
				t.Errorf("history message under %q", m.GetChatId())
			}
		}
		sc := w.command(&contract.Command{Kind: &contract.Command_StartChat{StartChat: &contract.StartChat{Identifier: "chat:" + fakenet.SuperGroup}}})
		expect(t, sc, contract.Outcome_OUTCOME_OK, "")
		if sc.GetChatId() != fakenet.MiniGroup {
			t.Errorf("StartChat of the re-IDed chat answers %q, want %q", sc.GetChatId(), fakenet.MiniGroup)
		}
		// The new raw id and the merged chat's id name no chat.
		for _, chat := range []string{fakenet.SuperGroup, fakenet.MergeFrom} {
			expect(t, w.command(sendCmd(chat, "reid-raw-"+chat+"-"+round, "nowhere", 0)), contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
			expect(t, w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(chat, fakenet.SuperMsg, ""), SendKey: "reid-raw-react-" + chat + "-" + round, Emoji: "🎉"}}}),
				contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
			expect(t, w.command(&contract.Command{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: chat, UpToMessageId: fakenet.SuperMsg}}}),
				contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
			expect(t, w.command(&contract.Command{Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: chat}}}),
				contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
		}
		// The merge's target is the chat it was, under its own id.
		mkey := "reid-merged-" + round
		expect(t, w.command(sendCmd(fakenet.MergeInto, mkey, "to the merge target", 0)), contract.Outcome_OUTCOME_OK, "")
		if e := w.wait("merge target echo", func(e *contract.ChatEvent) bool { return e.GetMessage().GetSendKey() == mkey }); e.GetMessage().GetChatId() != fakenet.MergeInto {
			t.Errorf("merge target echo under %q", e.GetMessage().GetChatId())
		}
		assertNeverNamed(t, w, fakenet.SuperGroup)
	}
	commands("1")
	w.shutdown()

	w = start(t, state, opts{script: script})
	again := w.wait("the later connect's message", func(e *contract.ChatEvent) bool {
		return strings.HasPrefix(e.GetMessage().GetMessageId(), fakenet.ReIDAgain)
	}).GetMessage()
	if again.GetChatId() != fakenet.MiniGroup {
		t.Fatalf("after a restart a message of the re-IDed chat came under %q, want %q", again.GetChatId(), fakenet.MiniGroup)
	}
	commands("2")
	w.shutdown()
}

const (
	takenCode      = `"code":"room_taken_refused"`
	supersededCode = `"code":"superseded_room_event_dropped"`
)

// lateRecords are the network log's records of the late messages under
// MiniGroup's key (fakenet Script.LateOld).
func lateRecords(t *testing.T, state string) []fakenet.Record {
	t.Helper()
	recs, err := fakenet.ReadNetworkLog(state)
	if err != nil {
		t.Fatal(err)
	}
	var out []fakenet.Record
	for _, r := range recs {
		if r.Kind == "late" {
			out = append(out, r)
		}
	}
	return out
}

// TestReIDLateOldKey: after the re-ID of MiniGroup to SuperGroup, messages
// still come under MiniGroup's key (fakenet Script.LateOld: as a
// mautrix-telegram basic group's message handled after the supergroup's
// MigrateFrom), from a person met nowhere else. The framework makes a fresh
// portal under that key; the hash of the key is the re-IDed room's id, so
// the worker gives it a superseded room: the framework handles every late
// message (no error goes back to the network side, as it would to
// mautrix-telegram's update loop), and nothing of them or of that room is
// sent, nor the PersonInfo of their sender
// (superseded_room_event_dropped, once per framework instance). The live
// room keeps the supergroup's state: its generation, name and members
// stay, no ChatInfo with the old group's name or members follows, and
// send, react and mark read on the chat id still reach the portal under
// its new key. No command names the superseded room: a StartChat that
// resolves to its portal and a chat id "" are unknown_chat. The same after
// a restart, where the later connect sends one more.
func TestReIDLateOldKey(t *testing.T) {
	state := newState(t)
	script := &fakenet.Script{ReID: true, LateOld: true}
	w := start(t, state, opts{script: script})
	w.signIn()
	w.wait("the merged room removed", func(e *contract.ChatEvent) bool { return e.GetChatRemoved().GetChatId() == fakenet.MergeFrom })
	late := lateRecords(t, state)
	if len(late) != fakenet.LateOldN {
		t.Fatalf("late messages %v, want %d: the case is not exercised", late, fakenet.LateOldN)
	}
	for _, r := range late {
		if r.Target != "ok" {
			t.Errorf("late message %s handled %q, want ok", r.Txn, r.Target)
		}
	}

	// assertRoomKept: the live room has the supergroup's info, nothing of
	// the late messages or of their sender is reported, and the old key's
	// portal has its own room, superseded. A worker started again sends a
	// ChatInfo only when the room's changed (none, here): every one that
	// comes is the supergroup's.
	assertRoomKept := func(when string, restarted bool) {
		t.Helper()
		superSeen := restarted
		var last *contract.ChatInfo
		for _, e := range w.events() {
			if s := fmt.Sprint(e); strings.Contains(s, fakenet.LateOldMsg) || strings.Contains(s, string(fakenet.LateSender)) || strings.Contains(s, "Straggler") {
				t.Errorf("%s: an event of the late messages was reported: %v", when, e)
			}
			ci := e.GetChatInfo()
			if ci.GetChatId() != fakenet.MiniGroup {
				continue
			}
			if ci.GetName() == "Super group "+fakenet.SuperGroup {
				superSeen = true
			} else if superSeen {
				t.Errorf("%s: a ChatInfo of the old group after the re-ID: %v", when, ci)
			}
			last = ci
		}
		if (last == nil && !restarted) || (last != nil && (last.GetName() != "Super group "+fakenet.SuperGroup || len(last.GetMembers()) != 3)) {
			t.Errorf("%s: the chat's last info %v, want the supergroup's name and 3 members", when, last)
		}
		st, done := openStore(t, state)
		defer done()
		room, err := st.RoomOfChat(t.Context(), fakenet.MiniGroup, string(fakenet.Me))
		if err != nil || room == nil {
			t.Fatalf("%s: the room's row: %v %v", when, room, err)
		}
		members, _ := st.Members(t.Context(), room.RoomID)
		name, _ := st.GetState(t.Context(), room.RoomID, "m.room.name", "")
		if room.CreatedGen != 1 || room.PortalID != fakenet.SuperGroup || room.Superseded || len(members) != 3 || !strings.Contains(string(name), "Super group") {
			t.Errorf("%s: the room's row %+v, %d members, name %s: want made once, standing for %s, the supergroup's", when, room, len(members), name, fakenet.SuperGroup)
		}
		var owner int
		var alt string
		_ = st.DB().QueryRow(t.Context(), `SELECT COUNT(*) FROM portal WHERE id = $1 AND mxid = $2`, fakenet.SuperGroup, room.RoomID).Scan(&owner)
		_ = st.DB().QueryRow(t.Context(), `SELECT COALESCE(mxid, '') FROM portal WHERE id = $1`, fakenet.MiniGroup).Scan(&alt)
		sup, _ := st.Room(t.Context(), alt)
		if owner != 1 || alt == "" || alt == room.RoomID || sup == nil || !sup.Superseded || sup.ChatID != "" || !sup.Created {
			t.Errorf("%s: live portal rows %d; the old key's portal room %q, its row %+v: want a superseded room of its own", when, owner, alt, sup)
		}
		// The sender's profile came, and is held until a use in a room
		// that is sent.
		var held int
		_ = st.DB().QueryRow(t.Context(), `SELECT COUNT(*) FROM lynx_people WHERE person_id = $1`, string(fakenet.LateSender)).Scan(&held)
		if held != 1 {
			t.Errorf("%s: %d stored profiles of the late messages' sender, want 1 (held)", when, held)
		}
	}
	assertRoomKept("after the late messages", false)
	errs := stderrOf(t, w)
	if n := strings.Count(errs, supersededCode); n != 1 {
		t.Errorf("%s logged %d times for %d late messages, want once", supersededCode, n, fakenet.LateOldN)
	}
	if strings.Contains(errs, takenCode) {
		t.Errorf("%s logged: a creation was refused", takenCode)
	}

	commands := func(round string) {
		t.Helper()
		key := "late-send-" + round
		expect(t, w.command(sendCmd(fakenet.MiniGroup, key, "after the late ones "+round, 0)), contract.Outcome_OUTCOME_OK, "")
		echo := w.wait("send echo", func(e *contract.ChatEvent) bool { return e.GetMessage().GetSendKey() == key }).GetMessage()
		if echo.GetChatId() != fakenet.MiniGroup {
			t.Errorf("send echo under %q, want %q", echo.GetChatId(), fakenet.MiniGroup)
		}
		if rec := networkRecord(t, state, "deliver", key); rec == nil || rec.Chat != fakenet.SuperGroup {
			t.Errorf("the network got the send as %+v, want it in %s", rec, fakenet.SuperGroup)
		}
		expect(t, w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.MiniGroup, fakenet.SuperMsg, ""), SendKey: "late-react-" + round, Emoji: "🎉"}}}),
			contract.Outcome_OUTCOME_OK, "")
		if rec := networkRecord(t, state, "react", fakenet.SuperMsg); rec == nil || rec.Chat != fakenet.SuperGroup {
			t.Errorf("the network got the reaction as %+v, want it in %s", rec, fakenet.SuperGroup)
		}
		expect(t, w.command(&contract.Command{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: fakenet.MiniGroup, UpToMessageId: fakenet.SuperMsg}}}),
			contract.Outcome_OUTCOME_OK, "")
		if rec := networkRecord(t, state, "read", fakenet.SuperMsg); rec == nil || rec.Chat != fakenet.SuperGroup {
			t.Errorf("the network got the read as %+v, want it in %s", rec, fakenet.SuperGroup)
		}
		// No command names the superseded room.
		expect(t, w.command(&contract.Command{Kind: &contract.Command_StartChat{StartChat: &contract.StartChat{Identifier: "chat:" + fakenet.MiniGroup}}}),
			contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
		expect(t, w.command(sendCmd("", "late-nowhere-"+round, "nowhere", 0)), contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
		assertNeverNamed(t, w, fakenet.SuperGroup)
	}
	commands("1")
	w.shutdown()

	// A new instance: the later connect's late message is handled the
	// same way, and its drop logged once more (once per instance).
	w = start(t, state, opts{script: script})
	w.wait("the later connect's message", func(e *contract.ChatEvent) bool {
		return strings.HasPrefix(e.GetMessage().GetMessageId(), fakenet.ReIDAgain)
	})
	if late := lateRecords(t, state); len(late) != fakenet.LateOldN+1 || late[len(late)-1].Target != "ok" {
		t.Fatalf("late messages after the restart %v, want one more, ok", late)
	}
	assertRoomKept("after the restart", true)
	if n := strings.Count(stderrOf(t, w), supersededCode); n != 2 {
		t.Errorf("%s logged %d times by two instances, want twice", supersededCode, n)
	}
	commands("2")
	w.shutdown()
}

// TestReIDRejoin: after the late messages of TestReIDLateOldKey made the
// old key's portal a superseded room, the user leaves the supergroup (its
// portal and room deleted: ChatRemoved under the chat's id), the network
// re-IDs the old key's portal onto the supergroup's key, which has no
// portal now (mautrix-telegram's checkDialogMigrated at the next dialog
// sync), and a message comes in the supergroup: it comes to the superseded
// room, which becomes a live room under the supergroup's own id. The host
// gets the ChatRemoved, then the room's ChatInfo under that id (the
// supergroup's name, from the sync after the re-ID), then the message
// under it; send, react and mark read on that id reach the portal, and a
// StartChat that resolves to it answers that id.
func TestReIDRejoin(t *testing.T) {
	testRejoin(t, false)
}

// TestReIDRejoinCancelledCreate is TestReIDRejoin where a message of the
// supergroup comes after the user left it and the network re-IDs the old
// key's portal onto the supergroup's key while the framework makes that
// message's room (fakenet Script.CancelCreate): bridgev2's ReIDPortal
// cancels the creation after the worker gave the room its id, deletes the
// supergroup's portal row, which has no room, and re-IDs the old key's
// portal onto it. The worker's row of the id it gave stays, never created:
// it is no chat, so the moved room still becomes the live room of the
// supergroup's id and every check of TestReIDRejoin holds; the message
// whose room was never made is not reported.
func TestReIDRejoinCancelledCreate(t *testing.T) {
	testRejoin(t, true)
}

func testRejoin(t *testing.T, cancel bool) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{ReID: true, LateOld: true, Rejoin: true, CancelCreate: cancel}})
	w.signIn()
	msg := w.wait("the message after the rejoin", func(e *contract.ChatEvent) bool {
		return e.GetMessage().GetMessageId() == fakenet.RejoinMsg
	}).GetMessage()
	if msg.GetChatId() != fakenet.SuperGroup {
		t.Fatalf("the message after the rejoin under %q, want %q", msg.GetChatId(), fakenet.SuperGroup)
	}
	result := "source re-ID'd"
	if cancel {
		result = "target deleted and source re-ID'd"
	}
	if rec := networkRecord(t, state, "reid", ""); rec == nil || rec.Chat != fakenet.MiniGroup || rec.Target != fakenet.SuperGroup+":"+result {
		t.Fatalf("the last re-ID %+v: the case is not exercised", rec)
	}
	if rec := networkRecord(t, state, "cancel_missed", ""); rec != nil {
		t.Fatalf("the room creation was not cancelled (%+v): the case is not exercised", rec)
	}
	if cancel {
		assertNeverCreated(t, state)
	}
	removed, info, message := -1, -1, -1
	var last *contract.ChatInfo
	for i, e := range w.events() {
		switch {
		case e.GetChatRemoved().GetChatId() == fakenet.MiniGroup:
			removed = i
		case e.GetChatInfo().GetChatId() == fakenet.SuperGroup:
			if info < 0 {
				info = i
			}
			last = e.GetChatInfo()
		case e.GetMessage().GetMessageId() == fakenet.RejoinMsg:
			message = i
		}
		if s := fmt.Sprint(e); strings.Contains(s, fakenet.LateOldMsg) || strings.Contains(s, fakenet.CancelledMsg) {
			t.Errorf("a late message, or one whose room was never made, was reported: %v", e)
		}
	}
	if removed < 0 || info < removed || message < info {
		t.Errorf("ChatRemoved of %s at %d, first ChatInfo of %s at %d, the message at %d: want them in that order",
			fakenet.MiniGroup, removed, fakenet.SuperGroup, info, message)
	}
	if last.GetName() != "Super group "+fakenet.SuperGroup || last.GetKind() != contract.ChatKind_CHAT_KIND_GROUP {
		t.Errorf("the chat's last info %v, want the supergroup's name, a group", last)
	}
	if !strings.Contains(stderrOf(t, w), `"code":"superseded_room_live"`) {
		t.Error("superseded_room_live not logged")
	}

	key := "rejoin-send"
	expect(t, w.command(sendCmd(fakenet.SuperGroup, key, "after the rejoin", 0)), contract.Outcome_OUTCOME_OK, "")
	echo := w.wait("send echo", func(e *contract.ChatEvent) bool { return e.GetMessage().GetSendKey() == key }).GetMessage()
	if echo.GetChatId() != fakenet.SuperGroup {
		t.Errorf("send echo under %q, want %q", echo.GetChatId(), fakenet.SuperGroup)
	}
	if rec := networkRecord(t, state, "deliver", key); rec == nil || rec.Chat != fakenet.SuperGroup {
		t.Errorf("the network got the send as %+v, want it in %s", rec, fakenet.SuperGroup)
	}
	expect(t, w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.SuperGroup, fakenet.RejoinMsg, ""), SendKey: "rejoin-react", Emoji: "🎉"}}}),
		contract.Outcome_OUTCOME_OK, "")
	if rec := networkRecord(t, state, "react", fakenet.RejoinMsg); rec == nil || rec.Chat != fakenet.SuperGroup {
		t.Errorf("the network got the reaction as %+v, want it in %s", rec, fakenet.SuperGroup)
	}
	expect(t, w.command(&contract.Command{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: fakenet.SuperGroup, UpToMessageId: fakenet.RejoinMsg}}}),
		contract.Outcome_OUTCOME_OK, "")
	if rec := networkRecord(t, state, "read", fakenet.RejoinMsg); rec == nil || rec.Chat != fakenet.SuperGroup {
		t.Errorf("the network got the read as %+v, want it in %s", rec, fakenet.SuperGroup)
	}
	r := w.command(&contract.Command{Kind: &contract.Command_StartChat{StartChat: &contract.StartChat{Identifier: "chat:" + fakenet.SuperGroup}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	if r.GetChatId() != fakenet.SuperGroup {
		t.Errorf("StartChat answers %q, want %q", r.GetChatId(), fakenet.SuperGroup)
	}
	expect(t, w.command(sendCmd(fakenet.MiniGroup, "rejoin-old", "to the removed chat", 0)), contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
	w.shutdown()
}

// assertNeverCreated: the worker keeps one row of the supergroup's id that
// was never created (the id it gave the cancelled creation), not deleted,
// and the id names the moved room (store.RoomOfChat), whose portal is the
// supergroup's.
func assertNeverCreated(t *testing.T, state string) {
	t.Helper()
	st, done := openStore(t, state)
	defer done()
	var never, live int
	_ = st.DB().QueryRow(t.Context(), `SELECT COUNT(*) FROM lynx_rooms WHERE chat_id = $1 AND created = 0 AND deleted = 0 AND superseded = 0`, fakenet.SuperGroup).Scan(&never)
	_ = st.DB().QueryRow(t.Context(), `SELECT COUNT(*) FROM lynx_rooms WHERE chat_id = $1 AND created = 1 AND deleted = 0 AND superseded = 0`, fakenet.SuperGroup).Scan(&live)
	if never != 1 || live != 1 {
		t.Fatalf("rows of %s: %d never created, %d live; want one of each", fakenet.SuperGroup, never, live)
	}
	room, err := st.RoomOfChat(t.Context(), fakenet.SuperGroup, string(fakenet.Me))
	if err != nil || room == nil || !room.Created {
		t.Fatalf("%s names %+v %v, want the moved room", fakenet.SuperGroup, room, err)
	}
	var owner int
	_ = st.DB().QueryRow(t.Context(), `SELECT COUNT(*) FROM portal WHERE id = $1 AND mxid = $2`, fakenet.SuperGroup, room.RoomID).Scan(&owner)
	if owner != 1 {
		t.Errorf("the room %s %s names is not its portal's", room.RoomID, fakenet.SuperGroup)
	}
}

// TestOrphanRoomRemovedAtStart: a merge (a re-ID onto a portal with a room)
// cut off between the framework's two steps leaves the source room
// reported and no portal for it: the framework deleted the source portal's
// row, and the goroutine that deletes its room (DeleteRoom, which reports
// ChatRemoved) never ran. Seeded here in a stopped worker's store: the
// framework's portal row of dm-alice's room is deleted. The next start,
// before any login runs, reports that chat ChatRemoved, and no other; a
// start after that reports it no more. Before the fix nothing was
// reported, and the host kept a chat that no longer exists.
func TestOrphanRoomRemovedAtStart(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	w.shutdown()
	st, done := openStore(t, state)
	var room string
	if err := st.DB().QueryRow(t.Context(), `SELECT room_id FROM lynx_rooms
		WHERE chat_id = $1 AND created = 1 AND deleted = 0 AND superseded = 0`, fakenet.DMAlice).Scan(&room); err != nil {
		t.Fatal(err)
	}
	if res, err := st.DB().Exec(t.Context(), `DELETE FROM portal WHERE mxid = $1`, room); err != nil {
		t.Fatal(err)
	} else if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("%d portal rows of the room deleted, want 1", n)
	}
	done()

	// removed are the ChatRemoved events a worker delivered, as
	// chat id and event id.
	removed := func(w *worker) []string {
		var out []string
		for _, e := range w.events() {
			if c := e.GetChatRemoved().GetChatId(); c != "" {
				out = append(out, c+" "+e.GetEventId())
			}
		}
		return out
	}
	w = start(t, state, opts{})
	first := w.wait("the orphan's ChatRemoved", func(e *contract.ChatEvent) bool { return e.GetChatRemoved().GetChatId() == fakenet.DMAlice })
	w.shutdown()
	once := fakenet.DMAlice + " " + first.GetEventId()
	if got := removed(w); len(got) != 1 || got[0] != once {
		t.Errorf("chats removed at the start: %v, want only %s", got, once)
	}
	if !strings.Contains(stderrOf(t, w), `"code":"orphan_room_removed"`) {
		t.Error("orphan_room_removed not logged")
	}

	// The sweep runs before the login starts: once it is connected, the
	// sweep is over.
	w = start(t, state, opts{})
	w.wait("connected", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	})
	w.shutdown()
	// The one before may come again, unacknowledged when that worker
	// stopped (the same event, replayed from the queue); nothing new.
	for _, got := range removed(w) {
		if got != once {
			t.Errorf("a chat removed at the start after: %s, want none but a replay of %s", got, once)
		}
	}
	// The worker's stderr file is the state's, kept across its starts.
	if n := strings.Count(stderrOf(t, w), `"code":"orphan_room_removed"`); n != 1 {
		t.Errorf("orphan_room_removed logged %d times over the starts, want once", n)
	}
}

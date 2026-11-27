// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// frameworkBackend is logBackend with the framework's own tables in the
// same database (as newFramework makes them: bridgev2.NewBridge on the
// store's database) and a bridge that has only them, so the backend's
// reads of the framework's portal and message rows answer from rows a test
// writes (and its ghost "a").
func frameworkBackend(t *testing.T) (*Backend, *store.Store, *lockedBuffer, *database.Database) {
	t.Helper()
	b, st, out := logBackend(t)
	bdb := database.New("test", database.MetaTypes{}, st.DB())
	if err := bdb.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The sender of the messages tests store (message_sender_fkey).
	if err := bdb.Ghost.Insert(context.Background(), &database.Ghost{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	b.brp.Store(&bridgev2.Bridge{ID: "test", DB: bdb})
	return b, st, out, bdb
}

// madeRoom has the instance make the room of key the way the framework
// does (GenerateDeterministicRoomID, then the bot's CreateRoom with a name
// and members) and stores the portal's row with that room.
func madeRoom(t *testing.T, fw *instance, bdb *database.Database, key networkid.PortalKey, name string, members ...id.UserID) id.RoomID {
	t.Helper()
	ctx := context.Background()
	room := fw.GenerateDeterministicRoomID(key)
	if _, err := fw.BotIntent().CreateRoom(ctx, createReq(room, name, members...)); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: key, MXID: room, Name: name}); err != nil {
		t.Fatal(err)
	}
	return room
}

func createReq(room id.RoomID, name string, members ...id.UserID) *mautrix.ReqCreateRoom {
	empty := ""
	return &mautrix.ReqCreateRoom{
		BeeperLocalRoomID:    room,
		BeeperInitialMembers: members,
		InitialState: []*event.Event{{Type: event.StateRoomName, StateKey: &empty,
			Content: event.Content{Parsed: &event.RoomNameEventContent{Name: name}}}},
	}
}

// TestCreateRoomRefusesTakenRoom: after the framework re-IDed a portal
// (chat:5 to channel:5, same room), a creation of that room for another
// key than one owning its id (the case GenerateDeterministicRoomID gives
// no superseded id for: the framework's rows could not be read, or every
// superseded id was taken) is refused with nothing written or reported:
// the room's state, name, members and generation stay, no ChatInfo comes;
// the refusal is logged once per room and instance, every one counted. A
// portal row whose own key gives the room id (the creating portal's) is
// let through, as is a room no portal has.
func TestCreateRoomRefusesTakenRoom(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb := frameworkBackend(t)
	oldKey := networkid.PortalKey{ID: "chat:5", Receiver: "acct"}
	newKey := networkid.PortalKey{ID: "channel:5", Receiver: "acct"}
	fw := b.newInstance()
	room := madeRoom(t, fw, bdb, oldKey, "Super group", formatGhost("a"), formatGhost("b"))
	if err := bdb.Portal.ReID(ctx, oldKey, newKey); err != nil {
		t.Fatal(err)
	}
	before, _ := st.Room(ctx, string(room))
	members, _ := st.Members(ctx, string(room))
	name, _ := st.GetState(ctx, string(room), event.StateRoomName.Type, "")
	events := len(outboxEvents(t, st))
	if before == nil || before.CreatedGen != 1 || len(members) != 2 || events == 0 {
		t.Fatalf("the room before: %+v, %d members, %d events", before, len(members), events)
	}

	refuse := func(fw *instance) {
		t.Helper()
		_, err := fw.BotIntent().CreateRoom(ctx, createReq(room, "Mini group", formatGhost("c")))
		if !errors.Is(err, errRoomTaken) || !errors.Is(err, mautrix.MRoomInUse) {
			t.Fatalf("CreateRoom of the re-IDed room under its old key: %v, want refused", err)
		}
	}
	refuse(fw)
	refuse(fw)
	after, _ := st.Room(ctx, string(room))
	membersAfter, _ := st.Members(ctx, string(room))
	nameAfter, _ := st.GetState(ctx, string(room), event.StateRoomName.Type, "")
	if after.CreatedGen != before.CreatedGen || after.PortalID != before.PortalID || after.ChatID != "chat:5" {
		t.Errorf("the room's row changed: %+v, was %+v", after, before)
	}
	if len(membersAfter) != 2 || string(nameAfter) != string(name) {
		t.Errorf("the room's members %v and name %s changed (were %v, %s)", membersAfter, nameAfter, members, name)
	}
	if n := len(outboxEvents(t, st)); n != events {
		t.Errorf("%d events reported by the refused creations", n-events)
	}
	if n := strings.Count(out.String(), `"code":"room_taken_refused"`); n != 1 {
		t.Errorf("room_taken_refused logged %d times by one instance, want once", n)
	}
	if n := fw.takenN.Load(); n != 2 {
		t.Errorf("%d refusals counted, want 2", n)
	}
	// Another instance logs its first refusal of the room again.
	refuse(b.newInstance())
	if n := strings.Count(out.String(), `"code":"room_taken_refused"`); n != 2 {
		t.Errorf("room_taken_refused logged %d times by two instances, want twice", n)
	}

	// The creating portal's own row (its key gives the room id) is let
	// through, as is a room no portal has.
	ownKey := networkid.PortalKey{ID: "chat:6", Receiver: "acct"}
	own := fw.GenerateDeterministicRoomID(ownKey)
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: ownKey, MXID: own}); err != nil {
		t.Fatal(err)
	}
	if _, err := fw.BotIntent().CreateRoom(ctx, createReq(own, "Own")); err != nil {
		t.Errorf("CreateRoom of the portal's own room: %v", err)
	}
	fresh := fw.GenerateDeterministicRoomID(networkid.PortalKey{ID: "chat:7", Receiver: "acct"})
	if _, err := fw.BotIntent().CreateRoom(ctx, createReq(fresh, "Fresh")); err != nil {
		t.Errorf("CreateRoom of a room no portal has: %v", err)
	}
}

// insertMessage stores a message row of the framework's.
func insertMessage(t *testing.T, bdb *database.Database, key networkid.PortalKey, msg networkid.MessageID, ts time.Time) id.EventID {
	t.Helper()
	evt := messageEventID(key, msg, "")
	if err := bdb.Message.Insert(context.Background(), &database.Message{
		ID: msg, MXID: evt, Room: key, SenderID: "a", SenderMXID: formatGhost("a"), Timestamp: ts,
	}); err != nil {
		t.Fatal(err)
	}
	return evt
}

// reads are the ReadUpTo events reported.
func reads(t *testing.T, st *store.Store) []*contract.ReadUpTo {
	t.Helper()
	var out []*contract.ReadUpTo
	for _, e := range outboxEvents(t, st) {
		if r := e.GetReadUpTo(); r != nil {
			out = append(out, r)
		}
	}
	return out
}

// TestStreamOrderReadAfterReID: a read by stream position in a re-IDed
// room is looked up under the portal's current key, by keyOfRoom's own
// read of the portal: here no Hooks.IsChannel is set, so channelRoom never
// reads the portal and the room's row still has the old key. The target is
// the message of the new key; the read is reported under the chat's id.
func TestStreamOrderReadAfterReID(t *testing.T) {
	ctx := context.Background()
	b, st, _, bdb := frameworkBackend(t)
	oldKey := networkid.PortalKey{ID: "chat:5", Receiver: "acct"}
	newKey := networkid.PortalKey{ID: "channel:5", Receiver: "acct"}
	fw := b.newInstance()
	room := madeRoom(t, fw, bdb, oldKey, "Group", formatGhost("a"))
	if err := bdb.Portal.ReID(ctx, oldKey, newKey); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1795000000, 0)
	insertMessage(t, bdb, newKey, "5.10", base)
	if err := fw.GhostIntent("a").(*Intent).MarkStreamOrderRead(ctx, room, 10, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	r := reads(t, st)
	if len(r) != 1 || r[0].GetChatId() != "chat:5" || r[0].GetUpToMessageId() != "5.10" {
		t.Fatalf("reads %v, want one of 5.10 under chat:5", r)
	}
}

// originalOfMigrated is a Hooks.OriginalMessageID of the shape of
// chat_telegram's: migrated:<digits>:<digits> is renamed from <digits>;
// another migrated: id is renamed with no original.
func originalOfMigrated(m networkid.MessageID) (networkid.MessageID, bool) {
	rest, ok := strings.CutPrefix(string(m), "migrated:")
	if !ok {
		return "", false
	}
	orig, ms, ok := strings.Cut(rest, ":")
	if !ok || orig == "" || ms == "" || strings.Trim(orig, "0123456789") != "" || strings.Trim(ms, "0123456789") != "" {
		return "", true
	}
	return networkid.MessageID(orig), true
}

// TestRenamedRowNamedByOriginalID: a row the connector renamed after its
// message was reported (Hooks.OriginalMessageID; mautrix-telegram's
// migrated:<id>:<time>) is named to the host by the id it was reported
// under, in every event that names an existing row: a read by event and
// by stream position, a delivery, an edit, a reaction added and removed
// (by its meta, and by the reaction's row), a deletion (by its meta, and
// by the message's event). A renamed row whose original id cannot be told
// is named in none of them: each is not reported, counted, the first of an
// instance logged. Without the hook a renamed id is an id like any other.
func TestRenamedRowNamedByOriginalID(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb := frameworkBackend(t)
	b.hooks.OriginalMessageID = originalOfMigrated
	fw := b.newInstance()
	room := madeRoom(t, fw, bdb, oldKey5, "Group", formatGhost("a"), formatGhost("b"))
	base := time.Unix(1795000000, 0)
	const renamed, malformed = "migrated:7:1795000100000", "migrated:x7:1795000100000"
	renamedEvt := insertMessage(t, bdb, oldKey5, renamed, base)
	malformedEvt := insertMessage(t, bdb, oldKey5, malformed, base.Add(time.Second))
	if err := bdb.Portal.ReID(ctx, oldKey5, newKey5); err != nil {
		t.Fatal(err)
	}
	reactEvt := id.EventID("$react-renamed")
	if err := bdb.Reaction.Upsert(ctx, &database.Reaction{Room: newKey5, MessageID: renamed, SenderID: "a", EmojiID: "e1", Emoji: "👍", MXID: reactEvt, SenderMXID: formatGhost("a"), Timestamp: base}); err != nil {
		t.Fatal(err)
	}
	ghost := fw.GhostIntent("a").(*Intent)

	// named has every event that names an existing row do so for the
	// row msg (its event evt), from the first event on.
	named := func(msg networkid.MessageID, evt id.EventID, at time.Time) {
		t.Helper()
		send := func(typ event.Type, parsed any, extra *bridgev2.MatrixSendExtra) {
			t.Helper()
			extra.Timestamp = at
			if _, err := ghost.SendMessage(ctx, room, typ, &event.Content{Parsed: parsed}, extra); err != nil {
				t.Fatal(err)
			}
		}
		if err := ghost.MarkRead(ctx, room, evt, at); err != nil {
			t.Fatal(err)
		}
		if err := ghost.MarkStreamOrderRead(ctx, room, 1, at); err != nil {
			t.Fatal(err)
		}
		fw.reportDelivered(ctx, &bridgev2.MessageStatus{DeliveredTo: []id.UserID{formatGhost("b")}},
			&bridgev2.MessageStatusEventInfo{RoomID: room, SourceEventID: evt})
		edit := &event.MessageEventContent{MsgType: event.MsgText, Body: "* edited",
			NewContent: &event.MessageEventContent{MsgType: event.MsgText, Body: "edited"}}
		edit.SetEdit(evt)
		send(event.EventMessage, edit, &bridgev2.MatrixSendExtra{MessageMeta: &database.Message{ID: msg, SenderID: "a"}})
		react := &event.ReactionEventContent{RelatesTo: event.RelatesTo{Type: event.RelAnnotation, EventID: evt, Key: "👍"}}
		meta := &database.Reaction{MessageID: msg, SenderID: "a", EmojiID: "e1", Emoji: "👍"}
		send(event.EventReaction, react, &bridgev2.MatrixSendExtra{ReactionMeta: meta})
		send(event.EventRedaction, &event.RedactionEventContent{Redacts: "$some-reaction"}, &bridgev2.MatrixSendExtra{ReactionMeta: meta})
		send(event.EventRedaction, &event.RedactionEventContent{Redacts: evt}, &bridgev2.MatrixSendExtra{MessageMeta: &database.Message{ID: msg}})
		send(event.EventRedaction, &event.RedactionEventContent{Redacts: evt}, &bridgev2.MatrixSendExtra{})
	}
	// ids are the message ids the events from the n-th on name.
	ids := func(n int) []string {
		t.Helper()
		var got []string
		for _, e := range outboxEvents(t, st)[n:] {
			switch {
			case e.GetReadUpTo() != nil:
				got = append(got, "read:"+e.GetReadUpTo().GetUpToMessageId())
			case e.GetDelivered() != nil:
				got = append(got, "delivered:"+e.GetDelivered().GetTarget().GetMessageId())
			case e.GetEdit() != nil:
				got = append(got, "edit:"+e.GetEdit().GetTarget().GetMessageId())
			case e.GetReaction() != nil:
				got = append(got, fmt.Sprintf("react:%s:%v", e.GetReaction().GetTarget().GetMessageId(), e.GetReaction().GetRemoved()))
			case e.GetMessageDeleted() != nil:
				got = append(got, "del:"+e.GetMessageDeleted().GetTarget().GetMessageId())
			}
			for _, c := range chatIDsOf(e) {
				if c != "chat:5" {
					t.Errorf("an event under %q, want chat:5: %v", c, e)
				}
			}
		}
		return got
	}

	n := len(outboxEvents(t, st))
	named(renamed, renamedEvt, base.Add(500*time.Millisecond))
	// The reaction's own row, removed by its event.
	if _, err := ghost.SendMessage(ctx, room, event.EventRedaction, &event.Content{Parsed: &event.RedactionEventContent{Redacts: reactEvt}},
		&bridgev2.MatrixSendExtra{Timestamp: base.Add(600 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	want := []string{"read:7", "read:7", "delivered:7", "edit:7", "react:7:false", "react:7:true", "del:7", "del:7", "react:7:true"}
	if got := ids(n); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the renamed row named as %v, want %v", got, want)
	}
	if fw.renamedN.Load() != 0 || strings.Contains(out.String(), "renamed_target_unmapped") {
		t.Errorf("a mapped row counted (%d) or logged as unmapped", fw.renamedN.Load())
	}

	// The malformed one: nothing names it (the read by stream position
	// at its time finds it, the last row at or before it).
	n = len(outboxEvents(t, st))
	named(malformed, malformedEvt, base.Add(1500*time.Millisecond))
	if got := ids(n); len(got) != 0 {
		t.Errorf("a renamed row with no original id named: %v", got)
	}
	if c := fw.renamedN.Load(); c != 8 {
		t.Errorf("%d unmapped targets counted, want 8", c)
	}
	if c := strings.Count(out.String(), `"code":"renamed_target_unmapped"`); c != 1 {
		t.Errorf("renamed_target_unmapped logged %d times by one instance, want once", c)
	}

	// Without the hook the renamed row is an id like any other.
	b.hooks.OriginalMessageID = nil
	n = len(outboxEvents(t, st))
	if err := ghost.MarkRead(ctx, room, renamedEvt, base.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := ids(n); fmt.Sprint(got) != "[read:"+renamed+"]" {
		t.Fatalf("without the hook: %v, want the renamed id", got)
	}
}

// chatIDsOf are the chat ids an event names.
func chatIDsOf(e *contract.ChatEvent) []string {
	var out []string
	for _, c := range []string{e.GetChatInfo().GetChatId(), e.GetChatAbilities().GetChatId(), e.GetChatRemoved().GetChatId(),
		e.GetMessage().GetChatId(), e.GetEdit().GetTarget().GetChatId(), e.GetReaction().GetTarget().GetChatId(),
		e.GetMessageDeleted().GetTarget().GetChatId(), e.GetReadUpTo().GetChatId(), e.GetDelivered().GetTarget().GetChatId(),
		e.GetHistoryBatch().GetChatId()} {
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

// reIDed is a backend whose framework re-IDed the portal of chat:5 to
// channel:5 after its room was made; it returns the live room's id.
func reIDed(t *testing.T) (*Backend, *store.Store, *lockedBuffer, *database.Database, id.RoomID) {
	t.Helper()
	b, st, out, bdb := frameworkBackend(t)
	room := madeRoom(t, b.newInstance(), bdb, oldKey5, "Super group", formatGhost("a"), formatGhost("b"))
	if err := bdb.Portal.ReID(context.Background(), oldKey5, newKey5); err != nil {
		t.Fatal(err)
	}
	return b, st, out, bdb, room
}

var (
	oldKey5 = networkid.PortalKey{ID: "chat:5", Receiver: "acct"}
	newKey5 = networkid.PortalKey{ID: "channel:5", Receiver: "acct"}
)

// TestSupersededRoomID: a portal under the key a room was re-IDed away
// from gets another room id, deterministic (the same for another instance
// and for a backend started again on the same store), not the live room's
// and recorded as superseded with no chat id; the next one when that id
// too is another portal's. Keys whose id is free get their own.
func TestSupersededRoomID(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb, room := reIDed(t)
	alt := b.newInstance().GenerateDeterministicRoomID(oldKey5)
	if alt == room || alt != supersededRoomIDFor(oldKey5, 0) {
		t.Fatalf("the old key's room id %s (live room %s)", alt, room)
	}
	if again := b.newInstance().GenerateDeterministicRoomID(oldKey5); again != alt {
		t.Errorf("another instance gives %s, want %s", again, alt)
	}
	restarted := New(&contract.Init{StateDir: t.TempDir()}, st, NewEmitter(st, b.log, nil), b.log, Hooks{}, 2)
	restarted.brp.Store(b.bridge())
	if again := restarted.newInstance().GenerateDeterministicRoomID(oldKey5); again != alt {
		t.Errorf("after a restart %s, want %s", again, alt)
	}
	r, err := st.Room(ctx, string(alt))
	if err != nil || r == nil || !r.Superseded || r.ChatID != "" || r.PortalID != "chat:5" {
		t.Fatalf("the superseded room's row %+v %v", r, err)
	}
	if live, _ := st.RoomOfChat(ctx, "chat:5", "acct"); live == nil || live.RoomID != string(room) {
		t.Fatalf("the chat id names %+v, want the live room", live)
	}
	if n := strings.Count(out.String(), `"code":"room_superseded"`); n == 0 {
		t.Error("room_superseded not logged")
	}
	// That id taken by another portal: the next one.
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: networkid.PortalKey{ID: "chat:9", Receiver: "acct"}, MXID: alt}); err != nil {
		t.Fatal(err)
	}
	if next := b.newInstance().GenerateDeterministicRoomID(oldKey5); next != supersededRoomIDFor(oldKey5, 1) {
		t.Errorf("with the first superseded id taken: %s", next)
	}
	for _, key := range []networkid.PortalKey{newKey5, {ID: "chat:7", Receiver: "acct"}} {
		if got := b.newInstance().GenerateDeterministicRoomID(key); got != roomIDFor(key) {
			t.Errorf("%v: %s, want its own id", key, got)
		}
	}
}

// TestSupersededRoomSilent: the superseded room is made (CreateRoom
// succeeds: the framework saves its portal and goes on) and nothing of it
// is reported: not its creation's ChatInfo or abilities, a message, a
// state change, a member, a read by event or by stream position, history,
// an upload (refused, as a channel's), nor its deletion. Each drop is
// counted and logged once per room and instance. The live room keeps its
// row, members and name.
func TestSupersededRoomSilent(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb, room := reIDed(t)
	fw := b.newInstance()
	before, _ := st.Room(ctx, string(room))
	members, _ := st.Members(ctx, string(room))
	name, _ := st.GetState(ctx, string(room), event.StateRoomName.Type, "")
	events := len(outboxEvents(t, st))

	alt := fw.GenerateDeterministicRoomID(oldKey5)
	req := createReq(alt, "Mini group", formatGhost("c"))
	empty := ""
	req.InitialState = append(req.InitialState, &event.Event{Type: event.StateBeeperRoomFeatures, StateKey: &empty,
		Content: event.Content{Parsed: &event.RoomFeatures{}}})
	if _, err := fw.BotIntent().CreateRoom(ctx, req); err != nil {
		t.Fatalf("CreateRoom of the superseded room: %v", err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: oldKey5, MXID: alt}); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1795000000, 0)
	evt := insertMessage(t, bdb, oldKey5, "late-1", base)
	ghost := fw.GhostIntent("a").(*Intent)
	if _, err := ghost.SendMessage(ctx, alt, event.EventMessage,
		&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "late"}},
		&bridgev2.MatrixSendExtra{Timestamp: base, MessageMeta: &database.Message{ID: "late-1", SenderID: "a", Room: oldKey5}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ghost.SendState(ctx, alt, event.StateRoomName, "", &event.Content{Parsed: &event.RoomNameEventContent{Name: "Renamed"}}, base); err != nil {
		t.Fatal(err)
	}
	if err := fw.GhostIntent("d").EnsureJoined(ctx, alt); err != nil {
		t.Fatal(err)
	}
	if err := ghost.MarkRead(ctx, alt, evt, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := ghost.MarkStreamOrderRead(ctx, alt, 1, base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ghost.UploadMedia(ctx, alt, []byte("x"), "x.png", "image/png"); err == nil {
		t.Error("an upload for the superseded room was kept")
	}
	if _, err := fw.BatchSend(ctx, alt, &mautrix.ReqBeeperBatchSend{Events: []*event.Event{{ID: "$h", Type: event.EventMessage, Sender: formatGhost("a"),
		Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "old"}}}}},
		[]*bridgev2.MatrixSendExtra{{MessageMeta: &database.Message{ID: "h-1", SenderID: "a"}}}); err != nil {
		t.Fatal(err)
	}
	if err := fw.BotIntent().DeleteRoom(ctx, alt, false); err != nil {
		t.Fatal(err)
	}
	if evs := outboxEvents(t, st)[events:]; len(evs) != 0 {
		t.Errorf("events reported of the superseded room: %v", evs)
	}
	if n := strings.Count(out.String(), `"code":"superseded_room_event_dropped"`); n != 1 {
		t.Errorf("superseded_room_event_dropped logged %d times by one instance, want once", n)
	}
	if n := fw.supersededDrops.Load(); n < 8 {
		t.Errorf("%d drops counted, want one for each thing not sent", n)
	}
	after, _ := st.Room(ctx, string(room))
	membersAfter, _ := st.Members(ctx, string(room))
	nameAfter, _ := st.GetState(ctx, string(room), event.StateRoomName.Type, "")
	if *after != *before || len(membersAfter) != len(members) || string(nameAfter) != string(name) {
		t.Errorf("the live room changed: %+v %v %s, was %+v %v %s", after, membersAfter, nameAfter, before, members, name)
	}
	if r, _ := st.Room(ctx, string(alt)); r == nil || !r.Deleted {
		t.Errorf("the superseded room's row after its deletion: %+v", r)
	}
	// Another instance logs its first drop again.
	_ = b.newInstance().GhostIntent("a").MarkRead(ctx, alt, evt, base)
	if n := strings.Count(out.String(), `"code":"superseded_room_event_dropped"`); n != 2 {
		t.Errorf("superseded_room_event_dropped logged %d times by two instances, want twice", n)
	}
}

// TestSupersededRoomLiveAgain: the superseded room of chat:5 (a late event
// of the basic group after its upgrade) is moved by the framework onto
// channel:5 after the user left the supergroup (its portal and room
// deleted, reported ChatRemoved under chat:5): mautrix-telegram's
// checkDialogMigrated at the next dialog sync, a re-ID onto a key without
// a portal. Every event of channel:5 now comes to that room: it becomes a
// live room under channel:5, the id a room made for that key gets; first
// its ChatInfo and abilities are reported, then the event that came (here
// a message), under that id, and the id names the room, whose portal is
// channel:5's. While a live room of the login has the id, it stays
// superseded (logged superseded_room_kept once per room and instance; the
// event is dropped); once that one is removed, its next event makes it
// live.
func TestSupersededRoomLiveAgain(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb, live := reIDed(t)
	fw := b.newInstance()
	ghost := fw.GhostIntent("a").(*Intent)
	base := time.Unix(1795000000, 0)
	message := func(key networkid.PortalKey, room id.RoomID, msg networkid.MessageID, at time.Time) {
		t.Helper()
		insertMessage(t, bdb, key, msg, at)
		if _, err := ghost.SendMessage(ctx, room, event.EventMessage,
			&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: string(msg)}},
			&bridgev2.MatrixSendExtra{Timestamp: at, MessageMeta: &database.Message{ID: msg, SenderID: "a", Room: key}}); err != nil {
			t.Fatal(err)
		}
	}

	// The late basic group's portal and its superseded room.
	alt := fw.GenerateDeterministicRoomID(oldKey5)
	req := createReq(alt, "Mini group", formatGhost("a"), formatGhost("c"))
	empty := ""
	req.InitialState = append(req.InitialState, &event.Event{Type: event.StateBeeperRoomFeatures, StateKey: &empty,
		Content: event.Content{Parsed: &event.RoomFeatures{}}})
	if _, err := fw.BotIntent().CreateRoom(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: oldKey5, MXID: alt}); err != nil {
		t.Fatal(err)
	}
	message(oldKey5, alt, "late-1", base)

	// The user leaves the supergroup: its portal, then its room, deleted.
	n := len(outboxEvents(t, st))
	if err := bdb.Portal.Delete(ctx, newKey5); err != nil {
		t.Fatal(err)
	}
	if err := fw.BotIntent().DeleteRoom(ctx, live, false); err != nil {
		t.Fatal(err)
	}
	if evs := chatEventsOf(outboxEvents(t, st)[n:]); len(evs) != 1 || evs[0].GetChatRemoved().GetChatId() != "chat:5" {
		t.Fatalf("the supergroup's deletion reported %v, want its ChatRemoved under chat:5", evs)
	}

	// A live room of the login has channel:5 (made for that key, whose
	// portal then moved on to channel:8: it stands for a portal): the
	// moved room stays superseded, and the event is dropped.
	held := fw.GenerateDeterministicRoomID(newKey5)
	if _, err := fw.BotIntent().CreateRoom(ctx, createReq(held, "Held", formatGhost("a"))); err != nil {
		t.Fatal(err)
	}
	heldKey := networkid.PortalKey{ID: "channel:8", Receiver: "acct"}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: heldKey, MXID: held}); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.ReID(ctx, oldKey5, newKey5); err != nil {
		t.Fatal(err)
	}
	n = len(outboxEvents(t, st))
	message(newKey5, alt, "5.20", base.Add(time.Second))
	message(newKey5, alt, "5.21", base.Add(2*time.Second))
	if evs := chatEventsOf(outboxEvents(t, st)[n:]); len(evs) != 0 {
		t.Fatalf("events while a live room has the chat id: %v", evs)
	}
	if r, _ := st.Room(ctx, string(alt)); r == nil || !r.Superseded || r.ChatID != "" {
		t.Fatalf("the room while a live room has the chat id: %+v", r)
	}
	if c := strings.Count(out.String(), `"code":"superseded_room_kept"`); c != 1 {
		t.Errorf("superseded_room_kept logged %d times by one instance, want once", c)
	}

	// That room removed: the next event makes the moved room live.
	if err := bdb.Portal.Delete(ctx, heldKey); err != nil {
		t.Fatal(err)
	}
	if err := fw.BotIntent().DeleteRoom(ctx, held, false); err != nil {
		t.Fatal(err)
	}
	n = len(outboxEvents(t, st))
	message(newKey5, alt, "5.22", base.Add(3*time.Second))
	var kinds []string
	for _, e := range chatEventsOf(outboxEvents(t, st)[n:]) {
		for _, c := range chatIDsOf(e) {
			if c != "channel:5" {
				t.Errorf("an event under %q, want channel:5: %v", c, e)
			}
		}
		switch {
		case e.GetChatInfo() != nil:
			kinds = append(kinds, "info:"+e.GetChatInfo().GetName())
		case e.GetChatAbilities() != nil:
			kinds = append(kinds, "abilities")
		case e.GetMessage() != nil:
			kinds = append(kinds, "msg:"+e.GetMessage().GetMessageId())
		}
	}
	if want := "[info:Mini group abilities msg:5.22]"; fmt.Sprint(kinds) != want {
		t.Fatalf("after the move: %v, want %s", kinds, want)
	}
	r, _ := st.Room(ctx, string(alt))
	if r == nil || r.Superseded || r.ChatID != "channel:5" || r.PortalID != "channel:5" || r.Deleted {
		t.Fatalf("the room made live: %+v", r)
	}
	if c := strings.Count(out.String(), `"code":"superseded_room_live"`); c != 1 {
		t.Errorf("superseded_room_live logged %d times, want once", c)
	}
	// The id names the room, whose portal is channel:5's (portalFor), and
	// is the id the portal's chat is answered with (StartChat, chatIDOf).
	named, _ := st.RoomOfChat(ctx, "channel:5", "acct")
	if named == nil || named.RoomID != string(alt) {
		t.Fatalf("channel:5 names %+v, want the room made live", named)
	}
	row, _ := bdb.Portal.GetByMXID(ctx, alt)
	if row == nil || row.PortalKey != newKey5 {
		t.Fatalf("the room's portal %+v, want channel:5's", row)
	}
	if got := b.chatIDOf(ctx, &bridgev2.Portal{Portal: row, Bridge: b.bridge()}); got != "channel:5" {
		t.Errorf("the portal's chat id %q, want channel:5", got)
	}
	// The next event is a message like any.
	n = len(outboxEvents(t, st))
	message(newKey5, alt, "5.23", base.Add(4*time.Second))
	if evs := chatEventsOf(outboxEvents(t, st)[n:]); len(evs) != 1 || evs[0].GetMessage().GetChatId() != "channel:5" {
		t.Errorf("the next event: %v, want its message under channel:5", evs)
	}
	// A new instance finds it live too.
	if err := b.newInstance().GhostIntent("a").MarkRead(ctx, alt, messageEventID(newKey5, "5.23", ""), base.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if r := reads(t, st); len(r) != 1 || r[0].GetChatId() != "channel:5" || r[0].GetUpToMessageId() != "5.23" {
		t.Errorf("a read in a new instance: %v", r)
	}
}

// TestNeverCreatedRowDoesNotHoldMovedRoom: the framework asked for the
// room id of channel:5 (GenerateDeterministicRoomID: the worker remembers
// the row) and never created it: bridgev2's ReIDPortal of the superseded
// portal onto channel:5, whose portal row had no room, cancelled the
// creation in flight and deleted that row (ReIDResultTargetDeletedAndSourceReIDd,
// no DeleteRoom: seeded here as it leaves the store; the framework's own
// path is TestReIDRejoinCancelledCreate in cmd/chat_fakenet). That row is
// no live room of channel:5: the moved superseded room becomes the live
// room of channel:5 at its next event, which is sent after its ChatInfo
// and abilities, and every later event of each new instance is sent too.
// The row stays as it was, and the chat id names the moved room.
func TestNeverCreatedRowDoesNotHoldMovedRoom(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb, live := reIDed(t)
	fw := b.newInstance()
	alt := fw.GenerateDeterministicRoomID(oldKey5)
	req := createReq(alt, "Mini group", formatGhost("a"))
	empty := ""
	req.InitialState = append(req.InitialState, &event.Event{Type: event.StateBeeperRoomFeatures, StateKey: &empty,
		Content: event.Content{Parsed: &event.RoomFeatures{}}})
	if _, err := fw.BotIntent().CreateRoom(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: oldKey5, MXID: alt}); err != nil {
		t.Fatal(err)
	}
	// The user leaves the supergroup: its portal, then its room, deleted.
	if err := bdb.Portal.Delete(ctx, newKey5); err != nil {
		t.Fatal(err)
	}
	if err := fw.BotIntent().DeleteRoom(ctx, live, false); err != nil {
		t.Fatal(err)
	}
	// A creation for channel:5 asked for its room id, then was cancelled.
	never := fw.GenerateDeterministicRoomID(newKey5)
	if never != roomIDFor(newKey5) || never >= alt {
		t.Fatalf("the never created room's id %s: want channel:5's own, ordered before %s", never, alt)
	}
	if err := bdb.Portal.ReID(ctx, oldKey5, newKey5); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1795000000, 0)
	for i := range 3 {
		fw := b.newInstance()
		msg := networkid.MessageID(fmt.Sprintf("5.%d", 30+i))
		at := base.Add(time.Duration(i) * time.Second)
		insertMessage(t, bdb, newKey5, msg, at)
		n := len(outboxEvents(t, st))
		if _, err := fw.GhostIntent("a").SendMessage(ctx, alt, event.EventMessage,
			&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: string(msg)}},
			&bridgev2.MatrixSendExtra{Timestamp: at, MessageMeta: &database.Message{ID: msg, SenderID: "a", Room: newKey5}}); err != nil {
			t.Fatal(err)
		}
		var kinds []string
		for _, e := range chatEventsOf(outboxEvents(t, st)[n:]) {
			for _, c := range chatIDsOf(e) {
				if c != "channel:5" {
					t.Errorf("an event under %q, want channel:5: %v", c, e)
				}
			}
			switch {
			case e.GetChatInfo() != nil:
				kinds = append(kinds, "info")
			case e.GetChatAbilities() != nil:
				kinds = append(kinds, "abilities")
			case e.GetMessage() != nil:
				kinds = append(kinds, "msg:"+e.GetMessage().GetMessageId())
			}
		}
		want := fmt.Sprintf("[msg:%s]", msg)
		if i == 0 {
			want = fmt.Sprintf("[info abilities msg:%s]", msg)
		}
		if fmt.Sprint(kinds) != want {
			t.Fatalf("instance %d: %v, want %s", i, kinds, want)
		}
	}
	if strings.Contains(out.String(), `"code":"superseded_room_kept"`) {
		t.Error("superseded_room_kept logged: the never created row held the room")
	}
	if r, _ := st.Room(ctx, string(alt)); r == nil || r.Superseded || r.ChatID != "channel:5" || r.Deleted {
		t.Fatalf("the moved room %+v, want live under channel:5", r)
	}
	if r, _ := st.Room(ctx, string(never)); r == nil || r.Created || r.Deleted || r.Superseded || r.ChatID != "channel:5" {
		t.Fatalf("the never created row %+v, want it as it was", r)
	}
	if named, _ := st.RoomOfChat(ctx, "channel:5", "acct"); named == nil || named.RoomID != string(alt) {
		t.Fatalf("channel:5 names %+v, want the moved room", named)
	}
}

// chatEventsOf are the events that are not the account's own PersonInfo
// (each instance reports it before its first event).
func chatEventsOf(evs []*contract.ChatEvent) []*contract.ChatEvent {
	var out []*contract.ChatEvent
	for _, e := range evs {
		if !e.GetPersonInfo().GetIsSelf() {
			out = append(out, e)
		}
	}
	return out
}

// TestEmitChatRefusesNoChatID: every event about a chat (the contract's
// Message, Edit, Reaction, MessageDeleted, ReadUpTo, Delivered, ChatInfo,
// ChatRemoved, HistoryBatch, ChatAbilities) whose chat id is empty is
// refused by emitChat before anything is stored (not even the account's
// own PersonInfo), counted, the first of an instance logged
// chat_event_without_chat_id; the same events with a chat id are stored,
// and a PersonInfo, which names no chat, is too.
func TestEmitChatRefusesNoChatID(t *testing.T) {
	b, st, out := logBackend(t)
	fw := b.newInstance()
	ref := func(chat string) *contract.MessageRef { return &contract.MessageRef{ChatId: chat, MessageId: "m"} }
	scoped := func(chat string) []isChatEventKind {
		return []isChatEventKind{
			&contract.Message{ChatId: chat, MessageId: "m"},
			&contract.Edit{Target: ref(chat)},
			&contract.Reaction{Target: ref(chat), Emoji: "x"},
			&contract.MessageDeleted{Target: ref(chat)},
			&contract.ReadUpTo{ChatId: chat, UpToMessageId: "m"},
			&contract.Delivered{Target: ref(chat)},
			&contract.ChatInfo{ChatId: chat},
			&contract.ChatRemoved{ChatId: chat},
			&contract.HistoryBatch{ChatId: chat},
			&contract.ChatAbilities{ChatId: chat},
		}
	}
	for i, k := range append(scoped(""), &contract.Edit{}, &contract.Reaction{}) {
		if seq := fw.emitChat("acct", fmt.Sprintf("e%d", i), k); seq != 0 {
			t.Errorf("%T with no chat id stored (seq %d)", k, seq)
		}
	}
	if evs := outboxEvents(t, st); len(evs) != 0 {
		t.Fatalf("stored: %v", evs)
	}
	if c := fw.noChatN.Load(); c != 12 {
		t.Errorf("%d refusals counted, want 12", c)
	}
	if c := strings.Count(out.String(), `"code":"chat_event_without_chat_id"`); c != 1 {
		t.Errorf("chat_event_without_chat_id logged %d times by one instance, want once", c)
	}
	for i, k := range append(scoped("chat:1"), &contract.PersonInfo{PersonId: "p"}) {
		if seq := fw.emitChat("acct", fmt.Sprintf("f%d", i), k); seq == 0 {
			t.Errorf("%T not stored", k)
		}
	}
	if evs := chatEventsOf(outboxEvents(t, st)); len(evs) != 11 {
		t.Errorf("%d stored, want 11", len(evs))
	}
	for _, k := range []isChatEventKind{&contract.PersonInfo{}, &contract.ConnectionState{}, &contract.SignInStep{}, &contract.SendResolved{}} {
		if _, isScoped := chatScoped(k); isScoped {
			t.Errorf("%T taken for an event about a chat", k)
		}
	}
}

// lateMessage stores the framework's row of a message of key and has the
// ghost "a" send it into room, as the framework does for an event.
func lateMessage(t *testing.T, fw *instance, bdb *database.Database, key networkid.PortalKey, room id.RoomID, msg networkid.MessageID, at time.Time) {
	t.Helper()
	insertMessage(t, bdb, key, msg, at)
	if _, err := fw.GhostIntent("a").SendMessage(context.Background(), room, event.EventMessage,
		&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: string(msg)}},
		&bridgev2.MatrixSendExtra{Timestamp: at, MessageMeta: &database.Message{ID: msg, SenderID: "a", Room: key}}); err != nil {
		t.Fatal(err)
	}
}

// chatKinds names the chat events from the n-th on, each with its chat id.
func chatKinds(t *testing.T, st *store.Store, n int) []string {
	t.Helper()
	var kinds []string
	for _, e := range chatEventsOf(outboxEvents(t, st)[n:]) {
		switch {
		case e.GetChatRemoved() != nil:
			kinds = append(kinds, "removed:"+e.GetChatRemoved().GetChatId())
		case e.GetChatInfo() != nil:
			kinds = append(kinds, "info:"+e.GetChatInfo().GetChatId()+":"+e.GetChatInfo().GetName())
		case e.GetMessage() != nil:
			kinds = append(kinds, "msg:"+e.GetMessage().GetChatId()+":"+e.GetMessage().GetMessageId())
		default:
			kinds = append(kinds, fmt.Sprintf("other:%v", e))
		}
	}
	return kinds
}

// TestSupersededRoomLiveWhenAlone: the late basic group's superseded room
// (chat:5's portal, superseded by the live room of chat:5 whose portal was
// re-IDed away) stays superseded, its events dropped, while that live room
// stands for a portal, wherever that portal moves. Once that portal is
// gone, the superseded room is the only room of chat:5, and its next event
// makes it the live room of chat:5 (liveAgain): its ChatInfo, then the
// event, under chat:5. The live room left either by its DeleteRoom (its
// ChatRemoved reported then) or with none (a merge cut off between the
// framework's two steps): then it is reported ChatRemoved first, and
// marked removed, once (orphan_room_removed). Before the fix the room
// stayed superseded for good.
func TestSupersededRoomLiveWhenAlone(t *testing.T) {
	for _, deleteRoom := range []bool{true, false} {
		t.Run(fmt.Sprintf("DeleteRoom=%v", deleteRoom), func(t *testing.T) {
			ctx := context.Background()
			b, st, out, bdb, live := reIDed(t)
			fw := b.newInstance()
			base := time.Unix(1795000000, 0)
			alt := fw.GenerateDeterministicRoomID(oldKey5)
			if _, err := fw.BotIntent().CreateRoom(ctx, createReq(alt, "Mini group", formatGhost("a"))); err != nil {
				t.Fatal(err)
			}
			if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: oldKey5, MXID: alt}); err != nil {
				t.Fatal(err)
			}
			// The live room's portal moves on: it still stands for one.
			movedKey := networkid.PortalKey{ID: "channel:6", Receiver: "acct"}
			if err := bdb.Portal.ReID(ctx, newKey5, movedKey); err != nil {
				t.Fatal(err)
			}
			n := len(outboxEvents(t, st))
			lateMessage(t, fw, bdb, oldKey5, alt, "late-1", base)
			if got := chatKinds(t, st, n); len(got) != 0 {
				t.Fatalf("events while the live room stands for a portal: %v", got)
			}
			if r, _ := st.Room(ctx, string(alt)); r == nil || !r.Superseded {
				t.Fatalf("the late room while the live room stands for a portal: %+v", r)
			}

			// The live room's portal goes.
			if err := bdb.Portal.Delete(ctx, movedKey); err != nil {
				t.Fatal(err)
			}
			if deleteRoom {
				if err := fw.BotIntent().DeleteRoom(ctx, live, false); err != nil {
					t.Fatal(err)
				}
			}
			n = len(outboxEvents(t, st))
			lateMessage(t, fw, bdb, oldKey5, alt, "late-2", base.Add(time.Second))
			want := "[info:chat:5:Mini group msg:chat:5:late-2]"
			if !deleteRoom {
				want = "[removed:chat:5 info:chat:5:Mini group msg:chat:5:late-2]"
			}
			if got := fmt.Sprint(chatKinds(t, st, n)); got != want {
				t.Fatalf("the late room's next event: %s, want %s", got, want)
			}
			r, _ := st.Room(ctx, string(alt))
			if r == nil || r.Superseded || r.ChatID != "chat:5" || r.Deleted {
				t.Fatalf("the late room: %+v, want live under chat:5", r)
			}
			if old, _ := st.Room(ctx, string(live)); old == nil || !old.Deleted {
				t.Fatalf("the room that left: %+v, want removed", old)
			}
			if named, _ := st.RoomOfChat(ctx, "chat:5", "acct"); named == nil || named.RoomID != string(alt) {
				t.Fatalf("chat:5 names %+v, want the late room", named)
			}
			wantLogged := 1
			if deleteRoom {
				wantLogged = 0
			}
			if c := strings.Count(out.String(), `"code":"orphan_room_removed"`); c != wantLogged {
				t.Errorf("orphan_room_removed logged %d times, want %d", c, wantLogged)
			}
			// The next event is a message like any; a late DeleteRoom of the
			// room that left reports nothing again.
			n = len(outboxEvents(t, st))
			if err := fw.BotIntent().DeleteRoom(ctx, live, false); err != nil {
				t.Fatal(err)
			}
			lateMessage(t, fw, bdb, oldKey5, alt, "late-3", base.Add(2*time.Second))
			if got := fmt.Sprint(chatKinds(t, st, n)); got != "[msg:chat:5:late-3]" {
				t.Errorf("after: %s", got)
			}
		})
	}
}

// liveUnder makes room a live room of chatID the way the framework's moves
// do: a superseded room of from (remembered, created) made live under
// chatID (store.SupersededRoomLive), standing for the portal of key.
func liveUnder(t *testing.T, st *store.Store, bdb *database.Database, room id.RoomID, from, chatID string, key networkid.PortalKey) {
	t.Helper()
	ctx := context.Background()
	if err := st.RememberSupersededRoom(ctx, string(room), from, "acct"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RoomCreated(ctx, string(room), false, false); err != nil {
		t.Fatal(err)
	}
	if made, err := st.SupersededRoomLive(ctx, string(room), chatID); err != nil || !made {
		t.Fatalf("made live: %v %v", made, err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: key, MXID: room}); err != nil {
		t.Fatal(err)
	}
}

// TestSupersededKeyBeforeCreation: before the framework creates a portal's
// room it updates the profiles of the room's people (the creating event's
// profiles: the portal has no room yet, updateGhostProfile asks
// supersededKey). The room it will be given is superseded not only when
// the room id of its key is another portal's, but also when a live room of
// the login that stands for a portal has the key's chat id (a superseded
// room made live under it, whose portal moved on): store.RoomCreated makes
// the new room a superseded room then, and supersededKey must say so, or
// the creating event's profiles are sent. Its answer matches what the
// creation does. A live room of that id whose portal is gone is no
// candidate: the room is not superseded, and the creation removes that
// room (ChatRemoved) and makes the new one the chat. Before the fix
// supersededKey answered false in the first case.
func TestSupersededKeyBeforeCreation(t *testing.T) {
	for _, gone := range []bool{false, true} {
		t.Run(fmt.Sprintf("gone=%v", gone), func(t *testing.T) {
			ctx := context.Background()
			b, st, _, bdb := frameworkBackend(t)
			fw := b.newInstance()
			holder := supersededRoomIDFor(networkid.PortalKey{ID: "chat:9", Receiver: "acct"}, 0)
			holderKey := networkid.PortalKey{ID: "chat:6", Receiver: "acct"}
			liveUnder(t, st, bdb, holder, "chat:9", "chat:5", holderKey)
			if gone {
				if err := bdb.Portal.Delete(ctx, holderKey); err != nil {
					t.Fatal(err)
				}
			}
			// The framework keeps the new portal's row before its room.
			if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: oldKey5}); err != nil {
				t.Fatal(err)
			}
			if _, superseded := fw.supersededKey(ctx, oldKey5); superseded == gone {
				t.Fatalf("supersededKey of chat:5 before its room: %v, want %v", superseded, !gone)
			}
			n := len(outboxEvents(t, st))
			room := fw.GenerateDeterministicRoomID(oldKey5)
			if room != roomIDFor(oldKey5) {
				t.Fatalf("the room id %s, want chat:5's own", room)
			}
			if _, err := fw.BotIntent().CreateRoom(ctx, createReq(room, "Five", formatGhost("a"))); err != nil {
				t.Fatal(err)
			}
			r, _ := st.Room(ctx, string(room))
			if r == nil || r.Superseded == gone {
				t.Fatalf("the created room %+v: superseded should be %v", r, !gone)
			}
			want := "[]"
			if gone {
				want = "[removed:chat:5 info:chat:5:Five]"
			}
			if got := fmt.Sprint(chatKinds(t, st, n)); got != want {
				t.Errorf("the creation reported %s, want %s", got, want)
			}
		})
	}
}

// TestRecreatedRoomForgetsOldKey: a room whose portal was re-IDed (its row
// followed the portal: portal_id channel:5) and removed is made again for
// a new portal of its own key, chat:5, which gets its room id again. Its
// row stands for that portal: its portal id is the chat's own (no lookup
// reads channel:5, the key of a portal that is gone). A superseded id whose
// row was a chat of the host (made live and removed since) is not given to
// a superseded room again: it keeps that chat's id, and the next id is
// given. Before the fix the row kept channel:5, and the id was reused.
func TestRecreatedRoomForgetsOldKey(t *testing.T) {
	ctx := context.Background()
	b, st, _, bdb, room := reIDed(t)
	fw := b.newInstance()
	before, _ := st.Room(ctx, string(room))
	fw.portalOfRoom(ctx, before)
	if before.PortalID != "channel:5" {
		t.Fatalf("the re-IDed room's row %+v, want it to follow channel:5", before)
	}
	// A superseded id of chat:5 that was the chat channel:7 and is removed.
	lived := supersededRoomIDFor(oldKey5, 0)
	liveUnder(t, st, bdb, lived, "chat:5", "channel:7", networkid.PortalKey{ID: "channel:7", Receiver: "acct"})
	if err := bdb.Portal.Delete(ctx, networkid.PortalKey{ID: "channel:7", Receiver: "acct"}); err != nil {
		t.Fatal(err)
	}
	if err := fw.BotIntent().DeleteRoom(ctx, lived, false); err != nil {
		t.Fatal(err)
	}
	if alt := fw.GenerateDeterministicRoomID(oldKey5); alt != supersededRoomIDFor(oldKey5, 1) {
		t.Errorf("the superseded id given %s, want the next one past the removed chat's %s", alt, lived)
	}

	// The re-IDed room's portal goes, and its room; a new portal of chat:5.
	if err := bdb.Portal.Delete(ctx, newKey5); err != nil {
		t.Fatal(err)
	}
	if err := fw.BotIntent().DeleteRoom(ctx, room, false); err != nil {
		t.Fatal(err)
	}
	again := fw.GenerateDeterministicRoomID(oldKey5)
	if again != room {
		t.Fatalf("the new portal's room id %s, want %s again", again, room)
	}
	if _, err := fw.BotIntent().CreateRoom(ctx, createReq(again, "Five again", formatGhost("a"))); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: oldKey5, MXID: again}); err != nil {
		t.Fatal(err)
	}
	r, _ := st.Room(ctx, string(again))
	if r == nil || r.PortalID != "chat:5" || r.ReIDed() || r.Superseded || r.Deleted || r.ChatID != "chat:5" || r.CreatedGen != 2 {
		t.Fatalf("the room made again: %+v, want chat:5's, standing for chat:5, generation 2", r)
	}
	if got := fw.keyOfRoom(ctx, r); got != oldKey5 {
		t.Errorf("its key %v", got)
	}
}

// TestSweepOrphanRooms: at a start, a live room that no portal row has and
// whose key's portal is gone (the source of a merge cut off after the
// framework deleted its portal's row, before its room was deleted) is
// reported ChatRemoved and marked removed, once. A room that stands for a
// portal is kept, and so is one whose key's portal row has no room yet (a
// creation cut off before the framework kept the room id: made again).
func TestSweepOrphanRooms(t *testing.T) {
	ctx := context.Background()
	b, st, out, bdb := frameworkBackend(t)
	fw := b.newInstance()
	source := networkid.PortalKey{ID: "chat:1", Receiver: "acct"}
	orphan := madeRoom(t, fw, bdb, source, "Source")
	kept := madeRoom(t, fw, bdb, networkid.PortalKey{ID: "chat:2", Receiver: "acct"}, "Target")
	pendingKey := networkid.PortalKey{ID: "chat:3", Receiver: "acct"}
	pending := fw.GenerateDeterministicRoomID(pendingKey)
	if _, err := fw.BotIntent().CreateRoom(ctx, createReq(pending, "Pending")); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.Insert(ctx, &database.Portal{PortalKey: pendingKey}); err != nil {
		t.Fatal(err)
	}
	if err := bdb.Portal.Delete(ctx, source); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		n := len(outboxEvents(t, st))
		b.newInstance().sweepOrphanRooms(ctx)
		want := "[]"
		if i == 0 {
			want = "[removed:chat:1]"
		}
		if got := fmt.Sprint(chatKinds(t, st, n)); got != want {
			t.Fatalf("sweep %d: %s, want %s", i, got, want)
		}
	}
	if r, _ := st.Room(ctx, string(orphan)); r == nil || !r.Deleted {
		t.Errorf("the orphan %+v, want removed", r)
	}
	for _, room := range []id.RoomID{kept, pending} {
		if r, _ := st.Room(ctx, string(room)); r == nil || r.Deleted {
			t.Errorf("%s: %+v, want kept", room, r)
		}
	}
	if c := strings.Count(out.String(), `"code":"orphan_room_removed"`); c != 1 {
		t.Errorf("orphan_room_removed logged %d times, want once", c)
	}
}

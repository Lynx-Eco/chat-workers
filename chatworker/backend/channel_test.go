// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
)

// TestChannelQueryAfterReID: the hook is asked about a portal the framework
// gave a new id after its room was made (bridgev2 ReIDPortal: the row's id
// changes, its room id, type and metadata stay) with its current key and
// metadata and the id its room was first reported under, and the worker's
// row of the room follows the new key (its chat id stays). It is told
// whether the room is made and reads the room's stored power levels. A
// portal whose room the worker has no row of is not a channel by the hook;
// a space is one, a direct chat never. A stopped instance's follow writes
// nothing after a wipe.
func TestChannelQueryAfterReID(t *testing.T) {
	ctx := context.Background()
	b, st := testBackend(t)
	var asked []ChannelQuery
	b.hooks.IsChannel = func(q ChannelQuery) bool { asked = append(asked, q); return true }
	oldKey := networkid.PortalKey{ID: "chat:5", Receiver: "acct"}
	newKey := networkid.PortalKey{ID: "channel:5", Receiver: "acct"}
	ownKey := networkid.PortalKey{ID: "channel:6", Receiver: "acct"}
	portal := func(key networkid.PortalKey, mxid id.RoomID, rt database.RoomType) *database.Portal {
		return &database.Portal{PortalKey: key, MXID: mxid, RoomType: rt, Name: "Group", Metadata: map[string]any{"last_sync": 1795000000}}
	}
	fw := b.newInstance()
	room := fw.GenerateDeterministicRoomID(oldKey)
	_ = fw.GenerateDeterministicRoomID(ownKey)
	if err := st.PutState(ctx, string(room), event.StatePowerLevels.Type, "",
		[]byte(`{"users":{"`+string(formatGhost("chan-ghost"))+`":75}}`)); err != nil {
		t.Fatal(err)
	}
	ask := func(name string, p *database.Portal, want bool) *ChannelQuery {
		t.Helper()
		asked = nil
		if got := fw.portalIsChannel(ctx, p, nil); got != want {
			t.Errorf("%s: %v, want %v", name, got, want)
		}
		if len(asked) == 0 {
			return nil
		}
		return &asked[0]
	}
	q := ask("re-IDed", portal(newKey, room, database.RoomTypeDefault), true)
	if q == nil || q.ReIDedFrom != "chat:5" || !q.Room || q.Portal.ID != "channel:5" || q.UserLevel("chan-ghost") != 75 || q.UserLevel("other") != 0 {
		t.Fatalf("re-IDed: asked %+v", q)
	}
	r, err := st.Room(ctx, string(room))
	if err != nil || r == nil || r.ChatID != "chat:5" || r.PortalID != "channel:5" || !r.ReIDed() {
		t.Fatalf("the room's row after the re-ID: %+v %v, want chat chat:5 standing for channel:5", r, err)
	}
	if q := ask("its own id", portal(ownKey, roomIDFor(ownKey), database.RoomTypeDefault), true); q == nil || q.ReIDedFrom != "" || !q.Room {
		t.Fatalf("its own id: asked %+v", q)
	}
	if q := ask("no room yet", portal(newKey, "", database.RoomTypeDefault), true); q == nil || q.Room || q.ReIDedFrom != "" || q.UserLevel("chan-ghost") != 0 {
		t.Fatalf("no room yet: asked %+v", q)
	}
	if q := ask("a room the worker has no row of", portal(newKey, "!unknown:lynx.invalid", database.RoomTypeDefault), false); q != nil {
		t.Fatalf("a room the worker has no row of: asked %+v", q)
	}
	ask("re-IDed space", portal(newKey, room, database.RoomTypeSpace), true)
	ask("direct chat", portal(ownKey, roomIDFor(ownKey), database.RoomTypeDM), false)
	ask("none", nil, false)
	b.hooks.IsChannel = nil
	ask("without a hook", portal(ownKey, roomIDFor(ownKey), database.RoomTypeDefault), false)
	b.hooks.IsChannel = func(q ChannelQuery) bool { return true }

	// After a wipe the stopped instance's follow writes nothing.
	if err := st.Wipe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.RememberRoom(ctx, string(room), "chat:5", "acct"); err != nil {
		t.Fatal(err)
	}
	fw.portalIsChannel(ctx, portal(newKey, room, database.RoomTypeDefault), nil)
	if r, _ := st.Room(ctx, string(room)); r == nil || r.PortalID != "chat:5" {
		t.Fatalf("a stopped instance moved the room after the wipe: %+v", r)
	}
	b.newInstance().portalIsChannel(ctx, portal(newKey, room, database.RoomTypeDefault), nil)
	if r, _ := st.Room(ctx, string(room)); r == nil || r.PortalID != "channel:5" {
		t.Fatalf("the new instance did not move the room: %+v", r)
	}
}

// TestEnsurePersonOrder: two rooms' events name the same person, whose
// PersonInfo is not reported yet. The first event's lookup is held inside
// its write of the PersonInfo; the second waits for it and does not pass
// it: the PersonInfo is stored before both events.
func TestEnsurePersonOrder(t *testing.T) {
	ctx := context.Background()
	b, st, _ := logBackend(t)
	fw := b.newInstance()
	if !fw.ensureSelf("acct", nil) {
		t.Fatal("own PersonInfo not stored")
	}
	if err := fw.GhostIntent("u-p").SetDisplayName(ctx, "P"); err != nil {
		t.Fatal(err)
	}
	mxid := formatGhost("u-p")
	inA, release := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	b.writeHook = func() {
		if held.CompareAndSwap(false, true) {
			close(inA)
			<-release
		}
	}
	use := func(msg string, done chan struct{}) {
		defer close(done)
		fw.ensurePerson(ctx, "acct", mxid)
		fw.emitChat("acct", eventID("msg", msg), &contract.Message{ChatId: msg, MessageId: msg})
	}
	aDone, bDone, bWaits := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go use("a", aDone)
	<-inA
	b.ensureHook = func() { close(bWaits) }
	go use("b", bDone)
	select {
	case <-bWaits:
	case <-bDone:
	}
	close(release)
	<-aDone
	<-bDone
	var order []string
	for _, e := range outboxEvents(t, st) {
		switch {
		case e.GetPersonInfo().GetIsSelf():
		case e.GetPersonInfo() != nil:
			order = append(order, "person")
		case e.GetMessage() != nil:
			order = append(order, "msg"+e.GetMessage().GetMessageId())
		}
	}
	if len(order) != 3 || order[0] != "person" {
		t.Fatalf("outbox %v, want the person before both messages", order)
	}
}

// TestRefusedReportsMadeAgain: while the account's own PersonInfo cannot
// be stored, a room's ChatInfo, its abilities and the PersonInfo of its
// member are refused (emitChat) and logged chat_event_refused once; none
// of them is recorded as reported. Once the outbox takes events again, the
// room's next event reports them, in order, before itself.
func TestRefusedReportsMadeAgain(t *testing.T) {
	ctx := context.Background()
	b, st, out := logBackend(t)
	fw := b.newInstance()
	if _, err := st.DB().Exec(ctx, `CREATE TRIGGER test_fail_self BEFORE INSERT ON lynx_outbox
		WHEN instr(NEW.event, CAST('self:' AS BLOB)) > 0 BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if err := fw.GhostIntent("u-p").SetDisplayName(ctx, "P"); err != nil {
		t.Fatal(err)
	}
	room := fw.GenerateDeterministicRoomID(networkid.PortalKey{ID: "grp", Receiver: "acct"})
	features := &event.Event{Type: event.StateBeeperRoomFeatures, StateKey: new(string),
		Content: event.Content{Parsed: &event.RoomFeatures{ID: "caps", Reply: event.CapLevelFullySupported}}}
	if _, err := fw.BotIntent().CreateRoom(ctx, &mautrix.ReqCreateRoom{
		BeeperLocalRoomID: room, InitialState: []*event.Event{features},
		BeeperInitialMembers: []id.UserID{formatGhost("u-p")},
	}); err != nil {
		t.Fatal(err)
	}
	if n := outboxLen(t, st); n != 0 {
		t.Fatalf("%d events stored while the own PersonInfo failed", n)
	}
	if n := strings.Count(out.String(), `"code":"chat_event_refused"`); n != 1 {
		t.Errorf("%d lines chat_event_refused, want 1", n)
	}
	if _, err := st.DB().Exec(ctx, `DROP TRIGGER test_fail_self`); err != nil {
		t.Fatal(err)
	}
	if _, err := fw.GhostIntent("u-p").SendMessage(ctx, room, event.EventMessage,
		&event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: "hi"}},
		&bridgev2.MatrixSendExtra{Timestamp: time.Now(), MessageMeta: &database.Message{ID: "m1", SenderID: "u-p"}}); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, e := range outboxEvents(t, st) {
		switch {
		case e.GetPersonInfo().GetIsSelf():
			order = append(order, "self")
		case e.GetPersonInfo() != nil:
			order = append(order, "person:"+e.GetPersonInfo().GetName())
		case e.GetChatInfo() != nil:
			order = append(order, "chat")
		case e.GetChatAbilities() != nil:
			order = append(order, "abilities")
		case e.GetMessage() != nil:
			order = append(order, "msg")
		}
	}
	if strings.Join(order, " ") != "self person:P chat abilities msg" {
		t.Fatalf("outbox %v, want self, the person, the chat, its abilities, the message", order)
	}
}

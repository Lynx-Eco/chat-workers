// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/json"
	"reflect"
	"testing"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
)

// storedPortal is a portal as bridgev2 reads it back from its table, its id
// made by the connector from the chat's JID (waid.MakePortalID) and its
// metadata (meta; nil: a new row's empty metadata) through JSON into the
// type the connector's GetDBMetaTypes makes.
func storedPortal(t *testing.T, jid types.JID, rt database.RoomType, meta *waid.PortalMetadata) *database.Portal {
	t.Helper()
	return storedPortalID(t, waid.MakePortalID(jid), rt, meta)
}

func storedPortalID(t *testing.T, id networkid.PortalID, rt database.RoomType, meta *waid.PortalMetadata) *database.Portal {
	t.Helper()
	if meta == nil {
		meta = &waid.PortalMetadata{}
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded := (&connector.WhatsAppConnector{}).GetDBMetaTypes().Portal()
	if err := json.Unmarshal(raw, decoded); err != nil {
		t.Fatal(err)
	}
	return &database.Portal{PortalKey: networkid.PortalKey{ID: id}, RoomType: rt, Metadata: decoded}
}

var (
	newsletterJID = types.NewJID("120363144038483540", types.NewsletterServer)
	groupJID      = types.NewJID("120363025246125486", types.GroupServer)
	communityJID  = types.NewJID("120363025246125999", types.GroupServer)
	phoneJID      = types.NewJID(testPhone, types.DefaultUserServer)
	lidJID        = types.NewJID(testLID, types.HiddenUserServer)
)

// TestBroadcastFeed: a newsletter (WhatsApp Channel) and the status
// broadcast are channels, from the portal's id alone, before and after its
// room is made; nothing else is. WHY: catches the hook left out or keyed on
// the wrong server (a newsletter's posts ingested as a group's messages),
// one that calls every @broadcast or a group a channel, and one that calls
// a community's announcement group (is_cag) or an admins-only group a
// channel (their members' messages would be dropped).
func TestBroadcastFeed(t *testing.T) {
	cag := &waid.PortalMetadata{CommunityAnnouncementGroup: true, AddressingMode: types.AddressingModeLID}
	for _, c := range []struct {
		name   string
		portal *database.Portal
		want   bool
	}{
		{"newsletter", storedPortal(t, newsletterJID, database.RoomTypeDefault, nil), true},
		{"newsletter with metadata", storedPortal(t, newsletterJID, database.RoomTypeDefault, &waid.PortalMetadata{DisappearingTimerSetAt: 1795000000}), true},
		{"status broadcast", storedPortal(t, types.StatusBroadcastJID, database.RoomTypeDefault, nil), true},
		{"broadcast list", storedPortal(t, types.NewJID("1795000000", types.BroadcastServer), database.RoomTypeDefault, nil), false},
		{"group", storedPortal(t, groupJID, database.RoomTypeDefault, nil), false},
		{"group in LID mode", storedPortal(t, groupJID, database.RoomTypeDefault, &waid.PortalMetadata{AddressingMode: types.AddressingModeLID}), false},
		{"community announcement group", storedPortal(t, groupJID, database.RoomTypeDefault, cag), false},
		{"community (space)", storedPortal(t, communityJID, database.RoomTypeSpace, nil), false},
		{"direct chat by phone", storedPortal(t, phoneJID, database.RoomTypeDM, nil), false},
		{"direct chat by LID", storedPortal(t, lidJID, database.RoomTypeDM, nil), false},
		{"bot chat", storedPortal(t, types.NewJID("867051314767696", types.BotServer), database.RoomTypeDM, nil), false},
		{"newsletter as a DM row", storedPortal(t, newsletterJID, database.RoomTypeDM, nil), false},
		{"newsletter as a space row", storedPortal(t, newsletterJID, database.RoomTypeSpace, nil), false},
		{"server only", storedPortalID(t, "newsletter", database.RoomTypeDefault, nil), false},
		{"no user", storedPortalID(t, "@newsletter", database.RoomTypeDefault, nil), false},
		{"not the connector's form", storedPortalID(t, "120363144038483540.0:1@newsletter", database.RoomTypeDefault, nil), false},
		{"bad JID", storedPortalID(t, "1.2.3@newsletter", database.RoomTypeDefault, nil), false},
		{"no portal", nil, false},
	} {
		for _, q := range []backend.ChannelQuery{
			{Portal: c.portal},
			{Portal: c.portal, Room: true, UserLevel: func(networkid.UserID) int { return 0 }},
		} {
			if got := broadcastFeed(q); got != c.want {
				t.Errorf("%s (room %v): %v, want %v", c.name, q.Room, got, c.want)
			}
		}
	}
}

// TestBroadcastFeedReID: a re-IDed portal is a channel only when it was
// first reported as one. The connector re-IDs only a direct chat (phone
// JID to LID), so no newsletter is re-IDed; were a room first reported as
// a direct chat or a group renamed to a newsletter's id, it is not called
// a channel. WHY: catches a hook that ignores ReIDedFrom, which could drop
// the messages of a chat the host already has as a conversation.
func TestBroadcastFeedReID(t *testing.T) {
	p := storedPortal(t, newsletterJID, database.RoomTypeDefault, nil)
	for _, c := range []struct {
		from networkid.PortalID
		want bool
	}{
		{waid.MakePortalID(phoneJID), false},
		{waid.MakePortalID(groupJID), false},
		{"not-a-jid", false},
		{waid.MakePortalID(types.NewJID("120363000000000001", types.NewsletterServer)), true},
	} {
		if got := broadcastFeed(backend.ChannelQuery{Portal: p, ReIDedFrom: c.from}); got != c.want {
			t.Errorf("re-IDed from %q: %v, want %v", c.from, got, c.want)
		}
	}
	// The connector's own re-ID, a direct chat from phone to LID, is a
	// direct chat (the framework never asks the hook of one) and not a
	// channel by the hook either.
	dm := storedPortal(t, lidJID, database.RoomTypeDefault, nil)
	if broadcastFeed(backend.ChannelQuery{Portal: dm, ReIDedFrom: waid.MakePortalID(phoneJID)}) {
		t.Error("a direct chat moved to its LID is a channel")
	}
}

// TestHooksIsChannel: the worker hands the backend the hook. WHY: catches
// the hook left unset (a newsletter would be a group, its posts ingested).
func TestHooksIsChannel(t *testing.T) {
	if h := hooks().IsChannel; h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(broadcastFeed).Pointer() {
		t.Fatal("hooks().IsChannel is not broadcastFeed")
	}
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
)

// storedPortal is a portal as bridgev2 reads it back from its table: the
// metadata the connector wrote (meta: what ExtraUpdates of wrapChatInfo
// sets, chatinfo.go:236; nil: the empty metadata a new portal row gets,
// database.Portal.ensureHasMetadata) goes through JSON into the type the
// connector's GetDBMetaTypes makes.
func storedPortal(t *testing.T, id networkid.PortalID, rt database.RoomType, meta *connector.PortalMetadata) *database.Portal {
	t.Helper()
	if meta == nil {
		meta = &connector.PortalMetadata{}
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	decoded := (&connector.TelegramConnector{}).GetDBMetaTypes().Portal()
	if err := json.Unmarshal(raw, decoded); err != nil {
		t.Fatal(err)
	}
	return &database.Portal{PortalKey: networkid.PortalKey{ID: id}, RoomType: rt, Metadata: decoded}
}

// synced is the metadata after a chat-info sync of a chat whose
// *tg.Channel (or *tg.Chat) has Megagroup = megagroup.
func synced(megagroup bool) *connector.PortalMetadata {
	return &connector.PortalMetadata{IsSuperGroup: megagroup, LastSync: jsontime.U(time.Unix(1795000000, 0)), ParticipantsCount: 42}
}

// inRoom is the query for a portal whose room is made, whose stored power
// levels give the users in levels their level (everyone else 0).
func inRoom(p *database.Portal, levels map[networkid.UserID]int) backend.ChannelQuery {
	return backend.ChannelQuery{Portal: p, Room: true, UserLevel: func(u networkid.UserID) int { return levels[u] }}
}

// broadcastLevels are a broadcast channel room's: the connector makes the
// channel's own ghost a member at superadminPowerLevel (chatinfo.go:196).
func broadcastLevels(id int64) map[networkid.UserID]int {
	return map[networkid.UserID]int{ids.MakeChannelUserID(id): 75}
}

func TestBroadcastChannel(t *testing.T) {
	channel := ids.MakePortalID(ids.PeerTypeChannel, 1001)
	forumGeneral := synced(true)
	forumGeneral.IsForumGeneral = true
	community := synced(false)
	community.IsCommunity = true
	discussion := storedPortal(t, ids.MakePortalID(ids.PeerTypeChannel, 1002), database.RoomTypeDefault, synced(true))
	discussion.ParentKey = networkid.PortalKey{ID: channel}
	for _, c := range []struct {
		name   string
		portal *database.Portal
		want   bool
	}{
		{"broadcast channel", storedPortal(t, channel, database.RoomTypeDefault, synced(false)), true},
		{"channel not synced yet", storedPortal(t, channel, database.RoomTypeDefault, nil), false},
		{"supergroup", storedPortal(t, channel, database.RoomTypeDefault, synced(true)), false},
		{"supergroup not synced yet", storedPortal(t, ids.MakePortalID(ids.PeerTypeChannel, 1003), database.RoomTypeDefault, nil), false},
		{"discussion group of a channel", discussion, false},
		{"basic group", storedPortal(t, ids.MakePortalID(ids.PeerTypeChat, 2001), database.RoomTypeDefault, synced(false)), false},
		{"direct chat", storedPortal(t, ids.MakePortalID(ids.PeerTypeUser, 3001), database.RoomTypeDM, synced(false)), false},
		{"saved messages", storedPortal(t, ids.MakePortalID(ids.PeerTypeUser, 3002), database.RoomTypeDM, &connector.PortalMetadata{LastSync: jsontime.UnixNow()}), false},
		{"forum topic", storedPortal(t, ids.MakeTopicPortalID(1004, 7), database.RoomTypeDefault, synced(true)), false},
		{"topic id without megagroup", storedPortal(t, ids.MakeTopicPortalID(1004, 7), database.RoomTypeDefault, synced(false)), false},
		{"forum general topic", storedPortal(t, ids.MakePortalID(ids.PeerTypeChannel, 1004), database.RoomTypeDefault, forumGeneral), false},
		{"forum space room", storedPortal(t, ids.MakeForumParentPortalID(1004), database.RoomTypeSpace, synced(true)), false},
		{"community", storedPortal(t, ids.MakePortalID(ids.PeerTypeChannel, 1005), database.RoomTypeSpace, community), false},
		{"community not a space", storedPortal(t, ids.MakePortalID(ids.PeerTypeChannel, 1005), database.RoomTypeDefault, community), false},
		{"not a Telegram portal id", storedPortal(t, "nocolon", database.RoomTypeDefault, synced(false)), false},
		{"other metadata", &database.Portal{PortalKey: networkid.PortalKey{ID: channel}, Metadata: &struct{}{}}, false},
		{"no metadata", &database.Portal{PortalKey: networkid.PortalKey{ID: channel}}, false},
		{"no portal", nil, false},
	} {
		// In a room whose power levels have the channel's own ghost as a
		// broadcast channel's do, and before its room is made.
		if got := broadcastChannel(inRoom(c.portal, broadcastLevels(1001))); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
		if got := broadcastChannel(backend.ChannelQuery{Portal: c.portal}); got != c.want {
			t.Errorf("%s, no room yet: %v, want %v", c.name, got, c.want)
		}
	}
}

// TestBroadcastNeedsOwnGhost: once its room is made, a synced channel peer
// that is not a supergroup is a channel only when the room's power levels
// give the channel's own ghost the superadmin level, which the connector
// sets only for chat.Broadcast. A channel-type peer that is neither a
// broadcast channel nor a megagroup (a monoforum, if Telegram does not set
// megagroup on it) has no such member and is a group; so is one whose own
// ghost has the anonymous admin's level of a megagroup (41), or whose
// power levels name another channel's ghost.
func TestBroadcastNeedsOwnGhost(t *testing.T) {
	p := storedPortal(t, ids.MakePortalID(ids.PeerTypeChannel, 1001), database.RoomTypeDefault, synced(false))
	for _, c := range []struct {
		name   string
		levels map[networkid.UserID]int
		want   bool
	}{
		{"own ghost superadmin", broadcastLevels(1001), true},
		{"own ghost creator level", map[networkid.UserID]int{ids.MakeChannelUserID(1001): 95}, true},
		{"no own ghost (monoforum)", nil, false},
		{"own ghost anonymous admin", map[networkid.UserID]int{ids.MakeChannelUserID(1001): 41}, false},
		{"another channel's ghost", broadcastLevels(1002), false},
	} {
		if got := broadcastChannel(inRoom(p, c.levels)); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	if broadcastChannel(backend.ChannelQuery{Portal: p, Room: true}) {
		t.Error("a room with no power level lookup is a channel")
	}
}

// TestHooksIsChannel: the worker hands the backend the hook.
func TestHooksIsChannel(t *testing.T) {
	if h := hooks().IsChannel; h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(broadcastChannel).Pointer() {
		t.Fatal("hooks().IsChannel is not broadcastChannel")
	}
}

// TestUpgradedGroupNeverChannel: a basic group upgraded to a supergroup is
// re-IDed by the connector (migrateChat, handletelegram.go:774: bridgev2
// ReIDPortal renames the portal's row to channel:<id> and keeps its room and
// metadata). Until the chat-info sync after it (which can fail) its row
// carries the basic group's metadata (last_sync set, is_supergroup false),
// and after it the supergroup's (is_supergroup true). The room was first
// reported as chat:<id> (ChannelQuery.ReIDedFrom): it is not a channel
// before the sync, after it, nor with power levels that would make a
// channel of it. A channel re-IDed from anything but a basic group is
// asked about on its current metadata.
func TestUpgradedGroupNeverChannel(t *testing.T) {
	newID := ids.MakePortalID(ids.PeerTypeChannel, 2001)
	from := ids.MakePortalID(ids.PeerTypeChat, 3001)
	before := storedPortal(t, newID, database.RoomTypeDefault, synced(false))
	after := storedPortal(t, newID, database.RoomTypeDefault, synced(true))
	for _, c := range []struct {
		name string
		q    backend.ChannelQuery
	}{
		{"before the resync", inRoom(before, nil)},
		{"after the resync", inRoom(after, nil)},
		{"before the resync, own ghost superadmin", inRoom(before, broadcastLevels(2001))},
		{"after the resync, own ghost superadmin", inRoom(after, broadcastLevels(2001))},
	} {
		c.q.ReIDedFrom = from
		if broadcastChannel(c.q) {
			t.Errorf("%s: an upgraded basic group is a channel", c.name)
		}
	}
	// Without the re-ID the same row is a broadcast channel's: the rule is
	// the re-ID's, not the metadata's.
	if !broadcastChannel(inRoom(before, broadcastLevels(2001))) {
		t.Error("the same portal not re-IDed is not a channel: the case is not exercised")
	}
	q := inRoom(before, broadcastLevels(2001))
	q.ReIDedFrom = ids.MakePortalID(ids.PeerTypeChannel, 2002)
	if !broadcastChannel(q) {
		t.Error("a channel re-IDed from another channel id is not asked on its metadata")
	}
	q.ReIDedFrom = "not-a-telegram-id"
	if broadcastChannel(q) {
		t.Error("a portal re-IDed from an id that does not parse is a channel")
	}
}

// TestHooksOriginalMessageID: the hook names the rows the connector
// renames when a basic group upgrades (the id its NamespaceMigratedMessages
// query makes of a basic group's message id: migrated:<id>:<time>), which
// the connector's own ParseMessageID refuses, and gives the id from before
// the rename; a renamed id of another shape is renamed with no original;
// a message id the connector makes for a basic group, a direct chat or a
// channel is not renamed.
func TestHooksOriginalMessageID(t *testing.T) {
	h := hooks()
	if h.OriginalMessageID == nil {
		t.Fatal("no OriginalMessageID hook")
	}
	plain := ids.MakeMessageID(nil, 42)
	renamed := networkid.MessageID(migratedPrefix + string(plain) + ":1795000000000")
	if _, _, err := ids.ParseMessageID(renamed); err == nil {
		t.Fatalf("the connector parses %q: the prefix is not its renamed form", renamed)
	}
	if orig, ok := h.OriginalMessageID(renamed); !ok || orig != plain {
		t.Errorf("%q: %q %v, want %q", renamed, orig, ok, plain)
	}
	for _, id := range []networkid.MessageID{"migrated:", "migrated:42", "migrated:42:", "migrated::1795000000000",
		"migrated:4x2:1795000000000", "migrated:42:17950x", "migrated:42:1795000000000:1", "migrated:7.42:1795000000000", "migrated:-42:1795000000000"} {
		if orig, ok := h.OriginalMessageID(id); !ok || orig != "" {
			t.Errorf("%q: %q %v, want renamed with no original", id, orig, ok)
		}
	}
	for _, id := range []networkid.MessageID{plain, ids.MakeMessageID(int64(7), 42), "call:1", "Migrated:42:1"} {
		if orig, ok := h.OriginalMessageID(id); ok || orig != "" {
			t.Errorf("%q: %q %v, want not renamed", id, orig, ok)
		}
	}
}

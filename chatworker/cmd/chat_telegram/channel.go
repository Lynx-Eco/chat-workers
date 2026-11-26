// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"maunium.net/go/mautrix/bridgev2/database"

	"lynx.invalid/ingest/chatworker/backend"
)

// broadcastChannel is the worker's Hooks.IsChannel: a Telegram broadcast
// channel is a channel (not ingested), although mautrix-telegram v0.2611.0
// bridges it as a room of the default type. The evidence is the
// connector's own, stored with the portal; nothing is asked of the network.
//
// mautrix-telegram keeps no "broadcast" flag. What it stores is
// PortalMetadata.IsSuperGroup, and it takes a channel peer whose
// IsSuperGroup is false for a broadcast channel itself (polls.go:239
// isBroadcastPortal, tomatrix.go:164, handletelegram.go:1241). That flag is
// false too before it is first written, so it counts only when the same
// write set LastSync: both are set by the ExtraUpdates of wrapChatInfo
// (chatinfo.go:236-246, IsSuperGroup = chat.Megagroup of the *tg.Channel,
// LastSync = now), the only writer of LastSync for a portal that is not a
// direct chat (metadata.go:169, called otherwise only by getDMChatInfo,
// chatinfo.go:107). Telegram sets exactly one of broadcast and megagroup on
// a channel (a gigagroup is a megagroup), so a synced channel portal that
// is not a supergroup is a broadcast channel. bridgev2 runs ExtraUpdates
// in Portal.UpdateInfo (portal.go:5427) and saves the portal (5436, as at
// a first creation, where the name is new) before it creates the room
// (createMatrixRoomInLoop: UpdateInfo at portal.go:5542, CreateRoom at
// 5669), and every room creation of a channel portal takes its info from
// GetChatInfo (chatinfo.go:575, wrapFullChatInfo then wrapChatInfo) or
// from a ChatResync built by wrapChatInfo
// (chatsync.go:379, handletelegram.go:190): a broadcast channel is known as
// one from its room's creation. A portal whose metadata was never written
// is a group until it is.
//
// Two more conditions, each on what is stored and with no network call:
//
//   - The room's own channel ghost has the superadmin level (75) in the
//     room's stored power levels. The connector puts that member, the
//     channel's own user id (ids.MakeChannelUserID of the same id) at
//     superadminPowerLevel, into the member list of a *tg.Channel only when
//     chat.Broadcast is set (chatinfo.go:196-200; the levels are at :44),
//     every time it builds the channel's info, so bridgev2 puts it into the
//     power levels the room is created with (getInitialMemberList,
//     portal.go:4873), which the worker stores before the room's first
//     ChatInfo (Intent.CreateRoom). A megagroup's own ghost gets 41 at most
//     (anonymous admin, chatinfo.go:716) and other ghosts are users or other
//     channels. So this rests the answer on Telegram's own broadcast flag,
//     not only on the megagroup flag being unset: a channel-type peer that
//     is neither (a monoforum, a channel's direct messages, if Telegram does
//     not set megagroup on it: not known here, go/README.md) is not called a
//     channel. Before the portal's room is made (ChannelQuery.Room false)
//     there are no power levels; the backend then asks only to hold the
//     profiles of the portal's people, which are reported at their first
//     use in a room that is sent, so that answer can be wrong without loss.
//   - The room was not first reported as a basic group. The connector's only
//     re-ID (migrateChat, handletelegram.go:748, from both callers at :674,
//     :682 and chatsync.go:299) renames a basic group's portal (chat:<id>)
//     to the supergroup it was upgraded to (channel:<id>), keeping the basic
//     group's metadata until the chat-info sync after it (which can fail,
//     handletelegram.go:781) rewrites it, and Telegram upgrades a basic group
//     only to a supergroup (messages.migrateChat), never to a broadcast
//     channel: a room first reported as chat:<id> is never a channel, before
//     that sync or after it. A real group is never dropped by this rule,
//     which only ever says "not a channel".
//
// Not channels: a basic group (peer "chat"), a direct chat and "saved
// messages" (peer "user", RoomTypeDM), a supergroup and a discussion group
// linked to a channel (megagroups: IsSuperGroup), a forum's topic (its id
// has a topic, and only a megagroup has topics) and its General topic
// (IsForumGeneral, a megagroup), a community (IsCommunity; it is a space,
// a channel by the space rule already) and a forum's own space room (a
// topic id of -1; a space too).
func broadcastChannel(q backend.ChannelQuery) bool {
	p := q.Portal
	if p == nil || p.RoomType != database.RoomTypeDefault || !strings.Contains(string(p.ID), ":") {
		return false
	}
	peer, chanID, topic, err := ids.ParsePortalID(p.ID)
	if err != nil || peer != ids.PeerTypeChannel || topic != 0 {
		return false
	}
	if q.ReIDedFrom != "" {
		// ParsePortalID panics on an id without a colon.
		if !strings.Contains(string(q.ReIDedFrom), ":") {
			return false
		}
		if from, _, _, err := ids.ParsePortalID(q.ReIDedFrom); err != nil || from == ids.PeerTypeChat {
			return false
		}
	}
	meta, ok := p.Metadata.(*connector.PortalMetadata)
	if !ok || meta == nil {
		return false
	}
	if meta.LastSync.IsZero() || meta.IsSuperGroup || meta.IsForumGeneral || meta.IsCommunity {
		return false
	}
	return !q.Room || (q.UserLevel != nil && q.UserLevel(ids.MakeChannelUserID(chanID)) >= broadcastGhostLevel)
}

// broadcastGhostLevel is mautrix-telegram's superadminPowerLevel
// (chatinfo.go:44), the level of a broadcast channel's own ghost.
const broadcastGhostLevel = 75

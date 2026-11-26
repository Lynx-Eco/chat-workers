// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
)

// broadcastFeed is the worker's Hooks.IsChannel: a WhatsApp Channel (a
// newsletter) and the status broadcast are channels (not ingested), as a
// Telegram broadcast channel is in chat_telegram. mautrix-whatsapp v0.2611.0
// bridges both as rooms of the default type (wrapNewsletterInfo,
// chatinfo.go:595; wrapStatusBroadcastInfo, :234).
//
// The evidence is the portal's own id, which the connector makes of the
// chat's JID and nothing else (waid.MakePortalID, waid/id.go:27-29; the
// key, connector/id.go:14-29): a newsletter's is <digits>@newsletter, and
// the status broadcast's is status@broadcast. The JID's server is what
// WhatsApp itself routes by, so the answer needs no metadata and holds from
// the portal's first row; nothing is asked of the network.
//
//   - A newsletter is a one-to-many feed: only its admins post, its
//     followers do not see each other, and the connector sends its posts as
//     the bridge bot, with no person behind them (makeEventSender,
//     connector/id.go:32-35).
//   - The status broadcast is not bridged at all with this worker's config
//     (EnableStatusBroadcast false: handlewhatsapp.go:304, :464,
//     backfill.go:446). Should a portal of it exist anyway, it is a feed of
//     contacts' 24-hour statuses, not a conversation (a reply to a status
//     goes to the direct chat). Broadcast lists are never portals: the
//     connector refuses their chat info (chatinfo.go:43-48), skips them in
//     the history sync (backfill.go:255), and puts a list's message in the
//     sender's direct chat (GetPortalKey, events.go:51-71).
//
// Not channels: a direct chat (by phone number, LID or a bot), a group of
// any kind, among them an announcement-only group (only admins post, but
// its members are the user's own group) and a community's announcement
// group (PortalMetadata.CommunityAnnouncementGroup, chatinfo.go:247-256: a
// group of the community's members, who may reply in threads, :283), which are
// ingested as groups; a community itself is a space (chatinfo.go:370-371),
// a channel by the space rule already. A portal the framework re-IDed is a
// channel only if it was first reported as one too (the connector re-IDs
// only a direct chat, from a phone JID to a LID: lidmigrate.go:110-140).
func broadcastFeed(q backend.ChannelQuery) bool {
	p := q.Portal
	if p == nil || p.RoomType != database.RoomTypeDefault || !feedJID(p.ID) {
		return false
	}
	return q.ReIDedFrom == "" || feedJID(q.ReIDedFrom)
}

// feedJID tells whether a portal id is exactly the connector's id of a
// newsletter or of the status broadcast.
func feedJID(id networkid.PortalID) bool {
	jid, err := waid.ParsePortalID(id)
	if err != nil || waid.MakePortalID(jid) != id {
		return false
	}
	switch jid.Server {
	case types.NewsletterServer:
		return jid.User != ""
	case types.BroadcastServer:
		return jid == types.StatusBroadcastJID
	}
	return false
}

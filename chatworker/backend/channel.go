// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"encoding/json"
	"errors"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// Channels are not sent. The contract's CHAT_KIND_CHANNEL is "a broadcast
// channel or a container of chats. Not ingested as a chat." The worker
// reports a room as CHANNEL when its portal is a space (bridgev2's
// database.RoomTypeSpace: the room is created with the m.space creation
// type, bridgev2 portal.go:5580, and kept as lynx_rooms.is_space), or when
// the connector's hook (Hooks.IsChannel) says so of its portal as stored
// (portalIsChannel: in chat_telegram, a broadcast channel, which
// mautrix-telegram v0.2611.0 makes a default room). A direct chat is never
// a channel; the hook is told whether the framework re-IDed the portal
// (reid.go).
// Of a channel room the worker emits its ChatInfo (and ChatRemoved), and
// nothing else: no message, edit, reaction, deletion, read, delivery,
// history batch or abilities, no spool row of an attachment uploaded for
// it, and no PersonInfo of its people. Each drop is counted
// (instance.channelDrops) and logged channel_event_dropped once per room
// and instance. Commands that target a channel are refused
// (channel_not_ingested).
//
// The kind is looked up for each event, from what is stored, so it holds
// after a restart. A space stays one: bridgev2 does not turn an existing
// room into a space or back (portal.go:5362), and a room created again
// takes its new kind (RoomCreated). The hook's answer can change when the
// connector stores new metadata for the portal (a room is a group until its
// connector's data says otherwise); the instance notes the kind it saw of
// each room (channelRoom), and the first time it looks at a room and when
// the kind changes, the room's ChatInfo is reported (when it changed)
// before the event that looked.
//
// People are reported once they matter in a room that is sent
// (updateGhostProfile, ensurePerson).

// codeChannel refuses a command that targets a channel (send, edit,
// delete, react, mark read, fetch history): OUTCOME_REJECTED, the contract
// having no outcome of its own for "not supported".
const codeChannel = "channel_not_ingested"

// creationIsSpace tells whether a room's creation content makes it a space.
// bridgev2 puts the type there as an event.RoomType (portal.go:5581), which
// never equals the string "m.space" inside an interface: compared so, no
// room was ever a space, and no chat was reported as CHANNEL.
func creationIsSpace(content map[string]any) bool {
	switch t := content["type"].(type) {
	case event.RoomType:
		return t == event.RoomTypeSpace
	case string:
		return t == string(event.RoomTypeSpace)
	}
	return false
}

// ChannelQuery is what the hook Hooks.IsChannel is asked about a portal
// that is neither a space nor a direct chat.
type ChannelQuery struct {
	// Portal is the portal's row as the framework stored it now: its
	// current key and the connector's metadata, decoded.
	Portal *database.Portal
	// ReIDedFrom is, when the framework gave the portal a new id after its
	// room was made (bridgev2 Bridge.ReIDPortal: the row is renamed, its
	// room and metadata are kept), the id the room was first reported
	// under (the chat's id in the contract); "" when it was not re-IDed.
	// What the connector stored before the re-ID was stored for that chat.
	ReIDedFrom networkid.PortalID
	// Room tells whether the portal's room is made; only then does
	// UserLevel read anything. A portal is asked about before its room is
	// made only for the profiles of its people (updateGhostProfile), which
	// are held, not dropped, when it is called a channel: such a profile is
	// reported at its person's first use in a room that is sent.
	Room bool
	// UserLevel is the power level the room's stored power levels give a
	// network user (their users_default when they are not listed; 0 when
	// the room is not made).
	UserLevel func(networkid.UserID) int
}

// portalIsChannel tells whether a stored portal is a channel: a space, or
// a portal the connector's hook calls one. A direct chat never is. A
// portal with a room is asked about with the worker's row of that room
// (room: given while the room is being made, when the portal's row does
// not name it yet; else read by the portal's room id), which follows the
// portal when the framework re-IDed it (followPortal), and with the room's
// power levels. A portal whose room the worker has no row of is not a
// channel by the hook.
func (fw *instance) portalIsChannel(ctx context.Context, p *database.Portal, room *store.Room) bool {
	if p == nil || p.RoomType == database.RoomTypeDM {
		return false
	}
	if p.RoomType == database.RoomTypeSpace {
		return true
	}
	if fw.hooks.IsChannel == nil {
		return false
	}
	if p.MXID != "" && (room == nil || room.RoomID != string(p.MXID)) {
		if room, _ = fw.roomKey(ctx, p.MXID); room == nil {
			return false
		}
	}
	q := ChannelQuery{Portal: p, UserLevel: func(networkid.UserID) int { return 0 }}
	if room != nil {
		fw.followPortal(ctx, room, p)
		if room.ChatID != string(p.ID) {
			q.ReIDedFrom = networkid.PortalID(room.ChatID)
		}
		q.Room = true
		pl, _ := fw.GetPowerLevels(ctx, id.RoomID(room.RoomID))
		q.UserLevel = func(u networkid.UserID) int { return pl.GetUserLevel(formatGhost(u)) }
	}
	return fw.hooks.IsChannel(q)
}

// storedChannel tells whether the portal of key (its current key) is a
// channel, from its row as the framework stored it (read again here: the
// framework's cached portal belongs to its own goroutine). A portal's type
// and the connector's metadata are saved when its info changes (bridgev2
// Portal.UpdateInfo, ExtraUpdates included), before its room is created
// and the profiles of its members are updated, so it is known there before
// the worker's own row has it. Without a framework (tests) only the stored
// room type counts.
func (fw *instance) storedChannel(ctx context.Context, key networkid.PortalKey) bool {
	if br := fw.bridge(); br != nil {
		p, err := br.DB.Portal.GetByKey(ctx, key)
		return err == nil && fw.portalIsChannel(ctx, p, nil)
	}
	return fw.storedSpace(ctx, key)
}

// storedSpace tells whether the portal row of key is a space (tests: no
// framework).
func (fw *instance) storedSpace(ctx context.Context, key networkid.PortalKey) bool {
	var t string
	err := fw.st.DB().QueryRow(ctx, `SELECT room_type FROM portal WHERE id = $1 AND receiver = $2`,
		string(key.ID), string(key.Receiver)).Scan(&t)
	return err == nil && database.RoomType(t) == database.RoomTypeSpace
}

// roomIsChannel tells whether a room is a channel (not ingested) now, from
// the framework's row of the room's portal: the one whose room id it is (a
// re-ID keeps it, and the worker's row then follows the portal's new key),
// or while the room is being made (the portal's row names no room yet) the
// one of the room's key.
func (fw *instance) roomIsChannel(ctx context.Context, room *store.Room) bool {
	if room == nil || room.Direct {
		return false
	}
	if room.Space {
		return true
	}
	if fw.hooks.IsChannel == nil {
		return false
	}
	br := fw.bridge()
	key := networkid.PortalKey{ID: networkid.PortalID(room.PortalID), Receiver: networkid.UserLoginID(room.Receiver)}
	if br == nil {
		return fw.storedSpace(ctx, key)
	}
	p := fw.portalOfRoom(ctx, room)
	if p == nil {
		if p, _ = br.DB.Portal.GetByKey(ctx, key); p == nil || p.MXID != "" {
			return false
		}
	}
	return fw.portalIsChannel(ctx, p, room)
}

// channelRoom tells whether a room is a channel now. The first time the
// instance looks at the room, and whenever its kind is not the one the
// instance saw last, the room's ChatInfo (and, for a room that is sent,
// its abilities) is reported first, when it changed: a room the hook
// turned into a channel is announced as one before its events stop, one
// that stopped being a channel (also while the worker was stopped) before
// its events go again, and a report that was refused (reportRefused) is
// made again.
func (fw *instance) channelRoom(ctx context.Context, roomID id.RoomID, room *store.Room) bool {
	ch := fw.roomIsChannel(ctx, room)
	fw.dropMu.Lock()
	prev, seen := fw.kinds[roomID]
	fw.kinds[roomID] = ch
	fw.dropMu.Unlock()
	if !seen || prev != ch {
		fw.reportChatInfo(ctx, roomID)
		if !ch {
			fw.reportAbilities(ctx, roomID)
		}
	}
	return ch
}

// sawKind records the kind a ChatInfo of the room was built with.
func (fw *instance) sawKind(roomID id.RoomID, ch bool) {
	fw.dropMu.Lock()
	fw.kinds[roomID] = ch
	fw.dropMu.Unlock()
}

// forgetKind makes the room's next event report its ChatInfo and
// abilities again (channelRoom): one of them was refused.
func (fw *instance) forgetKind(roomID id.RoomID) {
	fw.dropMu.Lock()
	delete(fw.kinds, roomID)
	fw.dropMu.Unlock()
}

// channelPortal tells whether a portal is a channel (not ingested).
func (fw *instance) channelPortal(ctx context.Context, p *bridgev2.Portal) bool {
	if p == nil || p.RoomType == database.RoomTypeDM {
		return false
	}
	return p.RoomType == database.RoomTypeSpace || (fw.hooks.IsChannel != nil && fw.storedChannel(ctx, p.PortalKey))
}

// personUnsent stores that a person's stored profile is not reported
// (store.PersonUnsent), unless what was reported last is that profile
// already; their next use in a room that is sent reports it
// (ensurePerson). It holds ensureMu, so an ensurePerson running for the
// person either reports the newer profile or looks again.
func (fw *instance) personUnsent(ctx context.Context, mxid id.UserID, info *contract.PersonInfo) {
	fw.ensureMu.Lock()
	defer fw.ensureMu.Unlock()
	err := fw.write(func() error { return fw.st.PersonInfoUnsent(ctx, string(mxid), contentHash(info)) })
	if err != nil && !errors.Is(err, store.ErrWiped) {
		fw.log.Error("person_store_failed")
	}
	fw.dropMu.Lock()
	delete(fw.checked, mxid)
	fw.dropMu.Unlock()
}

// ensurePerson reports, before an event of a room that is sent which has
// the person as a member, a sender or a mention, the person's stored
// PersonInfo when it is marked as not reported (personUnsent: a profile
// that came without an event of a room that is sent, or a report that was
// refused), through emitChat (the account's own PersonInfo first). The
// owner and the bot are not people. One person is looked up at a time
// (ensureMu): an event for the same person from another room waits until
// the PersonInfo is stored, and does not pass it. A person counts as
// checked in the instance only once nothing of theirs waits (stored, held
// for a sign-in, or nothing to report); a failure looks again next time.
func (fw *instance) ensurePerson(ctx context.Context, receiver networkid.UserLoginID, mxid id.UserID) {
	ghost, ok := parseGhost(mxid)
	if !ok {
		return
	}
	checked := func() bool {
		fw.dropMu.Lock()
		defer fw.dropMu.Unlock()
		return fw.checked[mxid]
	}
	if checked() {
		return
	}
	if fw.ensureHook != nil {
		fw.ensureHook()
	}
	fw.ensureMu.Lock()
	defer fw.ensureMu.Unlock()
	if checked() {
		return
	}
	raw, hash, err := fw.st.Person(ctx, string(mxid))
	if err != nil {
		return
	}
	if raw != nil && hash == store.PersonUnsent {
		p := map[string]any{}
		_ = json.Unmarshal(raw, &p)
		if !fw.reportOrHold(ctx, receiver, mxid, fw.personInfo(ghost, p)) {
			return
		}
	}
	fw.dropMu.Lock()
	fw.checked[mxid] = true
	fw.dropMu.Unlock()
}

// ensureSenders is ensurePerson for the senders of one event.
func (fw *instance) ensureSenders(ctx context.Context, receiver networkid.UserLoginID, in translateInput) {
	fw.ensurePerson(ctx, receiver, in.sender)
	if in.extra == nil {
		return
	}
	if m := in.extra.MessageMeta; m != nil && m.SenderID != "" {
		fw.ensurePerson(ctx, receiver, formatGhost(m.SenderID))
	}
	if r := in.extra.ReactionMeta; r != nil && r.SenderID != "" {
		fw.ensurePerson(ctx, receiver, formatGhost(r.SenderID))
	}
}

// dropChannel counts one event of the channel room (identified by key, not
// logged) that is not sent, and logs the first of each room in the instance.
func (fw *instance) dropChannel(key string) {
	fw.channelDrops.Add(1)
	fw.dropMu.Lock()
	first := !fw.dropped[key]
	fw.dropped[key] = true
	fw.dropMu.Unlock()
	if first {
		fw.log.Info("channel_event_dropped")
	}
}

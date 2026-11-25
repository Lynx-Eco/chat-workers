// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"fmt"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// A chat's id never changes for the host. The framework can give a portal
// a new key after its room was made (bridgev2 Bridge.ReIDPortal; in
// mautrix-telegram v0.2611.0 a basic group upgraded to a supergroup,
// migrateChat, handletelegram.go:748: chat:<id> becomes channel:<id>). It
// renames the portal's row (PortalQuery.ReID; the framework's message,
// reaction and backfill rows follow it by ON UPDATE CASCADE) and keeps the
// room, its id (MXID), its type and the connector's metadata; it calls
// nothing of the MatrixConnector or of an intent (portalreid.go). So the
// worker sees a re-ID only as a portal whose room is one of its rooms
// under another key than the row of that room says, and follows it where
// it maps a room to its portal (portalOfRoom, portalIsChannel) or a chat
// id to its portal (portalFor): the row's portal_id is rewritten through
// the instance's epoch guard (followPortal), and the room's chat_id, the
// id the host was given, stays.
//
// For the host, then: every event of the room keeps its chat id; every
// command that names that chat id reaches the portal under its current
// key; an id the host was never given (the portal's new raw id) names no
// chat (unknown_chat), as any id that is not a room's chat id does; a
// StartChat that reaches the portal answers its chat id. Nothing is sent
// for the re-ID itself: the chat did not change. Its kind is asked of the
// hook again with the portal's current key and metadata and the id it was
// first reported under (ChannelQuery.ReIDedFrom).
//
// A re-ID onto a portal that has a room of its own merges them: bridgev2
// deletes the source portal's row and then, in a goroutine of its own,
// sends a tombstone to the source room and deletes it through the bot
// (Intent.DeleteRoom): the source room is reported ChatRemoved under its
// own chat id, and the target stays the chat it was, under its own.
// A re-ID onto a portal row without a room deletes that row first and
// re-IDs the source as above; one of a portal without a room deletes it
// (no room, nothing reported).
//
// A superseded room. An event that still comes under the old key after the
// re-ID and may create a portal (mautrix-telegram: a message of the basic
// group handled after the channel's MigrateFrom, the two have update
// streams of their own, handletelegram.go:682; a basic group's dialog whose
// MigratedTo is not an InputChannel, chatsync.go:294 and :434) makes the
// framework insert a fresh portal row under the old key and create its
// room. The hash of the old key is the re-IDed room's id, which its portal
// still has: GenerateDeterministicRoomID gives the fresh portal another
// id instead (supersededRoom; deterministic), recorded as a superseded
// room (store.RememberSupersededRoom: no chat id). Its creation succeeds,
// so the framework saves the portal and handles the event (an error there
// would go back to the connector: in mautrix-telegram an error of the
// update handler, which ends its update loop). Nothing of a superseded
// room is ever sent (dropsRoom, reportChatInfo, reportAbilities,
// DeleteRoom, updateGhostProfile): each drop is counted and logged
// superseded_room_event_dropped once per room and instance. No chat id
// names it (store.RoomOfChat), and a StartChat that resolves to its portal
// answers unknown_chat. The live room is not touched.
//
// The framework can end the superseded portal like any: a re-ID of the old
// key onto the live portal again (mautrix-telegram's migrateChat for a
// late MigrateTo) merges it, deleting its row and then its room
// (DeleteRoom: the row is marked deleted, no ChatRemoved: it was never
// reported); a deletion of the portal does the same. When the live portal
// is gone, a later portal of the old key gets the old key's own room id
// again (no portal has it) and is the chat it was, and the superseded room
// of the old key, which its portal still has, becomes the live room of the
// old key's id at its next event (supersededNow): also when the live room
// was left with no DeleteRoom (a merge cut off between the framework's two
// steps: retireOrphan, and at a start sweepOrphanRooms). A re-ID of the
// superseded portal itself onto a key without a portal (the live one
// deleted before: in mautrix-telegram the user left the supergroup, and
// the next dialog sync's checkDialogMigrated, chatsync.go:285, migrates
// the basic group's portal again) makes every event of that key come to
// the superseded room: it becomes a live room, under the chat id of its
// new key (liveAgain). Every check of a room's being superseded asks
// through the follow of its portal (supersededNow), so the first event
// after the move is sent, after the room's ChatInfo.

// maxSuperseded bounds the ids tried for one key (supersededRoomIDFor).
const maxSuperseded = 8

// supersededRoom is, when the room id roomID of key belongs to another
// portal (the framework re-IDed it away from key), the first superseded
// id of key that no other portal has, remembered as a superseded room.
// It is not, and roomID stays the creation's, when the room id is free or
// key's own, when the framework's rows cannot be read, or when every
// superseded id is taken (refuseTakenRoom then refuses the creation). An id
// whose row was a chat of the host (made live, and removed since) is taken
// too: it keeps that chat's id and generations.
func (fw *instance) supersededRoom(ctx context.Context, key networkid.PortalKey, roomID id.RoomID) (id.RoomID, bool) {
	if !fw.takenByOther(ctx, key, roomID) {
		return "", false
	}
	for n := range maxSuperseded {
		alt := supersededRoomIDFor(key, n)
		if fw.takenByOther(ctx, key, alt) {
			continue
		}
		if lived, err := fw.st.RoomLivedAsChat(ctx, string(alt)); err != nil || lived {
			continue
		}
		err := fw.write(func() error {
			return fw.st.RememberSupersededRoom(ctx, string(alt), string(key.ID), string(key.Receiver))
		})
		if err != nil && !errors.Is(err, store.ErrWiped) {
			fw.log.Error("room_remember_failed")
		}
		fw.log.Info("room_superseded")
		return alt, true
	}
	return "", false
}

// takenByOther tells whether a portal row of the framework other than
// key's has the room id roomID.
func (fw *instance) takenByOther(ctx context.Context, key networkid.PortalKey, roomID id.RoomID) bool {
	br := fw.bridge()
	if br == nil {
		return false
	}
	p, err := br.DB.Portal.GetByMXID(ctx, roomID)
	if err != nil {
		fw.log.Warn("room_lookup_failed")
		return false
	}
	return p != nil && p.PortalKey != key
}

// errRoomTaken: the room asked for is another portal's (refuseTakenRoom).
var errRoomTaken = errors.New("the room is another portal's")

// refuseTakenRoom refuses the creation of a room that a portal row of the
// framework already has under a key whose hash the room id is not: the
// case GenerateDeterministicRoomID gave no superseded id for (the
// framework's rows could not be read then, or every superseded id of the
// key was taken). Such a creation would overwrite the other portal's room.
// A row of the key whose hash the id is, is the creating portal's, and is
// let through. A row that has a superseded id of its own key is never
// found here: the framework saves a portal's room id only after its
// CreateRoom (createMatrixRoomInLoop, portal.go:5684) and makes no room
// for a portal that has one (:5514). Counted (takenN) and logged
// room_taken_refused once per room and instance, by its code only.
func (fw *instance) refuseTakenRoom(ctx context.Context, roomID id.RoomID) error {
	br := fw.bridge()
	if br == nil {
		return nil
	}
	p, err := br.DB.Portal.GetByMXID(ctx, roomID)
	if err != nil {
		return err
	}
	if p == nil || roomIDFor(p.PortalKey) == roomID {
		return nil
	}
	fw.takenN.Add(1)
	fw.dropMu.Lock()
	first := !fw.taken[roomID]
	fw.taken[roomID] = true
	fw.dropMu.Unlock()
	if first {
		fw.log.Warn("room_taken_refused")
	}
	return fmt.Errorf("%w: %w", mautrix.MRoomInUse, errRoomTaken)
}

// dropsRoom tells whether an event of the room is not sent, and counts its
// drop: a superseded room's (nothing of it is sent) or a channel's
// (channelRoom, which reports the channel's ChatInfo when it changed).
func (fw *instance) dropsRoom(ctx context.Context, roomID id.RoomID, room *store.Room) bool {
	if fw.supersededNow(ctx, room) {
		fw.dropSuperseded(string(roomID))
		return true
	}
	if fw.channelRoom(ctx, roomID, room) {
		fw.dropChannel(string(roomID))
		return true
	}
	return false
}

// dropSuperseded counts one thing of a superseded room (key: its room id,
// not logged) that is not sent, and logs the first of each room in the
// instance.
func (fw *instance) dropSuperseded(key string) {
	fw.supersededDrops.Add(1)
	fw.dropMu.Lock()
	first := !fw.superseded[key]
	fw.superseded[key] = true
	fw.dropMu.Unlock()
	if first {
		fw.log.Info("superseded_room_event_dropped")
	}
}

// supersededKey is, when the portal of key has or is about to be given a
// superseded room, that room's id: its row's room when that is one. When it
// has no room yet (the event that creates it: the framework updates the
// profiles of its people before it asks for the room's id and creates it,
// and keeps the room id in the portal's row only after), the room it will
// be given is superseded when the room id of key belongs to another portal
// (it gets the first superseded id of key, unless that is taken too), and
// when it does not but a live room of the login that stands for a portal
// has the chat id of key (store.RoomCreated makes the room of key a
// superseded room then; one whose portal is gone is no candidate, and is
// removed before the room is made live: chatHeld). A profile that comes
// with an event of such a portal is held like a channel's
// (updateGhostProfile).
func (fw *instance) supersededKey(ctx context.Context, key networkid.PortalKey) (id.RoomID, bool) {
	br := fw.bridge()
	if br == nil {
		return "", false
	}
	if p, err := br.DB.Portal.GetByKey(ctx, key); err == nil && p != nil && p.MXID != "" {
		room, ok := fw.roomKey(ctx, p.MXID)
		return p.MXID, ok && fw.supersededNow(ctx, room)
	}
	if fw.takenByOther(ctx, key, roomIDFor(key)) {
		return supersededRoomIDFor(key, 0), true
	}
	own := roomIDFor(key)
	if live, err := fw.st.RoomOfChat(ctx, string(key.ID), string(key.Receiver)); err == nil && live != nil && live.RoomID != string(own) {
		return own, fw.chatHeld(ctx, string(key.ID), string(key.Receiver))
	}
	return own, false
}

// portalOfRoom is the framework's row of the portal whose room is room,
// read by the room id (which a re-ID keeps), nil when there is none. The
// room's row is made to follow it (followPortal).
func (fw *instance) portalOfRoom(ctx context.Context, room *store.Room) *database.Portal {
	br := fw.bridge()
	if br == nil || room == nil {
		return nil
	}
	p, err := br.DB.Portal.GetByMXID(ctx, id.RoomID(room.RoomID))
	if err != nil || p == nil {
		return nil
	}
	fw.followPortal(ctx, room, p)
	return p
}

// followPortal rewrites the room's portal id (store.RoomPortalMoved) when
// its portal p, whose room it is, has another id now, and sets it in room.
// The write goes through the instance's epoch guard; one that is refused
// or fails leaves room as it was (the next lookup tries again). A
// superseded room whose portal has another id than the key it was
// superseded under becomes a live room instead (liveAgain).
func (fw *instance) followPortal(ctx context.Context, room *store.Room, p *database.Portal) {
	if room == nil || p == nil || string(p.MXID) != room.RoomID || string(p.ID) == room.PortalID {
		return
	}
	if room.Superseded {
		fw.liveAgain(ctx, room, p)
		return
	}
	err := fw.write(func() error { return fw.st.RoomPortalMoved(ctx, room.RoomID, string(p.ID)) })
	if err != nil {
		if !errors.Is(err, store.ErrWiped) {
			fw.log.Error("room_follow_failed")
		}
		return
	}
	room.PortalID = string(p.ID)
	fw.log.Info("portal_reid_followed")
}

// supersededNow tells whether the room is a superseded room now, asked
// through the follow of its portal (portalOfRoom, followPortal): a
// superseded room the framework moved onto another key is made live there
// first, and is not. One whose portal still has the key it was superseded
// under is made live under that key's id (liveAgain) once no live room of
// the login that stands for a portal has that id (chatHeld): the live room
// it was superseded by was removed (DeleteRoom), or its portal is gone with
// no DeleteRoom (a merge cut off between the framework's two steps), and it
// is then the only room of that id.
func (fw *instance) supersededNow(ctx context.Context, room *store.Room) bool {
	if room == nil || !room.Superseded {
		return false
	}
	p := fw.portalOfRoom(ctx, room)
	if room.Superseded && p != nil && string(p.ID) == room.PortalID && !fw.chatHeld(ctx, room.PortalID, room.Receiver) {
		fw.liveAgain(ctx, room, p)
	}
	return room.Superseded
}

// chatHeld tells whether a live room of the login receiver has chatID and
// stands for a portal. A live room whose portal is gone (portalGone) does
// not. A failed read counts as held.
func (fw *instance) chatHeld(ctx context.Context, chatID, receiver string) bool {
	live, err := fw.st.RoomOfChat(ctx, chatID, receiver)
	if err != nil {
		return true
	}
	return live != nil && !fw.portalGone(ctx, live)
}

// portalGone tells whether the room certainly stands for no portal of the
// framework now: no portal row has its room id, and the portal row of the
// key it last stood for is gone or has another room. A row of that key
// with no room yet that could be making this room again (a creation cut
// off before the framework kept the room id in the row; the room's id is
// one GenerateDeterministicRoomID gives that key) means not gone. A failed
// read is not gone either.
func (fw *instance) portalGone(ctx context.Context, room *store.Room) bool {
	br := fw.bridge()
	if br == nil {
		return false
	}
	p, err := br.DB.Portal.GetByMXID(ctx, id.RoomID(room.RoomID))
	if err != nil || p != nil {
		return false
	}
	key := networkid.PortalKey{ID: networkid.PortalID(room.PortalID), Receiver: networkid.UserLoginID(room.Receiver)}
	kp, err := br.DB.Portal.GetByKey(ctx, key)
	if err != nil {
		return false
	}
	return kp == nil || kp.MXID != "" || !roomOfKey(key, id.RoomID(room.RoomID))
}

// roomOfKey tells whether roomID is one GenerateDeterministicRoomID gives
// key: its own (roomIDFor) or one of its superseded ids.
func roomOfKey(key networkid.PortalKey, roomID id.RoomID) bool {
	if roomIDFor(key) == roomID {
		return true
	}
	for n := range maxSuperseded {
		if supersededRoomIDFor(key, n) == roomID {
			return true
		}
	}
	return false
}

// retireOrphan removes the live room of chatID of the login receiver, other
// than the room except, when its portal is gone (portalGone): it is marked
// removed and reported
// ChatRemoved under its chat id, as a DeleteRoom of it would have been
// (same event id: one that comes after reports nothing again). The write
// goes through the instance's epoch guard, the event through its epoch.
// Logged orphan_room_removed.
func (fw *instance) retireOrphan(ctx context.Context, chatID, receiver, except string) {
	live, err := fw.st.RoomOfChat(ctx, chatID, receiver)
	if err != nil || live == nil || live.RoomID == except || !fw.portalGone(ctx, live) {
		return
	}
	fw.retire(ctx, live)
}

func (fw *instance) retire(ctx context.Context, room *store.Room) {
	if err := fw.write(func() error { return fw.st.RoomDeleted(ctx, room.RoomID) }); err != nil {
		if !errors.Is(err, store.ErrWiped) {
			fw.log.Error("room_remove_failed")
		}
		return
	}
	fw.log.Info("orphan_room_removed")
	receiver := networkid.UserLoginID(room.Receiver)
	if fw.allowed(receiver) {
		fw.emitChat(receiver, fw.genID("chat-removed", room.ChatID, num(room.CreatedGen)), &contract.ChatRemoved{ChatId: room.ChatID})
	}
}

// sweepOrphanRooms removes, at a start, every live room of a login whose
// events are reported that stands for no portal (portalGone): a merge (a
// re-ID onto a portal with a room) that was cut off after the framework
// deleted the source portal's row and before its goroutine deleted the
// room (Intent.DeleteRoom) left the room reported and no portal for it.
// It runs once the framework's tables are open and before any login is
// started (Backend.start): no event is handled and no room made yet, so a
// room no portal row has is certainly gone, not merely not loaded (the
// rows are read from the database, never from the framework's cache). A
// creation cut off before the portal kept its room id leaves the portal's
// row of the key with no room: not gone (portalGone), made again later.
func (fw *instance) sweepOrphanRooms(ctx context.Context) {
	rooms, err := fw.st.LiveRooms(ctx)
	if err != nil {
		fw.log.Error("room_sweep_failed")
		return
	}
	for _, r := range rooms {
		if fw.allowed(networkid.UserLoginID(r.Receiver)) && fw.portalGone(ctx, r) {
			fw.retire(ctx, r)
		}
	}
}

// liveAgain makes the superseded room of the portal p a live room: the
// framework moved p, which the room was made for under the key it was
// superseded under, onto another key, and it does that only when that key
// had no portal (a re-ID onto a portal with a room merges the two and
// deletes this room; one onto a portal row without a room deletes that
// row first): in mautrix-telegram, a late basic group's portal re-IDed
// onto its supergroup's key after the user left the supergroup (whose
// portal and room the framework deleted). Every event of that key now
// comes to this room. Its chat id becomes the new key's id, the chat id a
// room made for that key gets (store.SupersededRoomLive, through the
// instance's epoch guard), unless a live room of the login has that id:
// then it stays superseded, logged superseded_room_kept once per room and
// instance, and its next event tries again. A row of that id whose room
// was never created is not one: the framework's re-ID onto a key whose
// portal row has no room (ReIDResultTargetDeletedAndSourceReIDd) cancels
// a creation in flight of that key's room, after the worker remembered
// its id, and deletes the row without a DeleteRoom; that row stays as it
// is, and is no chat (store.RoomOfChat). No room is made for it later
// while this room's portal has the key (the framework makes none for a
// portal that has a room); once the portal is gone or has moved on, one
// that is made gets event ids past this room's, and is a superseded room
// while this one is live (store.RoomCreated). A removed room of that id
// (the store keeps its row, deleted, with its chat id and generations) was
// reported ChatRemoved when it was deleted (Intent.DeleteRoom): the id is
// reported again as a chat that came back, with event ids of its own.
// The room is then reported as at its first report in an instance
// (channelRoom: its ChatInfo, before which its members are reported, and
// its abilities unless it is a channel), before the event that asked;
// logged superseded_room_live. room is set to the stored row.
func (fw *instance) liveAgain(ctx context.Context, room *store.Room, p *database.Portal) {
	fw.liveMu.Lock()
	defer fw.liveMu.Unlock()
	// A live room of that id whose portal is gone is no candidate for it.
	fw.retireOrphan(ctx, string(p.ID), room.Receiver, room.RoomID)
	var made bool
	err := fw.write(func() error {
		var err error
		made, err = fw.st.SupersededRoomLive(ctx, room.RoomID, string(p.ID))
		return err
	})
	if err != nil {
		if !errors.Is(err, store.ErrWiped) {
			fw.log.Error("room_follow_failed")
		}
		return
	}
	now, ok := fw.roomKey(ctx, id.RoomID(room.RoomID))
	if !ok {
		return
	}
	*room = *now
	if room.Superseded {
		fw.dropMu.Lock()
		first := !fw.kept[room.RoomID]
		fw.kept[room.RoomID] = true
		fw.dropMu.Unlock()
		if first {
			fw.log.Warn("superseded_room_kept")
		}
		return
	}
	if !made {
		// Made live by another caller, which reported it.
		return
	}
	fw.log.Info("superseded_room_live")
	fw.forgetKind(id.RoomID(room.RoomID))
	fw.channelRoom(ctx, id.RoomID(room.RoomID), room)
}

// portalFor is the portal of a chat id the host was given (a room's chat
// id) for the login: the portal whose room that is, under its current key.
// Nil when the id names no room of the login (also the new raw id of a
// re-IDed portal, which no event carried), the room was removed, or its
// portal has no room.
func (b *Backend) portalFor(ctx context.Context, login *bridgev2.UserLogin, chatID string) (*bridgev2.Portal, error) {
	br := b.bridge()
	room, err := b.st.RoomOfChat(ctx, chatID, string(login.ID))
	if err != nil {
		return nil, err
	}
	if room == nil || room.Deleted {
		return nil, nil
	}
	row, err := br.DB.Portal.GetByMXID(ctx, id.RoomID(room.RoomID))
	if err != nil {
		return nil, err
	}
	if row == nil || row.Receiver != login.ID {
		return nil, nil
	}
	b.instanceOf(br).followPortal(ctx, room, row)
	portal, err := br.GetExistingPortalByKey(ctx, row.PortalKey)
	if err != nil {
		return nil, err
	}
	if portal == nil || string(portal.MXID) != room.RoomID {
		return nil, nil
	}
	return portal, nil
}

// chatIDOf is the chat id the host knows a portal with a room by: its
// room's chat id, which a re-ID does not change; the portal's own id when
// the worker has no row of its room; "" for a superseded room (one the
// framework moved onto a key without a portal is made live first, and has
// that key's id: followPortal).
func (b *Backend) chatIDOf(ctx context.Context, portal *bridgev2.Portal) string {
	if room, ok := b.roomKey(ctx, portal.MXID); ok {
		b.instanceOf(portal.Bridge).followPortal(ctx, room, portal.Portal)
		return room.ChatID
	}
	return string(portal.ID)
}

// keyOfRoom is the current portal key of a room: its portal's, read by the
// room id, else the one the room's row has. The read is its own: a caller
// that asked channelRoom first has not always followed the portal (a
// direct chat or a space, or no Hooks.IsChannel, is answered without
// reading it; a follow whose write was refused leaves the row's key).
func (fw *instance) keyOfRoom(ctx context.Context, room *store.Room) networkid.PortalKey {
	if p := fw.portalOfRoom(ctx, room); p != nil {
		return p.PortalKey
	}
	return networkid.PortalKey{ID: networkid.PortalID(room.PortalID), Receiver: networkid.UserLoginID(room.Receiver)}
}

// hostMessageID is the message id the host was given for the framework's
// row id msg: msg, or for a row the connector renamed after its message
// was reported, the id from before the rename (Hooks.OriginalMessageID).
// ok is false for a renamed row whose original id cannot be told: what
// would name it (a read, a delivery, an edit, a reaction, a deletion) is
// not reported. Those are counted (renamedN); the first of an instance is
// logged renamed_target_unmapped.
func (fw *instance) hostMessageID(msg networkid.MessageID) (networkid.MessageID, bool) {
	if fw.hooks.OriginalMessageID == nil {
		return msg, true
	}
	orig, renamed := fw.hooks.OriginalMessageID(msg)
	if !renamed {
		return msg, true
	}
	if orig != "" {
		return orig, true
	}
	if fw.renamedN.Add(1) == 1 {
		fw.log.Info("renamed_target_unmapped")
	}
	return "", false
}

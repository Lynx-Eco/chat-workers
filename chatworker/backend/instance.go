// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// instance is the backend as one framework instance sees it: the
// MatrixConnector handed to bridgev2.NewBridge (newFramework), carrying the
// store's epoch (store.Epoch) read when the instance was made. A logout
// stops the instance, wipes the store (a new epoch) and makes a new instance;
// a goroutine of the old one that outlives the stop still holds this view and
// the intents it handed out, and everything they write or report goes
// through the old epoch, so it is refused after the wipe:
//
//   - every store write of ours made for the framework (rooms, members, room
//     state, people, the spool's rows) runs in write (store.Since);
//   - every event it reports is stored through its epoch (emit:
//     Emitter.EmitSince).
//
// Methods with this receiver write and report for the instance. They must
// not call the Backend's own write or emit paths, which read the current
// epoch.
type instance struct {
	*Backend
	epoch uint64
	// stale counts the writes refused because the store was wiped after
	// the instance was made; the first is logged (stale_write_refused).
	stale atomic.Int64

	// announceMu is held while the account's own PersonInfo of a login is
	// emitted (ensureSelf), so that no chat event of that login passes it;
	// announced are the logins whose PersonInfo the instance has emitted,
	// announceN counts them (event ids).
	announceMu sync.Mutex
	announced  map[networkid.UserLoginID]bool
	announceN  uint64
	// signingIn is the last login the gate let through (string): the
	// login people met before the connection's account is known are
	// reported for (personReceiver).
	signingIn atomic.Value

	// channelDrops counts the events of channel rooms not sent; dropped
	// are the rooms whose first drop was logged (channel.go). kinds is
	// whether each room was a channel when the instance last looked, so a
	// change of its kind reports its ChatInfo again (noteKind); checked are
	// the people whose stored PersonInfo the instance knows is not waiting
	// for a use outside a channel (ensurePerson). Both are under dropMu.
	channelDrops atomic.Int64
	dropMu       sync.Mutex
	dropped      map[string]bool
	kinds        map[id.RoomID]bool
	checked      map[id.UserID]bool
	// ensureMu: one person at a time is looked up and reported by
	// ensurePerson, or marked as not reported (personUnsent).
	ensureMu sync.Mutex
	// refusedN counts the events emitChat refused; the first is logged
	// (chat_event_refused).
	refusedN atomic.Int64
	// takenN counts the room creations refused because another portal
	// has the room and no other id was given (reid.go, refuseTakenRoom);
	// taken are the rooms whose first refusal was logged (under dropMu).
	takenN atomic.Int64
	taken  map[id.RoomID]bool
	// supersededDrops counts what is not sent of superseded rooms (reid.go);
	// superseded are the rooms whose first drop was logged (under dropMu).
	supersededDrops atomic.Int64
	superseded      map[string]bool
	// kept are the superseded rooms whose portal moved onto a key that a
	// live room's chat id names, logged superseded_room_kept (under
	// dropMu); liveMu orders the making live of superseded rooms and their
	// first reports (reid.go, liveAgain).
	kept   map[string]bool
	liveMu sync.Mutex
	// renamedN counts the events not reported because they name a row the
	// connector renamed whose original id cannot be told
	// (Hooks.OriginalMessageID, hostMessageID); the first is logged
	// (renamed_target_unmapped).
	renamedN atomic.Int64
	// noChatN counts the events about a chat that emitChat refused for
	// their empty chat id; the first is logged (chat_event_without_chat_id).
	noChatN atomic.Int64
}

var (
	_ bridgev2.MatrixConnector                       = (*instance)(nil)
	_ bridgev2.MatrixConnectorWithArbitraryRoomState = (*instance)(nil)
)

// newInstance is a view at the store's current epoch.
func (b *Backend) newInstance() *instance {
	return &instance{Backend: b, epoch: b.st.Epoch(), announced: map[networkid.UserLoginID]bool{}, dropped: map[string]bool{},
		kinds: map[id.RoomID]bool{}, checked: map[id.UserID]bool{}, taken: map[id.RoomID]bool{}, superseded: map[string]bool{},
		kept: map[string]bool{}}
}

// instanceOf is the view a framework instance was made with. The framework
// of this backend is always made with one (newFramework); a bridge made
// otherwise gets a view at the current epoch.
func (b *Backend) instanceOf(br *bridgev2.Bridge) *instance {
	if br != nil {
		if fw, ok := br.Matrix.(*instance); ok && fw.Backend == b {
			return fw
		}
	}
	return b.newInstance()
}

// write runs one store write of ours made for the instance through the
// store's epoch guard: it is made before a wipe (which waits for it and
// deletes what it wrote) or not at all. After a wipe it returns
// store.ErrWiped, as a send or an event of before the wipe does. The first
// refused write of an instance is logged, by its code only.
func (fw *instance) write(fn func() error) error {
	err := fw.st.Since(fw.epoch, func() error {
		if h := fw.writeHook; h != nil {
			h()
		}
		return fn()
	})
	if errors.Is(err, store.ErrWiped) && fw.stale.Add(1) == 1 {
		fw.log.Warn("stale_write_refused")
	}
	return err
}

// emit stores and queues an event of the instance through its epoch, behind
// the silence switch. It returns the event's seq, 0 when it was not stored.
func (fw *instance) emit(eventID string, kind isChatEventKind) uint64 {
	return fw.emitSince(fw.epoch, eventID, kind)
}

// emitChat is emit for an event about the chats or the people of the login
// receiver (a message, an edit, a reaction, a deletion, a read, a delivery,
// a chat's info, abilities or removal, a history batch, a PersonInfo): the
// account's own PersonInfo is emitted first if the instance has not emitted
// it yet (ensureSelf). When that is not stored (the outbox failed, a wipe
// came, events are silenced) the event is not stored either: it is refused,
// and the next one tries the account's PersonInfo again. It returns the
// event's seq, 0 when it was not stored.
//
// An event about a chat (chatScoped) whose chat id is empty is refused
// before anything is stored: no chat has that id (a superseded room's is
// empty, reid.go, and nothing of it is sent). Counted (noChatN); the first
// of an instance is logged chat_event_without_chat_id.
func (fw *instance) emitChat(receiver networkid.UserLoginID, eventID string, kind isChatEventKind) uint64 {
	if chat, scoped := chatScoped(kind); scoped && chat == "" {
		if fw.noChatN.Add(1) == 1 {
			fw.log.Error("chat_event_without_chat_id")
		}
		return 0
	}
	var seq uint64
	if fw.ensureSelf(receiver, nil) {
		seq = fw.emit(eventID, kind)
	}
	if seq == 0 && fw.refusedN.Add(1) == 1 {
		fw.log.Warn("chat_event_refused")
	}
	return seq
}

// ensureSelf emits, once per instance and login, a PersonInfo with is_self
// and the account's own person id (selfFor: the id members and senders of
// that login's chats are reported with), through the instance's epoch, and
// tells whether it has been stored (now or before). It is called once the
// framework keeps a login (a stored login cached at a start, start; a new
// one after NewLogin returned, completeSignIn: login is set, so the hook
// SelfPersonID, the account's name and identifiers are known), and before
// every event about the login's chats or people (emitChat), which keeps the
// order where a login's events come before the framework kept it (a
// connector connects the new login's client before its sign-in completes).
// The login of another account than the connection's (not allowed) gets
// nothing. An event of the login waits on announceMu while the PersonInfo
// is being emitted, so none is stored before it; only a stored one counts
// as emitted.
func (fw *instance) ensureSelf(receiver networkid.UserLoginID, login *bridgev2.UserLogin) bool {
	if !fw.allowed(receiver) {
		return false
	}
	fw.announceMu.Lock()
	defer fw.announceMu.Unlock()
	if fw.announced[receiver] {
		return true
	}
	if login != nil {
		fw.rememberSelf(login)
	}
	self := fw.selfFor(receiver)
	if self == "" {
		return false
	}
	info := &contract.PersonInfo{PersonId: self, IsSelf: true}
	stored := fw.storedLogin(receiver, login)
	if stored != nil {
		if name := stored.RemoteName; name != "" {
			info.Name = &name
		}
		info.Identifiers = accountIdentifiers(stored.RemoteProfile, fw.scheme())
	}
	n := fw.announceN + 1
	if fw.emit(fw.genID("self", self, num(fw.boot), num(fw.epoch), num(n)), info) == 0 {
		return false
	}
	fw.announceN = n
	fw.announced[receiver] = true
	return true
}

// storedLogin is the login's row: the login itself when the framework
// handed it over (login), else as the framework stored it. A connector
// connects a new login (and its first events come) after NewLogin stored
// it and before the sign-in's last step returns (mautrix-telegram
// login.go:315), so its name and identifiers are known for the account's
// own PersonInfo also then. Read from the database, not the framework's
// login cache (its lock), with no network call.
func (fw *instance) storedLogin(receiver networkid.UserLoginID, login *bridgev2.UserLogin) *database.UserLogin {
	if login != nil {
		return login.UserLogin
	}
	br := fw.bridge()
	if br == nil || receiver == "" {
		return nil
	}
	stored, err := br.DB.UserLogin.GetByID(context.Background(), receiver)
	if err != nil {
		return nil
	}
	return stored
}

// emitState is Backend.emitState through the instance's epoch: the state
// counter is moved on through the instance's guard (write), so a state of
// a stopped instance moves nothing.
func (fw *instance) emitState(cs *contract.ConnectionState) {
	fw.emitStateBy(fw.emit, fw.write, cs)
}

// ---- MatrixConnector: what hands out intents or writes ----

func (fw *instance) GhostIntent(userID networkid.UserID) bridgev2.MatrixAPI {
	return &Intent{b: fw.Backend, fw: fw, mxid: formatGhost(userID), ghost: userID}
}

// BotIntent is asserted by NewBridge; the bot creates rooms and nothing else.
func (fw *instance) BotIntent() bridgev2.MatrixAPI {
	return &Intent{b: fw.Backend, fw: fw, mxid: BotMXID}
}

// NewUserIntent is the double puppet: the framework posts the user's own
// messages from other devices through it, so they arrive marked from-me.
func (fw *instance) NewUserIntent(ctx context.Context, userID id.UserID, accessToken string) (bridgev2.MatrixAPI, string, error) {
	if userID != OwnerMXID {
		return nil, "", fmt.Errorf("only the owner has a double puppet")
	}
	return &Intent{b: fw.Backend, fw: fw, mxid: OwnerMXID, doublePuppet: true}, accessToken, nil
}

func (fw *instance) SendBridgeStatus(ctx context.Context, st *status.BridgeState) error {
	fw.reportBridgeState(st)
	return nil
}

func (fw *instance) SendMessageStatus(ctx context.Context, ms *bridgev2.MessageStatus, info *bridgev2.MessageStatusEventInfo) {
	if ms == nil || info == nil {
		return
	}
	if ms.Status == event.MessageStatusSuccess && len(ms.DeliveredTo) > 0 {
		fw.reportDelivered(ctx, ms, info)
		return
	}
	fw.recordStatus(fw.epoch, info.SourceEventID, ms)
}

func (fw *instance) BatchSend(ctx context.Context, roomID id.RoomID, req *mautrix.ReqBeeperBatchSend, extras []*bridgev2.MatrixSendExtra) (*mautrix.RespBeeperBatchSend, error) {
	return fw.batchSend(ctx, roomID, req, extras)
}

// GenerateDeterministicRoomID is the room id of the portal of key: the hash
// of the key, or, when that id belongs to another portal (the framework
// re-IDed the room away from this key), a superseded room's id
// (supersededRoom).
func (fw *instance) GenerateDeterministicRoomID(key networkid.PortalKey) id.RoomID {
	roomID := roomIDFor(key)
	if alt, ok := fw.supersededRoom(context.Background(), key, roomID); ok {
		return alt
	}
	err := fw.write(func() error {
		return fw.st.RememberRoom(context.Background(), string(roomID), string(key.ID), string(key.Receiver))
	})
	if err != nil && !errors.Is(err, store.ErrWiped) {
		fw.log.Error("room_remember_failed")
	}
	return roomID
}

// chatScoped is, for an event about one chat, the chat id it names, and
// whether it is one. Of the contract's ChatEvent kinds (channel.proto),
// these name a chat: Message, Edit, Reaction, MessageDeleted, ReadUpTo,
// Delivered, ChatInfo, ChatRemoved, HistoryBatch and ChatAbilities (by
// their chat_id, or their target's). PersonInfo, ConnectionState,
// SignInStep and SendResolved name none.
func chatScoped(kind isChatEventKind) (string, bool) {
	switch k := kind.(type) {
	case *contract.Message:
		return k.GetChatId(), true
	case *contract.Edit:
		return k.GetTarget().GetChatId(), true
	case *contract.Reaction:
		return k.GetTarget().GetChatId(), true
	case *contract.MessageDeleted:
		return k.GetTarget().GetChatId(), true
	case *contract.ReadUpTo:
		return k.GetChatId(), true
	case *contract.Delivered:
		return k.GetTarget().GetChatId(), true
	case *contract.ChatInfo:
		return k.GetChatId(), true
	case *contract.ChatRemoved:
		return k.GetChatId(), true
	case *contract.HistoryBatch:
		return k.GetChatId(), true
	case *contract.ChatAbilities:
		return k.GetChatId(), true
	}
	return "", false
}

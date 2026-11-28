// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
)

func contentHash(m proto.Message) string {
	b, err := contract.Marshal(m)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

// reportChatInfo builds the chat's info from the stored room state and
// reports it when it differs from what was last reported. The event id is
// "chat:<chat>:<generation>"; the generation rises with every change, so a
// name that goes A, B, A again is three events.
func (fw *instance) reportChatInfo(ctx context.Context, roomID id.RoomID) {
	room, ok := fw.roomKey(ctx, roomID)
	if !ok || !room.Created || room.Deleted || !fw.allowed(networkid.UserLoginID(room.Receiver)) {
		return
	}
	if fw.supersededNow(ctx, room) {
		// Nothing of a superseded room is sent (reid.go).
		fw.dropSuperseded(string(roomID))
		return
	}
	receiver := networkid.UserLoginID(room.Receiver)
	info := &contract.ChatInfo{ChatId: room.ChatID}
	channel := fw.roomIsChannel(ctx, room)
	switch {
	case room.Direct:
		info.Kind = contract.ChatKind_CHAT_KIND_DIRECT
	case channel:
		info.Kind = contract.ChatKind_CHAT_KIND_CHANNEL
	default:
		info.Kind = contract.ChatKind_CHAT_KIND_GROUP
	}
	var name event.RoomNameEventContent
	if raw, _ := fw.st.GetState(ctx, string(roomID), event.StateRoomName.Type, ""); raw != nil && json.Unmarshal(raw, &name) == nil {
		info.Name = &name.Name
	}
	var topic event.TopicEventContent
	if raw, _ := fw.st.GetState(ctx, string(roomID), event.StateTopic.Type, ""); raw != nil && json.Unmarshal(raw, &topic) == nil {
		info.Topic = &topic.Topic
	}
	var avatar event.RoomAvatarEventContent
	if raw, _ := fw.st.GetState(ctx, string(roomID), event.StateRoomAvatar.Type, ""); raw != nil && json.Unmarshal(raw, &avatar) == nil {
		ref := handleFor(avatar.URL)
		info.AvatarRef = &ref
	}
	pl, _ := fw.GetPowerLevels(ctx, roomID)
	members, _ := fw.st.Members(ctx, string(roomID))
	self := fw.selfFor(receiver)
	byPerson := map[string]*contract.Member{}
	var listed []id.UserID
	for _, m := range members {
		uid := id.UserID(m.UserID)
		if uid == BotMXID {
			continue
		}
		p := fw.personID(uid, receiver)
		ms := membership(event.Membership(m.Membership))
		if p == "" || ms == contract.Membership_MEMBERSHIP_UNSPECIFIED {
			continue
		}
		listed = append(listed, uid)
		cm := &contract.Member{PersonId: p, Membership: ms, Role: role(pl.GetUserLevel(uid))}
		if m.Displayname != "" && uid != OwnerMXID {
			if prof := fw.profileName(ctx, uid); prof != "" && prof != m.Displayname {
				cm.Nickname = m.Displayname
			}
		}
		// The owner and the owner's own ghost are the same person: keep the
		// stronger entry.
		if prev, ok := byPerson[p]; ok {
			if prev.Membership == contract.Membership_MEMBERSHIP_JOINED && cm.Membership != contract.Membership_MEMBERSHIP_JOINED {
				continue
			}
			if cm.Role < prev.Role {
				cm.Role = prev.Role
			}
		}
		byPerson[p] = cm
	}
	for _, m := range byPerson {
		info.Members = append(info.Members, m)
		if m.Membership == contract.Membership_MEMBERSHIP_JOINED {
			info.TotalMemberCount++
		}
		if room.Direct && m.PersonId != self && info.OtherPersonId == "" {
			info.OtherPersonId = m.PersonId
		}
	}
	sort.Slice(info.Members, func(i, j int) bool { return info.Members[i].PersonId < info.Members[j].PersonId })
	// The framework does not pass on whether the connector's member list was
	// complete; a direct chat's two members always are.
	info.MembersComplete = room.Direct
	fw.sawKind(roomID, channel)
	// The members of a chat that is sent are reported before it, also those
	// first met in a channel (ensurePerson); a channel's are not.
	if !channel {
		for _, uid := range listed {
			fw.ensurePerson(ctx, receiver, uid)
		}
	}
	var gen uint64
	var changed bool
	err := fw.write(func() error {
		var err error
		gen, changed, err = fw.st.ChatInfoChanged(ctx, string(roomID), contentHash(info))
		return err
	})
	if errors.Is(err, store.ErrWiped) {
		return
	}
	if err != nil {
		fw.log.Error("chat_info_store_failed")
		return
	}
	if changed && fw.emitChat(receiver, fw.genID("chat", room.ChatID, num(gen)), info) == 0 {
		fw.reportRefused(roomID, func() error { return fw.st.ChatInfoRefused(ctx, string(roomID), gen) })
	}
}

// reportRefused undoes the record of a chat's report whose event emitChat
// refused (undo), so it is made again; the room's next event makes it
// (forgetKind, channelRoom).
func (fw *instance) reportRefused(roomID id.RoomID, undo func() error) {
	if err := fw.write(undo); err != nil && !errors.Is(err, store.ErrWiped) {
		fw.log.Error("chat_info_store_failed")
	}
	fw.forgetKind(roomID)
}

func (b *Backend) profileName(ctx context.Context, user id.UserID) string {
	raw, _ := b.st.GetProfile(ctx, string(user))
	if raw == nil {
		return ""
	}
	var p map[string]any
	if json.Unmarshal(raw, &p) != nil {
		return ""
	}
	name, _ := p["displayname"].(string)
	return name
}

// reportAbilities reports the room features when they changed.
func (fw *instance) reportAbilities(ctx context.Context, roomID id.RoomID) {
	room, ok := fw.roomKey(ctx, roomID)
	// A channel's abilities are not sent: nothing can be sent to it; nor a
	// superseded room's (reid.go).
	if !ok || !room.Created || room.Deleted || fw.supersededNow(ctx, room) || fw.roomIsChannel(ctx, room) || !fw.allowed(networkid.UserLoginID(room.Receiver)) {
		return
	}
	raw, _ := fw.findState(ctx, roomID, event.StateBeeperRoomFeatures.Type)
	if raw == nil {
		return
	}
	var rf event.RoomFeatures
	if err := json.Unmarshal(raw, &rf); err != nil {
		fw.log.Warn("room_features_undecodable")
		return
	}
	a := chatAbilities(room.ChatID, &rf)
	var gen uint64
	var changed bool
	err := fw.write(func() error {
		var err error
		gen, changed, err = fw.st.AbilitiesChanged(ctx, string(roomID), contentHash(a))
		return err
	})
	if errors.Is(err, store.ErrWiped) {
		return
	}
	if err != nil {
		fw.log.Error("abilities_store_failed")
		return
	}
	if changed && fw.emitChat(networkid.UserLoginID(room.Receiver), fw.genID("abilities", room.ChatID, num(gen)), a) == 0 {
		fw.reportRefused(roomID, func() error { return fw.st.AbilitiesRefused(ctx, string(roomID), gen) })
	}
}

// findState returns a state event's content whatever its state key (the
// features event is keyed by the bridge-info key).
func (b *Backend) findState(ctx context.Context, roomID id.RoomID, evtType string) ([]byte, error) {
	var content []byte
	err := b.st.DB().QueryRow(ctx, `SELECT content FROM lynx_state WHERE room_id = $1 AND type = $2 ORDER BY state_key LIMIT 1`,
		string(roomID), evtType).Scan(&content)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// updateGhostProfile stores a ghost's profile and reports it when changed.
// The profile read is not guarded: the write is, through the instance's
// epoch, so a write that is made proves no wipe came since the instance
// began, and the read before it was of the same store.
//
// A person is reported once they matter in a room that is sent. A profile
// that comes with an event of a room that is sent is reported at once (the
// event names the person), and so is a change of a person reported
// before. One that comes otherwise, of a person not reported yet (without
// any event: a connector updating the people of a network update, as
// mautrix-telegram does with every update's users, handletelegram.go:1057,
// and a channel's own ghost, :977; or with an event of a channel), is
// stored and marked as not reported: bridgev2 updates a ghost's profile
// again only when it changed or is stale, so the person's first use in a
// room that is sent reports it (ensurePerson). A person who never appears
// in a room that is sent is never reported.
func (fw *instance) updateGhostProfile(ctx context.Context, mxid id.UserID, ghost networkid.UserID, mutate func(map[string]any)) error {
	raw, lastHash, err := fw.st.Person(ctx, string(mxid))
	if err != nil {
		return err
	}
	p := map[string]any{}
	if raw != nil {
		_ = json.Unmarshal(raw, &p)
	}
	mutate(p)
	enc, err := json.Marshal(p)
	if err != nil {
		return err
	}
	if err := fw.write(func() error { return fw.st.PutProfile(ctx, string(mxid), string(ghost), enc) }); err != nil {
		return err
	}
	evt := bridgev2.GetRemoteEventFromContext(ctx)
	// A profile change caused by an event of a login whose events are not
	// reported is not reported either.
	if evt != nil && !fw.allowed(evt.GetPortalKey().Receiver) {
		return nil
	}
	info := fw.personInfo(ghost, p)
	reported := lastHash != "" && lastHash != store.PersonUnsent
	if !reported {
		if evt == nil {
			fw.personUnsent(ctx, mxid, info)
			return nil
		}
		key := evt.GetPortalKey()
		if room, superseded := fw.supersededKey(ctx, key); superseded {
			fw.dropSuperseded(string(room))
			fw.personUnsent(ctx, mxid, info)
			return nil
		} else if fw.storedChannel(ctx, key) {
			fw.dropChannel(string(roomIDFor(key)))
			fw.personUnsent(ctx, mxid, info)
			return nil
		}
	}
	fw.reportOrHold(ctx, fw.personReceiver(ctx), mxid, info)
	return nil
}

// reportOrHold reports a person for the login receiver (reportPersonTo),
// or holds it while a sign-in whose account is not yet known runs. It
// tells whether the PersonInfo is stored, held, or was reported before.
func (fw *instance) reportOrHold(ctx context.Context, receiver networkid.UserLoginID, mxid id.UserID, info *contract.PersonInfo) bool {
	fw.peopleMu.Lock()
	if fw.holdPeople {
		fw.heldPeople = append(fw.heldPeople, heldPerson{fw: fw, userID: mxid, info: info})
		fw.peopleMu.Unlock()
		return true
	}
	fw.peopleMu.Unlock()
	return fw.reportPersonTo(ctx, receiver, mxid, info)
}

func (fw *instance) reportPerson(ctx context.Context, mxid id.UserID, info *contract.PersonInfo) {
	fw.reportPersonTo(ctx, fw.personReceiver(ctx), mxid, info)
}

// reportPersonTo reports a person when changed, for the login r
// (personReceiver): r's own PersonInfo goes first. It tells whether the
// PersonInfo is stored or was reported before. One whose event is refused
// is marked as not reported again (store.PersonInfoRefused), for the
// person's next use to report it (ensurePerson).
func (fw *instance) reportPersonTo(ctx context.Context, r networkid.UserLoginID, mxid id.UserID, info *contract.PersonInfo) bool {
	var gen uint64
	var changed bool
	err := fw.write(func() error {
		var err error
		gen, changed, err = fw.st.PersonInfoChanged(ctx, string(mxid), contentHash(info))
		return err
	})
	if errors.Is(err, store.ErrWiped) {
		return false
	}
	if err != nil {
		fw.log.Error("person_store_failed")
		return false
	}
	if !changed {
		return true
	}
	evID := fw.genID("person", info.PersonId, num(gen))
	var seq uint64
	if r != "" {
		seq = fw.emitChat(r, evID, info)
	} else {
		// No login is known to have met the person (personReceiver): not
		// ordered after an account's own PersonInfo.
		seq = fw.emit(evID, info)
	}
	if seq != 0 {
		return true
	}
	if err := fw.write(func() error { return fw.st.PersonInfoRefused(ctx, string(mxid), gen) }); err != nil && !errors.Is(err, store.ErrWiped) {
		fw.log.Error("person_store_failed")
	}
	fw.dropMu.Lock()
	delete(fw.checked, mxid)
	fw.dropMu.Unlock()
	return false
}

// personReceiver is the login a person is reported for, whose own
// PersonInfo goes first (emitChat): the login of the remote event the
// profile came with, else the connection's account as far as it is known,
// else the login the gate let through last (a sign-in not complete yet).
// It is "" only for a connector that passes no gate, before its sign-in
// completed, with a profile that came without one of its login's events.
func (fw *instance) personReceiver(ctx context.Context) networkid.UserLoginID {
	if evt := bridgev2.GetRemoteEventFromContext(ctx); evt != nil {
		return evt.GetPortalKey().Receiver
	}
	if a := fw.knownAccount(); a != "" {
		return networkid.UserLoginID(a)
	}
	s, _ := fw.signingIn.Load().(string)
	return networkid.UserLoginID(s)
}

// releasePeople reports (keep) or drops the profiles held during a sign-in.
func (b *Backend) releasePeople(ctx context.Context, keep bool) {
	b.peopleMu.Lock()
	held := b.heldPeople
	b.heldPeople = nil
	b.holdPeople = false
	b.peopleMu.Unlock()
	if !keep {
		return
	}
	for _, h := range held {
		h.fw.reportPerson(ctx, h.userID, h.info)
	}
}

func (b *Backend) scheme() string {
	if b.hooks.IdentifierScheme != "" {
		return b.hooks.IdentifierScheme
	}
	if b.bridge() != nil {
		return b.bridge().Network.GetName().NetworkID
	}
	return "network"
}

func (b *Backend) personInfo(ghost networkid.UserID, p map[string]any) *contract.PersonInfo {
	info := &contract.PersonInfo{PersonId: string(ghost)}
	if name, ok := p["displayname"].(string); ok {
		info.Name = &name
	}
	if av, ok := p["avatar_url"].(string); ok {
		ref := handleFor(id.ContentURIString(av))
		info.AvatarRef = &ref
	}
	if ids, ok := p["com.beeper.bridge.identifiers"].([]any); ok {
		for _, v := range ids {
			if s, ok := v.(string); ok && s != "" {
				info.Identifiers = append(info.Identifiers, s)
			}
		}
	}
	if bot, ok := p["com.beeper.bridge.is_network_bot"].(bool); ok {
		info.IsBot = bot
	}
	if self := b.selfFor(""); self != "" && self == string(ghost) {
		info.IsSelf = true
	}
	return info
}

// reportBridgeState maps a bridge state to ConnectionState.
func (fw *instance) reportBridgeState(st *status.BridgeState) {
	if st == nil {
		return
	}
	if st.RemoteID != "" {
		if _, gone := fw.loggedOut.Load(st.RemoteID); gone {
			return
		}
		if !fw.allowed(st.RemoteID) {
			return
		}
	}
	if fw.refused.Load() && st.StateEvent == status.StateUnconfigured {
		// The stored login was refused; the ERROR already said why.
		return
	}
	cs := fw.hooks.ReportedState(st, fw.scheme())
	if cs == nil {
		return
	}
	fw.emitState(cs)
}

// emitState reports a connection state. Its event id carries a durable
// counter: a state reached again later is a new event, a repeat of the same
// event is not.
func (b *Backend) emitState(cs *contract.ConnectionState) {
	b.emitStateBy(b.emit, func(fn func() error) error { return fn() }, cs)
}

// emitStateBy is emitState through emit (Backend.emit, or an instance's),
// with the counter moved on through write (an instance's guard, or none).
// The state is logged only once it is stored.
func (b *Backend) emitStateBy(emit func(string, isChatEventKind) uint64, write func(func() error) error, cs *contract.ConnectionState) {
	var n uint64
	err := write(func() error {
		var err error
		n, err = b.st.Next(context.Background(), "state_event")
		return err
	})
	if errors.Is(err, store.ErrWiped) {
		return
	}
	if err != nil {
		b.log.Error("counter_failed")
		return
	}
	if emit(b.genID("state", num(n), num(int32(cs.State))), cs) != 0 {
		b.log.Info("connection_state", logx.N("state", int64(cs.State)))
	}
}

// reportDelivered turns a delivery receipt (the framework reports it as a
// success status with DeliveredTo) into Delivered events.
func (fw *instance) reportDelivered(ctx context.Context, ms *bridgev2.MessageStatus, info *bridgev2.MessageStatusEventInfo) {
	room, ok := fw.roomKey(ctx, info.RoomID)
	if ok && fw.dropsRoom(ctx, info.RoomID, room) {
		return
	}
	if !ok || !fw.allowed(networkid.UserLoginID(room.Receiver)) {
		return
	}
	target, _ := fw.bridge().DB.Message.GetPartByMXID(ctx, info.SourceEventID)
	if target == nil {
		return
	}
	// The id the host was given for the row (hostMessageID).
	msg, named := fw.hostMessageID(target.ID)
	if !named {
		return
	}
	ts := time.Now().UnixMilli()
	for _, u := range ms.DeliveredTo {
		p := fw.personID(u, networkid.UserLoginID(room.Receiver))
		if p == "" {
			continue
		}
		fw.ensurePerson(ctx, networkid.UserLoginID(room.Receiver), u)
		fw.emitChat(networkid.UserLoginID(room.Receiver), eventID("delivered", room.ChatID, string(msg), string(target.PartID), p), &contract.Delivered{
			Target:      &contract.MessageRef{ChatId: room.ChatID, MessageId: string(msg), PartId: string(target.PartID)},
			RecipientId: p,
			TimestampMs: ts,
		})
	}
}

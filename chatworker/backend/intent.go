// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// Intent is one acting identity: the bot, a ghost (a person on the network),
// or the owner's double puppet, handed out by one framework instance (fw):
// what it writes or reports goes through that instance's epoch.
type Intent struct {
	b            *Backend // fw.Backend
	fw           *instance
	mxid         id.UserID
	ghost        networkid.UserID
	doublePuppet bool
}

var (
	_ bridgev2.MatrixAPI                   = (*Intent)(nil)
	_ bridgev2.StreamOrderReadingMatrixAPI = (*Intent)(nil)
	_ bridgev2.ThreadReadingMatrixAPI      = (*Intent)(nil)
	_ bridgev2.MarkAsDMMatrixAPI           = (*Intent)(nil)
)

func (in *Intent) GetMXID() id.UserID   { return in.mxid }
func (in *Intent) IsDoublePuppet() bool { return in.doublePuppet }

func ensureParsed(content *event.Content, t event.Type) {
	if content.Parsed != nil {
		return
	}
	if content.VeryRaw == nil {
		raw, _ := json.Marshal(content)
		content.VeryRaw = raw
	}
	_ = content.ParseRaw(t)
}

// SendMessage receives every message part, edit, reaction and redaction the
// framework bridges, and turns it into one contract event in the outbox.
func (in *Intent) SendMessage(ctx context.Context, roomID id.RoomID, evtType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	b := in.b
	if content == nil {
		return nil, fmt.Errorf("%w: no content", mautrix.MBadJSON)
	}
	ensureParsed(content, evtType)
	if extra == nil {
		extra = &bridgev2.MatrixSendExtra{}
	}
	evtID := in.eventIDFor(roomID, evtType, content, extra)
	room, ok := b.roomKey(ctx, roomID)
	if ok && in.fw.dropsRoom(ctx, roomID, room) {
		return &mautrix.RespSendEvent{EventID: evtID}, nil
	}
	if ok && b.allowed(networkid.UserLoginID(room.Receiver)) {
		ts := extra.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		in.fw.translate(ctx, room, translateInput{
			evtType: evtType, content: content, extra: extra, ts: ts,
			sender: in.mxid, ghost: in.ghost, fromMe: in.doublePuppet,
		})
	}
	return &mautrix.RespSendEvent{EventID: evtID}, nil
}

func (in *Intent) eventIDFor(roomID id.RoomID, evtType event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) id.EventID {
	msg, isMsg := content.Parsed.(*event.MessageEventContent)
	isEdit := isMsg && msg.RelatesTo.GetReplaceID() != ""
	switch {
	case (evtType == event.EventMessage || evtType == event.EventSticker) && !isEdit && extra.MessageMeta != nil:
		m := extra.MessageMeta
		return messageEventID(m.Room, m.ID, m.PartID)
	case evtType == event.EventReaction && extra.ReactionMeta != nil:
		r := extra.ReactionMeta
		return reactionEventID(roomID, r.MessageID, r.MessagePartID, r.SenderID, r.EmojiID)
	}
	var target string
	if isEdit {
		target = string(msg.RelatesTo.GetReplaceID())
	} else if red, ok := content.Parsed.(*event.RedactionEventContent); ok {
		target = string(red.Redacts)
	}
	return otherEventID(string(roomID), evtType.Type, target, strconv.FormatInt(extra.Timestamp.UnixMilli(), 10), string(in.mxid))
}

type translateInput struct {
	evtType event.Type
	content *event.Content
	extra   *bridgev2.MatrixSendExtra
	ts      time.Time
	sender  id.UserID
	ghost   networkid.UserID
	fromMe  bool
	// sendKey is set when the event is the echo of our own command.
	sendKey string
}

func (b *Backend) senderOf(in translateInput, meta networkid.UserID, receiver networkid.UserLoginID) string {
	if meta != "" {
		return string(meta)
	}
	if in.ghost != "" {
		return string(in.ghost)
	}
	return b.personID(in.sender, receiver)
}

// translate turns one bridged Matrix event into a contract event.
func (fw *instance) translate(ctx context.Context, room *store.Room, in translateInput) {
	receiver := networkid.UserLoginID(room.Receiver)
	fw.ensureSenders(ctx, receiver, in)
	// A person the event names (a mention, the sender replied to) is
	// reported before it too.
	person := func(u id.UserID) string {
		fw.ensurePerson(ctx, receiver, u)
		return fw.personID(u, receiver)
	}
	// ref names an existing row the event is about, by the id the host
	// was given (hostMessageID); nil when that cannot be told: the event
	// is not reported.
	ref := func(msg networkid.MessageID, part networkid.PartID) *contract.MessageRef {
		host, ok := fw.hostMessageID(msg)
		if !ok {
			return nil
		}
		return &contract.MessageRef{ChatId: room.ChatID, MessageId: string(host), PartId: string(part)}
	}
	ms := in.ts.UnixMilli()
	switch in.evtType {
	case event.EventMessage, event.EventSticker, event.EventUnstablePollStart:
		msg, _ := in.content.Parsed.(*event.MessageEventContent)
		if msg != nil && msg.RelatesTo.GetReplaceID() != "" {
			fw.translateEdit(ctx, room, in, msg, person)
			return
		}
		if out := fw.buildMessage(ctx, room, in); out != nil {
			fw.emitChat(receiver, eventID("msg", out.ChatId, out.MessageId, out.PartId), out)
		}
	case event.EventReaction:
		react := in.content.AsReaction()
		r := in.extra.ReactionMeta
		out := &contract.Reaction{FromMe: in.fromMe, Emoji: react.RelatesTo.Key, TimestampMs: ms}
		if r != nil {
			out.Target = ref(r.MessageID, r.MessagePartID)
			out.SenderId = fw.senderOf(in, r.SenderID, receiver)
			// The framework adds variation selectors to the network's emoji;
			// when that is the only difference, the network's form is kept,
			// so an add and its removal name the same emoji.
			if r.EmojiID != "" && strings.ReplaceAll(react.RelatesTo.Key, "\ufe0f", "") == strings.ReplaceAll(string(r.EmojiID), "\ufe0f", "") {
				out.Emoji = string(r.EmojiID)
			} else if string(r.EmojiID) != react.RelatesTo.Key {
				out.EmojiId = string(r.EmojiID)
			}
		} else if target, _ := fw.bridge().DB.Message.GetPartByMXID(ctx, react.RelatesTo.EventID); target != nil {
			out.Target = ref(target.ID, target.PartID)
			out.SenderId = fw.senderOf(in, "", receiver)
		} else {
			fw.log.Warn("reaction_target_unknown")
			return
		}
		if out.Target == nil {
			return
		}
		fw.emitReaction(receiver, out)
	case event.EventRedaction:
		red := in.content.AsRedaction()
		if r := in.extra.ReactionMeta; r != nil {
			out := &contract.Reaction{
				Target: ref(r.MessageID, r.MessagePartID), SenderId: fw.senderOf(in, r.SenderID, receiver),
				FromMe: in.fromMe, Emoji: r.Emoji, Removed: true, TimestampMs: ms,
			}
			if out.Emoji == "" {
				out.Emoji = string(r.EmojiID)
			} else if string(r.EmojiID) != r.Emoji {
				out.EmojiId = string(r.EmojiID)
			}
			if out.Target != nil {
				fw.emitReaction(receiver, out)
			}
			return
		}
		if m := in.extra.MessageMeta; m != nil {
			fw.emitDeleted(receiver, room.ChatID, m.ID, m.PartID, fw.personID(in.sender, receiver), ms)
			return
		}
		// No meta (a part removed by an edit): map the event id back.
		if target, _ := fw.bridge().DB.Message.GetPartByMXID(ctx, red.Redacts); target != nil {
			fw.emitDeleted(receiver, room.ChatID, target.ID, target.PartID, fw.personID(in.sender, receiver), ms)
			return
		}
		if target, _ := fw.bridge().DB.Reaction.GetByMXID(ctx, red.Redacts); target != nil {
			if t := ref(target.MessageID, target.MessagePartID); t != nil {
				fw.emitReaction(receiver, &contract.Reaction{
					Target: t, SenderId: string(target.SenderID),
					Emoji: target.Emoji, Removed: true, TimestampMs: ms,
				})
			}
			return
		}
		fw.log.Warn("redaction_target_unknown")
	default:
		// Other event types (poll responses, room-local notices) have no
		// contract form; they are not reported.
	}
}

// buildMessage translates one bridged message part (not an edit).
func (b *Backend) buildMessage(ctx context.Context, room *store.Room, in translateInput) *contract.Message {
	receiver := networkid.UserLoginID(room.Receiver)
	person := func(u id.UserID) string { return b.personID(u, receiver) }
	ms := in.ts.UnixMilli()
	msg, _ := in.content.Parsed.(*event.MessageEventContent)
	m := in.extra.MessageMeta
	if m == nil {
		// The framework always passes the message's own row; without it
		// the message has no network id and cannot be reported.
		b.log.Warn("message_without_meta")
		return nil
	}
	out := &contract.Message{
		ChatId:      room.ChatID,
		MessageId:   string(m.ID),
		PartId:      string(m.PartID),
		PartIndex:   uint32(max(in.extra.PartIndex, 0)),
		SenderId:    b.senderOf(in, m.SenderID, receiver),
		FromMe:      in.fromMe,
		TimestampMs: ms,
		SendKey:     in.sendKey,
	}
	if out.SendKey == "" && m.SendTxnID != "" {
		out.SendKey = b.sendKeyForTxn(ctx, string(m.SendTxnID))
	}
	if self := b.selfFor(receiver); self != "" && out.SenderId == self {
		out.FromMe = true
	}
	replySender := ""
	if m.ReplyTo.MessageID != "" {
		r := &contract.MessageRef{ChatId: room.ChatID, MessageId: string(m.ReplyTo.MessageID)}
		if m.ReplyTo.PartID != nil {
			r.PartId = string(*m.ReplyTo.PartID)
		}
		out.ReplyTo = r
		if target, err := b.bridge().DB.Message.GetFirstOrSpecificPartByID(ctx, receiver, m.ReplyTo); err == nil && target != nil {
			replySender = person(target.SenderMXID)
			if replySender == "" {
				replySender = string(target.SenderID)
			}
		}
	}
	if m.ThreadRoot != "" {
		out.ThreadRootId = string(m.ThreadRoot)
	}
	var raw map[string]any
	if msg == nil {
		raw = in.content.Raw
	}
	out.Content = messageContent(contentInput{evtType: in.evtType, msg: msg, raw: raw, person: person, replySender: replySender})
	return out
}

func (fw *instance) translateEdit(ctx context.Context, room *store.Room, in translateInput, msg *event.MessageEventContent, person func(id.UserID) string) {
	receiver := networkid.UserLoginID(room.Receiver)
	newContent := msg.NewContent
	if newContent == nil {
		newContent = msg
	}
	out := &contract.Edit{EditedAtMs: in.ts.UnixMilli()}
	var msgID networkid.MessageID
	var partID networkid.PartID
	var ok bool
	if m := in.extra.MessageMeta; m != nil {
		msgID, partID = m.ID, m.PartID
		out.EditorId = fw.senderOf(in, m.SenderID, receiver)
	} else if target, _ := fw.bridge().DB.Message.GetPartByMXID(ctx, msg.RelatesTo.GetReplaceID()); target != nil {
		msgID, partID = target.ID, target.PartID
		out.EditorId = fw.senderOf(in, target.SenderID, receiver)
	} else {
		fw.log.Warn("edit_target_unknown")
		return
	}
	// The id the host was given for the edited row (hostMessageID).
	if msgID, ok = fw.hostMessageID(msgID); !ok {
		return
	}
	out.Target = &contract.MessageRef{ChatId: room.ChatID, MessageId: string(msgID), PartId: string(partID)}
	out.Content = messageContent(contentInput{evtType: in.evtType, msg: newContent, person: person})
	fw.emitChat(receiver, eventID("edit", room.ChatID, string(msgID), string(partID), num(out.EditedAtMs)), out)
}

func (fw *instance) emitReaction(receiver networkid.UserLoginID, r *contract.Reaction) {
	state := "added"
	if r.Removed {
		state = "removed"
	}
	emoji := r.Emoji
	if r.EmojiId != "" {
		emoji = r.EmojiId
	}
	t := r.GetTarget()
	fw.emitChat(receiver, eventID("react", t.GetChatId(), t.GetMessageId(), t.GetPartId(), r.SenderId, emoji, state, num(r.TimestampMs)), r)
}

// emitDeleted reports the deletion of the row msg, part, by the id the host
// was given (hostMessageID); not when that cannot be told.
func (fw *instance) emitDeleted(receiver networkid.UserLoginID, chatID string, msg networkid.MessageID, part networkid.PartID, by string, ms int64) {
	msg, ok := fw.hostMessageID(msg)
	if !ok {
		return
	}
	fw.emitChat(receiver, eventID("del", chatID, string(msg), string(part)), &contract.MessageDeleted{
		Target:      &contract.MessageRef{ChatId: chatID, MessageId: string(msg), PartId: string(part)},
		DeletedById: by,
		TimestampMs: ms,
	})
}

// SendState stores room state and reports chat info and abilities when they
// change.
func (in *Intent) SendState(ctx context.Context, roomID id.RoomID, evtType event.Type, stateKey string, content *event.Content, ts time.Time) (*mautrix.RespSendEvent, error) {
	fw := in.fw
	ensureParsed(content, evtType)
	if err := fw.putState(ctx, roomID, evtType, stateKey, content); err != nil {
		return nil, err
	}
	switch evtType {
	case event.StateRoomName, event.StateTopic, event.StateRoomAvatar, event.StateMember, event.StatePowerLevels:
		fw.reportChatInfo(ctx, roomID)
	case event.StateBeeperRoomFeatures:
		fw.reportAbilities(ctx, roomID)
	}
	return &mautrix.RespSendEvent{EventID: otherEventID(string(roomID), evtType.Type, stateKey, strconv.FormatInt(ts.UnixMilli(), 10))}, nil
}

func (fw *instance) putState(ctx context.Context, roomID id.RoomID, evtType event.Type, stateKey string, content *event.Content) error {
	if evtType == event.StateMember {
		mem, _ := content.Parsed.(*event.MemberEventContent)
		if mem == nil {
			return nil
		}
		return fw.write(func() error {
			_, err := fw.st.SetMember(ctx, string(roomID), store.Member{
				UserID: stateKey, Membership: string(mem.Membership),
				Displayname: mem.Displayname, AvatarURL: string(mem.AvatarURL),
			})
			return err
		})
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return err
	}
	return fw.write(func() error { return fw.st.PutState(ctx, string(roomID), evtType.Type, stateKey, raw) })
}

func (in *Intent) MarkRead(ctx context.Context, roomID id.RoomID, eventID id.EventID, ts time.Time) error {
	b := in.b
	room, ok := b.roomKey(ctx, roomID)
	if ok && in.fw.dropsRoom(ctx, roomID, room) {
		return nil
	}
	if !ok || !b.allowed(networkid.UserLoginID(room.Receiver)) {
		return nil
	}
	target, _ := b.bridge().DB.Message.GetPartByMXID(ctx, eventID)
	if target == nil {
		b.log.Warn("read_target_unknown")
		return nil
	}
	// The id the host was given for the target row (hostMessageID).
	msg, named := in.fw.hostMessageID(target.ID)
	if !named {
		return nil
	}
	in.fw.ensurePerson(ctx, networkid.UserLoginID(room.Receiver), in.mxid)
	in.fw.emitRead(room, b.personID(in.mxid, networkid.UserLoginID(room.Receiver)), in.doublePuppet, string(msg), ts)
	return nil
}

func (fw *instance) emitRead(room *store.Room, reader string, fromMe bool, msgID string, ts time.Time) {
	fw.emitChat(networkid.UserLoginID(room.Receiver), eventID("read", room.ChatID, reader, msgID), &contract.ReadUpTo{
		ChatId: room.ChatID, ReaderId: reader, FromMe: fromMe, UpToMessageId: msgID, TimestampMs: ts.UnixMilli(),
	})
}

// MarkThreadRead is a read inside a thread; the contract has no thread
// reads, so it is reported as a read of that message.
func (in *Intent) MarkThreadRead(ctx context.Context, roomID id.RoomID, eventID id.EventID, ts time.Time, threadID id.EventID) error {
	return in.MarkRead(ctx, roomID, eventID, ts)
}

// MarkStreamOrderRead is a read by stream position (some networks): it is
// reported as a read of the last message at or before that time.
func (in *Intent) MarkStreamOrderRead(ctx context.Context, roomID id.RoomID, streamOrder int64, ts time.Time) error {
	b := in.b
	room, ok := b.roomKey(ctx, roomID)
	if ok && in.fw.dropsRoom(ctx, roomID, room) {
		return nil
	}
	if !ok || !b.allowed(networkid.UserLoginID(room.Receiver)) {
		return nil
	}
	// The portal's current key: the framework's rows of a re-IDed portal
	// follow its new key (reid.go). That can find a row the connector
	// renamed, which is named by the id the host was given (hostMessageID).
	target, _ := b.bridge().DB.Message.GetLastPartAtOrBeforeTime(ctx, in.fw.keyOfRoom(ctx, room), ts)
	if target == nil {
		return nil
	}
	msg, named := in.fw.hostMessageID(target.ID)
	if !named {
		return nil
	}
	in.fw.ensurePerson(ctx, networkid.UserLoginID(room.Receiver), in.mxid)
	in.fw.emitRead(room, b.personID(in.mxid, networkid.UserLoginID(room.Receiver)), in.doublePuppet, string(msg), ts)
	return nil
}

// MarkUnread, MarkTyping, TagRoom, MuteRoom and MarkAsDM are user-local or
// ephemeral; the contract carries none of them upward.
func (in *Intent) MarkUnread(ctx context.Context, roomID id.RoomID, unread bool) error { return nil }
func (in *Intent) MarkTyping(ctx context.Context, roomID id.RoomID, typingType bridgev2.TypingType, timeout time.Duration) error {
	return nil
}
func (in *Intent) TagRoom(ctx context.Context, roomID id.RoomID, tag event.RoomTag, isTagged bool) error {
	return nil
}
func (in *Intent) MuteRoom(ctx context.Context, roomID id.RoomID, until time.Time) error { return nil }
func (in *Intent) MarkAsDM(ctx context.Context, roomID id.RoomID, otherUser id.UserID) error {
	return nil
}

// ---- profiles ----

func (in *Intent) SetDisplayName(ctx context.Context, name string) error {
	return in.updateProfile(ctx, func(p map[string]any) { p["displayname"] = name })
}

func (in *Intent) SetAvatarURL(ctx context.Context, avatarURL id.ContentURIString) error {
	return in.updateProfile(ctx, func(p map[string]any) { p["avatar_url"] = string(avatarURL) })
}

func (in *Intent) SetExtraProfileMeta(ctx context.Context, data any) error {
	m, err := toMap(data)
	if err != nil {
		return err
	}
	return in.updateProfile(ctx, func(p map[string]any) {
		for k, v := range m {
			p[k] = v
		}
	})
}

// SetProfile carries the whole profile (ReplaceEntireProfile).
func (in *Intent) SetProfile(ctx context.Context, data any) error {
	m, err := toMap(data)
	if err != nil {
		return err
	}
	return in.updateProfile(ctx, func(p map[string]any) {
		clear(p)
		for k, v := range m {
			p[k] = v
		}
	})
}

func toMap(data any) (map[string]any, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (in *Intent) updateProfile(ctx context.Context, mutate func(map[string]any)) error {
	if in.ghost == "" {
		// The bot's and the owner's own profiles are not people.
		return nil
	}
	return in.fw.updateGhostProfile(ctx, in.mxid, in.ghost, mutate)
}

// ---- rooms ----

func (in *Intent) CreateRoom(ctx context.Context, req *mautrix.ReqCreateRoom) (id.RoomID, error) {
	fw := in.fw
	roomID := req.BeeperLocalRoomID
	if roomID == "" {
		return "", fmt.Errorf("%w: room creation without a local room id", mautrix.MUnrecognized)
	}
	// A room another portal has, for which GenerateDeterministicRoomID gave
	// no superseded id (reid.go): refused before anything is written.
	if err := fw.refuseTakenRoom(ctx, roomID); err != nil {
		return "", err
	}
	announced, ok := fw.roomKey(ctx, roomID)
	if !ok {
		return "", fmt.Errorf("room %s was not announced by GenerateDeterministicRoomID", roomID)
	}
	// A live room of its chat id whose portal is gone is no candidate for
	// the id (reid.go, chatHeld): removed first, so that this room is not
	// made a superseded room by it.
	if announced.ChatID != "" {
		fw.retireOrphan(ctx, announced.ChatID, announced.Receiver, string(roomID))
	}
	isSpace := creationIsSpace(req.CreationContent)
	var superseded bool
	err := fw.write(func() error {
		var err error
		superseded, err = fw.st.RoomCreated(ctx, string(roomID), req.IsDirect, isSpace)
		return err
	})
	if err != nil {
		return "", err
	}
	if superseded {
		// Another live room has its chat id (reid.go): made a superseded
		// room, of which nothing is sent.
		fw.log.Warn("room_created_superseded")
	}
	features := false
	for _, st := range req.InitialState {
		if st == nil {
			continue
		}
		ensureParsed(&st.Content, st.Type)
		if err := fw.putState(ctx, roomID, st.Type, st.GetStateKey(), &st.Content); err != nil {
			return "", err
		}
		if st.Type == event.StateBeeperRoomFeatures {
			features = true
		}
	}
	if req.PowerLevelOverride != nil {
		raw, err := json.Marshal(req.PowerLevelOverride)
		if err != nil {
			return "", err
		}
		if err := fw.write(func() error { return fw.st.PutState(ctx, string(roomID), event.StatePowerLevels.Type, "", raw) }); err != nil {
			return "", err
		}
	}
	for _, u := range req.BeeperInitialMembers {
		err := fw.write(func() error {
			_, err := fw.st.SetMember(ctx, string(roomID), store.Member{UserID: string(u), Membership: string(event.MembershipJoin)})
			return err
		})
		if err != nil {
			return "", err
		}
	}
	fw.reportChatInfo(ctx, roomID)
	if features {
		fw.reportAbilities(ctx, roomID)
	}
	return roomID, nil
}

func (in *Intent) DeleteRoom(ctx context.Context, roomID id.RoomID, puppetsOnly bool) error {
	fw := in.fw
	room, ok := fw.roomKey(ctx, roomID)
	if !ok {
		return nil
	}
	if err := fw.write(func() error { return fw.st.RoomDeleted(ctx, string(roomID)) }); err != nil {
		return err
	}
	if room.Superseded {
		// Never reported, so not removed either (reid.go). Not asked
		// through the follow of its portal (supersededNow): a room that is
		// being deleted is not made live to be removed at once.
		fw.dropSuperseded(string(roomID))
		return nil
	}
	if room.Created && !room.Deleted && fw.allowed(networkid.UserLoginID(room.Receiver)) {
		fw.emitChat(networkid.UserLoginID(room.Receiver), fw.genID("chat-removed", room.ChatID, num(room.CreatedGen)), &contract.ChatRemoved{ChatId: room.ChatID})
	}
	return nil
}

func (in *Intent) EnsureJoined(ctx context.Context, roomID id.RoomID, params ...bridgev2.EnsureJoinedParams) error {
	changed, err := in.fw.setMembership(ctx, roomID, in.mxid, event.MembershipJoin)
	if err != nil {
		return err
	}
	if changed {
		in.fw.reportChatInfo(ctx, roomID)
	}
	return nil
}

func (in *Intent) EnsureInvited(ctx context.Context, roomID id.RoomID, userID id.UserID) error {
	members, err := in.b.GetMembers(ctx, roomID)
	if err != nil {
		return err
	}
	if m := members[userID]; m != nil && m.Membership == event.MembershipJoin {
		return nil
	}
	changed, err := in.fw.setMembership(ctx, roomID, userID, event.MembershipInvite)
	if err != nil {
		return err
	}
	if changed {
		in.fw.reportChatInfo(ctx, roomID)
	}
	return nil
}

func (fw *instance) setMembership(ctx context.Context, roomID id.RoomID, user id.UserID, m event.Membership) (bool, error) {
	members, err := fw.GetMembers(ctx, roomID)
	if err != nil {
		return false, err
	}
	cur := members[user]
	next := store.Member{UserID: string(user), Membership: string(m)}
	if cur != nil {
		next.Displayname, next.AvatarURL = cur.Displayname, string(cur.AvatarURL)
	}
	var changed bool
	err = fw.write(func() error {
		var err error
		changed, err = fw.st.SetMember(ctx, string(roomID), next)
		return err
	})
	return changed, err
}

// GetEvent answers from the framework's message table: id, room, sender and
// time of a bridged message part. The backend keeps no message bodies, so
// the content is empty.
func (in *Intent) GetEvent(ctx context.Context, roomID id.RoomID, eventID id.EventID) (*event.Event, error) {
	part, err := in.b.bridge().DB.Message.GetPartByMXID(ctx, eventID)
	if err != nil {
		return nil, err
	}
	if part == nil {
		return nil, fmt.Errorf("%w: event not known", mautrix.MNotFound)
	}
	sender := part.SenderMXID
	if sender == "" {
		sender = formatGhost(part.SenderID)
	}
	return &event.Event{
		ID:        part.MXID,
		RoomID:    roomID,
		Sender:    sender,
		Type:      event.EventMessage,
		Timestamp: part.Timestamp.UnixMilli(),
		Content:   event.Content{Parsed: &event.MessageEventContent{}},
	}, nil
}

// ---- media ----

func (in *Intent) DownloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
	return in.b.downloadMedia(ctx, uri, file)
}

func (in *Intent) DownloadMediaToFile(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo, writable bool, callback func(*os.File) error) error {
	return in.b.downloadMediaToFile(ctx, uri, file, writable, callback)
}

func (in *Intent) UploadMedia(ctx context.Context, roomID id.RoomID, data []byte, fileName, mimeType string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	if err := in.uploadAllowed(ctx, roomID); err != nil {
		return "", nil, err
	}
	uri, err := in.fw.spoolBytes(ctx, data, fileName, mimeType)
	return uri, nil, err
}

func (in *Intent) UploadMediaStream(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	if err := in.uploadAllowed(ctx, roomID); err != nil {
		return "", nil, err
	}
	uri, err := in.fw.spoolStream(ctx, cb)
	return uri, nil, err
}

var errNotOurs = errors.New("not a media id of this worker")

// errChannelMedia refuses an upload for a channel room (channel.go) or a
// superseded one (reid.go): its messages are not sent, so their
// attachments are not kept.
var errChannelMedia = errors.New("media of a channel is not kept")

// uploadAllowed refuses an upload for a channel or a superseded room.
// Uploads for no room (avatars of people and of chats: bridgev2
// Avatar.Reupload) are allowed.
func (in *Intent) uploadAllowed(ctx context.Context, roomID id.RoomID) error {
	if roomID == "" {
		return nil
	}
	if room, ok := in.b.roomKey(ctx, roomID); ok && in.fw.dropsRoom(ctx, roomID, room) {
		return errChannelMedia
	}
	return nil
}

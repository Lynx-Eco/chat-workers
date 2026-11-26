// SPDX-License-Identifier: AGPL-3.0-or-later

package fakenet

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// People and chats of the script.
const (
	Me    = networkid.UserID("u-me")
	Alice = networkid.UserID("u-alice")
	Bob   = networkid.UserID("u-bob")
	Carol = networkid.UserID("u-carol")

	DMAlice = "dm-alice"
	DMBob   = "dm-bob"
	Group   = "grp-1"
	DMCarol = "dm-carol" // created and removed again during the sync
	// Chan is the broadcast channel of Script.Channel; ChanPoster posts in
	// it and is met nowhere else. ChanMark is in every text, name and file
	// of it.
	Chan          = "chan-news"
	ChanPoster    = networkid.UserID("u-poster")
	ChanMark      = "CHANNELMARK"
	ChanPostAgain = "cp-again"
	// TurnChan is the room of Script.TurnedChannel, a group until its stored
	// metadata says broadcast; TurnMark is in every text of it from then
	// on. TurnBefore is its post while it is a group. TurnPoster posts in it
	// once it is a channel, and then TurnGroupMsg in the group.
	TurnChan      = "turn-news"
	TurnMark      = "TURNMARK"
	TurnBefore    = "lc1"
	TurnPoster    = networkid.UserID("u-erin")
	TurnGroupMsg  = "m-erin"
	TurnPostAgain = "lc-again" // begins the id of each later connect's post
	// AfterMsg begins the id of a message of Alice's in the group that every later connect
	// of a login with Script.Channel or Script.TurnedChannel sends after the
	// channel's posts: once it is delivered, so is whatever came of them.
	AfterMsg = "g-after"
	// ChanGhost is the channel's own ghost (Telegram's sender of a
	// broadcast channel's posts), Frank a person met nowhere but the group.
	// With Script.GhostUpdates both, and ChanPoster, get their profiles
	// without any event first.
	ChanGhost = networkid.UserID("c-news")
	Frank     = networkid.UserID("u-frank")
	FrankMsg  = "m-frank"

	// The chats of Script.ReID. MiniGroup becomes SuperGroup; MergeFrom is
	// merged into MergeInto.
	MiniGroup     = "mini-1"
	SuperGroup    = "super-1"
	MiniMsg       = "mm1"
	SuperMsg      = "sm1"
	ReIDAgain     = "sm-again" // begins the id of each later connect's message
	MergeFrom     = "mini-2"
	MergeInto     = "super-2"
	SuperHistoryN = 3
	// LateOldMsg begins the id of each message of Script.LateOld, which
	// LateSender sends; LateSender is met nowhere else.
	LateOldMsg = "mm-late"
	LateOldN   = 3
	LateSender = networkid.UserID("u-straggler")
	// RejoinMsg is Alice's message in SuperGroup of Script.Rejoin;
	// CancelledMsg hers whose room creation the re-ID cancels
	// (Script.CancelCreate).
	RejoinMsg    = "sm-rejoin"
	CancelledMsg = "sm-cancelled"

	// The chats of Script.ExistingHistory, which hold messages before the
	// sign-in. OldMark is in every text of OldChan. OldLiveMsg is Dave's
	// message that comes live after the sync made them.
	OldDM      = "dm-dave"
	OldGroup   = "grp-old"
	OldChan    = "chan-old"
	OldMark    = "OLDCHANMARK"
	OldLiveMsg = "old-live"
	Dave       = networkid.UserID("u-dave")

	// HistoryCount is the size of the group's older history.
	HistoryCount = 120
	// AliceText is the text of the first message; tests check logs for it.
	AliceText = "Hello from Alice SECRETTEXT"
)

// Base is the script's fixed clock: every scripted event has a time derived
// from it, so the same script gives the same events.
var Base = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

var historyBase = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var tinyPNG, _ = base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")

type person struct {
	name string
	ids  []string
	png  bool
}

var people = map[networkid.UserID]person{
	Alice:      {name: "Alice Example", ids: []string{"tel:+15550000001", "fakenet:alice"}, png: true},
	Bob:        {name: "Bob Example", ids: []string{"tel:+15550000002", "fakenet:bob"}},
	Carol:      {name: "Carol Example", ids: []string{"fakenet:carol"}},
	Dave:       {name: "Dave Example", ids: []string{"tel:+15550000004", "fakenet:dave"}},
	ChanPoster: {name: "Poster " + ChanMark, ids: []string{"fakenet:poster"}},
	TurnPoster: {name: "Erin Example", ids: []string{"fakenet:erin"}},
	ChanGhost:  {name: "News channel " + ChanMark, ids: []string{"fakenet:news"}},
	Frank:      {name: "Frank Example", ids: []string{"fakenet:frank"}},
	LateSender: {name: "Straggler Example", ids: []string{"fakenet:straggler"}},
}

// Client is one login's connection to the fake network.
type Client struct {
	c        *Connector
	ul       *bridgev2.UserLogin
	loggedIn atomic.Bool
	// gapped is set at a later connect with Script.HistoryGap: OldGroup
	// then holds that many more messages.
	gapped atomic.Bool
	// mu is held while one remote event is handed to the framework;
	// Disconnect takes it, so no event is handed over once it returned, as
	// with a real client whose connection is closed.
	mu      sync.Mutex
	stopped bool
	// cancelReID is armed (Script.CancelCreate) while CancelledMsg is
	// handed over: GetCapabilities takes it and makes the re-ID that
	// cancels the room creation, closing it once the re-ID returned.
	cancelReID atomic.Pointer[chan struct{}]
}

// queue hands one remote event to the framework, unless the client was
// disconnected.
func (cl *Client) queue(evt bridgev2.RemoteEvent) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.stopped {
		return
	}
	cl.ul.QueueRemoteEvent(evt)
}

var (
	_ bridgev2.NetworkAPI                    = (*Client)(nil)
	_ bridgev2.EditHandlingNetworkAPI        = (*Client)(nil)
	_ bridgev2.ReactionHandlingNetworkAPI    = (*Client)(nil)
	_ bridgev2.RedactionHandlingNetworkAPI   = (*Client)(nil)
	_ bridgev2.ReadReceiptHandlingNetworkAPI = (*Client)(nil)
	_ bridgev2.BackfillingNetworkAPI         = (*Client)(nil)
	_ bridgev2.IdentifierResolvingNetworkAPI = (*Client)(nil)
	_ bridgev2.ContactListingNetworkAPI      = (*Client)(nil)
)

func (cl *Client) self() networkid.UserID { return networkid.UserID(cl.ul.ID) }

func (cl *Client) key(chat string) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(chat), Receiver: cl.ul.ID}
}

func (cl *Client) Connect(ctx context.Context) {
	cl.mu.Lock()
	stopped := cl.stopped
	cl.mu.Unlock()
	if stopped {
		return
	}
	cl.loggedIn.Store(true)
	cl.ul.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	cl.ul.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
	meta, _ := cl.ul.Metadata.(*LoginMeta)
	synced := meta != nil && meta.Synced
	if meta != nil && !meta.Synced && !cl.c.script.NoSync {
		cl.sync(ctx)
		meta.Synced = true
		_ = cl.ul.Save(ctx)
	}
	if synced && cl.c.script.Channel {
		cl.send(Chan, ChanPoster, Base.Add(2*time.Hour), ChanPostAgain,
			&bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("again " + ChanMark)}})
	}
	// Ids of a later connect's posts: a framework drops a message whose id
	// it has (a post of an earlier connect).
	again := "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if synced && cl.c.script.ExistingHistory > 0 && cl.c.script.HistoryGap > 0 {
		cl.gapped.Store(true)
		cl.queue(&simplevent.ChatResync{
			EventMeta:       simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(OldGroup)},
			ChatInfo:        cl.chatInfo(OldGroup),
			LatestMessageTS: OldTime(cl.oldCount(OldGroup) - 1),
		})
	}
	if synced && cl.c.script.TurnedChannel {
		if cl.c.script.TurnBack {
			cl.queue(&simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(TurnChan)},
				ChatInfo:  &bridgev2.ChatInfo{ExtraUpdates: setBroadcast(false)},
			})
		}
		cl.send(TurnChan, Alice, Base.Add(2*time.Hour), TurnPostAgain+again,
			&bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("again " + TurnMark)}})
	}
	if synced && cl.c.script.ReID {
		if cl.c.script.LateOld {
			cl.lateOld(LateOldMsg+again, Base.Add(2*time.Hour))
		}
		cl.send(SuperGroup, Alice, Base.Add(2*time.Hour), ReIDAgain+again,
			&bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("again in the supergroup")}})
	}
	if synced && (cl.c.script.Channel || cl.c.script.TurnedChannel) {
		cl.send(Group, Alice, Base.Add(2*time.Hour+time.Second), AfterMsg+again,
			&bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("after the channels")}})
	}
	switch cl.c.script.AfterSync {
	case "bad_credentials":
		cl.ul.BridgeState.Send(status.BridgeState{StateEvent: status.StateBadCredentials, Error: "fake-bad-credentials"})
	case "logged_out":
		cl.ul.BridgeState.Send(status.BridgeState{StateEvent: status.StateLoggedOut, Error: "fake-logged-out"})
	}
	if t := cl.c.script.Ticker; t.Count > 0 {
		go cl.tick(ctx, t.Count, time.Duration(t.IntervalMs)*time.Millisecond)
	}
}

// Disconnect ends the client for good (the framework makes a new client to
// connect again); it returns once no remote event is being handed over.
func (cl *Client) Disconnect() {
	cl.loggedIn.Store(false)
	cl.mu.Lock()
	cl.stopped = true
	cl.mu.Unlock()
}
func (cl *Client) IsLoggedIn() bool { return cl.loggedIn.Load() }

func (cl *Client) LogoutRemote(ctx context.Context) {
	if ms := cl.c.script.LogoutHangMs; ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	if cl.c.script.LateWrites {
		keepLate(cl.c.br, cl.ul.ID)
	}
	cl.loggedIn.Store(false)
	cl.c.record(Record{Kind: "logout", Target: string(cl.ul.ID)})
}

func (cl *Client) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return userID == cl.self()
}

func (cl *Client) member(u networkid.UserID, pl int) bridgev2.ChatMember {
	m := bridgev2.ChatMember{EventSender: bridgev2.EventSender{Sender: u, IsFromMe: u == cl.self()}, Membership: event.MembershipJoin}
	if pl != 0 {
		m.PowerLevel = ptr.Ptr(pl)
	}
	return m
}

func (cl *Client) chatInfo(chat string) *bridgev2.ChatInfo {
	if cl.c.script.ExistingHistory > 0 {
		switch chat {
		case OldDM:
			return &bridgev2.ChatInfo{
				Type: ptr.Ptr(database.RoomTypeDM), CanBackfill: true,
				Members: &bridgev2.ChatMemberList{IsFull: true, OtherUserID: Dave,
					Members: []bridgev2.ChatMember{cl.member(cl.self(), 0), cl.member(Dave, 0)}},
			}
		case OldGroup:
			return &bridgev2.ChatInfo{
				Name: ptr.Ptr("Old friends"), Type: ptr.Ptr(database.RoomTypeGroupDM), CanBackfill: true,
				Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
					cl.member(cl.self(), 100), cl.member(Alice, 0), cl.member(Dave, 0),
				}},
			}
		case OldChan:
			// A default room, as mautrix-telegram makes a broadcast
			// channel, that its first chat info stores as broadcast: a
			// channel to the worker before its room is made.
			return &bridgev2.ChatInfo{
				Name: ptr.Ptr("Old news " + OldMark), CanBackfill: true,
				ExtraUpdates: setBroadcast(true),
				Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
					cl.member(cl.self(), 0), cl.member(Alice, 100),
				}},
			}
		}
	}
	switch {
	case strings.HasPrefix(chat, "dm-"):
		other := networkid.UserID("u-" + strings.TrimPrefix(chat, "dm-"))
		return &bridgev2.ChatInfo{
			Type: ptr.Ptr(database.RoomTypeDM),
			Members: &bridgev2.ChatMemberList{IsFull: true, OtherUserID: other,
				Members: []bridgev2.ChatMember{cl.member(cl.self(), 0), cl.member(other, 0)}},
		}
	case chat == Chan:
		return &bridgev2.ChatInfo{
			Name: ptr.Ptr("News " + ChanMark),
			Type: ptr.Ptr(database.RoomTypeSpace),
			Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
				cl.member(cl.self(), 0), cl.member(ChanPoster, 100),
			}},
		}
	case chat == TurnChan:
		return &bridgev2.ChatInfo{
			Name: ptr.Ptr("Turn news"),
			Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
				cl.member(cl.self(), 0), cl.member(Alice, 100),
			}},
		}
	case chat == MiniGroup || chat == MergeFrom:
		return &bridgev2.ChatInfo{
			Name: ptr.Ptr("Mini group " + chat),
			Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
				cl.member(cl.self(), 100), cl.member(Alice, 0),
			}},
		}
	case chat == SuperGroup || chat == MergeInto:
		return &bridgev2.ChatInfo{
			Name: ptr.Ptr("Super group " + chat),
			Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
				cl.member(cl.self(), 100), cl.member(Alice, 0), cl.member(Bob, 0),
			}},
		}
	case chat == Group:
		return &bridgev2.ChatInfo{
			Name:        ptr.Ptr("Dinner club"),
			Topic:       ptr.Ptr("Scripted group"),
			Type:        ptr.Ptr(database.RoomTypeGroupDM),
			CanBackfill: true,
			Members: &bridgev2.ChatMemberList{IsFull: true, Members: []bridgev2.ChatMember{
				cl.member(cl.self(), 100), cl.member(Alice, 50), cl.member(Bob, 0),
			}},
		}
	}
	return nil
}

func (cl *Client) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if info := cl.chatInfo(string(portal.ID)); info != nil {
		return info, nil
	}
	return nil, errors.New("fakenet: unknown chat")
}

func userInfo(u networkid.UserID) *bridgev2.UserInfo {
	p, ok := people[u]
	if !ok {
		return nil
	}
	info := &bridgev2.UserInfo{Name: ptr.Ptr(p.name), Identifiers: p.ids, IsBot: ptr.Ptr(false)}
	if p.png {
		info.Avatar = &bridgev2.Avatar{ID: networkid.AvatarID("av-" + string(u)), Get: func(ctx context.Context) ([]byte, error) { return tinyPNG, nil }}
	}
	return info
}

func (cl *Client) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	if ghost.ID == cl.self() {
		if cl.c.script.NoSelfInfo {
			return nil, nil
		}
		return &bridgev2.UserInfo{Name: ptr.Ptr(cl.c.script.AccountName)}, nil
	}
	return userInfo(ghost.ID), nil
}

// GetCapabilities is asked for a portal without a room only while its room
// is being made, after the framework asked for the room's id
// (createMatrixRoomInLoop), with the creation's cancellable context. With
// Script.CancelCreate, the creation of SuperGroup's room for CancelledMsg
// starts the re-ID of MiniGroup onto SuperGroup and waits until the
// framework cancels the creation: the re-ID waits for the creation's lock,
// and the creation returns once it sees its context cancelled.
func (cl *Client) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	if portal.MXID == "" && portal.ID == SuperGroup {
		if done := cl.cancelReID.Swap(nil); done != nil {
			go func() {
				defer close(*done)
				// Under cl.mu, held by the send that waits here.
				cl.reIDHeld(context.WithoutCancel(ctx), MiniGroup, SuperGroup)
			}()
			select {
			case <-ctx.Done():
			case <-time.After(30 * time.Second):
				// A fail-safe, not an order: the framework did not cancel.
				cl.c.record(Record{Kind: "cancel_missed", Chat: SuperGroup})
			}
		}
	}
	return &event.RoomFeatures{
		ID: "fi.fakenet.caps.v1",
		Formatting: event.FormattingFeatureMap{
			event.FmtBold: event.CapLevelFullySupported, event.FmtUserLink: event.CapLevelFullySupported,
		},
		File: event.FileFeatureMap{
			event.MsgImage: &event.FileFeatures{MimeTypes: map[string]event.CapabilitySupportLevel{"image/jpeg": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: 16 << 20},
		},
		MaxTextLength: 4096,
		Reply:         event.CapLevelFullySupported,
		Edit:          event.CapLevelFullySupported,
		Delete:        event.CapLevelFullySupported,
		Reaction:      event.CapLevelFullySupported,
		ReadReceipts:  true,
	}
}

// ---- scripted traffic ----

func text(body string) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: body}}
}

func (cl *Client) meta(t bridgev2.RemoteEventType, chat string, sender networkid.UserID, ts time.Time) simplevent.EventMeta {
	return simplevent.EventMeta{
		Type: t, PortalKey: cl.key(chat), CreatePortal: true, Timestamp: ts,
		Sender: bridgev2.EventSender{Sender: sender, IsFromMe: sender == cl.self()},
	}
}

func (cl *Client) send(chat string, sender networkid.UserID, ts time.Time, id string, cm *bridgev2.ConvertedMessage) {
	cl.queue(&simplevent.PreConvertedMessage{
		EventMeta: cl.meta(bridgev2.RemoteEventMessage, chat, sender, ts),
		ID:        networkid.MessageID(id),
		Data:      cm,
	})
}

// sync plays the initial burst: two direct chats and a group, every kind of
// event once.
func (cl *Client) sync(ctx context.Context) {
	tick := 0
	ts := func() time.Time { tick++; return Base.Add(time.Duration(tick) * time.Second) }
	if cl.c.script.GhostUpdates {
		cl.updateGhosts(ctx, ChanGhost, ChanPoster, Frank)
	}
	for _, chat := range []string{DMAlice, DMBob, Group, DMCarol} {
		cl.queue(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(chat), CreatePortal: true},
			ChatInfo:  cl.chatInfo(chat),
		})
	}
	cl.send(DMAlice, Alice, ts(), "m1", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
		Type: event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgText, Body: AliceText,
			Format: event.FormatHTML, FormattedBody: "<b>Hello</b> from Alice SECRETTEXT"},
	}}})
	uri, _ := cl.c.br.Matrix.GenerateContentURI(ctx, networkid.MediaID("img-1"))
	thumb, _ := cl.c.br.Matrix.GenerateContentURI(ctx, networkid.MediaID("img-1-thumb"))
	cl.send(DMBob, Bob, ts(), "m2", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{
		{ID: "img", Type: event.EventMessage, Content: &event.MessageEventContent{
			MsgType: event.MsgImage, Body: "photo.jpg", FileName: "photo.jpg", URL: uri,
			Info: &event.FileInfo{MimeType: "image/jpeg", Size: MediaSize, Width: 640, Height: 480, ThumbnailURL: thumb}}},
		{ID: "cap", Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "look at this"}},
	}})
	alice := cl.c.br.Matrix.FormatGhostMXID(Alice)
	cl.send(Group, Bob, ts(), "m3", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
		Type: event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "Hi group, Alice Example: welcome",
			Format: event.FormatHTML, FormattedBody: fmt.Sprintf(`Hi group, <a href="https://matrix.to/#/%s">Alice Example</a>: welcome`, alice),
			Mentions: &event.Mentions{UserIDs: []id.UserID{alice}}},
	}}})
	m4 := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("replying to bob")}}
	m4.ReplyTo = &networkid.MessageOptionalPartID{MessageID: "m3"}
	cl.send(Group, Alice, ts(), "m4", m4)
	m7 := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("in a thread")}}
	m7.ThreadRoot = ptr.Ptr(networkid.MessageID("m3"))
	cl.send(Group, Alice, ts(), "m7", m7)
	cl.queue(&simplevent.Message[string]{
		EventMeta:     cl.meta(bridgev2.RemoteEventEdit, DMAlice, Alice, ts()),
		TargetMessage: "m1",
		Data:          "Hello from Alice (edited)",
		ConvertEditFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, data string) (*bridgev2.ConvertedEdit, error) {
			return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{{
				Part: existing[0], Type: event.EventMessage,
				Content: &event.MessageEventContent{MsgType: event.MsgText, Body: data},
			}}}, nil
		},
	})
	cl.queue(&simplevent.Reaction{
		EventMeta: cl.meta(bridgev2.RemoteEventReaction, Group, Bob, ts()), TargetMessage: "m3", EmojiID: "👍", Emoji: "👍"})
	cl.queue(&simplevent.Reaction{
		EventMeta: cl.meta(bridgev2.RemoteEventReactionRemove, Group, Bob, ts()), TargetMessage: "m3", EmojiID: "👍"})
	cl.send(Group, Alice, ts(), "m5", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("to be deleted")}})
	cl.queue(&simplevent.MessageRemove{
		EventMeta: cl.meta(bridgev2.RemoteEventMessageRemove, Group, Alice, ts()), TargetMessage: "m5"})
	cl.send(DMAlice, cl.self(), ts(), "m6", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("sent from my phone")}})
	cl.queue(&simplevent.Receipt{
		EventMeta: cl.meta(bridgev2.RemoteEventDeliveryReceipt, DMAlice, Alice, ts()), Targets: []networkid.MessageID{"m6"}})
	cl.queue(&simplevent.Receipt{
		EventMeta: cl.meta(bridgev2.RemoteEventReadReceipt, DMAlice, Alice, ts()), LastTarget: "m6", Targets: []networkid.MessageID{"m6"}})
	cl.queue(&simplevent.Receipt{
		EventMeta: cl.meta(bridgev2.RemoteEventReadReceipt, Group, cl.self(), ts()), LastTarget: "m4", Targets: []networkid.MessageID{"m4"}})
	if cl.c.script.Channel {
		cl.syncChannel(ts)
	}
	if cl.c.script.ExistingHistory > 0 {
		cl.syncOld()
	}
	if cl.c.script.GhostUpdates {
		cl.send(Group, Frank, ts(), FrankMsg, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("frank in the group")}})
	}
	if cl.c.script.TurnedChannel {
		cl.syncTurnedChannel(ts)
	}
	if cl.c.script.ReID {
		cl.syncReID(ctx, ts)
	}
	cl.queue(&simplevent.ChatInfoChange{
		EventMeta:      cl.meta(bridgev2.RemoteEventChatInfoChange, Group, Alice, ts()),
		ChatInfoChange: &bridgev2.ChatInfoChange{ChatInfo: &bridgev2.ChatInfo{Name: ptr.Ptr("Renamed club")}},
	})
	cl.queue(&simplevent.ChatDelete{
		EventMeta: cl.meta(bridgev2.RemoteEventChatDelete, DMCarol, Carol, ts())})
	cl.c.record(Record{Kind: "synced"})
}

// syncOld plays the chats of Script.ExistingHistory: each made by a chat
// info alone, as a dialog sync makes a chat that has messages; the
// framework fetches its newest messages as it makes the room. Then OldDM's
// newest message comes again live, and a new one of Dave's.
func (cl *Client) syncOld() {
	for _, chat := range []string{OldDM, OldGroup, OldChan} {
		cl.queue(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(chat), CreatePortal: true},
			ChatInfo:  cl.chatInfo(chat),
		})
	}
	newest := cl.oldMessage(OldDM, cl.c.script.ExistingHistory-1)
	cl.queue(&simplevent.PreConvertedMessage{
		EventMeta: cl.meta(bridgev2.RemoteEventMessage, OldDM, newest.Sender.Sender, newest.Timestamp),
		ID:        newest.ID,
		Data:      newest.ConvertedMessage,
	})
	cl.send(OldDM, Dave, oldBase.Add(time.Duration(cl.c.script.ExistingHistory)*time.Minute), OldLiveMsg,
		&bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("dave, live")}})
}

// setBroadcast stores the portal's metadata as broadcast or not.
func setBroadcast(to bool) func(context.Context, *bridgev2.Portal) bool {
	return func(ctx context.Context, p *bridgev2.Portal) bool {
		m, _ := p.Metadata.(*PortalMeta)
		if m == nil || m.Broadcast == to {
			return false
		}
		m.Broadcast = to
		return true
	}
}

// updateGhosts gives people their profiles outside any event, as a
// connector does with the people of a network update (mautrix-telegram:
// every update's users, and a channel's own ghost).
func (cl *Client) updateGhosts(ctx context.Context, users ...networkid.UserID) {
	for _, u := range users {
		if ghost, err := cl.c.br.GetGhostByID(ctx, u); err == nil {
			ghost.UpdateInfo(ctx, userInfo(u))
		}
	}
}

// syncTurnedChannel plays the room of Script.TurnedChannel.
func (cl *Client) syncTurnedChannel(ts func() time.Time) {
	cl.queue(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(TurnChan), CreatePortal: true},
		ChatInfo:  cl.chatInfo(TurnChan),
	})
	cl.send(TurnChan, Alice, ts(), TurnBefore, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("before it is known")}})
	cl.queue(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(TurnChan)},
		ChatInfo:  &bridgev2.ChatInfo{ExtraUpdates: setBroadcast(true)},
	})
	cl.send(TurnChan, Alice, ts(), "lc2", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("post " + TurnMark)}})
	cl.queue(&simplevent.Reaction{
		EventMeta: cl.meta(bridgev2.RemoteEventReaction, TurnChan, Alice, ts()), TargetMessage: TurnBefore, EmojiID: "🔥", Emoji: "🔥"})
	cl.send(TurnChan, TurnPoster, ts(), "lc3", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("erin " + TurnMark)}})
	cl.send(Group, TurnPoster, ts(), TurnGroupMsg, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("erin in the group")}})
}

// syncReID plays the chats of Script.ReID: each is made with a message in
// it, then re-IDed (reID); after the re-ID Alice sends SuperMsg and reads
// up to it by stream position.
func (cl *Client) syncReID(ctx context.Context, ts func() time.Time) {
	for _, chat := range []string{MiniGroup, MergeFrom, MergeInto} {
		cl.queue(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(chat), CreatePortal: true},
			ChatInfo:  cl.chatInfo(chat),
		})
	}
	cl.send(MiniGroup, Alice, ts(), MiniMsg, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("in the mini group")}})
	cl.send(MergeFrom, Alice, ts(), "mm2", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("in the merged group")}})
	cl.send(MergeInto, Alice, ts(), "sm2", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("in the merge target")}})
	cl.reID(ctx, MiniGroup, SuperGroup)
	cl.send(SuperGroup, Alice, ts(), SuperMsg, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("in the supergroup")}})
	// A read by stream position (no target message), which the framework
	// maps to the last message at or before its time.
	cl.queue(&simplevent.Receipt{EventMeta: cl.meta(bridgev2.RemoteEventReadReceipt, SuperGroup, Alice, ts()), ReadUpToStreamOrder: 1})
	cl.reID(ctx, MergeFrom, MergeInto)
	if cl.c.script.LateOld {
		for i := range LateOldN {
			cl.lateOld(LateOldMsg+"-"+strconv.Itoa(i), ts())
		}
		if cl.c.script.Rejoin {
			cl.queue(&simplevent.ChatDelete{EventMeta: cl.meta(bridgev2.RemoteEventChatDelete, SuperGroup, cl.self(), ts())})
			if cl.c.script.CancelCreate {
				cl.cancelledCreate(ctx, ts())
			} else {
				cl.reID(ctx, MiniGroup, SuperGroup)
			}
			cl.send(SuperGroup, Alice, ts(), RejoinMsg, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("back in the supergroup")}})
		}
	}
}

// lateOld sends a message of LateSender's under MiniGroup's key, re-IDed
// before (Script.LateOld), and logs how the framework handled it.
func (cl *Client) lateOld(id string, ts time.Time) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.stopped {
		return
	}
	res := cl.ul.QueueRemoteEvent(&simplevent.PreConvertedMessage{
		EventMeta: cl.meta(bridgev2.RemoteEventMessage, MiniGroup, LateSender, ts),
		ID:        networkid.MessageID(id),
		Data:      &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("late in the old mini group")}},
	})
	outcome := "failed"
	if res.Ignored {
		outcome = "ignored"
	} else if res.Success {
		outcome = "ok"
	}
	cl.c.record(Record{Kind: "late", Txn: id, Chat: MiniGroup, Target: outcome})
}

// reID gives the portal of chat from the key of chat to, as
// mautrix-telegram's migrateChat does (handletelegram.go:748): the
// framework's own Bridge.ReIDPortal, then, when that renamed the source
// portal, the portal's info under its new key (GetChatInfo, UpdateInfo).
// The outcome goes to the network log ("reid", Target: to and the result).
func (cl *Client) reID(ctx context.Context, from, to string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.stopped {
		return
	}
	cl.reIDHeld(ctx, from, to)
}

// cancelledCreate sends CancelledMsg in SuperGroup, whose room creation
// makes the re-ID of MiniGroup onto SuperGroup (Script.CancelCreate,
// GetCapabilities), and returns once that re-ID returned. When the
// creation never asked (the message was not handled), the re-ID is made
// here, logged "cancel_missed".
func (cl *Client) cancelledCreate(ctx context.Context, ts time.Time) {
	done := make(chan struct{})
	cl.cancelReID.Store(&done)
	cl.send(SuperGroup, Alice, ts, CancelledMsg, &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("while the room is made")}})
	if cl.cancelReID.Swap(nil) != nil {
		cl.c.record(Record{Kind: "cancel_missed", Chat: SuperGroup})
		cl.reID(ctx, MiniGroup, SuperGroup)
		return
	}
	<-done
}

// reIDHeld is reID while cl.mu is held (by the caller, or by the send whose
// room creation made it: cancelledCreate).
func (cl *Client) reIDHeld(ctx context.Context, from, to string) {
	res, portal, err := cl.c.br.ReIDPortal(ctx, cl.key(from), cl.key(to))
	if err != nil {
		cl.c.record(Record{Kind: "reid", Chat: from, Target: to + ":error"})
		return
	}
	if (res == bridgev2.ReIDResultSourceReIDd || res == bridgev2.ReIDResultTargetDeletedAndSourceReIDd) && portal != nil {
		if info, err := cl.GetChatInfo(ctx, portal); err == nil {
			portal.UpdateInfo(ctx, info, cl.ul, nil, time.Time{})
		}
	}
	cl.c.record(Record{Kind: "reid", Chat: from, Target: to + ":" + res.String()})
}

// syncChannel plays the channel of Script.Channel: created, a post, a
// reaction to it, a post whose attachment is uploaded to the worker.
func (cl *Client) syncChannel(ts func() time.Time) {
	cl.queue(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: cl.key(Chan), CreatePortal: true},
		ChatInfo:  cl.chatInfo(Chan),
	})
	cl.send(Chan, ChanPoster, ts(), "cp1", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("post " + ChanMark)}})
	if cl.c.script.GhostUpdates {
		cl.send(Chan, ChanGhost, ts(), "cp-ghost", &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text("as the channel " + ChanMark)}})
	}
	cl.queue(&simplevent.Reaction{
		EventMeta: cl.meta(bridgev2.RemoteEventReaction, Chan, ChanPoster, ts()), TargetMessage: "cp1", EmojiID: "🔥", Emoji: "🔥"})
	cl.queue(&simplevent.Message[string]{
		EventMeta: cl.meta(bridgev2.RemoteEventMessage, Chan, ChanPoster, ts()),
		ID:        "cp2",
		Data:      ChanMark + " file",
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data string) (*bridgev2.ConvertedMessage, error) {
			uri, _, err := intent.UploadMedia(ctx, portal.MXID, []byte(data), ChanMark+".png", "image/png")
			if err != nil {
				return nil, err
			}
			return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage,
				Content: &event.MessageEventContent{MsgType: event.MsgImage, Body: ChanMark + ".png", URL: uri}}}}, nil
		},
	})
}

func (cl *Client) tick(ctx context.Context, n int, every time.Duration) {
	t := cl.c.script.Ticker
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Duration(t.DelayMs) * time.Millisecond):
	}
	pad := strings.Repeat("x", t.PadBytes)
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		chat := DMAlice
		sender := Alice
		if i%2 == 1 {
			chat, sender = DMBob, Bob
		}
		cl.send(chat, sender, Base.Add(time.Hour+time.Duration(i)*time.Second), fmt.Sprintf("t%03d", i),
			&bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text(fmt.Sprintf("tick %d %s", i, pad))}})
	}
}

// ---- outgoing ----

func (cl *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	c := cl.c
	body := msg.Content.Body
	txn := string(msg.InputTransactionID)
	chat := string(msg.Portal.ID)
	switch {
	case strings.Contains(body, MagicCertain):
		return nil, bridgev2.WrapErrorInStatus(errors.New("fakenet: refused")).
			WithIsCertain(true).WithStatus(event.MessageStatusFail).WithErrorReason(event.MessageStatusUnsupported)
	case strings.Contains(body, MagicUncertain):
		c.record(Record{Kind: "attempt", Txn: txn, Chat: chat})
		return nil, errors.New("fakenet: connection reset")
	case strings.Contains(body, MagicHang):
		if err := c.hang(ctx); err != nil {
			return nil, err
		}
	case strings.Contains(body, MagicCrashBefore):
		c.record(Record{Kind: "crash", Txn: txn, Chat: chat})
		os.Exit(3)
	}
	remote := c.deliver(txn, chat)
	if strings.Contains(body, MagicCrashAfter) {
		os.Exit(3)
	}
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{
		ID: networkid.MessageID(remote), SenderID: cl.self(), Timestamp: Base.Add(24 * time.Hour),
	}}, nil
}

func (cl *Client) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	cl.c.record(Record{Kind: "edit", Target: string(msg.EditTarget.ID)})
	return nil
}

func (cl *Client) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	return bridgev2.MatrixReactionPreResponse{SenderID: cl.self(), EmojiID: networkid.EmojiID(msg.Content.RelatesTo.Key), Emoji: msg.Content.RelatesTo.Key}, nil
}

func (cl *Client) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	cl.c.record(Record{Kind: "react", Chat: string(msg.Portal.ID), Target: string(msg.TargetMessage.ID)})
	return nil, nil
}

func (cl *Client) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	cl.c.record(Record{Kind: "unreact", Target: string(msg.TargetReaction.MessageID)})
	return nil
}

func (cl *Client) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	cl.c.record(Record{Kind: "delete", Target: string(msg.TargetMessage.ID)})
	return nil
}

func (cl *Client) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	var target string
	if msg.ExactMessage != nil {
		target = string(msg.ExactMessage.ID)
	}
	cl.c.record(Record{Kind: "read", Chat: string(msg.Portal.ID), Target: target})
	return nil
}

// ---- history ----

// HistoryID is the id of the i-th older message of the group.
func HistoryID(i int) string { return fmt.Sprintf("h%03d", i) }

func (cl *Client) historyMessage(i int) *bridgev2.BackfillMessage {
	senders := []networkid.UserID{Alice, Bob, cl.self()}
	s := senders[i%len(senders)]
	msg := &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text(fmt.Sprintf("history %03d", i))}},
		Sender:           bridgev2.EventSender{Sender: s, IsFromMe: s == cl.self()},
		ID:               networkid.MessageID(HistoryID(i)),
		Timestamp:        historyBase.Add(time.Duration(i) * time.Minute),
	}
	if i == 101 {
		msg.ReplyTo = &networkid.MessageOptionalPartID{MessageID: networkid.MessageID(HistoryID(100))}
	}
	if i == 100 {
		msg.Reactions = []*bridgev2.BackfillReaction{{Sender: bridgev2.EventSender{Sender: Bob}, EmojiID: "❤", Emoji: "❤"}}
	}
	return msg
}

// SuperHistoryID is the id of the i-th older message of SuperGroup.
func SuperHistoryID(i int) string { return fmt.Sprintf("sh%d", i) }

// oldBase is the clock of Script.ExistingHistory's messages: before Base,
// so the sync's own messages are newer.
var oldBase = time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)

// OldHistoryID is the id of the i-th message (oldest first) of a chat of
// Script.ExistingHistory.
func OldHistoryID(chat string, i int) string { return fmt.Sprintf("%s-o%03d", chat, i) }

// OldTime is the time of the i-th message of a chat of
// Script.ExistingHistory.
func OldTime(i int) time.Time { return oldBase.Add(time.Duration(i) * time.Minute) }

// OldSender is the sender of the i-th message of a chat of
// Script.ExistingHistory: in OldDM Dave and the account in turn, in OldGroup
// Alice, Dave and the account, in OldChan Alice.
func OldSender(chat string, i int, self networkid.UserID) networkid.UserID {
	switch chat {
	case OldDM:
		return []networkid.UserID{Dave, self}[i%2]
	case OldGroup:
		return []networkid.UserID{Alice, Dave, self}[i%3]
	}
	return Alice
}

// OldText is the text of the i-th message of a chat of
// Script.ExistingHistory.
func OldText(chat string, i int) string {
	if chat == OldChan {
		return fmt.Sprintf("post %d %s", i, OldMark)
	}
	return fmt.Sprintf("old %s %03d", chat, i)
}

func (cl *Client) oldMessage(chat string, i int) *bridgev2.BackfillMessage {
	s := OldSender(chat, i, cl.self())
	return &bridgev2.BackfillMessage{
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text(OldText(chat, i))}},
		Sender:           bridgev2.EventSender{Sender: s, IsFromMe: s == cl.self()},
		ID:               networkid.MessageID(OldHistoryID(chat, i)),
		Timestamp:        OldTime(i),
	}
}

// fetchOld serves a chat of Script.ExistingHistory as a network serves a
// chat's history: forward, the newest Count after the anchor (all of them
// when there is none, as mautrix-telegram asks for a new room); backward,
// the Count before the anchor or the cursor (an index), oldest first.
// oldCount is how many messages a chat of Script.ExistingHistory holds now.
func (cl *Client) oldCount(chat string) int {
	if chat == OldGroup && cl.gapped.Load() {
		return cl.c.script.ExistingHistory + cl.c.script.HistoryGap
	}
	return cl.c.script.ExistingHistory
}

func (cl *Client) fetchOld(p bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	chat := string(p.Portal.ID)
	dir := "backward"
	if p.Forward {
		dir = "forward"
	}
	cl.c.record(Record{Kind: "fetch", Chat: chat, Target: fmt.Sprintf("%s:%d", dir, p.Count)})
	n := cl.oldCount(chat)
	start, end := 0, n
	if p.AnchorMessage != nil {
		at := p.AnchorMessage.Timestamp
		if p.Forward {
			for start < n && !OldTime(start).After(at) {
				start++
			}
		} else {
			for end > 0 && !OldTime(end-1).Before(at) {
				end--
			}
		}
	}
	if !p.Forward && p.Cursor != "" {
		c, err := strconv.Atoi(string(p.Cursor))
		if err != nil || c < 0 || c > n {
			return nil, errors.New("fakenet: bad cursor")
		}
		end = min(end, c)
	}
	start = max(start, end-p.Count)
	msgs := make([]*bridgev2.BackfillMessage, 0, max(0, end-start))
	for i := start; i < end; i++ {
		msgs = append(msgs, cl.oldMessage(chat, i))
	}
	if p.Forward {
		return &bridgev2.FetchMessagesResponse{Messages: msgs, Forward: true}, nil
	}
	return &bridgev2.FetchMessagesResponse{Messages: msgs, Cursor: networkid.PaginationCursor(strconv.Itoa(start)), HasMore: start > 0}, nil
}

func (cl *Client) FetchMessages(ctx context.Context, p bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if cl.c.script.ExistingHistory > 0 {
		switch string(p.Portal.ID) {
		case OldDM, OldGroup, OldChan:
			return cl.fetchOld(p)
		}
	}
	if string(p.Portal.ID) == SuperGroup && !p.Forward && p.Cursor == "" {
		msgs := make([]*bridgev2.BackfillMessage, 0, SuperHistoryN)
		for i := 0; i < SuperHistoryN; i++ {
			msgs = append(msgs, &bridgev2.BackfillMessage{
				ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{text(fmt.Sprintf("super history %d", i))}},
				Sender:           bridgev2.EventSender{Sender: Alice},
				ID:               networkid.MessageID(SuperHistoryID(i)),
				Timestamp:        historyBase.Add(time.Duration(i) * time.Minute),
			})
		}
		return &bridgev2.FetchMessagesResponse{Messages: msgs, HasMore: false}, nil
	}
	if string(p.Portal.ID) != Group || p.Forward {
		return &bridgev2.FetchMessagesResponse{HasMore: false, Forward: p.Forward}, nil
	}
	end := HistoryCount
	if p.Cursor != "" {
		n, err := strconv.Atoi(string(p.Cursor))
		if err != nil || n < 0 || n > HistoryCount {
			return nil, errors.New("fakenet: bad cursor")
		}
		end = n
	}
	start := max(0, end-p.Count)
	msgs := make([]*bridgev2.BackfillMessage, 0, end-start)
	for i := start; i < end; i++ {
		msgs = append(msgs, cl.historyMessage(i))
	}
	return &bridgev2.FetchMessagesResponse{Messages: msgs, Cursor: networkid.PaginationCursor(strconv.Itoa(start)), HasMore: start > 0}, nil
}

// ---- people ----

func (cl *Client) resolve(ctx context.Context, uid networkid.UserID, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	ghost, err := cl.c.br.GetGhostByID(ctx, uid)
	if err != nil {
		return nil, err
	}
	resp := &bridgev2.ResolveIdentifierResponse{Ghost: ghost, UserID: uid, UserInfo: userInfo(uid)}
	if createChat {
		chat := "dm-" + strings.TrimPrefix(string(uid), "u-")
		resp.Chat = &bridgev2.CreateChatResponse{PortalKey: cl.key(chat), PortalInfo: cl.chatInfo(chat)}
	}
	return resp, nil
}

func (cl *Client) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	// "chat:<id>" resolves to a chat of the script by its network id (a
	// group's link, say).
	if chat, ok := strings.CutPrefix(identifier, "chat:"); ok {
		info := cl.chatInfo(chat)
		if info == nil || !createChat {
			return nil, nil
		}
		return &bridgev2.ResolveIdentifierResponse{Chat: &bridgev2.CreateChatResponse{PortalKey: cl.key(chat), PortalInfo: info}}, nil
	}
	for u, p := range people {
		if string(u) == identifier {
			return cl.resolve(ctx, u, createChat)
		}
		for _, i := range p.ids {
			if i == identifier || strings.TrimPrefix(i, "tel:") == identifier || strings.TrimPrefix(i, "fakenet:") == identifier {
				return cl.resolve(ctx, u, createChat)
			}
		}
	}
	return nil, nil
}

func (cl *Client) GetContactList(ctx context.Context) ([]*bridgev2.ResolveIdentifierResponse, error) {
	var out []*bridgev2.ResolveIdentifierResponse
	for _, u := range []networkid.UserID{Alice, Bob, Carol, Dave} {
		r, err := cl.resolve(ctx, u, false)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

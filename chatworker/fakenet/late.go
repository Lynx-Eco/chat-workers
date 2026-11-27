// SPDX-License-Identifier: AGPL-3.0-or-later

package fakenet

import (
	"context"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// What the late writes of a stopped framework instance carry
// (Script.LateWrites). Every name and text holds LateMark.
const (
	LateMark  = "LATEWRITE"
	LateChat  = "late-chat"
	LateGhost = networkid.UserID("u-late")
)

// late is the framework instance of a logged-out login (Script.LateWrites),
// kept across the logout in this process: the connector of the next
// instance is a new one.
var late struct {
	mu   sync.Mutex
	br   *bridgev2.Bridge
	user networkid.UserLoginID
}

// keepLate keeps the instance a login ran in when it is logged out.
func keepLate(br *bridgev2.Bridge, user networkid.UserLoginID) {
	late.mu.Lock()
	defer late.mu.Unlock()
	late.br, late.user = br, user
}

// fireLate runs the late writes of the instance kept by keepLate, once, if
// one is kept. It is called when a sign-in begins, in the instance that
// replaced it, so they come after the logout stopped the kept instance and
// wiped the store, and before the next account's sign-in.
func fireLate() {
	late.mu.Lock()
	br, user := late.br, late.user
	late.br = nil
	late.mu.Unlock()
	if br != nil {
		LateWrite(br, user)
	}
}

// LateWrite is what a goroutine of a stopped instance br that outlived its
// stop might still do for its login user, through the backend's view and
// intents that instance holds: announce and create a room with a name and a
// member, set room state and a member in a room of the login, set a
// person's name, upload bytes, report a bridge state. Its errors are
// ignored, as such a goroutine's would be.
func LateWrite(br *bridgev2.Bridge, user networkid.UserLoginID) {
	ctx := context.Background()
	ghost := br.Matrix.GhostIntent(LateGhost)
	room := br.Matrix.GenerateDeterministicRoomID(networkid.PortalKey{ID: LateChat, Receiver: user})
	_, _ = br.Bot.CreateRoom(ctx, &mautrix.ReqCreateRoom{
		BeeperLocalRoomID: room,
		InitialState: []*event.Event{{Type: event.StateRoomName, StateKey: new(string),
			Content: event.Content{Parsed: &event.RoomNameEventContent{Name: LateMark + " chat"}}}},
		BeeperInitialMembers: []id.UserID{ghost.GetMXID()},
	})
	dm := br.Matrix.GenerateDeterministicRoomID(networkid.PortalKey{ID: DMAlice, Receiver: user})
	_, _ = br.Bot.SendState(ctx, dm, event.StateTopic, "",
		&event.Content{Parsed: &event.TopicEventContent{Topic: LateMark + " topic"}}, time.Now())
	_ = ghost.EnsureJoined(ctx, dm)
	_ = ghost.SetDisplayName(ctx, LateMark+" person")
	_, _, _ = ghost.UploadMedia(ctx, dm, []byte(LateMark+" bytes"), LateMark+".png", "image/png")
	_ = br.Matrix.SendBridgeStatus(ctx, &status.BridgeState{StateEvent: status.StateConnected, RemoteID: user})
}

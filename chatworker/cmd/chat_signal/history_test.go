// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-signal/pkg/connector"
	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/backuppb"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
)

// TestHistoryIsTheLinkArchiveOnce: Signal's only history is the archive
// the phone hands the device at the link. With the backend's framework
// config (backfill queue off) the connector serves a chat's archive once,
// to the import that makes the chat's room, and deletes it then
// (backfill.go CompleteCallback); every fetch after that, and any fetch of
// a chat the archive did not have, a direct chat or a group, is answered
// with no response at all (nil, nil), which the backend answers OK with
// has_more false and no HistoryBatch (history.go fetchHistory: "resp ==
// nil"). Nothing is asked of the network. WHY: catches a backend change
// that turns the backfill queue on (the connector would then keep the
// archive and page it backwards: a behaviour this worker documents as
// absent), a fetch that reaches Signal for history it does not keep, and a
// fetch answered with an error (the host would retry a history that does
// not exist).
func TestHistoryIsTheLinkArchiveOnce(t *testing.T) {
	cfg := backend.BridgeConfig(backend.History{Initial: backend.DefaultInitialMessages, Catchup: backend.DefaultCatchupMessages})
	if cfg.Backfill.Queue.AnyEnabled() || !cfg.Backfill.Enabled {
		t.Fatalf("backfill config %+v: the archive would not be asked for at the link, or would be kept", cfg.Backfill)
	}
	sc, db := testSignal(t)
	sc.Bridge.Config = cfg
	device := link(t, sc, mineACI, minePNI, 2, "mine-password")
	c := &connector.SignalClient{Main: sc, Client: signalmeow.NewClient(device, zerolog.Nop(), nil)}
	if !c.IsLoggedIn() {
		t.Fatal("the test device is not logged in")
	}
	ctx := t.Context()
	portal := func(id networkid.PortalID) *bridgev2.Portal {
		return &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: id, Receiver: signalid.MakeUserLoginID(mineACI)}}}
	}
	dm := portal(signalid.MakeDMPortalID(libsignalgo.NewACIServiceID(thirdACI)))
	group := portal(networkid.PortalID("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="))
	for _, p := range []*bridgev2.Portal{dm, group} {
		for _, forward := range []bool{false, true} {
			resp, err := c.FetchMessages(ctx, bridgev2.FetchMessagesParams{Portal: p, Forward: forward, Count: 50})
			if err != nil || resp != nil {
				t.Fatalf("%s forward=%v with no archive: %v %v", p.ID, forward, resp, err)
			}
		}
	}
	// The archive has the direct chat with the third person (no messages
	// left in it: the import found them).
	chat, err := proto.Marshal(&backuppb.Chat{Id: 7, RecipientId: 9})
	must(t, err)
	recipient, err := proto.Marshal(&backuppb.Recipient{Id: 9})
	must(t, err)
	_, err = db.Exec(ctx, `INSERT INTO signalmeow_backup_recipient (account_id, recipient_id, aci_uuid, data) VALUES ($1, 9, $2, $3)`, mineACI.String(), thirdACI.String(), recipient)
	must(t, err)
	_, err = db.Exec(ctx, `INSERT INTO signalmeow_backup_chat (account_id, chat_id, recipient_id, data, total_message_count) VALUES ($1, 7, 9, $2, 3)`, mineACI.String(), chat)
	must(t, err)
	resp, err := c.FetchMessages(ctx, bridgev2.FetchMessagesParams{Portal: dm, Forward: true, Count: 50})
	if err != nil || resp == nil || resp.HasMore || resp.CompleteCallback == nil {
		t.Fatalf("the import of the archive: %+v %v", resp, err)
	}
	resp.CompleteCallback()
	var left int
	must(t, db.QueryRow(ctx, `SELECT COUNT(*) FROM signalmeow_backup_chat WHERE account_id=$1`, mineACI.String()).Scan(&left))
	if left != 0 {
		t.Fatalf("%d archive chats kept after the import", left)
	}
	resp, err = c.FetchMessages(ctx, bridgev2.FetchMessagesParams{Portal: dm, Forward: false, Count: 50})
	if err != nil || resp != nil {
		t.Fatalf("a fetch after the import: %v %v", resp, err)
	}
}

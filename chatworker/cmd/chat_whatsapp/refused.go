// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"maunium.net/go/mautrix/bridgev2"

	lynxstore "lynx.invalid/ingest/chatworker/store"
)

var (
	errNotWhatsApp = errors.New("not a WhatsApp connector and login")
	errNotLoggedIn = errors.New("WhatsApp did not take the companion's login")
	errNoPreKeys   = errors.New("the copy of a refused companion has no pre-keys")
)

// dropRefused is the worker's backend.Hooks.DropRefused: it undoes a
// sign-in the gate refused because it reached another account than the
// connection's.
//
// Why a drop, and not only an end: whatsmeow saves the paired companion
// (its row in whatsmeow_device, with its keys, and the main device's
// identity) and the account's own LID-phone mapping during pairing
// (handlePair, whatsmeow pair.go:226-232), before the connector calls the
// framework's NewLogin (WALogin.onLoginComplete, mautrix-whatsapp
// connector/login.go:391-427), where the gate refuses the login. The
// connector itself has stored nothing of the login yet: its LoadUserLogin
// never ran for it.
//
// What it does, here, synchronously:
//  1. reads the refused companion from the store into memory
//     (Container.GetDevice, sqlstore/container.go:182; the JID is the login
//     id and the metadata's device number, as the connector's LoadUserLogin
//     finds it, client.go:67-74);
//  2. deletes, in one transaction (dropAccount), every whatsmeow_device row
//     of that phone number (the rows of its identities, sessions, pre-keys,
//     sender keys, app state, contacts, chat settings, message secrets, NCT
//     salt and buffers go with it: ON DELETE CASCADE, and every connection
//     of the store has foreign_keys=ON), its whatsmeow_privacy_tokens rows
//     (that table has no foreign key, upgrades/00-latest-schema.sql:138-145),
//     and the account's own row of the global whatsmeow_lid_map, unless the
//     store's other devices know that person (keptMapping).
//
// Nothing of the connection's own account is deleted: only rows of the
// refused login's phone number, which is not the connection's account (the
// gate refuses only a login whose id differs from it).
//
// It returns the end at WhatsApp (unpair), run by the backend in the
// background: the companion is logged out with a client of its own on the
// copy in memory, so that it leaves the other phone's linked devices. A
// failed read of the device (no keys: nothing can be unpaired) makes an end
// that reports that failure; no device stored (nothing was paired), none.
// The delete does not wait for the network and does not depend on it.
func dropRefused(ctx context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) (func(context.Context) error, error) {
	wa, isWA := inner.(*connector.WhatsAppConnector)
	var meta *waid.UserLoginMetadata
	if login != nil && login.UserLogin != nil {
		meta, _ = login.Metadata.(*waid.UserLoginMetadata)
	}
	if !isWA || wa.DeviceStore == nil || wa.DB == nil || meta == nil || login.ID == "" {
		return nil, errNotWhatsApp
	}
	var device *store.Device
	var readErr error
	if meta.WADeviceID != 0 {
		device, readErr = wa.DeviceStore.GetDevice(ctx, waid.ParseUserLoginID(login.ID, meta.WADeviceID))
	}
	var end func(context.Context) error
	switch {
	case device != nil:
		proxy := wa.Config.Proxy
		end = func(ctx context.Context) error { return unpair(ctx, device, proxy) }
	case readErr != nil:
		end = func(context.Context) error { return readErr }
	}
	var lid string
	if device != nil {
		lid = device.LID.User
	}
	return end, dropAccount(ctx, wa.DB.Database, string(login.ID), lid)
}

// dropAccount deletes from the whatsmeow tables every companion row of the
// phone number phone (the digits; a login id), with what cascades from it
// and its privacy tokens, and the account's own LID-phone mapping unless
// keptMapping (lid: its LID's user as the refused device gave it, "" when
// not known). One transaction: either all of it goes or none of it.
//
// A mapping row is judged by the LID it holds, and deleted by that LID
// alone. whatsmeow_lid_map has one row a LID and one a phone number
// (00-latest-schema.sql:156-159), so the number's row is found by the
// number and judged with its own LID, beside the LIDs the refused
// companions carried; a row one of them keeps is never deleted on account
// of another.
//
// The transaction is run again while another writer holds the file
// (lynxstore.RetryBusy), within the caller's time: the connection's own
// client writes to the same file (a history sync can hold it for longer
// than one busy wait), and a drop that gave up there would end the
// connection as other_account_kept, own link and all, over a lock that
// passes.
func dropAccount(ctx context.Context, db *dbutil.Database, phone, lid string) error {
	return lynxstore.RetryBusy(ctx, func() error {
		return db.DoTxn(ctx, nil, func(ctx context.Context) error {
			return dropAccountTxn(ctx, db, phone, lid)
		})
	})
}

func dropAccountTxn(ctx context.Context, db *dbutil.Database, phone, lid string) error {
	rows, err := db.Query(ctx, `SELECT jid, lid FROM whatsmeow_device`)
	if err != nil {
		return err
	}
	var jids []string
	lids := map[string]bool{}
	if lid != "" {
		lids[lid] = true
	}
	for rows.Next() {
		var raw string
		var rowLID sql.NullString
		if err := rows.Scan(&raw, &rowLID); err != nil {
			_ = rows.Close()
			return err
		}
		jid, err := types.ParseJID(raw)
		if err != nil || jid.Server != types.DefaultUserServer || jid.User != phone {
			continue
		}
		jids = append(jids, raw)
		if l, err := types.ParseJID(rowLID.String); err == nil && l.Server == types.HiddenUserServer && l.User != "" {
			lids[l.User] = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, jid := range jids {
		if _, err := db.Exec(ctx, `DELETE FROM whatsmeow_privacy_tokens WHERE our_jid=$1`, jid); err != nil {
			return err
		}
		if _, err := db.Exec(ctx, `DELETE FROM whatsmeow_device WHERE jid=$1`, jid); err != nil {
			return err
		}
	}
	var mapped sql.NullString
	switch err := db.QueryRow(ctx, `SELECT lid FROM whatsmeow_lid_map WHERE pn=$1`, phone).Scan(&mapped); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	case mapped.String != "":
		lids[mapped.String] = true
	}
	for l := range lids {
		kept, err := keptMapping(ctx, db, phone, l)
		if err != nil {
			return err
		}
		if kept {
			continue
		}
		if _, err := db.Exec(ctx, `DELETE FROM whatsmeow_lid_map WHERE lid=$1`, l); err != nil {
			return err
		}
	}
	return nil
}

// keptMapping tells whether the store's remaining devices (the connection's
// own) know the person of phone and lid (lid may be ""), as a contact or by
// a Signal session or identity: the LID-phone mapping is then theirs too
// (whatsmeow keeps one global map, sqlstore/lidmap.go, which the
// connection's own client fills from its own messages) and stays. A
// session's or identity's address is "<user>[_<agent>]:<device>"
// (types.JID.SignalAddress; libsignal protocol/SignalProtocolAddress.go:7 ADDRESS_SEPARATOR; the agent 1 for a LID, none for a phone
// number).
func keptMapping(ctx context.Context, db *dbutil.Database, phone, lid string) (bool, error) {
	phoneJID := types.NewJID(phone, types.DefaultUserServer).String()
	var lidJID, lidAddr string
	if lid != "" {
		lidJID = types.NewJID(lid, types.HiddenUserServer).String()
		lidAddr = lid + "_1"
	}
	var kept bool
	err := db.QueryRow(ctx, `SELECT
		EXISTS(SELECT 1 FROM whatsmeow_contacts WHERE their_jid IN ($1, $2))
		OR EXISTS(SELECT 1 FROM whatsmeow_sessions WHERE substr(their_id, 1, instr(their_id, ':') - 1) IN ($3, $4))
		OR EXISTS(SELECT 1 FROM whatsmeow_identity_keys WHERE substr(their_id, 1, instr(their_id, ':') - 1) IN ($3, $4))`,
		phoneJID, nonEmpty(lidJID), phone, nonEmpty(lidAddr)).Scan(&kept)
	return kept, err
}

// nonEmpty is s, or a value no stored JID or address has when s is "".
func nonEmpty(s string) string {
	if s == "" {
		return "\x00"
	}
	return s
}

// unpairPoll is how often unpair looks whether WhatsApp took the login.
const unpairPoll = 50 * time.Millisecond

// unpair logs the refused companion out at WhatsApp, so that it leaves the
// linked devices of the other account's phone, with a whatsmeow client of
// its own on device, the copy dropRefused read before it deleted the rows.
// Every store of that copy is a no-op (detach): whatever the client would
// save while it runs (the device again, a LID, sessions of messages that
// come in, privacy tokens, the LID-phone map) goes nowhere, so the deleted
// rows are not written back.
// It connects (through the connector's proxy setting; the worker sets none,
// newConnector), waits until WhatsApp has taken the login (IsLoggedIn, set
// on the connect success, connectionevents.go:166, before whatsmeow's own
// pre-key upload and passive IQ, which it does not wait for), and calls
// Client.Logout (client.go:761-806: the remove-companion-device IQ, then a
// disconnect and Device.Delete, a no-op here). The connector's own logout
// (WhatsAppClient.LogoutRemote, connector/client.go:430-443) is the same
// call, on a client the refused login never got.
//
// nil (refused_session_ended) when WhatsApp answered the logout, or when it
// refused the login as no longer paired (a LoggedOut event, whose reasons
// all mean the companion is gone: ConnectFailureReason.IsLoggedOut,
// types/events/events.go:229-232); an error (refused_session_not_ended)
// when it could not connect (no network: the companion then stays linked
// on that phone until it is removed there, or WhatsApp drops it), did not
// take the login, did not answer, or ctx ended first. The keys of the copy
// are zeroed when it returns (not the copies whatsmeow made, which Go gives
// no way to reach).
func unpair(ctx context.Context, device *store.Device, proxy string) error {
	detach(device)
	cli := whatsmeow.NewClient(device, waLog.Noop)
	cli.EnableAutoReconnect = false
	cli.InitialAutoReconnect = false
	cli.DisableLoginAutoReconnect = true
	cli.ManualHistorySyncDownload = true
	cli.AutomaticMessageRerequestFromPhone = false
	defer forgetKeys(device)
	if proxy != "" {
		if err := cli.SetProxyAddress(proxy); err != nil {
			return err
		}
	}
	outcome := make(chan error, 1)
	cli.AddEventHandler(func(evt any) {
		var res error
		switch evt.(type) {
		case *events.LoggedOut:
			// Already unpaired: what the logout would do is done.
		case *events.ConnectFailure, *events.TemporaryBan, *events.ClientOutdated,
			*events.StreamReplaced, *events.StreamError, *events.Disconnected:
			res = errNotLoggedIn
		default:
			return
		}
		select {
		case outcome <- res:
		default:
		}
	})
	if err := cli.ConnectContext(ctx); err != nil {
		return err
	}
	defer cli.Disconnect()
	tick := time.NewTicker(unpairPoll)
	defer tick.Stop()
	for !cli.IsLoggedIn() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-outcome:
			return err
		case <-tick.C:
		}
	}
	return cli.Logout(ctx)
}

// detach takes the in-memory copy of a refused companion off every store:
// each is store.NoopStore, which reads nothing and writes nowhere, and the
// pre-key store is one that fails every call.
//
// The pre-key store must fail, not answer empty. After a connect success
// whatsmeow's own goroutine (connectionevents.go:187-199) asks the store
// how many pre-keys were uploaded; with a plain NoopStore that is 0 with no
// error, so it uploads the keys GetOrGenPreKeys gives (none) and, when
// WhatsApp accepts the empty list, indexes the last of them
// (prekeys.go:89, preKeys[len(preKeys)-1]): a panic in a goroutine of the
// worker. With an error the goroutine logs and skips the upload
// (connectionevents.go:188-189), and a later upload asked by the server
// stops at GetOrGenPreKeys (prekeys.go:66-70). The companion is about to
// be removed: it needs no pre-keys.
func detach(device *store.Device) {
	noop := &store.NoopStore{}
	device.SetAllStores(noop)
	device.PreKeys = &store.NoopStore{Error: errNoPreKeys}
	device.LIDs = noop
	device.Container = noop
	device.Log = waLog.Noop
}

// forgetKeys zeroes the private keys of the in-memory copy of a refused
// companion once its client is done.
func forgetKeys(d *store.Device) {
	clear(d.AdvSecretKey)
	if d.NoiseKey != nil && d.NoiseKey.Priv != nil {
		clear(d.NoiseKey.Priv[:])
	}
	if d.IdentityKey != nil && d.IdentityKey.Priv != nil {
		clear(d.IdentityKey.Priv[:])
	}
	if d.SignedPreKey != nil && d.SignedPreKey.Priv != nil {
		clear(d.SignedPreKey.Priv[:])
	}
}

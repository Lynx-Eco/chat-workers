// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-signal/pkg/connector"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/web"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/wspb"
	"go.mau.fi/util/dbutil"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	lynxstore "lynx.invalid/ingest/chatworker/store"
)

var (
	errNotSignal   = errors.New("not a Signal connector and login")
	errNotUnlinked = errors.New("Signal did not unlink the refused device")
)

// dropRefused is the worker's backend.Hooks.DropRefused: it undoes a
// sign-in the gate refused because it reached another account than the
// connection's.
//
// Why a drop, and not only an end: signalmeow stores the linked device
// during provisioning (finishProvisioning, signalmeow provisioning.go: the
// signalmeow_device row with the account's ACI and PNI identity keys, its
// password and backup keys, then its own identity keys, signed and Kyber
// pre-keys and its own recipient row with the profile key), before the
// connector calls the framework's NewLogin (QRLogin.loginComplete,
// connector login.go:220-232), where the gate refuses the login. The
// connector itself has stored nothing of the login yet.
//
// What it does, here, synchronously: reads the refused device's number and
// password (deviceCreds; nothing else of it, no key), and deletes its
// signalmeow_device row in one transaction (dropAccount). Every other
// signalmeow table that holds an account's rows (pre-keys, Kyber pre-keys,
// identity keys, sessions, sender keys, outbound sender key info, groups,
// recipients, profile keys, the event buffer, the backup's recipients,
// chats and messages) refers to that row by its account_id with ON DELETE
// CASCADE (store/upgrades/00-latest.sql), and every connection of the
// store has foreign_keys=ON, so they go with it. The one signalmeow table
// with no account, signalmeow_unregistered_users, is written only by a
// connected client, which the refused login never had.
//
// Nothing of the connection's own account is deleted: only the row of the
// refused login's ACI, which is not the connection's account (the gate
// refuses only a login whose id differs from it). The connection's own
// rows that name the other account (as a contact, say) are its own and
// stay.
//
// It returns the end at Signal (unlink), run by the backend in the
// background: the device removes itself from the other account's linked
// devices, as the connector's own logout does (LogoutRemote, Client.Unlink:
// DELETE /v1/devices/<its id> on its authenticated socket). No device
// stored (nothing was linked) is no end.
func dropRefused(ctx context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) (func(context.Context) error, error) {
	sc, isSignal := inner.(*connector.SignalConnector)
	if !isSignal || sc.Bridge == nil || sc.Bridge.DB == nil || login == nil || login.UserLogin == nil {
		return nil, errNotSignal
	}
	aci, err := signalid.ParseUserLoginID(login.ID)
	if err != nil || aci == uuid.Nil {
		return nil, errNotSignal
	}
	db := sc.Bridge.DB.Database
	creds, err := dropAccount(ctx, db, aci)
	if creds == nil {
		return nil, err
	}
	return durableUnlink(db, *creds), err
}

// deviceCreds are what a linked device signs in to Signal's server with
// (DeviceData.BasicAuthCreds: "<ACI>.<device id>" and its password).
type deviceCreds struct {
	aci      uuid.UUID
	deviceID int
	password string
}

func (c deviceCreds) username() string { return fmt.Sprintf("%s.%d", c.aci, c.deviceID) }

// dropAccount deletes the signalmeow_device row of aci, and with it (ON
// DELETE CASCADE) every signalmeow row of that account, in one
// transaction, and returns the device's credentials as they were (nil when
// no device of aci was stored, or it has no password left to sign in
// with). The transaction is run again while another writer holds the file
// (lynxstore.RetryBusy), within the caller's time: the connection's own
// client writes to the same file, and a drop that gave up there would end
// the connection as other_account_kept over a lock that passes.
//
// The credentials are returned also when the delete then failed (with its
// error), once they were read: the device is still linked to the other
// account, and its unlink at Signal (dropRefused's end) must not be lost
// because the local delete was (the backend runs the end, and ends the
// connection other_account_kept for the rows left). Only when they could
// not be read is nil returned with the error (logged
// refused_session_not_ended).
//
// The same transaction records the device's unlink as pending
// (recordPendingUnlink): once the delete is committed, the credentials
// survive in the pending record until the unlink is proven, so a worker
// that stops before its end ran (a crash, a kill) unlinks the device at its
// next start (retryPendingUnlinks).
func dropAccount(ctx context.Context, db *dbutil.Database, aci uuid.UUID) (*deviceCreds, error) {
	var creds *deviceCreds
	err := lynxstore.RetryBusy(ctx, func() error {
		return db.DoTxn(ctx, nil, func(ctx context.Context) error {
			var c deviceCreds
			switch err := db.QueryRow(ctx, `SELECT device_id, password FROM signalmeow_device WHERE aci_uuid=$1`, aci.String()).Scan(&c.deviceID, &c.password); {
			case errors.Is(err, sql.ErrNoRows):
				creds = nil
				return nil
			case err != nil:
				return err
			}
			if c.deviceID > 0 && c.password != "" {
				c.aci = aci
				creds = &c
				if err := recordPendingUnlink(ctx, db, c); err != nil {
					return err
				}
			} else {
				creds = nil
			}
			_, err := db.Exec(ctx, `DELETE FROM signalmeow_device WHERE aci_uuid=$1`, aci.String())
			return err
		})
	})
	return creds, err
}

// dropOrphanDevices is the worker's backend.Hooks.PrepareLoginsEnds: at each
// start, before any login connects, it deletes every signalmeow device (and
// what cascades from it) whose ACI is not the id of a login stored in the
// framework's user_login table.
//
// Such a device is one a sign-in stored that never became a login: the
// phone confirmed the link while the sign-in was being cancelled, or while
// the connector was replacing the QR code (Wait cancels the provisioning
// context, login.go:203-210, and a device stored just before that is sent
// on a channel nobody reads any more, Cancel :63-69), or the worker stopped
// in between; or one the gate could not judge (its read of the store
// failed, gate.go: the sign-in failed as store_error and nothing was
// dropped). The connector never uses it: a login loads only the device of
// its own ACI (LoadUserLogin, connector.go:113-140). It may be another
// account's than the connection's, so it is not kept.
//
// It is also unlinked at Signal: the sweep returns, for each device it
// deleted that had credentials, the unlink dropRefused's end makes (read in
// the same transaction as the delete, and recorded there as pending,
// recordPendingUnlink), which the backend runs in the background after the
// sweep (Hooks.PrepareLoginsEnds: within 30 seconds, logged
// refused_session_ended or refused_session_not_ended). No device of a
// sign-in that never became a login is thus left linked unseen. The hook
// itself makes no network call.
//
// Before the sweep, it takes up every unlink still pending from an earlier
// run (retryPendingUnlinks): one whose end never ran (the worker stopped
// between the delete and the unlink) or did not prove the device gone. Each
// is tried again, in the background like the sweep's, at most
// pendingUnlinkMaxAttempts times in all and for pendingUnlinkMaxAge after
// its delete; past either bound it is dropped and logged
// refused_session_not_ended, and the device stays among that phone's linked
// devices until it is removed there.
//
// What is kept is read from the stored user_login rows, never from the
// logins the framework loaded (the hook's logins argument): bridgev2
// v0.33.0 leaves out, with no error, a stored login whose LoadUserLogin
// failed (userlogin.go:67-75; the connector's fails on a store read,
// connector.go:113-123, and so does the backend's gate), so a transient read
// error at start would otherwise delete the connection's own device and its
// keys for good. The rows are read in the same transaction as the devices
// are deleted (a login written in between makes the transaction fail as
// busy and run again). When they cannot be read, nothing is deleted and the
// error is returned: the backend logs prepare_logins_failed and starts
// as usual; the sweep runs again at the next start.
//
// Nothing else can be deleted here that a live connection needs: the hook
// runs before the backend serves any command (run.go: start, then serve),
// so no sign-in of this worker is in progress; a framework restarted after
// a logout's wipe does not run it; and the store is the connection's own
// file (one state directory a connection), which no other connection's
// device is in.
//
// One case deletes and unlinks the connection's own device, and that is
// intended: a worker that stopped (a crash, a kill) in the middle of a
// logout, after the framework deleted the connection's user_login row
// (bridgev2 UserLogin.Delete, after the connector's LogoutRemote) and
// before the wipe emptied the store (backend resetAll), leaves the
// connection's own device with no login when the connector's own delete of
// it failed or never ran. The next start's sweep deletes it and unlinks it
// at Signal: a logout was asked for, and the unlink is that logout's own
// end at Signal (LogoutRemote's Client.Unlink, which may have failed or
// never run); its 401 proves it when the logout's unlink had already gone
// through. The host does not take the connection for signed in after a
// logout it asked for, so nothing is lost that it still wants.
func dropOrphanDevices(ctx context.Context, inner bridgev2.NetworkConnector, br *bridgev2.Bridge, _ []*bridgev2.UserLogin) ([]func(context.Context) error, error) {
	if _, isSignal := inner.(*connector.SignalConnector); !isSignal || br == nil || br.DB == nil {
		return nil, errNotSignal
	}
	db := br.DB.Database
	// The ends of an earlier run first: they are returned (and run) also
	// when the sweep then fails.
	ends, retryErr := retryPendingUnlinks(ctx, db)
	dropped, err := dropDevicesWithoutLogin(ctx, db)
	for _, c := range dropped {
		ends = append(ends, durableUnlink(db, c))
	}
	return ends, errors.Join(retryErr, err)
}

// errLoginsUnread is dropDevicesWithoutLogin's error when the stored logins
// could not be read: nothing was deleted.
var errLoginsUnread = errors.New("the stored logins could not be read: no device was deleted")

// dropDevicesWithoutLogin deletes, in one transaction (again while another
// writer holds the file), every signalmeow_device row whose aci_uuid is not
// the id of any user_login row (of any bridge or user: the most that could
// be kept), and returns the credentials of each it deleted that has some
// (a device id and a password), read in that transaction and recorded there
// as a pending unlink (recordPendingUnlink). When the user_login rows
// cannot be read, it deletes nothing; when the transaction fails, nothing
// is deleted, recorded or returned.
func dropDevicesWithoutLogin(ctx context.Context, db *dbutil.Database) ([]deviceCreds, error) {
	var dropped []deviceCreds
	err := lynxstore.RetryBusy(ctx, func() error {
		dropped = nil
		return db.DoTxn(ctx, nil, func(ctx context.Context) error {
			kept, err := storedLoginIDs(ctx, db)
			if err != nil {
				return fmt.Errorf("%w: %w", errLoginsUnread, err)
			}
			drop, err := devicesWithout(ctx, db, kept)
			if err != nil {
				return err
			}
			for _, d := range drop {
				if _, err := db.Exec(ctx, `DELETE FROM signalmeow_device WHERE aci_uuid=$1`, d.aciUUID); err != nil {
					return err
				}
				if c, ok := d.creds(); ok {
					if err := recordPendingUnlink(ctx, db, c); err != nil {
						return err
					}
					dropped = append(dropped, c)
				}
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return dropped, nil
}

// storedLoginIDs is the id of every stored user_login row.
func storedLoginIDs(ctx context.Context, db *dbutil.Database) (map[string]bool, error) {
	rows, err := db.Query(ctx, `SELECT id FROM user_login`)
	if err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids[id] = true
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return ids, rows.Err()
}

// storedDevice is a signalmeow_device row as the sweep reads it.
type storedDevice struct {
	aciUUID  string
	deviceID int
	password string
}

// creds are the device's credentials to sign in with, when it has some (a
// device id, a password and an ACI that is a UUID).
func (d storedDevice) creds() (deviceCreds, bool) {
	aci, err := uuid.Parse(d.aciUUID)
	if err != nil || aci == uuid.Nil || d.deviceID <= 0 || d.password == "" {
		return deviceCreds{}, false
	}
	return deviceCreds{aci: aci, deviceID: d.deviceID, password: d.password}, true
}

// devicesWithout is every signalmeow_device row whose aci_uuid is not in
// kept, with its credentials as stored.
func devicesWithout(ctx context.Context, db *dbutil.Database, kept map[string]bool) ([]storedDevice, error) {
	rows, err := db.Query(ctx, `SELECT aci_uuid, device_id, password FROM signalmeow_device`)
	if err != nil {
		return nil, err
	}
	var drop []storedDevice
	for rows.Next() {
		var d storedDevice
		if err := rows.Scan(&d.aciUUID, &d.deviceID, &d.password); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if !kept[d.aciUUID] {
			drop = append(drop, d)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return drop, rows.Err()
}

// A pending unlink is the record of a device deleted from the store whose
// unlink at Signal is not yet proven: its credentials (the ACI, the device
// id and the password, what unlink signs in with, nothing else of the
// device), when it was deleted, and how many times its unlink was started.
// It lives in the worker's own store file, which is encrypted as a whole
// (store.OpenDB, sqlite3mc) as the device's rows were, and is written in the
// transaction that deletes the device (dropAccount, dropDevicesWithoutLogin):
// the delete of a device with credentials is never committed without it.
// It is removed once the unlink is proven (durableUnlink) or given up
// (retryPendingUnlinks).
//
// A logout's wipe (store.Wipe) empties this table with every other: what a
// wipe takes, it takes of every account. That loses only the next try of an
// unlink already started in this run (the retries run before any command
// is served, so before any logout), whose outcome the log gives.
const createPendingUnlink = `CREATE TABLE IF NOT EXISTS lynx_signal_pending_unlink (
	aci_uuid  TEXT    NOT NULL,
	device_id INTEGER NOT NULL,
	password  TEXT    NOT NULL,
	added_at  INTEGER NOT NULL,
	attempts  INTEGER NOT NULL,
	PRIMARY KEY (aci_uuid, device_id)
)`

// The bounds of a pending unlink: it is started at most
// pendingUnlinkMaxAttempts times in all (the first in the run that deleted
// the device, then one at each start), and not after pendingUnlinkMaxAge
// from its delete. Past either it is dropped and logged
// refused_session_not_ended (errUnlinkGivenUp). Variables for the tests.
var (
	pendingUnlinkMaxAttempts = 5
	pendingUnlinkMaxAge      = 7 * 24 * time.Hour
	// pendingNow is the clock of added_at and of the age bound.
	pendingNow = time.Now
)

// pendingWriteLimit bounds the removal of a pending record after its
// unlink, which runs whatever is left of the unlink's own time.
const pendingWriteLimit = 5 * time.Second

// errUnlinkGivenUp is the end of a pending unlink past its bounds: it is
// not tried again, and the backend logs refused_session_not_ended.
var errUnlinkGivenUp = errors.New("the refused device's unlink was given up: past its attempts or its age")

// recordPendingUnlink records c's unlink as pending, inside the caller's
// transaction (ctx carries it), as started once. A record of the same
// device (its ACI and device id) is replaced: Signal may give a new link the
// id of one removed earlier.
func recordPendingUnlink(ctx context.Context, db *dbutil.Database, c deviceCreds) error {
	if _, err := db.Exec(ctx, createPendingUnlink); err != nil {
		return err
	}
	_, err := db.Exec(ctx, `INSERT INTO lynx_signal_pending_unlink (aci_uuid, device_id, password, added_at, attempts)
		VALUES ($1, $2, $3, $4, 1)
		ON CONFLICT (aci_uuid, device_id) DO UPDATE SET password=excluded.password, added_at=excluded.added_at, attempts=1`,
		c.aci.String(), c.deviceID, c.password, pendingNow().UnixMilli())
	return err
}

// forgetPendingUnlink removes c's pending record (only one with c's own
// password: a record replaced in between is another device's).
func forgetPendingUnlink(ctx context.Context, db *dbutil.Database, c deviceCreds) error {
	return lynxstore.RetryBusy(ctx, func() error {
		_, err := db.Exec(ctx, `DELETE FROM lynx_signal_pending_unlink WHERE aci_uuid=$1 AND device_id=$2 AND password=$3`,
			c.aci.String(), c.deviceID, c.password)
		return err
	})
}

// durableUnlink is the end at Signal of a deleted device whose unlink is
// recorded as pending: unlink, and on proof that the device is gone, the
// removal of the record. When the unlink is not proven, or the worker stops
// first, the record stays and the next start tries again
// (retryPendingUnlinks). A failed removal is not the end's error (the
// device is gone): the next start's unlink then gets Signal's 401, which
// proves it again, and removes the record.
func durableUnlink(db *dbutil.Database, c deviceCreds) func(context.Context) error {
	return func(ctx context.Context) error {
		if err := unlink(ctx, c); err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pendingWriteLimit)
		defer cancel()
		_ = forgetPendingUnlink(wctx, db, c)
		return nil
	}
}

// retryPendingUnlinks takes up, in one transaction (again while another
// writer holds the file), every pending unlink of an earlier run. One past
// its bounds (pendingUnlinkMaxAttempts started, or older than
// pendingUnlinkMaxAge) is removed, and its end is errUnlinkGivenUp at once
// (no network call: the backend logs refused_session_not_ended). Each other
// is counted as started once more and its end is durableUnlink's. A record
// with no usable credentials is removed with no end. When the transaction
// fails nothing is changed and no end is returned: the records stay for the
// next start.
func retryPendingUnlinks(ctx context.Context, db *dbutil.Database) ([]func(context.Context) error, error) {
	var ends []func(context.Context) error
	err := lynxstore.RetryBusy(ctx, func() error {
		ends = nil
		return db.DoTxn(ctx, nil, func(ctx context.Context) error {
			if _, err := db.Exec(ctx, createPendingUnlink); err != nil {
				return err
			}
			pending, err := pendingUnlinks(ctx, db)
			if err != nil {
				return err
			}
			now := pendingNow()
			for _, p := range pending {
				c, usable := p.creds()
				if !usable || p.attempts >= pendingUnlinkMaxAttempts || now.Sub(time.UnixMilli(p.addedAt)) > pendingUnlinkMaxAge {
					if _, err := db.Exec(ctx, `DELETE FROM lynx_signal_pending_unlink WHERE aci_uuid=$1 AND device_id=$2`, p.aciUUID, p.deviceID); err != nil {
						return err
					}
					if usable {
						ends = append(ends, func(context.Context) error { return errUnlinkGivenUp })
					}
					continue
				}
				if _, err := db.Exec(ctx, `UPDATE lynx_signal_pending_unlink SET attempts=attempts+1 WHERE aci_uuid=$1 AND device_id=$2`, p.aciUUID, p.deviceID); err != nil {
					return err
				}
				ends = append(ends, durableUnlink(db, c))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return ends, nil
}

// pendingUnlink is a lynx_signal_pending_unlink row.
type pendingUnlink struct {
	storedDevice
	addedAt  int64
	attempts int
}

// pendingUnlinks is every pending unlink record.
func pendingUnlinks(ctx context.Context, db *dbutil.Database) ([]pendingUnlink, error) {
	rows, err := db.Query(ctx, `SELECT aci_uuid, device_id, password, added_at, attempts FROM lynx_signal_pending_unlink`)
	if err != nil {
		return nil, err
	}
	var out []pendingUnlink
	for rows.Next() {
		var p pendingUnlink
		if err := rows.Scan(&p.aciUUID, &p.deviceID, &p.password, &p.addedAt, &p.attempts); err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return out, rows.Err()
}

// unlink removes the refused device from the other account's linked
// devices: it opens the device's authenticated socket to Signal's server
// (/v1/websocket/, with the device's "<ACI>.<device id>" and password, as
// signalmeow's connectAuthedWS does, client.go) and asks DELETE
// /v1/devices/<its id> there (Client.Unlink, provisioning.go), the
// connector's own logout.
//
// It speaks the socket itself, one request and its answer, as signalmeow's
// confirmDevice does (provisioning.go: web.OpenWebsocket,
// web.CreateWSRequest, wspb), not through signalmeow's SignalWebsocket:
// that one's connect loop clears a variable its request goroutine still
// reads when it stops (web/signalwebsocket.go:212 against :233), a data
// race the race detector reports. What Signal delivers to the device
// meanwhile is neither read nor acknowledged (it goes with the device). It
// logs nowhere and stores nothing: no client is made.
//
// Only what proves the device gone is taken for an end (nil):
//   - a 2xx answer to the delete;
//   - 401 when the socket opens: Signal's server authenticates the socket
//     by the device's own name and password and answers 401 when it knows
//     no such device with that password (signalmeow takes it, with basic
//     auth, for "logged out", web/signalwebsocket.go:312-315): the device
//     was deleted, or its account.
//
// Not proof:
//   - no answer: the socket closed while the delete waited. Signal may
//     close the socket of a device it just deleted (signalmeow's
//     sendRequestInternal takes that for success, isSelfDelete), but the
//     connection may as well have dropped first. The socket is opened again
//     (after reopenPause): its 401 then proves the end, and a socket that
//     opens means the device is still there, so the delete is asked again.
//     The same for a socket that could not be opened (no network, 5xx,
//     429);
//   - 403 when the socket opens: signalmeow takes it for "logged out" too,
//     but nothing in its source says what Signal means by it there, so it
//     is not taken for proof;
//   - any other answer to the delete or at the opening, or ctx ending
//     first.
//
// Those are errNotUnlinked: the backend logs refused_session_not_ended, as
// chat_whatsapp's unpair does when WhatsApp did not take the logout. The
// device's credentials went with its rows (dropRefused, or the start
// sweep), but its pending record keeps them (durableUnlink): the next start
// tries again, within the bounds of retryPendingUnlinks, after which the
// device stays among that phone's linked devices until it is removed there,
// or Signal drops it. (When dropRefused's delete failed, the rows stay and
// no record was written; the backend ends the connection
// other_account_kept, and the host deletes the store.)
func unlink(ctx context.Context, creds deviceCreds) error {
	ctx = zerolog.Nop().WithContext(ctx)
	for first := true; ; first = false {
		if !first {
			select {
			case <-time.After(reopenPause):
			case <-ctx.Done():
				return errNotUnlinked
			}
		}
		if done, err := deleteSelf(ctx, creds); done {
			return err
		}
		if ctx.Err() != nil {
			return errNotUnlinked
		}
	}
}

// reopenPause is how long unlink waits before it opens the socket again.
var reopenPause = 2 * time.Second

// deleteSelf opens the device's socket once and asks Signal to delete the
// device there. done is false when there is no verdict yet (the socket could
// not be opened for a reason that may pass, or closed before the answer):
// unlink tries again. When done, err is nil for proof that the device is
// gone, else errNotUnlinked.
func deleteSelf(ctx context.Context, creds deviceCreds) (done bool, err error) {
	username := creds.username()
	ws, resp, err := web.OpenWebsocket(ctx, (&url.URL{
		Scheme: "wss",
		Host:   web.APIHostname,
		Path:   web.WebsocketPath,
		User:   url.UserPassword(username, creds.password),
	}).String())
	if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
		web.CloseBody(resp)
		switch code := resp.StatusCode; {
		case code == http.StatusUnauthorized:
			return true, nil
		case code >= 500, code == http.StatusTooManyRequests:
			return false, nil
		}
		return true, errNotUnlinked
	}
	if err != nil {
		return false, nil
	}
	defer ws.CloseNow()
	const requestID = 1
	req := web.CreateWSRequest(http.MethodDelete, fmt.Sprintf("/v1/devices/%d", creds.deviceID), nil, &username, &creds.password)
	req.Id = proto.Uint64(requestID)
	typ := signalpb.WebSocketMessage_REQUEST
	if err := wspb.Write(ctx, ws, &signalpb.WebSocketMessage{Type: &typ, Request: req}); err != nil {
		return false, nil
	}
	for {
		msg := &signalpb.WebSocketMessage{}
		if err := wspb.Read(ctx, ws, msg); err != nil {
			return false, nil
		}
		if msg.GetType() != signalpb.WebSocketMessage_RESPONSE || msg.GetResponse().GetId() != requestID {
			// What Signal sends the device meanwhile: not ours to read.
			continue
		}
		if st := msg.GetResponse().GetStatus(); st >= 200 && st < 300 {
			return true, nil
		}
		return true, errNotUnlinked
	}
}

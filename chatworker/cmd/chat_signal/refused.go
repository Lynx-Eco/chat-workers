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
	creds, err := dropAccount(ctx, sc.Bridge.DB.Database, aci)
	if creds == nil {
		return nil, err
	}
	return func(ctx context.Context) error { return unlink(ctx, *creds) }, err
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
func dropAccount(ctx context.Context, db *dbutil.Database, aci uuid.UUID) (*deviceCreds, error) {
	var creds *deviceCreds
	err := lynxstore.RetryBusy(ctx, func() error {
		creds = nil
		return db.DoTxn(ctx, nil, func(ctx context.Context) error {
			var c deviceCreds
			switch err := db.QueryRow(ctx, `SELECT device_id, password FROM signalmeow_device WHERE aci_uuid=$1`, aci.String()).Scan(&c.deviceID, &c.password); {
			case errors.Is(err, sql.ErrNoRows):
				return nil
			case err != nil:
				return err
			}
			if _, err := db.Exec(ctx, `DELETE FROM signalmeow_device WHERE aci_uuid=$1`, aci.String()); err != nil {
				return err
			}
			if c.deviceID > 0 && c.password != "" {
				c.aci = aci
				creds = &c
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return creds, nil
}

// dropOrphanDevices is the worker's backend.Hooks.PrepareLogins: at each
// start, before any login connects, it deletes every signalmeow device (and
// what cascades from it) whose ACI is not the id of a login stored in the
// framework's user_login table.
//
// Such a device is one a sign-in stored that never became a login: the
// phone confirmed the link while the sign-in was being cancelled, or while
// the connector was replacing the QR code (Wait cancels the provisioning
// context, login.go:203-210, and a device stored just before that is sent
// on a channel nobody reads any more, Cancel :63-69), or the worker stopped
// in between. The connector never uses it: a login loads only the device
// of its own ACI (LoadUserLogin, connector.go:113-140). It may be another
// account's than the connection's, so it is not kept. It stays among that
// phone's linked devices until it is removed there (no network call here).
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
func dropOrphanDevices(ctx context.Context, inner bridgev2.NetworkConnector, br *bridgev2.Bridge, _ []*bridgev2.UserLogin) error {
	if _, isSignal := inner.(*connector.SignalConnector); !isSignal || br == nil || br.DB == nil {
		return errNotSignal
	}
	return dropDevicesWithoutLogin(ctx, br.DB.Database)
}

// errLoginsUnread is dropDevicesWithoutLogin's error when the stored logins
// could not be read: nothing was deleted.
var errLoginsUnread = errors.New("the stored logins could not be read: no device was deleted")

// dropDevicesWithoutLogin deletes, in one transaction (again while another
// writer holds the file), every signalmeow_device row whose aci_uuid is not
// the id of any user_login row (of any bridge or user: the most that could
// be kept). When the user_login rows cannot be read, it deletes nothing.
func dropDevicesWithoutLogin(ctx context.Context, db *dbutil.Database) error {
	return lynxstore.RetryBusy(ctx, func() error {
		return db.DoTxn(ctx, nil, func(ctx context.Context) error {
			kept, err := storedLoginIDs(ctx, db)
			if err != nil {
				return fmt.Errorf("%w: %w", errLoginsUnread, err)
			}
			drop, err := devicesWithout(ctx, db, kept)
			if err != nil {
				return err
			}
			for _, aci := range drop {
				if _, err := db.Exec(ctx, `DELETE FROM signalmeow_device WHERE aci_uuid=$1`, aci); err != nil {
					return err
				}
			}
			return nil
		})
	})
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

// devicesWithout is the aci_uuid of every signalmeow_device row not in kept.
func devicesWithout(ctx context.Context, db *dbutil.Database, kept map[string]bool) ([]string, error) {
	rows, err := db.Query(ctx, `SELECT aci_uuid FROM signalmeow_device`)
	if err != nil {
		return nil, err
	}
	var drop []string
	for rows.Next() {
		var aci string
		if err := rows.Scan(&aci); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if !kept[aci] {
			drop = append(drop, aci)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return drop, rows.Err()
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
// device's credentials were deleted with its rows (dropRefused), so nothing
// tries again later: the device stays among that phone's linked devices
// until it is removed there, or Signal drops it.
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

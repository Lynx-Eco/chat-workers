// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	lynxstore "lynx.invalid/ingest/chatworker/store"
)

var errNotGVoice = errors.New("not the worker's Google Voice connector and login")

// dropRefused is the worker's backend.Hooks.DropRefused, which the gate
// calls synchronously when it refuses a new login of another account than
// the connection's (another number: the login id is "<owner MXID>|<the
// account's primary Voice number>", login.go:139).
//
// Why a drop: the connector stores one row of the login before the
// framework's NewLogin, where the gate refuses it. SubmitCookies
// (login.go:116-171) asks Google for the account (GetAccount, no store),
// then takes the login's prefix, gvdb.GetLoginPrefix (login.go:140): an
// INSERT ... ON CONFLICT into gvoice_login_prefix keyed by the login id,
// which holds the other account's number, and only then calls NewLogin
// (:145). Nothing else: the connector's LoadUserLogin never runs for a
// refused login (the gate refuses before it), and its realtime connection
// is started only after NewLogin returned a login (:161). So that row, by
// its login id, is deleted here (dropPrefix). Nothing of the connection's
// own login is touched: its row has another login id, and its prefix (the
// one its person ids carry) stays.
//
// No end: there is no session of Google's to end that is the sign-in's
// own. The cookies are a Google web session (the one the user signed in
// to in the sign-in's web view, or a browser's whose cookies were copied);
// the connector has no logout at all (LogoutRemote only disconnects,
// client.go:283-286), and ending that session at Google would sign that
// browser out of every Google service. The backend logs
// refused_session_nothing_to_end.
//
// An error (not this worker's connector, no database, or the delete
// failed) means the row may still be there: the backend fails the sign-in
// as other_account_kept and the connection ENDS so (gate.go dropRefused).
func dropRefused(ctx context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) (func(context.Context) error, error) {
	n, ok := inner.(*gvNetwork)
	if !ok || n == nil || n.GVConnector == nil || n.DB == nil || login == nil || login.UserLogin == nil || login.ID == "" {
		return nil, errNotGVoice
	}
	return nil, dropPrefix(ctx, n.DB.Database, login.ID)
}

// dropPrefix deletes the prefix row of loginID, again while another writer
// holds the file (lynxstore.RetryBusy: the connection's own client writes
// to the same file), within ctx. One statement: it goes or it does not.
func dropPrefix(ctx context.Context, db *dbutil.Database, loginID networkid.UserLoginID) error {
	return lynxstore.RetryBusy(ctx, func() error {
		_, err := db.Exec(ctx, `DELETE FROM gvoice_login_prefix WHERE login_id=$1`, string(loginID))
		return err
	})
}

// dropOrphanPrefixes is the worker's backend.Hooks.PrepareLogins: at each
// start, before any login connects and before any command is served, every
// prefix row that belongs to no stored login goes (a sign-in cut off by a
// stop between the connector's prefix and the framework's NewLogin, or one
// whose sweep after a failed submission failed: sweptLogin). The keep-set
// is the stored user_login rows, read in the same statement, never the
// logins the framework loaded (bridgev2 leaves out, with no error, a login
// whose load failed, and that login's prefix must stay: its person ids
// carry it). When the stored logins cannot be read the statement fails and
// nothing is deleted (the backend logs prepare_logins_failed).
func dropOrphanPrefixes(ctx context.Context, inner bridgev2.NetworkConnector, _ *bridgev2.Bridge, _ []*bridgev2.UserLogin) error {
	n, ok := inner.(*gvNetwork)
	if !ok || n == nil || n.GVConnector == nil || n.DB == nil {
		return errNotGVoice
	}
	return dropPrefixesWithoutLogin(ctx, n.DB.Database)
}

// dropPrefixesWithoutLogin deletes, in one statement (again while another
// writer holds the file), every gvoice_login_prefix row whose login id is
// not the id of any user_login row (of any bridge or user: the most that
// could be kept).
func dropPrefixesWithoutLogin(ctx context.Context, db *dbutil.Database) error {
	return lynxstore.RetryBusy(ctx, func() error {
		_, err := db.Exec(ctx, `DELETE FROM gvoice_login_prefix WHERE login_id NOT IN (SELECT id FROM user_login)`)
		return err
	})
}

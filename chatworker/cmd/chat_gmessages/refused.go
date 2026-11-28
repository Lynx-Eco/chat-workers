// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
)

// endRefused is the worker's backend.Hooks.EndRefused: it undoes at Google
// the pairing of a sign-in the gate refused because it reached another
// phone, or another Google account, than the connection's (another login
// id: the phone's source id for a QR pairing, "<address>/<phone>" for a
// Google account pairing, finishLogin in login.go). The backend runs it
// once, in the background, within 30 seconds (refusedEndLimit).
//
// Why EndRefused and not DropRefused: the connector stores nothing of a
// login before the framework's NewLogin, where the gate refuses it. Both
// flows pair in memory (libgm keeps the new session in the login process's
// AuthData; the pairing callbacks and the pre-pairing event handler store
// nothing), and finishLogin's only write before NewLogin, the
// gmessages_login_prefix row, is not made with DeterministicIDPrefix
// (newConnector). NewLogin itself writes nothing before LoadUserLogin, the
// gate. So nothing of the other account is in the store to delete, as for
// chat_slack, and the refusal is the gate's own: the connector returns
// NewLogin's error from Wait (finishLogin), and the sign-in fails as
// principal_mismatch.
//
// What it ends: the pairing the refused sign-in just made, which the other
// phone now lists under Device pairing (and which, for a Google account
// pairing, Google keeps for that account). It unpairs it with a libgm
// client of its own on the refused login's session (the copy the login
// process made, never stored), choosing the request by the kind of
// session itself (unpairWith): RevokeRelayPairing for a QR pairing (pair.go
// UnpairBugle, an HTTP request with the pairing's token and browser),
// UNPAIR_GAIA_PAIRING for a Google account pairing (pair_google.go
// UnpairGaia, a message to the phone through Google with the account's
// cookies and the pairing's id). Not libgm's Client.Unpair, which the
// connector's own logout calls (GMClient.LogoutRemote): Unpair picks
// UnpairGaia whenever AuthData.HasCookies is true, and HasCookies is true
// for every session that is not a Google account's (client.go), so a QR
// pairing would get UNPAIR_GAIA_PAIRING with the zero pairing id and never
// its RevokeRelayPairing. The client is never connected (no long poll) and
// writes nothing: libgm has no store.
//
// nil (refused_session_ended) when Google took the request;
// backend.ErrNothingToEnd (refused_session_nothing_to_end) when the login
// has no pairing to undo (no session, or one without a pairing token or
// pairing id); any other error (refused_session_not_ended) when the
// request failed or ctx ended first (the pairing then stays on that phone
// until the user removes it there).
func endRefused(ctx context.Context, _ bridgev2.NetworkConnector, login *bridgev2.UserLogin) error {
	sess := refusedSession(login)
	if sess == nil {
		return backend.ErrNothingToEnd
	}
	return unpairBounded(ctx, login, sess)
}

// logoutUnpair is the worker's backend.Hooks.BeforeLogout: at a logout, right
// before the connector's own logout of the connection's login
// (GMClient.LogoutRemote, client.go), it revokes that login's QR pairing at
// Google with RevokeRelayPairing (UnpairBugle, unpairWith), on the login's
// own session while it is still whole, as endRefused does for a refused one.
//
// Why: the connector's logout calls libgm's Client.Unpair, which sends a QR
// pairing UNPAIR_GAIA_PAIRING with the zero pairing id (see endRefused), so
// without this a QR pairing stays listed on the phone, with a token nobody
// holds once the store is wiped. A Google account pairing is not touched
// here (backend.ErrNothingToEnd): the connector's logout ends it itself
// (UnpairGaia with its pairing id), and it is not ended twice. The
// connector's logout still runs after this, as it is: for a QR pairing its
// Unpair then fails or does nothing.
//
// The login's live client is disconnected first (disconnectLive), as the
// connector's logout would disconnect it next: connected, it would receive
// the phone's answer to the revoke (RevokePairData), whose handler clears
// the session's pairing in a goroutine of its own
// (handlegmessages.go handleGMEvent, invalidateSession) while the
// connector's logout sets the session to nil, a nil dereference that would
// crash the worker before the wipe. Disconnected, no event of the phone's
// reaches it any more.
//
// The backend runs it for the connection's own login only and waits for it
// at most its limit; the result is only logged, and the logout and its wipe
// go on whatever it is. It writes nothing: the session is read, never
// changed (libgm keeps no store).
func logoutUnpair(ctx context.Context, _ bridgev2.NetworkConnector, login *bridgev2.UserLogin) error {
	sess := refusedSession(login)
	if sess == nil || sess.IsGoogleAccount() {
		return backend.ErrNothingToEnd
	}
	disconnectLive(login)
	return unpairBounded(ctx, login, sess)
}

// disconnectLive disconnects login's live client (the connector's
// GMClient.Disconnect: its long poll to Google ends, and with it the events
// of the phone), when it has one. A variable, so that the tests see it run
// before the revoke.
var disconnectLive = func(login *bridgev2.UserLogin) {
	if gc, ok := login.Client.(*connector.GMClient); ok && gc != nil {
		gc.Disconnect()
	}
}

// unpairBounded undoes the pairing of sess, login's session, at Google
// (unpairSession), and returns when that is done or when ctx ends, whichever
// comes first.
func unpairBounded(ctx context.Context, login *bridgev2.UserLogin, sess *libgm.AuthData) error {
	httpSettings := exhttp.SensibleClientSettings
	if login.Bridge != nil {
		httpSettings = login.Bridge.GetHTTPClientSettings()
	}
	// UnpairBugle takes no context (libgm http.go makeProtobufHTTPRequest):
	// the call runs apart, and ctx bounds the wait for it.
	unpair := unpairSession
	done := make(chan error, 1)
	go func() { done <- unpair(ctx, sess, httpSettings) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// refusedSession is the login's session (a refused login's, or at a logout
// the connection's own) when it holds a pairing unpairWith would undo, else
// nil: a Google account pairing needs its
// pairing id, its cookies and the phone (UnpairGaia names the pairing by
// its id and sends to the phone with the cookies); a QR pairing its Tachyon
// token and the browser device (UnpairBugle returns nil, nil without them).
func refusedSession(login *bridgev2.UserLogin) *libgm.AuthData {
	if login == nil || login.UserLogin == nil {
		return nil
	}
	meta, ok := login.Metadata.(*connector.UserLoginMetadata)
	if !ok || meta == nil || meta.Session == nil {
		return nil
	}
	s := meta.Session
	if s.IsGoogleAccount() {
		if s.PairingID == uuid.Nil || !s.HasCookies() || s.Mobile == nil {
			return nil
		}
		return s
	}
	if len(s.TachyonAuthToken) == 0 || s.Browser == nil {
		return nil
	}
	return s
}

// unpairSession undoes the pairing of sess at Google with a libgm client of
// its own that logs nothing (zerolog.Nop), by the request unpairWith
// chooses. A variable, so that the tests run endRefused without Google.
var unpairSession = func(ctx context.Context, sess *libgm.AuthData, httpSettings exhttp.ClientSettings) error {
	cli := libgm.NewClient(sess, nil, zerolog.Nop(), httpSettings)
	defer cli.Disconnect()
	return unpairWith(ctx, sess, cli)
}

// unpairer is the two requests of libgm's Client that undo a pairing.
type unpairer interface {
	// UnpairBugle revokes a QR pairing (RevokeRelayPairing, pair.go).
	UnpairBugle() (*gmproto.RevokeRelayPairingResponse, error)
	// UnpairGaia ends a Google account pairing (UNPAIR_GAIA_PAIRING,
	// pair_google.go).
	UnpairGaia(ctx context.Context) error
}

var _ unpairer = (*libgm.Client)(nil)

// unpairWith sends cli the request that undoes sess's own kind of pairing:
// UnpairGaia for a Google account pairing (AuthData.IsGoogleAccount: it has
// the phone's Google registration id), UnpairBugle for a QR pairing. Never
// libgm's Client.Unpair (see endRefused): it would send a QR pairing's
// end as a Google account pairing's.
func unpairWith(ctx context.Context, sess *libgm.AuthData, cli unpairer) error {
	if sess.IsGoogleAccount() {
		return cli.UnpairGaia(ctx)
	}
	_, err := cli.UnpairBugle()
	return err
}

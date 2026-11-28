// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"strings"
	"time"

	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// gvNetwork is the connector the framework is given: mautrix-gvoice's,
// every method of it as it is (embedded: its optional interfaces, direct
// media and the user id check, come with it), but for two:
//   - LoadUserLogin refuses a login with no number (loginNumber), which
//     the connector makes for a Google account that has a Voice line but
//     no primary number (login.go:123-139: the login id is then
//     "<owner MXID>|"). Such an id names no account: any other Google
//     account in the same state would have the same one and pass the
//     backend's gate as the connection's own. The sign-in fails as the
//     connector's own fi.mau.gvoice.no_number, and its prefix row is
//     deleted.
//   - CreateLogin wraps the login process so that a submission the
//     connector failed leaves no prefix row of its own behind (sweptLogin).
type gvNetwork struct {
	*connector.GVConnector
}

func newGVNetwork(gv *connector.GVConnector) *gvNetwork {
	return &gvNetwork{GVConnector: gv}
}

// loginNumber is the number part of a login id of the connector's,
// "<owner MXID>|<number>" (login.go:139), "" when there is none. The
// owner's MXID (backend.OwnerMXID) has no "|".
func loginNumber(id networkid.UserLoginID) string {
	_, number, ok := strings.Cut(string(id), "|")
	if !ok {
		return ""
	}
	return number
}

// LoadUserLogin is the connector's, for a login with a number. The backend's
// gate runs before it (a login of another account than the connection's is
// refused there, refused.go), so this sees the connection's own logins and
// a first sign-in's.
func (n *gvNetwork) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	if login != nil && login.UserLogin != nil && loginNumber(login.ID) == "" {
		releaseNewLogin(login)
		if n.GVConnector != nil && n.DB != nil {
			// Best effort: the row holds the owner's MXID only. One left
			// is deleted at the next start (dropOrphanPrefixes).
			_ = dropPrefix(ctx, n.DB.Database, login.ID)
		}
		return connector.ErrLoginNoVoiceNumber
	}
	return n.GVConnector.LoadUserLogin(ctx, login)
}

// releaseNewLogin stops the bridge state queue the framework's NewLogin made
// for a new login before it called LoadUserLogin, when LoadUserLogin fails:
// NewLogin returns the error and drops the login without stopping it (as
// the backend's gate does for its own refusal, gate.go releaseNewLogin).
// Only a new login is touched: its Client is not set yet.
func releaseNewLogin(login *bridgev2.UserLogin) {
	if login.Client != nil || login.BridgeState == nil {
		return
	}
	login.BridgeState.Destroy()
	login.BridgeState = nil
}

// CreateLogin is the connector's, wrapped (sweptLogin).
func (n *gvNetwork) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	proc, err := n.GVConnector.CreateLogin(ctx, user, flowID)
	if err != nil {
		return nil, err
	}
	cookies, ok := proc.(bridgev2.LoginProcessCookies)
	if !ok {
		return proc, nil
	}
	return &sweptLogin{net: n, inner: cookies}, nil
}

// sweepLimit bounds the sweep after a failed submission (a delete in the
// store, which waits while another writer holds the file).
const sweepLimit = 30 * time.Second

// sweptLogin is the connector's login process (the one flow, "cookies":
// GVLogin, login.go), whose failed submissions are followed by a sweep of
// the prefix rows that belong to no stored login.
//
// Why: the connector's SubmitCookies stores the login's prefix row (an
// INSERT keyed by the login id, which holds the account's number,
// gvdb.GetLoginPrefix, login.go:140) before it calls the framework's
// NewLogin (:145). A NewLogin that fails before the backend's gate runs
// (its context ended: a cancel or a stop of the worker, userlogin.go
// NewLogin's first check) or for another reason after it leaves that row
// with no login: for another account than the connection's, a row of that
// account's number. The sweep deletes every row whose login id is no
// user_login row's; the backend runs one sign-in at a time and one
// submission of it at a time, so no other submission's row is in flight.
// A submission that succeeded is not swept: its row is its login's.
type sweptLogin struct {
	net   *gvNetwork
	inner bridgev2.LoginProcessCookies
}

var _ bridgev2.LoginProcessCookies = (*sweptLogin)(nil)

func (l *sweptLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return l.inner.Start(ctx)
}

func (l *sweptLogin) Cancel() { l.inner.Cancel() }

// SubmitCookies is the connector's; when it failed, the orphan prefix rows
// are deleted before its answer is returned as it is. A sweep that fails
// is not reported here: the next start's (dropOrphanPrefixes) runs it
// again.
func (l *sweptLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	step, err := l.inner.SubmitCookies(ctx, cookies)
	if err != nil && l.net != nil && l.net.GVConnector != nil && l.net.DB != nil {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sweepLimit)
		_ = dropPrefixesWithoutLogin(sctx, l.net.DB.Database)
		cancel()
	}
	return step, err
}

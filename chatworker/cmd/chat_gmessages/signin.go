// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"maps"
	"strings"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
	"maunium.net/go/mautrix/bridgev2"
)

// signInFlows are the two flows the worker offers, google first, so that a
// host that begins a sign-in with no flow named (the engine takes the first
// one listed) pairs through the Google account, the one flow Google still
// supports:
//   - google (connector.LoginFlowIDGoogle, GoogleLoginProcess): a WEB step
//     fi.mau.gmessages.google_account (the Google account's cookies from a
//     sign-in at accounts.google.com), then a SHOW_AND_WAIT step
//     fi.mau.gmessages.emoji (an emoji the user taps on the phone), then
//     fi.mau.gmessages.complete. On a connection whose stored login was
//     paired this way and still has its cookies, the first step may be the
//     emoji step (StartWithOverride: the connector pairs again with the
//     stored cookies), or the WEB step whose cookies then only re-sign the
//     existing pairing in, with no emoji (doOverrideLogin).
//   - qr (connector.LoginFlowIDQR, QRLoginProcess): one SHOW_AND_WAIT step
//     fi.mau.gmessages.qr, a QR code the phone's Google Messages scans
//     (Device pairing, QR code scanner), replaced every 30 seconds up to six
//     codes (qrExpiresIn), then fi.mau.gmessages.complete.
//
// Google has discontinued QR pairing: mautrix-gmessages v0.2611.0 lists only
// google (login.go LoginFlows: the QR flow is commented out there, and its
// CHANGELOG for v26.05 says "Disabled support for QR login as Google has
// discontinued it"), but its CreateLogin still makes the QR process. The
// worker lists it again, second (gmNetwork), only in case a phone still
// offers it. Named, so that a flow the connector adds later is not offered
// without a change here.
var signInFlows = []string{connector.LoginFlowIDGoogle, connector.LoginFlowIDQR}

// qrFlow is the QR flow as the connector's commented-out entry names it
// (login.go LoginFlows).
var qrFlow = bridgev2.LoginFlow{
	Name:        "QR",
	Description: "Pair by scanning a QR code on your phone",
	ID:          connector.LoginFlowIDQR,
}

// gmNetwork is the connector the framework is given: mautrix-gmessages', every
// method of it as it is (embedded: its optional interfaces come with it), but
// for GetLoginFlows, which lists the QR flow again after the connector's own,
// and CreateLogin, which
// wraps the Google flow's process (googleLogin).
type gmNetwork struct {
	*connector.GMConnector
}

func newGMNetwork(gc *connector.GMConnector) *gmNetwork {
	return &gmNetwork{GMConnector: gc}
}

// GetLoginFlows is the connector's own flows (google), then the QR flow. The
// backend lists the offered flows in this order (offeredFlows), so this
// order, not signInFlows', makes the default.
func (n *gmNetwork) GetLoginFlows() []bridgev2.LoginFlow {
	var flows []bridgev2.LoginFlow
	for _, f := range n.GMConnector.GetLoginFlows() {
		if f.ID != connector.LoginFlowIDQR {
			flows = append(flows, f)
		}
	}
	return append(flows, qrFlow)
}

// CreateLogin is the connector's, with the Google flow's process wrapped.
func (n *gmNetwork) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	proc, err := n.GMConnector.CreateLogin(ctx, user, flowID)
	if err != nil {
		return nil, err
	}
	if gp, ok := proc.(*connector.GoogleLoginProcess); ok {
		return &googleLogin{GoogleLoginProcess: gp}, nil
	}
	return proc, nil
}

// googleLogin is the Google flow's process (embedded: Start,
// StartWithOverride, Wait and Cancel are the connector's), with its cookie
// step guarded so that a sign-in of another Google account does not take
// the connection's own login's cookies with it.
//
// Why: on a connection whose login was paired with a Google account, the
// backend begins every sign-in with that login as the override
// (signInBegin), and the connector then tries the submitted cookies on the
// stored login itself first (doOverrideLogin, login.go): it puts them in the
// login's session (the same AuthData the running client uses), asks Google
// for the account, and when that is another account (or the request
// fails) sets the session's cookies to nil, not back to what they were,
// before it pairs the other account afresh (which the gate then refuses).
// The connection's own login would be left with no cookies: its next
// request to Google fails and, once the session is saved, it is
// gm-not-logged-in-can-reauth (NEEDS_USER) at the next start. The worker
// puts the login's cookies back when the step did not end in that login.
type googleLogin struct {
	*connector.GoogleLoginProcess
}

var (
	_ bridgev2.LoginProcessDisplayAndWait = (*googleLogin)(nil)
	_ bridgev2.LoginProcessCookies        = (*googleLogin)(nil)
	_ bridgev2.LoginProcessWithOverride   = (*googleLogin)(nil)
)

func (g *googleLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	restore := keepOwnCookies(g.Override)
	step, err := g.GoogleLoginProcess.SubmitCookies(ctx, cookies)
	restore(step)
	return step, err
}

// keepOwnCookies copies the cookies of the login being re-signed-in
// (override; nil: none) and returns what puts them back after the
// connector's SubmitCookies returned step: nothing when the step completed
// that same login (its cookies are then the new ones the user gave), or
// when the login had none (a re-sign-in after Google logged it out: there
// is nothing to keep); else the copy, only where the connector left the
// session with no cookies (doOverrideLogin's failure), so that cookies set
// meanwhile by anything else are not replaced.
func keepOwnCookies(override *bridgev2.UserLogin) func(step *bridgev2.LoginStep) {
	none := func(*bridgev2.LoginStep) {}
	if override == nil || override.UserLogin == nil {
		return none
	}
	meta, ok := override.Metadata.(*connector.UserLoginMetadata)
	if !ok || meta == nil || meta.Session == nil {
		return none
	}
	sess := meta.Session
	sess.CookiesLock.RLock()
	saved := maps.Clone(sess.Cookies)
	sess.CookiesLock.RUnlock()
	if saved == nil {
		return none
	}
	id := override.ID
	return func(step *bridgev2.LoginStep) {
		if step != nil && step.Type == bridgev2.LoginStepTypeComplete && step.CompleteParams != nil && step.CompleteParams.UserLoginID == id {
			return
		}
		sess.CookiesLock.Lock()
		if sess.Cookies == nil {
			sess.Cookies = saved
		}
		sess.CookiesLock.Unlock()
	}
}

// qrExpiresIn is the worker's Hooks.ShowExpiresIn: the seconds until the
// worker replaces a shown step (channel.proto SignInShow.expires_in_s; 0:
// it does not).
//
//   - A QR code of the connector's (step fi.mau.gmessages.qr, an address of
//     util.QRCodeURLBase's form carrying the pairing key): 30, the
//     connector's QRExpiryTime. The connector starts each code's clock
//     (PrevStart) right before it returns the code, in Start and in Wait
//     (login.go QRLoginProcess), and the backend reports the step at once,
//     so every code lasts the same and no clock is kept here. Wait then
//     asks Google for a new code (RefreshPhoneRelay) and returns it; after
//     the sixth code (MaxAttempts 6, about three minutes) it fails the
//     sign-in with fi.mau.gmessages.pair_qr_timeout instead.
//   - The Google flow's emoji (step fi.mau.gmessages.emoji): 0. The
//     connector never replaces it; Google ends the attempt when it is not
//     tapped in time (fi.mau.gmessages.pair_timeout, libgm
//     FinishGaiaPairing: REQUEST_OUT_OF_DATE, REQUEST_NOT_RECEIVED_QUICKLY,
//     VERIFICATION_TIMED_OUT), after a time the source does not give.
//   - Anything else (another step, a QR code that is not of that form): 0.
func qrExpiresIn(step *bridgev2.LoginStep) uint32 {
	if step == nil || step.Type != bridgev2.LoginStepTypeDisplayAndWait || step.StepID != connector.LoginStepIDQR {
		return 0
	}
	p := step.DisplayAndWaitParams
	if p == nil || p.Type != bridgev2.LoginDisplayTypeQR || !isPairingURL(p.Data) {
		return 0
	}
	return uint32(connector.QRExpiryTime / time.Second)
}

// isPairingURL tells whether s is a pairing address of libgm's form: the
// base address and the base64 of the pairing key and the session's keys
// (pair.go GenerateQRCodeData). It is shown as the QR code and never
// logged.
func isPairingURL(s string) bool {
	rest, ok := strings.CutPrefix(s, util.QRCodeURLBase)
	return ok && rest != ""
}

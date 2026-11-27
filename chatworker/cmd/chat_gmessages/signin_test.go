// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// pairingURL is a QR code of libgm's form (pair.go GenerateQRCodeData).
const pairingURL = util.QRCodeURLBase + "CiAKHgobChlwYWlyaW5nLWtleS1mb3ItdGVzdHMtb25seQ=="

// qrStep is the step the connector returns for a QR code (login.go
// QRLoginProcess Start and Wait).
func qrStep(code string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeDisplayAndWait,
		StepID: connector.LoginStepIDQR,
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeQR,
			Data: code,
		},
	}
}

// emojiStep is the Google flow's emoji step (login.go submitWithAuthData).
func emojiStep() *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeDisplayAndWait,
		StepID: connector.LoginStepIDEmoji,
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type:     bridgev2.LoginDisplayTypeEmoji,
			Data:     "🦊",
			ImageURL: libgm.GetEmojiSVG("🦊"),
		},
	}
}

// TestQRExpiresIn: every QR code of the connector's is told the connector's
// 30 seconds; the emoji, the cookie step and anything not a QR code of
// libgm's form are told 0. WHY: catches a lifetime left unset (the host
// would show a code the connector replaced as live), one copied wrong (the
// connector's QRExpiryTime is the number), and a lifetime made up for the
// emoji, which the connector never replaces.
func TestQRExpiresIn(t *testing.T) {
	want := uint32(connector.QRExpiryTime / time.Second)
	if want != 30 {
		t.Fatalf("the connector's QR lifetime is %d s now: check qrExpiresIn's doc and the README", want)
	}
	if got := qrExpiresIn(qrStep(pairingURL)); got != want {
		t.Fatalf("QR code: %d, want %d", got, want)
	}
	qrOfCodeKind := qrStep(pairingURL)
	qrOfCodeKind.DisplayAndWaitParams.Type = bridgev2.LoginDisplayTypeCode
	otherStepID := qrStep(pairingURL)
	otherStepID.StepID = connector.LoginStepIDEmoji
	for name, step := range map[string]*bridgev2.LoginStep{
		"emoji":                   emojiStep(),
		"no step":                 nil,
		"no display params":       {Type: bridgev2.LoginStepTypeDisplayAndWait, StepID: connector.LoginStepIDQR},
		"QR step of another kind": qrOfCodeKind,
		"QR of another step":      otherStepID,
		"cookie step":             {Type: bridgev2.LoginStepTypeCookies, StepID: connector.LoginStepIDGoogle},
		"not a pairing address":   qrStep("https://example.invalid/?c=abc"),
		"the base alone":          qrStep(util.QRCodeURLBase),
		"complete":                {Type: bridgev2.LoginStepTypeComplete, StepID: connector.LoginStepIDComplete},
	} {
		if got := qrExpiresIn(step); got != 0 {
			t.Errorf("%s: %d, want 0", name, got)
		}
	}
}

// TestLoginFlowsGoogleFirst: the worker lists the connector's Google flow
// first, as the connector names it, and then the QR flow, and offers
// exactly those two. WHY: catches an order that makes the QR flow, which
// Google has discontinued, the default (the backend lists the flows in this
// order and the engine begins the first), the QR flow missing (the
// connector lists only google: the backend would refuse qr as
// unknown_flow), and a flow the connector adds later offered unseen.
func TestLoginFlowsGoogleFirst(t *testing.T) {
	n := newGMNetwork(&connector.GMConnector{})
	flows := n.GetLoginFlows()
	var ids []string
	for _, f := range flows {
		ids = append(ids, f.ID)
	}
	if !slices.Equal(ids, []string{connector.LoginFlowIDGoogle, connector.LoginFlowIDQR}) {
		t.Fatalf("flows %v", ids)
	}
	if flows[1] != qrFlow || flows[1].Name == "" || flows[1].Description == "" {
		t.Fatalf("QR flow %+v", flows[1])
	}
	var google bridgev2.LoginFlow
	for _, f := range connector.LoginFlows {
		if f.ID == connector.LoginFlowIDGoogle {
			google = f
		}
	}
	if flows[0] != google {
		t.Fatalf("Google flow %+v, the connector's %+v", flows[0], google)
	}
	if !slices.Equal(hooks().SignInFlows, ids) {
		t.Fatalf("hooks offer %v, the network lists %v", hooks().SignInFlows, ids)
	}
}

// TestCreateLoginWrapsGoogle: the Google flow's process is the connector's
// inside googleLogin, with every kind of step the backend submits to it
// (wait, cookies, a start with the stored login as the override); the QR
// flow's is the connector's own; an unknown flow is the connector's error.
// No process reaches the network when made. WHY: catches a wrapper that
// hides one of the process's interfaces (the backend would answer its step
// unsupported_step, or start a re-sign-in without the override), and the
// cookie guard left off.
func TestCreateLoginWrapsGoogle(t *testing.T) {
	gc := &connector.GMConnector{}
	n := newGMNetwork(gc)
	proc, err := n.CreateLogin(context.Background(), nil, connector.LoginFlowIDGoogle)
	if err != nil {
		t.Fatal(err)
	}
	g, ok := proc.(*googleLogin)
	if !ok || g.GoogleLoginProcess == nil || g.Main != gc {
		t.Fatalf("google process %T", proc)
	}
	if _, ok := proc.(bridgev2.LoginProcessDisplayAndWait); !ok {
		t.Error("google: no Wait")
	}
	if _, ok := proc.(bridgev2.LoginProcessCookies); !ok {
		t.Error("google: no SubmitCookies")
	}
	if _, ok := proc.(bridgev2.LoginProcessWithOverride); !ok {
		t.Error("google: no StartWithOverride")
	}
	if _, ok := proc.(bridgev2.LoginProcessWithParams); ok {
		t.Error("google: StartWithParams would take the place of StartWithOverride")
	}
	proc, err = n.CreateLogin(context.Background(), nil, connector.LoginFlowIDQR)
	if err != nil {
		t.Fatal(err)
	}
	q, ok := proc.(*connector.QRLoginProcess)
	if !ok || q.Main != gc || q.MaxAttempts != 6 {
		t.Fatalf("QR process %T %+v", proc, proc)
	}
	if _, err := n.CreateLogin(context.Background(), nil, "phone"); !errors.Is(err, bridgev2.ErrInvalidLoginFlowID) {
		t.Fatalf("unknown flow: %v", err)
	}
}

// ownLogin is a stored Google account login with cookies.
func ownLogin(cookies map[string]string) (*bridgev2.UserLogin, *libgm.AuthData) {
	sess := &libgm.AuthData{Cookies: cookies}
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID:       networkid.UserLoginID("own@example.com/12345"),
		Metadata: &connector.UserLoginMetadata{Session: sess, IDPrefix: "0123456789abcdef"},
	}}
	return login, sess
}

func cookiesOf(s *libgm.AuthData) map[string]string {
	s.CookiesLock.RLock()
	defer s.CookiesLock.RUnlock()
	return maps.Clone(s.Cookies)
}

// TestKeepOwnCookies: when the connector's cookie step leaves the
// connection's own login with no cookies (doOverrideLogin found another
// account, or failed) and did not complete that login, its cookies are put
// back as they were before the step; a step that completed it keeps the
// new ones; cookies something else set meanwhile, and a login that had
// none, are left as they are. WHY: catches a refused re-sign-in with
// another Google account that silently signs the connection's own pairing
// out of Google (NEEDS_USER at its next start), and a guard that undoes a
// successful re-sign-in.
func TestKeepOwnCookies(t *testing.T) {
	own := map[string]string{"SID": "own-sid", "OSID": "own-osid"}

	login, sess := ownLogin(maps.Clone(own))
	restore := keepOwnCookies(login)
	sess.Cookies["SID"] = "tried" // the connector's SetCookies(other), then
	sess.SetCookies(nil)          // SetCookies(nil) on the mismatch
	restore(emojiStep())          // the other account's fresh pairing
	if got := cookiesOf(sess); !maps.Equal(got, own) {
		t.Fatalf("after the other account's emoji step: %v, want %v", got, own)
	}

	login, sess = ownLogin(maps.Clone(own))
	restore = keepOwnCookies(login)
	sess.SetCookies(nil)
	restore(nil) // an error
	if got := cookiesOf(sess); !maps.Equal(got, own) {
		t.Fatalf("after an error: %v", got)
	}

	login, sess = ownLogin(maps.Clone(own))
	restore = keepOwnCookies(login)
	fresh := map[string]string{"SID": "new-sid"}
	sess.SetCookies(fresh)
	restore(&bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: connector.LoginStepIDComplete,
		CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: login.ID, UserLogin: login}})
	if got := cookiesOf(sess); !maps.Equal(got, fresh) {
		t.Fatalf("after re-signing the same login in: %v, want the new ones", got)
	}

	// A completion of another login is not the override's.
	login, sess = ownLogin(maps.Clone(own))
	restore = keepOwnCookies(login)
	sess.SetCookies(nil)
	restore(&bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: connector.LoginStepIDComplete,
		CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: "other@example.com/9"}})
	if got := cookiesOf(sess); !maps.Equal(got, own) {
		t.Fatalf("after another login's completion: %v", got)
	}

	login, sess = ownLogin(maps.Clone(own))
	restore = keepOwnCookies(login)
	meanwhile := map[string]string{"SID": "meanwhile"}
	sess.SetCookies(meanwhile)
	restore(emojiStep())
	if got := cookiesOf(sess); !maps.Equal(got, meanwhile) {
		t.Fatalf("cookies set meanwhile were replaced: %v", got)
	}

	login, sess = ownLogin(nil)
	restore = keepOwnCookies(login)
	restore(emojiStep())
	if got := cookiesOf(sess); got != nil {
		t.Fatalf("a login with no cookies got %v", got)
	}

	// No override, or one with no session: nothing to keep, and no panic.
	for _, l := range []*bridgev2.UserLogin{
		nil,
		{},
		{UserLogin: &database.UserLogin{ID: "x"}},
		{UserLogin: &database.UserLogin{ID: "x", Metadata: map[string]any{}}},
		{UserLogin: &database.UserLogin{ID: "x", Metadata: &connector.UserLoginMetadata{}}},
		{UserLogin: &database.UserLogin{ID: "x", Metadata: (*connector.UserLoginMetadata)(nil)}},
	} {
		keepOwnCookies(l)(nil)
	}
}

// TestHooksShowExpiresIn: the worker hands the backend the QR lifetime
// hook. WHY: catches the hook left unset or set to another function.
func TestHooksShowExpiresIn(t *testing.T) {
	h := hooks().ShowExpiresIn
	if h == nil || reflect.ValueOf(h).Pointer() != reflect.ValueOf(qrExpiresIn).Pointer() {
		t.Fatal("hooks().ShowExpiresIn is not qrExpiresIn")
	}
}

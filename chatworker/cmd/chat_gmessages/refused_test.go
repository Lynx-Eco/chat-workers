// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"lynx.invalid/ingest/chatworker/backend"
)

// qrSession is a QR pairing's session as libgm holds it after the phone
// scanned the code (pair.go completePairing).
func qrSession() *libgm.AuthData {
	return &libgm.AuthData{TachyonAuthToken: []byte("tachyon"), Browser: &gmproto.Device{SourceID: "browser"}, Mobile: &gmproto.Device{SourceID: "phone"}}
}

// googleSession is a Google account pairing's session after the emoji was
// tapped (pair_google.go FinishGaiaPairing).
func googleSession() *libgm.AuthData {
	return &libgm.AuthData{
		TachyonAuthToken: []byte("tachyon"), Browser: &gmproto.Device{SourceID: "browser"}, Mobile: &gmproto.Device{SourceID: "other@example.com"},
		DestRegID: uuid.New(), PairingID: uuid.New(), Cookies: map[string]string{"SID": "other"},
	}
}

func refusedLogin(sess *libgm.AuthData) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID: "other@example.com/42", Metadata: &connector.UserLoginMetadata{Session: sess, IDPrefix: "00112233445566aa"},
	}}
}

// TestRefusedSession: the refused login's session is ended when it holds a
// pairing: a QR pairing's token and browser, a Google account pairing's
// pairing id, cookies and phone; a login with none of that, or none at
// all, has nothing to end. WHY: catches an end sent for a pairing that does
// not exist (libgm would answer nil and the log would say ended), and one
// left out for a pairing the other phone now lists.
func TestRefusedSession(t *testing.T) {
	noToken := qrSession()
	noToken.TachyonAuthToken = nil
	noBrowser := qrSession()
	noBrowser.Browser = nil
	noPairing := googleSession()
	noPairing.PairingID = uuid.Nil
	noCookies := googleSession()
	noCookies.Cookies = nil
	noPhone := googleSession()
	noPhone.Mobile = nil
	for _, c := range []struct {
		name  string
		login *bridgev2.UserLogin
		end   bool
	}{
		{"QR pairing", refusedLogin(qrSession()), true},
		{"Google account pairing", refusedLogin(googleSession()), true},
		{"QR, no token", refusedLogin(noToken), false},
		{"QR, no browser", refusedLogin(noBrowser), false},
		{"Google, no pairing id", refusedLogin(noPairing), false},
		{"Google, no cookies", refusedLogin(noCookies), false},
		{"Google, no phone", refusedLogin(noPhone), false},
		{"no session", refusedLogin(nil), false},
		{"other metadata", &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "x", Metadata: map[string]any{}}}, false},
		{"no row", &bridgev2.UserLogin{}, false},
		{"no login", nil, false},
	} {
		if got := refusedSession(c.login) != nil; got != c.end {
			t.Errorf("%s: end %v, want %v", c.name, got, c.end)
		}
	}
}

// fakeUnpair replaces unpairSession for one test.
func fakeUnpair(t *testing.T, f func(ctx context.Context, sess *libgm.AuthData) error) {
	t.Helper()
	orig := unpairSession
	unpairSession = func(ctx context.Context, sess *libgm.AuthData, _ exhttp.ClientSettings) error { return f(ctx, sess) }
	t.Cleanup(func() { unpairSession = orig })
}

// TestEndRefused: the hook unpairs the refused login's own session (the
// one the login process made, never another), answers nil when Google took
// it and the error when not, ErrNothingToEnd without a call when there is
// no pairing, and returns when its context ends even if the request does
// not (libgm's QR unpair takes no context). WHY: catches an end that
// unpairs the wrong session (the connection's own), an outcome logged as
// ended when it failed, and an end that outlives the backend's deadline.
func TestEndRefused(t *testing.T) {
	var got []*libgm.AuthData
	fakeUnpair(t, func(_ context.Context, sess *libgm.AuthData) error {
		got = append(got, sess)
		return nil
	})
	sess := qrSession()
	if err := endRefused(context.Background(), nil, refusedLogin(sess)); err != nil {
		t.Fatalf("QR: %v", err)
	}
	if len(got) != 1 || got[0] != sess {
		t.Fatalf("unpaired %v, want the refused login's session", got)
	}
	if err := endRefused(context.Background(), nil, refusedLogin(nil)); !errors.Is(err, backend.ErrNothingToEnd) {
		t.Fatalf("no session: %v", err)
	}
	if err := endRefused(context.Background(), nil, nil); !errors.Is(err, backend.ErrNothingToEnd) {
		t.Fatalf("no login: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("%d calls, want 1", len(got))
	}

	failed := errors.New("unpair failed")
	fakeUnpair(t, func(context.Context, *libgm.AuthData) error { return failed })
	if err := endRefused(context.Background(), nil, refusedLogin(googleSession())); !errors.Is(err, failed) {
		t.Fatalf("a failed unpair: %v", err)
	}

	release := make(chan struct{})
	defer close(release)
	fakeUnpair(t, func(context.Context, *libgm.AuthData) error { <-release; return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := endRefused(ctx, nil, refusedLogin(qrSession())); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a request that hangs: %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the end waited past its context")
	}
}

// recordingUnpairer is an unpairer that records which request it was sent
// and answers err.
type recordingUnpairer struct {
	calls []string
	err   error
}

func (r *recordingUnpairer) UnpairBugle() (*gmproto.RevokeRelayPairingResponse, error) {
	r.calls = append(r.calls, "bugle")
	return nil, r.err
}

func (r *recordingUnpairer) UnpairGaia(context.Context) error {
	r.calls = append(r.calls, "gaia")
	return r.err
}

// TestUnpairWith: a QR pairing is revoked with RevokeRelayPairing
// (UnpairBugle) and a Google account pairing ended with UNPAIR_GAIA_PAIRING
// (UnpairGaia), one request each, with the request's error returned as it
// is. WHY: catches libgm's Client.Unpair coming back (it sends UnpairGaia
// for a QR session too, as HasCookies is true for every session that is not
// a Google account's, so the QR pairing would stay on the other phone), and
// a failed request reported as ended.
func TestUnpairWith(t *testing.T) {
	if qrSession().IsGoogleAccount() || !googleSession().IsGoogleAccount() {
		t.Fatal("the test sessions are not of the kinds they are named")
	}
	if !qrSession().HasCookies() {
		t.Fatal("libgm's HasCookies no longer true for a QR session: the reason for unpairWith is gone, read it again")
	}
	failed := errors.New("unpair failed")
	for _, c := range []struct {
		name string
		sess *libgm.AuthData
		err  error
		want string
	}{
		{"QR pairing", qrSession(), nil, "bugle"},
		{"Google account pairing", googleSession(), nil, "gaia"},
		{"QR pairing, failed", qrSession(), failed, "bugle"},
		{"Google account pairing, failed", googleSession(), failed, "gaia"},
	} {
		r := &recordingUnpairer{err: c.err}
		if err := unpairWith(context.Background(), c.sess, r); !errors.Is(err, c.err) {
			t.Errorf("%s: %v, want %v", c.name, err, c.err)
		}
		if !slices.Equal(r.calls, []string{c.want}) {
			t.Errorf("%s: calls %v, want [%s]", c.name, r.calls, c.want)
		}
	}
}

// TestHooks: the hooks the worker gives the backend, and none it does not
// use. WHY: catches a hook left unset or set to another function,
// DropRefused or EndRefusedSession set (nothing is stored before the gate
// to drop, and the framework's LogoutRemote would report no outcome), and a
// channel or rate limit rule made up for a network that has none.
func TestHooks(t *testing.T) {
	h := hooks()
	same := func(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }
	if h.EndRefused == nil || !same(h.EndRefused, endRefused) {
		t.Error("EndRefused")
	}
	if h.SelfPersonID == nil || !same(h.SelfPersonID, selfPersonID) {
		t.Error("SelfPersonID")
	}
	if h.SendKeyFormat == nil || !same(h.SendKeyFormat, gmessagesSendKey) {
		t.Error("SendKeyFormat")
	}
	if h.ShowExpiresIn == nil || !same(h.ShowExpiresIn, qrExpiresIn) {
		t.Error("ShowExpiresIn")
	}
	if h.DropRefused != nil || h.EndRefusedSession || h.IsChannel != nil || h.RateLimitWait != nil ||
		h.WatchRateLimit != nil || h.OriginalMessageID != nil || h.PrepareLogins != nil {
		t.Error("a hook set that the worker does not use")
	}
	if h.IdentifierScheme != "gmessages" {
		t.Errorf("identifier scheme %q", h.IdentifierScheme)
	}
	if !slices.Equal(h.SignInFlows, []string{"google", "qr"}) {
		t.Errorf("flows %v", h.SignInFlows)
	}
	if !slices.Equal(h.EndedCodes, endedCodes) {
		t.Errorf("ended codes %v", h.EndedCodes)
	}
}

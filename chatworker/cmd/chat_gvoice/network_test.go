// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
)

// fakeCookies is a cookies login process: on a submission it stores a
// prefix row as the connector's SubmitCookies does before NewLogin
// (login.go:140), and answers with step or err.
type fakeCookies struct {
	net     *gvNetwork
	t       *testing.T
	step    *bridgev2.LoginStep
	err     error
	calls   int
	cancels int
}

func (f *fakeCookies) Start(context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeCookies, StepID: connector.LoginStepIDCookies}, nil
}

func (f *fakeCookies) Cancel() { f.cancels++ }

func (f *fakeCookies) SubmitCookies(context.Context, map[string]string) (*bridgev2.LoginStep, error) {
	f.calls++
	prefixOf(f.t, f.net, loginIDOf(otherNumber))
	return f.step, f.err
}

// TestSweptLogin: a submission the connector failed leaves no prefix row of
// its own (the one it stored before NewLogin), and its answer is the
// connector's as it is; the connection's own row stays; a submission that
// succeeded is not swept. Start and Cancel are the connector's. WHY:
// catches another account's number left in the store by a sign-in whose
// NewLogin failed before the gate (a cancel, a stop), a sweep that takes
// the connection's own prefix, and a wrapper that changes the connector's
// answer or drops a cancel.
func TestSweptLogin(t *testing.T) {
	n, db := testGV(t)
	mine := loginIDOf(mineNumber)
	prefixOf(t, n, mine)
	storeLogin(t, db, mine)
	own := prefixRows(t, db)
	failed := errors.New("NewLogin: context canceled")
	f := &fakeCookies{net: n, t: t, err: failed}
	l := &sweptLogin{net: n, inner: f}
	if st, err := l.Start(t.Context()); err != nil || st.StepID != connector.LoginStepIDCookies {
		t.Fatalf("start %v %v", st, err)
	}
	step, err := l.SubmitCookies(t.Context(), map[string]string{"SID": "s"})
	if step != nil || !errors.Is(err, failed) || f.calls != 1 {
		t.Fatalf("failed submission: %v %v (%d calls)", step, err, f.calls)
	}
	if after := prefixRows(t, db); !slices.Equal(after, own) {
		t.Fatalf("after a failed submission %v, want %v", after, own)
	}
	done := &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: connector.LoginStepIDComplete}
	f.step, f.err = done, nil
	if step, err := l.SubmitCookies(t.Context(), nil); step != done || err != nil {
		t.Fatalf("submission that succeeded: %v %v", step, err)
	}
	if after := prefixRows(t, db); len(after) != len(own)+1 {
		t.Fatalf("a submission that succeeded was swept: %v", after)
	}
	// A cancelled sign-in's context does not stop the sweep.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f.step, f.err = nil, failed
	if _, err := l.SubmitCookies(ctx, nil); !errors.Is(err, failed) {
		t.Fatalf("cancelled: %v", err)
	}
	if after := prefixRows(t, db); !slices.Equal(after, own) {
		t.Fatalf("after a cancelled submission %v, want %v", after, own)
	}
	l.Cancel()
	if f.cancels != 1 {
		t.Fatalf("cancels %d", f.cancels)
	}
}

// TestCreateLoginWraps: the connector's one flow's process is wrapped
// around the connector's own; its first step is the connector's WEB step
// (no network: the step is fixed), with the required cookies the connector
// names; an unknown flow is the connector's error. WHY: catches a process
// that reaches the backend unwrapped (a failed submission would leave its
// prefix row) and a cookie list changed upstream.
func TestCreateLoginWraps(t *testing.T) {
	keepPath(t)
	nc, err := newConnector(initWith(nil))
	if err != nil {
		t.Fatal(err)
	}
	n := nc.(*gvNetwork)
	proc, err := n.CreateLogin(t.Context(), &bridgev2.User{}, connector.LoginFlowIDCookies)
	if err != nil {
		t.Fatal(err)
	}
	l, ok := proc.(*sweptLogin)
	if !ok || l.net != n {
		t.Fatalf("process %T", proc)
	}
	if _, ok := l.inner.(*connector.GVLogin); !ok {
		t.Fatalf("inner %T", l.inner)
	}
	st, err := l.Start(t.Context())
	if err != nil || st.Type != bridgev2.LoginStepTypeCookies || st.StepID != "fi.mau.gvoice.cookies" || st.CookiesParams == nil {
		t.Fatalf("first step %+v %v", st, err)
	}
	p := st.CookiesParams
	if p.URL != "https://voice.google.com/signup" || p.UserAgent != "" || p.ExtractJS != "" || p.WaitForURLPattern != "" {
		t.Fatalf("web step %+v", p)
	}
	var required, optional []string
	for _, f := range p.Fields {
		if len(f.Sources) != 1 || f.Sources[0].Type != bridgev2.LoginCookieTypeCookie || f.Sources[0].Name != f.ID || f.Sources[0].CookieDomain == "" {
			t.Fatalf("field %+v", f)
		}
		if f.Required {
			required = append(required, f.ID)
		} else {
			optional = append(optional, f.ID)
		}
	}
	slices.Sort(required)
	slices.Sort(optional)
	if !slices.Equal(required, []string{"APISID", "HSID", "SAPISID", "SID", "SSID"}) ||
		!slices.Equal(optional, []string{"COMPASS", "OSID", "__Secure-1PSIDTS"}) {
		t.Fatalf("required %v optional %v", required, optional)
	}
	if _, err := n.CreateLogin(t.Context(), &bridgev2.User{}, "qr"); !errors.Is(err, bridgev2.ErrInvalidLoginFlowID) {
		t.Fatalf("unknown flow: %v", err)
	}
}

// TestLoadUserLoginRefusesNoNumber: a login with no number (a Google account
// with a Voice line but no primary number: its id would be the same for
// every such account) fails as the connector's no_number and its prefix row
// is deleted; a login with a number is the connector's own load (a client,
// signed in with the stored cookies, no network). WHY: catches a principal
// that names no account passing the gate as the connection's own, and a
// wrapper that keeps the connector's load from running.
func TestLoadUserLoginRefusesNoNumber(t *testing.T) {
	n, db := testGV(t)
	none := loginIDOf("")
	prefixOf(t, n, none)
	login := loginOf(none, "1")
	err := n.LoadUserLogin(t.Context(), login)
	var re bridgev2.RespError
	if !errors.As(err, &re) || re.ErrCode != "FI.MAU.GVOICE.NO_NUMBER" {
		t.Fatalf("no number: %v", err)
	}
	if login.Client != nil {
		t.Fatal("a client for a login with no number")
	}
	if rows := prefixRows(t, db); len(rows) != 0 {
		t.Fatalf("rows kept %v", rows)
	}
	own := loginOf(loginIDOf(mineNumber), "1")
	if err := n.LoadUserLogin(t.Context(), own); err != nil {
		t.Fatal(err)
	}
	c, ok := own.Client.(*connector.GVClient)
	if !ok || !c.IsLoggedIn() {
		t.Fatalf("client %T", own.Client)
	}
}

// TestGVNetworkKeepsOptionalInterfaces: the wrapped connector is still
// every optional interface the connector is, which the backend's gate
// passes on to the framework. WHY: catches a wrapper that hides direct
// media or the user id check.
func TestGVNetworkKeepsOptionalInterfaces(t *testing.T) {
	var nc bridgev2.NetworkConnector = newGVNetwork(&connector.GVConnector{})
	if _, ok := nc.(bridgev2.DirectMediableNetwork); !ok {
		t.Error("not DirectMediableNetwork")
	}
	if _, ok := nc.(bridgev2.IdentifierValidatingNetwork); !ok {
		t.Error("not IdentifierValidatingNetwork")
	}
}

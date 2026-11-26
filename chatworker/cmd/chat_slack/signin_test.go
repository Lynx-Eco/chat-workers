// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/mautrix-slack/pkg/connector"
	"go.mau.fi/mautrix-slack/pkg/slackid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"lynx.invalid/ingest/chatworker/backend"
)

// fakeProcess is a login process of a connector flow: on a submission it
// optionally plays the gate (calls the worker's DropRefused, as the
// backend's gated LoadUserLogin does inside NewLogin) and answers with
// what the email flow answers after a NewLogin error, or with an error.
type fakeProcess struct {
	net     *slackNetwork
	login   *bridgev2.UserLogin
	refuse  bool
	step    *bridgev2.LoginStep
	err     error
	ends    []func(context.Context) error
	calls   int
	cancels int
}

func (f *fakeProcess) Start(context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeUserInput, StepID: connector.LoginStepIDEmail}, nil
}

func (f *fakeProcess) Cancel() { f.cancels++ }

func (f *fakeProcess) submit(ctx context.Context) (*bridgev2.LoginStep, error) {
	f.calls++
	if f.refuse {
		end, err := dropRefused(ctx, f.net, f.login)
		if err != nil {
			return nil, err
		}
		f.ends = append(f.ends, end)
	}
	return f.step, f.err
}

func (f *fakeProcess) SubmitUserInput(ctx context.Context, _ map[string]string) (*bridgev2.LoginStep, error) {
	return f.submit(ctx)
}

func (f *fakeProcess) SubmitCookies(ctx context.Context, _ map[string]string) (*bridgev2.LoginStep, error) {
	return f.submit(ctx)
}

// cookiesOnly has no SubmitUserInput (the token flow's process).
type cookiesOnly struct{ p *fakeProcess }

func (f cookiesOnly) Start(ctx context.Context) (*bridgev2.LoginStep, error) { return f.p.Start(ctx) }
func (f cookiesOnly) Cancel()                                                { f.p.Cancel() }
func (f cookiesOnly) SubmitCookies(ctx context.Context, in map[string]string) (*bridgev2.LoginStep, error) {
	return f.p.SubmitCookies(ctx, in)
}

func refusedLogin(token string) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
		ID:       slackid.MakeUserLoginID("T0OTHER", "U0OTHER"),
		Metadata: &slackid.UserLoginMetadata{Token: token, CookieToken: "xoxd-c"},
	}}
}

// TestGatedLoginRefusal: a submission during which the gate refused a login
// fails with backend.ErrOtherAccount, whatever the connector answered: the
// step the email flow offers after swallowing NewLogin's error ("enter your
// email again"), its own error, or the gate's error passed through; the
// gate's other_account_kept stays that. A submission with no refusal is
// the connector's answer as it is, and the next submission of the same
// process is not taken for refused. WHY: catches the email flow's
// swallowed refusal reaching the host as a new email step (the host would
// never learn principal_mismatch), a refusal noted on the wrong
// submission, and a refusal that sticks to the next one.
func TestGatedLoginRefusal(t *testing.T) {
	emailAgain := &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeUserInput, StepID: connector.LoginStepIDEmail}
	next := &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeUserInput, StepID: connector.LoginStepIDEmailCode}
	other := errors.New("slack said no")
	for _, c := range []struct {
		name     string
		refuse   bool
		step     *bridgev2.LoginStep
		err      error
		cookies  bool
		wantStep *bridgev2.LoginStep
		wantErr  error
	}{
		{"no refusal: step", false, next, nil, false, next, nil},
		{"no refusal: error", false, nil, other, false, nil, other},
		{"refused, email asked again", true, emailAgain, nil, false, nil, backend.ErrOtherAccount},
		{"refused, from the web step", true, emailAgain, nil, true, nil, backend.ErrOtherAccount},
		{"refused, gate error returned", true, nil, backend.ErrOtherAccount, true, nil, backend.ErrOtherAccount},
		{"refused, another error", true, nil, other, false, nil, backend.ErrOtherAccount},
		{"refused, kept", true, nil, backend.ErrOtherAccountKept, false, nil, backend.ErrOtherAccountKept},
	} {
		t.Run(c.name, func(t *testing.T) {
			n := newSlackNetwork(&connector.SlackConnector{})
			f := &fakeProcess{net: n, login: refusedLogin("xoxc-1"), refuse: c.refuse, step: c.step, err: c.err}
			g := &gatedLogin{net: n, flow: connector.LoginFlowIDEmail, inner: f}
			var step *bridgev2.LoginStep
			var err error
			if c.cookies {
				step, err = g.SubmitCookies(context.Background(), nil)
			} else {
				step, err = g.SubmitUserInput(context.Background(), nil)
			}
			if step != c.wantStep || !errors.Is(err, c.wantErr) || (c.wantErr == nil && err != nil) {
				t.Fatalf("got %v %v, want %v %v", step, err, c.wantStep, c.wantErr)
			}
			if c.wantErr == backend.ErrOtherAccount && errors.Is(err, other) {
				t.Fatal("the connector's error leaked into the refusal")
			}
			// The next submission, with no refusal, is the connector's.
			f.refuse, f.step, f.err = false, next, nil
			if step, err := g.SubmitUserInput(context.Background(), nil); step != next || err != nil {
				t.Fatalf("next submission: %v %v", step, err)
			}
			if n.running != nil {
				t.Fatal("a submission left running")
			}
		})
	}
}

// TestGatedLoginDelegates: Start and Cancel are the connector's, and a
// submission of a kind the flow's process has no method for is an error
// that asks nothing of the process. WHY: catches a wrapper that drops a
// cancel (the process's own would never run) or calls the wrong method.
func TestGatedLoginDelegates(t *testing.T) {
	n := newSlackNetwork(&connector.SlackConnector{})
	f := &fakeProcess{net: n}
	g := &gatedLogin{net: n, flow: connector.LoginFlowIDAuthToken, inner: cookiesOnly{f}}
	if st, err := g.Start(context.Background()); err != nil || st.StepID != connector.LoginStepIDEmail {
		t.Fatalf("start %v %v", st, err)
	}
	g.Cancel()
	if f.cancels != 1 {
		t.Fatalf("cancels %d", f.cancels)
	}
	if _, err := g.SubmitUserInput(context.Background(), nil); !errors.Is(err, errNotOfFlow) || f.calls != 0 {
		t.Fatalf("fields to a cookies-only flow: %v (%d calls)", err, f.calls)
	}
	if _, err := g.SubmitCookies(context.Background(), nil); err != nil || f.calls != 1 {
		t.Fatalf("cookies to the token flow: %v (%d calls)", err, f.calls)
	}
}

// TestCreateLoginWraps: each offered flow's process is wrapped, with its
// flow id, around the connector's own process of that flow; an unknown
// flow is the connector's error (the backend refuses a flow not offered,
// "app" among them, before CreateLogin: smoke_test.go). WHY: catches a flow whose process reaches the backend
// unwrapped (its refusal would not be seen) and a wrong flow id (the
// session end would be decided for another flow).
func TestCreateLoginWraps(t *testing.T) {
	n := newSlackNetwork(&connector.SlackConnector{})
	user := &bridgev2.User{}
	for flow, want := range map[string]any{
		connector.LoginFlowIDEmail:     (*connector.SlackEmailLogin)(nil),
		connector.LoginFlowIDAuthToken: (*connector.SlackTokenLogin)(nil),
	} {
		proc, err := n.CreateLogin(context.Background(), user, flow)
		if err != nil {
			t.Fatalf("%s: %v", flow, err)
		}
		g, ok := proc.(*gatedLogin)
		if !ok || g.flow != flow || g.net != n {
			t.Fatalf("%s: %T %+v", flow, proc, proc)
		}
		var same bool
		switch want.(type) {
		case *connector.SlackEmailLogin:
			_, same = g.inner.(*connector.SlackEmailLogin)
		case *connector.SlackTokenLogin:
			_, same = g.inner.(*connector.SlackTokenLogin)
		}
		if !same {
			t.Fatalf("%s: inner %T", flow, g.inner)
		}
	}
	if _, err := n.CreateLogin(context.Background(), user, "qr"); !errors.Is(err, bridgev2.ErrInvalidLoginFlowID) {
		t.Fatalf("unknown flow: %v", err)
	}
}

// TestSlackNetworkKeepsOptionalInterfaces: the wrapped connector is still
// every optional interface the connector is, which the backend's gate
// passes on to the framework. WHY: catches a wrapper that hides direct
// media, the file size limit or the user id check.
func TestSlackNetworkKeepsOptionalInterfaces(t *testing.T) {
	var nc bridgev2.NetworkConnector = newSlackNetwork(&connector.SlackConnector{})
	if _, ok := nc.(bridgev2.DirectMediableNetwork); !ok {
		t.Error("not DirectMediableNetwork")
	}
	if _, ok := nc.(bridgev2.MaxFileSizeingNetwork); !ok {
		t.Error("not MaxFileSizeingNetwork")
	}
	if _, ok := nc.(bridgev2.IdentifierValidatingNetwork); !ok {
		t.Error("not IdentifierValidatingNetwork")
	}
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"sync"

	"go.mau.fi/mautrix-slack/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
)

// errNotOfFlow is a submit of a kind the connector's login process has no
// method for (the backend submits by the kind of the step the process
// itself gave, so it does not happen).
var errNotOfFlow = errors.New("the login process takes no submission of that kind")

// slackNetwork is the connector the framework is given: mautrix-slack's,
// every method of it as it is (embedded: its optional interfaces, direct
// media, the maximum file size, the user id check, come with it), but for
// CreateLogin, whose login processes it wraps (gatedLogin).
//
// Why: the gate (backend gate.go) refuses a new login of another account
// than the connection's inside the framework's NewLogin, and the backend
// fails the sign-in as principal_mismatch when the login process returns
// that error. The email flow does not return it: completeWorkspace
// (login-email.go) catches any error of its last step, NewLogin's
// included, and asks for the email again ("Slack created a session, but the
// bridge could not validate it"). The host would see the first step again,
// never the refusal, and the session the flow opened at Slack would stay
// open. The backend calls Hooks.DropRefused synchronously inside NewLogin,
// so dropRefused notes the refusal on the submission running (refusal),
// and gatedLogin answers that submission with backend.ErrOtherAccount
// whatever the connector returned.
type slackNetwork struct {
	*connector.SlackConnector

	mu sync.Mutex
	// running is the submission running now, nil between them. The backend
	// runs one sign-in at a time and one submission of it at a time (its
	// loginMu), and the gate is reached only from a submission (the
	// connector calls NewLogin from SubmitUserInput or SubmitCookies, never
	// from Start).
	running *submission
}

// submission is one call into a login process.
type submission struct {
	flow string
	// fields: a FIELDS step's submission (SubmitUserInput); else a WEB
	// step's (SubmitCookies).
	fields bool
	// refused: the gate refused a login during it.
	refused bool
}

func newSlackNetwork(sc *connector.SlackConnector) *slackNetwork {
	return &slackNetwork{SlackConnector: sc}
}

// CreateLogin is the connector's, wrapped.
func (n *slackNetwork) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	proc, err := n.SlackConnector.CreateLogin(ctx, user, flowID)
	if err != nil {
		return nil, err
	}
	return &gatedLogin{net: n, flow: flowID, inner: proc}, nil
}

// begin notes a submission as running.
func (n *slackNetwork) begin(flow string, fields bool) *submission {
	s := &submission{flow: flow, fields: fields}
	n.mu.Lock()
	n.running = s
	n.mu.Unlock()
	return s
}

// end notes that s is over and tells whether the gate refused a login
// during it.
func (n *slackNetwork) end(s *submission) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.running == s {
		n.running = nil
	}
	return s.refused
}

// refusal marks the submission running as refused and returns a copy of
// it; nil when none runs.
func (n *slackNetwork) refusal() *submission {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.running == nil {
		return nil
	}
	n.running.refused = true
	cp := *n.running
	return &cp
}

// gatedLogin is a login process of the connector whose submissions end in
// backend.ErrOtherAccount when the gate refused a login during them.
// It offers both kinds of submission: the backend submits by the kind of
// the step the process gave (a FIELDS step's values, a WEB step's
// captures), and each flow's process has the method for each kind of step
// it gives (email: both; token: WEB).
type gatedLogin struct {
	net   *slackNetwork
	flow  string
	inner bridgev2.LoginProcess
}

var (
	_ bridgev2.LoginProcessUserInput = (*gatedLogin)(nil)
	_ bridgev2.LoginProcessCookies   = (*gatedLogin)(nil)
)

func (g *gatedLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	return g.inner.Start(ctx)
}

func (g *gatedLogin) Cancel() { g.inner.Cancel() }

func (g *gatedLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	p, ok := g.inner.(bridgev2.LoginProcessUserInput)
	if !ok {
		return nil, errNotOfFlow
	}
	s := g.net.begin(g.flow, true)
	step, err := p.SubmitUserInput(ctx, input)
	return gated(g.net.end(s), step, err)
}

func (g *gatedLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	p, ok := g.inner.(bridgev2.LoginProcessCookies)
	if !ok {
		return nil, errNotOfFlow
	}
	s := g.net.begin(g.flow, false)
	step, err := p.SubmitCookies(ctx, cookies)
	return gated(g.net.end(s), step, err)
}

// gated is a submission's answer: the connector's, unless the gate refused
// a login during it. Then it is backend.ErrOtherAccount (the sign-in fails
// as principal_mismatch, backend signin.go errCode), or the gate's own
// error when that was backend.ErrOtherAccountKept (it could not undo what
// was stored: other_account_kept). Any step the connector offered after
// the refusal (the email flow asks for the email again) is dropped.
func gated(refused bool, step *bridgev2.LoginStep, err error) (*bridgev2.LoginStep, error) {
	if !refused {
		return step, err
	}
	if errors.Is(err, backend.ErrOtherAccountKept) {
		return nil, err
	}
	return nil, backend.ErrOtherAccount
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"strings"

	"github.com/slack-go/slack"
	"go.mau.fi/mautrix-slack/pkg/connector"
	"go.mau.fi/mautrix-slack/pkg/slackid"
	"maunium.net/go/mautrix/bridgev2"
)

// dropRefused is the worker's backend.Hooks.DropRefused, which the gate
// calls synchronously when it refuses a new login of another account than
// the connection's (another workspace, or another user in the same one: a
// Slack login id is "<team id>-<user id>").
//
// It deletes nothing: the connector stores nothing of a login before the
// framework's NewLogin, where the gate refuses it (the two flows offered only
// ask Slack, login-email.go and login-cookie.go SubmitCookies; the
// connector's LoadUserLogin never runs for a refused login). It is this
// hook, not EndRefused, because it is the one the backend runs at once,
// inside NewLogin: it notes the refusal on the running submission
// (slackNetwork.refusal), which then fails as principal_mismatch even
// where the email flow swallows the error (signin.go). The error is always
// nil.
//
// The end it returns, run by the backend in the background, signs out at
// Slack the session the refused sign-in made, when it made one: only a
// login the email flow completed from a FIELDS step (the code, the
// workspace or the two-factor code: Slack's email sign-in API made that
// token for this sign-in, login-email-api.go). Nothing is ended
// (refused_session_nothing_to_end) for:
//   - the token flow: the token is a browser session the user already had;
//     signing it out would sign that browser out;
//   - the email flow's WEB step for a workspace with single sign-on: the
//     web view may have reused a session the user already had in it;
//   - a refusal with no submission running (not reached: the gate is
//     reached only from one).
func dropRefused(_ context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) (func(context.Context) error, error) {
	n, ok := inner.(*slackNetwork)
	if !ok {
		return nil, nil
	}
	return refusedEnd(n.refusal(), login, ""), nil
}

// refusedEnd is the end of the session of a refused login made by submission
// s, nil when there is none to end (dropRefused). apiURL is Slack's API
// base ("" for slack-go's, https://slack.com/api/; tests give their own).
func refusedEnd(s *submission, login *bridgev2.UserLogin, apiURL string) func(context.Context) error {
	if s == nil || s.flow != connector.LoginFlowIDEmail || !s.fields || login == nil || login.UserLogin == nil {
		return nil
	}
	meta, ok := login.Metadata.(*slackid.UserLoginMetadata)
	if !ok || meta == nil || !userToken(meta.Token) {
		return nil
	}
	token, cookie := meta.Token, meta.CookieToken
	return func(ctx context.Context) error { return signOut(ctx, apiURL, token, cookie) }
}

// userToken tells whether a token is a user's client session (xoxc- of a
// browser, xoxs- of a desktop client), the kind auth.signout ends and the
// connector itself signs out at logout (client.go LogoutRemote,
// IsRealUser).
func userToken(token string) bool {
	return strings.HasPrefix(token, "xoxc-") || strings.HasPrefix(token, "xoxs-")
}

// signOut ends a user's session at Slack with auth.signout, the call the
// connector's LogoutRemote makes (client.go), on a client of the worker's
// own made as the connector makes one (the token, and the "d" cookie with
// it; client.go makeSlackClient) with no retries. nil
// (refused_session_ended) when Slack answered ok, or answered that the
// token no longer works (what the sign-out would do is done); an error
// (refused_session_not_ended) for anything else: no network, no answer,
// another error, or ctx ended first (the session then stays open at Slack
// until the user signs it out there or it expires). The strings of the
// token and cookie cannot be zeroed (Go strings): they stay on the heap
// until collected.
func signOut(ctx context.Context, apiURL, token, cookie string) error {
	opts := []slack.Option{}
	if cookie != "" {
		opts = append(opts, slack.OptionCookie("d", cookie))
	}
	if apiURL != "" {
		opts = append(opts, slack.OptionAPIURL(apiURL))
	}
	_, err := slack.New(token, opts...).SendAuthSignoutContext(ctx)
	if err == nil {
		return nil
	}
	var re slack.SlackErrorResponse
	if errors.As(err, &re) {
		switch re.Err {
		case "invalid_auth", "not_authed", "token_revoked", "token_expired", "account_inactive":
			return nil
		}
	}
	return err
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"go.mau.fi/mautrix-slack/pkg/slackid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
)

// TestSelfPersonID: the account's own person id is the connector's user id
// of the login's team and user (lower case), which is what its messages
// carry as their sender; a login id not of the form gives "" (the
// framework's default). WHY: catches the default left in place (the upper
// case login id, which no sender carries: the account's own messages would
// have a sender with no is_self PersonInfo) and a team/user swap.
func TestSelfPersonID(t *testing.T) {
	for _, c := range []struct {
		id   networkid.UserLoginID
		want networkid.UserID
	}{
		{"T0123ABC-U0456DEF", "t0123abc-u0456def"},
		{slackid.MakeUserLoginID("E01", "W02"), "e01-w02"},
		{"", ""},
		{"T0123", ""},
		{"T01-U02-X03", ""},
		{"t0123abc-u0456def", ""},
		{"T01-", ""},
		{"-U02", ""},
	} {
		login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: c.id}}
		if got := selfPersonID(login); got != c.want {
			t.Errorf("%q: %q, want %q", c.id, got, c.want)
		}
	}
	// The sender id the connector gives the account's own events.
	team, user := "T0123ABC", "U0456DEF"
	if got, want := selfPersonID(&bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: slackid.MakeUserLoginID(team, user)}}), slackid.MakeUserID(team, user); got != want {
		t.Fatalf("self %q, the connector's sender %q", got, want)
	}
	if selfPersonID(nil) != "" || selfPersonID(&bridgev2.UserLogin{}) != "" {
		t.Fatal("no login, an id")
	}
}

// TestHooksReportedState: a session Slack ended is ENDED with the
// connector's code; an account Slack deactivated, a workspace disabled, a
// user removed stay NEEDS_USER (the token is kept and a restart may
// connect again); the connector's transient and unknown codes keep their
// kinds. WHY: catches a code that changed upstream, a revoked session that
// waits as NEEDS_USER as if it could come back, and a deactivation reported
// ENDED.
func TestHooksReportedState(t *testing.T) {
	h := hooks()
	for _, c := range []struct {
		ev   status.BridgeStateEvent
		code status.BridgeStateErrorCode
		want contract.ConnectionStateKind
	}{
		{status.StateBadCredentials, "slack-invalid-auth", contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED},
		{status.StateBadCredentials, "slack-token-revoked", contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED},
		{status.StateBadCredentials, "slack-not-logged-in", contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED},
		{status.StateBadCredentials, "slack-account-inactive", contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER},
		{status.StateBadCredentials, "slack-team-disabled", contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER},
		{status.StateBadCredentials, "slack-user-removed-from-team", contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER},
		{status.StateTransientDisconnect, "slack-connection-error", contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING},
		{status.StateTransientDisconnect, "slack-rtm-disconnected", contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING},
		{status.StateUnknownError, "slack-rtm-fatal-error", contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR},
		{status.StateUnknownError, "slack-unknown-fetch-error", contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR},
	} {
		cs := h.ReportedState(&status.BridgeState{StateEvent: c.ev, Error: c.code}, h.IdentifierScheme)
		if cs.GetState() != c.want || cs.GetCode() != string(c.code) {
			t.Errorf("%s %s: %v %q, want %v", c.ev, c.code, cs.GetState(), cs.GetCode(), c.want)
		}
	}
	// Every code is one the connector names as a human error (client.go
	// init): a code renamed upstream drops out of that map.
	for _, code := range append(slices.Clone(endedCodes), codeAccountInactive, codeTeamDisabled, codeUserRemoved) {
		if _, ok := status.BridgeStateHumanErrors[code]; !ok {
			t.Errorf("%s is not a code of the connector", code)
		}
	}
}

// TestHooks: the hooks the worker gives the backend. WHY: catches a hook
// left unset or set to another function, the default identifier scheme
// ("slack", which no identifier the connector gives carries), the
// connector's "app" flow offered (its account is a bot, not the user's),
// and a send
// key mapping (nothing of the key reaches Slack: a mapping would only make
// the stored transaction id differ from the key).
func TestHooks(t *testing.T) {
	h := hooks()
	same := func(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }
	if h.SelfPersonID == nil || !same(h.SelfPersonID, selfPersonID) {
		t.Error("SelfPersonID")
	}
	if h.DropRefused == nil || !same(h.DropRefused, dropRefused) {
		t.Error("DropRefused")
	}
	if h.RateLimitWait == nil || !same(h.RateLimitWait, rateLimitWait) {
		t.Error("RateLimitWait")
	}
	if h.SendKeyFormat != nil || h.EndRefused != nil || h.EndRefusedSession || h.IsChannel != nil || h.ShowExpiresIn != nil {
		t.Error("a hook set that the worker does not use")
	}
	if h.IdentifierScheme != "slack-internal" {
		t.Errorf("identifier scheme %q", h.IdentifierScheme)
	}
	if !slices.Equal(h.SignInFlows, []string{"email", "token"}) {
		t.Errorf("flows %v", h.SignInFlows)
	}
}

// TestRateLimitWait: Slack's 429 after slack-go's retries is the rate
// limit with Slack's wait, also wrapped; a "ratelimited" API error is the
// rate limit with no wait; anything else is not. WHY: catches a history
// fetch Slack throttled answered as a plain failure (the host would retry
// at once).
func TestRateLimitWait(t *testing.T) {
	for _, c := range []struct {
		err  error
		ok   bool
		wait time.Duration
	}{
		{&slack.RateLimitedError{RetryAfter: 30 * time.Second}, true, 30 * time.Second},
		{fmt.Errorf("fetch: %w", &slack.RateLimitedError{RetryAfter: 3 * time.Second}), true, 3 * time.Second},
		{slack.SlackErrorResponse{Err: "ratelimited"}, true, 0},
		{slack.SlackErrorResponse{Err: "channel_not_found"}, false, 0},
		{errors.New("ratelimited"), false, 0},
		{nil, false, 0},
	} {
		wait, ok := rateLimitWait(c.err)
		if ok != c.ok || wait != c.wait {
			t.Errorf("%v: %v %v, want %v %v", c.err, wait, ok, c.wait, c.ok)
		}
	}
}

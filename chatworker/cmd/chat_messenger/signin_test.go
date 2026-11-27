// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/connector"
	"go.mau.fi/mautrix-meta/pkg/messagix/loginerrors"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"
	"maunium.net/go/mautrix/bridgev2"
)

// TestCookieSteps: each flow offered begins with one cookies step (the
// contract's WEB step) of the connector's step id, on its site's address,
// with the user agent the connector later sends Meta from, capturing the
// three cookies the connector requires (xs, c_user, datr), each required,
// each a cookie of that site's domain; its wait-for-URL pattern takes the
// site's messages pages and not its login page. The step asks nothing of
// the network. WHY: catches a flow whose first step the contract cannot
// carry, a capture the connector does not take (the sign-in would fail as
// missing cookies after the user signed in), a domain that would read the
// cookies of another site, and a pattern that closes the web view on the
// login page.
func TestCookieSteps(t *testing.T) {
	nc, err := newConnector(nil)
	if err != nil {
		t.Fatal(err)
	}
	mc := nc.(*connector.MetaConnector)
	for _, c := range []struct {
		flow, url, domain string
		done              []string
		notDone           []string
	}{
		{
			connector.FlowIDFacebookCookies, "https://www.facebook.com/", "facebook.com",
			[]string{"https://www.facebook.com/", "https://www.facebook.com/messages/t/100004567890123/", "https://www.facebook.com/messages/e2ee/t/123/", "https://www.facebook.com/?sk=h_chr"},
			[]string{"https://www.facebook.com/login/", "https://www.facebook.com/checkpoint/1501092823525282/", "https://m.facebook.com/", "https://www.messenger.com/"},
		},
		{
			connector.FlowIDMessengerCookies, "https://www.messenger.com/?no_redirect=true", "messenger.com",
			[]string{"https://www.messenger.com/?no_redirect=true", "https://www.messenger.com/t/100004567890123/", "https://www.messenger.com/e2ee/t/123"},
			[]string{"https://www.messenger.com/login/", "https://www.facebook.com/", "https://www.messenger.com/checkpoint/"},
		},
	} {
		proc, err := mc.CreateLogin(context.Background(), nil, c.flow)
		if err != nil {
			t.Fatalf("%s: %v", c.flow, err)
		}
		step, err := proc.Start(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", c.flow, err)
		}
		if step.Type != bridgev2.LoginStepTypeCookies || step.StepID != connector.LoginStepIDCookies || connector.LoginStepIDCookies != "fi.mau.meta.cookies" {
			t.Fatalf("%s: step %s %s", c.flow, step.Type, step.StepID)
		}
		p := step.CookiesParams
		if p == nil || p.URL != c.url || p.UserAgent != useragent.UserAgent || p.ExtractJS != "" {
			t.Fatalf("%s: params %+v", c.flow, p)
		}
		var ids []string
		for _, f := range p.Fields {
			ids = append(ids, f.ID)
			if !f.Required || len(f.Sources) != 1 || f.Sources[0].Type != bridgev2.LoginCookieTypeCookie ||
				f.Sources[0].Name != f.ID || f.Sources[0].CookieDomain != c.domain {
				t.Errorf("%s: field %+v", c.flow, f)
			}
		}
		if !slices.Equal(ids, []string{"xs", "c_user", "datr"}) {
			t.Errorf("%s: captures %v", c.flow, ids)
		}
		re := regexp.MustCompile(p.WaitForURLPattern)
		for _, u := range c.done {
			if !re.MatchString(u) {
				t.Errorf("%s: %s does not end the web view", c.flow, u)
			}
		}
		for _, u := range c.notDone {
			if re.MatchString(u) {
				t.Errorf("%s: %s ends the web view", c.flow, u)
			}
		}
		// The page the step opens matches the pattern itself: a host must
		// wait for the required cookies too before it closes the web view
		// (the framework's rule for WaitForURLPattern, bridgev2 login.go).
		if !re.MatchString(p.URL) {
			t.Errorf("%s: the start page no longer matches; the README's note on it is stale", c.flow)
		}
		proc.Cancel()
	}
}

// TestMissingCookiesFailBeforeMeta: a submission without all three cookies
// fails with the connector's missing-cookies error (the sign-in's failure
// code fi.mau.meta_missing_cookies) before any request to Meta: the check
// comes first in SubmitCookies, before a client is made. A value of a
// cookie is not in the error. WHY: catches a partial capture sent to Meta
// (a request that can only fail, from a fresh client), and a cookie value
// leaking into the code or the message the host is given.
func TestMissingCookiesFailBeforeMeta(t *testing.T) {
	nc, err := newConnector(nil)
	if err != nil {
		t.Fatal(err)
	}
	mc := nc.(*connector.MetaConnector)
	const secret = "lynx-test-cookie-value"
	for _, flow := range signInFlows {
		for _, captured := range []map[string]string{
			{},
			{"xs": secret},
			{"xs": secret, "c_user": "100004567890123"},
			{"c_user": "100004567890123", "datr": secret},
			{"xs": "", "c_user": "100004567890123", "datr": secret},
		} {
			proc, err := mc.CreateLogin(context.Background(), nil, flow)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := proc.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err = proc.(bridgev2.LoginProcessCookies).SubmitCookies(context.Background(), captured)
			var re bridgev2.RespError
			if !errors.As(err, &re) || re.ErrCode != loginerrors.MissingCookies.ErrCode {
				t.Fatalf("%s %v: %v, want missing cookies", flow, captured, err)
			}
			if strings.ToLower(re.ErrCode) != "fi.mau.meta_missing_cookies" {
				t.Fatalf("code %q", re.ErrCode)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("a cookie value in the error: %v", err)
			}
		}
	}
}

// TestPasswordFlowsAreNotOffered: the connector's other flows exist (so
// leaving them out is a choice, checked) and are not the cookie login.
func TestPasswordFlowsAreNotOffered(t *testing.T) {
	var all []string
	for _, f := range (&connector.MetaConnector{}).GetLoginFlows() {
		all = append(all, f.ID)
	}
	if !slices.Equal(all, []string{"facebook", "messenger", "messenger-lite", "messenger-lite-android"}) {
		t.Fatalf("the connector's flows are now %v: decide whether a new one fits a user's own account", all)
	}
}

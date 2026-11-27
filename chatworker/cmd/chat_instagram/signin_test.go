// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/igconnector"
	"go.mau.fi/mautrix-meta/pkg/messagix/loginerrors"
	"maunium.net/go/mautrix/bridgev2"
)

// TestCookieStep: the flow offered begins with one cookies step (the
// contract's WEB step) of the connector's step id, on Instagram's login
// page, with no user agent of the connector's (the web view keeps its
// own), capturing the three cookies the connector requires (sessionid,
// csrftoken, ds_user_id), required, and the five it takes when there (rur,
// shbid, shbts, mid, ig_did), not required, each a cookie of
// instagram.com; its wait-for-URL pattern takes the pages Instagram lands
// on after a sign-in and not the login, two-factor or challenge pages. The
// step asks nothing of the network. WHY: catches a first step the contract
// cannot carry, a capture the connector does not take, an optional cookie
// made required (a sign-in that cannot finish where Instagram set none), a
// domain that would read another site's cookies, and a pattern that closes
// the web view before the user signed in.
func TestCookieStep(t *testing.T) {
	nc, err := newConnector(nil)
	if err != nil {
		t.Fatal(err)
	}
	ic := nc.(*igconnector.IGConnector)
	proc, err := ic.CreateLogin(context.Background(), nil, igconnector.FlowIDCookies)
	if err != nil {
		t.Fatal(err)
	}
	defer proc.Cancel()
	step, err := proc.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if step.Type != bridgev2.LoginStepTypeCookies || step.StepID != igconnector.LoginStepIDCookies || igconnector.LoginStepIDCookies != "fi.mau.meta.cookies" {
		t.Fatalf("step %s %s", step.Type, step.StepID)
	}
	p := step.CookiesParams
	if p == nil || p.URL != "https://www.instagram.com/accounts/login/" || p.UserAgent != "" || p.ExtractJS != "" {
		t.Fatalf("params %+v", p)
	}
	var required, optional []string
	for _, f := range p.Fields {
		if f.Required {
			required = append(required, f.ID)
		} else {
			optional = append(optional, f.ID)
		}
		if len(f.Sources) != 1 || f.Sources[0].Type != bridgev2.LoginCookieTypeCookie || f.Sources[0].Name != f.ID || f.Sources[0].CookieDomain != "instagram.com" {
			t.Errorf("field %+v", f)
		}
	}
	if !slices.Equal(required, []string{"sessionid", "csrftoken", "ds_user_id"}) || !slices.Equal(optional, []string{"rur", "shbid", "shbts", "mid", "ig_did"}) {
		t.Fatalf("captures: required %v, optional %v", required, optional)
	}
	re := regexp.MustCompile(p.WaitForURLPattern)
	for _, u := range []string{
		"https://www.instagram.com/", "https://www.instagram.com/?hl=en", "https://www.instagram.com/direct/inbox/",
		"https://www.instagram.com/direct/t/340282366841710300949128123456789/", "https://www.instagram.com/accounts/onetap/?next=%2F",
	} {
		if !re.MatchString(u) {
			t.Errorf("%s does not end the web view", u)
		}
	}
	for _, u := range []string{
		p.URL, "https://www.instagram.com/accounts/login/two_factor?next=%2F", "https://www.instagram.com/challenge/",
		"https://www.instagram.com/challenge/action/", "https://www.instagram.com/ada.lovelace/", "https://instagram.com/",
	} {
		if re.MatchString(u) {
			t.Errorf("%s ends the web view", u)
		}
	}
}

// TestMissingCookiesFailBeforeInstagram: a submission without all three
// required cookies fails with the connector's missing-cookies error (the
// sign-in's failure code fi.mau.meta_missing_cookies) before any request
// to Instagram: the check comes first, before a client is made. The
// optional cookies are not asked for. A value of a cookie is not in the
// error. WHY: catches a partial capture sent to Instagram, and a cookie
// value leaking into the code or the message the host is given.
func TestMissingCookiesFailBeforeInstagram(t *testing.T) {
	nc, err := newConnector(nil)
	if err != nil {
		t.Fatal(err)
	}
	ic := nc.(*igconnector.IGConnector)
	const secret = "lynx-test-cookie-value"
	for _, captured := range []map[string]string{
		{},
		{"sessionid": secret},
		{"sessionid": secret, "csrftoken": secret},
		{"csrftoken": secret, "ds_user_id": "2", "rur": secret, "mid": secret},
		{"sessionid": "", "csrftoken": secret, "ds_user_id": "2"},
	} {
		proc, err := ic.CreateLogin(context.Background(), nil, igconnector.FlowIDCookies)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := proc.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, err = proc.(bridgev2.LoginProcessCookies).SubmitCookies(context.Background(), captured)
		var re bridgev2.RespError
		if !errors.As(err, &re) || re.ErrCode != loginerrors.MissingCookies.ErrCode || strings.ToLower(re.ErrCode) != "fi.mau.meta_missing_cookies" {
			t.Fatalf("%v: %v, want missing cookies", captured, err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("a cookie value in the error: %v", err)
		}
		for _, optional := range []string{"rur", "shbid", "shbts", "mid", "ig_did"} {
			if strings.Contains(err.Error(), optional) {
				t.Fatalf("an optional cookie asked for: %v", err)
			}
		}
	}
}

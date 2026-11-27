// SPDX-License-Identifier: AGPL-3.0-or-later

package metaworker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

func initWith(settings map[string]string) *contract.Init {
	return &contract.Init{Settings: settings}
}

// TestReadSettings: max_inbox_pages blank or absent is 1; a decimal count
// from 1 to 20 is taken; the backend's keys and the host's common
// device_name are let through; an unknown key, or a count out of range or
// not decimal, refuses the start with bad_settings. WHY: catches a lenient
// parse (a typo listing every chat of the account, or the connector's -1
// slipping through), a bound off by one, a misspelt setting started with
// silently, and a setting every worker may carry refused here (no Meta
// connection could start).
func TestReadSettings(t *testing.T) {
	for _, c := range []struct {
		settings map[string]string
		want     int
	}{
		{nil, 1},
		{map[string]string{SettingInboxPages: ""}, 1},
		{map[string]string{SettingInboxPages: "  "}, 1},
		{map[string]string{SettingInboxPages: "1"}, 1},
		{map[string]string{SettingInboxPages: " 3 "}, 3},
		{map[string]string{SettingInboxPages: "007"}, 7},
		{map[string]string{SettingInboxPages: "20"}, 20},
		{map[string]string{
			"max_initial_messages": "20", "max_catchup_messages": "0", "spool_budget_bytes": "1000",
			"log_level": "debug", "dev_plain_store": "1", "device_name": "Lynx",
		}, 1},
	} {
		s, err := ReadSettings(initWith(c.settings))
		if err != nil || s.InboxPages != c.want {
			t.Errorf("%v: %+v %v, want %d", c.settings, s, err, c.want)
		}
	}
	for _, bad := range []map[string]string{
		{SettingInboxPages: "0"},
		{SettingInboxPages: "-1"},
		{SettingInboxPages: "21"},
		{SettingInboxPages: "+2"},
		{SettingInboxPages: "2 0"},
		{SettingInboxPages: "1e1"},
		{SettingInboxPages: "0x2"},
		{SettingInboxPages: "all"},
		{SettingInboxPages: "99999999999999999999"},
		{SettingInboxPages: "٣"},
		{"max_initial_conversations": "15"},
		{"maxinboxpages": "2"},
		{"api_id": "1"},
		{"MAX_INBOX_PAGES": "2"},
	} {
		_, err := ReadSettings(initWith(bad))
		var se *backend.SettingsError
		if !errors.As(err, &se) || se.Code != "bad_settings" {
			t.Errorf("%v: %v, want bad_settings", bad, err)
		}
	}
}

// TestSendKey: the transaction id is a function of the key, never a number
// (so the connectors' getOTID makes a fresh, time-based offline threading
// id for every send), of the prefix and 32 lower-case hex digits; one
// already of that form is kept, anything else (one with upper-case digits,
// a number) is hashed. WHY: catches a key turned into a number (every
// message sent with a made-up time as its offline threading id), a mapping
// that is not a function of the key (the backend could not find a stored
// message's key again, send.go sendKeyForTxn), and two keys sharing an id.
func TestSendKey(t *testing.T) {
	keys := []string{"", "k1", "k2", "1", "42", "9223372036854775807", "lynx-", "send key with spaces", "ü"}
	seen := map[networkid.RawTransactionID]string{}
	for _, k := range keys {
		id := SendKey(k)
		if id != SendKey(k) {
			t.Fatalf("%q: not a function of the key", k)
		}
		if !isSendKeyID(string(id)) {
			t.Fatalf("%q: %q is not of the form", k, id)
		}
		if _, err := strconv.ParseInt(string(id), 10, 64); err == nil {
			t.Fatalf("%q: %q parses as a number (the connector would take it for an offline threading id)", k, id)
		}
		if other, dup := seen[id]; dup {
			t.Fatalf("%q and %q share %q", k, other, id)
		}
		seen[id] = k
		if SendKey(string(id)) != id {
			t.Fatalf("%q: an id of the form is not kept", id)
		}
	}
	kept := "lynx-" + strings.Repeat("ab", 16)
	if SendKey(kept) != networkid.RawTransactionID(kept) {
		t.Fatal("an id of the form was hashed")
	}
	for _, notKept := range []string{"lynx-" + strings.Repeat("AB", 16), "lynx-" + strings.Repeat("a", 31), "lynx-" + strings.Repeat("a", 33), "lynx-" + strings.Repeat("g", 32)} {
		if SendKey(notKept) == networkid.RawTransactionID(notKept) {
			t.Fatalf("%q kept as it is", notKept)
		}
	}
}

// TestSelfPersonID: the account's own person id is its login id when that
// is a FBID as metaid writes one, which is the sender id both connectors
// give the account's own events; anything else gives "" (the framework's
// default). WHY: catches a self id that no sender carries (the account's own
// messages would have a sender with no is_self PersonInfo), and one made up
// from a login id the connectors would not make.
func TestSelfPersonID(t *testing.T) {
	for _, c := range []struct {
		id   networkid.UserLoginID
		want networkid.UserID
	}{
		{metaid.MakeUserLoginID(100004567890123), "100004567890123"},
		{"17841400000000001", "17841400000000001"},
		{"1", "1"},
		{"", ""},
		{"0", ""},
		{"-5", ""},
		{"+5", ""},
		{"007", ""},
		{"12a", ""},
		{"99999999999999999999", ""},
	} {
		login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: c.id}}
		if got := SelfPersonID(login); got != c.want {
			t.Errorf("%q: %q, want %q", c.id, got, c.want)
		}
	}
	const fbid = 100004567890123
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: metaid.MakeUserLoginID(fbid)}}
	if got, want := SelfPersonID(login), metaid.MakeUserID(fbid); got != want {
		t.Fatalf("self %q, the connectors' sender %q", got, want)
	}
	if SelfPersonID(nil) != "" || SelfPersonID(&bridgev2.UserLogin{}) != "" {
		t.Fatal("no login, an id")
	}
}

// TestEndRefused: a refused login is ended nowhere and changed in nothing:
// the hook answers that there was nothing to end, and the login's cookies
// (the user's own web session) are left as they were. WHY: catches a
// refusal that would sign the user's browser session out, or a hook that
// reports a session ended that it never touched.
func TestEndRefused(t *testing.T) {
	c := &cookies.Cookies{Platform: types.Instagram}
	c.UpdateValues(map[cookies.MetaCookieName]string{cookies.IGCookieSessionID: "s", cookies.IGCookieDSUserID: "2", cookies.IGCookieCSRFToken: "t"})
	before := c.GetAll()
	meta := &metaid.UserLoginMetadata{Platform: types.Instagram, Cookies: c}
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "17841400000000002", Metadata: meta}}
	if err := EndRefused(context.Background(), nil, login); !errors.Is(err, backend.ErrNothingToEnd) {
		t.Fatalf("end: %v, want ErrNothingToEnd", err)
	}
	if login.Metadata != any(meta) || meta.Cookies != c || !maps.Equal(c.GetAll(), before) || login.Client != nil {
		t.Fatal("the refused login was changed")
	}
	if err := EndRefused(context.Background(), nil, nil); !errors.Is(err, backend.ErrNothingToEnd) {
		t.Fatalf("no login: %v", err)
	}
}

// TestRefusedLoginErrorReachesTheBackend: the connectors wrap the
// framework's NewLogin error with %w ("failed to save new login: %w",
// loginWithCookies in both), so the gate's refusal is still the error the
// backend finds (errCode: principal_mismatch, or other_account_kept). WHY:
// pins the one property of the connectors the worker's refusal rests on
// with no DropRefused hook: a wrap without %w would fail the sign-in as
// submit_failed, and the host would not learn that another account was
// reached.
func TestRefusedLoginErrorReachesTheBackend(t *testing.T) {
	for _, gate := range []error{backend.ErrOtherAccount, backend.ErrOtherAccountKept} {
		err := fmt.Errorf("failed to save new login: %w", gate)
		if !errors.Is(err, gate) {
			t.Fatalf("%v lost in the wrap", gate)
		}
	}
}

// TestRateLimitWait: Meta's 429 is the rate limit with no wait, also
// wrapped as the connectors wrap their errors; an unexpected status that is
// not 429, or any other error, is not. WHY: catches a history fetch Meta
// throttled answered as a plain failure (the host would retry at once), and
// any server error taken for a rate limit.
func TestRateLimitWait(t *testing.T) {
	for _, c := range []struct {
		err error
		ok  bool
	}{
		{httpclient.ErrRateLimited, true},
		{fmt.Errorf("failed to fetch messages: %w", httpclient.ErrRateLimited), true},
		{fmt.Errorf("%w: %w", errors.New("rate limited by Facebook"), httpclient.ErrRateLimited), true},
		{httpclient.ErrUnexpectedError, false},
		{httpclient.ErrRequestFailed, false},
		{errors.New("429"), false},
		{nil, false},
	} {
		wait, ok := RateLimitWait(c.err)
		if ok != c.ok || wait != 0 {
			t.Errorf("%v: %v %v, want 0 %v", c.err, wait, ok, c.ok)
		}
	}
}

// TestHooks: the hooks both workers give the backend, with the network's
// own; the network's lists are copied. WHY: catches a hook left unset or set
// to another function, a hook set that the workers do not use (DropRefused
// would make the backend's refusal path the WhatsApp one; ShowExpiresIn has
// nothing to time), and a worker's flow list shared with the backend's.
func TestHooks(t *testing.T) {
	flows := []string{"a", "b"}
	codes := []status.BridgeStateErrorCode{"x-ended"}
	h := Hooks(Network{Flows: flows, EndedCodes: codes, IdentifierScheme: "scheme"})
	same := func(a, b any) bool { return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer() }
	if h.SelfPersonID == nil || !same(h.SelfPersonID, SelfPersonID) {
		t.Error("SelfPersonID")
	}
	if h.SendKeyFormat == nil || !same(h.SendKeyFormat, SendKey) {
		t.Error("SendKeyFormat")
	}
	if h.EndRefused == nil || !same(h.EndRefused, EndRefused) {
		t.Error("EndRefused")
	}
	if h.RateLimitWait == nil || !same(h.RateLimitWait, RateLimitWait) {
		t.Error("RateLimitWait")
	}
	if h.DropRefused != nil || h.EndRefusedSession || h.IsChannel != nil || h.ShowExpiresIn != nil ||
		h.PrepareLogins != nil || h.WatchRateLimit != nil || h.OriginalMessageID != nil {
		t.Error("a hook set that the workers do not use")
	}
	if h.IdentifierScheme != "scheme" || !slices.Equal(h.SignInFlows, flows) || !slices.Equal(h.EndedCodes, codes) {
		t.Errorf("hooks %+v", h)
	}
	flows[0], codes[0] = "changed", "changed"
	if h.SignInFlows[0] != "a" || h.EndedCodes[0] != "x-ended" {
		t.Error("the network's lists are shared, not copied")
	}
}

// TestUpstream: Hello's upstream versions are the framework's, the
// connectors' module's and the HTTP client fork's, with the worker's own.
func TestUpstream(t *testing.T) {
	up := Upstream(map[string]string{"go.mau.fi/whatsmeow": "v1"})
	want := map[string]string{
		"maunium.net/go/mautrix":   MautrixVersion,
		"go.mau.fi/mautrix-meta":   MetaVersion,
		"github.com/beeper/req/v3": ReqVersion,
		"go.mau.fi/whatsmeow":      "v1",
	}
	if !maps.Equal(up, want) {
		t.Fatalf("upstream %v, want %v", up, want)
	}
	if len(Upstream(nil)) != 3 {
		t.Fatal("upstream without extras")
	}
}

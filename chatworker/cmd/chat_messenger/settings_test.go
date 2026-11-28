// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/connector"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

func initWith(settings map[string]string) *contract.Init {
	return &contract.Init{Settings: settings}
}

// TestNewConnectorConfig: the connector's config is its example with the
// worker's changes: max_inbox_pages reaches it as the pages after the first
// (never the connector's -1, every chat), no proxy or Tor, no Marketplace
// space, no restore of the encrypted history (no PIN step, no upload of
// sent messages), and the display name template that names a person by
// their name, else their username, with no suffix. The connector's own
// reconnect and page-delay defaults are kept. WHY: catches the inbox
// setting off by one or lost (every chat of the account listed at sign-in,
// each importing history), the encrypted history restore left on (a PIN
// asked after the cookies, and every message sent uploaded to Meta's
// backup), and a template change that is not parsed.
func TestNewConnectorConfig(t *testing.T) {
	for _, c := range []struct {
		settings map[string]string
		batches  int
	}{
		{nil, 0},
		{map[string]string{"max_inbox_pages": "1"}, 0},
		{map[string]string{"max_inbox_pages": "4"}, 3},
		{map[string]string{"max_inbox_pages": "20", "device_name": "ignored", "max_initial_messages": "20"}, 19},
	} {
		nc, err := newConnector(initWith(c.settings))
		if err != nil {
			t.Fatalf("%v: %v", c.settings, err)
		}
		mc, ok := nc.(*connector.MetaConnector)
		if !ok {
			t.Fatalf("connector %T", nc)
		}
		cfg := &mc.Config
		if cfg.ThreadBackfill.BatchCount != c.batches {
			t.Errorf("%v: batch count %d, want %d", c.settings, cfg.ThreadBackfill.BatchCount, c.batches)
		}
		if cfg.Proxy != "" || cfg.Tor || cfg.GetProxyFrom != "" || cfg.ProxyMedia || cfg.ProxyE2EE ||
			cfg.MarketplaceSpace || cfg.E2EEBackfill || cfg.SendPresenceOnTyping || cfg.LogRedactedLoginResponses {
			t.Errorf("%v: config %+v", c.settings, *cfg)
		}
		if cfg.ThreadBackfill.BatchDelay != 2*time.Second || cfg.MinFullReconnectIntervalSeconds != 3600 ||
			cfg.ForceRefreshIntervalSeconds != 72000 || cfg.CacheConnectionState {
			t.Errorf("%v: the connector's defaults changed: %+v", c.settings, *cfg)
		}
	}
	nc, err := newConnector(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &nc.(*connector.MetaConnector).Config
	for _, c := range []struct {
		p    connector.DisplaynameParams
		want string
	}{
		{connector.DisplaynameParams{DisplayName: "Ada Lovelace", Username: "ada", ID: 4}, "Ada Lovelace"},
		{connector.DisplaynameParams{Username: "ada", ID: 4}, "ada"},
		{connector.DisplaynameParams{ID: 4}, "Unknown user"},
	} {
		if got := cfg.FormatDisplayname(c.p); got != c.want {
			t.Errorf("%+v: name %q, want %q", c.p, got, c.want)
		}
	}
}

// TestNewConnectorRefusesBadSettings: a max_inbox_pages out of range or
// not a count, and an unknown key, refuse the start with bad_settings
// before any connector is made. WHY: catches the worker making a connector
// with a default in place of a value the host sent wrong.
func TestNewConnectorRefusesBadSettings(t *testing.T) {
	for _, bad := range []map[string]string{
		{"max_inbox_pages": "0"},
		{"max_inbox_pages": "-1"},
		{"max_inbox_pages": "21"},
		{"max_inbox_pages": "all"},
		{"max_initial_conversations": "15"},
		{"mode": "facebook"},
	} {
		nc, err := newConnector(initWith(bad))
		var se *backend.SettingsError
		if nc != nil || !errors.As(err, &se) || se.Code != "bad_settings" {
			t.Errorf("%v: %v %v, want bad_settings", bad, nc, err)
		}
	}
}

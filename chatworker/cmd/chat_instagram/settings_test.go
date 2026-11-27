// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/igconnector"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

func initWith(settings map[string]string) *contract.Init {
	return &contract.Init{Settings: settings}
}

// TestNewConnectorConfig: the connector's config is its example with the
// worker's changes: max_inbox_pages reaches it as the pages of the inbox,
// the first included (1: the chat backfill stays off), no proxy, no typing
// connections, and the display name template that names a person by their
// name, else their username, with no suffix. The connector's own defaults
// are kept where the worker does not set them (no media of reels and
// stories fetched in history, the cached connection state, the reconnect
// intervals, two seconds between pages). WHY: catches the inbox setting
// off by one or lost (the connector's chat backfill on with -1 would list
// every chat of the account, each importing history), the two extra
// connections of typing left on, and a template change that is not parsed.
func TestNewConnectorConfig(t *testing.T) {
	for _, c := range []struct {
		settings map[string]string
		batches  int
		backfill bool
	}{
		{nil, 1, false},
		{map[string]string{"max_inbox_pages": "1"}, 1, false},
		{map[string]string{"max_inbox_pages": "2"}, 2, true},
		{map[string]string{"max_inbox_pages": "20", "device_name": "ignored", "max_initial_messages": "20"}, 20, true},
	} {
		nc, err := newConnector(initWith(c.settings))
		if err != nil {
			t.Fatalf("%v: %v", c.settings, err)
		}
		ic, ok := nc.(*igconnector.IGConnector)
		if !ok {
			t.Fatalf("connector %T", nc)
		}
		cfg := &ic.Config
		if cfg.ThreadBackfill.BatchCount != c.batches || cfg.ThreadBackfill.Enabled() != c.backfill || cfg.ThreadBackfill.MaxConcurrent != 0 {
			t.Errorf("%v: thread backfill %+v, want %d pages", c.settings, cfg.ThreadBackfill, c.batches)
		}
		if cfg.Proxy != "" || cfg.GetProxyFrom != "" || cfg.ProxyMedia || !cfg.DisableTyping || cfg.LogRedactedLoginResponses {
			t.Errorf("%v: config %+v", c.settings, *cfg)
		}
		if !cfg.DisableXMABackfill || cfg.DisableXMAAlways || !cfg.CacheConnectionState || cfg.ThreadBackfill.BatchDelay != 2*time.Second ||
			cfg.MinFullReconnectIntervalSeconds != 3600 || cfg.ForceRefreshIntervalSeconds != 72000 {
			t.Errorf("%v: the connector's defaults changed: %+v", c.settings, *cfg)
		}
	}
	nc, err := newConnector(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &nc.(*igconnector.IGConnector).Config
	for _, c := range []struct {
		p    igconnector.DisplaynameParams
		want string
	}{
		{igconnector.DisplaynameParams{DisplayName: "Ada Lovelace", Username: "ada", ID: 4}, "Ada Lovelace"},
		{igconnector.DisplaynameParams{Username: "ada", ID: 4}, "ada"},
		{igconnector.DisplaynameParams{ID: 4}, "Unknown user"},
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
		{"disable_typing": "false"},
	} {
		nc, err := newConnector(initWith(bad))
		var se *backend.SettingsError
		if nc != nil || !errors.As(err, &se) || se.Code != "bad_settings" {
			t.Errorf("%v: %v %v, want bad_settings", bad, nc, err)
		}
	}
}

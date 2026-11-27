// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

func initWith(settings map[string]string) *contract.Init {
	return &contract.Init{Settings: settings}
}

func isBadSettings(err error) bool {
	var se *backend.SettingsError
	return errors.As(err, &se) && se.Code == "bad_settings"
}

// TestReadSettings: no settings is "Lynx" and 25 conversations; a
// device_name is trimmed and taken, a blank one is the default; the
// backend's own keys are let through for the backend to read; an unknown
// key, a device_name that is too long, holds a control character or is
// not UTF-8, and a bad count refuse the start. WHY: catches a lenient read
// (a misspelt key doing nothing unseen), a name the phone would show
// garbled, and a default that drifted from the connector's.
func TestReadSettings(t *testing.T) {
	s, err := readSettings(initWith(nil))
	if err != nil || s.deviceName != "Lynx" || s.conversations != 25 {
		t.Fatalf("defaults: %+v %v", s, err)
	}
	s, err = readSettings(initWith(map[string]string{
		"device_name": "  Lynx on the desk  ", "max_initial_conversations": "40",
		"max_initial_messages": "20", "max_catchup_messages": "0", "spool_budget_bytes": "1000000",
		"log_level": "debug", "dev_plain_store": "0",
	}))
	if err != nil || s.deviceName != "Lynx on the desk" || s.conversations != 40 {
		t.Fatalf("settings: %+v %v", s, err)
	}
	if s, err := readSettings(initWith(map[string]string{"device_name": "   "})); err != nil || s.deviceName != "Lynx" {
		t.Fatalf("blank name: %+v %v", s, err)
	}
	if s, err := readSettings(initWith(map[string]string{"device_name": strings.Repeat("é", 50)})); err != nil || s.deviceName != strings.Repeat("é", 50) {
		t.Fatalf("100-byte name: %+v %v", s, err)
	}
	for name, bad := range map[string]map[string]string{
		"unknown key":    {"device-name": "Lynx"},
		"other worker's": {"sync_create_limit": "10"},
		"long name":      {"device_name": strings.Repeat("x", maxDeviceNameBytes+1)},
		"newline":        {"device_name": "Lynx\nphone"},
		"escape":         {"device_name": "Lynx\x1b[31m"},
		"not UTF-8":      {"device_name": "Lynx\xff"},
		"bad count":      {"max_initial_conversations": "0"},
	} {
		if _, err := readSettings(initWith(bad)); !isBadSettings(err) {
			t.Errorf("%s: %v, want bad_settings", name, err)
		}
	}
}

// TestReadConversationCount: blank or absent is 25; a decimal count from 1
// to 100 is taken as it is; anything else refuses the start with
// bad_settings. WHY: catches a lenient parse (a typo making the connector
// list no conversation, which is also how it finds what it missed), 0
// slipping through, and a bound off by one.
func TestReadConversationCount(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{
		{"", 25},
		{"  ", 25},
		{"1", 1},
		{" 40 ", 40},
		{"007", 7},
		{"100", 100},
	} {
		got, err := readConversationCount(c.in)
		if err != nil || got != c.want {
			t.Errorf("%q: %d %v, want %d", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"0", "000", "-1", "101", "+5", "1e3", "4 0", "many", "0x10", "99999999999999999999", "٣"} {
		if _, err := readConversationCount(bad); !isBadSettings(err) {
			t.Errorf("%q: %v, want bad_settings", bad, err)
		}
	}
}

// TestNewConnectorConfig: the connector's config is its example with the
// worker's changes: the device name the phone shows, a device kind and
// browser the connector knows (it logs and keeps its own for one it does
// not, connector.go Init), the conversation count, the deterministic id
// prefix (nothing is written before the gate), no aggressive reconnect;
// the connector's other defaults are kept, and its name template runs.
// WHY: catches the device name not reaching the connector (the phone would
// show "mautrix-gmessages"), the id prefix left to a table row written
// before the gate, a device kind the phone does not take, and a template
// left unparsed (the connector would print an empty name).
func TestNewConnectorConfig(t *testing.T) {
	nc, err := newConnector(initWith(map[string]string{"device_name": "Lynx test", "max_initial_conversations": "40"}))
	if err != nil {
		t.Fatal(err)
	}
	n, ok := nc.(*gmNetwork)
	if !ok || n.GMConnector == nil {
		t.Fatalf("connector %T", nc)
	}
	c := &n.GMConnector.Config
	if c.DeviceMeta.OS != "Lynx test" || c.InitialChatSyncCount != 40 || !c.DeterministicIDPrefix || c.AggressiveReconnect || c.HackyStallDetect {
		t.Fatalf("config %+v", *c)
	}
	if _, ok := gmproto.BrowserType_value[c.DeviceMeta.Browser]; !ok || c.DeviceMeta.Browser != "OTHER" {
		t.Fatalf("browser %q", c.DeviceMeta.Browser)
	}
	if _, ok := gmproto.DeviceType_value[c.DeviceMeta.Type]; !ok || c.DeviceMeta.Type != "TABLET" {
		t.Fatalf("device type %q", c.DeviceMeta.Type)
	}
	// The connector's own defaults where the worker sets nothing.
	if c.PingInterval != 20*time.Minute || c.AlertTimeoutCount != 4 || c.DataReceiveCheckInterval != 2*time.Hour+55*time.Minute {
		t.Fatalf("defaults %v %d %v", c.PingInterval, c.AlertTimeoutCount, c.DataReceiveCheckInterval)
	}
	if got := c.FormatDisplayname("+1 555-0100", "Ada Lovelace", "Ada"); got != "Ada Lovelace" {
		t.Fatalf("name %q", got)
	}
	if got := c.FormatDisplayname("+1 555-0100", "", ""); got != "+1 555-0100" {
		t.Fatalf("name of a number %q", got)
	}
	nc, err = newConnector(initWith(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c := nc.(*gmNetwork).GMConnector.Config; c.DeviceMeta.OS != defaultDeviceName || c.InitialChatSyncCount != defaultInitialConversations {
		t.Fatalf("default config: %q %d", c.DeviceMeta.OS, c.InitialChatSyncCount)
	}
	if _, err := newConnector(initWith(map[string]string{"max_initial_conversations": "all"})); !isBadSettings(err) {
		t.Fatalf("a bad count: %v", err)
	}
	if _, err := newConnector(initWith(map[string]string{"bridge_id": "x"})); !isBadSettings(err) {
		t.Fatalf("an unknown key: %v", err)
	}
}

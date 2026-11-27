// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"os"
	"testing"

	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"go.mau.fi/mautrix-gvoice/pkg/libgv/gvproto"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

func initWith(settings map[string]string) *contract.Init {
	return &contract.Init{Settings: settings}
}

// keepPath restores the test process's PATH after the test: newConnector
// sets it (electron.go useElectron).
func keepPath(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", os.Getenv("PATH"))
}

func wantSettingsError(t *testing.T, what any, err error, code string) {
	t.Helper()
	var se *backend.SettingsError
	if !errors.As(err, &se) || se.Code != code {
		t.Errorf("%v: %v, want %s", what, err, code)
	}
}

// TestReadConversationCount: blank or absent is 20 (the connector's own
// default, one page of threads); a decimal count from 0 to 1000 is taken as
// it is; anything else refuses the start with bad_settings. WHY: catches a
// lenient parse (a typo making every thread a room, or none), the
// connector's -1 (every thread of the account) slipping through, and a
// bound off by one.
func TestReadConversationCount(t *testing.T) {
	for _, c := range []struct {
		in   string
		set  bool
		want int
	}{
		{"", false, 20},
		{"", true, 20},
		{"  ", true, 20},
		{"0", true, 0},
		{"1", true, 1},
		{" 40 ", true, 40},
		{"007", true, 7},
		{"1000", true, 1000},
	} {
		s := map[string]string{}
		if c.set {
			s[settingMaxInitialConversations] = c.in
		}
		got, err := readConversationCount(initWith(s))
		if err != nil || got != c.want {
			t.Errorf("%q: %d %v, want %d", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"-1", "1001", "+5", "1e3", "4 0", "many", "0x10", "99999999999999999999", "٣"} {
		_, err := readConversationCount(initWith(map[string]string{settingMaxInitialConversations: bad}))
		wantSettingsError(t, bad, err, "bad_settings")
	}
}

// TestReadSettings: the worker's two keys, the backend's own and the
// host's common device_name (accepted, not read) are let through; any other
// key refuses the start with bad_settings, the connector's own option names
// and other workers' settings among them. WHY: catches a misspelt setting
// started with silently, and a setting the backend reads, or one the host
// gives every chat worker, refused here (no connection could start).
func TestReadSettings(t *testing.T) {
	s, err := readSettings(initWith(map[string]string{
		"max_initial_conversations": "40", "electron_path": "",
		"max_initial_messages": "20", "max_catchup_messages": "0", "spool_budget_bytes": "1000",
		"log_level": "debug", "dev_plain_store": "1", "device_name": "Lynx",
	}))
	if err != nil || s.conversations != 40 || s.electron != "" {
		t.Fatalf("all known keys: %+v %v", s, err)
	}
	if s, err := readSettings(initWith(nil)); err != nil || s.conversations != 20 || s.electron != "" {
		t.Fatalf("no settings: %+v %v", s, err)
	}
	if s, err := readSettings(initWith(map[string]string{"device_name": "Common \x00 name"})); err != nil || s.conversations != 20 {
		t.Fatalf("device_name alone, any value (not read): %+v %v", s, err)
	}
	for _, bad := range []map[string]string{
		{"initial_thread_sync_count": "20"},
		{"displayname_template": "{{.Name}}"},
		{"Device_Name": "Lynx"},
		{"electron": "/usr/bin/electron"},
		{"Electron_Path": "/usr/bin/electron"},
		{"max_initial_conversations": "-1"},
		{"electron_path": "electron"},
	} {
		_, err := readSettings(initWith(bad))
		wantSettingsError(t, bad, err, "bad_settings")
	}
}

// TestNewConnectorConfig: the connector's config is its example with the
// thread count set, and its display name template is the example's, parsed
// (a contact's name, else the name Google gives, the number). WHY: catches
// the count not reaching the connector, and a template left unparsed (the
// connector would panic naming a person).
func TestNewConnectorConfig(t *testing.T) {
	keepPath(t)
	nc, err := newConnector(initWith(map[string]string{settingMaxInitialConversations: "40"}))
	if err != nil {
		t.Fatal(err)
	}
	n, ok := nc.(*gvNetwork)
	if !ok || n.GVConnector == nil {
		t.Fatalf("connector %T", nc)
	}
	c := n.GVConnector.Config
	if c.InitialThreadSyncCount != 40 {
		t.Fatalf("thread sync count %d", c.InitialThreadSyncCount)
	}
	info := &gvproto.Contact{Name: mineNumber, PhoneNumber: mineNumber}
	if got := c.FormatDisplayname(info, &connector.ProcessedContact{Name: "Ada L"}); got != "Ada L" {
		t.Fatalf("contact's name %q", got)
	}
	if got := c.FormatDisplayname(info, &connector.ProcessedContact{}); got != mineNumber {
		t.Fatalf("no contact name %q", got)
	}
	nc, err = newConnector(initWith(nil))
	if err != nil || nc.(*gvNetwork).GVConnector.Config.InitialThreadSyncCount != 20 {
		t.Fatalf("default: %v", err)
	}
	if _, err := newConnector(initWith(map[string]string{settingMaxInitialConversations: "all"})); err == nil {
		t.Fatal("a bad count was taken")
	}
	if _, err := newConnector(initWith(map[string]string{"device_name": "Lynx"})); err != nil {
		t.Fatalf("a common device_name refused the start: %v", err)
	}
}

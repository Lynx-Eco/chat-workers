// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"strings"
	"testing"

	"go.mau.fi/mautrix-signal/pkg/connector"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// TestSettings: device_name is taken trimmed, a blank one is "Lynx", and
// the backend's own keys are let through; an unknown key, or a device name
// that is not a short line of text, refuses the start with bad_settings.
// The connector is configured with the name and without the example's
// avatar address on maunium.net. WHY: catches a misspelt setting started
// with silently (a "devicename" the phone would never show), a name with a
// newline or too long for Signal to take sent at the link, and a setting
// the backend reads refused here (no Signal connection could start).
func TestSettings(t *testing.T) {
	for _, c := range []struct {
		settings map[string]string
		want     string
	}{
		{nil, "Lynx"},
		{map[string]string{"device_name": ""}, "Lynx"},
		{map[string]string{"device_name": "  "}, "Lynx"},
		{map[string]string{"device_name": " Kitchen Mac "}, "Kitchen Mac"},
		{map[string]string{"device_name": "Lynx — ünïcode"}, "Lynx — ünïcode"},
		{map[string]string{"device_name": strings.Repeat("n", maxDeviceNameBytes)}, strings.Repeat("n", maxDeviceNameBytes)},
		{map[string]string{
			"max_initial_messages": "20", "max_catchup_messages": "0", "spool_budget_bytes": "1000",
			"log_level": "debug", "dev_plain_store": "1",
		}, "Lynx"},
	} {
		n, err := newConnector(&contract.Init{Settings: c.settings})
		if err != nil {
			t.Fatalf("%v: %v", c.settings, err)
		}
		cfg := n.(*connector.SignalConnector).Config
		if cfg.DeviceName != c.want {
			t.Errorf("%v: device name %q, want %q", c.settings, cfg.DeviceName, c.want)
		}
		if cfg.NoteToSelfAvatar != "" || cfg.UseContactAvatars || cfg.IdentityChangeNotices {
			t.Errorf("%v: config %+v", c.settings, cfg)
		}
	}
	for _, bad := range []map[string]string{
		{"devicename": "Lynx"},
		{"max_initial_conversations": "15"},
		{"api_id": "1"},
		{"device_name": "two\nlines"},
		{"device_name": "tab\there"},
		{"device_name": "\x00"},
		{"device_name": string([]byte{0xff, 0xfe})},
		{"device_name": strings.Repeat("n", maxDeviceNameBytes+1)},
	} {
		_, err := newConnector(&contract.Init{Settings: bad})
		var se *backend.SettingsError
		if !errors.As(err, &se) || se.Code != "bad_settings" {
			t.Errorf("%q: %v, want bad_settings", bad, err)
		}
	}
}

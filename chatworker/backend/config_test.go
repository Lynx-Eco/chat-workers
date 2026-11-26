// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"errors"
	"testing"

	"lynx.invalid/ingest/chatworker/contract"
)

// TestReadHistory: with no setting a new connection imports each new chat's
// recent messages (the default is not 0: a Telegram sign-in once landed
// every chat empty because nothing sent FetchHistory), a host can turn
// either import off with 0, and a value that is not a count up to the bound
// refuses the start instead of turning history off or asking a network for
// thousands of messages per chat unseen.
func TestReadHistory(t *testing.T) {
	read := func(settings map[string]string) (History, error) {
		return ReadHistory(&contract.Init{Settings: settings})
	}
	h, err := read(nil)
	if err != nil || h.Initial != DefaultInitialMessages || h.Catchup != DefaultCatchupMessages || h.Initial == 0 {
		t.Fatalf("defaults: %+v %v", h, err)
	}
	h, err = read(map[string]string{SettingInitialMessages: "0", SettingCatchupMessages: " 0 "})
	if err != nil || h.Initial != 0 || h.Catchup != 0 {
		t.Fatalf("off: %+v %v", h, err)
	}
	h, err = read(map[string]string{SettingInitialMessages: "500", SettingCatchupMessages: "7"})
	if err != nil || h.Initial != MaxHistorySetting || h.Catchup != 7 {
		t.Fatalf("set: %+v %v", h, err)
	}
	if c := BridgeConfig(h).Backfill; !c.Enabled || c.MaxInitialMessages != 500 || c.MaxCatchupMessages != 7 || c.Queue.Enabled {
		t.Fatalf("the framework's backfill config: %+v", c)
	}
	for _, bad := range []string{"501", "-1", "+5", "ten", "1e3", "99999999999999999999"} {
		for _, key := range []string{SettingInitialMessages, SettingCatchupMessages} {
			_, err := read(map[string]string{key: bad})
			var se *SettingsError
			if !errors.As(err, &se) || se.Code != "bad_settings" {
				t.Errorf("%s=%q: %v, want bad_settings", key, bad, err)
			}
		}
	}
}

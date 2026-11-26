// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"gopkg.in/yaml.v3"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// telegramConnector is newConnector with dummy api settings and these.
func telegramConnector(t *testing.T, settings map[string]string) (*connector.TelegramConnector, error) {
	t.Helper()
	s := map[string]string{"api_id": "12345", "api_hash": dummyHash}
	for k, v := range settings {
		s[k] = v
	}
	nc, err := newConnector(&contract.Init{Settings: s})
	if err != nil {
		return nil, err
	}
	tc, ok := nc.(*connector.TelegramConnector)
	if !ok {
		t.Fatalf("connector %T", nc)
	}
	return tc, nil
}

// TestSyncLimits: sync_update_limit and sync_create_limit (a connection's
// own settings, or the host's) reach the connector's config before it
// starts; absent, the connector's defaults stand (100 dialogs looked at, 15
// rooms made: today's sign-in); the update limit is raised to a higher
// create limit, since rooms are made only among the dialogs looked at; and a
// value that is not a count up to 1000 refuses the start with bad_settings,
// the connector's -1 (no limit: thousands of requests in a burst on a large
// account) among them.
func TestSyncLimits(t *testing.T) {
	example := &connector.TelegramConnector{}
	raw, data, _ := example.GetConfig()
	if err := yaml.Unmarshal([]byte(raw), data); err != nil {
		t.Fatal(err)
	}
	if example.Config.Sync.UpdateLimit != 100 || example.Config.Sync.CreateLimit != 15 {
		t.Fatalf("the connector's defaults moved: update %d, create %d (go/README.md says 100 and 15)",
			example.Config.Sync.UpdateLimit, example.Config.Sync.CreateLimit)
	}
	for _, c := range []struct {
		settings       map[string]string
		update, create int
	}{
		{nil, 100, 15},
		{map[string]string{settingSyncCreateLimit: "", settingSyncUpdateLimit: " "}, 100, 15},
		{map[string]string{settingSyncCreateLimit: "200"}, 200, 200},
		{map[string]string{settingSyncCreateLimit: "200", settingSyncUpdateLimit: "300"}, 300, 200},
		{map[string]string{settingSyncCreateLimit: "1000", settingSyncUpdateLimit: "10"}, 1000, 1000},
		{map[string]string{settingSyncUpdateLimit: "50"}, 50, 15},
		{map[string]string{settingSyncUpdateLimit: "5"}, 15, 15},
		{map[string]string{settingSyncCreateLimit: " 1000 "}, 1000, 1000},
		{map[string]string{settingSyncCreateLimit: "0"}, 100, 0},
		{map[string]string{settingSyncCreateLimit: "0", settingSyncUpdateLimit: "0"}, 0, 0},
	} {
		tc, err := telegramConnector(t, c.settings)
		if err != nil {
			t.Fatalf("%v: %v", c.settings, err)
		}
		if got := tc.Config.Sync; got.UpdateLimit != c.update || got.CreateLimit != c.create {
			t.Errorf("%v: update %d, create %d; want %d, %d", c.settings, got.UpdateLimit, got.CreateLimit, c.update, c.create)
		}
		// The rest of the config is what it was.
		if tc.Config.APIID != 12345 || tc.Config.DeviceInfo.DeviceModel != "Lynx" || tc.Config.Takeout.DialogSync {
			t.Errorf("%v: config %+v", c.settings, tc.Config)
		}
	}
	for _, bad := range []string{"-1", "1001", "+5", "ten", "1e3", "0x10", "99999999999999999999", "1 0"} {
		for _, key := range []string{settingSyncCreateLimit, settingSyncUpdateLimit} {
			_, err := telegramConnector(t, map[string]string{key: bad})
			var se *backend.SettingsError
			if !errors.As(err, &se) || se.Code != "bad_settings" {
				t.Errorf("%s=%q: %v, want bad_settings", key, bad, err)
			}
		}
	}
}

// TestRefusesBadSyncLimit: the worker started with a sync limit out of
// bounds reports ERROR bad_settings, as for any other setting, and refuses
// commands; it never reaches the network.
func TestRefusesBadSyncLimit(t *testing.T) {
	s, p, _ := run(t, map[string]string{"api_id": "12345", "api_hash": dummyHash, settingSyncCreateLimit: "-1"})
	cs := waitState(t, s, contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR)
	if cs.GetCode() != "bad_settings" {
		t.Fatalf("code %q", cs.GetCode())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, err := s.Command(ctx, &contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if err != nil || r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "bad_settings" {
		t.Fatalf("command while unconfigured: %v %v", r, err)
	}
	if err := s.Shutdown(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if code, err := p.Wait(10 * time.Second); err != nil || code != 0 {
		t.Fatalf("exit %d %v", code, err)
	}
}

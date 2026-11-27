// SPDX-License-Identifier: AGPL-3.0-or-later

package metaworker

import (
	"strconv"
	"strings"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// SettingInboxPages is the Meta workers' own setting: how many pages of the
// account's chat list (Meta's inbox, newest activity first) the connector
// syncs after a sign-in, the first included. Each chat listed becomes a
// room and imports its newest max_initial_messages; a chat not listed gets
// its room when a message comes in it. The size of a page is Meta's (the
// connectors name none).
//
// The connectors call it thread_backfill.batch_count, each with its own
// meaning, which the workers translate (Settings.InboxPages):
//   - chat_messenger (connector/threadbackfill.go): the pages fetched after
//     the one Meta pushes at connect (0: none), for the plain chats (sync
//     group 1) and, as many again, for the end-to-end encrypted chats (sync
//     group 95), after the first connect of a login only
//     (UserLoginMetadata.BackfillCompleted, E2EEBackfillCompleted). The
//     connector's own default, -1, lists every chat of the account.
//   - chat_instagram (igconnector/chatsync.go): every page, the first
//     included (1, and 0, list only the first: ThreadBackfillConfig.Enabled).
//     Its default is 0.
const SettingInboxPages = "max_inbox_pages"

const (
	// DefaultInboxPages: only the page Meta gives first, as chat_slack and
	// chat_whatsapp make 15 chats at sign-in, not every chat.
	DefaultInboxPages = 1
	// MaxInboxPages bounds the setting: every chat listed imports its
	// history when its room is made, two seconds between pages
	// (thread_backfill.batch_delay), which is what Meta rate-limits.
	MaxInboxPages = 20
)

// knownSettings are every key a Meta connection's Init may carry: the
// workers' own, those the backend reads for every worker
// (backend/config.go ReadHistory, backend.go SetSpoolKey, run.go), and
// device_name, which the host may give every chat worker
// (worker_settings) and these do not read (Meta's connectors have no device
// name). Any other refuses the start, so a misspelt or misplaced setting is
// seen at once instead of doing nothing.
var knownSettings = map[string]bool{
	SettingInboxPages:              true,
	"device_name":                  true,
	backend.SettingInitialMessages: true,
	backend.SettingCatchupMessages: true,
	"spool_budget_bytes":           true,
	"log_level":                    true,
	"dev_plain_store":              true,
}

// Settings are what a Meta worker reads of Init for its connector.
type Settings struct {
	// InboxPages is max_inbox_pages: 1 to MaxInboxPages.
	InboxPages int
}

// ReadSettings reads Init's settings strictly: an unknown key, or a
// max_inbox_pages that is not a decimal count from 1 to MaxInboxPages
// (spaces around it aside), is bad_settings; an absent or blank one is
// DefaultInboxPages. The backend's own keys are checked by the backend
// (ReadHistory reads the history settings right after the connector is
// made, run.go newFramework). No value is logged.
func ReadSettings(init *contract.Init) (Settings, error) {
	s := Settings{InboxPages: DefaultInboxPages}
	for key, value := range init.GetSettings() {
		if !knownSettings[key] {
			return Settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
		if key != SettingInboxPages {
			continue
		}
		v := strings.TrimSpace(value)
		if v == "" {
			continue
		}
		if strings.TrimLeft(v, "0123456789") != "" {
			return Settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > MaxInboxPages {
			return Settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
		s.InboxPages = n
	}
	return s, nil
}

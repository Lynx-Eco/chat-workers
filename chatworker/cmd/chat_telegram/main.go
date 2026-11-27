// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_telegram is the Telegram worker: the backend with the upstream
// mautrix-telegram connector, unmodified.
//
//	chat_telegram --socket-path <path>
//	chat_telegram --source-commit    print the stamped commit and exit 0
//
// Settings from Init: api_id and api_hash (required; never logged),
// device_name (default "Lynx"), sync_update_limit and sync_create_limit
// (readSyncLimits).
// Animated stickers are passed on unconverted (no lottie or ffmpeg).
//
// Exit codes: 0 after Shutdown or when the host closed the connection (on
// SIGTERM it logs sigterm and dies of the signal, as without a handler), 1 on
// an error before Hello (a store that could not be reached now among them:
// store_open_failed, no Hello sent), 2 on bad arguments, 4
// (backend.ExitRestartFailed) when no framework instance could be started
// again after a logout wiped the store, 5 (backend.ExitStoreRefused) when the
// store must not be used as it is (reported as ConnectionState ERROR first).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// Stamped with -ldflags -X at build time.
var (
	version         = "0.0.0-dev"
	sourceCommit    = ""
	mautrixVersion  = "v0.33.0"
	telegramVersion = "v0.2611.0"
)

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	s := init.GetSettings()
	apiID, err := strconv.Atoi(strings.TrimSpace(s["api_id"]))
	apiHash := strings.TrimSpace(s["api_hash"])
	if err != nil || apiID <= 0 || apiHash == "" {
		return nil, &backend.SettingsError{Code: "missing_settings"}
	}
	tc := &connector.TelegramConnector{}
	example, data, _ := tc.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	tc.Config.APIID = apiID
	tc.Config.APIHash = apiHash
	tc.Config.DeviceInfo.DeviceModel = "Lynx"
	if name := strings.TrimSpace(s["device_name"]); name != "" {
		tc.Config.DeviceInfo.DeviceModel = name
	}
	tc.Config.AnimatedSticker.Target = "disable"
	tc.Config.ProxyConfig.Type = "disabled"
	update, create, err := readSyncLimits(init, tc.Config.Sync.UpdateLimit, tc.Config.Sync.CreateLimit)
	if err != nil {
		return nil, err
	}
	tc.Config.Sync.UpdateLimit = update
	tc.Config.Sync.CreateLimit = create
	if err := tc.ValidateConfig(); err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	return tc, nil
}

// The connector's dialog sync after a sign-in (chatsync.go syncChats): it
// looks at the sync_update_limit most recently active dialogs
// (sync.update_limit, 100 in the connector's example config) and makes rooms
// for the sync_create_limit newest of them (sync.create_limit, 15); every
// other chat gets its room when a message comes in it. Each room made imports
// max_initial_messages (backend.ReadHistory). A start whose limits are
// greater than those last applied runs the sync again (resync.go).
const (
	settingSyncUpdateLimit = "sync_update_limit"
	settingSyncCreateLimit = "sync_create_limit"
	// maxSyncLimit bounds both. The connector's -1 (no limit) is not taken:
	// its sync runs one request after another with no spacing between
	// chats, so a sign-in costs, for every dialog looked at, a member list
	// request (a group's getFullChat, a supergroup's getParticipants) and,
	// for every room made, its import (one getHistory up to 100 messages):
	// on an account of 5,000 dialogs about 10,000 requests in one burst. A
	// FLOOD_WAIT of under 5 minutes is slept out by the connector (five
	// tries a request); a longer one ends the sync, which resumes at the
	// worker's next start. 1,000 dialogs cover what nearly every account is
	// active in, at no more than 10 getDialogs, 1,000 member lists and
	// 1,000 imports.
	maxSyncLimit = 1000
)

// readSyncLimits reads sync_update_limit and sync_create_limit from Init:
// each absent or blank is the connector's own default (update, create); a
// value that is not a decimal count from 0 to maxSyncLimit refuses the start
// (bad_settings), so a typo does not make every room or none unseen. The
// update limit is raised to the create limit when it is lower: rooms are
// made only among the dialogs looked at (chatsync.go handleDialogs), so a
// create limit above it would be cut to it silently. 0 for create makes no
// room at sign-in; 0 for update looks at no dialog (no room either).
func readSyncLimits(init *contract.Init, update, create int) (int, int, error) {
	for _, s := range []struct {
		key string
		to  *int
	}{{settingSyncUpdateLimit, &update}, {settingSyncCreateLimit, &create}} {
		v := strings.TrimSpace(init.GetSettings()[s.key])
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > maxSyncLimit || strings.TrimLeft(v, "0123456789") != "" {
			return 0, 0, &backend.SettingsError{Code: "bad_settings"}
		}
		*s.to = n
	}
	return max(update, create), create, nil
}

// hooks are what the backend needs to know of the Telegram connector.
func hooks() backend.Hooks {
	return backend.Hooks{
		// Telegram's random_id must be a positive int64.
		SendKeyFormat:    backend.NumericSendKey,
		IdentifierScheme: "telegram",
		// Not EndRefusedSession: the connector's client cannot call
		// auth.logOut before it is connected, and its LogoutRemote then
		// deletes rows by the other account's id. endRefused logs the
		// refused session out with a client of its own (refused.go).
		EndRefused: endRefused,
		// A user account only: not the connector's "bot" (a bot is not
		// a user's account) nor "manual" (a session made elsewhere,
		// imported).
		SignInFlows: []string{connector.LoginFlowIDPhone, connector.LoginFlowIDQR},
		// A broadcast channel is a channel, on the connector's stored
		// metadata (channel.go).
		IsChannel: broadcastChannel,
		// The basic group's messages from before its upgrade, renamed:
		// named by the ids the host was given.
		OriginalMessageID: migratedOriginal,
		// A history fetch that meets a FLOOD_WAIT is answered
		// rate_limited with the wait (floodwait.go).
		RateLimitWait:  floodWait,
		WatchRateLimit: watchFloodWaits,
		// The dialog sync runs again at a start whose sync limits grew
		// (resync.go).
		PrepareLogins: resyncOnGrownLimits,
	}
}

// migratedPrefix begins the id the connector gives the rows of a basic
// group's messages when it upgrades to a supergroup, before it re-IDs the
// portal (NamespaceMigratedMessages, handletelegram.go:749; the query
// store/upgrades/10-migrated-message-ids.go:29 makes 'migrated:' || id ||
// ':' || <unix ms> of every id with neither '.' nor ':', which is a basic
// group's message id: ids.MakeMessageID, the decimal message number). The
// connector refuses such ids itself (ids.ParseMessageID). The host was
// given the message's id before the rename, never this one.
const migratedPrefix = "migrated:"

// migratedOriginal is the worker's Hooks.OriginalMessageID: an id with
// migratedPrefix is a renamed row, and its original id is the decimal
// number after the prefix when the id is exactly
// migrated:<digits>:<digits>; otherwise it cannot be told ("").
func migratedOriginal(id networkid.MessageID) (networkid.MessageID, bool) {
	rest, ok := strings.CutPrefix(string(id), migratedPrefix)
	if !ok {
		return "", false
	}
	orig, ms, ok := strings.Cut(rest, ":")
	if !ok || !decimal(orig) || !decimal(ms) {
		return "", true
	}
	return networkid.MessageID(orig), true
}

// decimal tells whether s is one or more ASCII digits.
func decimal(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func main() {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], sourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet("chat_telegram", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_telegram",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream: map[string]string{
			"maunium.net/go/mautrix":     mautrixVersion,
			"go.mau.fi/mautrix-telegram": telegramVersion,
		},
		BridgeID:     "telegram",
		NewConnector: newConnector,
		Hooks:        hooks(),
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}

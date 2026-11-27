// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_gmessages is the Google Messages worker: the backend with the
// upstream mautrix-gmessages connector (and its libgm under it), unmodified.
// It pairs with the user's Android phone the way Google's "Messages for web"
// does, and sends and receives the phone's SMS, MMS and RCS through it: the
// phone must stay online, with Google Messages as its default SMS app. One
// connection is one pairing with one phone: the principal is the connector's
// login id (self.go). It is registered in the engine
// (src/providers/chat_gmessages.rs) and runs only where a host's allowlist
// names it; whether an image carries it is the release's choice (see
// go/README.md, "The Google Messages worker").
//
//	chat_gmessages --socket-path <path>
//	chat_gmessages --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; Google Messages has no app id or
// secret), read strictly (readSettings): device_name (default "Lynx", the
// name the phone shows under Device pairing), max_initial_conversations
// (default 25, a decimal count from 1 to 100: the conversations the
// connector lists at each connect, readConversationCount), and the
// backend's own (max_initial_messages, max_catchup_messages,
// spool_budget_bytes, log_level, dev_plain_store). Any other key, or a value
// out of its form, refuses the start (bad_settings).
//
// Exit codes: as chat_telegram's (0 after Shutdown or when the host closed
// the connection, 1 on an error before Hello, 2 on bad arguments, 4
// backend.ExitRestartFailed, 5 backend.ExitStoreRefused).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.mau.fi/mautrix-gmessages/pkg/connector"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// Stamped with -ldflags -X at build time.
var (
	version          = "0.0.0-dev"
	sourceCommit     = ""
	mautrixVersion   = "v0.33.0"
	gmessagesVersion = "v0.2611.0"
)

// defaultDeviceName is the name the phone shows for the pairing (Google
// Messages, Device pairing), the connector's device_meta.os.
const defaultDeviceName = "Lynx"

// maxDeviceNameBytes bounds device_name, as chat_signal bounds its own. The
// name travels in every pairing request's BrowserDetails (libgm
// util.BrowserDetailsMessage, set from the connector's config in
// GMConnector.Init); Google's limit on it is not in the connector's source.
const maxDeviceNameBytes = 100

// The device kind the phone is told, the connector's defaults written out so
// that a change of them upstream is not taken unseen. TABLET is what keeps
// the pairing beside the user's own Messages for web: the connector's
// example says one web, two tablets and one PWA can be connected at the
// same time (example-config.yaml device_meta.type); a WEB device would take
// the user's browser session away, or lose to it.
const (
	deviceBrowser = "OTHER"
	deviceType    = "TABLET"
)

// The worker's own settings.
const (
	settingDeviceName = "device_name"
	// settingMaxInitialConversations is the connector's
	// initial_chat_sync_count (readConversationCount).
	settingMaxInitialConversations = "max_initial_conversations"
	// defaultInitialConversations is the connector's own default (its
	// example-config.yaml), not the 15 of chat_whatsapp and chat_slack: the
	// same count is the window the connector looks in for messages it
	// missed while the worker was away (readConversationCount), and a
	// smaller one would miss more.
	defaultInitialConversations = 25
	// maxInitialConversations bounds the setting: every conversation listed
	// that has no room yet gets one, and its import is one request to the
	// phone for max_initial_messages messages, at the connect that lists it.
	// The connector asks Google for that many in one request with no paging
	// (chatsync.go SyncConversations); what Google does with a larger count
	// is not in the source.
	maxInitialConversations = 100
)

// knownSettings are every key a Google Messages connection's Init may carry:
// the worker's own and those the backend reads for every worker
// (backend/config.go ReadHistory, backend.go SetSpoolKey, run.go). Any
// other refuses the start, so a misspelt or misplaced setting is seen at
// once instead of doing nothing.
var knownSettings = map[string]bool{
	settingDeviceName:              true,
	settingMaxInitialConversations: true,
	backend.SettingInitialMessages: true,
	backend.SettingCatchupMessages: true,
	"spool_budget_bytes":           true,
	"log_level":                    true,
	"dev_plain_store":              true,
}

// settings are what the worker reads of Init for the connector.
type settings struct {
	deviceName    string
	conversations int
}

// readSettings reads Init's settings strictly: an unknown key, a
// device_name that is not valid UTF-8, holds a control character or is
// longer than maxDeviceNameBytes (after trimming spaces), or a
// max_initial_conversations out of readConversationCount's form, is
// bad_settings. An absent or blank device_name is defaultDeviceName. The
// backend's own keys are checked by the backend.
func readSettings(init *contract.Init) (settings, error) {
	s := settings{deviceName: defaultDeviceName, conversations: defaultInitialConversations}
	for key, value := range init.GetSettings() {
		if !knownSettings[key] {
			return settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
		switch key {
		case settingDeviceName:
			name := strings.TrimSpace(value)
			if !utf8.ValidString(name) || len(name) > maxDeviceNameBytes || strings.IndexFunc(name, unicode.IsControl) >= 0 {
				return settings{}, &backend.SettingsError{Code: "bad_settings"}
			}
			if name != "" {
				s.deviceName = name
			}
		case settingMaxInitialConversations:
			n, err := readConversationCount(value)
			if err != nil {
				return settings{}, err
			}
			s.conversations = n
		}
	}
	return s, nil
}

// readConversationCount reads max_initial_conversations: absent or blank is
// defaultInitialConversations; a decimal count from 1 to
// maxInitialConversations is taken; anything else (0, a sign, a space
// inside) is bad_settings.
//
// It is the connector's initial_chat_sync_count, and it is more than an
// initial count. At every connect (when the phone says the new session is
// active: handlegmessages.go handleUserAlert, BROWSER_ACTIVE) the connector
// lists that many of the inbox's newest conversations (ListConversations,
// chatsync.go SyncConversations) and resyncs each (syncConversation): a
// conversation with no room gets one, whose import is its newest
// max_initial_messages; one with a room whose last message is newer than
// the newest stored imports up to max_catchup_messages. When data comes
// again after a quiet spell, or the stall check runs, it lists them again
// and resyncs them only when one has a message newer than the last data it
// received (a gap). A conversation outside the list gets its room when a
// message comes in it. 0 is refused: the connector would ask Google for
// none, and the list is also how it finds what it missed.
func readConversationCount(value string) (int, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return defaultInitialConversations, nil
	}
	if strings.TrimLeft(v, "0123456789") != "" {
		return 0, &backend.SettingsError{Code: "bad_settings"}
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > maxInitialConversations {
		return 0, &backend.SettingsError{Code: "bad_settings"}
	}
	return n, nil
}

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	// The history settings are the backend's: it reads them strictly right
	// after this (run.go newFramework, ReadHistory).
	s, err := readSettings(init)
	if err != nil {
		return nil, err
	}
	gc := &connector.GMConnector{}
	example, data, _ := gc.GetConfig()
	// Config.UnmarshalYAML parses the display name template, which is what
	// reading the connector's config checks (it has no ValidateConfig). The
	// example's template, {{or .FullName .PhoneNumber}}, has no suffix to
	// take off, so it is kept and nothing is parsed again.
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	c := &gc.Config
	c.DeviceMeta.OS = s.deviceName
	c.DeviceMeta.Browser = deviceBrowser
	c.DeviceMeta.Type = deviceType
	c.InitialChatSyncCount = s.conversations
	// The id prefix of the login's people, chats and messages is made from
	// the login id (a hash) instead of a row of the connector's
	// gmessages_login_prefix table: that row would be written in
	// finishLogin (login.go) before the framework's NewLogin, where the gate
	// refuses another account, and keep that account's login id (for a
	// Google account pairing, its address) in the store (refused.go).
	c.DeterministicIDPrefix = true
	// The connector's defaults, written out: a pairing another Messages for
	// web session made inactive waits for the user (NEEDS_USER, state.go)
	// instead of taking itself back every time; no extra resyncs on a
	// stall.
	c.AggressiveReconnect = false
	c.HackyStallDetect = false
	return newGMNetwork(gc), nil
}

// hooks are what the backend needs to know of the Google Messages connector.
func hooks() backend.Hooks {
	return backend.Hooks{
		// The host's send key becomes the temporary id the phone echoes
		// with the message (send.go).
		SendKeyFormat: gmessagesSendKey,
		// The account's own person id is the connector's id of the phone's
		// own participant, not the login id (self.go).
		SelfPersonID:     selfPersonID,
		IdentifierScheme: identifierScheme,
		// A sign-in refused at the gate (another phone or Google account
		// than the connection's): the connector stores nothing of a login
		// before NewLogin (with DeterministicIDPrefix), so nothing is
		// deleted; the pairing the sign-in made is undone at Google in the
		// background (refused.go).
		EndRefused: endRefused,
		// At a logout, a QR pairing is revoked at Google before the
		// connector's own logout, which would not revoke it (refused.go).
		BeforeLogout: logoutUnpair,
		// A pairing Google or the phone ended is ENDED; a Google web
		// sign-in that expired, an inactive session and the like stay
		// NEEDS_USER (state.go).
		EndedCodes: endedCodes,
		// The Google account pairing first (the default), then QR pairing,
		// which Google has discontinued (signin.go).
		SignInFlows: signInFlows,
		// How long a shown QR code lasts (signin.go).
		ShowExpiresIn: qrExpiresIn,
	}
}

func main() {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], sourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet("chat_gmessages", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_gmessages",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream: map[string]string{
			"maunium.net/go/mautrix":      mautrixVersion,
			"go.mau.fi/mautrix-gmessages": gmessagesVersion,
		},
		BridgeID:     "gmessages",
		NewConnector: newConnector,
		Hooks:        hooks(),
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}

// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_gvoice is the Google Voice worker: the backend with the upstream
// mautrix-gvoice connector (and its libgv under it), unmodified. One
// connection is the Google Voice number of one Google account, signed in
// with the cookies of a Google web session on voice.google.com (one WEB
// step). The principal is the connector's login id, "<owner MXID>|<the
// account's primary Voice number>" (mautrix-gvoice v0.2611.0
// connector/login.go:139). It is registered in the engine and runs only
// where a host's allowlist names it; whether an image carries it is the
// release's choice (see go/README.md, "The Google Voice worker").
//
//	chat_gvoice --socket-path <path>
//	chat_gvoice --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; Google Voice has no app id or
// secret), read strictly (readSettings): max_initial_conversations
// (default 20, a decimal count from 0 to 1000: the connector's
// initial_thread_sync_count), electron_path (default none: the optional
// Electron the connector signs sent texts with, electron.go), device_name
// (accepted and not read: the connector has no device name), and the
// backend's own (max_initial_messages, max_catchup_messages,
// spool_budget_bytes, log_level, dev_plain_store). Any other key, or a
// malformed value, refuses the start (bad_settings); an electron_path whose
// file is not there refuses it as electron_not_found.
//
// Exit codes: as chat_telegram's (0 after Shutdown or when the host closed
// the connection, 1 on an error before Hello, 2 on bad arguments, 4
// backend.ExitRestartFailed, 5 backend.ExitStoreRefused).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-gvoice/pkg/connector"
	"go.mau.fi/mautrix-gvoice/pkg/libgv"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// Stamped with -ldflags -X at build time.
var (
	version        = "0.0.0-dev"
	sourceCommit   = ""
	mautrixVersion = "v0.33.0"
	gvoiceVersion  = "v0.2611.0"
)

const (
	// settingMaxInitialConversations is the connector's
	// initial_thread_sync_count (readConversationCount).
	settingMaxInitialConversations = "max_initial_conversations"
	// defaultInitialConversations is the connector's own default (its
	// example-config.yaml): one page of Google Voice's thread list, which
	// the connector lists at every connect whatever the setting.
	defaultInitialConversations = 20
	// maxInitialConversations bounds the setting, as chat_slack bounds its
	// own: every room made costs a GetThread request or more for its import
	// (README "History a connection gets by itself"), once.
	maxInitialConversations = 1000
)

// knownSettings are every key a Google Voice connection's Init may carry:
// the worker's own and those the backend reads for every worker
// (backend/config.go ReadHistory, backend.go SetSpoolKey, run.go). Any
// other refuses the start, so a misspelt or misplaced setting is seen at
// once instead of doing nothing. device_name is accepted and not read, as by
// the other workers: the host may give it every chat worker (a common
// setting), and the connector has no device name.
var knownSettings = map[string]bool{
	settingMaxInitialConversations: true,
	settingElectronPath:            true,
	"device_name":                  true,
	backend.SettingInitialMessages: true,
	backend.SettingCatchupMessages: true,
	"spool_budget_bytes":           true,
	"log_level":                    true,
	"dev_plain_store":              true,
}

// settings are what the worker reads of Init for the connector.
type settings struct {
	conversations int
	// electron is the absolute path of the Electron to run, "" for none.
	electron string
}

// readSettings reads Init's settings strictly: an unknown key, a
// max_initial_conversations that is not a count from 0 to
// maxInitialConversations, or a malformed electron_path is bad_settings.
// The backend's own keys are checked by the backend (run.go newFramework,
// right after this).
func readSettings(init *contract.Init) (settings, error) {
	for key := range init.GetSettings() {
		if !knownSettings[key] {
			return settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
	}
	n, err := readConversationCount(init)
	if err != nil {
		return settings{}, err
	}
	electron, err := readElectronPath(init)
	if err != nil {
		return settings{}, err
	}
	return settings{conversations: n, electron: electron}, nil
}

// readConversationCount reads max_initial_conversations from Init: absent or
// blank is defaultInitialConversations; a decimal count from 0 to
// maxInitialConversations is taken; anything else (a sign, a space inside,
// the connector's own -1 for "every thread of the account") refuses the
// start with bad_settings.
//
// It is the connector's initial_thread_sync_count. At every connect
// (handlegvoice.go fetchNewMessages) the connector lists the first page of
// the account's threads in the ALL_THREADS folder (texts, calls and
// voicemails; 20 a page, libgv client.go ListThreads), newest first, and
// resyncs each: a thread with no room gets one, with its newest
// max_initial_messages. Once per login (until its metadata says the sync
// is done) it then pages on through older threads until it has seen this
// many (syncOlderThreads, handlegvoice.go:286-323). So the first page is
// always made, whatever the value: 0 to 20 all make about 20 rooms; 40
// makes about 40. Every thread not listed gets its room when a message
// comes in it.
func readConversationCount(init *contract.Init) (int, error) {
	v := strings.TrimSpace(init.GetSettings()[settingMaxInitialConversations])
	if v == "" {
		return defaultInitialConversations, nil
	}
	if strings.TrimLeft(v, "0123456789") != "" {
		return 0, &backend.SettingsError{Code: "bad_settings"}
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > maxInitialConversations {
		return 0, &backend.SettingsError{Code: "bad_settings"}
	}
	return n, nil
}

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	s, err := readSettings(init)
	if err != nil {
		return nil, err
	}
	// Before the connector exists: what it finds as "electron" is decided
	// here, once per framework instance (electron.go).
	if err := useElectron(s.electron); err != nil {
		return nil, err
	}
	gv := &connector.GVConnector{}
	// The example's display name template, "{{ or .Contact.Name .Name }}"
	// (a person's name from the account's Google contacts, else the number
	// Google gives as the name), has no suffix: it is kept. The connector
	// parses it when its config is read (config.go UnmarshalYAML).
	example, data, _ := gv.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	gv.Config.InitialThreadSyncCount = s.conversations
	return newGVNetwork(gv), nil
}

// hooks are what the backend needs to know of the Google Voice connector.
func hooks() backend.Hooks {
	return backend.Hooks{
		// The account's own person id is the connector's user id of its
		// own number, "<prefix>.<number>", the form of every person id of
		// the login (self.go).
		SelfPersonID: selfPersonID,
		// The connector's network id, named (self.go).
		IdentifierScheme: identifierScheme,
		// Not SendKeyFormat: nothing of the host's key reaches Google
		// Voice (the connector sends with a random transaction id of its
		// own, handlematrix.go:44-49), so the key is kept as the
		// transaction id the framework stores on the message's row (README
		// "The Google Voice worker", sending).
		//
		// A sign-in refused at the gate (another number than the
		// connection's): the connector stored a row of the login's prefix
		// before the framework's NewLogin, so it is deleted at once; there
		// is no session to end at Google (refused.go).
		DropRefused: dropRefused,
		// A prefix row a sign-in left with no login (one cut off by a stop
		// between the connector's two steps) is deleted at the next start
		// (refused.go).
		PrepareLogins: dropOrphanPrefixes,
		// Cookies Google no longer takes are ENDED (state.go).
		EndedCodes: endedCodes,
		// The connector's one flow, named so that a flow it adds later is
		// not offered without a change here.
		SignInFlows: []string{connector.LoginFlowIDCookies},
		// A history fetch that ends on Google's 429 is answered
		// rate_limited with its Retry-After.
		RateLimitWait: rateLimitWait,
	}
}

// maxRetryAfter bounds the wait read from a Retry-After header, so that a
// huge number does not overflow a Duration.
const maxRetryAfter = 24 * time.Hour

// rateLimitWait is the worker's Hooks.RateLimitWait: an error of the
// connector's FetchMessages that is Google's 429, with the wait its
// Retry-After header names in seconds (0 when it names none, or a date).
// libgv does not try a 429 again (request.go MakeRequest retries only a
// network error or a 5xx): the 429 reaches the framework as
// *libgv.ResponseError with the response, its body already read.
func rateLimitWait(err error) (time.Duration, bool) {
	var re *libgv.ResponseError
	if !errors.As(err, &re) || re.Resp == nil || re.Resp.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}
	v := strings.TrimSpace(re.Resp.Header.Get("Retry-After"))
	if v == "" || strings.TrimLeft(v, "0123456789") != "" {
		return 0, true
	}
	secs, perr := strconv.ParseInt(v, 10, 64)
	if perr != nil || secs > int64(maxRetryAfter/time.Second) {
		return maxRetryAfter, true
	}
	return time.Duration(secs) * time.Second, true
}

func main() {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], sourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet("chat_gvoice", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_gvoice",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream: map[string]string{
			"maunium.net/go/mautrix":   mautrixVersion,
			"go.mau.fi/mautrix-gvoice": gvoiceVersion,
		},
		BridgeID:     "gvoice",
		NewConnector: newConnector,
		Hooks:        hooks(),
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}

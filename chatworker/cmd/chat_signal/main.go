// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_signal is the Signal worker: the backend with the upstream
// mautrix-signal connector (signalmeow, and libsignal through its cgo
// binding libsignalgo, under it), unmodified. It is a linked device of the
// user's phone, signed in by a QR code that Signal's own app scans under
// Settings, Linked devices. It is registered in the engine
// (src/providers/chat_signal.rs) and runs only where a host's allowlist
// names it; whether an image carries it is the release's choice (see
// go/README.md, "The Signal worker").
//
//	chat_signal --socket-path <path>
//	chat_signal --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; Signal has no app id or secret), read
// strictly (readSettings): device_name (default "Lynx", the name the phone
// shows under Linked devices), and the backend's own (max_initial_messages,
// max_catchup_messages, spool_budget_bytes, log_level, dev_plain_store). Any
// other key, or a device_name that is not a short line of text, refuses the
// start (bad_settings).
//
// History: Signal hands a linked device the account's history once, at the
// link, as a transfer archive the phone uploads when the user agrees there;
// it has no later backfill. Each chat of the archive becomes a room at
// sign-in with its newest max_initial_messages from the archive, and the
// connector then deletes the chat's archive (the backend's backfill queue is
// off: backfill.go:186-205). A later FetchHistory finds no archive and is
// answered OK with has_more false and no HistoryBatch (history_test.go).
//
// Exit codes: as chat_telegram's (0 after Shutdown or when the host closed
// the connection, 1 on an error before Hello, 2 on bad arguments, 4
// backend.ExitRestartFailed, 5 backend.ExitStoreRefused).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.mau.fi/mautrix-signal/pkg/connector"
	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// Stamped with -ldflags -X at build time.
var (
	version        = "0.0.0-dev"
	sourceCommit   = ""
	mautrixVersion = "v0.33.0"
	signalVersion  = "v0.2611.0"
)

// libsignalModule names libsignal in Hello's upstream versions. Its version
// is not stamped: it is libsignalgo.Version, the version whose library the
// binary links (libsignalgo/version.go calls signal_ffi_version_0_106_1,
// which only that version's libsignal_ffi.a defines, so a binary linked
// against another one does not build).
const libsignalModule = "github.com/signalapp/libsignal"

// defaultDeviceName is the name the phone shows under Linked devices.
const defaultDeviceName = "Lynx"

// maxDeviceNameBytes bounds device_name. Signal keeps the name encrypted
// (signalmeow devicename.go EncryptDeviceName: a protobuf of a 33-byte
// ephemeral key, a 16-byte IV and the ciphertext, as long as the name),
// and the server takes a bounded field; 100 bytes of name stay well under
// what Signal's own apps send. The server's exact limit is not in the
// connector's source.
const maxDeviceNameBytes = 100

// settingDeviceName is this worker's own setting.
const settingDeviceName = "device_name"

// knownSettings are every key a Signal connection's Init may carry: the
// worker's own and those the backend reads for every worker
// (backend/config.go ReadHistory, backend.go SetSpoolKey, run.go). Any
// other refuses the start, so a misspelt or misplaced setting is seen at
// once instead of doing nothing.
var knownSettings = map[string]bool{
	settingDeviceName:              true,
	backend.SettingInitialMessages: true,
	backend.SettingCatchupMessages: true,
	"spool_budget_bytes":           true,
	"log_level":                    true,
	"dev_plain_store":              true,
}

// settings are what the worker reads of Init for the connector.
type settings struct {
	deviceName string
}

// readSettings reads Init's settings strictly: an unknown key, or a
// device_name that is not valid UTF-8, holds a control character or is
// longer than maxDeviceNameBytes (after trimming spaces), is bad_settings.
// An absent or blank device_name is defaultDeviceName. The backend's own
// keys are checked by the backend.
func readSettings(init *contract.Init) (settings, error) {
	s := settings{deviceName: defaultDeviceName}
	for key, value := range init.GetSettings() {
		if !knownSettings[key] {
			return settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
		if key != settingDeviceName {
			continue
		}
		name := strings.TrimSpace(value)
		if !utf8.ValidString(name) || len(name) > maxDeviceNameBytes || strings.IndexFunc(name, unicode.IsControl) >= 0 {
			return settings{}, &backend.SettingsError{Code: "bad_settings"}
		}
		if name != "" {
			s.deviceName = name
		}
	}
	return s, nil
}

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	// The history settings are the backend's: it reads them strictly right
	// after this (run.go newFramework, ReadHistory).
	s, err := readSettings(init)
	if err != nil {
		return nil, err
	}
	sc := &connector.SignalConnector{}
	example, data, _ := sc.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	c := &sc.Config
	// The name the phone shows under Linked devices, sent encrypted when
	// the device is linked (login.go:153-155, provisioning.go confirmDevice).
	c.DeviceName = s.deviceName
	// The example's avatar of the Note to Self chat is an mxc:// address on
	// maunium.net: there is no homeserver to serve it.
	c.NoteToSelfAvatar = ""
	// Notices the connector makes up are not messages of the network.
	c.IdentityChangeNotices = false
	// A person's name is their own Signal profile's, then their number,
	// then their username (the example's template, which has no suffix).
	// Names from the phone's address book are not used, nor its avatars.
	c.UseContactAvatars = false
	c.UseOutdatedProfiles = false
	c.ExtEvPolls = false
	// The connector has no ValidateConfig: PostProcess parses the display
	// name template, which is what reading its config checks.
	if err := c.PostProcess(); err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	return sc, nil
}

// hooks are what the backend needs to know of the Signal connector.
func hooks() backend.Hooks {
	return backend.Hooks{
		// A sign-in refused at the gate (another account than the
		// connection's): signalmeow stored the linked device during
		// provisioning, before the gate saw the login, so it is deleted
		// from the store at once and then unlinked at Signal in the
		// background (refused.go).
		DropRefused: dropRefused,
		// A device whose sign-in was cut off after signalmeow stored it,
		// before any login was kept for it (a cancel or the QR's refresh
		// racing the phone's confirmation, or a gate that could not read
		// the store), is deleted at the next start and unlinked at Signal
		// in the background (refused.go).
		PrepareLoginsEnds: dropOrphanDevices,
		// The connector's codes for a linked device Signal no longer takes
		// (removed on the phone, the account gone) are ENDED, not
		// NEEDS_USER (state.go).
		EndedCodes:       endedCodes,
		SendKeyFormat:    signalSendKey,
		IdentifierScheme: identifierScheme,
		// Linking as a device by QR code: the connector's only flow, named
		// so that a flow it adds later is not offered without a change
		// here.
		SignInFlows: []string{loginFlowQR},
		// How long a shown QR code lasts (signin.go).
		ShowExpiresIn: qrExpiresIn,
		// The account's own ACI, the id its messages carry (self.go).
		SelfPersonID: selfPersonID,
		// No IsChannel: the connector bridges direct chats and groups only
		// (an announcement-only group is a group); it has no broadcast
		// feeds, and stories are not bridged.
	}
}

// loginFlowQR is the connector's one sign-in flow (login.go:37-50); the
// connector exports no name for it.
const loginFlowQR = "qr"

// identifierScheme names a Signal username in an identifier
// ("signal:<username>", the contract's "<connector>:<username>"), the form
// the connector itself gives a person's username (contactToUserInfo,
// chatinfo.go:120-125). Phone numbers are "tel:+<digits>" (chatinfo.go:121).
// The framework prefixes the account's RemoteProfile.Username with it (the
// connector sets it from the account record, handlesignal.go:946-948) and
// takes it off a StartChat identifier before the connector resolves it; the
// connector then takes an E.164 number ("+<digits>"), a username
// (signalmeow.SignalUsernameRegex), a signal.me link or a service id
// (ResolveIdentifier, chatinfo.go:282-299).
//
// Person ids are the connector's user ids: a person's ACI (a UUID in its
// text form), or "pni:<uuid>" for one known only by phone number identity
// (signalid.MakeUserIDFromServiceID, ids.go:119-128). The connection's
// principal is the login id, the account's ACI (login.go:223
// signalid.MakeUserLoginID); Signal keeps an account's ACI across a change
// of its phone number, so the principal does too.
const identifierScheme = "signal"

// sendKeyPrefix begins every transaction id the worker gives the connector.
// It is not a digit: the connector takes an input transaction id that parses
// as a decimal number for the message's Signal timestamp, which is also its
// id (getTimestampForEvent, handlematrix.go:108-120; the message id is
// "<ACI>|<timestamp>", signalid.MakeMessageID). A send key turned into a
// number (backend.NumericSendKey, as Telegram's) would send every message
// with a made-up timestamp. Any other id leaves the timestamp to the event's
// own, the time of the send (backend send.go newEvent).
const sendKeyPrefix = "lynx-"

// sendKeyHexLen is the number of hex digits after the prefix.
const sendKeyHexLen = 32

// signalSendKey is the worker's Hooks.SendKeyFormat. It maps the host's
// send key to a transaction id that is never a number (sendKeyPrefix and
// 32 lower-case hex digits of the key's SHA-256), so the connector stamps
// each message with the time it is sent. The same key always gives the
// same id; a key already of that form is kept as it is. The id stays on the
// framework's row of the message (SendTxnID), where the backend finds the
// key of a stored message by mapping each journal key again and comparing
// (send.go sendKeyForTxn).
//
// What keeps a send from going out twice is the journal, not the id: a key
// already sent or in flight is answered from the journal without the
// network (backend send.go runKeyed). Signal itself would take a second send
// of the same content as another message: its id is its timestamp, new at
// each send.
func signalSendKey(key string) networkid.RawTransactionID {
	if isSendKeyID(key) {
		return networkid.RawTransactionID(key)
	}
	sum := sha256.Sum256([]byte(key))
	return networkid.RawTransactionID(sendKeyPrefix + hex.EncodeToString(sum[:sendKeyHexLen/2]))
}

// isSendKeyID tells whether s is the prefix and exactly sendKeyHexLen
// lower-case hex digits.
func isSendKeyID(s string) bool {
	rest, ok := strings.CutPrefix(s, sendKeyPrefix)
	if !ok || len(rest) != sendKeyHexLen {
		return false
	}
	for _, c := range []byte(rest) {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// licence is the SPDX expression chat_signal's Hello reports: the binary
// links libsignal, licensed under the AGPL version 3 only, so as a whole it
// can be conveyed under that version only (go/NOTICE), not "or later" as
// the rest of go/.
const licence = "AGPL-3.0-only"

func main() {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], sourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet("chat_signal", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_signal",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream: map[string]string{
			"maunium.net/go/mautrix":   mautrixVersion,
			"go.mau.fi/mautrix-signal": signalVersion,
			libsignalModule:            libsignalgo.Version,
		},
		BridgeID:     "signal",
		NewConnector: newConnector,
		Hooks:        hooks(),
		Licence:      licence,
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}

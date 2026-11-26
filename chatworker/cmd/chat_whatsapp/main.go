// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_whatsapp is the WhatsApp worker: the backend with the upstream
// mautrix-whatsapp connector (and whatsmeow under it), unmodified. It is a
// linked device of the user's phone, signed in by a QR code or a pairing
// code. It is registered in the engine (src/providers/chat_whatsapp.rs) and
// runs only where a host's allowlist names it; whether an image carries it
// is the release's choice (see go/README.md, "The WhatsApp worker").
//
//	chat_whatsapp --socket-path <path>
//	chat_whatsapp --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; WhatsApp has no app id or secret):
// device_name (default "Lynx", the name the phone shows under linked
// devices) and max_initial_conversations (default 15: the chats made after
// sign-in from the phone's history sync; the others are made when a message
// comes). No full history sync is asked of the phone, no expired media is
// asked for again, status broadcasts are not bridged, and animated stickers
// are passed on unconverted (no ffmpeg; without it the connector also
// advertises no conversion of voice notes or GIFs).
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
	"time"

	"go.mau.fi/mautrix-whatsapp/pkg/connector"
	"go.mau.fi/whatsmeow"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

// Stamped with -ldflags -X at build time.
var (
	version          = "0.0.0-dev"
	sourceCommit     = ""
	mautrixVersion   = "v0.33.0"
	whatsappVersion  = "v0.2611.0"
	whatsmeowVersion = "v0.0.0-20261116172437-19d61352436f"
)

// defaultInitialConversations bounds the chats made from the history sync
// after a sign-in (the connector's default, -1, makes them all), as
// chat_telegram keeps the connector's create_limit of 15.
const defaultInitialConversations = 15

// displaynameTemplate names a person as the connector's example does,
// without its " (WA)" suffix: the connector parses and runs the template
// when its config is read, so one is required.
const displaynameTemplate = `{{or .BusinessName .PushName .Phone .RedactedPhone "Unknown user"}}`

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	s := init.GetSettings()
	wc := &connector.WhatsAppConnector{}
	example, data, _ := wc.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	c := &wc.Config
	c.OSName = "Lynx"
	if name := strings.TrimSpace(s["device_name"]); name != "" {
		c.OSName = name
	}
	c.BrowserName = "unknown"
	c.Proxy = ""
	c.GetProxyURL = ""
	c.DisplaynameTemplate = displaynameTemplate
	// Notices the connector makes up are not messages of the network.
	c.CallStartNotices = false
	c.IdentityChangeNotices = false
	c.EnableStatusBroadcast = false
	c.DisableStatusBroadcastSend = true
	c.SendPresenceOnTyping = false
	// Previews would be asked of a homeserver; there is none.
	c.URLPreviews = false
	c.DirectMediaAutoRequest = false
	c.LazyAvatars = false
	c.AnimatedSticker.Target = "disable"
	c.HistorySync.MaxInitialConversations = backend.SettingInt(init, "max_initial_conversations", defaultInitialConversations)
	c.HistorySync.RequestFullSync = false
	c.HistorySync.FullSyncConfig.DaysLimit = 0
	c.HistorySync.FullSyncConfig.SizeLimit = 0
	c.HistorySync.FullSyncConfig.StorageQuota = 0
	c.HistorySync.MediaRequests.AutoRequestMedia = false
	c.HistorySync.BackwardsOnDemand = false
	// The connector has no ValidateConfig: PostProcess parses and runs the
	// display name template, which is what reading its config checks.
	if err := c.PostProcess(); err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	return wc, nil
}

// hooks are what the backend needs to know of the WhatsApp connector.
func hooks() backend.Hooks {
	return backend.Hooks{
		// A sign-in refused at the gate (another account than the
		// connection's): whatsmeow had stored the paired companion before
		// the gate saw the login, so it is deleted from the store at once
		// and then unpaired at WhatsApp in the background (refused.go).
		DropRefused: dropRefused,
		// The connector's codes for a companion WhatsApp unlinked (removed
		// on the phone, expired, the phone logged out, banned) are ENDED,
		// not NEEDS_USER (state.go).
		EndedCodes:       endedCodes,
		SendKeyFormat:    whatsappSendKey,
		IdentifierScheme: identifierScheme,
		// Pairing as a linked device only, by QR code or by a pairing code
		// typed on the phone: the connector's two flows, named so that a
		// flow it adds later is not offered without a change here.
		SignInFlows: []string{connector.LoginFlowIDQR, connector.LoginFlowIDPhone},
		// How long a shown QR code lasts; a pairing code's lifetime is
		// not known (signin.go).
		ShowExpiresIn: newCodeLifetimes(time.Now).expiresIn,
		// The account's own LID, the form its messages carry (self.go).
		SelfPersonID: selfPersonID,
		// A newsletter (WhatsApp Channel) and the status broadcast are
		// channels, by the portal's JID (channel.go).
		IsChannel: broadcastFeed,
	}
}

// identifierScheme names a WhatsApp handle that is not a phone number in an
// identifier ("whatsapp:<handle>", the contract's "<connector>:<username>",
// channel.proto PersonInfo.identifiers). The framework uses it for two
// things only: it prefixes the account's RemoteProfile.Username in its
// identifiers (backend translate.go accountIdentifiers), and it is taken off
// a StartChat identifier, after "tel:", before the connector resolves it
// (backend commands.go startChat).
//
// Phone numbers are "tel:+<digits>" whatever the scheme, and are the only
// identifiers a WhatsApp person or account has today: the account's comes
// from RemoteProfile.Phone, which the connector sets to "+<digits>" at
// sign-in (login.go:406-413), a person's from the connector itself
// ("tel:+<digits>", for a LID's ghost too when its phone is known,
// userinfo.go:274-289). The connector sets no Username (login.go:408-413,
// syncRemoteProfile client.go:449-482 sets the name and avatar), so no
// "whatsapp:" identifier is reported. "tel" here would turn a WhatsApp
// handle, should the connector report one, into a false phone number.
// StartChat takes "tel:+<digits>", "+<digits>", "whatsapp:lid-<digits>" or
// "whatsapp:<digits>@lid" (the connector's validateIdentifer,
// startchat.go:74-100).
//
// No Lynx consumer reads chat identifiers yet: the product keeps a person's
// id, name and flags and drops the identifiers, and nothing matches a
// "whatsapp:" scheme; its phone identities are E.164, which "tel:+<digits>"
// carries as they are.
//
// Person ids are not identifiers: they are the connector's user ids, the
// phone number's digits or "lid-<digits>" (waid.MakeUserID), and the
// connection's principal is the login id, the phone number's digits
// (login.go:406, waid.MakeUserLoginID). The worker reports them as the
// connector makes them and cannot make them stabler: a person met by phone
// number and then by LID (the connector moves a direct chat to the LID,
// lidmigrate.go:83-140) has two person ids, both with the same tel:
// identifier when the phone is known. When the account's number changes, the
// principal changes with it: signing in again is another account to the
// gate (principal_mismatch), and the number's old person id and tel:
// identifier are not the new ones. The account's own person id is its LID
// (self.go), not the number; whether WhatsApp keeps the LID across a number
// change is not in the source.
const identifierScheme = "whatsapp"

// whatsappSendKeyHexLen is the number of hex digits after the prefix of the
// ids WhatsApp Web makes (whatsmeow's GenerateMessageID: 9 bytes).
const whatsappSendKeyHexLen = 18

// whatsappSendKey is the worker's Hooks.SendKeyFormat. It maps the host's
// send key to a message id of WhatsApp Web's form (whatsmeow.
// WebMessageIDPrefix and 18 upper-case hex digits, as Client.
// GenerateMessageID makes them, whatsmeow send.go:41-61), which the
// connector sends as the message's own id instead of a random one: an input
// transaction id with that prefix is the id of a message
// (handlematrix.go:117-121), of a reaction's removal (:246-248), an edit
// (:260-261) and a deletion (:312-313). The same key
// always gives the same id. A key already of that form is kept as it is;
// any other is hashed.
//
// The id is not what keeps a send from going out twice: the journal is. A
// key already sent, or in flight (also at a restart), is answered from the
// journal without the network (backend send.go runKeyed), so the network
// sees one id once per stored journal. Neither whatsmeow nor the connector
// would stop a second send of an id: SendMessage sends whatever id it is
// given (send.go:191-230, 391-430; the ack is waited for by that id,
// request.go:56-61), and the connector checks only its prefix. What WhatsApp's
// server and the recipients' phones do with a second message of the same id
// is not in the source. A second one can only follow a wipe of the store (a
// logout) and the host sending the same key again on the new sign-in.
//
// What the fixed id buys: the network id of a message is known from its key
// alone. bridgev2 keeps the transaction id on the message's row
// (portal.go:1575-1576, SendTxnID), and the backend finds the key of a
// stored message by mapping each journal key again and comparing
// (sendKeyForTxn), which needs the mapping to be a function of the key. The
// message's id the host is told also holds it ("<chat>:<sender>:<id>",
// waid.MakeMessageID), so a send answered unknown can be matched to the
// message once it is seen.
func whatsappSendKey(key string) networkid.RawTransactionID {
	if isWebMessageID(key) {
		return networkid.RawTransactionID(key)
	}
	sum := sha256.Sum256([]byte(key))
	return networkid.RawTransactionID(whatsmeow.WebMessageIDPrefix + strings.ToUpper(hex.EncodeToString(sum[:whatsappSendKeyHexLen/2])))
}

// isWebMessageID tells whether s is the prefix and exactly 18 upper-case hex
// digits.
func isWebMessageID(s string) bool {
	rest, ok := strings.CutPrefix(s, whatsmeow.WebMessageIDPrefix)
	if !ok || len(rest) != whatsappSendKeyHexLen {
		return false
	}
	for _, c := range []byte(rest) {
		if (c < '0' || c > '9') && (c < 'A' || c > 'F') {
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
	fs := flag.NewFlagSet("chat_whatsapp", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_whatsapp",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream: map[string]string{
			"maunium.net/go/mautrix":     mautrixVersion,
			"go.mau.fi/mautrix-whatsapp": whatsappVersion,
			"go.mau.fi/whatsmeow":        whatsmeowVersion,
		},
		BridgeID:     "whatsapp",
		NewConnector: newConnector,
		Hooks:        hooks(),
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}

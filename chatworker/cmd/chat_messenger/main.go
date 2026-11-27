// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_messenger is the Facebook Messenger worker: the backend with the
// upstream mautrix-meta connector (its pkg/connector, the one upstream's
// cmd/mautrix-meta runs; messagix, and whatsmeow for the end-to-end
// encrypted chats, under it), unmodified. One connection is one Facebook
// account: the principal is the connector's login id, the account's FBID in
// decimal (metaid.MakeUserLoginID). It signs in with the cookies of a web
// session of facebook.com or messenger.com (a WEB step). It is registered
// in the engine (provider id "messenger") and runs only where a host's
// allowlist names it; whether an image carries it is the release's choice
// (see go/README.md, "The Instagram and Messenger workers").
//
//	chat_messenger --socket-path <path>
//	chat_messenger --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; Meta has no app id or secret), read
// strictly (metaworker.ReadSettings): max_inbox_pages (default 1, 1 to 20:
// the pages of the chat list synced after a sign-in), the backend's own,
// and device_name, which is not read (the connector has no device name).
// Any other key refuses the start (bad_settings).
//
// Exit codes: as chat_telegram's (0 after Shutdown or when the host closed
// the connection, 1 on an error before Hello, 2 on bad arguments, 4
// backend.ExitRestartFailed, 5 backend.ExitStoreRefused).
package main

import (
	"go.mau.fi/mautrix-meta/pkg/connector"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/metaworker"
)

// Stamped with -ldflags -X at build time.
var (
	version      = "0.0.0-dev"
	sourceCommit = ""
	// whatsmeowVersion is go.mau.fi/whatsmeow's, the client of the
	// end-to-end encrypted chats (chatworker/go.mod pins it).
	whatsmeowVersion = "v0.0.0-20261116172437-19d61352436f"
)

// identifierScheme is the connector's network id ("facebook",
// MetaConnector.GetName), named so that a change upstream does not move
// it. The connector gives no identifier of a person (userinfo.go
// wrapUserInfo sets a name, an avatar and is_bot) and no username of the
// account (login.go sets only its name), so the framework reports none
// with it; it is taken off a StartChat identifier, and the connector then
// takes a person's FBID in decimal (startchat.go ResolveIdentifier,
// metaid.ParseIDFromString), the person id the host was given.
const identifierScheme = "facebook"

// signInFlows are the connector's two cookie flows, in its order, so that a
// host that begins a sign-in with no flow named (the engine takes the first
// one listed) signs in on facebook.com:
//   - facebook (connector.FlowIDFacebookCookies): the cookies of a web
//     session of https://www.facebook.com/, the platform types.Facebook;
//   - messenger (connector.FlowIDMessengerCookies): the cookies of a web
//     session of https://www.messenger.com/, types.Messenger.
//
// Each is one WEB step, fi.mau.meta.cookies, capturing the cookies xs,
// c_user and datr (cookies.FBRequiredCookies, all required). Not the
// connector's messenger-lite and messenger-lite-android flows
// (login.go MetaNativeLogin): the user's password typed into the worker
// for Meta's mobile login API, and a "native" session of Messenger's apps,
// not the user's web session. A SignInBegin of either is unknown_flow,
// like any id not listed. Named, so that a flow the connector adds later is
// not offered without a change here.
var signInFlows = []string{connector.FlowIDFacebookCookies, connector.FlowIDMessengerCookies}

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	s, err := metaworker.ReadSettings(init)
	if err != nil {
		return nil, err
	}
	mc := &connector.MetaConnector{}
	example, data, _ := mc.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	c := &mc.Config
	// No proxy, no Tor: the worker talks to Meta from the host it runs on.
	c.Proxy = ""
	c.Tor = false
	c.GetProxyFrom = ""
	c.ProxyMedia = false
	c.ProxyE2EE = false
	// Presence is not the host's to set, nor a space of Marketplace chats
	// (each stays its own chat, under no parent).
	c.SendPresenceOnTyping = false
	c.MarketplaceSpace = false
	c.LogRedactedLoginResponses = false
	// No restore of the encrypted chats' history from Messenger's secure
	// storage: it would ask the user's storage PIN after the cookies (a
	// FIELDS step, login_history.go) and upload every message sent to that
	// backup (history_upload.go). The encrypted chats then begin at the
	// sign-in (their CanBackfill is false: chatinfo.go makeMinimalChatInfo).
	c.E2EEBackfill = false
	// The pages of the chat list after the first (metaworker.SettingInboxPages);
	// 0 fetches none, never every chat (-1, the connector's default).
	c.ThreadBackfill.BatchCount = s.InboxPages - 1
	// The connector has no ValidateConfig: PostProcess parses the display
	// name template (the example's, "{{or .DisplayName .Username "Unknown
	// user"}}", with no suffix), which is what reading its config checks.
	if err := c.PostProcess(); err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	return mc, nil
}

// hooks are what the backend needs to know of the Messenger connector
// (metaworker.Hooks: the self id, the send key, the refusal, the rate
// limit), with the connector's flows, its codes of a session Meta ended
// (state.go) and its identifier scheme. No IsChannel: Messenger's chats are
// direct chats and groups; a folder is a space, a channel by the space rule.
func hooks() backend.Hooks {
	return metaworker.Hooks(metaworker.Network{
		Flows:            signInFlows,
		EndedCodes:       endedCodes,
		IdentifierScheme: identifierScheme,
	})
}

func main() {
	metaworker.Main(backend.Worker{
		Name:         "chat_messenger",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream:     metaworker.Upstream(map[string]string{"go.mau.fi/whatsmeow": whatsmeowVersion}),
		BridgeID:     "messenger",
		NewConnector: newConnector,
		Hooks:        hooks(),
	})
}

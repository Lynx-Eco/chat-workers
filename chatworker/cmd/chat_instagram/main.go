// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_instagram is the Instagram worker: the backend with the upstream
// mautrix-meta connector for Instagram (its pkg/igconnector, the one
// upstream's cmd/mautrix-instagram runs, with instameow under it),
// unmodified. One connection is one Instagram account: the principal is the
// connector's login id, the account's messaging FBID in decimal
// (metaid.MakeUserLoginID of instameow's GetOwnFBID), not its Instagram user
// id. It signs in with the cookies of a web session of instagram.com (a WEB
// step). It is registered in the engine (provider id "instagram") and runs
// only where a host's allowlist names it; whether an image carries it is
// the release's choice (see go/README.md, "The Instagram and Messenger
// workers").
//
// The module's pkg/connector is not this worker's: since mautrix-meta's
// Instagram split it refuses an Instagram login (meta-ig-not-supported,
// connector/client.go connectWithRetry) and offers no Instagram flow.
//
//	chat_instagram --socket-path <path>
//	chat_instagram --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; Meta has no app id or secret), read
// strictly (metaworker.ReadSettings): max_inbox_pages (default 1, 1 to 20:
// the pages of the inbox synced after a sign-in), the backend's own, and
// device_name, which is not read (the connector has no device name). Any
// other key refuses the start (bad_settings).
//
// Exit codes: as chat_telegram's (0 after Shutdown or when the host closed
// the connection, 1 on an error before Hello, 2 on bad arguments, 4
// backend.ExitRestartFailed, 5 backend.ExitStoreRefused).
package main

import (
	"go.mau.fi/mautrix-meta/pkg/igconnector"
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
)

// identifierScheme names an Instagram username in an identifier
// ("instagram:<username>"), the form the connector itself gives a person's
// username (chatinfo.go wrapUserInfo) and the scheme the framework puts on
// the account's own (connectWithMailbox sets RemoteProfile.Username). It
// is also the connector's network id (IGConnector.GetName). The framework
// takes it off a StartChat identifier; the connector then takes only a
// person's FBID in decimal (startchat.go ResolveIdentifier,
// metaid.ParseIDFromString), the person id the host was given, not a
// username: "instagram:<username>" does not start a chat.
const identifierScheme = "instagram"

// signInFlows is the connector's one cookie flow, instagram
// (igconnector.FlowIDCookies, login.go MetaCookieLogin): one WEB step,
// fi.mau.meta.cookies, on https://www.instagram.com/accounts/login/,
// capturing the cookies sessionid, csrftoken and ds_user_id (required,
// cookies.IGRequiredCookies) and rur, shbid, shbts, mid and ig_did (taken
// when the web view has them, IGOptionalCookies).
//
// Not the connector's android and instagram-password flows (login_native.go
// MetaNativeLogin): the user's password typed into the worker, for
// Instagram's Android or web login API, with its two-factor and challenge
// steps answered through the worker; and both may store an "app
// installation identity" of the owner before the gate sees the login
// (getInstaNativeClient's SaveMobileLoginDevice, PutInstagramLoginDevice). A SignInBegin of either is unknown_flow, like
// any id not listed. Named, so that a flow the connector adds later is not
// offered without a change here.
var signInFlows = []string{igconnector.FlowIDCookies}

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	s, err := metaworker.ReadSettings(init)
	if err != nil {
		return nil, err
	}
	ic := &igconnector.IGConnector{}
	example, data, _ := ic.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	c := &ic.Config
	// No proxy: the worker talks to Instagram from the host it runs on.
	c.Proxy = ""
	c.GetProxyFrom = ""
	c.ProxyMedia = false
	// Typing is not reported to the host (the backend drops it), and the
	// connector needs two more connections a login for it.
	c.DisableTyping = true
	c.LogRedactedLoginResponses = false
	// The pages of the inbox, the first included
	// (metaworker.SettingInboxPages): 1 is the first page only (the
	// connector's chat backfill is off below 2,
	// ThreadBackfillConfig.Enabled). No limit of concurrent backfills across
	// logins: a worker has one login.
	c.ThreadBackfill.BatchCount = s.InboxPages
	c.ThreadBackfill.MaxConcurrent = 0
	// The connector has no ValidateConfig: PostProcess parses the display
	// name template (the example's, "{{or .DisplayName .Username "Unknown
	// user"}}", with no suffix), which is what reading its config checks.
	if err := c.PostProcess(); err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	return ic, nil
}

// hooks are what the backend needs to know of the Instagram connector
// (metaworker.Hooks: the self id, the send key, the refusal, the rate
// limit), with its flow, its codes of a session Instagram ended (state.go)
// and its identifier scheme. No IsChannel: the connector bridges Instagram's
// direct chats and groups (its inbox's threads).
func hooks() backend.Hooks {
	return metaworker.Hooks(metaworker.Network{
		Flows:            signInFlows,
		EndedCodes:       endedCodes,
		IdentifierScheme: identifierScheme,
	})
}

func main() {
	metaworker.Main(backend.Worker{
		Name:         "chat_instagram",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream:     metaworker.Upstream(nil),
		BridgeID:     "instagram",
		NewConnector: newConnector,
		Hooks:        hooks(),
	})
}

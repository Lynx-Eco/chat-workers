// SPDX-License-Identifier: AGPL-3.0-or-later

// chat_slack is the Slack worker: the backend with the upstream
// mautrix-slack connector (and its fork of slack-go under it), unmodified.
// One connection is one Slack user in one workspace (team): the principal is
// the connector's login id, "<team id>-<user id>" (slackid.MakeUserLoginID).
// It is registered in the engine (src/providers/chat_slack.rs) and runs only
// where a host's allowlist names it; whether an image carries it is the
// release's choice (see go/README.md, "The Slack worker").
//
//	chat_slack --socket-path <path>
//	chat_slack --source-commit    print the stamped commit and exit 0
//
// Settings from Init (none required; Slack has no app id or secret):
// max_initial_conversations (default 15, a decimal count from 0 to 1000:
// the conversations the connector lists at each connect and makes rooms
// for, readConversationCount), and the history settings every worker takes
// (backend.ReadHistory). device_name is not read: the connector has no
// device name (Slack is told nothing of the client but the mautrix-go user
// agent the connector sets). Custom emoji reactions are reported as their
// ":shortcode:", not as images.
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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/slack-go/slack"
	"go.mau.fi/mautrix-slack/pkg/connector"
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
	slackVersion   = "v0.2611.0"
	// slackgoVersion is the version of github.com/beeper/slackgo, the fork
	// the connector requires in place of github.com/slack-go/slack (go.mod's
	// replace).
	slackgoVersion = "v0.0.0-20261109170826-c51406334f5e"
)

// The conversations the connector makes rooms for at each connect
// (readConversationCount).
const (
	settingMaxInitialConversations = "max_initial_conversations"
	// defaultInitialConversations: as chat_whatsapp's
	// max_initial_conversations and chat_telegram's sync_create_limit.
	defaultInitialConversations = 15
	// maxInitialConversations bounds the setting, as chat_telegram bounds
	// its sync limits: every room made costs one conversations.history
	// request for its import (README "History a connection gets by
	// itself"), at every connect that makes it.
	maxInitialConversations = 1000
)

// displaynameTemplate names a person as the connector's example does,
// without its " (bot)" suffix: PersonInfo.is_bot carries that (the
// connector sets the ghost's IsBot, chatinfo.go wrapUserInfo). The
// connector parses the template when its config is read, so one is required.
const displaynameTemplate = `{{or .Profile.DisplayName .Profile.RealName .Name}}`

func newConnector(init *contract.Init) (bridgev2.NetworkConnector, error) {
	count, err := readConversationCount(init)
	if err != nil {
		return nil, err
	}
	sc := &connector.SlackConnector{}
	example, data, _ := sc.GetConfig()
	if err := yaml.Unmarshal([]byte(example), data); err != nil {
		return nil, &backend.SettingsError{Code: "connector_config_failed"}
	}
	c := sc.Config
	c.DisplaynameTemplate = displaynameTemplate
	// A custom emoji reaction as an image would be an mxc:// URI in a
	// reaction's emoji (handleslack.go getReactionInfo); the host gets
	// ":name:" instead.
	c.CustomEmojiReactions = false
	c.WorkspaceAvatarInRooms = false
	c.MuteChannelsByDefault = false
	// Channels, private channels, direct messages and group DMs alike.
	c.DMOnly = false
	c.Backfill.ConversationCount = count
	// The connector parses its templates only when its config is read
	// (Config.UnmarshalYAML): read the final values again, as it would read
	// them from a file. It has no ValidateConfig; this is what reading its
	// config checks.
	raw, err := yaml.Marshal(&c)
	if err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	var final connector.Config
	if err := yaml.Unmarshal(raw, &final); err != nil {
		return nil, &backend.SettingsError{Code: "bad_settings"}
	}
	sc.Config = final
	return newSlackNetwork(sc), nil
}

// readConversationCount reads max_initial_conversations from Init: absent or
// blank is defaultInitialConversations; a decimal count from 0 to
// maxInitialConversations is taken; anything else (a sign, a space inside,
// the connector's own -1 for "every conversation of the boot response")
// refuses the start with bad_settings, so a typo does not make every chat
// a room, or none, unseen.
//
// It is the connector's backfill.conversation_count. At every connect
// (client.go connect, syncChannels) the connector of a user signed in with
// a browser token (xoxc-, both flows offered) lists that many of the
// user's conversations with users.conversations (public and private
// channels, group DMs and DMs, pages of up to 200, in the order Slack
// gives, not by recency), sorts them by their newest message
// (client.counts), and queues a resync of each: a channel or a private
// channel gets its room whether or not it has messages, a DM or a group DM
// only when Slack gave it a newest message. Every chat not listed gets its
// room when a message comes in it (handleslack.go wrapEvent, CreatePortal).
// 0 lists none: no room is made at a connect, and every chat waits for its
// next message.
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

// signInFlows are two of the connector's three flows, in its order, so
// that a host that begins a sign-in with no flow named (the engine takes
// the first one listed) signs in by email:
//   - email (connector.LoginFlowIDEmail, login-email.go): the email address,
//     a CAPTCHA when Slack asks for one (a WEB step), the six-character code
//     Slack mails, the workspace when the address has several, and the
//     two-factor code when the workspace asks for one. A workspace that does
//     not take email sign-in (single sign-on) is a WEB step instead.
//   - token (connector.LoginFlowIDAuthToken, login-cookie.go): a browser
//     session's xoxc- token and its "d" cookie, as one WEB step whose two
//     capture fields a host may also submit without a web view.
//
// Not the connector's "app" (connector.LoginFlowIDApp, login-app.go: a Slack
// app's bot and app-level tokens): its account is the app's bot user, not
// the user's own, as chat_telegram leaves out its "bot" flow. A
// SignInBegin of it is unknown_flow, like any id not listed.
//
// Named, so that a flow the connector adds later is not offered without a
// change here.
var signInFlows = []string{connector.LoginFlowIDEmail, connector.LoginFlowIDAuthToken}

// hooks are what the backend needs to know of the Slack connector.
func hooks() backend.Hooks {
	return backend.Hooks{
		// The account's own person id is the connector's user id of the
		// login's team and user, lower case, not the login id (self.go).
		SelfPersonID: selfPersonID,
		// The connector's own identifier of a person, so that the one the
		// host is given can be given back to StartChat (self.go).
		IdentifierScheme: identifierScheme,
		// Not SendKeyFormat: nothing of the host's key reaches Slack (the
		// connector sends no client_msg_id, handlematrix.go sendToSlack), so
		// the key is kept as the transaction id the framework stores on the
		// message's row (README "The Slack worker", sending).
		//
		// A sign-in refused at the gate: the email flow swallows the
		// framework's NewLogin error and asks for the email again
		// (login-email.go completeWorkspace), so the refusal is noted here,
		// synchronously, and the sign-in made to fail as principal_mismatch
		// (signin.go); a session the email flow opened at Slack for it is
		// signed out in the background (refused.go). The connector stores
		// nothing of a login before NewLogin: nothing is deleted.
		DropRefused: dropRefused,
		// A session Slack ended (revoked, invalid) is ENDED; an account
		// Slack deactivated or removed stays NEEDS_USER (state.go).
		EndedCodes:  endedCodes,
		SignInFlows: signInFlows,
		// A history fetch that ends on Slack's 429 is answered
		// rate_limited with Slack's Retry-After.
		RateLimitWait: rateLimitWait,
	}
}

// rateLimitWait is the worker's Hooks.RateLimitWait: an error of the
// connector's FetchMessages that is Slack's rate limit, with the wait Slack
// named. slack-go (beeper's fork, as the connector makes its client with
// OptionRetry(5): client.go makeSlackClient) waits out a 429 by itself and
// tries again up to five times; only when those are spent does the error
// reach the framework, as *slack.RateLimitedError with the last
// Retry-After (misc.go RateLimitedError). An API error "ratelimited" in a
// 200 response carries no wait.
func rateLimitWait(err error) (time.Duration, bool) {
	var rl *slack.RateLimitedError
	if errors.As(err, &rl) {
		return max(rl.RetryAfter, 0), true
	}
	var re slack.SlackErrorResponse
	if errors.As(err, &re) && (re.Err == "ratelimited" || re.Err == "rate_limited") {
		return 0, true
	}
	return 0, false
}

func main() {
	// The engine's build check, before anything else.
	if backend.AnswerSourceCommit(os.Args[1:], sourceCommit, os.Stdout) {
		os.Exit(0)
	}
	fs := flag.NewFlagSet("chat_slack", flag.ContinueOnError)
	socket := fs.String("socket-path", "", "host socket to connect to")
	if err := fs.Parse(os.Args[1:]); err != nil || *socket == "" {
		fmt.Fprintln(os.Stderr, `{"level":"error","code":"usage"}`)
		os.Exit(2)
	}
	w := backend.Worker{
		Name:         "chat_slack",
		Version:      version,
		SourceCommit: sourceCommit,
		Upstream: map[string]string{
			"maunium.net/go/mautrix":    mautrixVersion,
			"go.mau.fi/mautrix-slack":   slackVersion,
			"github.com/beeper/slackgo": slackgoVersion,
		},
		BridgeID:     "slack",
		NewConnector: newConnector,
		Hooks:        hooks(),
	}
	os.Exit(backend.Run(context.Background(), *socket, w, os.Stderr))
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package metaworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/backend"
)

// Network is what differs between the two Meta workers' hooks.
type Network struct {
	// Flows are the connector's sign-in flows the worker offers
	// (Hooks.SignInFlows), each a cookie flow of the user's own account.
	Flows []string
	// EndedCodes are the connector's codes of a session Meta ended
	// (Hooks.EndedCodes).
	EndedCodes []status.BridgeStateErrorCode
	// IdentifierScheme names a username in an identifier
	// (Hooks.IdentifierScheme).
	IdentifierScheme string
}

// Hooks are what the backend needs to know of a Meta connector: n's, and
// those both connectors share.
func Hooks(n Network) backend.Hooks {
	return backend.Hooks{
		// The account's own person id is its login id, the FBID (SelfPersonID).
		SelfPersonID:     SelfPersonID,
		IdentifierScheme: n.IdentifierScheme,
		// Never a number: the connectors take a numeric transaction id for
		// the message's offline threading id, a timestamp (SendKey).
		SendKeyFormat: SendKey,
		// A sign-in refused at the gate (another account than the
		// connection's): the connectors store nothing of a login before the
		// framework's NewLogin, where the gate refuses it, and return its
		// error, so it fails as principal_mismatch with nothing to delete;
		// the session is the user's own web session, never signed out
		// (EndRefused).
		EndRefused:  EndRefused,
		EndedCodes:  slices.Clone(n.EndedCodes),
		SignInFlows: slices.Clone(n.Flows),
		// A history fetch Meta answered 429 is rate_limited (RateLimitWait).
		RateLimitWait: RateLimitWait,
	}
}

// SelfPersonID is the workers' Hooks.SelfPersonID: the account's own person
// id is its login id, the account's FBID in decimal, which is also the user
// id of its ghost and the sender of its own messages in both connectors
// (connector/ids.go selfEventSender and makeEventSender, igconnector/ids.go
// the same; both connectors take their own ghost by
// networkid.UserID(UserLogin.ID), client.go connectWithTable and
// connectWithMailbox). In chat_messenger's end-to-end encrypted chats the
// sender is the user part of the msgr JID, the same FBID
// (connector/ids.go makeWAEventSender). For Instagram the FBID is the account's messaging
// FBID as Instagram's web page gives it (instameow GetOwnFBID,
// IGUserEIMU), not its Instagram user id (the login's metadata keeps that,
// IGID, and the ds_user_id cookie). It is the framework's default;
// set, so that the rule is in one place and checked: "" for a login id that
// is not a positive decimal int64 as metaid.MakeUserID writes one (then the
// framework takes the login id as it is).
func SelfPersonID(login *bridgev2.UserLogin) networkid.UserID {
	if login == nil || login.UserLogin == nil {
		return ""
	}
	id := string(login.ID)
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil || n <= 0 || strconv.FormatInt(n, 10) != id {
		return ""
	}
	return metaid.MakeUserID(n)
}

// sendKeyPrefix begins every transaction id the workers give a connector.
// It is not a digit: both connectors take an input transaction id that
// parses as a positive decimal int64 for the message's offline threading id
// (getOTID, connector/handlematrix.go and igconnector/handlematrix.go), the
// client's id of a message, which Meta's own clients make of the time of
// the send (methods.GenerateEpochID: milliseconds << 22). A send key turned
// into a number (backend.NumericSendKey, as Telegram's) would send every
// message with a made-up time in it. Any other id leaves the connector to
// make one at the send, as Meta's apps do.
const sendKeyPrefix = "lynx-"

// sendKeyHexLen is the number of hex digits after the prefix.
const sendKeyHexLen = 32

// SendKey is the workers' Hooks.SendKeyFormat, as chat_signal's: the host's
// send key mapped to a transaction id that is never a number
// (sendKeyPrefix and 32 lower-case hex digits of the key's SHA-256). The
// same key always gives the same id; a key already of that form is kept as
// it is. The id stays on the framework's row of the message (SendTxnID),
// where the backend finds the key of a stored message by mapping each
// journal key again and comparing (backend send.go sendKeyForTxn).
//
// Nothing of it reaches Meta but in one place: chat_messenger's reaction
// (and its removal) in an end-to-end encrypted chat is sent with the
// transaction id as its message id (connector/handlematrix.go
// HandleMatrixReaction, whatsmeow SendRequestExtra.ID). Every message, edit
// and deletion gets its id from the connector (a new offline threading id;
// the network's message id comes back in the answer).
//
// What keeps a send from going out twice is the journal, not the id: a key
// already sent or in flight is answered from the journal without the
// network (backend send.go runKeyed). Neither connector matches an incoming
// message to a send by its offline threading id (no
// RemoteMessageWithTransactionID), so a message whose send was answered
// UNKNOWN and did go out arrives as the account's own message with no
// send_key.
func SendKey(key string) networkid.RawTransactionID {
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

// EndRefused is the workers' Hooks.EndRefused for a login the gate refused
// (another account than the connection's): there is nothing to end, and it
// touches nothing (the log says refused_session_nothing_to_end).
//
// Why not DropRefused: both connectors' cookie flows ask Meta only
// (LoadMessagesPage, LoadIndex) and store nothing of the login before the
// framework's NewLogin (connector/login.go and igconnector/login.go
// loginWithCookies); what they store, they store after it returns (the
// Instagram reconnection state, PutReconnectionState; Messenger's
// end-to-end encrypted device, at the login's first connect), so for a
// refused login never. And they return NewLogin's error, wrapped with %w
// ("failed to save new login"), so the sign-in fails as principal_mismatch
// without the hook chat_slack needs for its email flow. That wrap is read
// in v0.2611.0's source (connector/login.go:240, igconnector/login.go:211)
// and kept by chatworker/go.mod's pin; no test here can reach it without
// Meta (loginWithCookies asks Meta before NewLogin), so a version bump must
// read it again.
//
// Why no end at Meta: the session is the user's own, a web session the
// user signed into in the sign-in's web view (or one whose cookies the user
// gave), not one the sign-in made for the worker; signing it out would
// sign that browser out. Neither connector has a remote logout either:
// Instagram's LogoutRemote only disconnects and forgets the cookies
// (igconnector/client.go); Messenger's also deletes its end-to-end
// encrypted device (whatsmeow's) from the local store and resets it
// (connector/client.go), and sends nothing to Meta either.
func EndRefused(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error {
	return backend.ErrNothingToEnd
}

// RateLimitWait is the workers' Hooks.RateLimitWait: an error of the
// connector's FetchMessages that is Meta's 429 (httpclient.ErrRateLimited,
// after messagix's own retries; also wrapped, as wrapMetaRespError and
// wrapInstagramRespError keep it in the chain). Meta names no wait the
// connectors read: 0.
func RateLimitWait(err error) (time.Duration, bool) {
	if errors.Is(err, httpclient.ErrRateLimited) {
		return 0, true
	}
	return 0, false
}

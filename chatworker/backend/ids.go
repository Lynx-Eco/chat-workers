// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

const ghostPrefix = "g_"

// formatGhost and parseGhost map a network user id to a ghost MXID and back.
// They must round-trip: mentions and power levels are keyed by MXID.
func formatGhost(user networkid.UserID) id.UserID {
	return id.NewUserID(ghostPrefix+id.EncodeUserLocalpart(string(user)), ServerName)
}

func parseGhost(user id.UserID) (networkid.UserID, bool) {
	local, server, err := user.Parse()
	if err != nil || server != ServerName || !strings.HasPrefix(local, ghostPrefix) {
		return "", false
	}
	dec, err := id.DecodeUserLocalpart(strings.TrimPrefix(local, ghostPrefix))
	if err != nil || dec == "" {
		return "", false
	}
	return networkid.UserID(dec), true
}

func hashHex(n int, parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0x1f})
	}
	return hex.EncodeToString(h.Sum(nil)[:n])
}

// eventID is a contract event id: the kind and its components joined with
// ':'. Each component has '%' and ':' escaped (%25, %3A), so two different
// component lists never give the same id, and a component with neither is
// kept as it is (the ids of the same event stay the same across runs).
func eventID(kind string, parts ...string) string {
	var b strings.Builder
	b.WriteString(kind)
	for _, p := range parts {
		b.WriteByte(':')
		if strings.ContainsAny(p, "%:") {
			p = idEscaper.Replace(p)
		}
		b.WriteString(p)
	}
	return b.String()
}

var idEscaper = strings.NewReplacer("%", "%25", ":", "%3A")

// genID is the id of an event the worker generates (its own PersonInfo,
// connection states, chat and person info by generation, abilities,
// removals, sign-in steps, edit echoes): eventID with the store's nonce as
// its first component (store.Nonce). Those ids are built from the store's
// own counters and generations, which start again in a new store file or a
// wiped one, while the host lands each event id once for the life of the
// connection's landed log: the nonce keeps them from naming an earlier
// store's events, and keeps them the same within a store. The ids of the
// network's events (eventID: messages, edits, reactions, deletions, reads,
// deliveries, history pages) are the network's own and carry no nonce, so a
// network event synced again after a store loss is the same event.
func (b *Backend) genID(kind string, parts ...string) string {
	return eventID(kind, append([]string{b.st.Nonce()}, parts...)...)
}

// num formats a number as an event id component.
func num[T ~int64 | ~uint64 | ~int32](n T) string {
	return strconv.FormatInt(int64(n), 10)
}

// Deterministic Matrix-side ids. They never leave the worker.

func roomIDFor(key networkid.PortalKey) id.RoomID {
	return id.RoomID("!r" + hashHex(12, string(key.ID), string(key.Receiver)) + ":" + ServerName)
}

// supersededRoomIDFor is the n-th room id a portal of key gets when the room
// id of key (roomIDFor) belongs to another portal (reid.go): deterministic,
// and never a roomIDFor (another prefix).
func supersededRoomIDFor(key networkid.PortalKey, n int) id.RoomID {
	return id.RoomID("!s" + hashHex(12, string(key.ID), string(key.Receiver), "superseded", strconv.Itoa(n)) + ":" + ServerName)
}

func messageEventID(key networkid.PortalKey, msg networkid.MessageID, part networkid.PartID) id.EventID {
	return id.EventID("$m" + hashHex(12, string(key.ID), string(key.Receiver), string(msg), string(part)) + ":" + ServerName)
}

func reactionEventID(room id.RoomID, msg networkid.MessageID, part networkid.PartID, sender networkid.UserID, emoji networkid.EmojiID) id.EventID {
	return id.EventID("$x" + hashHex(12, string(room), string(msg), string(part), string(sender), string(emoji)) + ":" + ServerName)
}

func otherEventID(parts ...string) id.EventID {
	return id.EventID("$o" + hashHex(12, parts...) + ":" + ServerName)
}

// Media URIs. A direct-media id from the connector, bytes the worker spooled,
// and a file the host placed for an outgoing attachment each get their own
// prefix; the contract's handle is derived from the same value.

const (
	mediaDirect   = "d_"
	mediaSpool    = "s_"
	mediaOutgoing = "o_"

	handleDirect = "direct:"
	handleSpool  = "spool:"
)

func directURI(mediaID networkid.MediaID) id.ContentURIString {
	return id.ContentURI{Homeserver: ServerName, FileID: mediaDirect + base64.RawURLEncoding.EncodeToString(mediaID)}.CUString()
}

func spoolURI(sha string) id.ContentURIString {
	return id.ContentURI{Homeserver: ServerName, FileID: mediaSpool + sha}.CUString()
}

func outgoingURI(rel string) id.ContentURIString {
	return id.ContentURI{Homeserver: ServerName, FileID: mediaOutgoing + base64.RawURLEncoding.EncodeToString([]byte(rel))}.CUString()
}

type mediaKind int

const (
	mediaUnknown mediaKind = iota
	mediaKindDirect
	mediaKindSpool
	mediaKindOutgoing
)

// parseMediaURI splits one of our URIs into its kind and value (media id,
// sha256 hex, or relative path).
func parseMediaURI(uri id.ContentURIString) (mediaKind, []byte, error) {
	parsed, err := uri.Parse()
	if err != nil {
		return mediaUnknown, nil, err
	}
	if parsed.Homeserver != ServerName {
		return mediaUnknown, nil, errors.New("foreign media server")
	}
	switch {
	case strings.HasPrefix(parsed.FileID, mediaDirect):
		b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(parsed.FileID, mediaDirect))
		return mediaKindDirect, b, err
	case strings.HasPrefix(parsed.FileID, mediaSpool):
		return mediaKindSpool, []byte(strings.TrimPrefix(parsed.FileID, mediaSpool)), nil
	case strings.HasPrefix(parsed.FileID, mediaOutgoing):
		b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(parsed.FileID, mediaOutgoing))
		return mediaKindOutgoing, b, err
	}
	return mediaUnknown, nil, errors.New("unknown media id")
}

// handleFor turns a media URI into the contract's attachment handle. Empty
// for anything that is not ours.
func handleFor(uri id.ContentURIString) string {
	kind, val, err := parseMediaURI(uri)
	if err != nil {
		return ""
	}
	switch kind {
	case mediaKindDirect:
		return handleDirect + base64.RawURLEncoding.EncodeToString(val)
	case mediaKindSpool:
		return handleSpool + string(val)
	}
	return ""
}

// parseHandle turns a contract handle back into its kind and value.
func parseHandle(handle string) (mediaKind, []byte, error) {
	switch {
	case strings.HasPrefix(handle, handleDirect):
		b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(handle, handleDirect))
		if err != nil || len(b) == 0 {
			return mediaUnknown, nil, errors.New("bad direct handle")
		}
		return mediaKindDirect, b, nil
	case strings.HasPrefix(handle, handleSpool):
		sha := strings.TrimPrefix(handle, handleSpool)
		if len(sha) != 64 || strings.Trim(sha, "0123456789abcdef") != "" {
			return mediaUnknown, nil, errors.New("bad spool handle")
		}
		return mediaKindSpool, []byte(sha), nil
	}
	return mediaUnknown, nil, errors.New("unknown handle")
}

// NumericSendKey maps any send key to a positive int64 in decimal, for
// networks whose transaction id must be one (Telegram's random_id). A key
// that already is a positive decimal int64 is kept as it is.
func NumericSendKey(key string) networkid.RawTransactionID {
	if n, err := strconv.ParseInt(key, 10, 64); err == nil && n > 0 && strconv.FormatInt(n, 10) == key {
		return networkid.RawTransactionID(key)
	}
	sum := sha256.Sum256([]byte(key))
	n := int64(binary.BigEndian.Uint64(sum[:8]) & 0x7fffffffffffffff)
	if n == 0 {
		n = 1
	}
	return networkid.RawTransactionID(strconv.FormatInt(n, 10))
}

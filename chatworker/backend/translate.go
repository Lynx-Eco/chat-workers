// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
)

// contentKind maps a Matrix message type (and event type) to the contract.
func contentKind(evtType event.Type, msg *event.MessageEventContent) contract.ContentKind {
	if evtType == event.EventSticker {
		return contract.ContentKind_CONTENT_KIND_STICKER
	}
	if evtType.Type == event.EventUnstablePollStart.Type {
		return contract.ContentKind_CONTENT_KIND_POLL
	}
	if msg == nil {
		return contract.ContentKind_CONTENT_KIND_OTHER
	}
	switch msg.MsgType {
	case event.MsgText, "":
		return contract.ContentKind_CONTENT_KIND_TEXT
	case event.MsgNotice:
		return contract.ContentKind_CONTENT_KIND_NOTICE
	case event.MsgEmote:
		return contract.ContentKind_CONTENT_KIND_EMOTE
	case event.MsgImage:
		return contract.ContentKind_CONTENT_KIND_IMAGE
	case event.MsgVideo:
		return contract.ContentKind_CONTENT_KIND_VIDEO
	case event.MsgAudio:
		if msg.MSC3245Voice != nil {
			return contract.ContentKind_CONTENT_KIND_VOICE
		}
		return contract.ContentKind_CONTENT_KIND_AUDIO
	case event.MsgFile:
		return contract.ContentKind_CONTENT_KIND_FILE
	case event.MsgLocation:
		return contract.ContentKind_CONTENT_KIND_LOCATION
	}
	return contract.ContentKind_CONTENT_KIND_OTHER
}

func isMedia(kind contract.ContentKind) bool {
	switch kind {
	case contract.ContentKind_CONTENT_KIND_IMAGE, contract.ContentKind_CONTENT_KIND_VIDEO,
		contract.ContentKind_CONTENT_KIND_AUDIO, contract.ContentKind_CONTENT_KIND_VOICE,
		contract.ContentKind_CONTENT_KIND_FILE, contract.ContentKind_CONTENT_KIND_STICKER:
		return true
	}
	return false
}

var matrixToLink = regexp.MustCompile(`https://matrix\.to/#/([^"'<>\s?]+)`)

// rewriteLinks replaces matrix.to links to people with lynx-person:<id>
// links and returns the person ids it found linked.
func rewriteLinks(html string, person func(id.UserID) string) (string, map[string]bool) {
	linked := map[string]bool{}
	out := matrixToLink.ReplaceAllStringFunc(html, func(s string) string {
		m := matrixToLink.FindStringSubmatch(s)
		dec, err := url.PathUnescape(m[1])
		if err != nil || !strings.HasPrefix(dec, "@") {
			return s
		}
		p := person(id.UserID(dec))
		if p == "" {
			return s
		}
		linked[p] = true
		return "lynx-person:" + url.PathEscape(p)
	})
	return out, linked
}

// parseGeoURI reads "geo:lat,lon[,alt][;params]".
func parseGeoURI(uri string) (lat, lon float64, ok bool) {
	rest, found := strings.CutPrefix(uri, "geo:")
	if !found {
		return 0, 0, false
	}
	if i := strings.IndexByte(rest, ';'); i >= 0 {
		rest = rest[:i]
	}
	parts := strings.Split(rest, ",")
	if len(parts) < 2 {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(parts[0], 64)
	lon, err2 := strconv.ParseFloat(parts[1], 64)
	return lat, lon, err1 == nil && err2 == nil
}

// contentInput is everything messageContent needs besides the content.
type contentInput struct {
	evtType event.Type
	msg     *event.MessageEventContent
	raw     map[string]any
	person  func(id.UserID) string
	// replySender is the person the framework adds to the mentions because
	// the message replies to them (empty when not a reply).
	replySender string
}

// messageContent translates one Matrix message part to the contract.
func messageContent(in contentInput) *contract.MessageContent {
	msg := in.msg
	kind := contentKind(in.evtType, msg)
	c := &contract.MessageContent{Kind: kind}
	if msg == nil {
		if body, ok := in.raw["body"].(string); ok {
			c.Text = body
		}
		return c
	}
	linked := map[string]bool{}
	if msg.Format == event.FormatHTML && msg.FormattedBody != "" {
		c.FormattedHtml, linked = rewriteLinks(msg.FormattedBody, in.person)
	}
	switch {
	case isMedia(kind):
		c.Text = msg.GetCaption()
		if c.Text == "" {
			c.FormattedHtml = ""
		}
		c.Attachments = []*contract.Attachment{attachmentOf(kind, msg)}
	case kind == contract.ContentKind_CONTENT_KIND_LOCATION:
		c.Text = msg.Body
		if lat, lon, ok := parseGeoURI(msg.GeoURI); ok {
			c.Location = &contract.Location{Latitude: lat, Longitude: lon, Label: msg.Body}
		}
	default:
		c.Text = msg.Body
	}
	if msg.Mentions != nil {
		seen := map[string]bool{}
		for _, u := range msg.Mentions.UserIDs {
			p := in.person(u)
			// The framework mentions an empty user when a history reply's
			// target is not stored yet: such entries are dropped.
			if p == "" || seen[p] {
				continue
			}
			seen[p] = true
			c.Mentions = append(c.Mentions, &contract.Mention{
				PersonId:       p,
				ImpliedByReply: p == in.replySender && !linked[p],
			})
		}
	}
	if len(msg.BeeperLinkPreviews) > 0 && msg.BeeperLinkPreviews[0] != nil {
		lp := msg.BeeperLinkPreviews[0]
		c.Extras = map[string]string{}
		if lp.MatchedURL != "" {
			c.Extras["link_preview.url"] = lp.MatchedURL
		}
		if lp.Title != "" {
			c.Extras["link_preview.title"] = lp.Title
		}
		if lp.Description != "" {
			c.Extras["link_preview.description"] = lp.Description
		}
	}
	if msg.Info != nil && msg.Info.MauGIF {
		if c.Extras == nil {
			c.Extras = map[string]string{}
		}
		c.Extras["gif"] = "true"
	}
	return c
}

func attachmentOf(kind contract.ContentKind, msg *event.MessageEventContent) *contract.Attachment {
	uri := msg.URL
	if msg.File != nil && msg.File.URL != "" {
		uri = msg.File.URL
	}
	a := &contract.Attachment{
		Handle:   handleFor(uri),
		Kind:     kind,
		Filename: msg.GetFileName(),
	}
	if info := msg.Info; info != nil {
		a.MimeType = info.MimeType
		if info.Size > 0 {
			a.SizeBytes = uint64(info.Size)
		}
		if info.Width > 0 {
			a.Width = uint32(info.Width)
		}
		if info.Height > 0 {
			a.Height = uint32(info.Height)
		}
		if info.Duration > 0 {
			a.DurationMs = uint32(info.Duration)
		}
		thumb := info.ThumbnailURL
		if info.ThumbnailFile != nil && info.ThumbnailFile.URL != "" {
			thumb = info.ThumbnailFile.URL
		}
		if thumb != "" {
			a.ThumbnailRef = handleFor(thumb)
		}
	}
	return a
}

// ReportedState is the ConnectionState the backend reports for a bridge
// state of a login it reports (nil: none): connectionState, except that a
// BAD_CREDENTIALS state whose code is one of h.EndedCodes is ENDED (the
// network ended the session), not NEEDS_USER. scheme is the identifier
// scheme (Hooks.IdentifierScheme, else the connector's network id).
func (h Hooks) ReportedState(st *status.BridgeState, scheme string) *contract.ConnectionState {
	cs := connectionState(st, scheme)
	if cs != nil && st.StateEvent == status.StateBadCredentials && slices.Contains(h.EndedCodes, st.Error) {
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
	}
	return cs
}

// connectionState maps the framework's bridge state to the contract.
func connectionState(st *status.BridgeState, scheme string) *contract.ConnectionState {
	cs := &contract.ConnectionState{
		Code:        stateCode(st),
		Principal:   string(st.RemoteID),
		AccountName: st.RemoteName,
	}
	switch st.StateEvent {
	case status.StateStarting:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_STARTING
	case status.StateUnconfigured:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT
	case status.StateConnecting:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTING
	case status.StateBackfilling:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_IMPORTING
	case status.StateConnected:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
	case status.StateTransientDisconnect:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_RECONNECTING
	case status.StateBadCredentials:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_NEEDS_USER
	case status.StateLoggedOut:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED
	case status.StateUnknownError, status.StateBridgeUnreachable:
		cs.State = contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR
	default:
		// RUNNING and anything newer: not a connection state.
		return nil
	}
	p := st.RemoteProfile
	cs.AccountIdentifiers = accountIdentifiers(p, scheme)
	if p.Avatar != "" {
		cs.AccountAvatarRef = handleFor(p.Avatar)
	}
	if cs.AccountName == "" {
		cs.AccountName = p.Name
	}
	return cs
}

// accountIdentifiers are the account's identifiers from its login's remote
// profile: its phone, e-mail and username.
func accountIdentifiers(p status.RemoteProfile, scheme string) []string {
	var ids []string
	if p.Phone != "" {
		phone := p.Phone
		if !strings.HasPrefix(phone, "+") {
			phone = "+" + phone
		}
		ids = append(ids, "tel:"+phone)
	}
	if p.Email != "" {
		ids = append(ids, "mailto:"+p.Email)
	}
	if p.Username != "" {
		ids = append(ids, scheme+":"+p.Username)
	}
	return ids
}

// stateCode is the connector's fixed error code when it gave one, else the
// state's own name.
func stateCode(st *status.BridgeState) string {
	if st.Error != "" {
		return string(st.Error)
	}
	return strings.ToLower(string(st.StateEvent))
}

func support(level event.CapabilitySupportLevel) contract.Support {
	switch {
	case level >= event.CapLevelFullySupported:
		return contract.Support_SUPPORT_YES
	case level == event.CapLevelPartialSupport:
		return contract.Support_SUPPORT_PARTIAL
	default:
		return contract.Support_SUPPORT_NO
	}
}

func boolSupport(b bool) contract.Support {
	if b {
		return contract.Support_SUPPORT_YES
	}
	return contract.Support_SUPPORT_NO
}

// chatAbilities maps the framework's room features to the contract. The
// whole document goes along as raw_json.
func chatAbilities(chatID string, rf *event.RoomFeatures) *contract.ChatAbilities {
	raw, _ := json.Marshal(rf)
	sum := sha256.Sum256(raw)
	a := &contract.ChatAbilities{
		ChatId:           chatID,
		Version:          rf.GetID() + "#" + hex.EncodeToString(sum[:6]),
		SendText:         contract.Support_SUPPORT_YES,
		MaxTextLength:    uint32(max(rf.MaxTextLength, 0)),
		Reply:            support(rf.Reply),
		Edit:             support(rf.Edit),
		Delete:           support(rf.Delete),
		React:            support(rf.Reaction),
		AllowedReactions: append([]string(nil), rf.AllowedReactions...),
		ReadReceipts:     boolSupport(rf.ReadReceipts),
		RawJson:          string(raw),
	}
	if rf.Reaction >= event.CapLevelFullySupported && len(rf.AllowedReactions) > 0 {
		a.React = contract.Support_SUPPORT_PARTIAL
	}
	if rf.EditMaxAge != nil {
		a.EditMaxAgeS = uint32(rf.EditMaxAge.Seconds())
	}
	if rf.DeleteMaxAge != nil {
		a.DeleteMaxAgeS = uint32(rf.DeleteMaxAge.Seconds())
	}
	kinds := map[contract.ContentKind]bool{}
	var maxBytes int64
	for msgType, ff := range rf.File {
		if ff == nil {
			continue
		}
		ok := false
		for _, level := range ff.MimeTypes {
			if level > event.CapLevelUnsupported {
				ok = true
				break
			}
		}
		if !ok {
			continue
		}
		k := fileKind(msgType)
		if k == contract.ContentKind_CONTENT_KIND_UNSPECIFIED {
			continue
		}
		kinds[k] = true
		if ff.MaxSize > maxBytes {
			maxBytes = ff.MaxSize
		}
	}
	for k := range kinds {
		a.SendAttachmentKinds = append(a.SendAttachmentKinds, k)
	}
	sort.Slice(a.SendAttachmentKinds, func(i, j int) bool { return a.SendAttachmentKinds[i] < a.SendAttachmentKinds[j] })
	if maxBytes > 0 {
		a.MaxAttachmentBytes = uint64(maxBytes)
	}
	return a
}

func fileKind(t event.CapabilityMsgType) contract.ContentKind {
	switch t {
	case event.MsgImage:
		return contract.ContentKind_CONTENT_KIND_IMAGE
	case event.MsgVideo, event.CapMsgGIF:
		return contract.ContentKind_CONTENT_KIND_VIDEO
	case event.MsgAudio:
		return contract.ContentKind_CONTENT_KIND_AUDIO
	case event.CapMsgVoice:
		return contract.ContentKind_CONTENT_KIND_VOICE
	case event.MsgFile:
		return contract.ContentKind_CONTENT_KIND_FILE
	case event.CapMsgSticker:
		return contract.ContentKind_CONTENT_KIND_STICKER
	}
	return contract.ContentKind_CONTENT_KIND_UNSPECIFIED
}

// role maps a power level to the contract's member role.
func role(level int) contract.MemberRole {
	switch {
	case level >= 95:
		return contract.MemberRole_MEMBER_ROLE_OWNER
	case level >= 50:
		return contract.MemberRole_MEMBER_ROLE_ADMIN
	default:
		return contract.MemberRole_MEMBER_ROLE_MEMBER
	}
}

func membership(m event.Membership) contract.Membership {
	switch m {
	case event.MembershipJoin:
		return contract.Membership_MEMBERSHIP_JOINED
	case event.MembershipInvite:
		return contract.Membership_MEMBERSHIP_INVITED
	case event.MembershipLeave, event.MembershipBan:
		return contract.Membership_MEMBERSHIP_LEFT
	}
	return contract.Membership_MEMBERSHIP_UNSPECIFIED
}

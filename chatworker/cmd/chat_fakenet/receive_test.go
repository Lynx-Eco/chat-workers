// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"strings"
	"testing"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

func TestHelloAndSignedOut(t *testing.T) {
	w := start(t, newState(t), opts{})
	h := w.hello
	if h.GetWorkerName() != "chat_fakenet" || h.GetLicence() != "AGPL-3.0-or-later" || !h.GetStoreEncrypted() ||
		h.GetSourceCommit() != "0123456789abcdef0123456789abcdef01234567" || h.GetWorkerVersion() != "0.1.0-test" ||
		h.GetUpstreamVersions()["maunium.net/go/mautrix"] != "v0.33.0" || h.GetLastUnackedSeq() != 0 {
		t.Fatalf("hello: %v", h)
	}
	w.wait("signed out", func(e *contract.ChatEvent) bool {
		return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_SIGNED_OUT
	})
	r := w.command(&contract.Command{Kind: &contract.Command_ListSignInFlows{ListSignInFlows: &contract.ListSignInFlows{}}})
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || len(r.GetFlows()) != 4 {
		t.Fatalf("flows: %v", r)
	}
	r = w.command(&contract.Command{Kind: &contract.Command_Send{Send: &contract.Send{ChatId: fakenet.DMAlice, SendKey: "k", Text: "x"}}})
	if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED || r.GetCode() != "not_connected" {
		t.Fatalf("send while signed out: %v", r)
	}
	w.shutdown()
}

// TestReceivePath checks every upward event kind the initial sync produces.
func TestReceivePath(t *testing.T) {
	w := start(t, newState(t), opts{})
	w.signIn()
	evs := w.events()

	// Two-part message: image by direct media, then the caption.
	m2 := messages(evs, fakenet.DMBob, "m2")
	if len(m2) != 2 {
		t.Fatalf("m2 parts: %v", m2)
	}
	img, capt := m2[0], m2[1]
	if img.GetPartId() != "img" || img.GetPartIndex() != 0 || capt.GetPartId() != "cap" || capt.GetPartIndex() != 1 {
		t.Fatalf("m2 part grouping: %v / %v", img, capt)
	}
	att := img.GetContent().GetAttachments()
	if img.GetContent().GetKind() != contract.ContentKind_CONTENT_KIND_IMAGE || len(att) != 1 ||
		!strings.HasPrefix(att[0].GetHandle(), "direct:") || att[0].GetMimeType() != "image/jpeg" ||
		att[0].GetSizeBytes() != fakenet.MediaSize || att[0].GetWidth() != 640 || !strings.HasPrefix(att[0].GetThumbnailRef(), "direct:") ||
		att[0].GetFilename() != "photo.jpg" {
		t.Fatalf("image part: %v", img)
	}
	if capt.GetContent().GetText() != "look at this" || img.GetSenderId() != string(fakenet.Bob) || img.GetFromMe() {
		t.Fatalf("caption part: %v", capt)
	}

	// Explicit mention, rewritten from the ghost link.
	m3 := messages(evs, fakenet.Group, "m3")
	if len(m3) != 1 {
		t.Fatalf("m3: %v", m3)
	}
	c3 := m3[0].GetContent()
	if len(c3.GetMentions()) != 1 || c3.GetMentions()[0].GetPersonId() != string(fakenet.Alice) || c3.GetMentions()[0].GetImpliedByReply() ||
		!strings.Contains(c3.GetFormattedHtml(), "lynx-person:u-alice") || strings.Contains(c3.GetFormattedHtml(), "matrix.to") {
		t.Fatalf("m3 mention: %v", c3)
	}

	// Reply: network ids, and the framework's added mention flagged.
	m4 := messages(evs, fakenet.Group, "m4")
	if len(m4) != 1 || m4[0].GetReplyTo().GetMessageId() != "m3" || m4[0].GetReplyTo().GetChatId() != fakenet.Group {
		t.Fatalf("m4 reply: %v", m4)
	}
	ms := m4[0].GetContent().GetMentions()
	if len(ms) != 1 || ms[0].GetPersonId() != string(fakenet.Bob) || !ms[0].GetImpliedByReply() {
		t.Fatalf("m4 implied mention: %v", ms)
	}
	m7 := messages(evs, fakenet.Group, "m7")
	if len(m7) != 1 || m7[0].GetThreadRootId() != "m3" {
		t.Fatalf("m7 thread: %v", m7)
	}
	// From me on another device, via the double puppet.
	m6 := messages(evs, fakenet.DMAlice, "m6")
	if len(m6) != 1 || !m6[0].GetFromMe() || m6[0].GetSenderId() != string(fakenet.Me) {
		t.Fatalf("m6 from me: %v", m6)
	}

	var edit *contract.Edit
	var added, removed *contract.Reaction
	var deleted *contract.MessageDeleted
	var readAlice, readMe *contract.ReadUpTo
	var delivered *contract.Delivered
	people := map[string]*contract.PersonInfo{}
	chats := map[string]*contract.ChatInfo{}
	abilities := map[string]*contract.ChatAbilities{}
	ids := map[string]bool{}
	for _, e := range evs {
		if ids[e.GetEventId()] {
			t.Fatalf("event id %q twice in one run", e.GetEventId())
		}
		ids[e.GetEventId()] = true
		switch {
		case e.GetEdit() != nil:
			edit = e.GetEdit()
		case e.GetReaction() != nil && e.GetReaction().GetRemoved():
			removed = e.GetReaction()
		case e.GetReaction() != nil:
			added = e.GetReaction()
		case e.GetMessageDeleted() != nil:
			deleted = e.GetMessageDeleted()
		case e.GetReadUpTo() != nil && e.GetReadUpTo().GetFromMe():
			readMe = e.GetReadUpTo()
		case e.GetReadUpTo() != nil:
			readAlice = e.GetReadUpTo()
		case e.GetDelivered() != nil:
			delivered = e.GetDelivered()
		case e.GetPersonInfo() != nil:
			people[e.GetPersonInfo().GetPersonId()] = e.GetPersonInfo()
		case e.GetChatInfo() != nil:
			chats[e.GetChatInfo().GetChatId()] = e.GetChatInfo()
		case e.GetChatAbilities() != nil:
			abilities[e.GetChatAbilities().GetChatId()] = e.GetChatAbilities()
		}
	}
	if edit == nil || edit.GetTarget().GetMessageId() != "m1" || edit.GetContent().GetText() != "Hello from Alice (edited)" || edit.GetEditorId() != string(fakenet.Alice) {
		t.Fatalf("edit: %v", edit)
	}
	if added == nil || removed == nil || added.GetTarget().GetMessageId() != "m3" || added.GetEmoji() != "👍" ||
		removed.GetTarget().GetMessageId() != "m3" || removed.GetSenderId() != string(fakenet.Bob) {
		t.Fatalf("reactions: %v / %v", added, removed)
	}
	if deleted == nil || deleted.GetTarget().GetMessageId() != "m5" {
		t.Fatalf("delete: %v", deleted)
	}
	if readAlice == nil || readAlice.GetUpToMessageId() != "m6" || readAlice.GetReaderId() != string(fakenet.Alice) {
		t.Fatalf("read by alice: %v", readAlice)
	}
	if readMe == nil || readMe.GetUpToMessageId() != "m4" || readMe.GetChatId() != fakenet.Group {
		t.Fatalf("read on my other device: %v", readMe)
	}
	if delivered == nil || delivered.GetTarget().GetMessageId() != "m6" || delivered.GetRecipientId() != string(fakenet.Alice) {
		t.Fatalf("delivered: %v", delivered)
	}
	alice := people[string(fakenet.Alice)]
	if alice == nil || alice.GetName() != "Alice Example" || !contains(alice.GetIdentifiers(), "tel:+15550000001") ||
		!strings.HasPrefix(alice.GetAvatarRef(), "spool:") {
		t.Fatalf("person alice: %v", alice)
	}
	dm := chats[fakenet.DMAlice]
	if dm == nil || dm.GetKind() != contract.ChatKind_CHAT_KIND_DIRECT || dm.GetOtherPersonId() != string(fakenet.Alice) || !dm.GetMembersComplete() {
		t.Fatalf("dm info: %v", dm)
	}
	g := chats[fakenet.Group]
	if g == nil || g.GetKind() != contract.ChatKind_CHAT_KIND_GROUP || g.GetName() != "Renamed club" || g.GetTopic() != "Scripted group" || len(g.GetMembers()) != 3 {
		t.Fatalf("group info: %v", g)
	}
	roles := map[string]contract.MemberRole{}
	for _, m := range g.GetMembers() {
		roles[m.GetPersonId()] = m.GetRole()
		if m.GetMembership() != contract.Membership_MEMBERSHIP_JOINED {
			t.Fatalf("member %v", m)
		}
	}
	if roles[string(fakenet.Me)] != contract.MemberRole_MEMBER_ROLE_OWNER || roles[string(fakenet.Alice)] != contract.MemberRole_MEMBER_ROLE_ADMIN ||
		roles[string(fakenet.Bob)] != contract.MemberRole_MEMBER_ROLE_MEMBER {
		t.Fatalf("roles %v", roles)
	}
	a := abilities[fakenet.Group]
	if a == nil || a.GetMaxTextLength() != 4096 || a.GetEdit() != contract.Support_SUPPORT_YES || a.GetRawJson() == "" ||
		len(a.GetSendAttachmentKinds()) != 1 || a.GetSendAttachmentKinds()[0] != contract.ContentKind_CONTENT_KIND_IMAGE {
		t.Fatalf("abilities: %v", a)
	}
	var connected bool
	for _, e := range evs {
		if cs := e.GetConnectionState(); cs != nil && cs.GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED && cs.GetPrincipal() == string(fakenet.Me) {
			connected = true
		}
	}
	if !connected {
		t.Fatalf("no CONNECTED state")
	}
	w.shutdown()
	assertCleanLogs(t, w)
}

// assertCleanLogs: the worker's stderr carries none of the run's message
// text, principal, phone numbers, shown codes or the store key.
func assertCleanLogs(t *testing.T, w *worker) {
	t.Helper()
	logs := stderrOf(t, w)
	if !strings.Contains(logs, `"code":"started"`) {
		t.Fatalf("no log lines captured")
	}
	for _, bad := range []string{"SECRETTEXT", "Hello from Alice", fakenet.ShownCode, "QRSECRET", `"u-me"`, "u-me", "+1555", "Alice Example", "a0a1a2a3", "oKGio6Sl"} {
		if strings.Contains(logs, bad) {
			t.Fatalf("logs contain %q", bad)
		}
	}
}

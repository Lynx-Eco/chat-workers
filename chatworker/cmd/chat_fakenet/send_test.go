// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/fakenet"
)

func sendCmd(chat, key, text string, deadline time.Duration) *contract.Command {
	c := &contract.Command{Kind: &contract.Command_Send{Send: &contract.Send{ChatId: chat, SendKey: key, Text: text}}}
	if deadline > 0 {
		c.DeadlineMs = time.Now().Add(deadline).UnixMilli()
	}
	return c
}

func deliveries(t *testing.T, state, txn string) int {
	t.Helper()
	recs, err := fakenet.ReadNetworkLog(state)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range recs {
		if r.Kind == "deliver" && r.Txn == txn {
			n++
		}
	}
	return n
}

func expect(t *testing.T, r *contract.CommandReply, outcome contract.Outcome, code string) {
	t.Helper()
	if r.GetOutcome() != outcome || r.GetCode() != code {
		t.Fatalf("reply %v, want %v %q", r, outcome, code)
	}
}

func TestSendPath(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{script: &fakenet.Script{HangMs: 3000}})
	w.signIn()

	r := w.command(sendCmd(fakenet.DMAlice, "8200000000001", "on my way", 0))
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	if r.GetRemoteMessageId() == "" || r.GetSendKey() != "8200000000001" || r.GetDeduplicated() {
		t.Fatalf("send: %v", r)
	}
	echo := w.wait("echo", func(e *contract.ChatEvent) bool { return e.GetMessage().GetSendKey() == "8200000000001" }).GetMessage()
	if echo.GetMessageId() != r.GetRemoteMessageId() || !echo.GetFromMe() || echo.GetContent().GetText() != "on my way" || echo.GetChatId() != fakenet.DMAlice {
		t.Fatalf("echo: %v", echo)
	}
	r2 := w.command(sendCmd(fakenet.DMAlice, "8200000000001", "on my way", 0))
	expect(t, r2, contract.Outcome_OUTCOME_OK, "")
	if !r2.GetDeduplicated() || r2.GetRemoteMessageId() != r.GetRemoteMessageId() {
		t.Fatalf("repeat: %v", r2)
	}
	if n := deliveries(t, state, "8200000000001"); n != 1 {
		t.Fatalf("%d deliveries of a key sent twice", n)
	}
	// The same key for another chat, other content or another kind of
	// command is not a repeat: refused, and nothing goes out.
	for _, c := range []*contract.Command{
		sendCmd(fakenet.DMBob, "8200000000001", "on my way", 0),
		sendCmd(fakenet.DMAlice, "8200000000001", "on my way!", 0),
		{Kind: &contract.Command_Edit{Edit: &contract.EditMessage{Target: ref(fakenet.DMAlice, r.GetRemoteMessageId(), ""), SendKey: "8200000000001", Text: "x"}}},
	} {
		rr := w.command(c)
		expect(t, rr, contract.Outcome_OUTCOME_REJECTED, "send_key_reused")
		if rr.GetSendKey() != "8200000000001" || rr.GetDeduplicated() {
			t.Fatalf("reused key: %v", rr)
		}
	}
	if n := deliveries(t, state, "8200000000001"); n != 1 {
		t.Fatalf("%d deliveries after the key was reused", n)
	}

	// Two concurrent sends of one key: one delivery, one deduplicated answer.
	var wg sync.WaitGroup
	replies := make([]*contract.CommandReply, 2)
	for i := range replies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			replies[i] = w.command(sendCmd(fakenet.DMBob, "conc-1", "together", 0))
		}()
	}
	wg.Wait()
	if replies[0].GetOutcome() != contract.Outcome_OUTCOME_OK || replies[1].GetOutcome() != contract.Outcome_OUTCOME_OK ||
		replies[0].GetDeduplicated() == replies[1].GetDeduplicated() || replies[0].GetRemoteMessageId() != replies[1].GetRemoteMessageId() {
		t.Fatalf("concurrent: %v / %v", replies[0], replies[1])
	}
	if n := deliveries(t, state, "conc-1"); n != 1 {
		t.Fatalf("%d deliveries of two concurrent sends", n)
	}

	r = w.command(sendCmd(fakenet.DMAlice, "k-certain", "please "+fakenet.MagicCertain, 0))
	if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED {
		t.Fatalf("certain: %v", r)
	}
	r = w.command(sendCmd(fakenet.DMAlice, "k-certain", "please "+fakenet.MagicCertain, 0))
	if r.GetOutcome() != contract.Outcome_OUTCOME_REJECTED {
		t.Fatalf("certain again: %v", r)
	}
	r = w.command(sendCmd(fakenet.DMAlice, "k-uncertain", fakenet.MagicUncertain, 0))
	if r.GetOutcome() != contract.Outcome_OUTCOME_UNKNOWN {
		t.Fatalf("uncertain: %v", r)
	}
	r = w.command(sendCmd(fakenet.DMAlice, "k-uncertain", fakenet.MagicUncertain, 0))
	expect(t, r, contract.Outcome_OUTCOME_UNKNOWN, "in_flight")

	// A hanging network call: unknown at the deadline, resolved later.
	began := time.Now()
	hang := sendCmd(fakenet.DMBob, "k-hang", fakenet.MagicHang, 2*time.Second)
	r = w.command(hang)
	took := time.Since(began)
	expect(t, r, contract.Outcome_OUTCOME_UNKNOWN, "deadline")
	if took < 1500*time.Millisecond || took > 2800*time.Millisecond {
		t.Fatalf("deadline answer after %v", took)
	}
	res := w.wait("send resolved", func(e *contract.ChatEvent) bool { return e.GetSendResolved().GetSendKey() == "k-hang" }).GetSendResolved()
	if res.GetCommandId() != hang.GetCommandId() || res.GetOutcome() != contract.Outcome_OUTCOME_OK || res.GetRemoteMessageId() == "" {
		t.Fatalf("resolved: %v", res)
	}
	r = w.command(sendCmd(fakenet.DMBob, "k-hang", fakenet.MagicHang, 0))
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || !r.GetDeduplicated() || r.GetRemoteMessageId() != res.GetRemoteMessageId() {
		t.Fatalf("after resolution: %v", r)
	}
	r = w.command(sendCmd("no-such-chat", "k-x", "x", 0))
	expect(t, r, contract.Outcome_OUTCOME_REJECTED, "unknown_chat")
	r = w.command(sendCmd(fakenet.DMBob, "", "x", 0))
	expect(t, r, contract.Outcome_OUTCOME_REJECTED, "missing_send_key")
	w.shutdown()
}

func TestSendCrashes(t *testing.T) {
	for _, c := range []struct {
		word       string
		deliveries int
	}{{fakenet.MagicCrashAfter, 1}, {fakenet.MagicCrashBefore, 0}} {
		t.Run(c.word, func(t *testing.T) {
			state := newState(t)
			w := start(t, state, opts{})
			w.signIn()
			ctx, cancel := w.ctx(10 * time.Second)
			_, err := w.s.Command(ctx, sendCmd(fakenet.DMAlice, "k-crash", c.word, 0))
			cancel()
			if err == nil {
				t.Fatalf("crashing send was answered")
			}
			if code, err := w.proc.Wait(10 * time.Second); err != nil || code != 3 {
				t.Fatalf("exit %d %v", code, err)
			}
			w2 := start(t, state, opts{})
			w2.wait("connected", func(e *contract.ChatEvent) bool {
				return e.GetConnectionState().GetState() == contract.ConnectionStateKind_CONNECTION_STATE_KIND_CONNECTED
			})
			r := w2.command(sendCmd(fakenet.DMAlice, "k-crash", c.word, 0))
			expect(t, r, contract.Outcome_OUTCOME_UNKNOWN, "in_flight_at_restart")
			if n := deliveries(t, state, "k-crash"); n != c.deliveries {
				t.Fatalf("%d deliveries, want %d", n, c.deliveries)
			}
			w2.shutdown()
		})
	}
}

func ref(chat, msg, part string) *contract.MessageRef {
	return &contract.MessageRef{ChatId: chat, MessageId: msg, PartId: part}
}

func TestEditDeleteReactRead(t *testing.T) {
	w := start(t, newState(t), opts{})
	w.signIn()
	r := w.command(&contract.Command{Kind: &contract.Command_Edit{Edit: &contract.EditMessage{Target: ref(fakenet.DMAlice, "m6", ""), SendKey: "e1", Text: "edited"}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	w.wait("edit echo", func(e *contract.ChatEvent) bool {
		return e.GetEdit().GetTarget().GetMessageId() == "m6" && e.GetEdit().GetContent().GetText() == "edited"
	})
	r = w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.Group, "m4", ""), SendKey: "r1", Emoji: "🎉"}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	w.wait("react echo", func(e *contract.ChatEvent) bool {
		return e.GetReaction().GetEmoji() == "🎉" && e.GetReaction().GetFromMe() && !e.GetReaction().GetRemoved()
	})
	r = w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.Group, "m4", ""), SendKey: "r2", Emoji: "🎉", Remove: true}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	w.wait("unreact echo", func(e *contract.ChatEvent) bool {
		return e.GetReaction().GetEmoji() == "🎉" && e.GetReaction().GetRemoved()
	})
	r = w.command(&contract.Command{Kind: &contract.Command_React{React: &contract.React{Target: ref(fakenet.Group, "m4", ""), SendKey: "r3", Emoji: "🎉", Remove: true}}})
	expect(t, r, contract.Outcome_OUTCOME_REJECTED, "unknown_reaction")

	// Deleting a two-part message reports both parts.
	r = w.command(&contract.Command{Kind: &contract.Command_Delete{Delete: &contract.DeleteMessage{Target: ref(fakenet.DMBob, "m2", ""), SendKey: "d1"}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	for _, part := range []string{"img", "cap"} {
		w.wait("deleted "+part, func(e *contract.ChatEvent) bool {
			t := e.GetMessageDeleted().GetTarget()
			return t.GetMessageId() == "m2" && t.GetPartId() == part
		})
	}
	r = w.command(&contract.Command{Kind: &contract.Command_MarkRead{MarkRead: &contract.MarkRead{ChatId: fakenet.Group, UpToMessageId: "m7"}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	w.wait("own read", func(e *contract.ChatEvent) bool {
		return e.GetReadUpTo().GetUpToMessageId() == "m7" && e.GetReadUpTo().GetFromMe()
	})
	w.shutdown()
}

func TestStartChatHistoryAttachments(t *testing.T) {
	state := newState(t)
	w := start(t, state, opts{})
	w.signIn()
	r := w.command(&contract.Command{Kind: &contract.Command_StartChat{StartChat: &contract.StartChat{Identifier: "tel:+15550000004"}}})
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	if r.GetChatId() != "dm-dave" || r.GetPersonId() != "u-dave" {
		t.Fatalf("start chat: %v", r)
	}
	w.wait("dave chat", func(e *contract.ChatEvent) bool { return e.GetChatInfo().GetChatId() == "dm-dave" })
	w.wait("dave person", func(e *contract.ChatEvent) bool { return e.GetPersonInfo().GetPersonId() == "u-dave" })
	r = w.command(&contract.Command{Kind: &contract.Command_ListContacts{ListContacts: &contract.ListContacts{Search: "bob"}}})
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || len(r.GetContacts()) != 1 || r.GetContacts()[0].GetPersonId() != "u-bob" {
		t.Fatalf("contacts: %v", r)
	}

	// 120 messages in pages of 50, 50, 20, ids preserved.
	cursor := ""
	seen := map[string]bool{}
	pages := 0
	for {
		cmd := &contract.Command{Kind: &contract.Command_FetchHistory{FetchHistory: &contract.FetchHistory{ChatId: fakenet.Group, Cursor: cursor, Count: 50}}}
		r = w.command(cmd)
		expect(t, r, contract.Outcome_OUTCOME_OK, "")
		pages++
		batch := w.wait("history batch", func(e *contract.ChatEvent) bool {
			return e.GetHistoryBatch().GetCommandId() == cmd.GetCommandId()
		}).GetHistoryBatch()
		for _, m := range batch.GetMessages() {
			if seen[m.GetMessageId()] {
				t.Fatalf("history message %s twice", m.GetMessageId())
			}
			seen[m.GetMessageId()] = true
		}
		if !r.GetHasMore() {
			break
		}
		cursor = r.GetCursor()
		if pages > 5 {
			t.Fatalf("too many pages")
		}
	}
	if pages != 3 || len(seen) != fakenet.HistoryCount || !seen[fakenet.HistoryID(0)] || !seen[fakenet.HistoryID(119)] {
		t.Fatalf("history: %d pages, %d messages", pages, len(seen))
	}
	var reply101 *contract.Message
	for _, e := range w.events() {
		for _, m := range e.GetHistoryBatch().GetMessages() {
			if m.GetMessageId() == fakenet.HistoryID(101) {
				reply101 = m
			}
		}
	}
	if reply101 == nil || reply101.GetReplyTo().GetMessageId() != fakenet.HistoryID(100) {
		t.Fatalf("history reply: %v", reply101)
	}
	for _, m := range reply101.GetContent().GetMentions() {
		if m.GetPersonId() == "" {
			t.Fatalf("empty mention in a history reply")
		}
	}
	w.wait("history reaction", func(e *contract.ChatEvent) bool {
		return e.GetReaction().GetTarget().GetMessageId() == fakenet.HistoryID(100)
	})

	// Attachments: the image of m2 by its direct handle.
	var handle string
	for _, m := range messages(w.events(), fakenet.DMBob, "m2") {
		if a := m.GetContent().GetAttachments(); len(a) == 1 {
			handle = a[0].GetHandle()
		}
	}
	fetch := func(h, name string, max uint64) *contract.CommandReply {
		return w.command(&contract.Command{Kind: &contract.Command_FetchAttachment{FetchAttachment: &contract.FetchAttachment{Handle: h, TargetName: name, MaxBytes: max}}})
	}
	r = fetch(handle, "img.bin", 1<<20)
	expect(t, r, contract.Outcome_OUTCOME_OK, "")
	want := fakenet.MediaBytes("img-1", fakenet.MediaSize)
	sum := sha256.Sum256(want)
	got, err := os.ReadFile(filepath.Join(state, "transfer", "img.bin"))
	if err != nil || r.GetBytesWritten() != fakenet.MediaSize || r.GetSha256Hex() != hex.EncodeToString(sum[:]) || string(got) != string(want) {
		t.Fatalf("fetch: %v (%v)", r, err)
	}
	expect(t, fetch(handle, "small.bin", 1000), contract.Outcome_OUTCOME_REJECTED, "too_large")
	if _, err := os.Stat(filepath.Join(state, "transfer", "small.bin")); err == nil {
		t.Fatalf("a refused fetch left a file")
	}
	expect(t, fetch(handle, "../escape.bin", 0), contract.Outcome_OUTCOME_REJECTED, "bad_target")
	expect(t, fetch(handle, "a/b", 0), contract.Outcome_OUTCOME_REJECTED, "bad_target")
	expect(t, fetch("spool:"+strings.Repeat("0", 64), "x.bin", 0), contract.Outcome_OUTCOME_REJECTED, "not_found")
	expect(t, fetch("bogus", "x.bin", 0), contract.Outcome_OUTCOME_REJECTED, "not_found")
	// The network refusing a direct id is not distinguishable from a failed download.
	expect(t, fetch("direct:bm8", "x.bin", 0), contract.Outcome_OUTCOME_REJECTED, "fetch_failed")
	// Alice's avatar was spooled when her profile arrived.
	var avatar string
	for _, e := range w.events() {
		if p := e.GetPersonInfo(); p.GetPersonId() == string(fakenet.Alice) {
			avatar = p.GetAvatarRef()
		}
	}
	r = fetch(avatar, "avatar.png", 0)
	if r.GetOutcome() != contract.Outcome_OUTCOME_OK || r.GetBytesWritten() == 0 {
		t.Fatalf("avatar fetch: %v", r)
	}
	w.shutdown()
}

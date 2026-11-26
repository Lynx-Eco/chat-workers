// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

// inflightSend lets a second command with the same key, arriving while the
// first is still running, wait for the first one's answer.
type inflightSend struct {
	done        chan struct{}
	reply       *contract.CommandReply
	chatID      string
	kind        string
	fingerprint string
}

// pendingSend is a keyed command whose framework event may resolve after
// the command was answered (deadline passed, handler backgrounded).
type pendingSend struct {
	mu        sync.Mutex
	commandID string
	key       string
	receiver  networkid.UserLoginID
	answered  bool // the command was answered OUTCOME_UNKNOWN
	// sync: the goroutine running QueueMatrixEvent will still report the
	// result; a status then is not the final word.
	sync     bool
	finished bool
	// epoch is the store's when the send began: nothing of it is written
	// or reported after a wipe (logout) since (store.Since).
	epoch uint64
}

func reply(cmdID string, outcome contract.Outcome, code string) *contract.CommandReply {
	return &contract.CommandReply{CommandId: cmdID, Outcome: outcome, Code: code}
}

func rejected(cmdID, code string) *contract.CommandReply {
	return reply(cmdID, contract.Outcome_OUTCOME_REJECTED, code)
}

func ok(cmdID string) *contract.CommandReply { return reply(cmdID, contract.Outcome_OUTCOME_OK, "") }

// statusKey is what a kept message status is kept under: the event id of
// its send and the epoch of the framework instance that reported it. A
// send's event id depends only on its key (txnEventID), so after a logout
// and a sign-in again a send of the same key has the same id as one of the
// stopped instance: kept under the event id alone, a late status of the
// stopped instance (a goroutine that outlived its stop) replaced the one
// the new instance had reported for its own send, which then read none
// (takeStatus drops a status of another epoch).
type statusKey struct {
	evtID id.EventID
	epoch uint64
}

// recordStatus keeps a message status that the framework instance of epoch
// reported, and resolves the send it is for when that was answered unknown.
// A status acts only on a send of its own epoch (ps.epoch), and a kept
// status is kept and read under its own epoch (statusKey, takeStatus); one
// of an instance stopped by a wipe is not kept at all, and logout clears
// the kept ones (forgetSends).
func (b *Backend) recordStatus(epoch uint64, evtID id.EventID, ms *bridgev2.MessageStatus) {
	b.statusMu.Lock()
	if epoch == b.st.Epoch() {
		cp := *ms
		b.statuses[statusKey{evtID, epoch}] = &cp
	}
	ps := b.waiting[evtID]
	b.statusMu.Unlock()
	// A status for a command already answered unknown resolves it.
	if ps != nil && ps.epoch == epoch && ms.Status != event.MessageStatusPending {
		ps.mu.Lock()
		late := ps.answered && !ps.sync
		ps.mu.Unlock()
		if late {
			b.resolveByStatus(evtID, ps, ms)
		}
	}
}

// resolveByStatus finishes a send that was answered unknown from a final
// status.
func (b *Backend) resolveByStatus(evtID id.EventID, ps *pendingSend, ms *bridgev2.MessageStatus) {
	ctx := context.Background()
	outcome, code := statusOutcome(ms)
	remote := ""
	if outcome == contract.Outcome_OUTCOME_OK {
		if br := b.bridge(); br != nil {
			if part, _ := br.DB.Message.GetPartByTxnID(ctx, ps.receiver, evtID, b.txnFor(ps.key)); part != nil {
				remote = string(part.ID)
			}
		}
	}
	b.finishSend(ctx, evtID, ps, outcome, code, remote)
}

// answeredPending is for a send answered unknown whose framework result is
// not final: from now on a status resolves it (recordStatus). A status that
// came before this point, the one the result was read with (taken) or one
// recorded since, would be missed by recordStatus, so it resolves the send
// here. recordStatus stores the status before it reads the flags, and this
// sets the flags before it reads the statuses, so one of the two sees the
// other; finishSend runs once either way.
func (b *Backend) answeredPending(evtID id.EventID, ps *pendingSend, taken *bridgev2.MessageStatus) {
	ps.mu.Lock()
	ps.answered = true
	ps.sync = false
	ps.mu.Unlock()
	if st := b.takeStatus(ps.epoch, evtID); st != nil {
		taken = st
	}
	if taken != nil && taken.Status != event.MessageStatusPending {
		b.resolveByStatus(evtID, ps, taken)
	}
}

// forgetSends is for logout: every send still waiting for its result ends
// silently (finishSend does nothing for it), so nothing of it is journaled
// or reported after the wipe. A send already past that check is held off by
// its epoch (finishSend).
func (b *Backend) forgetSends() {
	b.statusMu.Lock()
	waiting := b.waiting
	b.waiting = map[id.EventID]*pendingSend{}
	clear(b.statuses)
	b.statusMu.Unlock()
	for _, ps := range waiting {
		ps.mu.Lock()
		ps.finished = true
		ps.mu.Unlock()
	}
}

// takeStatus removes the status kept for evtID that the instance of epoch
// (the send's) reported, and returns it.
func (b *Backend) takeStatus(epoch uint64, evtID id.EventID) *bridgev2.MessageStatus {
	b.statusMu.Lock()
	defer b.statusMu.Unlock()
	k := statusKey{evtID, epoch}
	st := b.statuses[k]
	delete(b.statuses, k)
	return st
}

func statusOutcome(ms *bridgev2.MessageStatus) (contract.Outcome, string) {
	if ms.Status == event.MessageStatusSuccess {
		return contract.Outcome_OUTCOME_OK, ""
	}
	code := string(ms.ErrorReason)
	if ms.IsCertain {
		if code == "" {
			code = "rejected"
		}
		return contract.Outcome_OUTCOME_REJECTED, code
	}
	if code == "" {
		code = "network_error"
	}
	return contract.Outcome_OUTCOME_UNKNOWN, code
}

// resultOutcome maps the framework's synchronous result (and the status it
// sent meanwhile) to the contract. pending: the result is not final yet.
func resultOutcome(res bridgev2.EventHandlingResult, st *bridgev2.MessageStatus) (outcome contract.Outcome, code string, pending bool) {
	switch {
	case errors.Is(res.Error, bridgev2.ErrHandlerBackgrounded):
		return contract.Outcome_OUTCOME_UNKNOWN, "backgrounded", true
	case st != nil && st.Status != event.MessageStatusSuccess && st.Status != event.MessageStatusPending:
		o, c := statusOutcome(st)
		return o, c, false
	case res.Error != nil:
		ms := bridgev2.WrapErrorInStatus(res.Error)
		o, c := statusOutcome(&ms)
		if o == contract.Outcome_OUTCOME_OK {
			o, c = contract.Outcome_OUTCOME_UNKNOWN, "network_error"
		}
		return o, c, false
	case res.Queued && !res.Ignored:
		return contract.Outcome_OUTCOME_UNKNOWN, "pending", true
	case res.Success:
		return contract.Outcome_OUTCOME_OK, "", false
	}
	return contract.Outcome_OUTCOME_UNKNOWN, "no_result", false
}

// sendKeyForTxn maps a network transaction id found on a bridged message
// back to the host's send key, when the journal knows it.
func (b *Backend) sendKeyForTxn(ctx context.Context, txn string) string {
	var key string
	rows, err := b.st.DB().Query(ctx, `SELECT send_key FROM lynx_send_journal`)
	if err != nil {
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if rows.Scan(&k) == nil && string(b.txnFor(k)) == txn {
			key = k
			break
		}
	}
	return key
}

func (b *Backend) txnFor(key string) networkid.RawTransactionID {
	if b.hooks.SendKeyFormat != nil {
		return b.hooks.SendKeyFormat(key)
	}
	return networkid.RawTransactionID(key)
}

// currentLogin is the one signed-in login whose events may go upward.
func (b *Backend) currentLogin(ctx context.Context) *bridgev2.UserLogin {
	if b.bridge() == nil {
		return nil
	}
	user, err := b.bridge().GetExistingUserByMXID(ctx, OwnerMXID)
	if err != nil || user == nil {
		return nil
	}
	for _, l := range user.GetUserLogins() {
		if _, gone := b.loggedOut.Load(l.ID); gone {
			continue
		}
		if b.allowed(l.ID) {
			return l
		}
	}
	return nil
}

func (b *Backend) targetPart(ctx context.Context, login *bridgev2.UserLogin, ref *contract.MessageRef) *database.Message {
	if ref == nil || ref.GetMessageId() == "" {
		return nil
	}
	var p *database.Message
	if ref.GetPartId() != "" {
		p, _ = b.bridge().DB.Message.GetPartByID(ctx, login.ID, networkid.MessageID(ref.GetMessageId()), networkid.PartID(ref.GetPartId()))
	} else {
		p, _ = b.bridge().DB.Message.GetFirstPartByID(ctx, login.ID, networkid.MessageID(ref.GetMessageId()))
	}
	return p
}

// keyed is what a send-type command supplies: how to build the framework
// event, and how to report success upward.
type keyed struct {
	cmdID    string
	deadline time.Time
	key      string
	chatID   string
	kind     string
	// fingerprint is what the command asks for (commandFingerprint).
	fingerprint string
	build       func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal) (*event.Event, string)
	onOK        func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal, res bridgev2.EventHandlingResult, evt *event.Event) (remote string, ok bool)
}

// runKeyed runs one send-type command with the journal:
//   - a key already used for another chat, another kind of command or other
//     content is refused (send_key_reused), without the network;
//   - a key already sent answers OK, deduplicated, without the network;
//   - a key in flight answers UNKNOWN (in_flight, or in_flight_at_restart
//     when it was in flight when the worker last stopped);
//   - otherwise the row is written in_flight BEFORE the framework event is
//     queued, and finished when the result is known.
func (b *Backend) runKeyed(ctx context.Context, k keyed) *contract.CommandReply {
	withKey := func(r *contract.CommandReply) *contract.CommandReply { r.SendKey = k.key; return r }
	if k.key == "" {
		return rejected(k.cmdID, "missing_send_key")
	}
	b.sendMu.Lock()
	if inf := b.inflight[k.key]; inf != nil {
		b.sendMu.Unlock()
		if !sameCommand(k, inf.chatID, inf.kind, inf.fingerprint) {
			return withKey(rejected(k.cmdID, "send_key_reused"))
		}
		return withKey(b.waitInflight(k, inf))
	}
	row, err := b.st.JournalGet(ctx, k.key)
	if err != nil {
		b.sendMu.Unlock()
		return withKey(rejected(k.cmdID, "store_error"))
	}
	if row != nil {
		b.sendMu.Unlock()
		return withKey(journalAnswer(k, row, b.boot))
	}
	inf := &inflightSend{done: make(chan struct{}), chatID: k.chatID, kind: k.kind, fingerprint: k.fingerprint}
	b.inflight[k.key] = inf
	b.sendMu.Unlock()
	r := b.runKeyedFresh(ctx, k)
	withKey(r)
	inf.reply = r
	b.sendMu.Lock()
	delete(b.inflight, k.key)
	b.sendMu.Unlock()
	close(inf.done)
	return r
}

// sameCommand tells whether a key's earlier use asked for the same thing. A
// row written before fingerprints were kept is compared by chat and kind.
func sameCommand(k keyed, chatID, kind, fingerprint string) bool {
	return chatID == k.chatID && kind == k.kind && (fingerprint == "" || fingerprint == k.fingerprint)
}

// commandFingerprint is a digest of what a send-type command asks for: the
// command with its send key and the paths of its attachments left out (the
// host may place a file anew for each attempt of the same send).
func commandFingerprint(m proto.Message) string {
	c := proto.Clone(m)
	switch v := c.(type) {
	case *contract.Send:
		v.SendKey = ""
		for _, a := range v.Attachments {
			a.Path = ""
		}
	case *contract.EditMessage:
		v.SendKey = ""
	case *contract.DeleteMessage:
		v.SendKey = ""
	case *contract.React:
		v.SendKey = ""
	}
	raw, err := contract.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:16])
}

func journalAnswer(k keyed, row *store.JournalRow, boot uint64) *contract.CommandReply {
	cmdID := k.cmdID
	if !sameCommand(k, row.ChatID, row.Kind, row.Fingerprint) {
		return rejected(cmdID, "send_key_reused")
	}
	switch row.State {
	case store.Sent:
		r := ok(cmdID)
		r.RemoteMessageId, r.Deduplicated = row.RemoteID, true
		return r
	case store.Rejected:
		return rejected(cmdID, row.Code)
	}
	if row.Boot != boot {
		return reply(cmdID, contract.Outcome_OUTCOME_UNKNOWN, "in_flight_at_restart")
	}
	return reply(cmdID, contract.Outcome_OUTCOME_UNKNOWN, "in_flight")
}

func (b *Backend) waitInflight(k keyed, inf *inflightSend) *contract.CommandReply {
	var timeout <-chan time.Time
	if !k.deadline.IsZero() {
		t := time.NewTimer(time.Until(k.deadline))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case <-inf.done:
	case <-timeout:
		return reply(k.cmdID, contract.Outcome_OUTCOME_UNKNOWN, "in_flight")
	}
	r := proto.Clone(inf.reply).(*contract.CommandReply)
	r.CommandId = k.cmdID
	if r.Outcome == contract.Outcome_OUTCOME_OK {
		r.Deduplicated = true
	}
	return r
}

func (b *Backend) runKeyedFresh(ctx context.Context, k keyed) *contract.CommandReply {
	// Everything the send writes or reports is written through this epoch:
	// after a wipe (a logout while it runs) nothing of it is (store.Since).
	epoch := b.st.Epoch()
	login := b.currentLogin(ctx)
	if login == nil {
		return rejected(k.cmdID, b.notConnectedCode())
	}
	portal, err := b.portalFor(ctx, login, k.chatID)
	if err != nil {
		return rejected(k.cmdID, "store_error")
	}
	if portal == nil {
		return rejected(k.cmdID, "unknown_chat")
	}
	if b.instanceOf(portal.Bridge).channelPortal(ctx, portal) {
		return rejected(k.cmdID, codeChannel)
	}
	evt, code := k.build(ctx, login, portal)
	if evt == nil {
		return rejected(k.cmdID, code)
	}
	if !k.deadline.IsZero() && time.Now().After(k.deadline) {
		return rejected(k.cmdID, "deadline_passed")
	}
	var inserted bool
	err = b.st.Since(epoch, func() error {
		var err error
		inserted, err = b.st.JournalBegin(ctx, store.JournalRow{Key: k.key, ChatID: k.chatID, Kind: k.kind, Boot: b.boot, CommandID: k.cmdID, Fingerprint: k.fingerprint})
		return err
	})
	if errors.Is(err, store.ErrWiped) {
		return rejected(k.cmdID, b.notConnectedCode())
	}
	if err != nil {
		return rejected(k.cmdID, "store_error")
	}
	if !inserted {
		row, _ := b.st.JournalGet(ctx, k.key)
		if row != nil {
			return journalAnswer(k, row, b.boot)
		}
		return rejected(k.cmdID, "store_error")
	}
	ps := &pendingSend{commandID: k.cmdID, key: k.key, receiver: login.ID, sync: true, epoch: epoch}
	b.statusMu.Lock()
	b.waiting[evt.ID] = ps
	b.statusMu.Unlock()

	type result struct {
		res bridgev2.EventHandlingResult
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				done <- result{bridgev2.EventHandlingResult{Error: fmt.Errorf("handler panicked")}}
			}
		}()
		// The login's own instance: after a logout it is stopped, and the
		// event never reaches the new one.
		br := login.Bridge
		done <- result{br.QueueMatrixEvent(br.BackgroundCtx, evt)}
	}()
	var timeout <-chan time.Time
	if !k.deadline.IsZero() {
		t := time.NewTimer(time.Until(k.deadline))
		defer t.Stop()
		timeout = t.C
	}
	// finalize reads the result with the status sent meanwhile, and returns
	// that status too. After a wipe since the send began it reads nothing
	// and reports nothing (onOK reports): the outcome is unknown.
	finalize := func(res bridgev2.EventHandlingResult) (outcome contract.Outcome, code, remote string, pending bool, st *bridgev2.MessageStatus) {
		err := b.st.Since(epoch, func() error {
			st = b.takeStatus(epoch, evt.ID)
			outcome, code, pending = resultOutcome(res, st)
			if outcome == contract.Outcome_OUTCOME_OK {
				var good bool
				remote, good = k.onOK(ctx, login, portal, res, evt)
				if !good {
					outcome, code = contract.Outcome_OUTCOME_UNKNOWN, "no_db_row"
				}
			}
			return nil
		})
		if err != nil {
			return contract.Outcome_OUTCOME_UNKNOWN, "logged_out", "", false, nil
		}
		return outcome, code, remote, pending, st
	}
	select {
	case r := <-done:
		outcome, code, remote, pending, st := finalize(r.res)
		if pending {
			b.answeredPending(evt.ID, ps, st)
			return reply(k.cmdID, outcome, code)
		}
		b.finishSend(ctx, evt.ID, ps, outcome, code, remote)
		out := reply(k.cmdID, outcome, code)
		out.RemoteMessageId = remote
		out.Deduplicated = r.res.Ignored && outcome == contract.Outcome_OUTCOME_OK
		return out
	case <-timeout:
		ps.mu.Lock()
		ps.answered = true
		ps.mu.Unlock()
		go func() {
			r := <-done
			outcome, code, remote, pending, st := finalize(r.res)
			if !pending {
				b.finishSend(context.Background(), evt.ID, ps, outcome, code, remote)
			} else {
				b.answeredPending(evt.ID, ps, st)
			}
		}()
		return reply(k.cmdID, contract.Outcome_OUTCOME_UNKNOWN, "deadline")
	}
}

// finishSend records the final state of a key once, and reports it with
// SendResolved when the command was already answered unknown. An uncertain
// result leaves the row in flight: nobody knows whether it went.
func (b *Backend) finishSend(ctx context.Context, evtID id.EventID, ps *pendingSend, outcome contract.Outcome, code, remote string) {
	ps.mu.Lock()
	if ps.finished {
		ps.mu.Unlock()
		return
	}
	ps.finished = true
	answered := ps.answered
	ps.mu.Unlock()
	b.statusMu.Lock()
	delete(b.waiting, evtID)
	b.statusMu.Unlock()
	// A wipe since the send began (a logout that came while it waited, even
	// after the check above): nothing of it is written or reported.
	_ = b.st.Since(ps.epoch, func() error {
		var err error
		switch outcome {
		case contract.Outcome_OUTCOME_OK:
			err = b.st.JournalFinish(ctx, ps.key, store.Sent, remote, "")
		case contract.Outcome_OUTCOME_REJECTED:
			err = b.st.JournalFinish(ctx, ps.key, store.Rejected, "", code)
		}
		if err != nil {
			b.log.Error("journal_finish_failed")
		}
		if answered {
			b.emit(eventID("resolved", ps.commandID), &contract.SendResolved{
				CommandId: ps.commandID, SendKey: ps.key, Outcome: outcome, Code: code, RemoteMessageId: remote,
			})
		}
		return nil
	})
}

func (b *Backend) notConnectedCode() string {
	if b.refused.Load() {
		return "other_account_kept"
	}
	return "not_connected"
}

func deadlineOf(cmd *contract.Command) time.Time {
	if cmd.GetDeadlineMs() <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(cmd.GetDeadlineMs())
}

func (b *Backend) txnEventID(key string) id.EventID {
	return id.EventID(database.NetworkTxnMXIDPrefix + string(b.txnFor(key)))
}

func (b *Backend) newEvent(portal *bridgev2.Portal, t event.Type, evtID id.EventID, content any) *event.Event {
	return &event.Event{
		Sender:    OwnerMXID,
		Type:      t,
		ID:        evtID,
		RoomID:    portal.MXID,
		Timestamp: time.Now().UnixMilli(),
		Content:   event.Content{Parsed: content},
	}
}

// outgoingHTML turns lynx-person:<id> links back into links to ghosts.
func outgoingHTML(html string) string {
	const prefix = "lynx-person:"
	var out strings.Builder
	for {
		i := strings.Index(html, prefix)
		if i < 0 {
			out.WriteString(html)
			return out.String()
		}
		out.WriteString(html[:i])
		rest := html[i+len(prefix):]
		end := strings.IndexAny(rest, "\"' <>")
		if end < 0 {
			end = len(rest)
		}
		out.WriteString("https://matrix.to/#/" + string(formatGhost(networkid.UserID(unescape(rest[:end])))))
		html = rest[end:]
	}
}

func unescape(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

// cmdSend builds the framework message for Send.
func (b *Backend) cmdSend(ctx context.Context, cmd *contract.Command, s *contract.Send) *contract.CommandReply {
	if strings.HasPrefix(s.GetText(), CommandPrefix) {
		return rejected(cmd.GetCommandId(), "bad_text")
	}
	if len(s.GetAttachments()) > 1 {
		return rejected(cmd.GetCommandId(), "too_many_attachments")
	}
	var content *event.MessageEventContent
	return b.runKeyed(ctx, keyed{
		cmdID: cmd.GetCommandId(), deadline: deadlineOf(cmd), key: s.GetSendKey(), chatID: s.GetChatId(), kind: "send",
		fingerprint: commandFingerprint(s),
		build: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal) (*event.Event, string) {
			content = &event.MessageEventContent{MsgType: event.MsgText, Body: s.GetText()}
			if h := s.GetFormattedHtml(); h != "" {
				content.Format, content.FormattedBody = event.FormatHTML, outgoingHTML(h)
			}
			if len(s.GetMentions()) > 0 {
				content.Mentions = &event.Mentions{}
				for _, m := range s.GetMentions() {
					if m.GetPersonId() != "" {
						content.Mentions.Add(formatGhost(networkid.UserID(m.GetPersonId())))
					}
				}
			}
			if att := s.GetAttachments(); len(att) == 1 {
				code := b.outgoingAttachment(content, att[0])
				if code != "" {
					return nil, code
				}
			}
			if rt := s.GetReplyToMessageId(); rt != "" {
				target, _ := b.bridge().DB.Message.GetFirstPartByID(ctx, login.ID, networkid.MessageID(rt))
				if target == nil {
					return nil, "unknown_reply_target"
				}
				content.RelatesTo = (&event.RelatesTo{}).SetReplyTo(target.MXID)
			}
			return b.newEvent(portal, event.EventMessage, b.txnEventID(s.GetSendKey()), content), ""
		},
		onOK: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal, res bridgev2.EventHandlingResult, evt *event.Event) (string, bool) {
			part, _ := b.bridge().DB.Message.GetPartByTxnID(ctx, login.ID, evt.ID, b.txnFor(s.GetSendKey()))
			if part == nil && res.EventID != "" {
				part, _ = b.bridge().DB.Message.GetPartByMXID(ctx, res.EventID)
			}
			if part == nil {
				return "", false
			}
			if !res.Ignored {
				b.echoSend(ctx, portal, login, part, content, s.GetSendKey())
			}
			return string(part.ID), true
		},
	})
}

// echoSend reports the network's accepted message upward, carrying the
// send key. The framework does not call SendMessage for a message that came
// from our side, so the backend reports it from the stored row.
func (b *Backend) echoSend(ctx context.Context, portal *bridgev2.Portal, login *bridgev2.UserLogin, part *database.Message, content *event.MessageEventContent, key string) {
	room, ok := b.roomKey(ctx, portal.MXID)
	if !ok {
		return
	}
	extra := &bridgev2.MatrixSendExtra{MessageMeta: part, Timestamp: part.Timestamp}
	// Reported through the epoch of the framework instance the portal
	// belongs to, as the instance's own events are.
	b.instanceOf(portal.Bridge).translate(ctx, room, translateInput{
		evtType: event.EventMessage, content: &event.Content{Parsed: content}, extra: extra, ts: part.Timestamp,
		sender: OwnerMXID, fromMe: true, sendKey: key,
	})
}

func (b *Backend) outgoingAttachment(content *event.MessageEventContent, a *contract.OutgoingAttachment) string {
	path, err := b.insideStateDir(a.GetPath())
	if err != nil {
		return "bad_attachment_path"
	}
	fi, err := os.Stat(path)
	if err != nil || !fi.Mode().IsRegular() {
		return "attachment_not_found"
	}
	rel := strings.TrimPrefix(path, b.stateDir+"/")
	switch a.GetKind() {
	case contract.ContentKind_CONTENT_KIND_IMAGE:
		content.MsgType = event.MsgImage
	case contract.ContentKind_CONTENT_KIND_VIDEO:
		content.MsgType = event.MsgVideo
	case contract.ContentKind_CONTENT_KIND_AUDIO, contract.ContentKind_CONTENT_KIND_VOICE:
		content.MsgType = event.MsgAudio
		if a.GetKind() == contract.ContentKind_CONTENT_KIND_VOICE {
			content.MSC3245Voice = &event.MSC3245Voice{}
		}
	default:
		content.MsgType = event.MsgFile
	}
	name := a.GetFilename()
	if name == "" {
		name = fi.Name()
	}
	if content.Body == "" {
		content.Body = name
	}
	content.FileName = name
	content.URL = outgoingURI(rel)
	content.Info = &event.FileInfo{MimeType: a.GetMimeType(), Size: int(fi.Size())}
	return ""
}

func (b *Backend) cmdEdit(ctx context.Context, cmd *contract.Command, e *contract.EditMessage) *contract.CommandReply {
	var target *database.Message
	return b.runKeyed(ctx, keyed{
		cmdID: cmd.GetCommandId(), deadline: deadlineOf(cmd), key: e.GetSendKey(), chatID: e.GetTarget().GetChatId(), kind: "edit",
		fingerprint: commandFingerprint(e),
		build: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal) (*event.Event, string) {
			target = b.targetPart(ctx, login, e.GetTarget())
			if target == nil {
				return nil, "unknown_message"
			}
			content := &event.MessageEventContent{MsgType: event.MsgText, Body: e.GetText()}
			if h := e.GetFormattedHtml(); h != "" {
				content.Format, content.FormattedBody = event.FormatHTML, outgoingHTML(h)
			}
			content.SetEdit(target.MXID)
			return b.newEvent(portal, event.EventMessage, b.txnEventID(e.GetSendKey()), content), ""
		},
		onOK: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal, res bridgev2.EventHandlingResult, evt *event.Event) (string, bool) {
			now := time.Now().UnixMilli()
			b.instanceOf(portal.Bridge).emitChat(login.ID, b.genID("edit", e.GetTarget().GetChatId(), string(target.ID), string(target.PartID), num(now)), &contract.Edit{
				Target:     &contract.MessageRef{ChatId: e.GetTarget().GetChatId(), MessageId: string(target.ID), PartId: string(target.PartID)},
				Content:    &contract.MessageContent{Kind: contract.ContentKind_CONTENT_KIND_TEXT, Text: e.GetText(), FormattedHtml: e.GetFormattedHtml()},
				EditedAtMs: now,
				EditorId:   b.selfFor(login.ID),
			})
			return string(target.ID), true
		},
	})
}

// cmdDelete deletes a message. With no part id the whole message goes: the
// framework is given one redaction (the network deletes the message), and
// one MessageDeleted per part is reported.
func (b *Backend) cmdDelete(ctx context.Context, cmd *contract.Command, d *contract.DeleteMessage) *contract.CommandReply {
	var parts []*database.Message
	return b.runKeyed(ctx, keyed{
		cmdID: cmd.GetCommandId(), deadline: deadlineOf(cmd), key: d.GetSendKey(), chatID: d.GetTarget().GetChatId(), kind: "delete",
		fingerprint: commandFingerprint(d),
		build: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal) (*event.Event, string) {
			target := b.targetPart(ctx, login, d.GetTarget())
			if target == nil {
				return nil, "unknown_message"
			}
			parts = []*database.Message{target}
			if d.GetTarget().GetPartId() == "" {
				if all, _ := b.bridge().DB.Message.GetAllPartsByID(ctx, login.ID, target.ID); len(all) > 0 {
					parts = all
				}
			}
			evt := b.newEvent(portal, event.EventRedaction, b.txnEventID(d.GetSendKey()), &event.RedactionEventContent{Redacts: target.MXID})
			evt.Redacts = target.MXID
			return evt, ""
		},
		onOK: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal, res bridgev2.EventHandlingResult, evt *event.Event) (string, bool) {
			now := time.Now().UnixMilli()
			for _, p := range parts {
				b.instanceOf(portal.Bridge).emitDeleted(login.ID, d.GetTarget().GetChatId(), p.ID, p.PartID, b.selfFor(login.ID), now)
			}
			return string(parts[0].ID), true
		},
	})
}

func (b *Backend) cmdReact(ctx context.Context, cmd *contract.Command, r *contract.React) *contract.CommandReply {
	var target *database.Message
	return b.runKeyed(ctx, keyed{
		cmdID: cmd.GetCommandId(), deadline: deadlineOf(cmd), key: r.GetSendKey(), chatID: r.GetTarget().GetChatId(), kind: "react",
		fingerprint: commandFingerprint(r),
		build: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal) (*event.Event, string) {
			target = b.targetPart(ctx, login, r.GetTarget())
			if target == nil {
				return nil, "unknown_message"
			}
			if !r.GetRemove() {
				content := &event.ReactionEventContent{RelatesTo: event.RelatesTo{Type: event.RelAnnotation, EventID: target.MXID, Key: r.GetEmoji()}}
				return b.newEvent(portal, event.EventReaction, b.txnEventID(r.GetSendKey()), content), ""
			}
			reactions, _ := b.bridge().DB.Reaction.GetAllToMessage(ctx, login.ID, target.ID)
			var mine *database.Reaction
			for _, re := range reactions {
				if (re.Emoji == r.GetEmoji() || string(re.EmojiID) == r.GetEmoji()) && login.Client.IsThisUser(ctx, re.SenderID) {
					mine = re
				}
			}
			if mine == nil {
				return nil, "unknown_reaction"
			}
			evt := b.newEvent(portal, event.EventRedaction, b.txnEventID(r.GetSendKey()), &event.RedactionEventContent{Redacts: mine.MXID})
			evt.Redacts = mine.MXID
			return evt, ""
		},
		onOK: func(ctx context.Context, login *bridgev2.UserLogin, portal *bridgev2.Portal, res bridgev2.EventHandlingResult, evt *event.Event) (string, bool) {
			b.instanceOf(portal.Bridge).emitReaction(login.ID, &contract.Reaction{
				Target:   &contract.MessageRef{ChatId: r.GetTarget().GetChatId(), MessageId: string(target.ID), PartId: string(target.PartID)},
				SenderId: b.selfFor(login.ID), FromMe: true, Emoji: r.GetEmoji(), Removed: r.GetRemove(),
				TimestampMs: time.Now().UnixMilli(),
			})
			return string(target.ID), true
		},
	})
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/store"
)

const (
	defaultHistoryPage = 50
	maxHistoryPage     = 200
	// maxBatchMessages bounds one HistoryBatch event; a larger framework
	// batch is split.
	maxBatchMessages = 100
	// maxBatchBytes bounds one HistoryBatch event's encoding, well under the
	// 4 MiB frame cap.
	maxBatchBytes = 1 << 20
)

type historyKey struct{}

// historyCtx travels in the context from FetchHistory through the framework
// to BatchSend.
type historyCtx struct {
	commandID string
	cursor    string
	hasMore   bool
	forward   bool
	emitted   bool
}

// fetchHistory asks the connector for one page with the host's count and
// cursor, and has the framework bridge it; BatchSend reports it as
// HistoryBatch events carrying the command id.
func (b *Backend) fetchHistory(ctx context.Context, cmdID string, f *contract.FetchHistory) *contract.CommandReply {
	login := b.currentLogin(ctx)
	if login == nil {
		return rejected(cmdID, b.notConnectedCode())
	}
	portal, err := b.portalFor(ctx, login, f.GetChatId())
	if err != nil {
		return rejected(cmdID, "store_error")
	}
	if portal == nil {
		return rejected(cmdID, "unknown_chat")
	}
	if b.instanceOf(portal.Bridge).channelPortal(ctx, portal) {
		return rejected(cmdID, codeChannel)
	}
	api, isBackfill := login.Client.(bridgev2.BackfillingNetworkAPI)
	if !isBackfill {
		return rejected(cmdID, "not_supported")
	}
	count := int(f.GetCount())
	if count <= 0 {
		count = defaultHistoryPage
	}
	count = min(count, maxHistoryPage)
	b.historyMu.Lock()
	defer b.historyMu.Unlock()
	var anchor *database.Message
	if f.GetForward() {
		if last, _ := b.bridge().DB.Message.GetLastNInPortal(ctx, portal.PortalKey, 1); len(last) > 0 {
			anchor = last[0]
		}
	} else {
		anchor, _ = b.bridge().DB.Message.GetFirstPortalMessage(ctx, portal.PortalKey)
	}
	_ = b.bridge().DB.BackfillTask.EnsureExists(ctx, portal.PortalKey, login.ID)
	task, _ := b.bridge().DB.BackfillTask.GetNextForPortal(ctx, portal.PortalKey, true)
	resp, err := api.FetchMessages(ctx, bridgev2.FetchMessagesParams{
		Portal: portal, Forward: f.GetForward(), Cursor: networkid.PaginationCursor(f.GetCursor()),
		AnchorMessage: anchor, Count: count, Task: task, AllowSlowFetch: true,
	})
	if err != nil {
		b.log.Warn("history_fetch_failed")
		return rejected(cmdID, "fetch_failed")
	}
	r := ok(cmdID)
	if resp == nil {
		return r
	}
	r.HasMore, r.Cursor = resp.HasMore, string(resp.Cursor)
	hc := &historyCtx{commandID: cmdID, cursor: string(resp.Cursor), hasMore: resp.HasMore, forward: f.GetForward()}
	msgs := portal.Internal().CutoffMessages(ctx, resp.Messages, resp.AggressiveDeduplication, f.GetForward(), anchor)
	if len(msgs) > 0 {
		hctx := context.WithValue(ctx, historyKey{}, hc)
		if err := portal.Internal().SendBackfill(hctx, login, msgs, f.GetForward(), resp.MarkRead, false, resp.CompleteCallback); err != nil {
			b.log.Warn("history_bridge_failed")
			return reply(cmdID, contract.Outcome_OUTCOME_UNKNOWN, "bridge_failed")
		}
	} else if resp.CompleteCallback != nil {
		resp.CompleteCallback()
	}
	if !hc.emitted {
		// Nothing new on this page: say so, so every FetchHistory has an
		// answer in the event stream too.
		b.instanceOf(portal.Bridge).emitChat(login.ID, eventID("history", f.GetChatId(), cmdID, "empty"), &contract.HistoryBatch{
			ChatId: f.GetChatId(), Cursor: hc.cursor, HasMore: hc.hasMore, Forward: hc.forward, CommandId: cmdID,
		})
	}
	return r
}

// batchSend receives history with event ids the framework already chose
// (they must be kept: the framework inserts its rows after this returns).
func (fw *instance) batchSend(ctx context.Context, roomID id.RoomID, req *mautrix.ReqBeeperBatchSend, extras []*bridgev2.MatrixSendExtra) (*mautrix.RespBeeperBatchSend, error) {
	ids := make([]id.EventID, 0, len(req.Events))
	for _, evt := range req.Events {
		ids = append(ids, evt.ID)
	}
	room, ok := fw.roomKey(ctx, roomID)
	if ok && fw.dropsRoom(ctx, roomID, room) {
		return &mautrix.RespBeeperBatchSend{EventIDs: ids}, nil
	}
	if !ok || !fw.allowed(networkid.UserLoginID(room.Receiver)) {
		return &mautrix.RespBeeperBatchSend{EventIDs: ids}, nil
	}
	hc, _ := ctx.Value(historyKey{}).(*historyCtx)
	var msgs []*contract.Message
	var later []translateInput
	for i, evt := range req.Events {
		extra := &bridgev2.MatrixSendExtra{}
		if i < len(extras) && extras[i] != nil {
			cp := *extras[i]
			extra = &cp
		}
		if extra.Timestamp.IsZero() {
			extra.Timestamp = time.UnixMilli(evt.Timestamp)
		}
		ensureParsed(&evt.Content, evt.Type)
		ghost, _ := parseGhost(evt.Sender)
		in := translateInput{
			evtType: evt.Type, content: &evt.Content, extra: extra, ts: extra.Timestamp,
			sender: evt.Sender, ghost: ghost, fromMe: evt.Sender == OwnerMXID,
		}
		msg, isMsg := evt.Content.Parsed.(*event.MessageEventContent)
		if (evt.Type == event.EventMessage || evt.Type == event.EventSticker) && (!isMsg || msg.RelatesTo.GetReplaceID() == "") {
			fw.ensureSenders(ctx, networkid.UserLoginID(room.Receiver), in)
			if m := fw.buildMessage(ctx, room, in); m != nil {
				msgs = append(msgs, m)
			}
			continue
		}
		// Reactions and the rest inside a batch have no place in a
		// HistoryBatch; they are reported as their own events after it.
		later = append(later, in)
	}
	forward := false
	if hc == nil && len(msgs) > 0 {
		forward = unaskedForward(req.Forward, fw.portalHeldMessages(ctx, roomID))
	}
	fw.emitBatches(room, msgs, hc, forward)
	for _, in := range later {
		fw.translate(ctx, room, in)
	}
	return &mautrix.RespBeeperBatchSend{EventIDs: ids}, nil
}

// unaskedForward is the forward flag of a batch the framework sends unasked:
// true only for a catch-up, messages newer than what the chat held
// (reqForward, and the chat held messages before this batch). The initial
// import of a chat whose room the framework just made (bridgev2
// doForwardBackfill with no last message) is forward in the framework's
// request too, but the chat held nothing: it is a chat's older history, and
// the host stores it as history, not as a replay of a gap (a replay is what
// later handling may act on). Anything not forward is history.
func unaskedForward(reqForward, held bool) bool { return reqForward && held }

// portalHeldMessages tells whether the framework's database holds a message
// of the portal of roomID. Asked in BatchSend, before the framework stores
// the batch's rows (bridgev2 sendBatch inserts them after BatchSend
// returns), so the batch itself is not counted. A failed read says no: the
// batch is then reported as history, the class nothing acts on.
func (fw *instance) portalHeldMessages(ctx context.Context, roomID id.RoomID) bool {
	br := fw.bridge()
	if br == nil {
		return false
	}
	p, err := br.DB.Portal.GetByMXID(ctx, roomID)
	if err != nil || p == nil {
		return false
	}
	last, err := br.DB.Message.GetLastNInPortal(ctx, p.PortalKey, 1)
	return err == nil && len(last) > 0
}

// emitBatches reports msgs (oldest first) as HistoryBatch events of at most
// maxBatchMessages and maxBatchBytes each, in order. A batch the framework
// sends unasked (hc nil: the newest messages of a chat whose room it just
// made, or a catch-up, BridgeConfig) has no command id and carries forward
// as given (unaskedForward). A batch that answers a FetchHistory carries
// the command's direction.
func (fw *instance) emitBatches(room *store.Room, msgs []*contract.Message, hc *historyCtx, forward bool) {
	if len(msgs) == 0 {
		return
	}
	var chunks [][]*contract.Message
	var cur []*contract.Message
	size := 0
	for _, m := range msgs {
		n := len(mustMarshal(m))
		if len(cur) > 0 && (len(cur) >= maxBatchMessages || size+n > maxBatchBytes) {
			chunks = append(chunks, cur)
			cur, size = nil, 0
		}
		cur = append(cur, m)
		size += n
	}
	chunks = append(chunks, cur)
	for i, chunk := range chunks {
		batch := &contract.HistoryBatch{ChatId: room.ChatID, Messages: chunk}
		last := i == len(chunks)-1
		first, end := chunk[0], chunk[len(chunk)-1]
		span := []string{first.MessageId, first.PartId, end.MessageId, end.PartId}
		if hc != nil {
			batch.CommandId, batch.Forward = hc.commandID, hc.forward
			batch.Cursor = hc.cursor
			batch.HasMore = hc.hasMore || !last
			hc.emitted = true
			// A page that answers a command is that command's event
			// (historyBatchID).
			span = append([]string{hc.commandID}, span...)
		} else {
			batch.Forward = forward
			batch.HasMore = !last
		}
		fw.emitChat(networkid.UserLoginID(room.Receiver), historyBatchID(room.ChatID, span...), batch)
	}
}

// historyBatchID is a HistoryBatch's event id: its chat and the span of its
// messages, the network's ids, so that a page the network sends again
// unasked (after a lost store) is the same event. A page that answers a
// FetchHistory also carries the command id first: the batch names its
// command, so two commands for the same page are two events (the host
// lands an id once, and a command whose page was taken for a repeat of
// another's would never get it), while the messages in them keep their
// ids and are the same messages.
func historyBatchID(chatID string, parts ...string) string {
	return eventID("history", append([]string{chatID}, parts...)...)
}

func mustMarshal(m *contract.Message) []byte {
	b, _ := contract.Marshal(m)
	return b
}

// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
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

// The codes of a FetchHistory refused by the worker itself, beyond those of
// every command. Each is OUTCOME_REJECTED: nothing of the page was bridged.
const (
	// codeRateLimited: the network asked for a wait before the next
	// request; CommandReply.retry_after_s carries it when it is known.
	codeRateLimited = "rate_limited"
	// codeFetchTimedOut: the network did not answer (or the connector
	// waited out a wait the worker could not see) until the command's
	// deadline, and the worker ended the request.
	codeFetchTimedOut = "fetch_timed_out"
	// codeWorkerBusy: another FetchHistory is running in the worker (one
	// the host may have stopped waiting for); nothing was asked of the
	// network.
	codeWorkerBusy = "worker_busy"
)

const (
	// maxInWorkerRateWait is the longest rate-limit wait the worker lets a
	// connector sleep out inside a FetchHistory. A longer one ends the
	// fetch at once as rate_limited with the wait, so that the host paces
	// (and the history lock is not held for it). mautrix-telegram retries a
	// request up to 5 times, sleeping out each FLOOD_WAIT under 5 minutes:
	// with this bound that is at most 5 x 16 s = 80 s, inside the engine's
	// default deadline of 120 s.
	maxInWorkerRateWait = 15 * time.Second
	// historyReplyMargin is how long before the command's deadline the
	// worker ends a fetch (a quarter of the time left when that is less),
	// so that its answer reaches the host while the host still waits.
	historyReplyMargin = 2 * time.Second
	// historyFetchBound bounds a fetch of a command with no deadline.
	historyFetchBound = 4 * time.Minute
)

// fetchHistory asks the connector for one page with the host's count and
// cursor, and has the framework bridge it; BatchSend reports it as
// HistoryBatch events carrying the command id.
//
// One fetch runs at a time (historyMu); another is refused worker_busy at
// once rather than waiting behind it, since the one running may be a fetch
// the host already gave up on (its deadline passed), and a command that
// waited for it would run after its own deadline.
//
// The network request is bounded by the command's deadline (fetchBound),
// and a rate-limit wait is answered rate_limited with the wait rather than
// slept out when it is longer than maxInWorkerRateWait or would run past
// that bound (rateWatch).
func (b *Backend) fetchHistory(ctx context.Context, cmd *contract.Command, f *contract.FetchHistory) *contract.CommandReply {
	cmdID := cmd.GetCommandId()
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
	if !b.historyMu.TryLock() {
		return rejected(cmdID, codeWorkerBusy)
	}
	defer b.historyMu.Unlock()
	bound, inTime := fetchBound(time.Now(), deadlineOf(cmd))
	if !inTime {
		return rejected(cmdID, "deadline_passed")
	}
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
	fctx, watch := b.watchFetch(ctx, bound)
	resp, err := api.FetchMessages(fctx, bridgev2.FetchMessagesParams{
		Portal: portal, Forward: f.GetForward(), Cursor: networkid.PaginationCursor(f.GetCursor()),
		AnchorMessage: anchor, Count: count, Task: task, AllowSlowFetch: true,
	})
	// Read before end, which cancels fctx: DeadlineExceeded only when the
	// bound ended the fetch (a stop of the worker cancels ctx instead).
	timedOut := errors.Is(fctx.Err(), context.DeadlineExceeded)
	cut := watch.end()
	if err != nil {
		code, wait := fetchFailure(err, cut, b.hooks.RateLimitWait, timedOut)
		switch code {
		case codeRateLimited:
			b.log.Warn("history_rate_limited", logx.N("retry_after_s", int64(wait)))
		case codeFetchTimedOut:
			b.log.Warn("history_fetch_timed_out")
		default:
			b.log.Warn("history_fetch_failed")
		}
		r := rejected(cmdID, code)
		r.RetryAfterS = wait
		return r
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
	// The page's events are in the outbox now (emitChat stores each before
	// it returns). The reply goes ahead of queued events on the wire
	// (wire.Endpoint.SendReply), so the host may read it before them: they
	// follow, at least once, also after a crash or a restart.
	return r
}

// fetchBound is when a fetch asked at now must end: historyReplyMargin
// before the command's deadline (a quarter of the time left when that is
// less), or historyFetchBound from now when the command has none. ok is
// false when the deadline has passed.
func fetchBound(now, deadline time.Time) (time.Time, bool) {
	if deadline.IsZero() {
		return now.Add(historyFetchBound), true
	}
	left := deadline.Sub(now)
	if left <= 0 {
		return time.Time{}, false
	}
	return deadline.Add(-min(historyReplyMargin, left/4)), true
}

// rateWatch ends one FetchMessages call early: at its bound, and as soon as
// the connector is about to sleep out a rate-limit wait that is longer than
// maxInWorkerRateWait or would end after the bound (onWait, through
// Hooks.WatchRateLimit). A wait it lets the connector sleep out is not
// reported: the fetch goes on.
type rateWatch struct {
	mu     sync.Mutex
	bound  time.Time
	cancel context.CancelFunc
	now    func() time.Time
	ended  bool
	// cut is set when a wait ended the fetch, with that wait.
	cut     bool
	cutWait time.Duration
}

// watchFetch is the context a fetch ending at bound gives the connector,
// and its watch. The caller ends the watch (end) when the fetch returns.
func (b *Backend) watchFetch(ctx context.Context, bound time.Time) (context.Context, *rateWatch) {
	fctx, cancel := context.WithDeadline(ctx, bound)
	w := &rateWatch{bound: bound, cancel: cancel, now: time.Now}
	if b.hooks.WatchRateLimit != nil {
		fctx = b.hooks.WatchRateLimit(fctx, w.onWait)
	}
	return fctx, w
}

// onWait is told of a wait the connector is about to sleep out.
func (w *rateWatch) onWait(wait time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ended || w.cut {
		return
	}
	// mautrix-telegram sleeps a FLOOD_WAIT out with a second more
	// (tgerr.FloodWait).
	if wait > maxInWorkerRateWait || w.now().Add(wait+time.Second).After(w.bound) {
		w.cut, w.cutWait = true, wait
		w.cancel()
	}
}

// end stops the watch (a later onWait does nothing), releases its context,
// and tells whether a wait ended the fetch, and that wait.
func (w *rateWatch) end() rateCut {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ended = true
	w.cancel()
	return rateCut{cut: w.cut, wait: w.cutWait}
}

// rateCut is how a rateWatch ended: cut when a rate-limit wait ended the
// fetch, with that wait.
type rateCut struct {
	cut  bool
	wait time.Duration
}

// fetchFailure is the code and the wait (whole seconds; 0: not known) of a
// FetchMessages that failed with err: rate_limited when a wait ended it
// (cut) or err is the network's rate limit (rateLimitWait); fetch_timed_out
// when the fetch's own bound ended it (timedOut); fetch_failed otherwise.
func fetchFailure(err error, cut rateCut, rateLimitWait func(error) (time.Duration, bool), timedOut bool) (string, uint32) {
	if cut.cut {
		return codeRateLimited, retryAfter(cut.wait)
	}
	if rateLimitWait != nil {
		if wait, ok := rateLimitWait(err); ok {
			if wait <= 0 {
				return codeRateLimited, 0
			}
			return codeRateLimited, retryAfter(wait)
		}
	}
	if timedOut {
		return codeFetchTimedOut, 0
	}
	return "fetch_failed", 0
}

// retryAfter is CommandReply.retry_after_s for a wait the network named:
// its whole seconds rounded up, and one more (mautrix-telegram's own margin
// when it sleeps one out), at least 1.
func retryAfter(wait time.Duration) uint32 {
	wait = max(wait, 0)
	s := wait / time.Second
	if wait%time.Second != 0 {
		s++
	}
	if s >= math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(s) + 1
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

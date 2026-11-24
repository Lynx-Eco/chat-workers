// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"lynx.invalid/ingest/chatworker/contract"
)

// Command handles one host command and returns its reply. It never panics:
// a panic in the framework or connector becomes OUTCOME_UNKNOWN.
func (b *Backend) Command(ctx context.Context, cmd *contract.Command) (r *contract.CommandReply) {
	id := cmd.GetCommandId()
	defer func() {
		if v := recover(); v != nil {
			b.log.Error("command_panicked")
			r = reply(id, contract.Outcome_OUTCOME_UNKNOWN, "internal_error")
		}
	}()
	if b.bridge() == nil {
		return rejected(id, b.notReadyCode())
	}
	switch k := cmd.Kind.(type) {
	case *contract.Command_ListSignInFlows:
		return b.listFlows(id)
	case *contract.Command_SignInBegin:
		return b.signInBegin(ctx, id, k.SignInBegin)
	case *contract.Command_SignInSubmit:
		return b.signInSubmit(ctx, id, k.SignInSubmit)
	case *contract.Command_SignInCancel:
		return b.signInCancel(id)
	case *contract.Command_Logout:
		return b.logout(ctx, id)
	case *contract.Command_Send:
		return b.cmdSend(ctx, cmd, k.Send)
	case *contract.Command_Edit:
		return b.cmdEdit(ctx, cmd, k.Edit)
	case *contract.Command_Delete:
		return b.cmdDelete(ctx, cmd, k.Delete)
	case *contract.Command_React:
		return b.cmdReact(ctx, cmd, k.React)
	case *contract.Command_MarkRead:
		return b.markRead(ctx, id, k.MarkRead)
	case *contract.Command_StartChat:
		return b.startChat(ctx, id, k.StartChat)
	case *contract.Command_FetchHistory:
		return b.fetchHistory(ctx, id, k.FetchHistory)
	case *contract.Command_FetchAttachment:
		return b.fetchAttachment(ctx, id, k.FetchAttachment)
	case *contract.Command_ListContacts:
		return b.listContacts(ctx, id, k.ListContacts)
	}
	return rejected(id, "unsupported")
}

// notReady is set when the framework could not be started (bad settings).
func (b *Backend) notReadyCode() string {
	if c := b.notReady.Load(); c != nil {
		return c.(string)
	}
	return "not_ready"
}

func (b *Backend) markRead(ctx context.Context, cmdID string, m *contract.MarkRead) *contract.CommandReply {
	login := b.currentLogin(ctx)
	if login == nil {
		return rejected(cmdID, b.notConnectedCode())
	}
	portal, err := b.portalFor(ctx, login, m.GetChatId())
	if err != nil || portal == nil {
		return rejected(cmdID, "unknown_chat")
	}
	if b.instanceOf(portal.Bridge).channelPortal(ctx, portal) {
		return rejected(cmdID, codeChannel)
	}
	target, _ := b.bridge().DB.Message.GetLastPartByID(ctx, login.ID, networkid.MessageID(m.GetUpToMessageId()))
	if target == nil {
		return rejected(cmdID, "unknown_message")
	}
	now := time.Now()
	evt := &event.Event{
		Sender: OwnerMXID, Type: event.EphemeralEventReceipt, RoomID: portal.MXID,
		Content: event.Content{Parsed: &event.ReceiptEventContent{
			target.MXID: {event.ReceiptTypeRead: {OwnerMXID: {Timestamp: now}}},
		}},
	}
	evt.Mautrix.EventSource = event.SourceEphemeral
	res := b.bridge().QueueMatrixEvent(ctx, evt)
	outcome, code, _ := resultOutcome(res, nil)
	if outcome == contract.Outcome_OUTCOME_OK {
		if room, ok := b.roomKey(ctx, portal.MXID); ok {
			b.instanceOf(portal.Bridge).emitRead(room, b.selfFor(login.ID), true, string(target.ID), now)
		}
	}
	return reply(cmdID, outcome, code)
}

// startChat resolves an identifier and creates the chat when needed.
func (b *Backend) startChat(ctx context.Context, cmdID string, s *contract.StartChat) *contract.CommandReply {
	login := b.currentLogin(ctx)
	if login == nil {
		return rejected(cmdID, b.notConnectedCode())
	}
	resolver, isResolver := login.Client.(bridgev2.IdentifierResolvingNetworkAPI)
	if !isResolver {
		return rejected(cmdID, "not_supported")
	}
	ident := strings.TrimSpace(s.GetIdentifier())
	ident = strings.TrimPrefix(ident, "tel:")
	ident = strings.TrimPrefix(ident, b.scheme()+":")
	if ident == "" {
		return rejected(cmdID, "bad_identifier")
	}
	resp, err := resolver.ResolveIdentifier(ctx, ident, true)
	if err != nil {
		return rejected(cmdID, errCode(err, "resolve_failed"))
	}
	if resp == nil {
		return rejected(cmdID, "not_found")
	}
	if resp.Ghost != nil && resp.UserInfo != nil {
		resp.Ghost.UpdateInfo(ctx, resp.UserInfo)
	}
	r := ok(cmdID)
	r.PersonId = string(resp.UserID)
	if resp.Chat != nil {
		portal := resp.Chat.Portal
		if portal == nil {
			portal, err = b.bridge().GetPortalByKey(ctx, resp.Chat.PortalKey)
			if err != nil {
				return rejected(cmdID, "store_error")
			}
		}
		if portal.MXID == "" {
			if err := portal.CreateMatrixRoom(ctx, login, resp.Chat.PortalInfo); err != nil {
				return reply(cmdID, contract.Outcome_OUTCOME_UNKNOWN, "create_failed")
			}
		}
		r.ChatId = b.chatIDOf(ctx, portal)
		if r.ChatId == "" {
			// A superseded room (reid.go): no chat id names it.
			return rejected(cmdID, "unknown_chat")
		}
	}
	return r
}

func (b *Backend) listContacts(ctx context.Context, cmdID string, l *contract.ListContacts) *contract.CommandReply {
	login := b.currentLogin(ctx)
	if login == nil {
		return rejected(cmdID, b.notConnectedCode())
	}
	var list []*bridgev2.ResolveIdentifierResponse
	var err error
	search := strings.TrimSpace(l.GetSearch())
	if searcher, isSearcher := login.Client.(bridgev2.UserSearchingNetworkAPI); isSearcher && search != "" {
		list, err = searcher.SearchUsers(ctx, search)
	} else if lister, isLister := login.Client.(bridgev2.ContactListingNetworkAPI); isLister {
		list, err = lister.GetContactList(ctx)
	} else {
		return rejected(cmdID, "not_supported")
	}
	if err != nil {
		return rejected(cmdID, errCode(err, "list_failed"))
	}
	r := ok(cmdID)
	needle := strings.ToLower(search)
	for _, c := range list {
		if c == nil || c.UserID == "" {
			continue
		}
		p := &contract.PersonInfo{PersonId: string(c.UserID)}
		if c.UserInfo != nil {
			if c.UserInfo.Name != nil {
				p.Name = c.UserInfo.Name
			}
			p.Identifiers = append(p.Identifiers, c.UserInfo.Identifiers...)
			if c.UserInfo.IsBot != nil {
				p.IsBot = *c.UserInfo.IsBot
			}
		}
		if needle != "" && !matches(p, needle) {
			continue
		}
		r.Contacts = append(r.Contacts, p)
	}
	return r
}

func matches(p *contract.PersonInfo, needle string) bool {
	if strings.Contains(strings.ToLower(p.GetName()), needle) || strings.Contains(strings.ToLower(p.PersonId), needle) {
		return true
	}
	for _, i := range p.Identifiers {
		if strings.Contains(strings.ToLower(i), needle) {
			return true
		}
	}
	return false
}

// logout ends the session at the network and leaves nothing of the account
// or of the people it talked to in the worker: the login and its chats are
// deleted through the framework, then every table of the store is emptied
// (store.Wipe) and the media removed, and ENDED is reported. Nothing in
// between is reported.
//
// The framework keeps what it read in memory (its people above all, which it
// never evicts and whose rows Wipe deletes), so the instance is stopped
// before the wipe and a new one is started on the emptied store: after a
// logout the worker is as it was at its first start, and can sign in again.
func (b *Backend) logout(ctx context.Context, cmdID string) *contract.CommandReply {
	b.loginMu.Lock()
	defer b.loginMu.Unlock()
	if si := b.signIn; si != nil {
		b.endSignIn(si)
	}
	b.releasePeople(ctx, false)
	principal := ""
	if login := b.currentLogin(ctx); login != nil {
		principal = string(login.ID)
	}
	// A stop of the worker waits for the wipe and its report (wipeMu).
	b.wipeMu.Lock()
	defer b.wipeMu.Unlock()
	b.silenced.Store(true)
	restarted, err := b.resetAll(ctx, "")
	b.silenced.Store(false)
	if err != nil {
		b.log.Error("wipe_failed")
	}
	b.emitState(&contract.ConnectionState{
		State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: "logged_out", Principal: principal,
	})
	if !restarted {
		// The logout is done, but this process cannot sign in again: it
		// answers, reports, and exits (ExitRestartFailed).
		b.requestExit()
	}
	switch {
	case err != nil:
		return reply(cmdID, contract.Outcome_OUTCOME_UNKNOWN, "wipe_failed")
	case !restarted:
		return reply(cmdID, contract.Outcome_OUTCOME_OK, "restart_failed")
	}
	return ok(cmdID)
}

// resetAll ends and deletes every stored login (the connection's, and one of
// another account found at start or let through by a connector that loads
// logins itself: each is this worker's session), stops the framework
// instance, empties the store with its outbox (Wipe) and the media, and
// starts a new instance on the emptied store. Each login is logged out at
// the network first, except one of the account own (when own is not ""): a
// wipe because of another account (wipeOther) ends that account's session,
// never the connection's own; a logout ("") ends them all. The caller holds
// loginMu and wipeMu and has silenced events. restarted is false when no new
// instance could be started: the caller then has the worker exit
// (requestExit).
func (b *Backend) resetAll(ctx context.Context, own string) (restarted bool, err error) {
	if br := b.bridge(); br != nil {
		if user, _ := br.GetExistingUserByMXID(ctx, OwnerMXID); user != nil {
			for _, l := range user.GetUserLogins() {
				b.loggedOut.Store(l.ID, true)
				b.dropLogin(ctx, l, own == "" || string(l.ID) != own)
			}
		}
		br.StopWithTimeout(5 * time.Second)
	}
	b.forgetSends()
	// An event begun before the wipe that waited on the outbox lock while it
	// ran is not stored after it (Emitter.EmitSince), so nothing of the old
	// login is in the outbox or the queue between the wipe and the reset.
	err = b.st.Wipe(ctx)
	b.out.Reset()
	if merr := b.wipeMedia(); err == nil {
		err = merr
	}
	b.forgetSelves()
	b.account.Store(b.init.GetPrincipal())
	b.loggedOut.Clear()
	// What was refused is gone: the connection may sign in.
	b.refused.Store(false)
	if b.newFramework(context.Background()) == nil {
		b.log.Error("restart_failed")
		return false, err
	}
	return true, err
}

// requestExit has the worker exit with ExitRestartFailed (serve), after the
// replies of the commands running and the queued events are sent.
func (b *Backend) requestExit() {
	b.exitOnce.Do(func() { close(b.exitReq) })
}

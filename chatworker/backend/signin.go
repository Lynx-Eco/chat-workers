// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
)

// signIn is one running login process.
type signIn struct {
	proc   bridgev2.LoginProcess
	flowID string
	step   *bridgev2.LoginStep
	cancel context.CancelFunc
	ctx    context.Context
	ended  bool
	// account is the connection's account when the sign-in began ("" when
	// not known yet): a sign-in that ends as another account is refused.
	account string
}

func fieldKind(t bridgev2.LoginInputFieldType) contract.FieldKind {
	switch t {
	case bridgev2.LoginInputFieldTypePhoneNumber:
		return contract.FieldKind_FIELD_KIND_PHONE
	case bridgev2.LoginInputFieldTypeEmail:
		return contract.FieldKind_FIELD_KIND_EMAIL
	case bridgev2.LoginInputFieldType2FACode, bridgev2.LoginInputFieldTypeCaptchaCode:
		return contract.FieldKind_FIELD_KIND_CODE
	case bridgev2.LoginInputFieldTypePassword, bridgev2.LoginInputFieldTypeToken:
		return contract.FieldKind_FIELD_KIND_PASSWORD
	case bridgev2.LoginInputFieldTypeURL:
		return contract.FieldKind_FIELD_KIND_URL
	case bridgev2.LoginInputFieldTypeSelect:
		return contract.FieldKind_FIELD_KIND_CHOICE
	}
	return contract.FieldKind_FIELD_KIND_TEXT
}

func showKind(t bridgev2.LoginDisplayType) contract.ShowKind {
	switch t {
	case bridgev2.LoginDisplayTypeQR:
		return contract.ShowKind_SHOW_KIND_QR
	case bridgev2.LoginDisplayTypeEmoji:
		return contract.ShowKind_SHOW_KIND_EMOJI
	case bridgev2.LoginDisplayTypeCode:
		return contract.ShowKind_SHOW_KIND_CODE
	}
	return contract.ShowKind_SHOW_KIND_NOTHING
}

func captureSource(t bridgev2.LoginCookieFieldSourceType) contract.WebCaptureSource {
	switch t {
	case bridgev2.LoginCookieTypeCookie:
		return contract.WebCaptureSource_WEB_CAPTURE_SOURCE_COOKIE
	case bridgev2.LoginCookieTypeLocalStorage:
		return contract.WebCaptureSource_WEB_CAPTURE_SOURCE_LOCAL_STORAGE
	case bridgev2.LoginCookieTypeRequestHeader:
		return contract.WebCaptureSource_WEB_CAPTURE_SOURCE_REQUEST_HEADER
	case bridgev2.LoginCookieTypeRequestBody:
		return contract.WebCaptureSource_WEB_CAPTURE_SOURCE_REQUEST_BODY
	}
	// "special": a value the app's own script must produce.
	return contract.WebCaptureSource_WEB_CAPTURE_SOURCE_SCRIPT
}

// stepEvent translates a framework login step. complete is handled apart.
func (b *Backend) stepEvent(flowID string, step *bridgev2.LoginStep) *contract.SignInStep {
	out := &contract.SignInStep{FlowId: flowID, StepId: step.StepID, Instructions: step.Instructions}
	switch step.Type {
	case bridgev2.LoginStepTypeUserInput:
		out.Kind = contract.SignInStepKind_SIGN_IN_STEP_KIND_FIELDS
		if p := step.UserInputParams; p != nil {
			for _, f := range p.Fields {
				out.Fields = append(out.Fields, &contract.SignInField{
					Id: f.ID, Kind: fieldKind(f.Type), Name: f.Name, Description: f.Description,
					Pattern: f.Pattern, Options: append([]string(nil), f.Options...),
				})
			}
		}
	case bridgev2.LoginStepTypeDisplayAndWait:
		out.Kind = contract.SignInStepKind_SIGN_IN_STEP_KIND_SHOW_AND_WAIT
		if p := step.DisplayAndWaitParams; p != nil {
			show := &contract.SignInShow{Kind: showKind(p.Type), Payload: p.Data, ImageUrl: p.ImageURL}
			if b.hooks.ShowExpiresIn != nil {
				show.ExpiresInS = b.hooks.ShowExpiresIn(step)
			}
			out.Show = show
		}
	case bridgev2.LoginStepTypeCookies:
		out.Kind = contract.SignInStepKind_SIGN_IN_STEP_KIND_WEB
		if p := step.CookiesParams; p != nil {
			web := &contract.SignInWeb{Url: p.URL, UserAgent: p.UserAgent, ExtractJs: p.ExtractJS, WaitForUrlPattern: p.WaitForURLPattern}
			for _, f := range p.Fields {
				wf := &contract.WebCaptureField{Id: f.ID, Required: f.Required, Pattern: f.Pattern}
				for _, s := range f.Sources {
					wf.Sources = append(wf.Sources, &contract.WebCapture{
						Source: captureSource(s.Type), Name: s.Name, CookieDomain: s.CookieDomain, RequestUrlRegex: s.RequestURLRegex,
					})
				}
				web.Fields = append(web.Fields, wf)
			}
			out.Web = web
		}
	case bridgev2.LoginStepTypeWebAuthn:
		out.Kind = contract.SignInStepKind_SIGN_IN_STEP_KIND_PASSKEY
		if p := step.WebAuthnParams; p != nil {
			raw, _ := json.Marshal(p)
			out.PasskeyRequestJson = string(raw)
		}
	default:
		return nil
	}
	return out
}

func (b *Backend) emitStep(s *contract.SignInStep) {
	n, err := b.st.Next(context.Background(), "signin_event")
	if err != nil {
		b.log.Error("counter_failed")
		return
	}
	b.log.Info("sign_in_step", logx.N("kind", int64(s.Kind)))
	b.emit(b.genID("signin", s.FlowId, s.StepId, num(n)), s)
}

func (b *Backend) emitFailed(flowID, stepID, code string) {
	b.emitStep(&contract.SignInStep{
		FlowId: flowID, StepId: stepID, Kind: contract.SignInStepKind_SIGN_IN_STEP_KIND_FAILED,
		Failed: &contract.SignInFailed{Code: code},
	})
}

// errCode is a fixed code for a login error: the framework's error code when
// the error carries one, else a generic one.
func errCode(err error, fallback string) string {
	if errors.Is(err, ErrOtherAccount) {
		return "principal_mismatch"
	}
	if errors.Is(err, ErrGateStore) {
		return "store_error"
	}
	var re bridgev2.RespError
	if errors.As(err, &re) && re.ErrCode != "" {
		return strings.ToLower(re.ErrCode)
	}
	return fallback
}

// offeredFlows are the connector's sign-in flows the worker offers
// (Hooks.SignInFlows), in the connector's order.
func (b *Backend) offeredFlows() []bridgev2.LoginFlow {
	all := b.bridge().Network.GetLoginFlows()
	if b.hooks.SignInFlows == nil {
		return all
	}
	var out []bridgev2.LoginFlow
	for _, f := range all {
		if slices.Contains(b.hooks.SignInFlows, f.ID) {
			out = append(out, f)
		}
	}
	return out
}

func (b *Backend) listFlows(cmdID string) *contract.CommandReply {
	r := ok(cmdID)
	for _, f := range b.offeredFlows() {
		r.Flows = append(r.Flows, &contract.SignInFlow{Id: f.ID, Name: f.Name, Description: f.Description})
	}
	return r
}

func (b *Backend) signInBegin(ctx context.Context, cmdID string, begin *contract.SignInBegin) *contract.CommandReply {
	if b.refused.Load() {
		return rejected(cmdID, "other_account_kept")
	}
	// A flow the worker does not offer is unknown to the host, whatever
	// the connector has.
	known := false
	for _, f := range b.offeredFlows() {
		known = known || f.ID == begin.GetFlowId()
	}
	if !known {
		return rejected(cmdID, "unknown_flow")
	}
	b.loginMu.Lock()
	defer b.loginMu.Unlock()
	if old := b.signIn; old != nil {
		b.endSignIn(old)
	}
	user, err := b.bridge().GetUserByMXID(ctx, OwnerMXID)
	if err != nil {
		return rejected(cmdID, "store_error")
	}
	account, err := b.connectionAccount(ctx)
	if err != nil {
		return rejected(cmdID, "store_error")
	}
	sctx, cancel := context.WithCancel(b.bridge().BackgroundCtx)
	proc, err := b.bridge().Network.CreateLogin(sctx, user, begin.GetFlowId())
	if err != nil {
		cancel()
		return rejected(cmdID, errCode(err, "flow_failed"))
	}
	si := &signIn{proc: proc, flowID: begin.GetFlowId(), cancel: cancel, ctx: sctx, account: account}
	b.signIn = si
	if account != "" {
		b.peopleMu.Lock()
		b.holdPeople = true
		b.peopleMu.Unlock()
	}
	// Re-linking an account that already has a login uses the override.
	var override *bridgev2.UserLogin
	for _, l := range user.GetUserLogins() {
		if b.init.GetPrincipal() == "" || string(l.ID) == b.init.GetPrincipal() {
			override = l
		}
	}
	var step *bridgev2.LoginStep
	switch p := proc.(type) {
	case bridgev2.LoginProcessWithParams:
		step, err = p.StartWithParams(sctx, bridgev2.LoginStartParams{Override: override})
	case bridgev2.LoginProcessWithOverride:
		if override != nil {
			step, err = p.StartWithOverride(sctx, override)
		} else {
			step, err = proc.Start(sctx)
		}
	default:
		step, err = proc.Start(sctx)
	}
	if err != nil {
		b.failSignIn(si, "", errCode(err, "start_failed"))
		return ok(cmdID)
	}
	b.advance(si, step)
	return ok(cmdID)
}

// advance reports a step; display-and-wait steps are waited on in the
// background, and every new step (a rotated QR too) is reported again.
func (b *Backend) advance(si *signIn, step *bridgev2.LoginStep) {
	if step == nil {
		b.failSignIn(si, "", "no_step")
		return
	}
	si.step = step
	if step.Type == bridgev2.LoginStepTypeComplete {
		b.completeSignIn(si, step)
		return
	}
	ev := b.stepEvent(si.flowID, step)
	if ev == nil {
		// client_http and anything newer: not supported by the contract.
		b.failSignIn(si, step.StepID, "unsupported_step")
		return
	}
	b.emitStep(ev)
	if step.Type == bridgev2.LoginStepTypeDisplayAndWait {
		waiter, isWaiter := si.proc.(bridgev2.LoginProcessDisplayAndWait)
		if !isWaiter {
			b.failSignIn(si, step.StepID, "unsupported_step")
			return
		}
		go func() {
			next, err := waiter.Wait(si.ctx)
			b.loginMu.Lock()
			defer b.loginMu.Unlock()
			if si.ended || b.signIn != si {
				// Ended meanwhile (a cancel, a new begin): a login of
				// another account the process stored after that is not
				// left in the store either.
				b.refuseIfOtherStored(si, step.StepID)
				return
			}
			if err != nil {
				b.failSignIn(si, step.StepID, errCode(err, "wait_failed"))
				return
			}
			b.advance(si, next)
		}()
	}
}

func (b *Backend) endSignIn(si *signIn) {
	if si.ended {
		return
	}
	si.ended = true
	si.cancel()
	si.proc.Cancel()
	if b.signIn == si {
		b.signIn = nil
	}
}

// failSignIn ends a sign-in that failed with code. When a login of another
// account than the connection's was stored by then (a connector that loads
// its new logins itself bypasses the gate: its login process may store the
// login and fail after, or be cancelled after), the failure is the same as
// completeSignIn's for such a login (refuseIfOtherStored): the host learns
// that, never code, which would tell it nothing of the login left in the
// store.
func (b *Backend) failSignIn(si *signIn, stepID, code string) {
	b.endSignIn(si)
	b.releasePeople(context.Background(), false)
	if b.refuseIfOtherStored(si, stepID) {
		return
	}
	b.emitFailed(si.flowID, stepID, code)
}

// How often, and how far apart, otherAccountStored reads the stored logins
// before it takes the store for unreadable.
const (
	storedReadAttempts = 3
	storedReadBackoff  = 100 * time.Millisecond
)

// storedVerdict is what the store says of the logins of a sign-in that
// ended (otherAccountStored).
type storedVerdict int

const (
	// storedNone: no login of another account than the sign-in's.
	storedNone storedVerdict = iota
	// storedOther: a login of another account is stored.
	storedOther
	// storedUnreadable: the stored logins could not be read.
	storedUnreadable
)

// otherAccountStored tells whether the store holds a login of another
// account than the one the sign-in began for, when that one is known. A
// failed read is tried again (storedReadAttempts); one that still fails is
// storedUnreadable: no evidence of another account, and no evidence that
// there is none.
func (b *Backend) otherAccountStored(si *signIn) storedVerdict {
	if si.account == "" {
		return storedNone
	}
	var ids []string
	var err error
	for attempt := range storedReadAttempts {
		if attempt > 0 {
			<-time.After(storedReadBackoff * time.Duration(attempt))
		}
		if ids, err = b.st.StoredLoginIDs(context.Background()); err == nil {
			break
		}
	}
	if err != nil {
		b.log.Error("login_read_failed")
		return storedUnreadable
	}
	for _, id := range ids {
		if id != si.account {
			return storedOther
		}
	}
	return storedNone
}

// refuseIfOtherStored ends a sign-in that is over (failed, cancelled,
// completed without a login, or ended before its process returned) by the
// rule of a login of another account stored past the gate, and tells
// whether it did:
//   - another account's login is stored: refuseStoredOther (everything
//     wiped; only that account's logins are logged out at the network);
//   - the stored logins cannot be read: refuseUnreadable (nothing wiped,
//     nobody logged out; other_account_kept, so that the host deletes the
//     store);
//   - neither: false, and the caller reports its own outcome.
func (b *Backend) refuseIfOtherStored(si *signIn, stepID string) bool {
	switch b.otherAccountStored(si) {
	case storedOther:
		b.refuseStoredOther(si, stepID)
		return true
	case storedUnreadable:
		b.refuseUnreadable(si, stepID)
		return true
	}
	return false
}

// refuseUnreadable ends a sign-in whose store could not be read for its
// logins: a read error is no evidence of another account, so nothing is
// wiped and no login is logged out at the network (the connection's own
// would be among them); but the store cannot be taken for clean either. The
// sign-in fails as other_account_kept and the connection ENDS with the same
// code: the host deletes the store itself. Until then this worker takes no
// new sign-in (refused).
func (b *Backend) refuseUnreadable(si *signIn, stepID string) {
	b.refused.Store(true)
	b.emitFailed(si.flowID, stepID, "other_account_kept")
	b.emitState(&contract.ConnectionState{
		State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: "other_account_kept",
	})
}

// refuseStoredOther ends a sign-in that stored a login of another account
// (completeSignIn, refuseIfOtherStored): everything goes, as at logout
// (every login ended and deleted, the store and the outbox wiped, a new
// framework instance), but only the other account's logins are logged out
// at the network, never the connection's own (wipeOther), and the sign-in
// fails and the connection ENDS with the codes of refusedAfterLoginCodes.
// A stop of the worker waits for it (wipeMu).
func (b *Backend) refuseStoredOther(si *signIn, stepID string) {
	b.wipeMu.Lock()
	defer b.wipeMu.Unlock()
	b.log.Warn("principal_mismatch_after_login")
	failure, ended, restarted := b.wipeOther(si.account)
	b.emitFailed(si.flowID, stepID, failure)
	b.emitState(&contract.ConnectionState{
		State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: ended,
	})
	if !restarted {
		b.requestExit()
	}
}

// wipeOther wipes everything because a login of another account than
// account is stored (a sign-in past the gate, or a start that found one):
// every login is ended and deleted, and only the logins of other accounts
// than account are logged out at the network; the store, the outbox and the
// media are wiped and a new framework instance started (resetAll). Events
// are silenced meanwhile: events of the other account may be among those
// queued (allowed() kept them back only once its account was known). It
// returns the sign-in failure and ENDED codes for how the wipe went
// (refusedAfterLoginCodes). When the wipe failed the worker takes no new
// sign-in until it is stopped (refused): the host deletes that store. The
// caller holds loginMu and wipeMu.
func (b *Backend) wipeOther(account string) (failure, ended string, restarted bool) {
	b.silenced.Store(true)
	restarted, err := b.resetAll(context.Background(), account)
	b.silenced.Store(false)
	if err != nil {
		b.log.Error("wipe_failed")
	}
	b.refused.Store(err != nil)
	failure, ended = refusedAfterLoginCodes(err)
	return failure, ended, restarted
}

// completeSignIn reports a finished sign-in. A login of another account is
// normally refused before it exists (the gate in gate.go: NewLogin fails and
// the sign-in fails as principal_mismatch). A connector that loads its new
// logins itself (NewLoginParams.LoadUserLogin) bypasses the gate: then the
// other account's login exists and may have synced (nothing of it went
// upward: allowed), so everything goes, as at logout (every login ended and
// deleted, the store and the outbox wiped, a new framework instance; only
// the other account's login logged out at the network), and the
// sign-in fails as principal_mismatch and the connection ENDED
// (other_account_wiped). principal_mismatch tells the host that nothing of
// the other account is kept, so it is reported only when the wipe succeeded:
// when it did not, rows of that account may still be in the store (and its
// events in the outbox, sent again at the next start), and the sign-in fails
// as other_account_kept and the connection ENDED with the same code, so that
// the host deletes the store itself.
func (b *Backend) completeSignIn(si *signIn, step *bridgev2.LoginStep) {
	ctx := context.Background()
	var loginID networkid.UserLoginID
	var ul *bridgev2.UserLogin
	if p := step.CompleteParams; p != nil {
		loginID, ul = p.UserLoginID, p.UserLogin
	}
	if ul == nil && loginID != "" {
		ul = b.bridge().GetCachedUserLoginByID(loginID)
	}
	si.ended = true
	si.cancel()
	if b.signIn == si {
		b.signIn = nil
	}
	if loginID == "" {
		b.releasePeople(ctx, false)
		// A process that stored another account's login and then completed
		// without naming it leaves that login like one that failed.
		if b.refuseIfOtherStored(si, step.StepID) {
			return
		}
		b.emitFailed(si.flowID, step.StepID, "no_login")
		return
	}
	if si.account != "" && string(loginID) != si.account {
		b.releasePeople(ctx, false)
		b.refuseStoredOther(si, step.StepID)
		return
	}
	b.loggedOut.Delete(loginID)
	b.account.Store(string(loginID))
	b.rememberSelf(ul)
	// The framework has kept the login (NewLogin returned): its account's
	// own PersonInfo, unless an event of the login sent it first
	// (emitChat), and then the people held during the sign-in.
	br := b.bridge()
	if ul != nil {
		br = ul.Bridge
	}
	b.instanceOf(br).ensureSelf(loginID, ul)
	b.releasePeople(ctx, true)
	done := &contract.SignInDone{Principal: string(loginID)}
	if ul != nil {
		done.AccountName = ul.RemoteName
	}
	b.emitStep(&contract.SignInStep{FlowId: si.flowID, StepId: step.StepID, Kind: contract.SignInStepKind_SIGN_IN_STEP_KIND_DONE, Done: done})
}

// refusedAfterLoginCodes are the sign-in failure code and the ENDED state's
// code of a login of another account refused after it was stored (the
// connector bypassed the gate), given how the wipe of everything went:
// principal_mismatch and other_account_wiped when it succeeded (nothing of
// that account is kept), else other_account_kept for both (the host must not
// take the store for clean).
func refusedAfterLoginCodes(wipeErr error) (failure, ended string) {
	if wipeErr != nil {
		return "other_account_kept", "other_account_kept"
	}
	return "principal_mismatch", "other_account_wiped"
}

// dropLogin deletes a login and its chats, after it logged the login out at
// the network when remote is set. Nothing about it is reported (allowed() is
// false for it).
func (b *Backend) dropLogin(ctx context.Context, ul *bridgev2.UserLogin, remote bool) {
	ups, _ := b.bridge().DB.UserPortal.GetAllForLogin(ctx, ul.UserLogin)
	ul.Delete(ctx, status.BridgeState{}, bridgev2.DeleteOpts{LogoutRemote: remote})
	var portals []*bridgev2.Portal
	for _, up := range ups {
		if p, err := b.bridge().GetExistingPortalByKey(ctx, up.Portal); err == nil && p != nil {
			portals = append(portals, p)
		}
	}
	bridgev2.DeleteManyPortals(ctx, portals, nil)
	b.forgetSelf(ul.ID)
}

func (b *Backend) signInSubmit(ctx context.Context, cmdID string, sub *contract.SignInSubmit) *contract.CommandReply {
	b.loginMu.Lock()
	defer b.loginMu.Unlock()
	si := b.signIn
	if si == nil || si.step == nil {
		return rejected(cmdID, "no_sign_in")
	}
	if sub.GetStepId() != si.step.StepID {
		return rejected(cmdID, "no_such_step")
	}
	values := map[string]string{}
	for k, v := range sub.GetValues() {
		values[k] = v
	}
	var next *bridgev2.LoginStep
	var err error
	switch si.step.Type {
	case bridgev2.LoginStepTypeUserInput:
		p, isInput := si.proc.(bridgev2.LoginProcessUserInput)
		if !isInput {
			return rejected(cmdID, "unsupported_step")
		}
		if params := si.step.UserInputParams; params != nil {
			for i := range params.Fields {
				f := &params.Fields[i]
				f.FillDefaultValidate()
				v, present := values[f.ID]
				if !present {
					if f.Optional {
						continue
					}
					return rejected(cmdID, "missing_field")
				}
				if f.Validate != nil {
					clean, verr := f.Validate(v)
					if verr != nil {
						return rejected(cmdID, "invalid_field")
					}
					values[f.ID] = clean
				}
			}
		}
		next, err = p.SubmitUserInput(si.ctx, values)
	case bridgev2.LoginStepTypeCookies:
		p, isCookies := si.proc.(bridgev2.LoginProcessCookies)
		if !isCookies {
			return rejected(cmdID, "unsupported_step")
		}
		next, err = p.SubmitCookies(si.ctx, values)
	case bridgev2.LoginStepTypeWebAuthn:
		p, isPasskey := si.proc.(bridgev2.LoginProcessWebAuthn)
		if !isPasskey {
			return rejected(cmdID, "unsupported_step")
		}
		next, err = p.SubmitWebAuthnResponse(si.ctx, json.RawMessage(values["response"]))
	default:
		return rejected(cmdID, "nothing_to_submit")
	}
	if err != nil {
		// Errors of a login process are fatal to it.
		b.failSignIn(si, si.step.StepID, errCode(err, "submit_failed"))
		return ok(cmdID)
	}
	b.advance(si, next)
	return ok(cmdID)
}

func (b *Backend) signInCancel(cmdID string) *contract.CommandReply {
	b.loginMu.Lock()
	defer b.loginMu.Unlock()
	if si := b.signIn; si != nil {
		stepID := ""
		if si.step != nil {
			stepID = si.step.StepID
		}
		b.failSignIn(si, stepID, "cancelled")
	}
	return ok(cmdID)
}

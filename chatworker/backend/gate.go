// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/mediaproxy"

	"lynx.invalid/ingest/chatworker/contract"
)

// ErrOtherAccount is returned by the gated LoadUserLogin for a new login of
// another account than the connection's. The framework's NewLogin returns it
// before it stores or caches the login, so nothing of that account is ever
// written; the sign-in ends as principal_mismatch.
var ErrOtherAccount = errors.New("the sign-in reached another account than the connection's")

// ErrGateStore is returned by the gated LoadUserLogin when it could not read
// the store to learn the connection's account. The login fails (store_error);
// it is not taken for another account's, which could be the connection's
// own, and nothing is ended at the network.
var ErrGateStore = errors.New("the login gate could not read the store")

// ErrNothingToEnd is what Hooks.EndRefused returns when the refused login
// had no session of its own at the network to end (none was opened, or it
// existed before the sign-in): nothing was ended, and nothing was tried.
var ErrNothingToEnd = errors.New("the refused login has no session to end")

// ErrOtherAccountKept is returned by the gated LoadUserLogin for a new login
// of another account than the connection's whose connector had stored it
// before the gate (Hooks.DropRefused) and could not delete all of it: the
// sign-in fails as other_account_kept, not principal_mismatch, the
// connection is ENDED with the same code (the host deletes the store) and
// no sign-in is begun on that store meanwhile.
var ErrOtherAccountKept = errors.New("the sign-in reached another account than the connection's, and what was stored of it could not be deleted")

// refusedEndLimit bounds Hooks.EndRefused (a connection to the network and
// one call).
const refusedEndLimit = 30 * time.Second

// refusedDropLimit bounds Hooks.DropRefused: a few deletes in the store,
// which wait while another writer holds the file (the connection's own
// client; one busy wait is 5 s). Long enough for such a lock to pass: a
// drop that gives up ends the connection as other_account_kept.
const refusedDropLimit = 60 * time.Second

// gatedNetwork is the network connector the framework is given: the real one
// with LoadUserLogin gated (see loadGate).
//
// The framework (bridgev2 v0.33.0) type-asserts these optional interfaces on
// its Network: StoppableNetwork, DirectMediableNetwork,
// IdentifierValidatingNetwork, PortalBridgeInfoFillingNetwork,
// MaxFileSizeingNetwork, SplitPortalMigratingNetwork and
// NetworkResettingNetwork (ConfigValidatingNetwork only in mxmain, which no
// worker uses). Embedding the interface would hide them all. For the first
// six "absent" has an exact equivalent (do nothing, report valid, report no
// direct media), so gatedNetwork implements them and delegates when the real
// connector has them. NetworkResettingNetwork has none (without it the
// framework recreates every client itself), so it is passed on only by
// gatedResettingNetwork, used when the real connector has it.
// TestGatedNetworkKeepsOptionalInterfaces pins this list.
type gatedNetwork struct {
	bridgev2.NetworkConnector
	b *Backend
	// fw is the framework instance the gate is for: a new login it lets
	// through is noted there (signingIn) until its sign-in completes.
	fw *instance
}

type gatedResettingNetwork struct {
	*gatedNetwork
	reset bridgev2.NetworkResettingNetwork
}

var (
	_ bridgev2.StoppableNetwork               = (*gatedNetwork)(nil)
	_ bridgev2.DirectMediableNetwork          = (*gatedNetwork)(nil)
	_ bridgev2.IdentifierValidatingNetwork    = (*gatedNetwork)(nil)
	_ bridgev2.PortalBridgeInfoFillingNetwork = (*gatedNetwork)(nil)
	_ bridgev2.MaxFileSizeingNetwork          = (*gatedNetwork)(nil)
	_ bridgev2.SplitPortalMigratingNetwork    = (*gatedNetwork)(nil)
	_ bridgev2.NetworkResettingNetwork        = gatedResettingNetwork{}
)

func (b *Backend) gate(inner bridgev2.NetworkConnector, fw *instance) bridgev2.NetworkConnector {
	g := &gatedNetwork{NetworkConnector: inner, b: b, fw: fw}
	if r, ok := inner.(bridgev2.NetworkResettingNetwork); ok {
		return gatedResettingNetwork{gatedNetwork: g, reset: r}
	}
	return g
}

// innerNetwork is the real connector behind the framework's Network.
func innerNetwork(n bridgev2.NetworkConnector) bridgev2.NetworkConnector {
	switch g := n.(type) {
	case *gatedNetwork:
		return g.NetworkConnector
	case gatedResettingNetwork:
		return g.NetworkConnector
	}
	return n
}

// LoadUserLogin refuses a new login of another account (loadGate) and
// otherwise is the real connector's. The account's own PersonInfo is not
// emitted here: the framework has not kept the login yet (NewLogin inserts
// it after this returns, bridgev2 userlogin.go:291; at a start it is cached
// after, userlogin.go:76), and a login that is not kept must not be
// announced. It is emitted once it is (start, completeSignIn), or before the
// login's first event (emitChat).
func (g *gatedNetwork) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	allowed, err := g.b.loadGate(ctx, login.ID)
	if err != nil {
		g.b.log.Error("login_gate_store_failed")
		releaseNewLogin(login)
		return fmt.Errorf("%w: %w", ErrGateStore, err)
	}
	if !allowed {
		if err := g.b.endRefused(g.NetworkConnector, login); err != nil {
			return err
		}
		return ErrOtherAccount
	}
	if err := g.NetworkConnector.LoadUserLogin(ctx, login); err != nil {
		return err
	}
	if g.fw != nil {
		g.fw.signingIn.Store(string(login.ID))
	}
	return nil
}

func (g *gatedNetwork) Stop() {
	if s, ok := g.NetworkConnector.(bridgev2.StoppableNetwork); ok {
		s.Stop()
	}
}

func (g *gatedNetwork) SetUseDirectMedia() {
	if d, ok := g.NetworkConnector.(bridgev2.DirectMediableNetwork); ok {
		d.SetUseDirectMedia()
	}
}

func (g *gatedNetwork) Download(ctx context.Context, mediaID networkid.MediaID, params map[string]string) (mediaproxy.GetMediaResponse, error) {
	if d, ok := g.NetworkConnector.(bridgev2.DirectMediableNetwork); ok {
		return d.Download(ctx, mediaID, params)
	}
	return nil, errNoDirect
}

func (g *gatedNetwork) ValidateUserID(id networkid.UserID) bool {
	if v, ok := g.NetworkConnector.(bridgev2.IdentifierValidatingNetwork); ok {
		return v.ValidateUserID(id)
	}
	return true
}

func (g *gatedNetwork) FillPortalBridgeInfo(portal *bridgev2.Portal, content *event.BridgeEventContent) {
	if f, ok := g.NetworkConnector.(bridgev2.PortalBridgeInfoFillingNetwork); ok {
		f.FillPortalBridgeInfo(portal, content)
	}
}

func (g *gatedNetwork) SetMaxFileSize(maxSize int64) {
	if m, ok := g.NetworkConnector.(bridgev2.MaxFileSizeingNetwork); ok {
		m.SetMaxFileSize(maxSize)
	}
}

func (g *gatedNetwork) PreSplitPortalMigrate(ctx context.Context) error {
	if m, ok := g.NetworkConnector.(bridgev2.SplitPortalMigratingNetwork); ok {
		return m.PreSplitPortalMigrate(ctx)
	}
	return nil
}

func (g gatedResettingNetwork) ResetHTTPTransport()      { g.reset.ResetHTTPTransport() }
func (g gatedResettingNetwork) ResetNetworkConnections() { g.reset.ResetNetworkConnections() }

// connectionAccount is the account this connection is for: the principal
// Init names, else the account of a stored login, else "" (not known yet).
// It reads the store, never the framework's cache: the gate runs while
// NewLogin holds the framework's cache lock.
func (b *Backend) connectionAccount(ctx context.Context) (string, error) {
	if p := b.init.GetPrincipal(); p != "" {
		return p, nil
	}
	ids, err := b.st.StoredLoginIDs(ctx)
	if err != nil || len(ids) == 0 {
		return "", err
	}
	return ids[0], nil
}

// loadGate tells whether a login may be loaded. A login already stored (a
// start, or a re-link of the same account) always may; a new one only when
// it is the connection's account or the account is not known yet. An error
// is a failed read of the store: then nothing is known about the login.
func (b *Backend) loadGate(ctx context.Context, id networkid.UserLoginID) (bool, error) {
	ids, err := b.st.StoredLoginIDs(ctx)
	if err != nil {
		return false, err
	}
	if slices.Contains(ids, string(id)) {
		return true, nil
	}
	account, err := b.connectionAccount(ctx)
	if err != nil {
		return false, err
	}
	return account == "" || account == string(id), nil
}

// releaseNewLogin stops the bridge state queue (a goroutine) that the
// framework's NewLogin made for a new login before it asked LoadUserLogin:
// when the gate refuses the login, NewLogin returns the error and drops the
// login without stopping it. Only a new login is touched: its Client is not
// set yet, while every login the framework has cached got its Client from
// LoadUserLogin before it was cached (a re-link of a stored login comes
// with that login's own queue, which stays), and a stored login being
// loaded at start has no queue yet (loadUserLogin makes it after).
func releaseNewLogin(login *bridgev2.UserLogin) {
	if login.Client != nil || login.BridgeState == nil {
		return
	}
	login.BridgeState.Destroy()
	login.BridgeState = nil
}

// endRefused cleans up after a refused login: NewLogin had already made a
// bridge state queue for it (releaseNewLogin). When the worker's connector allows it, the
// session the sign-in opened at the network is ended: by the worker's own
// Hooks.EndRefused, or (Hooks.EndRefusedSession) with a client that is
// never stored, cached or connected. It runs in the background, and a stop
// of the worker waits for it a bounded time (waitRefusedEnds).
//
// With Hooks.DropRefused, what the connector stored of the login is first
// deleted here, synchronously (dropRefused). The result is nil, or
// ErrOtherAccountKept when that delete failed.
func (b *Backend) endRefused(inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) error {
	releaseNewLogin(login)
	b.log.Warn("principal_mismatch")
	if b.hooks.DropRefused != nil {
		return b.dropRefused(inner, login)
	}
	if end := b.hooks.EndRefused; end != nil {
		b.goRefusedEnd(refusedEndLimit, endOutcome(func(ctx context.Context) error { return end(ctx, inner, login) }))
		return nil
	}
	if !b.hooks.EndRefusedSession {
		return nil
	}
	b.goRefusedEnd(10*time.Second, func(ctx context.Context) refusedOutcome {
		if err := inner.LoadUserLogin(ctx, login); err != nil || login.Client == nil {
			return refusedNotEnded
		}
		login.Client.LogoutRemote(ctx)
		login.Client = nil
		// LogoutRemote reports nothing: the result is not known.
		return refusedUnknown
	})
	return nil
}

// endOutcome is the run of an end of a refused session (EndRefused's, or
// the one DropRefused returned) for goRefusedEnd: its result as an outcome.
func endOutcome(end func(ctx context.Context) error) func(ctx context.Context) refusedOutcome {
	return func(ctx context.Context) refusedOutcome {
		switch err := end(ctx); {
		case errors.Is(err, ErrNothingToEnd):
			return refusedNothingToEnd
		case err != nil:
			return refusedNotEnded
		}
		return refusedEnded
	}
}

// dropRefused runs Hooks.DropRefused for a refused login, at once and
// whether or not the worker is stopping (what the connector stored of
// another account must not outlive the refusal), then starts the end at
// the network it returned like EndRefused's. When the delete failed, rows
// of that account may still be in the store: as when a wipe of another
// account failed (wipeOther), the worker begins no sign-in on that store
// (refused), the connection is ENDED other_account_kept so that the host
// deletes the store, and the sign-in fails with the same code
// (ErrOtherAccountKept, errCode).
func (b *Backend) dropRefused(inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) error {
	ctx, cancel := context.WithTimeout(context.Background(), refusedDropLimit)
	end, err := b.hooks.DropRefused(ctx, inner, login)
	cancel()
	if end != nil {
		b.goRefusedEnd(refusedEndLimit, endOutcome(end))
	} else {
		b.logRefusedOutcome(refusedNothingToEnd)
	}
	if err == nil {
		return nil
	}
	b.log.Error("refused_login_not_dropped")
	b.refused.Store(true)
	b.emitState(&contract.ConnectionState{
		State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: "other_account_kept",
	})
	return ErrOtherAccountKept
}

// refusedOutcome is how the end of a refused session went; each but
// refusedUnknown is logged by its fixed code (logRefusedOutcome).
type refusedOutcome int

const (
	refusedUnknown refusedOutcome = iota
	// refused_session_ended: the network confirmed the logout.
	refusedEnded
	// refused_session_nothing_to_end: there was no session to end.
	refusedNothingToEnd
	// refused_session_not_ended: the logout failed, timed out, or was
	// still running when the worker stopped; the session may still be
	// authorised at the network.
	refusedNotEnded
)

func (b *Backend) logRefusedOutcome(o refusedOutcome) {
	switch o {
	case refusedEnded:
		b.log.Info("refused_session_ended")
	case refusedNothingToEnd:
		b.log.Info("refused_session_nothing_to_end")
	case refusedNotEnded:
		b.log.Warn("refused_session_not_ended")
	}
}

// refusedEnd is one end of a refused session running in the background.
// Its outcome is logged once (report): by the run when it returns, or as
// refused_session_not_ended by waitRefusedEnds when the worker stops first.
type refusedEnd struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (b *Backend) report(e *refusedEnd, o refusedOutcome) {
	e.once.Do(func() { b.logRefusedOutcome(o) })
}

// goRefusedEnd runs end in the background within limit, tracked until it
// returns so that a stop of the worker can wait for it (waitRefusedEnds).
// Once the worker is stopping (waitRefusedEnds was called: a command still
// running, a sign-in step, can reach the gate after that) it is not
// started: it is logged as refused_session_not_ended at once.
func (b *Backend) goRefusedEnd(limit time.Duration, end func(ctx context.Context) refusedOutcome) {
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	e := &refusedEnd{cancel: cancel, done: make(chan struct{})}
	b.endsMu.Lock()
	if b.endsStopping {
		b.endsMu.Unlock()
		cancel()
		b.report(e, refusedNotEnded)
		return
	}
	if b.ends == nil {
		b.ends = map[*refusedEnd]struct{}{}
	}
	b.ends[e] = struct{}{}
	b.endsMu.Unlock()
	go func() {
		defer close(e.done)
		defer cancel()
		b.report(e, end(ctx))
		b.endsMu.Lock()
		delete(b.ends, e)
		b.endsMu.Unlock()
	}()
}

// waitRefusedEnds waits until every end of a refused session running now
// has returned, or until deadline. Each still running then is cancelled and
// logged as refused_session_not_ended (its own result, should it come
// later, is not logged): the worker is about to exit, and a logout cut off
// by the exit must not pass without a trace. The caller chooses deadline
// inside the time the host gives it to stop; a deadline already past
// (abandonRefusedEnds) waits for nothing. From the first call on, no new
// end is started (goRefusedEnd).
func (b *Backend) waitRefusedEnds(deadline time.Time) {
	b.endsMu.Lock()
	b.endsStopping = true
	running := make([]*refusedEnd, 0, len(b.ends))
	for e := range b.ends {
		running = append(running, e)
	}
	b.endsMu.Unlock()
	if len(running) == 0 {
		return
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	for _, e := range running {
		select {
		case <-e.done:
		case <-ctx.Done():
			b.report(e, refusedNotEnded)
			e.cancel()
		}
	}
}

// abandonRefusedEnds is waitRefusedEnds with no wait (SIGTERM: the kill
// follows): each end still running is logged as refused_session_not_ended
// and cancelled, and none is started after.
func (b *Backend) abandonRefusedEnds() { b.waitRefusedEnds(time.Time{}) }

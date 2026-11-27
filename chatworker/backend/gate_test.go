// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/wire"
)

// plainNet has none of the optional interfaces.
type plainNet struct{ bridgev2.NetworkConnector }

// fullNet has the ones whose calls the test observes.
type fullNet struct {
	bridgev2.NetworkConnector
	stopped, reset bool
}

func (f *fullNet) Stop()                                                            { f.stopped = true }
func (f *fullNet) ValidateUserID(networkid.UserID) bool                             { return false }
func (f *fullNet) ResetHTTPTransport()                                              {}
func (f *fullNet) ResetNetworkConnections()                                         { f.reset = true }
func (f *fullNet) SetMaxFileSize(int64)                                             {}
func (f *fullNet) FillPortalBridgeInfo(*bridgev2.Portal, *event.BridgeEventContent) {}

// TestGatedNetworkKeepsOptionalInterfaces: the gated connector offers the
// framework every optional interface it asserts on its Network (bridgev2
// v0.33.0) exactly as the real connector does: the six with an exact
// "absent" behaviour always, delegating when the real one has them, and
// NetworkResettingNetwork only when the real one has it.
func TestGatedNetworkKeepsOptionalInterfaces(t *testing.T) {
	b := &Backend{}
	for _, c := range []struct {
		name      string
		inner     bridgev2.NetworkConnector
		resetting bool
	}{{"plain", plainNet{}, false}, {"full", &fullNet{}, true}} {
		g := b.gate(c.inner, nil)
		if _, ok := g.(bridgev2.StoppableNetwork); !ok {
			t.Errorf("%s: not StoppableNetwork", c.name)
		}
		if _, ok := g.(bridgev2.DirectMediableNetwork); !ok {
			t.Errorf("%s: not DirectMediableNetwork", c.name)
		}
		if _, ok := g.(bridgev2.PortalBridgeInfoFillingNetwork); !ok {
			t.Errorf("%s: not PortalBridgeInfoFillingNetwork", c.name)
		}
		if _, ok := g.(bridgev2.MaxFileSizeingNetwork); !ok {
			t.Errorf("%s: not MaxFileSizeingNetwork", c.name)
		}
		if _, ok := g.(bridgev2.SplitPortalMigratingNetwork); !ok {
			t.Errorf("%s: not SplitPortalMigratingNetwork", c.name)
		}
		v, ok := g.(bridgev2.IdentifierValidatingNetwork)
		if !ok || v.ValidateUserID("x") == c.resetting {
			t.Errorf("%s: ValidateUserID not the real connector's", c.name)
		}
		r, ok := g.(bridgev2.NetworkResettingNetwork)
		if ok != c.resetting {
			t.Errorf("%s: NetworkResettingNetwork %v, want %v", c.name, ok, c.resetting)
		}
		g.(bridgev2.StoppableNetwork).Stop()
		if ok {
			r.ResetNetworkConnections()
		}
		if innerNetwork(g) != c.inner {
			t.Errorf("%s: innerNetwork is not the real connector", c.name)
		}
		if f, isFull := c.inner.(*fullNet); isFull && (!f.stopped || !f.reset) {
			t.Errorf("full: Stop or ResetNetworkConnections not delegated")
		}
		if _, err := g.(bridgev2.DirectMediableNetwork).Download(t.Context(), networkid.MediaID("m"), nil); err == nil {
			t.Errorf("%s: Download without direct media did not fail", c.name)
		}
	}
}

// recordingNet counts the real connector's LoadUserLogin calls.
type recordingNet struct {
	bridgev2.NetworkConnector
	loads atomic.Int32
}

func (r *recordingNet) LoadUserLogin(context.Context, *bridgev2.UserLogin) error {
	r.loads.Add(1)
	return nil
}

// someClient stands for the Client of a login the framework has loaded.
type someClient struct{ bridgev2.NetworkAPI }

// TestGateStoreErrorIsNotAnotherAccount: when the gate cannot read the store
// to learn the connection's account, the login fails as a store error. It is
// not taken for another account's (no ErrOtherAccount, no
// principal_mismatch), and no client is made to end its session at the
// network (it may be the connection's own account). The bridge state queue
// NewLogin made for a new login is stopped (NewLogin drops the login on the
// error); the queue of a login already loaded (a re-link) stays.
func TestGateStoreErrorIsNotAnotherAccount(t *testing.T) {
	b, st := testBackend(t)
	b.hooks.EndRefusedSession = true
	inner := &recordingNet{}
	g := b.gate(inner, nil)
	_ = st.DB().Close()
	br := &bridgev2.Bridge{}
	fresh := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-any"}}
	fresh.BridgeState = br.NewBridgeStateQueue(fresh)
	err := g.LoadUserLogin(t.Context(), fresh)
	if err == nil || errors.Is(err, ErrOtherAccount) || errCode(err, "fallback") != "store_error" {
		t.Fatalf("gate on a store error: %v (code %q)", err, errCode(err, "fallback"))
	}
	if fresh.BridgeState != nil {
		t.Fatal("the new login's bridge state queue was left running")
	}
	loaded := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-any"}, Client: someClient{}}
	loaded.BridgeState = br.NewBridgeStateQueue(loaded)
	defer loaded.BridgeState.Destroy()
	if err := g.LoadUserLogin(t.Context(), loaded); !errors.Is(err, ErrGateStore) {
		t.Fatalf("gate on a store error, loaded login: %v", err)
	}
	if loaded.BridgeState == nil {
		t.Fatal("a loaded login's bridge state queue was stopped")
	}
	time.Sleep(300 * time.Millisecond)
	if n := inner.loads.Load(); n != 0 {
		t.Fatalf("the real connector loaded the login %d times", n)
	}
}

// TestGateEndRefusedHook: a new login of another account than the
// connection's is refused (ErrOtherAccount) without the real connector
// loading it, and the worker's EndRefused hook is called once, in the
// background, with the real connector and that same unstored login. A login
// of the connection's account is loaded and the hook is not called.
func TestGateEndRefusedHook(t *testing.T) {
	b, st := testBackend(t)
	// The framework's login table, empty: no login is stored yet.
	if _, err := st.DB().Exec(t.Context(), `CREATE TABLE user_login (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	b.init.Principal = "u-mine"
	inner := &recordingNet{}
	type call struct {
		inner bridgev2.NetworkConnector
		login *bridgev2.UserLogin
		ok    bool
	}
	calls := make(chan call, 2)
	b.hooks.EndRefused = func(ctx context.Context, n bridgev2.NetworkConnector, l *bridgev2.UserLogin) error {
		_, hasDeadline := ctx.Deadline()
		calls <- call{n, l, hasDeadline}
		return nil
	}
	g := b.gate(inner, nil)
	other := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}}
	other.BridgeState = (&bridgev2.Bridge{}).NewBridgeStateQueue(other)
	if err := g.LoadUserLogin(t.Context(), other); !errors.Is(err, ErrOtherAccount) || errCode(err, "fallback") != "principal_mismatch" {
		t.Fatalf("other account: %v", err)
	}
	if other.BridgeState != nil {
		t.Fatal("the refused login's bridge state queue was left running")
	}
	select {
	case c := <-calls:
		if c.inner != inner || c.login != other || !c.ok {
			t.Fatalf("hook called with %v %v deadline %v", c.inner, c.login, c.ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("EndRefused was not called")
	}
	mine := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-mine"}}
	if err := g.LoadUserLogin(t.Context(), mine); err != nil {
		t.Fatalf("own account: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if n := inner.loads.Load(); n != 1 {
		t.Fatalf("the real connector loaded %d logins, want 1 (the connection's)", n)
	}
	if len(calls) != 0 {
		t.Fatal("EndRefused called for the connection's own account")
	}
}

// lockedBuffer is a log sink written from another goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// TestEndRefusedLogsResult: the EndRefused hook's result is logged by its
// own code: ended (nil), nothing to end (ErrNothingToEnd: not logged as
// ended), not ended (any other error).
func TestEndRefusedLogsResult(t *testing.T) {
	for _, c := range []struct {
		err        error
		want, deny string
	}{
		{nil, `"refused_session_ended"`, "nothing_to_end"},
		{ErrNothingToEnd, `"refused_session_nothing_to_end"`, `"refused_session_ended"`},
		{errors.New("no network"), `"refused_session_not_ended"`, `"refused_session_ended"`},
	} {
		b, _ := testBackend(t)
		out := &lockedBuffer{}
		b.log = logx.New(out, "c")
		done := make(chan struct{})
		b.hooks.EndRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error {
			defer close(done)
			return c.err
		}
		b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}})
		<-done
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(out.String(), c.want) {
			if time.Now().After(deadline) {
				t.Fatalf("hook result %v: log %q lacks %s", c.err, out.String(), c.want)
			}
			time.Sleep(5 * time.Millisecond)
		}
		if strings.Contains(out.String(), c.deny) {
			t.Fatalf("hook result %v: log %q has %s", c.err, out.String(), c.deny)
		}
	}
}

// shutdownWorker serves b on a pipe, sends Shutdown with grace and reads up
// to the ShutdownAck (acked is closed then). served gets the time from the
// Shutdown to serve's return.
func shutdownWorker(t *testing.T, b *Backend, grace time.Duration) (acked <-chan struct{}, served <-chan time.Duration) {
	t.Helper()
	ep, host := pipeEndpoint(t)
	ack := make(chan struct{})
	done := make(chan time.Duration, 1)
	f, err := wire.EncodeFrame(wire.HostOpcode, &contract.HostFrame{Body: &contract.HostFrame_Shutdown{
		Shutdown: &contract.Shutdown{GraceMs: uint32(grace.Milliseconds())}}})
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	go func() {
		if code := b.serve(t.Context(), ep, func() {}); code != 0 {
			t.Errorf("serve returned %d after Shutdown, want 0", code)
		}
		done <- time.Since(began)
	}()
	if _, err := host.Write(f); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = host.SetReadDeadline(time.Now().Add(60 * time.Second))
		for {
			var wf contract.WorkerFrame
			if err := wire.ReadFrame(host, wire.WorkerOpcode, &wf); err != nil {
				return
			}
			if wf.GetShutdownAck() != nil {
				close(ack)
				_, _ = io.Copy(io.Discard, host)
				return
			}
		}
	}()
	return ack, done
}

// TestShutdownWaitsForRefusedLogout: Shutdown arrives while the logout of a
// refused session runs. The worker sends its ShutdownAck and then waits for
// the logout (here well inside the bound: three quarters of a 20 s grace)
// before it exits, and the log says it was logged out, once, before
// "shutdown".
func TestShutdownWaitsForRefusedLogout(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	started, release := make(chan struct{}), make(chan struct{})
	b.hooks.EndRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error {
		close(started)
		<-release
		return nil
	}
	b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}})
	<-started
	acked, served := shutdownWorker(t, b, 20*time.Second)
	select {
	case <-acked:
	case <-time.After(30 * time.Second):
		t.Fatal("no ShutdownAck")
	}
	// The logout has not returned: the worker must still be there. Without
	// the wait it exits at once after the ack; a second is ample for that
	// (on a slower host this check only gets weaker, it never fails wrongly).
	select {
	case <-served:
		t.Fatal("the worker exited while the refused session's logout was running")
	case <-time.After(time.Second):
	}
	if strings.Contains(out.String(), `"shutdown"`) {
		t.Fatal("the worker logged its shutdown while the refused session's logout was running")
	}
	close(release)
	select {
	case <-served:
	case <-time.After(30 * time.Second):
		t.Fatal("the worker did not exit after the logout returned")
	}
	logs := out.String()
	if n := strings.Count(logs, `"refused_session_ended"`); n != 1 || strings.Contains(logs, `"refused_session_not_ended"`) {
		t.Fatalf("log %q: want refused_session_ended once and no refused_session_not_ended", logs)
	}
	if strings.Index(logs, `"refused_session_ended"`) > strings.Index(logs, `"shutdown"`) {
		t.Fatalf("log %q: the logout's result after the shutdown line", logs)
	}
}

// TestShutdownGraceOfARefusedReLink: the engine stops a worker whose
// re-link to another account it refused (principal_mismatch) with a grace
// of 30 s (REFUSED_STOP_GRACE, src/chat_connections/types.rs). The worker
// takes that grace as given (it is not taken for too large, which would make
// it the default 5 s), and a logout of the refused session still running
// gets three quarters of it from Shutdown: 22.5 s, where the default grace
// leaves 3.75 s.
func TestShutdownGraceOfARefusedReLink(t *testing.T) {
	const engineGraceMs = 30_000
	if g := shutdownGrace(engineGraceMs); g != 30*time.Second {
		t.Fatalf("Shutdown with grace_ms %d is taken as %v, want 30s", engineGraceMs, g)
	}
	if w := refusedEndsWait(shutdownGrace(engineGraceMs)); w != 22500*time.Millisecond {
		t.Fatalf("the refused logout gets %v of the engine's 30 s grace, want 22.5s", w)
	}
	for _, ms := range []uint32{0, engineGraceMs + 1, 60_000} {
		if g := shutdownGrace(ms); g != 5*time.Second {
			t.Fatalf("Shutdown with grace_ms %d is taken as %v, want the default 5s", ms, g)
		}
	}
	if w := refusedEndsWait(shutdownGrace(5_000)); w != 3750*time.Millisecond {
		t.Fatalf("the refused logout gets %v of a 5 s grace, want 3.75s", w)
	}
}

// TestShutdownAtTheEnginesRefusedGrace: with the engine's 30 s grace, a
// logout of the refused session that takes longer than the default grace's
// share (3.75 s) is still waited for: it ends, the log says
// refused_session_ended, and the worker exits right after it, inside the
// grace.
func TestShutdownAtTheEnginesRefusedGrace(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	const logout = 4500 * time.Millisecond
	started := make(chan struct{})
	b.hooks.EndRefused = func(ctx context.Context, _ bridgev2.NetworkConnector, _ *bridgev2.UserLogin) error {
		close(started)
		// The network logout: it takes its time unless the stop cuts it.
		select {
		case <-time.After(logout):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}})
	<-started
	_, served := shutdownWorker(t, b, 30*time.Second)
	select {
	case took := <-served:
		t.Logf("exited %v after Shutdown", took)
		if took >= 30*time.Second {
			t.Fatalf("exited %v after Shutdown, past the grace", took)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("the worker did not exit after the logout returned")
	}
	logs := out.String()
	if strings.Count(logs, `"refused_session_ended"`) != 1 || strings.Contains(logs, `"refused_session_not_ended"`) {
		t.Fatalf("log %q: want refused_session_ended once and no refused_session_not_ended", logs)
	}
}

// TestShutdownBoundsRefusedLogout: a logout that hangs (it ignores its
// context) does not hold the worker past the bound: with a 6 s grace it
// exits after three quarters of it (4.5 s, never before: the wait is on a
// deadline) and inside the grace, the log says refused_session_not_ended
// once, and the logout's context is cancelled. Its own result, when it comes
// later, is not logged.
func TestShutdownBoundsRefusedLogout(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	started, release := make(chan struct{}), make(chan struct{})
	ctxErr := make(chan error, 1)
	b.hooks.EndRefused = func(ctx context.Context, _ bridgev2.NetworkConnector, _ *bridgev2.UserLogin) error {
		close(started)
		<-release
		ctxErr <- ctx.Err()
		return nil
	}
	b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}})
	<-started
	const grace = 6 * time.Second
	_, served := shutdownWorker(t, b, grace)
	select {
	case took := <-served:
		t.Logf("exited %v after Shutdown (grace %v)", took, grace)
		if took < grace*refusedEndsShare/4 || took >= grace {
			t.Fatalf("exited %v after Shutdown: want from %v (the bound) to %v (the grace)", took, grace*refusedEndsShare/4, grace)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a hanging logout held the worker past its grace")
	}
	if n := strings.Count(out.String(), `"refused_session_not_ended"`); n != 1 {
		t.Fatalf("log %q: want refused_session_not_ended once", out.String())
	}
	close(release)
	select {
	case err := <-ctxErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the logout's context: %v, want cancelled at the bound", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the logout did not return")
	}
	b.waitRefusedEnds(time.Now().Add(10 * time.Second))
	logs := out.String()
	if strings.Contains(logs, `"refused_session_ended"`) || strings.Count(logs, `"refused_session_not_ended"`) != 1 {
		t.Fatalf("log %q: the late result was logged as well", logs)
	}
}

// TestRefusedEndAfterStopBegan: a logout of a refused session that a command
// still running reaches after the stop began (waitRefusedEnds took its
// look) is not started, and is logged as refused_session_not_ended at once.
func TestRefusedEndAfterStopBegan(t *testing.T) {
	b, _ := testBackend(t)
	out := &lockedBuffer{}
	b.log = logx.New(out, "c")
	var called atomic.Bool
	b.hooks.EndRefused = func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error {
		called.Store(true)
		return nil
	}
	b.waitRefusedEnds(time.Now().Add(time.Second))
	b.endRefused(&recordingNet{}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}})
	if n := strings.Count(out.String(), `"refused_session_not_ended"`); n != 1 {
		t.Fatalf("log %q: want refused_session_not_ended once, at once", out.String())
	}
	b.waitRefusedEnds(time.Now().Add(5 * time.Second))
	if called.Load() || strings.Contains(out.String(), `"refused_session_ended"`) {
		t.Fatal("a logout was started after the stop began")
	}
}

// TestBeforeLogout: Hooks.BeforeLogout runs for the connection's own login
// only (its id is the known account), once, with the real connector behind
// the gate and that login; its result is logged by its code
// (logout_prepared, nothing for ErrNothingToEnd, logout_prepare_failed for
// an error, a panic or a hook that does not return within the limit), and
// a hook that ignores its context holds the logout up no longer than the
// limit. WHY: catches the hook run for another account's login, a logout
// held up or broken by its hook, and a failure logged as done.
func TestBeforeLogout(t *testing.T) {
	inner := &recordingNet{}
	own := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-own"}}
	other := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "u-other"}}
	newB := func(hook func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error) (*Backend, *bridgev2.Bridge, *lockedBuffer) {
		b, _ := testBackend(t)
		out := &lockedBuffer{}
		b.log = logx.New(out, "c")
		b.account.Store("u-own")
		b.hooks.BeforeLogout = hook
		return b, &bridgev2.Bridge{Network: b.gate(inner, nil)}, out
	}

	var calls []*bridgev2.UserLogin
	var nets []bridgev2.NetworkConnector
	b, br, out := newB(func(ctx context.Context, n bridgev2.NetworkConnector, l *bridgev2.UserLogin) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("the hook's context has no deadline")
		}
		calls, nets = append(calls, l), append(nets, n)
		return nil
	})
	b.beforeLogout(context.Background(), br, other)
	b.beforeLogout(context.Background(), br, nil)
	if len(calls) != 0 {
		t.Fatalf("ran for another account's login: %d calls", len(calls))
	}
	b.beforeLogout(context.Background(), br, own)
	if len(calls) != 1 || calls[0] != own || nets[0] != bridgev2.NetworkConnector(inner) {
		t.Fatalf("calls %v with %v, want the own login once with the real connector", calls, nets)
	}
	if logs := out.String(); !strings.Contains(logs, `"logout_prepared"`) || strings.Contains(logs, "logout_prepare_failed") {
		t.Fatalf("log %q", logs)
	}
	// None set: nothing runs.
	b.hooks.BeforeLogout = nil
	b.beforeLogout(context.Background(), br, own)

	orig := beforeLogoutLimit
	beforeLogoutLimit = 50 * time.Millisecond
	t.Cleanup(func() { beforeLogoutLimit = orig })
	release := make(chan struct{})
	defer close(release)
	for _, c := range []struct {
		name       string
		hook       func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error
		want, deny string
	}{
		{"nothing to end", func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error { return ErrNothingToEnd }, "", "logout_prepare"},
		{"failed", func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error { return errors.New("x") }, `"logout_prepare_failed"`, `"logout_prepared"`},
		{"panicked", func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error { panic("x") }, `"logout_prepare_failed"`, `"logout_prepared"`},
		{"hangs", func(context.Context, bridgev2.NetworkConnector, *bridgev2.UserLogin) error { <-release; return nil }, `"logout_prepare_failed"`, `"logout_prepared"`},
	} {
		b, br, out := newB(c.hook)
		start := time.Now()
		b.beforeLogout(context.Background(), br, own)
		if time.Since(start) > 5*time.Second {
			t.Fatalf("%s: held the logout past its limit", c.name)
		}
		logs := out.String()
		if (c.want != "" && !strings.Contains(logs, c.want)) || strings.Contains(logs, c.deny) {
			t.Errorf("%s: log %q, want %s and no %s", c.name, logs, c.want, c.deny)
		}
	}
}

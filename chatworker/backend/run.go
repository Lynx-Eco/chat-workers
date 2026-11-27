// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
	"lynx.invalid/ingest/chatworker/wire"
)

// Licence is the SPDX expression of every worker binary built from go/.
const Licence = "AGPL-3.0-or-later"

// Exit codes of a worker beside 0 (Shutdown, or the host closed), 1 (an
// error before Hello, or Hello failed) and 2 (bad arguments, main). A worker
// binary may add its own above these (chat_fakenet: 3, a scripted crash).
const (
	// ExitRestartFailed is the worker's exit code after a logout (or the
	// wipe of a refused account) whose new framework instance could not be
	// started: the store is wiped and the worker cannot sign in again until
	// it is started anew. The log line before it has the code
	// "exit_restart_failed".
	ExitRestartFailed = 4
	// ExitStoreRefused is the exit code after the store was refused as it
	// is (store.OpenError with a verdict: store_key_invalid,
	// store_not_encrypted, store_key_mismatch, store_corrupt): Hello and one
	// ConnectionState ERROR with the store's code were sent first. Nothing
	// on disk was changed. A store that could not be reached now
	// (store_open_failed) is not refused: no Hello, exit 1.
	ExitStoreRefused = 5
)

// SettingsError refuses a start because of the host's settings; Code is
// reported as ConnectionState ERROR.
type SettingsError struct{ Code string }

func (e *SettingsError) Error() string { return "settings: " + e.Code }

// Worker describes one worker binary.
type Worker struct {
	Name         string
	Version      string
	SourceCommit string
	Upstream     map[string]string
	BridgeID     networkid.BridgeID
	// NewConnector builds the network connector from Init. It must not
	// touch the network. A *SettingsError is reported as is.
	NewConnector func(init *contract.Init) (bridgev2.NetworkConnector, error)
	Hooks        Hooks
	// Prepare, when set, runs after NewBridge and before the framework
	// starts (test hooks of the fake network).
	Prepare func(b *Backend)
}

// Run is a worker's whole life: connect, Init, Hello, start the framework,
// serve commands until Shutdown (exit 0) or the host closes (exit 0). On
// SIGTERM, at any point, it logs and dies of SIGTERM at once (onSigterm).
// Errors before Hello exit 1, a store that could not be reached now
// (store_open_failed) among them; a store refused as it is (a verdict, see
// refuseStore) exits ExitStoreRefused after Hello; a framework that cannot
// be started again after a wipe exits ExitRestartFailed.
func Run(ctx context.Context, socketPath string, w Worker, stderr io.Writer) int {
	pre := logx.New(stderr, "")
	var sigLog atomic.Pointer[logx.Logger]
	var sigBackend atomic.Pointer[Backend]
	sigLog.Store(&pre)
	defer onSigterm(&sigLog, &sigBackend)()
	init, hs, err := wire.Dial(ctx, socketPath, wire.DefaultConfig())
	if err != nil {
		pre.Error("connect_failed", logx.N("kind", int64(wire.KindOf(err))))
		return 1
	}
	log := logx.New(stderr, init.GetConnectionId())
	sigLog.Store(&log)
	stateDir := init.GetStateDir()
	if stateDir == "" || !filepath.IsAbs(stateDir) {
		log.Error("bad_state_dir")
		_ = hs.Close()
		return 1
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		log.Error("state_dir_failed")
		_ = hs.Close()
		return 1
	}
	level := zerolog.WarnLevel
	if lv, err := zerolog.ParseLevel(init.GetSettings()["log_level"]); err == nil && lv != zerolog.NoLevel {
		level = lv
	}
	upLog := logx.NewFilter(stderr, init.GetConnectionId()).Upstream(level)
	// The key's only copies from here on are the store's (to open pooled
	// connections) and the derived spool keys; Init's is zeroed.
	key := bytes.Clone(init.GetStoreKey())
	clear(init.GetStoreKey())
	defer clear(key)
	db, encrypted, err := store.OpenDB(filepath.Join(stateDir, "worker.db"), store.OpenOptions{
		Key: key, DevPlain: init.GetSettings()["dev_plain_store"] == "1",
	}, upLog)
	if err != nil {
		code := store.CodeOpenFailed
		var oe *store.OpenError
		if errors.As(err, &oe) {
			code = oe.Code
		}
		if code == store.CodeOpenFailed {
			// Not a verdict on the store: the file could not be reached
			// now (permissions, a full disk, a busy file), or a query of
			// the cipher checks failed. No Hello is sent, so the engine
			// counts a failed start: it starts the worker again with its
			// backoff, and after 5 failed starts within 600 s parks the
			// connection as crash_loop until someone restarts it. This
			// log line goes to stderr, which the engine does not keep.
			// Answering Hello with store_encrypted false here would have
			// the host park the connection for good as an unencrypted
			// store.
			log.Error("store_open_failed")
			_ = hs.Close()
			return 1
		}
		return refuseStore(hs, w, log, code)
	}
	defer db.Close()
	st, err := store.Open(ctx, db)
	if err != nil {
		// The file opened and passed the cipher checks; the worker's own
		// tables could not be made or brought up to date on it. Handled as
		// store_open_failed is (no Hello, exit 1), with its own code.
		log.Error("store_setup_failed")
		_ = hs.Close()
		return 1
	}
	// The host takes every seq at or below its stored mark for a repeat: a
	// worker whose queue was lost or reset numbers on above it.
	if err := st.RaiseSeqFloor(ctx, init.GetHostStoredSeq()); err != nil {
		log.Error("seq_floor_failed")
		_ = hs.Close()
		return 1
	}
	pending, err := st.Pending(ctx)
	if err != nil {
		log.Error("outbox_read_failed")
		_ = hs.Close()
		return 1
	}
	boot, err := st.Next(ctx, "boot")
	if err != nil {
		log.Error("counter_failed")
		_ = hs.Close()
		return 1
	}
	var last uint64
	if len(pending) > 0 {
		last = pending[len(pending)-1].Seq
	}
	out := NewEmitter(st, log, pending)
	b := New(init, st, out, log, w.Hooks, boot)
	sigBackend.Store(b)
	// Before Hello, like every other step a worker cannot serve without.
	if err := b.SetSpoolKey(key); err != nil {
		log.Error("spool_key_failed")
		_ = hs.Close()
		return 1
	}
	clear(key)
	ep, err := hs.Hello(&contract.Hello{
		ContractVersion:  contract.Version,
		WorkerName:       w.Name,
		WorkerVersion:    w.Version,
		SourceCommit:     w.SourceCommit,
		Licence:          Licence,
		UpstreamVersions: w.Upstream,
		StoreEncrypted:   encrypted,
		LastUnackedSeq:   last,
	})
	if err != nil {
		log.Error("hello_failed")
		return 1
	}
	log.Info("started", logx.N("replay", int64(len(pending))), logx.N("boot", int64(boot)))
	pumpCtx, stopPump := context.WithCancel(ctx)
	defer stopPump()
	go out.Pump(pumpCtx, ep)
	// What a crash left: spool files being written, scratch files, partial
	// transfers. Once per process: a framework restart after a logout
	// follows a wipe of all three.
	sweep(b.mediaDir, filepath.Join(b.stateDir, "tmp"), b.transferDir)
	b.start(ctx, w, st, upLog)
	return b.serve(ctx, ep, stopPump)
}

// refuseStore reports a store that must not be used as it is (a bad key, a
// plain file, a wrong key, a corrupt file; never a failure to reach it,
// store_open_failed, which exits 1 with no Hello): Hello (not
// encrypted, nothing unacknowledged), then one ConnectionState ERROR with
// the fixed code. The event has seq 0: it is not in any outbox (there is no
// store to keep it in), so it is not acknowledged and never replayed; its
// event id is unique per start. Then the worker closes and exits
// ExitStoreRefused.
func refuseStore(hs *wire.Handshake, w Worker, log logx.Logger, code string) int {
	logStoreRefused(log, code)
	ep, err := hs.Hello(&contract.Hello{
		ContractVersion:  contract.Version,
		WorkerName:       w.Name,
		WorkerVersion:    w.Version,
		SourceCommit:     w.SourceCommit,
		Licence:          Licence,
		UpstreamVersions: w.Upstream,
	})
	if err != nil {
		log.Error("hello_failed")
		return 1
	}
	now := time.Now()
	ev := &contract.ChatEvent{
		EventId:     fmt.Sprintf("state:store:%s:%d", code, now.UnixNano()),
		EmittedAtMs: now.UnixMilli(),
	}
	setKind(ev, &contract.ConnectionState{State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, Code: code})
	if b, err := contract.Marshal(ev); err == nil {
		if framed, err := wire.EncodeQueuedEvent(0, b); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = ep.SendEvent(ctx, framed)
			cancel()
		}
	}
	_ = ep.Close(5 * time.Second)
	return ExitStoreRefused
}

// logStoreRefused logs the store's fixed code; logx takes only constants.
func logStoreRefused(log logx.Logger, code string) {
	switch code {
	case store.CodeKeyInvalid:
		log.Error(store.CodeKeyInvalid)
	case store.CodeNotEncrypted:
		log.Error(store.CodeNotEncrypted)
	case store.CodeKeyMismatch:
		log.Error(store.CodeKeyMismatch)
	case store.CodeCorrupt:
		log.Error(store.CodeCorrupt)
	default:
		log.Error(store.CodeOpenFailed)
	}
}

func (b *Backend) start(ctx context.Context, w Worker, st *store.Store, upLog zerolog.Logger) {
	b.worker, b.upLog = w, upLog
	br := b.newFramework(ctx)
	if br == nil {
		return
	}
	user, err := br.GetUserByMXID(ctx, OwnerMXID)
	if err != nil || user == nil {
		b.failStart("store_error")
		return
	}
	logins := user.GetUserLogins()
	if p := b.init.GetPrincipal(); p != "" {
		other := false
		for _, l := range logins {
			if string(l.ID) != p {
				// A login of another account than the connection's (a
				// re-link past the gate cut off by a stop or a crash
				// before its wipe was done): it is not started, nothing
				// of it is reported, and the store is wiped.
				b.loggedOut.Store(l.ID, true)
				other = true
			}
		}
		if other {
			b.refuseStoredAtStart()
			return
		}
	}
	if b.init.GetPrincipal() == "" {
		account, err := b.connectionAccount(ctx)
		if err != nil {
			b.failStart("store_error")
			return
		}
		b.account.Store(account)
	}
	// The stored logins are kept (cached) now and not yet connected: the
	// account's own PersonInfo goes first (ensureSelf).
	fw := b.instanceOf(br)
	for _, l := range logins {
		b.rememberSelf(l)
		fw.ensureSelf(l.ID, l)
	}
	// Before any login runs, rooms whose portal is certainly gone (a merge
	// cut off between the framework's two steps) are reported removed.
	fw.sweepOrphanRooms(ctx)
	if prepare := b.hooks.PrepareLogins; prepare != nil {
		if err := prepare(ctx, innerNetwork(br.Network), br, logins); err != nil {
			b.log.Warn("prepare_logins_failed")
		}
	}
	if err := br.StartLogins(ctx); err != nil {
		b.failStart("start_failed")
		return
	}
	go br.PostStart(ctx)
}

// refuseStoredAtStart ends a start whose store holds a login of another
// account than Init's principal: a store such a login is in never survives a
// start. Everything is wiped as when a sign-in stored such a login
// (wipeOther: every login deleted, only the other account's logged out at
// the network, never the connection's own), and the connection is reported
// ENDED: other_account_wiped when the wipe succeeded (nothing of either
// account is kept; the connection signs in again from an empty store), else
// other_account_kept (the host deletes the store itself; meanwhile no
// sign-in is begun on it). There is no sign-in to fail. The engine reads
// ENDED as at any time (the connection needs the user, no restart) and
// takes other_account_kept as during a sign-in. A framework that could not
// be started again after the wipe has the worker exit (ExitRestartFailed),
// as after a logout.
func (b *Backend) refuseStoredAtStart() {
	b.loginMu.Lock()
	defer b.loginMu.Unlock()
	b.wipeMu.Lock()
	defer b.wipeMu.Unlock()
	b.log.Warn("principal_mismatch_at_start")
	_, ended, restarted := b.wipeOther(b.init.GetPrincipal())
	b.emitState(&contract.ConnectionState{
		State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ENDED, Code: ended,
	})
	if !restarted {
		b.requestExit()
	}
}

// failStart reports that the framework could not be started; commands are
// then answered with code.
func (b *Backend) failStart(code string) {
	b.notReady.Store(code)
	// The code is in the ERROR state; the log has a constant (logx).
	b.log.Error("not_started")
	b.emitState(&contract.ConnectionState{State: contract.ConnectionStateKind_CONNECTION_STATE_KIND_ERROR, Code: code})
}

// newFramework builds a framework instance on the store and starts its
// connectors, not its logins. NewBridge makes it the backend's instance
// (Init). On failure the code is reported and nil returned.
func (b *Backend) newFramework(ctx context.Context) *bridgev2.Bridge {
	w := b.worker
	b.brp.Store(nil)
	conn, err := w.NewConnector(b.init)
	var history History
	if err == nil {
		history, err = ReadHistory(b.init)
	}
	if err != nil {
		var se *SettingsError
		if errors.As(err, &se) {
			b.failStart(se.Code)
		} else {
			b.failStart("start_failed")
		}
		return nil
	}
	// The framework gets the connector with its LoadUserLogin gated: a new
	// login of another account is refused before it is stored (gate.go). It
	// gets the backend as a view of its own (instance), which carries the
	// store's epoch of now: after a wipe (a logout) nothing this instance
	// writes or reports through it is stored. newFramework runs at start and
	// after a wipe (resetAll), never while one runs: the epoch read here is
	// the one the instance's store begins with.
	fw := b.newInstance()
	br := bridgev2.NewBridge(w.BridgeID, b.st.DB(), b.upLog, BridgeConfig(history), fw, b.gate(conn, fw), commands.NewProcessor)
	br.ExternallyManagedDB = true
	if dmn, isDirect := conn.(bridgev2.DirectMediableNetwork); isDirect {
		dmn.SetUseDirectMedia()
	}
	if w.Prepare != nil {
		w.Prepare(b)
	}
	if err := br.DB.Upgrade(ctx); err != nil {
		b.brp.Store(nil)
		b.failStart("store_upgrade_failed")
		return nil
	}
	if err := br.StartConnectors(ctx); err != nil {
		b.brp.Store(nil)
		b.failStart("start_failed")
		return nil
	}
	return br
}

func (b *Backend) serve(ctx context.Context, ep *wire.Endpoint, stopPump context.CancelFunc) int {
	cmdCtx, cancelCmds := context.WithCancel(context.Background())
	defer cancelCmds()
	var running sync.WaitGroup
	for {
		select {
		case <-b.exitReq:
			// A wipe left no framework instance (resetAll): this process
			// cannot sign in again. The running commands' replies (the
			// logout's) and the queued events (its ENDED) go out first.
			b.log.Error("exit_restart_failed", logx.N("exit", ExitRestartFailed))
			deadline := time.Now().Add(5 * time.Second)
			waitUntil(&running, deadline)
			if !b.out.Flush(deadline) {
				b.log.Warn("exit_flush_incomplete", logx.N("left", int64(b.out.Backlog())))
			}
			b.waitRefusedEnds(deadline)
			stopPump()
			_ = ep.Close(time.Until(deadline) + 100*time.Millisecond)
			return ExitRestartFailed
		case <-ctx.Done():
			b.stopOnClose()
			_ = ep.Close(time.Second)
			return 0
		case f, open := <-ep.Inbound():
			if !open {
				if err := ep.ReadErr(); err != nil {
					b.log.Warn("host_read_failed", logx.N("kind", int64(wire.KindOf(err))))
				} else {
					b.log.Info("host_closed")
				}
				b.stopOnClose()
				_ = ep.Close(time.Second)
				return 0
			}
			switch body := f.Body.(type) {
			case *contract.HostFrame_Ack:
				b.out.Ack(ctx, body.Ack.GetUpToSeq())
			case *contract.HostFrame_Command:
				cmd := body.Command
				running.Add(1)
				go func() {
					defer running.Done()
					// What must follow the reply runs after it is sent
					// (whenReplied), whether or not it could be.
					ar := &afterReply{}
					defer ar.run()
					r := b.Command(withAfterReply(cmdCtx, ar), cmd)
					if r.CommandId == "" {
						r.CommandId = cmd.GetCommandId()
					}
					_ = ep.SendReply(cmdCtx, r)
				}()
			case *contract.HostFrame_Shutdown:
				grace := shutdownGrace(body.Shutdown.GetGraceMs())
				started := time.Now()
				deadline := started.Add(grace)
				// A wipe in progress (a cancelled re-link's, a logout's) is
				// waited for up to half the grace, and none begins after:
				// one cut in half leaves the store as neither before nor
				// after.
				b.waitWipe(started.Add(grace / 2))
				b.stopBridge(max(grace/2-time.Since(started), grace/4))
				if !b.out.Flush(deadline.Add(-200 * time.Millisecond)) {
					b.log.Warn("shutdown_flush_incomplete", logx.N("left", int64(b.out.Backlog())))
				}
				stopPump()
				sctx, cancel := context.WithDeadline(context.Background(), deadline)
				_ = ep.SendOrdered(sctx, &contract.WorkerFrame{Body: &contract.WorkerFrame_ShutdownAck{ShutdownAck: &contract.ShutdownAck{}}})
				cancel()
				// A logout of a refused session still running gets what is
				// left of three quarters of the grace (refusedEndsShare),
				// after the events and the ack: the host ends the process
				// once the grace is over.
				b.waitRefusedEnds(started.Add(refusedEndsWait(grace)))
				_ = ep.Close(time.Until(deadline) + 100*time.Millisecond)
				b.log.Info("shutdown")
				return 0
			}
		}
	}
}

// How long a stop waits for the logout of a refused session still running
// (waitRefusedEnds). Shutdown carries the host's grace (grace_ms; the engine
// sends 5 s by default, DEFAULT_STOP_GRACE, and ends the process group once
// it is over): the wait ends at refusedEndsShare quarters of it, so that the
// worker closes and exits inside it. When the host closed the connection or
// Run's context ended, nothing says how long is left: the wait is
// refusedEndsAtClose, alongside the framework's stop (stopOnClose). The
// engine that lost the channel sends SIGTERM to the worker's process group
// soon after it closed its end (a few hundred ms), often before that wait is
// over: the SIGTERM handler (Run) then logs what is still running as not
// ended and exits at once.
//
// The engine stops a worker that refused a re-link to another account
// (its sign-in failed principal_mismatch) with a grace of maxShutdownGrace,
// so that the logout of that account's session, begun at the refusal,
// gets up to refusedEndsWait(maxShutdownGrace) = 22.5 s from Shutdown.
const (
	refusedEndsShare   = 3
	refusedEndsAtClose = 1 * time.Second
	// maxShutdownGrace is the largest grace Shutdown is taken at; a larger
	// one, or none, is defaultShutdownGrace.
	maxShutdownGrace     = 30 * time.Second
	defaultShutdownGrace = 5 * time.Second
)

// shutdownGrace is the grace Shutdown.grace_ms gives: as given from 1 ms up
// to maxShutdownGrace, else defaultShutdownGrace.
func shutdownGrace(graceMs uint32) time.Duration {
	grace := time.Duration(graceMs) * time.Millisecond
	if grace <= 0 || grace > maxShutdownGrace {
		return defaultShutdownGrace
	}
	return grace
}

// refusedEndsWait is how long after Shutdown a logout of a refused session
// still running is waited for: refusedEndsShare quarters of the grace.
func refusedEndsWait(grace time.Duration) time.Duration {
	return grace * refusedEndsShare / 4
}

// waitWipe waits until no wipe of everything runs (wipeMu), at most until
// deadline, and keeps any from beginning after: the worker is stopping, and
// a wipe not begun leaves the store as it was, which the next start judges
// (refuseStoredAtStart). One still running at the deadline is logged
// (shutdown_wipe_incomplete); the stop goes on.
func (b *Backend) waitWipe(deadline time.Time) {
	held := make(chan struct{})
	go func() {
		// Never unlocked: the process is about to exit.
		b.wipeMu.Lock()
		close(held)
	}()
	select {
	case <-held:
	case <-time.After(time.Until(deadline)):
		b.log.Warn("shutdown_wipe_incomplete")
	}
}

// stopOnClose stops after the host closed the connection or Run's context
// ended: the framework's stop (up to 5 s) and the wait for the logouts of
// refused sessions (refusedEndsAtClose) run side by side, and it returns
// when both are over.
func (b *Backend) stopOnClose() {
	ends := make(chan struct{})
	go func() {
		defer close(ends)
		b.waitRefusedEnds(time.Now().Add(refusedEndsAtClose))
	}()
	b.stopBridge(5 * time.Second)
	<-ends
}

// onSigterm handles SIGTERM for the whole of Run. The engine sends it to the
// worker's process group when it stops a worker that did not exit on
// Shutdown or after it lost the channel, and SIGKILL 2 s later
// (KILL_TIMEOUT): so nothing is waited for. It logs sigterm, logs each
// logout of a refused session still running as refused_session_not_ended
// (abandonRefusedEnds), and then dies of SIGTERM as it would have without
// the handler (dieOfSigterm): the engine names a worker's end by its wait
// status, and a death by a signal is worker_killed, not a clean exit. The
// framework is not stopped (that could take 5 s) and the store not closed
// (SQLite's journal makes an end at any point safe; the outbox keeps what
// was not acknowledged). Without the handler Go's default would end the
// process the same way, with no trace in the log. It returns a function that
// uninstalls it.
func onSigterm(logp *atomic.Pointer[logx.Logger], bp *atomic.Pointer[Backend]) func() {
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case <-term:
		case <-done:
			return
		}
		logp.Load().Warn("sigterm")
		if b := bp.Load(); b != nil {
			b.abandonRefusedEnds()
		}
		dieOfSigterm()
	}()
	return func() {
		signal.Stop(term)
		close(done)
	}
}

// dieOfSigterm ends the process by SIGTERM: the log lines are already
// written (logx writes each straight to stderr, unbuffered), the handler is
// removed and the signal sent again to this process, which the runtime's
// default then makes a death by SIGTERM. Should the process still be there
// a second later, it exits 143 (128 + SIGTERM, a shell's way to say it).
func dieOfSigterm() {
	signal.Reset(syscall.SIGTERM)
	_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	<-time.After(time.Second)
	os.Exit(128 + int(syscall.SIGTERM))
}

// waitUntil waits for wg, at most until deadline.
func waitUntil(wg *sync.WaitGroup, deadline time.Time) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
	}
}

func (b *Backend) stopBridge(limit time.Duration) {
	if br := b.bridge(); br != nil {
		br.StopWithTimeout(limit)
	}
}

// SettingInt reads an integer setting.
func SettingInt(init *contract.Init, key string, def int) int {
	v := strings.TrimSpace(init.GetSettings()[key])
	if v == "" {
		return def
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
		if n > 1<<30 {
			return def
		}
	}
	return n
}

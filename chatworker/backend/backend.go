// SPDX-License-Identifier: AGPL-3.0-or-later

// Package backend implements the framework's MatrixConnector and MatrixAPI
// (maunium.net/go/mautrix/bridgev2) with no Matrix homeserver, for one local
// user, and translates everything to and from the channel contract.
//
// Each framework instance is given its own view of the backend (instance),
// which carries the store's epoch of when it was made: what an instance
// stopped by a logout still writes or reports after the wipe is refused
// (instance.write, instance.emit). The framework's own tables and the
// connector's child database have no such guard (see go/README.md).
//
// In every framework instance, before the first event about a signed-in
// login's chats or the people it met (a chat's info, abilities or removal,
// a message, an edit, a reaction, a deletion, a read, a delivery, a history
// batch, a PersonInfo) is stored, a PersonInfo with is_self and the
// account's own person id (selfFor, the id its chats' members and senders
// carry) has been stored in that instance: once the framework keeps the
// login, or else just before that first event (instance.ensureSelf,
// emitChat). The one exception, a person no login is known to have met
// yet, is in go/README.md, "Event order".
//
// Upward: every call the framework makes (a message per part, an edit, a
// reaction, a room, a ghost profile, a bridge state) becomes a contract event
// written to the outbox. Nothing slow happens inside those calls.
//
// Downward: every contract command becomes the framework's own input (a
// Matrix-shaped event queued with a zero buffer so the result is synchronous,
// or a direct call such as ResolveIdentifier or a login process step).
package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"lynx.invalid/ingest/chatworker/contract"
	"lynx.invalid/ingest/chatworker/logx"
	"lynx.invalid/ingest/chatworker/store"
)

// Hooks are what differs between connectors.
type Hooks struct {
	// SendKeyFormat turns the host's send key into the transaction id the
	// connector uses as the network-side id. Nil keeps the key as it is.
	SendKeyFormat func(key string) networkid.RawTransactionID
	// SelfPersonID is the network user id of a login's own account. Nil
	// means the login id is the user id (true for Telegram and fakenet).
	SelfPersonID func(login *bridgev2.UserLogin) networkid.UserID
	// ShowExpiresIn is the lifetime of a shown QR or code, in seconds, when
	// the connector knows it (the framework's step does not carry it).
	ShowExpiresIn func(step *bridgev2.LoginStep) uint32
	// IdentifierScheme prefixes usernames in identifiers ("telegram"),
	// default the connector's network id.
	IdentifierScheme string
	// EndRefusedSession: after a sign-in of another account is refused, a
	// client is made for it (LoadUserLogin on the unstored login) and its
	// LogoutRemote called, to end the session the sign-in opened. Set it only
	// for a connector whose LoadUserLogin and LogoutRemote, for a login never
	// stored or connected, write nothing to the store and keep nothing.
	EndRefusedSession bool
	// EndRefused, when set, is used instead of EndRefusedSession: it ends
	// the session a refused sign-in opened at the network, given the real
	// connector and the refused login as the connector's login flow built
	// it (never stored, cached or connected). It runs once, in the
	// background, under a deadline. It must write nothing to the store and
	// keep nothing; nil means the session was ended, ErrNothingToEnd that
	// there was none to end, and each is logged by its own code.
	EndRefused func(ctx context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) error
	// DropRefused, when set, is used instead of EndRefused and
	// EndRefusedSession, for a connector whose login flow stores the new
	// session itself before the gate sees its login (whatsmeow saves the
	// paired device during pairing). The gate calls it synchronously, once,
	// before it refuses the login, also while the worker is stopping, with
	// the same arguments as EndRefused and a context of its own (bounded,
	// not cancelled with the sign-in). It must delete from the store
	// everything the connector stored of that login, and nothing of any
	// other login. It returns end, the end of the login's session at the
	// network, which the backend runs as it runs EndRefused (in the
	// background, under the same deadline, not started once the worker
	// stops; the same results, logged by the same codes); a nil end is
	// logged as refused_session_nothing_to_end. end may come with an error.
	// An error means something of the refused login may still be in the
	// store: the sign-in fails as other_account_kept, the connection is
	// ENDED with the same code, and no sign-in is begun on that store
	// (ErrOtherAccountKept), never principal_mismatch.
	DropRefused func(ctx context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) (end func(ctx context.Context) error, err error)
	// EndedCodes are the connector's bridge state error codes that say the
	// network ended the session (the device was unlinked, the account
	// banned): a BAD_CREDENTIALS state with one of them is reported ENDED,
	// with that code, instead of NEEDS_USER (ReportedState). Nil: every
	// BAD_CREDENTIALS state is NEEDS_USER.
	EndedCodes []status.BridgeStateErrorCode
	// SignInFlows, when not nil, are the only sign-in flows (by id) the
	// worker offers: ListSignInFlows lists only these of the connector's,
	// and SignInBegin of any other is refused as unknown_flow. Nil offers
	// every flow of the connector.
	SignInFlows []string
	// IsChannel tells whether a portal that is not a space is a channel all
	// the same (channel.go), from what is stored of it (ChannelQuery: the
	// portal's row, read again for each call, with the connector's metadata
	// decoded; whether the framework re-IDed it; its room's power levels). It
	// must make no network call, and say true only on positive evidence in
	// the connector's own stored data: a room is a group until that says
	// otherwise. Nil: only a space is a channel.
	IsChannel func(q ChannelQuery) bool
	// RateLimitWait tells whether an error of the connector's FetchMessages
	// is the network's rate limit, and the wait the network named (0: none
	// named). FetchHistory answers such an error OUTCOME_REJECTED
	// rate_limited with the wait (history.go). Nil: no error is one.
	RateLimitWait func(err error) (wait time.Duration, ok bool)
	// WatchRateLimit returns ctx made so that the connector, given it, has
	// onWait called with the wait each time it is about to sleep out a
	// rate-limit wait itself (mautrix-telegram: a FLOOD_WAIT under 5
	// minutes). FetchHistory gives the connector such a context and ends the
	// fetch at once when the wait is too long to sleep out inside the
	// command (history.go). onWait may be called from any goroutine, also
	// after the fetch returned. Nil: such waits are not seen, and a fetch
	// that sleeps is ended only at the command's deadline (fetch_timed_out).
	WatchRateLimit func(ctx context.Context, onWait func(wait time.Duration)) context.Context
	// OriginalMessageID tells, of a message id of the framework's rows,
	// whether it is one the connector gave the row after its message was
	// reported (renamed), and then the id the message was reported under
	// (orig; "" when that cannot be told from the id). In mautrix-telegram,
	// the messages of a basic group from before its upgrade to a
	// supergroup, renamed migrated:<id>:<time>. Every id of an existing row
	// the worker names to the host (the target of a read, a delivery, an
	// edit, a reaction or a deletion) is the original one; what would name
	// a renamed row without one is not reported (reid.go, hostMessageID).
	// It must make no network call. Nil: no row is renamed.
	OriginalMessageID func(id networkid.MessageID) (orig networkid.MessageID, renamed bool)
	// PrepareLogins, when set, runs once at each start of the worker (not
	// after a wipe's restart of the framework), after the stored logins are
	// loaded and kept and before any of them connects (StartLogins), with
	// the real connector, the framework instance and those logins (none
	// before the connection's first sign-in). It may change a login's
	// metadata and save it (the connector's client holds the same
	// metadata), and keep facts of its own in the framework's kv_store
	// (br.DB.KV), which a wipe empties with the account. It must make no
	// network call. An error is logged (prepare_logins_failed) and the
	// start goes on. Nil: nothing runs.
	PrepareLogins func(ctx context.Context, inner bridgev2.NetworkConnector, br *bridgev2.Bridge, logins []*bridgev2.UserLogin) error
}

// Backend is what the worker's framework instances share. Each instance is
// handed its own view of it as its MatrixConnector (instance).
type Backend struct {
	// brp is the framework instance. Logout replaces it with a new one on
	// the emptied store (see logout), so it is read through bridge().
	brp   atomic.Pointer[bridgev2.Bridge]
	st    *store.Store
	out   *Emitter
	log   logx.Logger
	init  *contract.Init
	hooks Hooks
	boot  uint64
	// worker and upLog build a framework instance (start, logout).
	worker Worker
	upLog  zerolog.Logger

	stateDir    string
	mediaDir    string
	transferDir string
	// spool is the encrypted media spool (spool.go).
	spool *spool

	selfMu sync.RWMutex
	self   map[networkid.UserLoginID]networkid.UserID

	statusMu sync.Mutex
	statuses map[statusKey]*bridgev2.MessageStatus
	waiting  map[id.EventID]*pendingSend

	sendMu   sync.Mutex
	inflight map[string]*inflightSend

	loginMu sync.Mutex
	signIn  *signIn

	// silenced drops every event while Logout wipes the store.
	silenced atomic.Bool
	// holdPeople buffers ghost profiles while a sign-in whose account is not
	// yet known runs (only when Init names a principal).
	peopleMu   sync.Mutex
	holdPeople bool
	heldPeople []heldPerson
	// loggedOut are logins the backend ended itself; their late states are
	// not reported again.
	loggedOut sync.Map
	// refused is set while the store may still hold rows of another account
	// than the connection's: its wipe failed (wipeOther), or its logins
	// could not be read after a sign-in (refuseUnreadable). The host deletes
	// such a store; until it stops the worker, no sign-in is begun on it and
	// commands are answered other_account_kept.
	refused atomic.Bool
	// wipeMu is held by a wipe of everything (logout, wipeOther) from its
	// start until its outcome is queued: a Shutdown waits for it within the
	// grace (waitWipe), so that a stop does not cut a wipe in half.
	wipeMu sync.Mutex
	// account is the connection's account as far as it is known (string):
	// Init's principal, else the stored login's, else the last signed-in
	// one's; "" when none is known. Only its events go upward (allowed).
	account atomic.Value
	// notReady holds the code when the framework could not start.
	notReady atomic.Value
	// exitReq is closed when the worker must exit with ExitRestartFailed
	// (requestExit).
	exitReq  chan struct{}
	exitOnce sync.Once

	// historyMu is held by the one FetchHistory that runs at a time
	// (history.go); another is refused worker_busy while it is held.
	historyMu sync.Mutex

	// endsMu guards ends, the logouts of refused sessions running in the
	// background (endRefused, waitRefusedEnds).
	endsMu       sync.Mutex
	ends         map[*refusedEnd]struct{}
	endsStopping bool

	// emitBetween, when set (tests only), runs in emit between its read of
	// the store's epoch and its silence check.
	emitBetween func()
	// writeHook, when set (tests only), runs in instance.write inside the
	// store's epoch guard, before the write.
	writeHook func()
	// ensureHook, when set (tests only), runs in ensurePerson for a person
	// not checked yet, before it waits for its turn (ensureMu).
	ensureHook func()
}

type heldPerson struct {
	// fw is the framework instance the profile came from: it is reported
	// through that instance's epoch.
	fw     *instance
	userID id.UserID
	info   *contract.PersonInfo
}

// New makes the backend. The framework is attached by NewBridge (Init),
// given a view of the backend per instance (instance).
func New(init *contract.Init, st *store.Store, out *Emitter, log logx.Logger, hooks Hooks, boot uint64) *Backend {
	b := &Backend{
		st:          st,
		out:         out,
		log:         log,
		init:        init,
		hooks:       hooks,
		boot:        boot,
		stateDir:    init.GetStateDir(),
		mediaDir:    filepath.Join(init.GetStateDir(), "media"),
		transferDir: filepath.Join(init.GetStateDir(), "transfer"),
		self:        map[networkid.UserLoginID]networkid.UserID{},
		statuses:    map[statusKey]*bridgev2.MessageStatus{},
		waiting:     map[id.EventID]*pendingSend{},
		inflight:    map[string]*inflightSend{},
		exitReq:     make(chan struct{}),
	}
	b.account.Store(init.GetPrincipal())
	return b
}

// knownAccount is the connection's account, "" when not known yet.
func (b *Backend) knownAccount() string { s, _ := b.account.Load().(string); return s }

// SetSpoolKey derives the media spool's keys from the store key (the
// caller zeroes its copy afterwards). The budget is spool_budget_bytes, by
// default SpoolBudget.
func (b *Backend) SetSpoolKey(storeKey []byte) error {
	sp, err := newSpool(b.mediaDir, storeKey, int64(SettingInt(b.init, "spool_budget_bytes", SpoolBudget)))
	if err != nil {
		return err
	}
	b.spool = sp
	return nil
}

// Bridge is the framework instance, once attached.
func (b *Backend) Bridge() *bridgev2.Bridge { return b.bridge() }

func (b *Backend) bridge() *bridgev2.Bridge { return b.brp.Load() }

// ---- MatrixConnector ----

func (b *Backend) Init(br *bridgev2.Bridge) { b.brp.Store(br) }

func (b *Backend) Start(ctx context.Context) error {
	if err := os.MkdirAll(b.mediaDir, 0o700); err != nil {
		return fmt.Errorf("media directory: %w", err)
	}
	return nil
}

func (b *Backend) PreStop() {}
func (b *Backend) Stop()    {}

func (b *Backend) GetCapabilities() *bridgev2.MatrixCapabilities {
	return &bridgev2.MatrixCapabilities{
		// Members arrive in CreateRoom, with no invite/join dance per member.
		AutoJoinInvites: true,
		// Without it the framework never sends backward history.
		BatchSending: true,
		// One SetProfile call per ghost change carries everything.
		ReplaceEntireProfile: true,
	}
}

func (b *Backend) ParseGhostMXID(userID id.UserID) (networkid.UserID, bool) {
	return parseGhost(userID)
}

func (b *Backend) FormatGhostMXID(userID networkid.UserID) id.UserID { return formatGhost(userID) }

// GhostIntent, BotIntent, NewUserIntent, SendBridgeStatus,
// SendMessageStatus, BatchSend and GenerateDeterministicRoomID are the
// instance's (instance.go): they hand out intents, write or report.

func (b *Backend) GenerateContentURI(ctx context.Context, mediaID networkid.MediaID) (id.ContentURIString, error) {
	return directURI(mediaID), nil
}

func (b *Backend) ParseContentURI(ctx context.Context, uri id.ContentURIString) (networkid.MediaID, error) {
	kind, val, err := parseMediaURI(uri)
	if err != nil {
		return nil, err
	}
	if kind != mediaKindDirect {
		return nil, fmt.Errorf("not a direct media URI")
	}
	return networkid.MediaID(val), nil
}

func (b *Backend) GetPowerLevels(ctx context.Context, roomID id.RoomID) (*event.PowerLevelsEventContent, error) {
	pl := &event.PowerLevelsEventContent{}
	raw, err := b.st.GetState(ctx, string(roomID), event.StatePowerLevels.Type, "")
	if err != nil || raw == nil {
		return pl, err
	}
	return pl, json.Unmarshal(raw, pl)
}

func (b *Backend) GetMembers(ctx context.Context, roomID id.RoomID) (map[id.UserID]*event.MemberEventContent, error) {
	ms, err := b.st.Members(ctx, string(roomID))
	if err != nil {
		return nil, err
	}
	out := make(map[id.UserID]*event.MemberEventContent, len(ms))
	for _, m := range ms {
		out[id.UserID(m.UserID)] = &event.MemberEventContent{
			Membership:  event.Membership(m.Membership),
			Displayname: m.Displayname,
			AvatarURL:   id.ContentURIString(m.AvatarURL),
		}
	}
	return out, nil
}

func (b *Backend) GetMemberInfo(ctx context.Context, roomID id.RoomID, userID id.UserID) (*event.MemberEventContent, error) {
	ms, err := b.GetMembers(ctx, roomID)
	if err != nil {
		return nil, err
	}
	return ms[userID], nil
}

// GetStateEvent answers from the stored room state.
func (b *Backend) GetStateEvent(ctx context.Context, roomID id.RoomID, eventType event.Type, stateKey string) (*event.Event, error) {
	raw, err := b.st.GetState(ctx, string(roomID), eventType.Type, stateKey)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, mautrix.MNotFound
	}
	evt := &event.Event{
		Type:     eventType,
		RoomID:   roomID,
		StateKey: &stateKey,
		Content:  event.Content{VeryRaw: raw},
	}
	_ = evt.Content.ParseRaw(eventType)
	return evt, nil
}

func (b *Backend) GenerateDeterministicEventID(roomID id.RoomID, key networkid.PortalKey, messageID networkid.MessageID, partID networkid.PartID) id.EventID {
	return messageEventID(key, messageID, partID)
}

func (b *Backend) GenerateReactionEventID(roomID id.RoomID, target *database.Message, sender networkid.UserID, emojiID networkid.EmojiID) id.EventID {
	var msg networkid.MessageID
	var part networkid.PartID
	if target != nil {
		msg, part = target.ID, target.PartID
	}
	return reactionEventID(roomID, msg, part, sender, emojiID)
}

func (b *Backend) ServerName() string { return ServerName }

// ---- identity helpers ----

// rememberSelf records a login's own network user id.
func (b *Backend) rememberSelf(login *bridgev2.UserLogin) {
	if login == nil {
		return
	}
	self := networkid.UserID(login.ID)
	if b.hooks.SelfPersonID != nil {
		if s := b.hooks.SelfPersonID(login); s != "" {
			self = s
		}
	}
	b.selfMu.Lock()
	b.self[login.ID] = self
	b.selfMu.Unlock()
}

func (b *Backend) forgetSelf(loginID networkid.UserLoginID) {
	b.selfMu.Lock()
	delete(b.self, loginID)
	b.selfMu.Unlock()
}

func (b *Backend) forgetSelves() {
	b.selfMu.Lock()
	clear(b.self)
	b.selfMu.Unlock()
}

// selfFor is the own person id of the login that receives a chat.
func (b *Backend) selfFor(receiver networkid.UserLoginID) string {
	b.selfMu.RLock()
	defer b.selfMu.RUnlock()
	if s, ok := b.self[receiver]; ok {
		return string(s)
	}
	if receiver != "" {
		return string(receiver)
	}
	for _, s := range b.self {
		return string(s)
	}
	return ""
}

// personID maps an MXID to the contract's person id: a ghost to its network
// user id, the owner to the login's own account. Empty for anything else.
func (b *Backend) personID(user id.UserID, receiver networkid.UserLoginID) string {
	if user == OwnerMXID {
		return b.selfFor(receiver)
	}
	if g, ok := parseGhost(user); ok {
		return string(g)
	}
	return ""
}

// allowed tells whether events of a login may go upward. When the
// connection's account is known (Init's principal, or the stored login's),
// only that account's events do: a sign-in that turns out to be another
// account is logged out and wiped before anything of it is reported, also
// when a connector that bypasses the gate (completeSignIn) let its login
// connect and sync first.
func (b *Backend) allowed(receiver networkid.UserLoginID) bool {
	if b.silenced.Load() {
		return false
	}
	a := b.knownAccount()
	return a == "" || receiver == "" || string(receiver) == a
}

// emit is Emitter.Emit behind the silence switch. The store's epoch is read
// before the switch: a logout silences, then wipes (a new epoch), so an
// event that got past the switch before it was set carries the epoch before
// the wipe and is not stored after it (Emitter.EmitSince). Read after the
// switch, the epoch could be the new one, and an event of the old login
// that passed the switch before the logout set it would be stored and sent
// after the wipe (TestEmitEpochBeforeSilence).
//
// The events a framework instance reports go through the instance's epoch
// instead (instance.emit), read when the instance was made.
// It returns the event's seq, 0 when it was not stored.
func (b *Backend) emit(eventID string, kind isChatEventKind) uint64 {
	epoch := b.st.Epoch()
	if f := b.emitBetween; f != nil {
		f()
	}
	return b.emitSince(epoch, eventID, kind)
}

// emitSince is Emitter.EmitSince behind the silence switch: the event's
// seq, 0 when it was not stored (silenced, wiped since epoch, or the outbox
// failed).
func (b *Backend) emitSince(epoch uint64, eventID string, kind isChatEventKind) uint64 {
	if b.silenced.Load() {
		return 0
	}
	return b.out.EmitSince(epoch, eventID, kind)
}

// roomKey is the chat a room stands for, with its login.
func (b *Backend) roomKey(ctx context.Context, roomID id.RoomID) (*store.Room, bool) {
	r, err := b.st.Room(ctx, string(roomID))
	if err != nil || r == nil {
		return nil, false
	}
	return r, true
}

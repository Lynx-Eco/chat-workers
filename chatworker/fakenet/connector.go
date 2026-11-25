// SPDX-License-Identifier: AGPL-3.0-or-later

// Package fakenet is a scripted bridgev2 network connector with no I/O, for
// tests of the backend. It is built only into chat_fakenet and never into a
// shipped worker for a real network.
//
// The script is a JSON file named by Init.settings["script"] (see Script);
// magic words in sent text steer failures (see the Magic constants). What the
// "network" accepted is appended to <state_dir>/fakenet-network.jsonl, which
// survives crashes, so tests can count deliveries across restarts.
package fakenet

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.mau.fi/util/configupgrade"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/mediaproxy"
)

// Script steers the fake network. Every field has a usable zero value.
type Script struct {
	// Account is the login id (and own user id) a sign-in completes as.
	Account string `json:"account"`
	// QRAccount, when set, is the account the QR flow completes as instead.
	QRAccount string `json:"qr_account"`
	// BypassLoadGate makes the fake login load its new login itself
	// (NewLoginParams.LoadUserLogin), as some connectors do, so the worker's
	// gate on the framework's LoadUserLogin is not asked.
	BypassLoadGate bool `json:"bypass_load_gate"`
	// CompleteDelayMs holds the end of a sign-in that long after its login
	// is made and connected, as a connector that syncs before it reports
	// the login does (with BypassLoadGate: the login's sync runs first).
	CompleteDelayMs int `json:"complete_delay_ms"`
	// FailAfterLogin makes the sign-in fail once its login is made and
	// connected (with BypassLoadGate: a login the gate never saw, left in
	// the store by a login process that failed after).
	FailAfterLogin bool `json:"fail_after_login"`
	// CompleteNoLoginID makes a sign-in that made its login complete without
	// naming it (LoginCompleteParams with no UserLoginID): with
	// BypassLoadGate, a login left in the store by a process that ended
	// without one.
	CompleteNoLoginID bool `json:"complete_no_login_id"`
	// StoreAfterCancelGate, when set, is the path of a FIFO: a QR sign-in
	// whose wait is cancelled (the host's SignInCancel) then makes its login
	// all the same, once something was written to that file, and only then
	// returns from its wait (with BypassLoadGate: a login stored after the
	// cancel, by a login process that does not heed it at once).
	StoreAfterCancelGate string `json:"store_after_cancel_gate"`
	// AccountName is the account's display name.
	AccountName string `json:"account_name"`
	// NoDedupe makes the network accept a repeated transaction id again.
	NoDedupe bool `json:"no_dedupe"`
	// HangMs bounds "!hang" (0: until the worker stops).
	HangMs int `json:"hang_ms"`
	// LogoutHangMs holds LogoutRemote that long before it logs out, and it
	// does not heed its context: a network that does not answer a logout.
	LogoutHangMs int `json:"logout_hang_ms"`
	// ShowWaitMs is how long the shown code waits for "the other device".
	ShowWaitMs int `json:"show_wait_ms"`
	// QRRotateMs is the life of each QR code (two rotations, then done).
	QRRotateMs int `json:"qr_rotate_ms"`
	// AfterSync is a state the login reports after its initial sync:
	// "bad_credentials" or "logged_out".
	AfterSync string `json:"after_sync"`
	// FailRestart makes every start of the connector after the first in
	// the process fail, as when a framework instance cannot be started again
	// on the store a logout emptied.
	FailRestart bool `json:"fail_restart"`
	// NoSync skips the initial sync.
	NoSync bool `json:"no_sync"`
	// NoSelfInfo makes the account's own user info unknown to the network
	// (GetUserInfo answers nothing for it), as a network that sends the own
	// profile late or never: the framework then reports no profile of the
	// account's own ghost, which otherwise happens to come before its chats.
	NoSelfInfo bool `json:"no_self_info"`
	// Channel adds a broadcast channel to the sync (Chan: a space room, the
	// kind the worker reports as CHAT_KIND_CHANNEL) with a post by a poster
	// met nowhere else, a reaction to it and a post with an uploaded
	// attachment; every later connect of the login posts once more
	// (ChanPostAgain).
	Channel bool `json:"channel"`
	// TurnedChannel adds a room of the default type to the sync (TurnChan)
	// that the network first describes with no evidence of what it is (a
	// group: a post by Alice), then with its stored metadata saying
	// broadcast (PortalMeta, IsBroadcast: a channel). Then come a post, a
	// reaction, and a post by TurnPoster, who is met nowhere else before
	// and then sends TurnGroupMsg in the group; every later connect of the
	// login posts once more in it (TurnPostAgain).
	TurnedChannel bool `json:"turned_channel"`
	// TurnBack, with TurnedChannel, stores TurnChan's metadata as not
	// broadcast again at every later connect, before its post.
	TurnBack bool `json:"turn_back"`
	// GhostUpdates gives ChanGhost, ChanPoster and Frank their profiles at
	// the start of the sync, without any event; with Channel, ChanGhost
	// posts in the channel, and Frank writes in the group (FrankMsg).
	GhostUpdates bool `json:"ghost_updates"`
	// ReID adds two chats to the sync that the network then re-IDs the way
	// mautrix-telegram re-IDs a basic group upgraded to a supergroup
	// (migrateChat: the framework's Bridge.ReIDPortal, then, when the source
	// portal was renamed, a chat-info sync of it under its new key):
	// MiniGroup, after Alice's MiniMsg in it, becomes SuperGroup, where she
	// then sends SuperMsg; and MergeFrom is re-IDed onto MergeInto, which
	// has a room of its own (the framework deletes MergeFrom's room). The
	// network serves SuperGroup's older history (SuperHistoryID), and every
	// later connect of the login sends one more message of Alice's in
	// SuperGroup (ReIDAgain).
	ReID bool `json:"reid"`
	// LateOld, with ReID, has LateOldN messages of LateSender's come under
	// MiniGroup's key after the re-IDs (an event of the old key handled
	// after the re-ID, as mautrix-telegram's basic group message after the
	// supergroup's MigrateFrom), and one more at each later connect, before
	// its ReIDAgain message. Each one's handling result goes to the network
	// log ("late", Txn: its id, Target: ok, ignored or failed).
	LateOld bool `json:"late_old"`
	// Rejoin, with LateOld, has the user leave SuperGroup after the late
	// messages (a chat deletion: the framework deletes its portal and
	// room), the network re-ID MiniGroup's portal (the late messages' one)
	// onto SuperGroup again, which then has no portal (mautrix-telegram's
	// checkDialogMigrated at the next dialog sync), and Alice send
	// RejoinMsg in SuperGroup.
	Rejoin bool `json:"rejoin"`
	// CancelCreate, with Rejoin, has Alice's CancelledMsg come in
	// SuperGroup after the user left it, before that re-ID, and makes the
	// re-ID while the framework is making that message's room: from
	// GetCapabilities, the network's first answer after the framework
	// asked for the room's id (GenerateDeterministicRoomID). The framework
	// cancels the creation (Bridge.ReIDPortal: the target's room is being
	// made), deletes SuperGroup's portal row, which has no room, and re-IDs
	// MiniGroup's portal onto that key (ReIDResultTargetDeletedAndSourceReIDd);
	// the message's handling fails. The worker keeps the room id it gave,
	// never created. A creation that is not cancelled is logged
	// ("cancel_missed") and the re-ID is made after it.
	CancelCreate bool `json:"cancel_create"`
	// LateWrites keeps, when a login is logged out, the framework instance
	// it ran in, and has that instance write (through the view and the
	// intents the backend gave it) at the start of the next sign-in, after
	// the logout stopped it and wiped the store: a goroutine of an old
	// instance that outlived its stop (see LateWrite).
	LateWrites bool `json:"late_writes"`
	// Ticker sends Count messages after the sync, one every IntervalMs,
	// alternating between the two direct chats.
	Ticker struct {
		Count      int `json:"count"`
		IntervalMs int `json:"interval_ms"`
		DelayMs    int `json:"delay_ms"`
		// PadBytes makes each message that much longer.
		PadBytes int `json:"pad_bytes"`
	} `json:"ticker"`
}

// Magic words in the text of a sent message.
const (
	MagicCertain     = "!certain"      // refused, certainly not sent
	MagicUncertain   = "!uncertain"    // failed, maybe sent
	MagicHang        = "!hang"         // the network call does not return (see HangMs)
	MagicCrashBefore = "!crash-before" // exit 3 before the network accepted it
	MagicCrashAfter  = "!crash-after"  // exit 3 after the network accepted it
)

// Connector is the fake network.
type Connector struct {
	br       *bridgev2.Bridge
	db       *dbutil.Database
	script   Script
	stateDir string

	mu        sync.Mutex
	delivered map[string]string // txn -> remote id
	logFile   *os.File
	seq       int
}

var (
	_ bridgev2.NetworkConnector      = (*Connector)(nil)
	_ bridgev2.DirectMediableNetwork = (*Connector)(nil)
)

// New reads the script named in the settings.
func New(stateDir string, settings map[string]string) (*Connector, error) {
	c := &Connector{stateDir: stateDir, delivered: map[string]string{}}
	if path := settings["script"]; path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("script: %w", err)
		}
		if err := json.Unmarshal(raw, &c.script); err != nil {
			return nil, fmt.Errorf("script: %w", err)
		}
	}
	if c.script.Account == "" {
		c.script.Account = string(Me)
	}
	if c.script.AccountName == "" {
		c.script.AccountName = "Fake User"
	}
	if c.script.ShowWaitMs <= 0 {
		c.script.ShowWaitMs = 200
	}
	if c.script.QRRotateMs <= 0 {
		c.script.QRRotateMs = 150
	}
	return c, nil
}

// NetworkLog is the path of the delivery log.
func NetworkLog(stateDir string) string { return filepath.Join(stateDir, "fakenet-network.jsonl") }

// Record is one line of the delivery log. Besides what the network
// accepted: "login" (Target: the login's account) when a sign-in made its
// login, "logout" (Target: the account) when a login was logged out,
// "synced" after an initial sync.
type Record struct {
	Kind     string `json:"kind"`
	Txn      string `json:"txn,omitempty"`
	Chat     string `json:"chat,omitempty"`
	RemoteID string `json:"remote_id,omitempty"`
	Target   string `json:"target,omitempty"`
}

// ReadNetworkLog reads the delivery log (tests).
func ReadNetworkLog(stateDir string) ([]Record, error) {
	f, err := os.Open(NetworkLog(stateDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Record
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r Record
		if json.Unmarshal(sc.Bytes(), &r) == nil {
			out = append(out, r)
		}
	}
	return out, sc.Err()
}

func (c *Connector) Init(br *bridgev2.Bridge) {
	c.br = br
	c.db = br.DB.Child(VersionTable, upgrades, dbutil.ZeroLogger(br.Log.With().Str("db_section", "fakenet").Logger()))
	recs, _ := ReadNetworkLog(c.stateDir)
	for _, r := range recs {
		if r.Kind == "deliver" {
			c.delivered[r.Txn] = r.RemoteID
			c.seq++
		}
	}
	f, err := os.OpenFile(NetworkLog(c.stateDir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err == nil {
		c.logFile = f
	}
}

func (c *Connector) record(r Record) {
	if c.logFile == nil {
		return
	}
	line, _ := json.Marshal(r)
	_, _ = c.logFile.Write(append(line, '\n'))
	_ = c.logFile.Sync()
}

func (c *Connector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName: "FakeNet", NetworkURL: "https://fakenet.invalid", NetworkID: "fakenet",
		BeeperBridgeType: "fakenet", DefaultCommandPrefix: "!fake",
	}
}

// LoginMeta is stored with each login.
type LoginMeta struct {
	Synced bool `json:"synced"`
}

// PortalMeta is stored with each portal. Broadcast is set by the second
// chat info of TurnChan (Script.TurnedChannel): the connector's own stored
// evidence that the room is a broadcast channel, as mautrix-telegram's
// metadata is for a Telegram channel.
type PortalMeta struct {
	Broadcast bool `json:"broadcast,omitempty"`
}

// IsBroadcast is the worker's Hooks.IsChannel for fakenet: a portal whose
// stored metadata says broadcast.
func IsBroadcast(p *database.Portal) bool {
	m, ok := p.Metadata.(*PortalMeta)
	return ok && m != nil && m.Broadcast
}

func (c *Connector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		UserLogin: func() any { return &LoginMeta{} },
		Portal:    func() any { return &PortalMeta{} },
	}
}

func (c *Connector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (c *Connector) GetConfig() (string, any, configupgrade.Upgrader) { return "", nil, nil }

func (c *Connector) GetBridgeInfoVersion() (int, int) { return 1, 1 }

func (c *Connector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	login.Client = &Client{c: c, ul: login}
	return nil
}

func (c *Connector) SetUseDirectMedia() {}

// MediaBytes are the deterministic bytes of a fake media id.
func MediaBytes(mediaID string, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = mediaID[i%len(mediaID)] ^ byte(i)
	}
	return b
}

// MediaSize is the size of every fake image.
const MediaSize = 12345

func (c *Connector) Download(ctx context.Context, mediaID networkid.MediaID, params map[string]string) (mediaproxy.GetMediaResponse, error) {
	id := string(mediaID)
	if !strings.HasPrefix(id, "img-") {
		return nil, errors.New("fakenet: no such media")
	}
	return mediaproxy.GetMediaResponseRawData(MediaBytes(id, MediaSize)), nil
}

// deliver is the network accepting a message.
func (c *Connector) deliver(txn, chat string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, seen := c.delivered[txn]; seen && txn != "" && !c.script.NoDedupe {
		c.record(Record{Kind: "duplicate", Txn: txn, Chat: chat, RemoteID: prev})
		return prev
	}
	c.seq++
	remote := fmt.Sprintf("net-%d", c.seq)
	if txn != "" {
		c.delivered[txn] = remote
	}
	c.record(Record{Kind: "deliver", Txn: txn, Chat: chat, RemoteID: remote})
	return remote
}

func (c *Connector) hang(ctx context.Context) error {
	var limit <-chan time.Time
	if c.script.HangMs > 0 {
		limit = time.After(time.Duration(c.script.HangMs) * time.Millisecond)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-limit:
		return nil
	}
}

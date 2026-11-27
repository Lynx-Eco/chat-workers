# go/: chat workers (AGPL-3.0-or-later)

Everything under `go/` is licensed **AGPL-3.0-or-later** (`LICENSE`), separately
from the rest of this repository, with one exception:
`chatworker/contract/channel.pb.go` is generated from the contract file, which
is Apache-2.0, and carries that file's licence line. No code outside `go/` links it: the engine
starts a worker binary as its own process and speaks the channel contract
(`crates/ingest_models/schemas/channel.proto`, `ingest.channel.v1`) to it over a
Unix socket.

## Layout

One Go module, `chatworker/` (module path `lynx.invalid/ingest/chatworker`, a
path that can never be fetched):

| Package | What |
|---|---|
| `contract/` | Go types of `ingest.channel.v1`, generated and committed (`gen.sh` has the pinned tools and the command); checked against the shared vectors in `crates/ingest_models/tests/vectors/channel_v1` |
| `wire/` | v2 framing and the worker's end of the connection |
| `hosttest/` | an in-process fake of the engine's side, for tests only |
| `store/` | the encrypted SQLite file (`db.go`) and the worker's own tables (delivery queue, send journal) beside bridgev2's in it |
| `backend/` | the bridgev2 backend, the command handlers, the encrypted media spool and the worker's run loop |
| `logx/` | logging policy: fixed codes and the connection id only |
| `fakenet/` | a scripted test network connector, only in `chat_fakenet` |
| `cmd/chat_fakenet/` | the fake-network worker and the end-to-end tests |
| `cmd/chat_telegram/` | the Telegram worker: the unmodified `go.mau.fi/mautrix-telegram` v0.2611.0 connector (it pins mautrix-go v0.33.0) |
| `cmd/chat_whatsapp/` | the WhatsApp worker, registered in the engine (`src/providers/chat_whatsapp.rs`); whether a release carries it is the release's choice (it stages the workers it names with `--go-bin`): the unmodified `go.mau.fi/mautrix-whatsapp` v0.2611.0 connector (it pins the same mautrix-go v0.33.0) and `go.mau.fi/whatsmeow` under it |
| `cmd/chat_signal/` | the Signal worker: the unmodified `go.mau.fi/mautrix-signal` v0.2611.0 connector (the same mautrix-go v0.33.0), signalmeow under it, and libsignal v0.106.1 linked statically (`libsignal.pin`, "The Signal worker") |
| `cmd/chat_slack/` | the Slack worker, registered in the engine (`src/providers/chat_slack.rs`): the unmodified `go.mau.fi/mautrix-slack` v0.2611.0 connector (the same mautrix-go v0.33.0) and the fork of slack-go it requires |

A worker is the upstream mautrix-go `bridgev2` framework, one upstream network
connector, and our backend, which implements bridgev2's `MatrixConnector` and
`MatrixAPI` with no Matrix homeserver and translates to the contract. Upstream
components and their licences: `NOTICE`.

Workers take `--socket-path <path>` and nothing else; settings and secrets
come in `Init`. The one other invocation is `--source-commit` alone: the
worker prints its stamped commit and a newline and exits 0 before it touches
anything (the engine's build check, "In the engine" below).

## The Telegram worker

`chat_telegram` requires `api_id` and `api_hash` in `Init.settings`
(`missing_settings` otherwise; neither is ever logged); optional:
`device_name` (default `Lynx`), `sync_update_limit`, `sync_create_limit`
(below), and the history settings every worker takes ("History a
connection gets by itself", below). The engine lets one connection set
`sync_create_limit`, `sync_update_limit` and the two history settings for
itself at its sign-in's begin, and change them later (its worker is then
started again), over the host's (`README.md`, "Chat connection settings");
the worker sees one merged `Init.settings` and does not tell them apart.

**The dialog sync after sign-in** (the connector's `sync.update_limit` and
`sync.create_limit`, `chatsync.go`): the connector looks at the
`sync_update_limit` most recently active dialogs (default 100) and makes
rooms for the `sync_create_limit` newest of them (default 15); any other
chat gets its room when a message comes in it, and until then a
`FetchHistory` of it is `unknown_chat`. Each is a decimal count from 0 to
1000 (`cmd/chat_telegram`, `readSyncLimits`); anything else, the
connector's own `-1` ("no limit") included, refuses the start with
`bad_settings`. Blank or absent is the default (today's sign-in). The
update limit is raised to the create limit when it is lower (rooms are made
only among the dialogs looked at, so the create limit would be cut to it
silently). A create limit of 0 makes no room at sign-in; an update limit of
0 looks at no dialog (and makes none). The connector runs the sync once
per login and, once it has finished, skips it at every later connect (its
`sync-chats` command would run it again, and is unreachable here). So the
worker runs it again itself when a limit grows (`cmd/chat_telegram`,
`resync.go`, the backend's `Hooks.PrepareLogins`): at each start, after the
stored login is loaded and before it connects, it compares the two limits
(as the connector applies them: the update limit raised to the create
limit) with those it last applied, kept in the framework's `kv_store`
(`lynx_telegram_sync_limits`; a wipe empties it with the account). When
either is greater, the login's dialog sync is reset (the five fields of the
login's metadata the connector's own restart clears) and saved, and the
connector's connect-time sync runs again from the newest dialog with the
new limits: what the network is asked is that of a sign-in's sync with
them (below): a room already made is not made again, its info and members
are read again as for any dialog looked at, and only messages newer than
the newest it holds are fetched. A limit that
shrinks or stays resets nothing (the rooms made stay). With no record (the
first start of this build, a store a wipe emptied, a connection not signed
in yet) the limits are recorded and nothing is reset: the sign-in's sync ran
with the limits of its own start. A reset whose save fails is logged
(`prepare_logins_failed`), the start goes on, and the next start tries
again. A sync cut off (a stop, a long `FLOOD_WAIT`) resumes at the worker's
next start with the limits that start is given, less the dialogs it already
did; that is why the engine gives a connection its own settings at every
start, not at its sign-in only.

What the sync costs the network, for the dialogs it looks at (U, the
update limit) and the rooms it makes (C, the create limit), one request
after another with nothing between chats (the connector sleeps out a
`FLOOD_WAIT` under 5 minutes and tries a request five times; a longer wait
ends the sync, which resumes at the next start):

- one `messages.getDialogs` per 100 dialogs looked at;
- per dialog looked at, whether or not its room is made: a basic group one
  `messages.getFullChat`, a supergroup one `channels.getParticipants` (its
  100 most recent members), a direct chat or a broadcast channel nothing;
- per room made, its initial import ("History a connection gets by
  itself"): one `messages.getHistory` when `max_initial_messages` is at
  most 100 (0: none), up to five for 500, the connector waiting 2, 4, 6 and
  8 s between them.

So an account of N dialogs, of which a share g are groups, with
`max_initial_messages` at its default of 50:

| Limits (update / create) | Rooms at sign-in | Requests at sign-in, at most |
| --- | --- | --- |
| default (100 / 15) | min(N, 15) | 1 + g·min(N, 100) + min(N, 15): 116 |
| 200 / 200 | min(N, 200) | 2 + g·min(N, 200) + min(N, 200): 402 |
| 1000 / 1000 (the most) | min(N, 1000) | 10 + g·min(N, 1000) + min(N, 1000): 2,010 |
| -1 / -1 (refused) | N | ⌈N/100⌉ + 1 + g·N + N: about 10,000 for N = 5,000 |

The cap is there because of the last line: thousands of requests in one
burst, which Telegram answers with ever longer `FLOOD_WAIT`s, on the user's
own account. 1000 covers the dialogs nearly every account is active in. A
host that makes many rooms can set `max_initial_messages` low (0 makes the
rooms with no import) and page each chat's history itself with
`FetchHistory`, paced ("History a host asks for"). Not measured against
Telegram: how long such a sign-in takes, and where its `FLOOD_WAIT`s start.

Animated stickers are passed on unconverted (no lottie or ffmpeg), the proxy
is off, and send keys become positive int64 `random_id`s. Its sign-in flows
are `phone` (code, then the password if the account has one) and `qr` only
(`backend.Hooks.SignInFlows`): not the connector's `bot` (a bot is not a
user's account) nor `manual` (a session made elsewhere, imported); a
`SignInBegin` of those is refused as `unknown_flow`. Its smoke test starts it
with dummy settings and never contacts Telegram.

**A sign-in that reaches another account** than the connection's fails as
`principal_mismatch` and leaves nothing of that account in the store: the
connector's login flows keep the new session (auth key, data centre, salt)
and the account's user only in memory until they call the framework's
`NewLogin`, and the gate (`backend/gate.go`) refuses the login there, before
anything is inserted. The connection's own login, if the store has one, stays
as it was, and so does the store: the engine, told `principal_mismatch` by
the worker's failed sign-in step, sends no `Logout` (which would end the
connection's own account at the network and wipe the store) and returns
the connection to its state before the re-link; it stops the worker with a
grace of 30 s (the most `Shutdown` is taken at), so that the logout of the
refused session below gets up to 22.5 s. (Only a worker that completes a
sign-in as another account is logged out by the engine.) A connector that
loads its new logins itself passes the gate: the other account's login is
then stored and may have synced, and the worker wipes everything itself, as
at logout, the connection's own login and store included; only the other
account's session is ended at the network, never the connection's own
(`backend.wipeOther`: its login is deleted here, its session at the
network is left as it is). When that wipe
succeeded the sign-in fails as `principal_mismatch` and the connection is
ENDED `other_account_wiped`: nothing of either account is kept, and the
connection signs in again from an empty store. When it failed (the log says
`wipe_failed`) rows of the other account may still be in the store, so the
worker does not say `principal_mismatch`: the sign-in fails as
`other_account_kept`, the connection is ENDED with the same code, and the
engine deletes the state directory and forgets the store key itself.
The same rule holds for a sign-in that such a connector's login process
ends any other way once it has stored the other account's login (it fails
after, the host cancels it, it completes without naming its login, or it
stores the login after the cancel): the store is wiped, and the failure is
`principal_mismatch` (ENDED `other_account_wiped`) or `other_account_kept`
(ENDED the same), never the process's own code
(`backend.refuseIfOtherStored`). A store whose logins cannot be read at
that point (three reads) is no evidence of another account: nothing is
wiped and nobody is logged out at the network, but the sign-in fails as
`other_account_kept` (ENDED the same; the log says `login_read_failed`) so
that the engine deletes the store, and the worker begins no sign-in on it
meanwhile. `principal_mismatch` from a worker always means it kept nothing
of the other account.

**A store never keeps another account's login past a start.** A re-link
cut off before its wipe was done (the worker stopped or killed during it,
or a crash of the login process) can leave the other account's login in the
store. A start that finds a stored login of another account than
`Init.principal` runs none of it: it wipes everything by the same rule
(only the other account's session ended at the network) and reports the
connection ENDED `other_account_wiped`, or `other_account_kept` when the
wipe failed (the log says `principal_mismatch_at_start`;
`backend.refuseStoredAtStart`). There is no sign-in to fail at a start: the
engine takes ENDED `other_account_kept` as during a sign-in (stop, delete
the state directory, forget the key, `needs_user`) and ENDED
`other_account_wiped` as a connection that needs the user (its worker
stopped). A sign-in begun on such a start goes on on the emptied store, or
ends `other_account_kept`. And a stop waits for a wipe in progress (a
cancelled re-link's, a logout's): `Shutdown` waits for it until half the
host's grace has passed (the engine stops a cancelled or timed-out re-link
with its longer grace, 30 s, so up to 15 s), lets no new one begin, and
logs `shutdown_wipe_incomplete` if one still runs then.

The session that a sign-in refused at the gate authorised at Telegram is then
logged out in the background (`cmd/chat_telegram/refused.go`): a client of
the worker's own on a copy of the session in memory (no session file, no
update state, nothing in the store) calls `auth.logOut` once, within 30 s,
and the key is zeroed in the slices the worker's own code holds (the
refused login's, and that of its copy of the session). The client library's
copies are not: gotd copies the key into structures of its own, which Go
cannot zero, and they stay on the heap until they are garbage collected and
the memory reused. The log says `refused_session_ended` or
`refused_session_not_ended` (no network, Telegram refused it or did not
answer: the device then stays authorised on that account until it is
removed there), or
`refused_session_nothing_to_end` when the login carried no session to end:
no auth key, or a session of the `manual` flow (not offered), which would
have existed before the sign-in and is not ended. A stop of the worker
waits for a logout still running: after `Shutdown` (its events and its
`ShutdownAck` sent first) until three quarters of the host's grace have
passed since `Shutdown` came; when the host closed the connection up to
1 s, alongside the framework's stop. A logout still running then is logged
as `refused_session_not_ended` and the worker exits
(`backend.waitRefusedEnds`); so is one that a command still running reaches
after the stop began (it is not started). The engine that lost the channel
sends SIGTERM a few hundred milliseconds after it closed its end, often
before that second is over: on SIGTERM the worker logs `sigterm`, logs each
logout still running as `refused_session_not_ended` and dies of SIGTERM at
once.

## The WhatsApp worker

`chat_whatsapp` is the same backend with the unmodified mautrix-whatsapp
connector: a linked device ("companion") of the user's phone. The engine
has its provider (`whatsapp_linked`, worker `chat_whatsapp`, service
`whatsapp_linked_chat`, in `src/providers/chat_whatsapp.rs`); like every chat
worker it runs only where the host's allowlist names it. Whether a host
allows it is the host's choice, and whether an image carries it the
release's.
`build.sh` builds, vets and tests it with the others.

It needs no settings (WhatsApp has no app id or secret); optional:
`device_name` (default `Lynx`, the name the phone lists under linked devices)
and `max_initial_conversations` (default 15: the chats made from the phone's
history sync after sign-in; any other is made when a message comes in it).
Each of those chats opens with its newest messages from that history sync,
up to `max_initial_messages` ("History a connection gets by itself",
below): the connector keeps the history sync in its own tables and hands it
over only through the framework's backfill of a new room, so it is not
delivered twice. No full history sync is asked of the phone, expired media is not asked for
again, status broadcasts are not bridged, call and identity notices are off,
the proxy is off, and animated stickers are passed on unconverted. There is
no ffmpeg, so the connector converts no voice note or GIF either. Its
sign-in flows are the connector's two, `qr` (a QR code to scan, shown again
as it rotates) and `phone` (the phone number, then a pairing code to type on
the phone); any other `SignInBegin` is refused as `unknown_flow`. Its smoke
test starts it with no settings and never contacts WhatsApp.

What it tells the backend of WhatsApp (`backend.Hooks`, each checked against
the mautrix-whatsapp v0.2611.0 and whatsmeow source, cited in the code, and
unit-tested on stored rows with no network):

- **How long a shown code lasts** (`signin.go`). A QR code is told what is
  left of the connector's schedule: the first a minute, the five after it
  20 seconds each (the connector's `qrIntervals`, copied; then the sign-in
  fails). The step carries no index or time, so the worker keeps the clock,
  per sign-in, told apart by the device keys in the codes; a code shown again
  because the server rotated its secret is told what is left of its
  interval. A pairing code is told 0 ("does not expire"): the connector never
  replaces it, and whatsmeow says its exact expiry is unknown (bounded by the
  login socket the server closes about 160 seconds after connecting); the
  step ends in a `SignInFailed` when that happens.
- **The account's own person id** (`self.go`) is its LID's user id,
  `lid-<digits>`, the form the connector gives the account's messages in
  groups and LID chats and moves direct chats to. The login id, and the
  connection's principal, stays the phone number's digits. The account's
  messages in a chat still addressed by phone number carry the digits as
  their sender; they are `from_me` all the same (the connector marks every
  event of the account's own JIDs, and the backend reports the user's double
  puppet as `from_me`), but that sender's `PersonInfo` is not `is_self`. A
  device with no LID yet (paired before WhatsApp gave one) uses the digits
  until its next connection gives it one.
- **Channels** (`channel.go`): a newsletter (a WhatsApp Channel,
  `<digits>@newsletter`) and the status broadcast (`status@broadcast`, not
  bridged anyway) are `CHANNEL`, by the portal's JID alone. Groups are
  groups, including admins-only groups and a community's announcement group
  (its members may reply); a community is a space, so a channel already.
  Broadcast lists are never chats (the connector puts their messages in the
  sender's direct chat).
- **Send keys** become ids of WhatsApp Web's form (`3EB0` and 18 upper-case
  hex digits, hashed from the key), which the connector sends as the
  message's own id (and an edit's and a deletion's). The journal, not the
  id, keeps a send from going out twice: whatsmeow and the connector send
  any id they are given. The fixed id lets a stored message be traced back
  to its key.
- **Identifiers**: phone numbers are `tel:+<digits>` (the account's from its
  login, a person's from the connector, for a LID too when its phone is
  known); the scheme `whatsapp` names a handle that is not a phone number,
  which the connector does not report today. Person ids are the connector's
  user ids (phone digits or `lid-<digits>`): a person met by number and then
  by LID has two, with the same `tel:` identifier. A number change is a new
  principal (`principal_mismatch` at the gate).

**A re-link that reaches another account** (`refused.go`) fails as
`principal_mismatch` like Telegram's, but WhatsApp needs more than the
gate: whatsmeow saves the paired companion (its keys, the main device's
identity, the account's LID-phone mapping) during pairing, before the
connector calls `NewLogin`, and the companion is then linked on the other
account's phone. So the worker sets `backend.Hooks.DropRefused` (not
`EndRefused`), which the gate runs synchronously before it refuses the
login, also while the worker is stopping:

1. the refused companion is read into memory
   (`Container.GetDevice`, by the login id and the device number in the
   login's metadata);
2. in one transaction, every `whatsmeow_device` row of that phone number is
   deleted (its identities, sessions, pre-keys, sender keys, app state,
   contacts, chat settings, message secrets, salt and buffers go with it by
   `ON DELETE CASCADE`), its `whatsmeow_privacy_tokens` rows (no foreign
   key there), and the account's own row of the global `whatsmeow_lid_map`,
   unless the connection's own companion knows that person (a contact, a
   session or an identity of that number or LID: the mapping is then its
   own too). A mapping row is judged by the LID it holds and deleted by
   that LID alone (the table has one row a LID and one a number), so a row
   the own companion knows is never deleted on account of another LID the
   refused number once had. Only rows of the refused number are touched,
   and the gate refuses only a number that is not the connection's: the
   connection's own link stays as it was. The transaction is tried again
   while another writer holds the file (the connection's own client), for
   up to 60 s: a lock that passes never ends the connection;
3. then, in the background (30 s, waited for by a stop as Telegram's
   logout is), the companion is logged out at WhatsApp so that it leaves
   the other phone's linked devices: a whatsmeow client of the worker's own
   on the copy in memory, every store of it a no-op (nothing it would save
   reaches the store, so the deleted rows are not written back) and its
   pre-key store one that fails (whatsmeow then skips its pre-key upload
   after connecting, which would panic on the empty list a no-op store
   gives), connects
   through the connector's proxy setting (none), waits until WhatsApp takes
   the login and calls `Client.Logout` (the `remove-companion-device` IQ,
   the call the connector's `LogoutRemote` makes). The keys of the copy are
   zeroed after (not whatsmeow's own copies). The log says
   `refused_session_ended` (WhatsApp answered, or said the companion was
   already unpaired), `refused_session_not_ended` (no network, no answer,
   the stop came first: the companion stays linked on that phone until it
   is removed there; nothing of it is left in the store) or
   `refused_session_nothing_to_end` (no companion was stored).

When the delete fails (the log says `refused_login_not_dropped`) the
worker does not say `principal_mismatch` ("nothing kept"): the sign-in
fails as `other_account_kept`, the connection is ENDED with the same code
(the engine deletes the store), and no sign-in is begun on that store
meanwhile, as when a wipe of another account fails. The connector itself
stores nothing of the refused login (its `LoadUserLogin` never runs for
it); whatsmeow's in-memory LID cache keeps the other account's mapping
until the worker stops (it is not on disk).

Not covered: a sign-in that pairs and then fails before `NewLogin` (the
connector's 10 s wait for the server's 515 runs out, or the host cancels
in that window) leaves its companion stored and linked, whichever account
it is; the gate never sees it.

**A companion WhatsApp unlinked** (`state.go`): the connector reports it
as `BAD_CREDENTIALS` with a code of its own; the worker's
`backend.Hooks.EndedCodes` make the backend report those ENDED (the
contract's "the network ended the session"), not NEEDS_USER, with the
connector's code, which the host keeps as it is. The host restarts the
worker after neither; the connection waits for the user to link again (a
new pairing: whatsmeow has deleted the device from the store, and the
connector cleared the login's device number).

| Connector code | When (whatsmeow) | Reported |
|---|---|---|
| `wa-logged-out` | logged out with reason 401, at connect or as the stream error `device_removed`: removed under Linked devices on the phone, or dropped by WhatsApp (about 14 days without the phone online; the two look the same) | ENDED |
| `wa-main-device-gone` | reason 403 (WhatsApp Web: LOCKED): the phone's own WhatsApp logged out | ENDED |
| `wa-unknown-logout` | reason 406 (WhatsApp Web: BANNED), or any other logged-out reason | ENDED |
| `wa-not-logged-in` | a connect of a login whose device is gone (after one of the above) | ENDED |
| `wa-temporary-ban` | reason 402, with an expiry: the session is not ended | NEEDS_USER |
| `wa-phone-offline` | the phone unseen for 12 days, still connected | RECONNECTING |
| `wa-transient-disconnect`, `wa-keepalive-timeout` | the connection dropped; the connector reconnects | RECONNECTING |
| `wa-stream-replaced`, `wa-client-outdated`, `wa-stream-error`, `wa-connection-failed`, `wa-connect-failure-<n>` | another client took the device, the worker is too old, other failures | ERROR |

No new code was needed: the connector's codes tell a removed device
(`wa-logged-out`) from a banned account (`wa-unknown-logout`, or
`wa-temporary-ban`). chat_telegram and chat_fakenet set no `EndedCodes`:
their `BAD_CREDENTIALS` stays NEEDS_USER.

To confirm on a real account (not in the source): how long a pairing code
lasts; what WhatsApp does with a second message of an id it has seen (only
possible after a logout wiped the journal and the host sent the same key
again); that the logout of a refused companion removes it from the other
phone's linked devices (and how WhatsApp answers it when the server has
not yet taken the new companion's pre-keys); which reasons WhatsApp
actually sends for a removed, expired and banned companion.

## The Slack worker

`chat_slack` is the same backend with the unmodified mautrix-slack v0.2611.0
connector and the fork of slack-go it requires (`github.com/beeper/slackgo`,
go.mod's `replace`). One connection is one Slack user in one workspace: the
principal is the connector's login id, `<team id>-<user id>` in Slack's
upper case, and the account name the one the connector builds,
`<workspace name> - <email>`. Its engine id is `slack_session`, its worker id
`chat_slack` (registered in the engine, `src/providers/chat_slack.rs`); like
every chat worker it runs only where the host's allowlist names it.
`build.sh` builds, vets and tests it with the others.

**Settings.** None is required. `max_initial_conversations` (default 15, a
decimal count from 0 to 1000; anything else, the connector's own `-1`
included, refuses the start with `bad_settings`) is the connector's
`backfill.conversation_count`: at every connect the connector lists that many
of the user's conversations (`users.conversations`: public and private
channels, group DMs and DMs, in the order Slack gives them, not by recency;
pages of up to 200), and resyncs each: a channel gets its room whether or
not it has messages, a DM or a group DM only when Slack names a newest
message for it (`client.counts`). Each room made imports its newest
`max_initial_messages` (one `conversations.history` request); every chat
not listed gets its room when a message comes in it. 0 makes no room at a
connect. Because the list runs at every connect, a greater value takes
effect at the next start, with no resync of the worker's own. The history
settings are every worker's ("History a connection gets by itself"):
thread replies are not imported (the connector skips them in a channel's
history, and the framework's thread backfill is off), only thread roots.
`device_name` is not read: the connector has no device name. The connector's
other options are its defaults (5 members synced per channel, at its room's
creation), but for: custom emoji reactions are reported as `:shortcode:`,
not as images; a person's name has no ` (bot)` suffix (`PersonInfo.is_bot`
says it); every kind of conversation is bridged (not `dm_only`). The
workspace is a space, so a channel by the space rule; Slack channels, private
channels and group DMs are groups (no `IsChannel` hook).

**Sign-in flows.** The host chooses with `SignInBegin.flow_id`; one that
names none gets the first listed, `email`. Two of the connector's three
flows are offered, in its order; any other is `unknown_flow`, the
connector's `app` among them (a Slack app's bot and app-level tokens: its
account is the app's bot user, not the user's own, as chat_telegram leaves
out its `bot` flow):

| Flow | Steps (step id `fi.mau.slack.login.…`, contract kind) |
|---|---|
| `email` (default) | `enter_email` FIELDS (`email`, EMAIL); `email_captcha` WEB when Slack wants a CAPTCHA (one SCRIPT capture `captcha_token`, the connector's extract script); `enter_email_code` FIELDS (`code`, CODE: the six characters Slack mails); `select_workspace` FIELDS (`workspace`, CHOICE) when the address has several; `two_factor` FIELDS (`two_factor_code`, CODE) when the workspace asks; `workspace_browser` WEB for a workspace with single sign-on (as the token flow's step) |
| `token` | `enter_auth_token` WEB: captures `auth_token` (a browser session's `xoxc-` token) and `cookie_token` (its `d` cookie, `xoxd-`), both required. A host may submit the two values in `SignInSubmit` without a web view: the backend hands a WEB step's values to the connector as they come |

All end in `complete` (DONE). A wrong code, an expired code, a rate limit or
a workspace that cannot be signed into answers the same or an earlier step
again with new instructions, not a failure; a value that does not match a
field's pattern is `invalid_field`. A failure's code is the connector's,
lower case (`fi.mau.slack.invalid_auth`, `fi.mau.slack.token_revoked`,
`fi.mau.slack.rate_limited`, …), or the backend's. The tokens and the
cookie travel only in `SignInSubmit` and the encrypted store (the login's
metadata), never in a log line (the connector's own lines keep their level
only, `logx`).

**A logout** (the host's `Logout`) signs the session out at Slack
(`auth.signout`, the connector's own `LogoutRemote`) whichever flow made
it: a session captured in the sign-in's web view is the connection's own to
end, while a token pasted from the user's own browser (the token flow
without a web view, meant for our own tests) is that browser's session, which
is then signed out too. A refusal (below) treats the same token-flow session
the other way and never signs it out: the worker cannot tell a session the
web view made from one it reused or one pasted from a browser, and a
refused sign-in is not the user asking to sign anything out, while a
logout is. So a token-flow session the gate refused stays open at Slack
until the user signs it out there.

**A re-link that reaches another account** (another workspace, or another
user in the same one) fails as `principal_mismatch` and keeps nothing of
it: the connector stores nothing of a login before the framework's
`NewLogin`, where the gate refuses it. One step more than Telegram's: the
email flow catches every error of its last step, `NewLogin`'s refusal
included, and asks for the email again (`login-email.go`
`completeWorkspace`), so the host would never see the refusal. The worker
wraps the connector's login processes (`signin.go`): its
`backend.Hooks.DropRefused` (`refused.go`), which the gate runs
synchronously inside `NewLogin`, deletes nothing and notes the refusal on
the submission running, and that submission then fails with the gate's
error whatever the connector answered. The session the refused sign-in
opened is signed out at Slack in the background (`auth.signout`, the call of
the connector's own logout, 30 s, waited for by a stop as Telegram's is)
only when the email flow made it from its code, workspace or two-factor
step; never for the token flow (the user's own browser session: signing it
out would sign that browser out) or the email flow's single sign-on step
(the web view may have reused a session). The log says
`refused_session_ended` (Slack answered, or said the token no longer
works), `refused_session_not_ended`, or `refused_session_nothing_to_end`
for those two.

**A session Slack ended** (`state.go`): `slack-invalid-auth` (the token no
longer works: signed out, the password changed, an admin ended the
sessions), `slack-token-revoked` (signed out, by the user elsewhere or an
admin) and `slack-not-logged-in` (the next start after one of those: the
connector deletes the token) are ENDED with that code
(`backend.Hooks.EndedCodes`). `slack-account-inactive` (deactivated),
`slack-team-disabled` and `slack-user-removed-from-team` stay NEEDS_USER:
the connector keeps the token and asks for a restart, which connects again
if an admin undid it (whether Slack then takes the token is not in the
source). Neither is restarted by the host. Connection drops
(`slack-connection-error`, `slack-rtm-disconnected`, the socket-mode codes)
are RECONNECTING; the connector's unknown errors ERROR.

**Sending.** Nothing of the host's send key reaches Slack: the connector
posts with `chat.postMessage` (or uploads and shares a file) with no
`client_msg_id`, and slack-go retries only a 429 (Slack did not take the
request). So no `SendKeyFormat`: the key is the transaction id the
framework stores on the message's row. A certain success answers OK with
the message's id, `<team>-<channel>-<ts>` (Slack's ts in the connector's
message id, the id every message of that chat carries). An answer of Slack
naming an error is REJECTED (certain); a 5xx or no answer is UNKNOWN, never
sent again: the journal answers the same key `in_flight` (or
`in_flight_at_restart`) from then on, and a late result of the same call
(a deadline that passed, a file share still pending) comes as
`SendResolved`; a pending file share that resolves OK reports its message
(with its `send_key`) just before it, as a synchronous send does (backend
`send.go` `lateOK`; see "The Google Messages worker", sending). There is
nothing more to check: Slack has no lookup by a
key of the client's, so a message that did go out after an UNKNOWN arrives
as the account's own message (`from_me`) with no `send_key`, and the host
cannot tie it to the key.

**History a host asks for** ends on Slack's rate limit (429 after
slack-go's five tries) as `rate_limited` with Slack's `Retry-After`
(`Hooks.RateLimitWait`).

Its smoke test starts it with no settings, begins each flow to its first
step and never contacts Slack. To confirm against Slack (not in the
source): the order `users.conversations` lists conversations in; whether a
refused email sign-in's `auth.signout` removes the session from the user's
sessions; whether a deactivated account's token works again after
reactivation; Slack's rate limits on `conversations.history` for a browser
token.

## The Signal worker

`chat_signal` is the same backend with the unmodified mautrix-signal
connector: a linked device of the user's Signal account, like Signal
Desktop. Its connector id is `signal` (the engine's provider, `Init`'s
`connector`, the framework's bridge id), its worker and binary
`chat_signal` (registered in the engine, `src/providers/chat_signal.rs`);
like every chat worker it runs only where the host's allowlist names it.
`build.sh` builds, vets and tests it
with the others when it finds its native library (below), and otherwise
leaves it out of `build` and `test` with one line on stderr.

**Settings**, read strictly (an unknown key, or a bad value, refuses the
start with `bad_settings`): `device_name` (default `Lynx`, the name the phone
lists under Linked devices; at most 100 bytes of text with no control
character), and the backend's own: `max_initial_messages`,
`max_catchup_messages`, `spool_budget_bytes`, `log_level`,
`dev_plain_store`. A setting added to the backend for every worker must be
added to `knownSettings` in `main.go` too. The connector's other options
are fixed: no Note to Self avatar (the example's is an address on
maunium.net), no identity-change notices, names from the person's own
Signal profile, then the number, then the username (no address-book names
or avatars).

**Sign-in** is the connector's one flow, `qr`: a `sgnl://linkdevice?...`
address shown as a QR code, which the user scans in Signal under Settings,
Linked devices, "Link a new device" (Signal's apps do not take the address
any other way: opening it on the phone only points to that screen). Every
code is told `expires_in_s` 45: the connector replaces a code that was not
scanned 45 seconds after it was shown (its `qrRefreshInterval`, copied;
Signal's server closes the socket at 60), up to 20 times, and the sign-in
then fails as `fi.mau.bridge.login_timed_out`. Signal's refusals of the
link reach the host as the connector's codes: `fi.mau.signal.device_limit_reached`,
`fi.mau.signal.device_link_code_invalid`,
`fi.mau.signal.device_link_rate_limited`,
`fi.mau.signal.device_link_missing_capability`,
`fi.mau.signal.device_link_rejected`.

What it tells the backend of Signal (`backend.Hooks`, each checked against
the mautrix-signal v0.2611.0 source, cited in the code, and unit-tested
with no network):

- **The account's own person id** (`self.go`) is its ACI (a UUID), the id
  its messages carry, and the same string as the login id, which is the
  connection's principal. Signal keeps an ACI across a change of the
  account's phone number, so a number change is not a new principal. The
  phone number is the account's `tel:` identifier, not an id.
- **Send keys** become `lynx-` and 32 hex digits hashed from the key, never
  a number: the connector takes a numeric transaction id for the message's
  Signal timestamp, which is also its id, so a number made from the key
  (Telegram's mapping) would send each message with a made-up time. The
  message is stamped with the time of the send. As everywhere, the journal
  keeps a send from going out twice.
- **Identifiers**: `tel:+<digits>` and `signal:<username>`, as the connector
  writes them. Person ids are ACIs, or `pni:<uuid>` for someone known only
  by number.
- **No channels**: the connector bridges direct chats and groups only (an
  announcement-only group is a group); stories are not bridged.

**History.** Signal has no backfill: a linked device gets the account's
history once, at the link, as an archive the phone uploads when the user
agrees to transfer messages there. The connector makes a chat for every
conversation of the archive that has messages (no limit: there is no
setting like WhatsApp's `max_initial_conversations`), each opening with its
newest `max_initial_messages` from the archive, and then deletes that chat's
archive (the framework's backfill queue is off, so the connector does not
keep it for paging). Older messages are not kept anywhere. A `FetchHistory`
is answered OK with `has_more` false and no `HistoryBatch` (the connector
finds no archive and returns no page): there is nothing more to ask Signal
for. After the link, only new messages arrive.

**A re-link that reaches another account** (`refused.go`) fails as
`principal_mismatch`, with `backend.Hooks.DropRefused` as WhatsApp's:
signalmeow stores the linked device (its identity keys, password, backup
keys, pre-keys and own recipient row) during provisioning, before the
connector calls `NewLogin`. The drop reads the device's number and password
(no key) and deletes its `signalmeow_device` row in one transaction (tried
again while the connection's own client holds the file); every other
signalmeow table refers to that row with `ON DELETE CASCADE`, so the
account's keys, sessions, sender keys, groups, recipients, event buffer and
archive go with it. The connection's own rows are untouched, also those
that name the other person. Then, in the background (30 s), the device
removes itself from the other account's linked devices as the connector's
logout does: its own authenticated socket to Signal, `DELETE
/v1/devices/<its id>`, no client, nothing stored, logging nowhere (one
request and its answer, as signalmeow's own `confirmDevice` speaks the
socket, not through signalmeow's `SignalWebsocket`, whose stop has a data
race the race detector reports). The log says `refused_session_ended` only
on proof that the device is gone: Signal answered the delete with success,
or refused the device's credentials with 401 when its socket opened (it
knows no such device). A socket closed on the delete, or one that could not
be opened (no network, 5xx, 429), is not proof: the socket is opened again
2 s later, and either gets that 401 or opens, and then the delete is asked
again. Anything else is
`refused_session_not_ended` (no network, another answer, a 403 at the
opening, whose meaning signalmeow's source does not give, or the 30 s ran
out: the device stays linked on that phone until it is removed there, as
chat_whatsapp's unpair does; its credentials went with the store's rows, so
nothing tries again). `refused_session_nothing_to_end`: no device was
stored. When the local delete fails, the device's credentials were read
first and its unlink runs all the same; the rows stay, the log says
`refused_login_not_dropped`, and the connection is ENDED
`other_account_kept` (the host deletes the store). When even they could not
be read, the log says `refused_session_not_ended`, never
`nothing_to_end`.

A device stored by a sign-in that never became a login (the host cancelled,
or the connector replaced the QR code, just as the phone confirmed the
link; or the worker stopped in between; or the gate could not read the
store to judge the login, which then fails as `store_error` with nothing
dropped) is deleted at the next start, and unlinked at Signal
(`backend.Hooks.PrepareLoginsEnds`): every `signalmeow_device` row whose ACI
is not the id of a stored `user_login` row goes, with its rows, and each
that had credentials is then unlinked in the background as a refused
device is (30 s, logged `refused_session_ended` or
`refused_session_not_ended`). The keep-set
is read from the stored rows in the sweep's own transaction, never from the
logins the framework loaded: bridgev2 leaves out, with no error, a login
whose load failed (a transient store read), and that login's device must
stay. When the stored logins cannot be read, nothing is deleted (the log
says `prepare_logins_failed`) and the sweep runs at the next start. It runs
before the worker serves any command, so no sign-in is in progress, and the
store holds one connection's devices only. One whose unlink failed stays
among that phone's linked devices until it is removed there (its
credentials went with its rows).

**A device Signal no longer takes** (`state.go`) is ENDED, not NEEDS_USER,
with the connector's code:

| Connector code | When | Reported |
|---|---|---|
| `signal-logged-out` | Signal refused the device's credentials (403 or 401 on its socket): removed under Linked devices on the phone, or dropped by Signal; also at every start after that (the connector cleared the device's password) | ENDED |
| `signal-not-logged-in` | a login whose device is gone from the store | ENDED |
| `signal-disconnected`, `signal-connection-error`, `signal-server-error`, `signal-rate-limited`, `signal-connect-failed` | the connection dropped or could not be made; the connector retries | RECONNECTING |
| `signal-idle-primary-device` | Signal has not seen the phone for long and warns it will unlink the device; it still works | RECONNECTING |
| `signal-connection-failed` | the socket failed over 500 times without staying up a minute; the connector stops | ERROR |

Linking again is a new device; the same account's new device replaces the
stored one.

**The native library.** The connector's `pkg/libsignalgo` is cgo over
libsignal's FFI (`#cgo LDFLAGS: -lsignal_ffi -ldl -lm -lz -lstdc++`; the
header ships in the module): chat_signal links `libsignal_ffi.a`, Signal's
Rust library with BoringSSL compiled in, statically, beside SQLite3
Multiple Ciphers. The version is pinned in `libsignal.pin` (tag v0.106.1,
commit `904c8dc8216073b078a5f2f5090e3da8382e6a92`), which must equal the
connector's `signalversion.Version`: `build.sh` refuses to go on when they
differ, and the link itself fails on another version's library
(`libsignalgo/version.go` calls `signal_ffi_version_0_106_1`). The library
is not committed. `build.sh` takes it from:

- `LYNX_LIBSIGNAL_DIR`: a directory with `libsignal_ffi.a` (and no shared
  `libsignal_ffi`) and its `STAMP`, as `./build.sh libsignal` leaves them
  in `go/.libsignal/lib/`; checked as the cache's is: chat_signal is left
  out (an error with `LYNX_REQUIRE_SIGNAL=1`) unless the `STAMP`'s first
  line is `commit <the pinned commit>`, its second `version <the pinned
  version>` and its last `libsignal_ffi.a <the library's SHA-256>`. A
  library built elsewhere from the pinned commit needs that file written
  for it (the lines between are free: `./build.sh libsignal` puts the
  rustc version there). Like the cache's, it catches a stale or mixed-up
  library, not tampering; or
- `go/.libsignal/lib/` (`LIBSIGNAL_CACHE`), where `./build.sh libsignal`
  puts it: it fetches the pinned commit (shallow, by hash) into
  `go/.libsignal/src/` (or verifies `LIBSIGNAL_SRC`, a checkout you have:
  the pinned commit, no local changes) and runs mautrix-signal's own
  `RUSTFLAGS="-Ctarget-feature=-crt-static" RUSTC_WRAPPER=""
  cargo build --locked -p libsignal-ffi --profile=release`, then stamps the
  library with the commit, version, rustc and its SHA-256. It needs git,
  Rust (the checkout's `rust-toolchain` names 1.98.1; `RUSTUP_TOOLCHAIN`
  overrides it: 1.96.0 built it on the build host, libsignal's minimum is
  1.93.1), cmake, clang, a C++ compiler and protoc with protobuf's
  well-known `.proto` files (Debian `protobuf-compiler` and
  `libprotobuf-dev`; `PROTOC` names another protoc). About 12 minutes at
  `-j 3` on the build host. **Not run end to end by `build.sh` yet**, and
  no test runs its cargo step: the library it was checked against was
  built by hand with the same command (2026-11-26), and given through
  `LYNX_LIBSIGNAL_DIR` it now needs the `STAMP` above written for it.

Without the library, `build`, `plain`, `test` and `env` go on without
chat_signal and print `chat_signal skipped: <why>` on stderr; `vet` checks
it all the same (vet compiles cgo without linking). `LYNX_REQUIRE_SIGNAL=1`
turns the skip into an error, for a build that must ship it. Linking needs
zlib's linker name (`-lz`; Debian `zlib1g-dev`): where only the runtime
`libz.so.1` exists, `build.sh` links through a `libz.so` link in
`go/.libsignal/zlib/`; the binary does not keep libz among its needed
libraries. At run time chat_signal needs glibc, `libstdc++.so.6` and
`libgcc_s.so.1`.

For an image builder that should ship chat_signal: the packages above
(Rust toolchain, cmake, clang, g++, protobuf-compiler, libprotobuf-dev,
zlib1g-dev, git), `./build.sh libsignal` (or a cached `go/.libsignal/` from
an earlier stage keyed on `libsignal.pin`), then `LYNX_REQUIRE_SIGNAL=1
./build.sh build`; the runtime image needs `libstdc++6` and `libgcc-s1`
(and a glibc at least as new as the build's). On darwin/arm64 nothing of
this has been run (see `build.sh darwin`).

Measured (linux/amd64, 2026-11-26, shared host): `chat_signal` from
`./build.sh build` is 59,426,400 B stripped (`chat_whatsapp` 30,125,304 B);
`libsignal_ffi.a` 213,817,352 B; idle VmRSS after start 33,960 KiB (smoke
test, 1 run); needed libraries `libm.so.6 libstdc++.so.6 libgcc_s.so.1
libc.so.6`, highest symbol version GLIBC_2.39. libsignal_ffi.a and
libsqlite3.a define no global symbol in common (`nm`), and the smoke and
unit tests run both in one binary.

To confirm on a real account (not in the source): that a refused device's
`DELETE /v1/devices/<id>` removes it from the other phone's list; how
Signal answers the link of a device past the account's limit; what the
phone does with the transfer archive when the user declines it (the
connector waits for it, then makes no chats); how the account's own
messages sent from the phone arrive (as sync messages, `from_me`).

## The Instagram and Messenger workers

`chat_instagram` and `chat_messenger` are the same backend with the
unmodified mautrix-meta v0.2611.0, the module's two connectors: Instagram's
(`pkg/igconnector`, with instameow under it, the connector of upstream's
`cmd/mautrix-instagram`) and Facebook Messenger's (`pkg/connector`, with
messagix under it and whatsmeow for the end-to-end encrypted chats, the
connector of upstream's `cmd/mautrix-meta`). The module's Messenger connector
has no Instagram mode any more: it offers no Instagram flow and refuses an
Instagram login (`meta-ig-not-supported`), so each worker links its own
connector and only that one. What they share is in `chatworker/metaworker`:
the strict settings, the hooks below and the main program; both link
Beeper's fork of `imroc/req` (go.mod's `replace`), the HTTP client that
gives their requests a browser's TLS and HTTP/2 fingerprints. Their provider
ids are `instagram` and `messenger`, their worker ids `chat_instagram` and
`chat_messenger`; like every chat worker they run only where the host's
allowlist names them. `build.sh` builds, vets and tests them with the
others.

One connection is one account. The principal is the connector's login id,
the account's FBID in decimal: for Messenger the Facebook account's user id
(the `c_user` cookie), for Instagram the messaging FBID Instagram's web page
gives the account (`IGUserEIMU`), not its Instagram user id (`ds_user_id`).
It is also the account's own person id (`metaworker.SelfPersonID`, the
framework's default made explicit): both connectors give the account's own
messages that sender, and the encrypted Messenger chats the same FBID as the
user of the msgr JID. People are their FBIDs too. Instagram gives a person
the identifier `instagram:<username>`, and the account its own from its
username; Messenger gives none (its scheme is the connector's network id,
`facebook`). A StartChat takes a person id (an FBID), not a username, in
both.

**Settings.** None is required, and they are read strictly (an unknown key
refuses the start with `bad_settings`, as chat_signal's): `max_inbox_pages`
(default 1, a decimal count from 1 to 20) is how many pages of the chat
list, newest activity first and of Meta's own size, the connector syncs
after a sign-in, the first included. Each chat listed becomes a room and
imports its newest `max_initial_messages`; every chat not listed gets its
room when a message comes in it. The connectors' own defaults are -1 (every
chat of the account) for Messenger and 0 for Instagram; the setting is their
`thread_backfill.batch_count`, translated (Messenger counts the pages after
the one Meta pushes at connect, for its plain chats and as many again for
its encrypted ones; Instagram counts every page), run once a login, two
seconds between pages. `device_name` is accepted and not read (the
connectors have no device name); the backend's own keys are the backend's.
The rest is the connectors' example config, but for: no proxy and no Tor;
no Marketplace space (Messenger); no typing connections (Instagram: two
more sockets a login for something the backend drops); and no restore of
Messenger's encrypted history (`e2ee_backfill` off: it would ask the
user's secure storage PIN after the cookies and upload every message sent
to Meta's backup). A person is named by their name, else their username,
with no suffix.

What is synced at sign-in: the first page of the inbox (more with
`max_inbox_pages`), a message request among it as a message-request chat
(Messenger's pending folder; Instagram's pending or spam threads), no chat
of Messenger's spam folder, and for Messenger the encrypted chats Meta
pushes at connect. Messenger's encrypted chats need a second connection, a whatsmeow
device the connector registers for the account at its first connect and
keeps in the store (the `whatsmeow_*` tables, beside the login's
`wa_device_id`); they have no history (their `CanBackfill` is false without
the PIN restore) and begin at the sign-in. Instagram keeps its connection
state (sequence id, reconnection state) in the connector's own tables. A
re-link of the same account replaces the login's metadata (the connector's
`CopyFrom`): Messenger then registers a new encrypted device at the next
connect, and the old device's rows stay in the store (upstream's way).

**Sign-in flows.** Meta signs in on the web: each flow is one WEB step,
`fi.mau.meta.cookies` (the framework's cookies step), that opens the site in
a web view and captures its session cookies (source COOKIE, of the site's
domain), then `fi.mau.meta.complete` (DONE). The host chooses with
`SignInBegin.flow_id`; one that names none gets the first listed.

| Worker | Flow | Page | User agent | Cookies (required; optional) | Done when the URL matches |
|---|---|---|---|---|---|
| chat_messenger | `facebook` (default) | `https://www.facebook.com/` | the connector's desktop Chrome on Linux (`useragent.UserAgent`), the one it then sends Meta | `xs`, `c_user`, `datr` | `^https://www\.facebook\.com/(?:messages/(?:e2ee/)?(?:t/[0-9]+/?)?)?(?:\?.*)?$` |
| chat_messenger | `messenger` | `https://www.messenger.com/?no_redirect=true` | the same | `xs`, `c_user`, `datr` | `^https://www\.messenger\.com/(?:e2ee/)?(?:t/[0-9]+/?)?(?:\?.*)?$` |
| chat_instagram | `instagram` (the only one) | `https://www.instagram.com/accounts/login/` | none (the web view keeps its own) | `sessionid`, `csrftoken`, `ds_user_id`; `rur`, `shbid`, `shbts`, `mid`, `ig_did` | `^https://www\.instagram\.com/(?:direct/(?:inbox/\|t/[0-9]+/)?\|accounts/onetap/)?(?:\?.*)?$` |

No extract script: every capture is a cookie, which the web view reads
from its cookie store (some of them are HTTP-only at Meta). Both
Messenger pages match their own pattern before the user signs in: a host
closes the web view only when the URL matches and the
required cookies are there (the framework's rule for the pattern). A
submission without a required cookie fails as `fi.mau.meta_missing_cookies`
before anything is asked of Meta. Not offered, any begin of them
`unknown_flow`: Messenger's `messenger-lite` and `messenger-lite-android`
and Instagram's `android` and `instagram-password`, which take the user's
password into the worker for Meta's mobile or web login API. A failure's
code is the connector's, lower case: `fi.mau.meta_checkpoint_error`,
`fi.mau.meta_challenge_error`, `fi.mau.meta_consent_error` (a checkpoint, a
challenge or a consent to clear in the app or site first),
`fi.mau.meta_token_error` (the cookies were logged out at once),
`fi.mau.meta_rate_limited`, `fi.mau.meta_account_suspended` (Instagram),
`fi.mau.meta_inbox_load_failed`, `m_unknown`, or the backend's. The cookies
travel only in `SignInSubmit` and the encrypted store (the login's
metadata), never in a log line.

**A logout** (the host's `Logout`) ends nothing at Meta: neither connector
has a remote logout (both `LogoutRemote` disconnect and forget the cookies;
Messenger also deletes its encrypted device from the store, not from
Meta). The web session the user signed into stays signed in until the user
ends it in Meta's settings ("Where you're logged in") or it expires.

**A re-link that reaches another account** fails as `principal_mismatch`
and keeps nothing of it: both cookie flows only ask Meta (the messages page,
the Instagram index) before the framework's `NewLogin`, where the gate
refuses the login, store nothing before it, and return its error wrapped
with `%w`, so the backend sees the refusal as it is. No `DropRefused` (there
is nothing to delete) and no end at Meta: the session is the user's own web
session, and signing it out would sign that browser out.
`metaworker.EndRefused` says so (`refused_session_nothing_to_end`).

**A session Meta ended** (`state.go` of each): ENDED with the connector's
code for `meta-cookie-removed`, `meta-redirected-to-login` and
`meta-user-id-is-zero` (Meta answered that the session is no longer valid:
logged out, the password changed, the session removed or expired; the
connector deletes the cookies), `meta-not-logged-in` (the next start after
one of those) and `dgw-connection-unauthorized` (the realtime socket refused
the session), and for Instagram `ig-connect-missing-user-id`. NEEDS_USER, the
user clears it in Meta's own app or site and starts the connection again
(the connector keeps the cookies; Instagram's checks again by itself, at
growing intervals): `fb-checkpoint-required` (both), `fb-challenge-required`,
`fb-consent-required`, `fb-account-suspended` (Messenger),
`ig-challenge-required`, `ig-consent-required`, `ig-account-suspended`,
`ig-account-restricted` (Instagram), and `meta-lserror-1357053` (an error of
Meta's the connectors take as bad credentials, its meaning not in the
source). Connection drops are RECONNECTING (`meta-transient-disconnect`,
`ig-dgw-connection-error`, `wa-transient-disconnect`, connect errors while
the connector retries); its unknown errors ERROR. A failure of Messenger's
encrypted connection alone is reported CONNECTED with its `wa-…` code: the
other chats still work.

**Sending.** Both connectors give each message the client's id Meta's own
clients give it, an "offline threading id", made of the time of the send;
they take a transaction id that is a positive decimal number for it. So the
send key is mapped as chat_signal's (`metaworker.SendKey`: `lynx-` and 32
hex digits of its SHA-256), never a number, and the connector makes a fresh
id at each send. Nothing of the key reaches Meta but one thing: Messenger
sends a reaction (or its removal) in an encrypted chat with the transaction
id as its message id. A certain success answers OK with the network's
message id as the chat's other messages carry it (`fb:<Meta's message
id>`, and `wa:<chat>:<sender>:<id>` in Messenger's encrypted chats). A send the
connector cannot confirm (no answer, a timeout, a 5xx or a rate limit after
its own retries, which may have gone through) is UNKNOWN, never sent again:
the journal answers the same key `in_flight` from then on. Neither connector
ties an incoming message to a send by its offline threading id, so a message
that did go out after an UNKNOWN arrives as the account's own message
(`from_me`) with no `send_key`.

**History a host asks for** ends on Meta's 429 (after messagix's own
retries) as `rate_limited` with no wait (`Hooks.RateLimitWait`): Meta names
none the connectors read.

Their smoke tests start each with no settings, begin each flow to its WEB
step, fail a partial capture and never contact Meta. To confirm on a real
account (not in the source): how often Meta checkpoints a session first
seen from a server (the worker talks to Meta from the host, not from the
phone that signed in; the Messenger flow's user agent is a desktop Linux
Chrome, Instagram's the web view's own); whether messenger.com still signs
in (`facebook` is the default); how many chats a page of the inbox holds; whether Meta's servers
take a reaction in an encrypted chat whose message id is not a number; and
what the account's "Where you're logged in" and its encrypted chats' device
list show after a sign-in (Messenger registers a device for the encrypted
chats, which the account's contacts may be told of).

## The Google Messages worker

`chat_gmessages` is the same backend with the unmodified mautrix-gmessages
v0.2611.0 connector (and its `libgm`): it pairs with the user's Android
phone as Google's Messages for web does, and sends and receives the phone's
SMS, MMS and RCS through it. Its connector id is `gmessages` (the engine's
provider, `Init`'s `connector`, the framework's bridge id), its worker and
binary `chat_gmessages` (registered in the engine,
`src/providers/chat_gmessages.rs`); like every chat worker it runs only
where the host's allowlist names it. `build.sh` builds, vets and tests it
with the others.

Nothing reaches the network but through the phone: Google relays every
request to it, so **the phone must stay on and online, with Google
Messages as its default SMS app** (a send the phone refuses for that is
REJECTED), and with battery optimisation for Messages off. A phone that
does not answer for a while is RECONNECTING (`gm-phone-not-responding`),
never parked; low battery, mobile data and the phone's data saver are not
reported at all. The connector speaks Google's undocumented web API (its
Google account pairing even sends Google's notice that such use is not
authorised by Google's terms), and the phone shows the pairing under
Device pairing as `device_name`, a tablet: Google lets one web, two tablet
and one PWA session be connected at once, so it stands beside the user's
own Messages for web.

**Settings**, read strictly (an unknown key, or a bad value, refuses the
start with `bad_settings`): `device_name` (default `Lynx`; at most 100
bytes of text with no control character), `max_initial_conversations`
(default 25, the connector's own; a decimal count from 1 to 100), and the
backend's own: `max_initial_messages`, `max_catchup_messages`,
`spool_budget_bytes`, `log_level`, `dev_plain_store`. A setting added to the
backend for every worker must be added to `knownSettings` in `main.go` too.
`max_initial_conversations` is the connector's `initial_chat_sync_count`,
and more than an initial count: at every connect (once the phone says the
new session is active) the connector lists that many of the inbox's newest
conversations and resyncs each, so a conversation with no room gets one
with its newest `max_initial_messages`, and one with a room that missed
messages imports up to `max_catchup_messages`; when data comes again after a
quiet spell it lists them again and resyncs them only if one has a gap. A
conversation outside the list gets its room when a message comes in it.
That is all a pairing syncs: the inbox's conversations only, no contact
list, and no older history (only `FetchHistory`).
The connector's other options are its defaults (a ping of the phone every
20 minutes, a state after 15 minutes without an answer), but for: the id
prefix of a login's people, chats and messages is a hash of the login id
(`deterministic_id_prefix`, so that nothing is written before the gate,
below); a session another Messages for web took over waits for the user
instead of taking itself back every time (`aggressive_reconnect` off, the
default written out).

**Sign-in flows.** The host chooses with `SignInBegin.flow_id`; one that
names none gets the first listed, `google`. Google has discontinued QR
pairing: the connector lists only `google`, its `qr` flow is commented out
of its list, and its CHANGELOG for v26.05 says "Disabled support for QR
login as Google has discontinued it". Its `CreateLogin` still makes the QR
process, and the worker lists `qr` again, second (`signin.go`,
`gmNetwork`), only in case a phone still offers it; where it does not, the
QR flow fails at its first step or times out. Any other id is
`unknown_flow`.

| Flow | Steps (step id `fi.mau.gmessages.…`, contract kind) |
|---|---|
| `google` (default) | `google_account` WEB at `https://accounts.google.com/AccountChooser?continue=https://messages.google.com/web/config`, capturing the cookies `SID`, `HSID`, `SSID`, `APISID`, `SAPISID` (`.google.com`) and `OSID` (`messages.google.com`), all required, and `__Secure-1PSIDTS` (optional); then `emoji` SHOW_AND_WAIT (EMOJI, with an SVG image address), the emoji the user taps on the phone; `expires_in_s` 0 (Google times it out, after a time the source does not give: `fi.mau.gmessages.pair_timeout`) |
| `qr` (discontinued by Google) | `qr` SHOW_AND_WAIT (QR: an address of `support.google.com/messages/?p=web_computer#?c=` and the pairing key), scanned in the phone's Google Messages, Device pairing, QR code scanner. Each code is told `expires_in_s` 30 (the connector's `QRExpiryTime`, read from it): the connector replaces it after 30 s, six codes in all, then fails as `fi.mau.gmessages.pair_qr_timeout`. Beginning the flow already asks Google for a pairing key |

Both end in `complete` (DONE). On a connection whose stored login was paired
with a Google account, a sign-in of `google` may begin at `emoji` (the
connector pairs again with the stored cookies when the pairing is gone), or
its cookies re-sign the existing pairing in with no emoji (when only the
cookies expired). Failures carry the connector's codes, lower case:
`fi.mau.gmessages.pair_no_devices` (account pairing not enabled in the
app), `.pair_init_timeout` (the phone did not answer), `.pair_phone_inactive`,
`.pair_phone_not_found` (a re-pair whose phone is gone: pair as a new
connection), `.pair_incorrect_emoji`, `.pair_cancelled`, `.pair_timeout`,
`.pair_qr_timeout`, `.account_cant_pair`, or `m_unknown`. The cookies travel
only in `SignInSubmit` and the encrypted store, never in a log line.

**The account and its people** (`self.go`). The principal is the
connector's login id: for a QR pairing the phone's id, for a Google account
pairing `<the account's address>/<the phone's id at Google>`; so the
connection is tied to the flow it was first paired with and to that phone
(a new phone, or the other flow, is another account to the gate). Every id
of a login is `<id prefix>.<the phone's id>`. The account's own person id
is `<id prefix>.1`, the phone's participant "1", which the connector always
takes for the user. A phone's own participants are also one per SIM: with
two SIMs (or one, if the phone names its own messages by the SIM), the
account's own messages may carry a sender id that is not the own id; they
are `from_me` all the same. A person's identifier is `tel:` and the number
as the phone gives it (usually E.164; a short code or an alphanumeric
sender as it is); the account has none. `StartChat` takes a phone number or
a person id.

**A re-pair that reaches another phone or account** fails as
`principal_mismatch` and keeps nothing of it: both flows pair in memory,
and the connector's one write before the framework's `NewLogin` (a
`gmessages_login_prefix` row naming the login id) is not made with the
hashed prefix, so the gate refuses the login before anything of it is
stored (`refused.go`, `Hooks.EndRefused`, no `DropRefused`). The pairing
the refused sign-in made is then undone at Google in the background (30
s, waited for by a stop): `RevokeRelayPairing` for a QR pairing,
`UNPAIR_GAIA_PAIRING` for a Google account one, chosen by the session's
kind (`unpairWith`), on a client of the worker's own that is never
connected. Not libgm's `Client.Unpair`, which the connector's own logout
calls: it sends `UNPAIR_GAIA_PAIRING` (with the zero pairing id) for a QR
pairing too, as libgm's `HasCookies` is true for every session that is not
a Google account's. The log says `refused_session_ended`,
`refused_session_not_ended` or `refused_session_nothing_to_end`. One step
more: a `google` sign-in on a connection with a Google account login first
tries the new cookies on that login itself and, when they are another
account's, leaves that login with no cookies; the worker puts them back
(`googleLogin`), so a refused sign-in does not sign the connection's own
pairing out of Google. **A logout** unpairs at Google, which takes the
pairing off the phone: a Google account pairing by the connector's own
`LogoutRemote` (`UNPAIR_GAIA_PAIRING` with its pairing id); a QR pairing,
which that logout would not revoke (libgm's `Unpair`, above), first by the
worker (`Hooks.BeforeLogout`, `logoutUnpair`: the live client is
disconnected first, so that the phone's answer to the revoke cannot reach
the connector's handler while its logout clears the session, then
`RevokeRelayPairing` on the connection's own session while it is still
whole, waited for at most 10 s;
its result is only logged, `logout_prepared` or `logout_prepare_failed`,
and the local wipe goes on either way), then by that logout as it is. A QR
pairing whose revoke failed stays listed on the phone until the user
removes it there, with a token nobody holds once the store is wiped.

**State** (`state.go`). ENDED, the pairing gone: `gm-unpaired` (revoked on
the phone, or Google ended the account pairing), `gm-unpaired-other-phone`
(the same while another phone uses Messages with that account, which can
unpair this one daily), `gm-unpaired-entity-not-found`, and at the next
start `gm-not-logged-in` and `gm-not-logged-in-can-repair`. NEEDS_USER, the
pairing kept: `gm-logged-out-invalid-credentials`, `-401-polling`,
`-workspace-session` (Google signs a Workspace account out of the web every
14 days by default), `-no-email-in-config` and at the next start
`gm-not-logged-in-can-reauth` (sign in to Google again: the `google` flow
then needs no emoji); `gm-switched-to-google-login` (the phone moved to
Google account pairing, which a QR pairing does not follow);
`gm-browser-inactive`, `-timeout`, `-inactivity` (another web session took
the phone's active session: a start of the worker, or a sign-in, takes it
back). RECONNECTING: `gm-phone-not-responding`, `gm-listen-error`,
`gm-ping-failed`. ERROR: `gm-listen-fatal-error`, `gm-connection-failed`.
None is restarted by the host after ENDED or NEEDS_USER.

**Sending** is asynchronous. The send key becomes the message's temporary
id (`Hooks.SendKeyFormat`, `send.go`): a version 4 UUID, the form Google
Messages' own app makes, hashed from the key (a key already of that form is
kept). The connector first waits for the phone to take the request: a phone
that does not answer is UNKNOWN `m.event_too_old` when libgm stops waiting
(Google holds the request, and the phone may still send it); a refusal of
the phone is REJECTED (`m.event_not_handled`) when it is certain (Messages
not the default SMS app, the phone moved to another Google account) and
UNKNOWN `m.event_not_handled` when the
phone called it temporary (the connector itself asks again 3, 8 and 20 s
later, with the same temporary id, before it gives up). Once the phone took
it, the send is answered UNKNOWN `pending`, and the result comes as
`SendResolved`: the phone echoes the temporary id on the message, which is
how the framework ties the message to its send, and the result is OK with
the message's id (`<id prefix>.<message id>`, the id every message of the
chat carries) once the phone reports it sent, REJECTED when it reports a
failure, UNKNOWN `m.event_too_old` when it has not echoed the message after
a minute or the send is three minutes old (the connector's timeouts, held
back while the phone throttles its pushes). Nothing is sent again by the
backend. A send resolved OK this way reports the account's own message as
a synchronous success does, once, just before its `SendResolved`: one
`Message` of the stored row, `from_me`, with its `send_key`, the message's
id and the chat's id (backend `send.go`: the framework makes no event of a
phone's echo it matched to a pending send, so `resolveByStatus` reports it
through the command's `lateOK`, `echoSend`; armed only when the connector
itself answered the send pending). None is reported for a send resolved
REJECTED or UNKNOWN, but an UNKNOWN end is not the last word on its
message: a send answered UNKNOWN (a phone that did not answer, whose send
Google holds and the connector keeps pending in the framework) or resolved
UNKNOWN (the minute's or the three minutes' timeout) is kept for 24 hours
(`send.go` `keepForLateEchoLocked`, `lateEchoLimit`, in memory: not past a
restart or a logout). When the phone sends it within that time, the
framework saves its row and sends a success status with no event of its
own; the backend then reports the message once, as above, after the
send's answer, and marks the key's journal row sent with the message's id
(the same key is answered OK from then on). No second `SendResolved`: the
command keeps its UNKNOWN answer. After 24 hours, a restart or a logout,
the phone's later success reaches the host neither as a `SendResolved` nor
as a message (the framework has saved the row, so no later sync reports it
either); a later failure is never reported. Nor does a send whose handler
the framework put in the background (it took more than five minutes,
`ErrHandlerBackgrounded`) and that the connector then answered pending: its
late success is a `SendResolved` OK with no message, since the backend
cannot tell that answer from a synchronous connector's late success, whose
message it does not report.

Its smoke test starts it with no settings, begins the `google` flow to its
first step and never contacts Google (the `qr` flow's first step does). To
confirm on a real phone (not in the source): whether Google still pairs by
QR; which participant id the phone gives the account's own messages, with
one SIM and with two; whether the refused pairing's unpair, sent with no
connected session, removes it from the other phone; how long Google leaves
the emoji to be tapped.

## The Google Voice worker

`chat_gvoice` is the same backend with the unmodified mautrix-gvoice
v0.2611.0 connector (its `libgv` under it): the text messages of the
Google Voice number of one Google account, read and sent as Google's web
app does, with the cookies of a Google web session. Its connector id is
`gvoice` (the engine's provider, `Init`'s `connector`, the framework's
bridge id), its worker and binary `chat_gvoice`; like every chat worker it
runs only where the host's allowlist names it. `build.sh` builds, vets and
tests it with the others.

**What it runs.** Go code alone: every request to Google is HTTPS from Go,
authorized by the cookies and a `SAPISIDHASH` header computed in Go
(`libgv/request.go`). One optional outside piece: **Electron**, with which
the connector signs each text it sends (the "tracking data" Google's web
app computes by running a program Google serves; the connector loads it in
a hidden Electron window from `https://voice.google.com/about`,
`connector/electron.go`, `electron.mjs`). The connector looks for a program
named `electron` on `PATH` at each connect; without one it sends every text
with the placeholder `!`, as its own Docker image does (it ships no
Electron). Whether Google takes unsigned texts as readily is not in the
source (the connector names Electron's status in a 429's error, as if it
may not). The worker decides what the connector finds: the setting
`electron_path` names the program (an absolute path ending in `/electron`;
the worker's `PATH` becomes that file's directory alone); without it the
worker's `PATH` is empty and no Electron ever runs, whatever the host has.
A host that names one that is missing or not executable cannot start the
worker (`electron_not_found`). What Electron itself needs is not checked:
a version that runs an ES module as its main script, its shared libraries,
the network to voice.google.com, and a display: on Linux an X server or
Wayland (the connector passes no headless flag), so a server runs it under
Xvfb through a wrapper script named `electron`, which names its interpreter
and every program by absolute path (`PATH` is that one directory). On macOS
the bundle's binary is `Electron.app/Contents/MacOS/Electron`: a wrapper
named `electron` that execs it. None of it has been run.

**Settings**, read strictly (an unknown key, or a bad value, refuses the
start with `bad_settings`): `max_initial_conversations` (default 20, a
decimal count from 0 to 1000; the connector's own `-1` refused) is the
connector's `initial_thread_sync_count`: at every connect the connector
lists the newest page of the account's threads (20: texts, calls and
voicemails, the `ALL_THREADS` folder) and makes a room for each that has
none, and once per login pages on through older threads until it has seen
this many; so 0 to 20 all make about 20 rooms. Each room made imports its
newest `max_initial_messages`; every thread not listed gets its room when a
message comes in it. `electron_path` (above) is the host's alone, never a
connection's own: it names a program the worker runs. The backend's own:
`max_initial_messages`, `max_catchup_messages`, `spool_budget_bytes`,
`log_level`, `dev_plain_store`. `device_name` is accepted and not read (the
host may give it every worker; the connector has no device name). The
display name template is the connector's example (the
person's name in the account's Google contacts, else the number).

**What arrives.** Texts (SMS and MMS: images, audio, video and files,
downloaded from Google), group texts, and the account's calls and
voicemails as text messages of the thread with that person (the connector
merges a 1:1 number's call and voicemail threads into its text thread):
"Voice call • 2 mins", "Missed voice call", "Voicemail: <Google's
transcript>" (the voicemail's audio is not attached). They are Google
Voice's own thread items, with Google's ids, and are passed on as they
come. An item the connector does not know arrives as a notice ("Unknown
message type, please view it on the Google Voice app"). Names, avatars,
other numbers and emails of a person come from the account's Google
contacts (the connector loads up to 500 at each connect, and looks up each
new number). Sending takes text and images (PNG, JPEG, BMP, TIFF, GIF, at
most 2 MiB; 160 characters is the connector's advertised text limit).

**Sign-in** is the connector's one flow, `cookies`: one WEB step
`fi.mau.gvoice.cookies` at `https://voice.google.com/signup`, with eight
COOKIE captures, `SID`, `HSID`, `SSID`, `APISID`, `SAPISID` (domain
`.google.com`, required), `__Secure-1PSIDTS` (`.google.com`), `OSID`
(`voice.google.com`) and `COMPASS` (`clients6.google.com`), no user agent,
no extract script and no URL to wait for: the app submits once the required
cookies are there and the user is done. The values go to Google as cookies
as they are (a host may also submit them without a web view). The
submission asks Google for the account and ends in
`fi.mau.gvoice.complete` (DONE), or fails as
`m_forbidden` (Google refused the cookies), `m_limit_exceeded` (rate
limited), `m_unknown` (another answer, or no answer), or
`fi.mau.gvoice.no_number` (the Google account has no Voice number, or, a
refusal of the worker's own, a Voice line but no primary number: the
connector's login id would then be the same for every such account), or
one of the backend's (`principal_mismatch`, `submit_failed`, ...). The
cookies are a full Google session (Gmail, Drive, every service of the
account, not Voice alone): they travel only in `SignInSubmit` and the
encrypted store (the login's metadata, with the changed cookies Google
sends saved as they come), never in a log line.

**The account.** The principal is the connector's login id,
`@owner:lynx.invalid|<the account's primary Voice number>` (E.164 as
Google gives it; the connector cannot read the Google account's address),
and the account name that number. The account's own person id
(`self.go`) is the connector's user id of its own number,
`<prefix>.<number>`, the form of every person id of the login: `<prefix>`
is a number the connector's own table (`gvoice_login_prefix`) gives each
login, so person ids are per login; a re-link of the same account keeps
its prefix. Identifiers are `tel:+<digits>` (and `mailto:` from a contact);
`StartChat` takes `tel:` or a bare international number (or, for a North
American account, ten digits), or a person id of the login.

**A re-link that reaches another account** (another number) fails as
`principal_mismatch` and keeps nothing of it (`refused.go`), with
`backend.Hooks.DropRefused` as WhatsApp's: the connector stores one row
before the framework's `NewLogin`, the login's prefix (keyed by the login
id, so by the other account's number), and that row is deleted at once.
There is no session to end: the connector has no logout (its
`LogoutRemote` only disconnects), and ending the Google web session would
sign that browser out of Google; the log says
`refused_session_nothing_to_end`. A sign-in whose `NewLogin` failed before
the gate (a cancel, a stop) would leave the same row: every failed
submission is followed by a sweep of the prefix rows no stored login has
(`network.go`), and each start runs the same sweep
(`backend.Hooks.PrepareLogins`), whose keep-set is the stored `user_login`
rows read in the same statement. The same Google account with a new Voice
number is another principal. **A logout** ends nothing at Google either:
the session the cookies belong to stays signed in at Google (in the
sign-in's web view, or the browser they were copied from) until it is
signed out there.

**Cookies Google no longer takes** (`state.go`): `gv-bad-credentials`
(Google answered 401 or 403: the cookies expired or the session was signed
out; at a connect, on the realtime channel, the message list, or after a
send or read Google refused so) is ENDED with that code, and so is
`gv-not-logged-in` (named, not sent by this version). `gv-connect-error`,
`gv-rate-limited`, `gv-transient-disconnect` and `gv-too-many-retries` are
RECONNECTING (the connector retries after 1 minute, or 10 after 5 drops);
nothing of the connector's is NEEDS_USER.

**Sending.** Nothing of the host's send key reaches Google: the connector
sends with a random transaction id of its own, so no `SendKeyFormat`. A
certain success answers OK with the message's id, Google's thread item id
(`threadItemID` of the send's answer, the field its messages carry as their
id). Google's 4xx answers are REJECTED (certain; a 401 or 403 also checks
the session), but 408; a 5xx or no answer is UNKNOWN, never sent again by
the worker: the journal answers the same key `in_flight` from then on.
**libgv itself retries** a request that got no answer or a 5xx, up to 11
times more with 132 s of waits between them, with the same body (the same
transaction id):
a text may reach Google twice if Google does not drop the repeat by that id
(not in the source). A message that did go out after an UNKNOWN arrives as
the account's own (`from_me`) with no `send_key`.

**History a host asks for** ends on Google's 429 as `rate_limited` with
its `Retry-After` (`Hooks.RateLimitWait`).

Its smoke test starts it with no settings, begins the flow to its WEB step
and never contacts Google. To confirm against Google (not in the source):
that a Google sign-in completes in the app's web view at all (Google
refuses some embedded browsers) and which of the eight cookies it sets
there; that `primaryDestinationID` is the Voice number in E.164; that a
sent text's thread item id is the id its copy then carries in the thread;
whether Google sends unsigned texts, and drops a repeated transaction id;
how long the cookies last; Electron under Xvfb with the connector's script.

## History a connection gets by itself

A new connection's chats arrive with their recent messages, with no command
from the host: when the framework makes a chat's room (at sign-in, for each
chat the connector makes in its initial sync; later, for a chat made when a
message comes in it) and the connector can backfill it, the framework fetches
the chat's newest `max_initial_messages` (default 50) in the room's creation,
before any later event of that chat, and the worker reports them as
`HistoryBatch` events with no command id, oldest first, split at 100
messages or 1 MiB like any batch, with the network's ids
(`history:<chat>:<first>:<last>`, "Event ids"). When the connector says a
chat it has missed messages (mautrix-telegram after a channel or supergroup
gap its update stream does not replay, mautrix-whatsapp for a history sync of
a chat it has), up to `max_catchup_messages` (default 100) newer than the
newest stored are fetched the same way. **An initial import is
`forward = false` and a catch-up is `forward = true`**, and the host stores
each message so: an initial import as history, a catch-up as a replay of the
gap after a disconnect (the class later handling may act on). The framework's
request is forward for both; the worker tells them apart in `BatchSend` by
whether the framework's database held any message of the chat before the
batch (`unaskedForward`, `portalHeldMessages`): bridgev2 stores a batch's
rows only after `BatchSend` returns, so the batch is not counted, and a room
just made holds none. A page that answers a `FetchHistory` keeps the
command's direction. Older history comes only on
`FetchHistory` (the framework's backward queue is off). Both settings are
counts per chat from 0 (off) to 500 (`backend.ReadHistory`); any other value
refuses the start with `bad_settings`. Before this the defaults were 0, and
nothing in the engine or the product sent `FetchHistory` then: a real
Telegram sign-in landed every chat with no message (`TestHistoryOnSignIn` is
the test that shows it). The engine sends it now
(`ChatConnections::fetch_history`, one page a call, "History a host asks
for", below); the product does not page with it yet.

No message lands twice: the framework drops what it fetched that is not newer
than the newest stored message of the chat (and, of those at the same time, any whose
id it has), and a live message whose id it has stored is ignored, so the
newest message fetched in the import and then sent again live by the network
reaches the host once. A restart fetches nothing again: the rooms exist. A
channel's history is fetched (one request) and dropped like everything else
of a channel ("Channels are not sent").

What it costs the network: one history request per chat whose room is made,
one at a time (the framework handles a login's events in order, and
mautrix-telegram allows one history request at a time and waits out a
`FLOOD_WAIT`, retrying up to five times, but does not space requests for
different chats). A Telegram sign-in with the defaults is 15 requests of 50
messages (`getHistory`), whatever the number of chats: one per room its
dialog sync makes, `sync_create_limit` of them ("The Telegram worker", which
counts the sync's other requests too).

A connection's own `max_initial_messages` or `max_catchup_messages` (set at
its sign-in's begin, `README.md` "Chat connection settings") reaches the
worker merged into `Init.settings` like the host's, and is read and bounded
the same way (`TestHistoryOnSignInSetting` starts the worker with such a
map).

## History a host asks for

`FetchHistory` asks the connector for one page of one chat (`backend/history.go`):
a backward fetch anchors at the oldest message the worker holds of the chat
(an empty cursor resumes correctly), the count defaults to 50 and is capped
at 200, and the page is reported as `HistoryBatch` events carrying the
command id (`history:<chat>:<command>:...`; one empty batch when nothing on
the page was new), then the reply carries `cursor` and `has_more`. The
events are in the outbox before the reply is sent, but the reply is written
ahead of queued events (`wire.Endpoint.SendReply`), so the host may read it
first; the events follow at least once (after a crash, from the outbox).
The host takes the landed batches with the command id as the page.

- **One at a time.** A `FetchHistory` that comes while another runs is
  `OUTCOME_REJECTED` `worker_busy` at once, nothing asked of the network.
  Before, it waited on the history lock, possibly past its own deadline (the
  one running may be one the host already gave up on), and then fetched for
  a host that was no longer waiting.
- **Bounded by the deadline.** The network request runs until 2 s before
  the command's `deadline_ms` (a quarter of the time left when that is
  less; 4 minutes for a command with none). A request still running then is
  ended: `OUTCOME_REJECTED` `fetch_timed_out`, nothing bridged, answered
  while the host still waits. A command whose deadline has passed is
  `deadline_passed`.
- **The network's rate limit** is answered `OUTCOME_REJECTED` `rate_limited`
  with `CommandReply.retry_after_s`: the wait's whole seconds rounded up,
  and one more (0: the network named none). mautrix-telegram's
  `FetchMessages` retries its request up to 5 times and sleeps out each
  `FLOOD_WAIT` under 5 minutes itself (`tgerr.FloodWait`, no option to stop
  it; worst case about 25 minutes in one command, holding the lock). The
  worker lets it sleep out a wait of at most 15 s that ends before the
  bound (5 such waits fit the engine's default deadline of 120 s); for any
  other it ends the request at once, through the request's context, as the
  connector is about to sleep. It sees that moment only by the warning
  `tgerr.FloodWait` logs to the context's logger just before it sleeps
  (`Waiting on flood wait`, with the wait), which the worker reads through a
  logger of its own in the request's context (`backend.WatchLogWaits`,
  `Hooks.WatchRateLimit`; the context carried no logger before, so no line
  is lost). A wait the connector does not sleep out (5 minutes or more, or a
  fifth in a row) comes back as the `FLOOD_WAIT` error, read with its wait
  (`Hooks.RateLimitWait`). `TestFloodWaitHooks` pins both against the
  connector's own code; a connector update that changes the log line turns
  a long wait into `fetch_timed_out` at the bound, not into a 25-minute
  sleep. `fetch_failed` is any other error of the network.
- **WhatsApp** never asks the network or the phone on a `FetchHistory`:
  on-demand backfill from the phone is off (`BackwardsOnDemand`), so
  mautrix-whatsapp pages only through what the phone's history sync left in
  its own tables, and it empties a chat's rows once the chat's first import
  is done (the backfill queue is off: `DeleteAllInChat`). A fetch is
  therefore usually answered `OUTCOME_OK` with one empty batch and
  `has_more` false, which on WhatsApp means "nothing more on this device",
  not the chat's beginning. It is never `rate_limited`.

The fake network has a script knob for each case: `history_rate_limits` (a
wait the connector sleeps out, logged as `tgerr.FloodWait` logs it, or one
it fails with at once) and `history_hang_ms` (a network that does not
answer); `TestHistoryRateLimited` and `TestHistoryTimedOutAndBusy` drive
them.

## The store is encrypted

The worker keeps one SQLite file, `<state_dir>/worker.db` (with its `-wal` and
`-shm`), holding bridgev2's tables, the connector's (its network session, a
dbutil child database) and ours (`store/`). It is encrypted with SQLite3
Multiple Ciphers: cipher `chacha20` (ChaCha20 with a Poly1305 tag on every
page, 4096-byte pages, a random 16-byte salt as the first bytes of the file),
keyed with the 32 bytes of `Init.store_key` used as a **raw key** (no KDF).

- The key goes in the URI of each pooled connection's open
  (`cipher=chacha20&key=raw:hex:…`), built in a byte buffer that is zeroed
  when the open returns; it is never logged and never in an error
  (`store.OpenError`'s text is its code). Init's copy, and the frame buffer
  Init was read into, are zeroed once read. A PRAGMA in a connect hook would
  not work: mattn reads the schema before the hook runs. Not zeroed, because
  Go cannot reach them: mattn's C copy of the URI (freed, not cleared),
  SQLite's own copy of the URI parameters (kept for the connection's life),
  the cipher's key material, and what the Go and C heaps free without
  clearing. The promise is that the key reaches no file, log, error or
  reply, not that no copy is left in the process's memory (`store/db.go`,
  `connector`).
- **Every connection is keyed.** The store opens its own pool
  (`store/db.go`, `connector`) instead of a registered driver: the
  framework, the connector's child database, `withFullSync` (the send
  journal's in-flight row, synchronous=FULL) and `Wipe`'s checkpoint
  connection all take their connections from it, so each is opened with the
  key and checked as below (`TestEveryConnectionKeyed`).
- **Fail closed, before anything is created:** `SELECT sqlite3mc_version()` on
  an in-memory database must answer; an existing file is first probed
  immutable and read-only (so a wrong key touches neither it nor a WAL left by
  an unclean stop); then on every new connection, before its first write,
  `PRAGMA cipher` must be `chacha20` and
  `sqlite3mc_codec_data('cipher_salt')` must show an encrypting codec. A file
  that starts with the plain SQLite header is refused and left alone.
  `Hello.store_encrypted` is true only after all of that held.
- Connections: `foreign_keys=ON`, `journal_mode=WAL`, `synchronous=NORMAL`,
  `busy_timeout=5000`, `secure_delete=ON` (Wipe overwrites what it deletes),
  `temp_store=MEMORY` (no plaintext temporary files), `_txlock=immediate`; at
  most 5, and all 5 kept open when idle: a new connection starts with a cold
  page cache that must be decrypted again.

A store that must not be used as it is (the first four codes below: verdicts,
final until someone acts) is reported, then the worker exits **5**
(`backend.ExitStoreRefused`): Hello (`store_encrypted` false,
`last_unacked_seq` 0), then one ConnectionState ERROR with **seq 0** (there is
no outbox to keep it in; it is never replayed, its event id is unique per
start). Codes:

| Code | When | Disk |
|---|---|---|
| `store_key_invalid` | `Init.store_key` is not exactly 32 bytes | nothing touched |
| `store_not_encrypted` | the linked SQLite is not sqlite3mc, the cipher in effect is not chacha20, or `worker.db` is a plain SQLite file | nothing created; an existing file untouched |
| `store_key_mismatch` | page 1 of an existing file fails to decrypt (`SQLITE_NOTADB`) | file untouched, never deleted or recreated |
| `store_corrupt` | SQLite found the decrypted file malformed | untouched |
| `store_open_failed` | the file could not be reached now (I/O, permissions, a full disk, a busy file), or a query of the checks above failed (out of memory, I/O): only an answer that says the store is not encrypted, or SQLite's "no such function", is `store_not_encrypted` | untouched. **Not a verdict and not reported as one:** no Hello is sent and the worker exits 1 (the code goes to its stderr, which the engine does not keep). The engine counts a failed start: it starts the worker again with its backoff, and after 5 failed starts within 600 s it parks the connection as `crash_loop` until someone restarts it |

Page 1 of a damaged file fails its tag exactly like a wrong key, so such a
file is reported as `store_key_mismatch`: the cipher cannot tell the two apart.

A file that opened and passed the checks but on which the worker's own
tables cannot be set up (`store_setup_failed`), and a spool key that cannot
be derived (`spool_key_failed`), end the same way as `store_open_failed`:
no Hello, exit 1.

The `plainstore` development build (below) refuses like any build whose
library does not encrypt (`store_not_encrypted`) unless
`Init.settings["dev_plain_store"] == "1"`; the encrypted build ignores that
setting.

**What the wipe tests prove on an encrypted file.** `TestLogoutWipes` reads
`worker.db*` byte by byte for the fake network's names and texts. On the
encrypted file that scan only shows that nothing is stored in plaintext, so
the test also decrypts a copy of the stopped store (checkpointed, then
`PRAGMA rekey = ''`, which rewrites every page, free pages included) and scans
that plaintext: it must hold the schema, and the names before the logout and
none of them after.

## Everything else on disk

| Path | Holds | At rest |
|---|---|---|
| `worker.db`, `-wal`, `-shm` | everything the worker keeps | encrypted (above) |
| `media/` | the spool: bytes the connector pushed (avatars, uploads) | encrypted, see below |
| `tmp/` | scratch files while one call runs (an upload the connector streams, a direct download of the `file` kind) | **plaintext**, 0600, removed when the call ends and swept at start |
| `transfer/` | what the host asked for with `FetchAttachment` | **plaintext by contract**, see below |

**The spool** (`backend/spool.go`, Go standard library only: `crypto/hkdf`,
`crypto/aes`, `crypto/cipher`). Spool key = HKDF-SHA256(store key, no salt,
`"lynx chatworker spool v1 key"`); each file is `"LXSPOOL2"`, a random 32-byte
salt, then AES-256-GCM chunks of 64 KiB under the file key
HKDF-SHA256(spool key, salt, `"lynx chatworker spool v2 file"`). The nonce of
chunk *i* is *i* (11 bytes, big-endian) and a final-chunk flag byte (the STREAM
construction), and every chunk authenticates the header: reordering, dropping,
truncating, appending or moving chunks between files fails. A file is named
HMAC-SHA256(name key, sha256 of its bytes) (name key from
`"lynx chatworker spool v2 names"`), so the directory does not reveal
digests either, and the final chunk also authenticates that name: a file
renamed or copied to another's name does not decrypt, and is a miss
(`not_found`, logged `spool_unreadable`); it is checked before any byte is
read, and a write of the same bytes replaces it. Files of version 1 (no name
binding) are not read: the spool is a cache, and they are removed at the
next eviction. Bytes are decrypted only while they are read (FetchAttachment,
or the framework re-reading an upload). Budget: `SpoolBudget` = 256 MiB
(`spool_budget_bytes` in settings overrides): after each write the oldest files
(by modification time; a repeat write refreshes it) go first, never one being
read nor the one just written, so a single file over the budget stays until
the next write. An evicted handle answers `not_found`.

**The transfer directory** is where the host asked for the plaintext bytes,
so they stay plaintext: the worker writes each under a temporary name
(`.partial-*`, mode 0600) and renames it only when complete, so a refused
(`too_large`, …) or failed transfer leaves nothing; partials left by a crash
are swept once at start (`.spool-*` in `media/` and `tmp/` too).

Logout empties the store (`Wipe`) and removes `media/`, `transfer/` and
`tmp/`; the new framework instance starts on the emptied store.

**What a stopped framework instance can still write.** A logout (or the
wipe after a sign-in of another account) stops the framework instance (up
to 5 s), wipes the store and starts a new instance. Every wipe starts a new
epoch of the store (`store.Epoch`); a write made through an older epoch
(`store.Since`, `AppendEventSince`) is refused with `store.ErrWiped` and
stores nothing, and a wipe waits for such a write in flight. Each framework
instance is given its own view of the backend (`backend/instance.go`), which
carries the epoch of when the instance was made, as do the intents it hands
out. Guarded so, after the wipe nothing of a goroutine of the old instance
that outlived the stop is stored:

- the events it reports (the outbox);
- the worker's own rows it has written (`lynx_rooms`, `lynx_members`,
  `lynx_state`, `lynx_people`, `lynx_spool`, and the spool file of an
  upload, removed again);
- sends, through the epoch read when each send began.

A refused write returns `store.ErrWiped` to the framework; the first of an
instance is logged `stale_write_refused`. **Not guarded:** the framework's
own tables (`portal`, `ghost`, `message`, `user_login`, …) and the
connector's child database (its session). The wipe empties them after the
instance was stopped, but a goroutine of the old instance that outlives the
stop could still write there, into the store of the next sign-in.

## Event numbering

Upward events are numbered by the outbox's AUTOINCREMENT counter
(`lynx_outbox`, `sqlite_sequence`), which Wipe keeps, so a seq never repeats.
At start the worker raises that counter to `Init.host_stored_seq` (the
highest seq of the connection the host has stored, which it takes for a
repeat at or below): a worker whose store was lost or reset numbers new
events above the host's mark; queued events keep their seq, and the counter
never lowers (`store.RaiseSeqFloor`).

## Event ids

An event id is unique for the life of a connection's landed log: the host
lands each id once and takes a repeat of it for the same event
(`channel.proto`, `ChatEvent.event_id`). Ids of the network's events are
built from the network's own ids (`msg:`, `edit:`, `react:`, `del:`,
`read:`, `delivered:`, and a history page the framework sends unasked,
`history:<chat>:<first>:<last>`), so a message synced again after the store
was lost is the same event and is not landed twice. Ids that carry the
host's command id are the host's (`resolved:<command>`; a page that answers
a `FetchHistory`, `history:<chat>:<command>:<first>:<last>`, and an empty
one, `history:<chat>:<command>:empty`): the batch names its command, so two
commands that get the same page are two events, each landed for its
command, while the messages in them keep the network's ids.
Ids of the events the worker generates are built from counters and
generations of its store, which start again in a new store file (and the
generations in a wiped one), so each carries the store's **nonce** as its
first part (`backend.genID`): `self:`, `state:`, `chat:`, `person:`,
`abilities:`, `chat-removed:`, `signin:`, and the echo of an edit command
(`edit:<nonce>:...`). The nonce is 12 random bytes in hex, made with the
store (`lynx_store_nonce`, `store.Nonce`) and kept across restarts, so a
replay within a store has the same ids; a store made before nonces gets one
at its first start. So a worker upgraded to the nonce ids lands, once, its
generated state again under new ids (each `ChatInfo`, abilities,
`PersonInfo` and connection state it reports after the upgrade, its own
`PersonInfo` first): idempotent state, below, that says what the host
already has. **A wipe renews it** (in the same transaction): the
generations it empties would otherwise repeat, and the queue it empties
holds nothing to replay. A store refused at start (`refuseStore`) has no
store to read: its one event's id is made from the code and the time.

So after a store is lost or wiped, what the new store syncs again from the
network lands only where its id is the network's (the messages and the
rest above, which the host takes for repeats). The state the worker
**generates** for what it syncs again (a chat's `ChatInfo`, its
`ChatAbilities`, a person's `PersonInfo`, a `ChatRemoved`) lands again,
under new ids: these events are idempotent state, each saying how its chat
or person stands now (not a change to apply to what was), so one landed
again says what the host already knows.

## Event order: the account's own PersonInfo first

In every framework instance, before the first event about a signed-in
login's chats or people (`ChatInfo`, `ChatAbilities`, `ChatRemoved`, a
`Message`, `Edit`, `Reaction`, `MessageDeleted`, `ReadUpTo`, `Delivered`, a
`HistoryBatch`, a `PersonInfo` of someone it met) is stored, a `PersonInfo`
with `is_self = true` and `person_id` = the account's own id has been stored
in that instance. That id is the one the login's chats name the account by
(`Member.person_id`, a message's `sender_id`: `backend.selfFor`, the
connector's `Hooks.SelfPersonID`, else the login id). It is emitted once per
instance and login (`backend/instance.go`, `ensureSelf`), so once per
accepted sign-in and once per worker start that loads a stored login (a
re-link of the same account in the same instance emits none again):

- once the framework keeps the login: a stored login when it is loaded and
  cached at a start, before its client connects (`Backend.start`); a new
  one when `NewLogin` has returned (`completeSignIn`). It carries the
  account's name and identifiers as the login has them, with no network
  call;
- before that, if an event of the login comes first (a connector connects
  the new login's client before its sign-in completes, as mautrix-telegram
  does): just before that event, with the name and identifiers of the login
  as the framework stored it (`NewLogin` stores it before the connector
  connects it), read from the database with no network call; with
  `person_id` and `is_self` only when no stored login is found.

An event of the login waits while it is being emitted, and when it could
not be stored (the outbox failed) the event is refused too and the next one
tries it again first. A refused event is counted and logged
`chat_event_refused` (the first of each framework instance). A refused
`ChatInfo`, `ChatAbilities` or `PersonInfo` is not recorded as reported: a
room's are made again before its next event, a person's before their next
use (see "Channels are not sent", people). It goes through the instance's epoch like every event
of the instance: a sign-in of another account refused at the gate, or wiped
after it, emits none, and neither does a stored login of another account
(refused at a start). A `PersonInfo` of someone else is reported for the
login of the event it came with, else for the connection's account, else for
the login the gate let through last; with none of the three (a connector
that passes no gate, before its sign-in completes, and the profile change,
without one of its login's events, of a person reported before) it is not
ordered. A person not reported yet is reported for the login of the room
where they are first used (see the people paragraph below). Later profiles of the
account's own ghost keep `is_self`. Not ordered against it: `ConnectionState`,
`SignInStep`, `SendResolved`, and the events of an earlier run still in the
outbox, which are replayed first at a start.

## Channels are not sent

The contract's `CHAT_KIND_CHANNEL` is "a broadcast channel or a container of
chats. Not ingested as a chat." The worker reports a room as `CHANNEL` when
its bridgev2 portal is a space (`database.RoomTypeSpace`; in mautrix-telegram
v0.2611.0 a forum's space room and a community), or when the connector's hook
(`Hooks.IsChannel`) says so of the portal as the framework stored it
(`backend.ChannelQuery`: the portal's row with its current key and the
connector's metadata, the id the room was first reported under when the
framework re-IDed the portal since, whether the room is made, and the power
levels the worker stored for it). A direct chat is never one, nor is a
portal whose room the worker has no row of.
Of a `CHANNEL` room the worker sends its `ChatInfo` (and its `ChatRemoved`)
and nothing else: no message, edit, reaction, deletion, read, delivery,
history batch or abilities, no spool row for an attachment uploaded for it,
and no `PersonInfo` of its people. The drops are counted and logged
`channel_event_dropped` once per room and framework instance. Send, edit,
delete, react, mark read and fetch history on it are `OUTCOME_REJECTED`,
`channel_not_ingested`.

In `chat_telegram` the hook makes a Telegram broadcast channel `CHANNEL`
(mautrix-telegram bridges it as a default room). The evidence is the
connector's own stored data, with no network call (`cmd/chat_telegram/channel.go`):
a portal of a channel peer (`channel:<id>`, no topic) whose metadata has been
written by a chat-info sync (`last_sync` set) and says it is not a supergroup
(`is_supergroup` false; also not a forum's General topic nor a community),
and, once its room is made, whose room's power levels give the channel's own
ghost (the user id the connector gives the channel itself) the superadmin
level 75. The connector itself takes such a portal for a broadcast channel;
it makes the channel's own ghost a member at that level only when Telegram's
`broadcast` flag is set (`chatinfo.go:196`), every time it builds the
channel's info, so the room is made with it and the worker stores it before
the room's first `ChatInfo`. bridgev2 stores the metadata before it creates
the room, so a broadcast channel is `CHANNEL` from its first `ChatInfo`.
Before the room is made the hook is asked only whether to hold the profiles
of the portal's people (they are reported at their first use in a room that
is sent, so that answer may be wrong without loss). A portal whose metadata
was never written is a group until it is. Not `CHANNEL`: a basic group, a
direct chat and "saved messages", a supergroup (also a gigagroup and the
discussion group linked to a channel), a forum's topics and its General
topic, and a basic group upgraded to a supergroup: the connector re-IDs its
portal to `channel:<id>` keeping the basic group's metadata until the
chat-info sync after the re-ID (which can fail) rewrites it, and Telegram
upgrades a basic group only to a supergroup, so a room first reported as
`chat:<id>` is never `CHANNEL`, before that sync or after it. That rule only
ever says "not a channel": it cannot drop a real group.

In `chat_whatsapp` the hook makes a newsletter (`<digits>@newsletter`) and the
status broadcast (`status@broadcast`) `CHANNEL` (`cmd/chat_whatsapp/channel.go`).
mautrix-whatsapp makes a portal's id of the chat's JID and nothing else, so
the id is the evidence, from the portal's first row; a portal re-IDed from
anything else is not called one (the connector re-IDs only direct chats).

The kind is looked up for each event from what is stored, so it holds after a
restart. A space stays one (bridgev2 does not turn an existing room into a
space or back, and a room created again takes its new kind); the hook's answer
can change when the connector stores new metadata. The first time a framework
instance looks at a room, and whenever it sees the room's kind change, the
room's `ChatInfo` (and, for a room that is sent, its abilities) is sent before
that event when it changed, so a room whose kind changed while the worker was
stopped is announced with its new kind; its events stop (or go again) from that
event on.

People: a person is reported once they matter in a room that is sent. A
profile that comes with an event of a room that is sent is reported at once,
and so is any profile change of a person reported before. A profile of a
person not reported yet that comes otherwise (with an event of a channel, or
without any event: mautrix-telegram updates every user of each network update
and a channel's own ghost, the sender of its posts, that way) is stored and
marked as not reported. The first time that person is a member of, the sender
in (message, edit, reaction, deletion, read, delivery, history) or mentioned
by an event of a room that is sent, their `PersonInfo` is sent before that
event, with the stored profile and after the account's own `PersonInfo`; an
event of another room for the same person waits for it. A person who never
appears in a room that is sent is never reported: a channel's own `ChatInfo`
lists its members, but does not report their `PersonInfo`.

Limits: a Telegram monoforum (a channel's direct messages, the `monoforum`
flag of a channel) has no handling in mautrix-telegram v0.2611.0: nothing in
the connector sets it apart, so a message or dialog with its peer would make a
`channel:<id>` portal like any channel's, of which the connector stores only
`is_supergroup` (the channel's `megagroup` flag). Whether such updates reach a
user's account at all was not checked, and the sources here (the connector
and its TL schema, `pkg/gotd/tg`: `Channel.Monoforum`, `LinkedMonoforumID`,
"this supergroup or monoforum") do not say whether Telegram sets `megagroup`,
or `broadcast`, on a monoforum. The hook no longer rests on `megagroup`
being unset alone: it also needs the channel's own ghost at level 75, which
the connector sets only for Telegram's `broadcast` flag. A monoforum that
is not flagged `broadcast` is therefore a group (sent), whatever its
`megagroup` flag; one that Telegram flagged `broadcast` would be `CHANNEL`,
and that it never is cannot be shown from these sources.

## A chat's id never changes

The `chat_id` of a room is the id it was first reported under, for as long
as the store lives (`lynx_rooms.chat_id`; `backend/reid.go`). bridgev2 can
give a portal a new key after its room was made (`Bridge.ReIDPortal`; in
mautrix-telegram v0.2611.0 a basic group upgraded to a supergroup,
`chat:<id>` becoming `channel:<id>`, `migrateChat`). It renames the
portal's row (its messages, reactions and backfill rows follow by cascade)
and keeps the room, its id, its type and the connector's metadata, and calls
nothing of the worker's: the worker sees the re-ID where it maps a room to
its portal (by the room's id, which is stable) or a chat id to its portal,
as a portal whose key is not the one its room's row has, and rewrites the
row's `portal_id` there, through the instance's epoch guard like every write
of an instance (logged `portal_reid_followed`). For the host, after a re-ID:

- every event of the chat keeps its chat id, and nothing is sent for the
  re-ID itself (the chat did not change; the info the connector syncs after
  it comes as a `ChatInfo` under the same id, its kind asked of the hook
  again, see above);
- every command that names the chat id (send, edit, delete, react, mark
  read, fetch history) reaches the portal under its current key, and its
  echoes carry the chat id; a `StartChat` that resolves to the portal
  answers the chat id;
- a chat id is only ever one a room was first reported under: the
  portal's new raw id, which the host was never given, answers
  `unknown_chat` (so does the id of a removed room);
- a re-ID onto a portal that has a room of its own merges the two: bridgev2
  deletes the source portal's row and then, in a goroutine of its own,
  tombstones and deletes the source room, which the worker reports as
  `ChatRemoved` under the source's chat id (then `unknown_chat`); the
  target stays the chat it was, under its own id. A worker stopped between
  the two leaves the source room reported but without a portal: its
  commands answer `unknown_chat`, and no `ChatRemoved` follows. A re-ID onto
  a portal row without a room deletes that row and re-IDs the source as
  above; a re-ID of a portal without a room deletes it (nothing reported).

It holds across restarts (the row is stored). A logout wipe forgets it with
the rest of the store: the next sign-in reports its chats under their
portals' ids of then.

An event that comes under the old key after the re-ID and may create a
portal (in mautrix-telegram a message of the basic group handled after the
supergroup's `MigrateFrom`, the two have update streams of their own; or a
basic group's dialog whose `MigratedTo` is not an `InputChannel`) makes the
framework insert a fresh portal row under the old key and ask for its room.
The hash of the old key is the re-IDed room's id, which its portal still
has, so the worker gives the fresh portal a superseded room instead
(`GenerateDeterministicRoomID`: another id, made from the key alone, so the
same after a restart; `lynx_rooms.superseded`, with no chat id). Its
creation succeeds, the framework saves the portal and handles the event:
nothing fails, so nothing goes back to the connector as an error (in
mautrix-telegram a failed event is an error of its update handler, which
ends the update loop and the client, `UNKNOWN_ERROR`, and the update comes
again at the next connect). Nothing of a superseded room is ever sent: no
`ChatInfo`, abilities, message, edit, reaction, deletion, read, delivery,
history or `ChatRemoved`, no spool row of an upload for it. A profile that
comes with one of its events, of a person not reported yet, is held: stored,
and reported at that person's first use in a room that is sent; a change to
the profile of a person already reported is sent as any. The drops are
counted and logged
`superseded_room_event_dropped` once per room and framework instance. No
chat id names it (a lookup by chat id never returns it), and a `StartChat`
that resolves to its portal answers `unknown_chat`. The live room is not
touched: its state, name, members and generation stay, no `ChatInfo` with
the old group's name or members comes, and every command on the chat id
reaches the portal under its new key. Limit: the late event itself is
dropped (handled, not failed): it is of the same conversation, but nothing
routes it to the live room. An event under the old key that may not create
a portal is dropped by the framework ("portal doesn't exist").

The framework ends a superseded portal like any: a re-ID of the old key
onto the live portal again (mautrix-telegram's `migrateChat` for a late
`MigrateTo`) merges it, deleting its row and then its room, and a deletion
of the portal deletes its room; the room's row is marked deleted, with no
`ChatRemoved` (it was never reported). Once the live portal is gone, a
later portal of the old key gets the old key's own room id again and is
the chat it was, and so does a superseded room of the old key that its
portal still has: at its next event, once no live room of the login that
stands for a portal has the old key's id (the live room was removed, or its
portal is gone without a `DeleteRoom`), it becomes the live room of that id
(as below, `superseded_room_live`); while the live room's portal merely
moves on, it stays superseded. A live room whose portal is gone with no
`DeleteRoom` (a merge cut off after the framework deleted the source
portal's row, before its goroutine deleted the room) is reported
`ChatRemoved` and marked removed when another room takes its chat id, and
at the next start before any login runs (`sweepOrphanRooms`, logged
`orphan_room_removed`): "gone" is certain there, read from the framework's
rows (no portal row has the room, and its key's row is gone or has another
room; a key's row with no room yet may be making it again, and is not). A
row that is made again for a new portal (its room id is free again) is
refreshed when its id is given out: it stands for that portal, not for one
it followed before. A superseded id whose row was once a chat is not given
out again (it keeps that chat's id and generations). A profile that comes
with the event that creates a superseded room (the framework updates the
room's people before it asks for the room id) is held as well: the room it
will be given is known then (its key's room id is another portal's, or a
live room of the login has the key's id). A creation that would take
another portal's room and for which no superseded id could be given (the
framework's rows could not be read, or all of the key's superseded ids are
other portals') is refused (`room_taken_refused`), before anything is
written or reported. A logout wipe forgets superseded rooms with the rest.

A superseded portal can itself be re-IDed onto a key that has no portal:
in mautrix-telegram, the user leaves the supergroup (the framework deletes
its portal and room: `ChatRemoved` under the chat's id, as for any chat),
and the next dialog sync migrates the basic group's portal again
(`checkDialogMigrated`). Every event of the supergroup then comes to the
superseded room, so it becomes a live room, at the first thing that asks
whether it is superseded (each such check follows the room's portal
first): its chat id becomes the new key's own id, the id a room made for
that key gets (`channel:<id>`), its `ChatInfo` (its members' `PersonInfo`
before it, held profiles included) and abilities are reported, and then
the event that asked, under that id (logged `superseded_room_live`). A
room removed before under that id keeps its row (deleted, with its chat id
and generations), so the id comes back as a chat that was removed, with
new event ids. While a live room of the login has that id, the room stays
superseded (logged `superseded_room_kept` once per room and instance) and
is tried again at its next event. Only a created room is a live room, so
the row kept for a supergroup room whose creation the re-ID cancelled (its
portal row had no room, and the framework deleted it) names no chat and
holds nothing back, and a room created under a chat id that a live room of
the login already has is made a superseded room: one chat id names one
live room. Not handled: a network that re-IDs a
portal away from a key and later makes a new portal under that same key
while the first lives: the new one is a superseded room, its chat not
reported (mautrix-telegram does not reuse a basic group's id).

As a last guard, an event about a chat (a message, an edit, a reaction, a
deletion, a read, a delivery, a `ChatInfo`, `ChatRemoved`, a history batch,
abilities) whose chat id is empty is never stored (counted, logged
`chat_event_without_chat_id` once per instance).

In mautrix-telegram the messages of a basic group from before its upgrade
are renamed by the connector (`migrated:<id>:<time>`,
`NamespaceMigratedMessages`) before the re-ID; the host was given `<id>`
under the same chat id. Every event that names one of those rows (a read
by event, or by stream position when it is the last message at that time,
a delivery, an edit, a reaction added or removed, a deletion) names it by
`<id>` (`Hooks.OriginalMessageID`, which reads it from an id that is
exactly `migrated:<digits>:<digits>`); one whose renamed id is of another
shape is not reported (logged `renamed_target_unmapped` once per
instance). A message's own id, its reply target and its thread root are
the connector's ids, never a renamed row's. A command that targets one of
those messages by the id it was reported with answers `unknown_message`
(the connector refuses the renamed ids itself, `ids.ParseMessageID`, and the
row is not found by the original). History from before the upgrade:
a backward `FetchHistory` whose oldest stored message is a renamed row has
the connector start from the newest message (it cannot use that row as the
anchor), and the framework drops every message newer than the anchor: the
answer is an empty batch, with no duplicate and no renamed id, and the
basic group's history from before what the worker stored is not fetched
(read in the sources, not run).

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Shutdown done, or the host closed the connection |
| 1 | an error before Hello (connection, Init, state directory, a store that could not be reached now, the store's tables, the spool key), or Hello failed. The engine counts a failed start: started again with its backoff, then parked as `crash_loop` after 5 failed starts within 600 s, until the host restarts the connection (`RestartChatConnection`) or the engine starts again |
| 2 | bad arguments |
| 3 | `chat_fakenet` only: a crash its script asked for |
| 4 | `backend.ExitRestartFailed`: after a logout wiped the store, no new framework instance could be started; the log line before it is `exit_restart_failed` |
| 5 | `backend.ExitStoreRefused`: the store must not be used as it is (a bad key, a plain file, a wrong key, a corrupt file; not a store that merely could not be reached, which is exit 1); Hello and one ConnectionState ERROR with the code above were sent first, nothing on disk changed |
| (signal) | SIGTERM, at any point: the worker logs `sigterm` and each logout of a refused session still running as `refused_session_not_ended`, then dies of SIGTERM at once, as it would without a handler (no exit code; the engine reports `worker_killed`). The engine sends SIGKILL 2 s after SIGTERM |

## Build and test

Requires Go 1.26 or later with cgo, a C compiler (gcc on Linux), `curl`,
`unzip`, `sha256sum`, `ar` and `readelf`. From `go/`:

    ./build.sh build     # linux/amd64, stripped, stamped, encrypting SQLite, into go/bin/
    ./build.sh lib       # fetch, verify and build libsqlite3.a only
    ./build.sh libsignal # fetch and build libsignal_ffi.a for chat_signal ("The Signal worker")
    ./build.sh vet       # go vet ./...            (encrypting build)
    ./build.sh test      # go test ./... -count=1  (encrypting build)
    eval "$(./build.sh env)"   # then plain `go test ./...` uses the encrypting build too
    ./build.sh plain     # DEVELOPMENT ONLY, see below
    ./build.sh darwin    # darwin/arm64, on a Mac: UNTESTED

Without the Signal library, `build` and `test` leave `chat_signal` out (one
line on stderr says so). A build whose directory will be staged with
`xtask build-binaries --all --go-bin <dir>` must contain `chat_signal`: run
it as `LYNX_REQUIRE_SIGNAL=1 ./build.sh build <dir>` with the library
present ("The Signal worker"), or stage with `--only` and name the workers
the release ships ("Staging", below).

The checks before a change goes in, from `go/chatworker` after
`eval "$(../build.sh env)"`:

```
gofmt -l .
go mod verify
go vet ./...
go test -count=1 ./...
go test -count=1 -race ./...
```

**With the engine.** `crates/ingest_fake_chat/tests/go_worker_interop.rs`
has the Rust engine drive a built `chat_fakenet` end to end (sign-in by steps,
landing, sends, history, restarts, a lost store, SIGTERM, a refused re-link,
erase). It runs only when `LYNX_GO_CHAT_FAKENET` names the binary by an
absolute path, and otherwise passes with a line saying it was skipped, so the
Rust test run never builds Go. No CI runs it (this repository has none at
the moment, and a CI run would skip it). **Run it, and say that you did with
its result, before every pull request that touches `src/connected_workers`,
`src/chat_connections`, `src/chat_landing`,
`crates/ingest_protocol/src/v2/channel*`, `crates/ingest_models/schemas/channel.proto`
or `go/chatworker/{backend,wire,contract,store}`**, on the pull request's
head, with the worker built from that head: from the repository root,
`(cd go && ./build.sh build /tmp/chatworker)`, then
`LYNX_GO_CHAT_FAKENET=/tmp/chatworker/chat_fakenet cargo nextest run -p
ingest_fake_chat --test go_worker_interop --locked --no-capture`;
`LYNX_GO_CHAT_STDERR=<file>` keeps the worker's stderr. A change that alters
what the engine sees of the worker updates the test in the same pull
request.

## In the engine

**Registry.** The engine registers `telegram` in every build
(`src/providers/chat_telegram.rs`): provider `telegram`, worker and binary
`chat_telegram`, either placement, any OS, connected, signs in by steps, one
service `telegram_chat` of the chat domain, risk `elevated` (its warning,
shown before sign-in, is in that file). With the engine's
cargo feature `fake-chat` (off by default; a staging or test build only) it
also registers `fakenet` (worker `chat_fakenet`) and `fakechat` (the Rust
`ingest_fake_chat`), risk none; without it neither exists anywhere in the
registry or the catalog. A host lists and starts a chat provider only where
its allowlist (`enabled_workers`) names the worker, this host supports it and
the binary is installed (`<INGEST_PROVIDER_ROOT>/bin/<binary>` on a strict
host). A connected worker needs an allowlist that names it: without one (the
Mac today) none runs, while every run-once worker does as before
(`WorkerSpec::enabled_by`; entries are worker ids, matched exactly). Today's
server allowlist (`ingest_google`) lists none either. The general provider
catalog (`ListProviders`) lists a chat provider only where the allowlist
enables its worker, so a host that names none lists what it listed before. A connection whose worker is no longer enabled is left down by
the engine-start pass (`chat_boot_not_on_this_host`, its record untouched)
and refused by a restart. Every Go worker (`chat_fakenet`, `chat_telegram`,
`chat_whatsapp`, `chat_slack`, `chat_signal`) is declared `WorkerBuild::Go`:
nothing in cargo builds them.

**Staging.** `xtask build-binaries` never builds a Go worker. With
`--go-bin <dir>` (the out-dir of `./build.sh build <dir>`) it copies each
selected one from there into `bin/` and indexes it in `providers.json` like a
cargo worker (its provider id, its services) plus `"build": "go"`; a missing
binary is an error. Without `--go-bin` it leaves them out of the stage and the
index with a `note:` line (not `skipping worker`), and `--only chat_telegram`
is an error. A release that ships a Go worker runs `go/build.sh build <dir>`
on the same commit first, then `xtask build-binaries ... --go-bin <dir>`, and
publishes that commit's `go/` (the AGPL). A strict host with no allowlist (the
Mac) does not require a connected worker's binary; one whose allowlist names
it does. Staging every worker (`xtask build-binaries --all --go-bin <dir>`)
needs `chat_signal` in `<dir>` like the others, so the Go build must have
run with the Signal library present: `LYNX_REQUIRE_SIGNAL=1 ./build.sh build
<dir>` ("The Signal worker"), which fails instead of leaving it out. A
release that does not ship Signal names its workers with `--only` instead of
`--all`.

**Settings.** The engine gives a worker the host's common settings and, over
them, those of its provider alone
(`ChatConnectionsBuilder::provider_settings`): Telegram's `api_id` and
`api_hash` go there, so no other connector's worker sees them. They are held
in memory only and never printed (`Debug` shows the keys). Settings reach a
worker only through a `ChatConnections` the host builds with them and
installs (`InstallChatConnections`): the supervisor's default chat service
has none, and `chat_telegram` started by it idles and refuses every sign-in
with `missing_settings`.

**Build check.** `build.sh` stamps `main.sourceCommit` (the checkout's
`git rev-parse HEAD`, with `-dirty` after it when `go/` has uncommitted
changes), which a worker prints for `--source-commit` alone (then exits 0,
before it touches anything) and sends as `Hello.source_commit`. A host that
sets `ConnectedWorkersBuilder::expected_worker_commit` (intended: the
ingest_bin revision it pins, its Go workers built from that commit) asks
each Go worker's binary `--source-commit` before it reads the connection's
key or spawns the worker for its channel (cleared environment, no stdin, at
most 5 s): an answer other than that commit exactly or a non-zero exit
parks the connection `worker_build_mismatch`, and the worker never gets
`Init` (no key, no settings, no store opened). A binary that does not answer
in time is killed and says nothing about its build: that start failed
(`worker_build_unanswered`) and is tried again like any failed start, still
with nothing handed over. `Hello.source_commit` is
compared again, before the store is looked at (a binary that answers one
commit and is stamped with another: parked the same way, after `Init`). A
parked connection is not restarted, nothing of its worker lands, its store
is kept; an erase still completes on this host (its network logout is
`not_running`). Workers built by cargo (the Rust `ingest_fake_chat`) are
exempt: they come from the engine's own source and lockfile, and may report
an empty commit. Without an expected commit any build runs (development),
except on a strict host (`INGEST_PROVIDER_STRICT`, an app bundle), which
runs no Go worker then (`worker_build_unchecked`). This is a check for skew
between the engine and a separately built worker, not a trust boundary:
whoever can replace the binary can make it answer anything.

**Restart.** A parked connection (`crash_loop`, a refused store, a build
mismatch) stays down until the host sends `RestartChatConnection` (or the
engine starts again), once the cause is put right.

### The encrypting SQLite

Every worker links **SQLite3 Multiple Ciphers 2.7.1** (SQLite 3.54.1, MIT
licence) statically, through mattn/go-sqlite3 v1.14.53 built with
`-tags libsqlite3`. The amalgamation is **not committed**: `build.sh` fetches

    https://github.com/utelle/SQLite3MultipleCiphers/releases/download/v2.7.1/sqlite3mc-2.7.1-sqlite-3.54.1-amalgamation.zip

into `go/.sqlite3mc/` (git-ignored; `SQLITE3MC_ZIP_FILE=<path>` uses a copy
you already have), refuses it unless its SHA-256 equals the one committed in
`sqlite3mc.sha256` (`d6bc5967…ba01af0`, the release's own SHA256SUMS), and
compiles `sqlite3mc_amalgamation.c` once into `libsqlite3.a` with

    -O2 -fPIC -DSQLITE_ENABLE_RTREE -DSQLITE_THREADSAFE=1 -DHAVE_USLEEP=1
    -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_OMIT_DEPRECATED
    -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
    -DSQLITE_USE_URI=1 -ffile-prefix-map=<src>=sqlite3mc

(mattn's options for its bundled SQLite, plus URI parameters, which carry the
cipher and the key; the archive is made with `ar rcsD`, deterministic). The
library directory holds only that archive, so mattn's `-lsqlite3` resolves to
it; `build.sh build` then refuses a binary whose `readelf -d` names a shared
SQLite. At run time a linux worker needs only glibc (`chat_fakenet`:
libc.so.6; `chat_telegram`: libm.so.6 and libc.so.6, libwebp being compiled
in), nothing of SQLite or OpenSSL.

The two store build tags are exclusive and one is required (`store/tag_*.go`;
a build with neither does not compile), as is cgo (`store/requires_cgo.go`):

- `libsqlite3`: the encrypting build, the only one `build.sh build|test|vet`
  makes and the only one to ship.
- `plainstore` (`./build.sh plain`, into `go/bin-plain/`): **development and
  tests only.** It links the driver's own plain SQLite. Such a worker refuses
  to start (`store_not_encrypted`) unless `Init.settings["dev_plain_store"]
  == "1"`; the encrypted build ignores that setting. Its store and backend
  tests run with `go test -tags plainstore ./store/ ./backend/`.

Test builds of a worker (`hosttest.BuildWorker`) pass on the tag the test
binary was built with (`store.BuildTag`).

### Reproducible

Two builds of the same commit are byte-identical, also against a library
built in another directory. Needed for that: `-trimpath`,
`-ffile-prefix-map` and `ar D` for the library, and `-ldflags -buildid=`
(Go's build ID hashes the cgo flags, which name the library's directory).
`libsqlite3.a` built here: sha256 `8c3775f8…ec5f9a86b` (gcc 14, 2026-11-19,
the same as on 2026-11-18 in another directory). Not checked: a build with an
empty Go build cache, or another compiler version (the library's stamp names
the compiler).

Checked 2026-11-19: two `./build.sh build` runs into different directories
gave the same `chat_fakenet` (sha256 `9d0c92f0…5f5361360a7`).

### Measured (linux/amd64, 2026-11-19, shared host under load)

| | encrypting (sqlite3mc) | plain (`plainstore`) |
|---|---|---|
| `chat_fakenet` size (`build`, `plain`) | 18,721,336 B | 18,335,896 B |
| `chat_telegram` size (`build`) | 54,126,328 B | not measured |
| VmRSS idle after sync / after the 120-message import (`TestMeasureRSS`, 1 run) | 34,656 / 35,056 KiB | 33,412 / 33,508 KiB |
| 120-message history import, wall (`TestMeasureRSS`, 1 run) | 378 ms | 834 ms |

The library adds about 385 KB to the binary. `TestMeasureRSS` and
`TestSlowHostDoesNotStallNetwork` pass `dev_plain_store`, so the same tests
time a `plainstore` build (`go test -tags plainstore -run
'TestMeasureRSS|TestSlowHost' ./cmd/chat_fakenet/`; the encrypting build
ignores the setting, and the other end-to-end tests need the encrypting
build). Single runs on a loaded host: the import times are noise (on
2026-11-18, on the parcel this was ported from, three runs each gave
111–256 ms encrypted and 80–203 ms plain).

### darwin/arm64 (untested)

`./build.sh darwin` holds the exact commands for a Mac: Apple clang,
`MACOSX_DEPLOYMENT_TARGET` (default 13.0), the same verified amalgamation and
options compiled with `-arch arm64 -mmacosx-version-min=…`, archived with
`libtool -static`, our `-L` ahead of mattn's hard-coded
`-L/opt/homebrew/opt/sqlite/lib`, and `otool -L` refusing any SQLite dylib in
the result. **None of it has been run**: the build host has no macOS SDK.

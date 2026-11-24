// SPDX-License-Identifier: AGPL-3.0-or-later

package contract

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

// defaultVectorDir is the shared vector directory, relative to this package.
const defaultVectorDir = "../../../crates/ingest_models/tests/vectors/channel_v1"

// VectorDirEnv, when set, names another vector directory (for a copy of go/
// that stands without the rest of the repository).
const VectorDirEnv = "LYNX_CHANNEL_VECTORS_DIR"

func vectorDirectory() string {
	if dir := os.Getenv(VectorDirEnv); dir != "" {
		return dir
	}
	return defaultVectorDir
}

func readVectors(t *testing.T) map[string][]byte {
	t.Helper()
	vectorDir := vectorDirectory()
	entries, err := os.ReadDir(vectorDir)
	if os.IsNotExist(err) {
		// Never skipped: without the vectors nothing checks that this
		// side and the Rust side agree on the contract.
		t.Fatalf("shared vectors not found at %s; set %s to their directory", vectorDir, VectorDirEnv)
	}
	if err != nil {
		t.Fatalf("read %s: %v", vectorDir, err)
	}
	out := map[string][]byte{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".hex") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(vectorDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		b, err := hex.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatalf("%s: not hex: %v", name, err)
		}
		out[strings.TrimSuffix(name, ".hex")] = b
	}
	if len(out) == 0 {
		t.Fatalf("no vectors in %s", vectorDir)
	}
	return out
}

// isHostFrame tells which frame type a vector holds, by its file name:
// host_* and command_* are HostFrames, everything else a WorkerFrame.
func isHostFrame(name string) bool {
	return strings.HasPrefix(name, "host_") || strings.HasPrefix(name, "command_")
}

// TestSharedVectorsRoundTrip decodes every shared vector and re-encodes it
// deterministically to the same bytes.
func TestSharedVectorsRoundTrip(t *testing.T) {
	vectors := readVectors(t)
	names := make([]string, 0, len(vectors))
	for name := range vectors {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		stored := vectors[name]
		t.Run(name, func(t *testing.T) {
			var msg proto.Message
			if isHostFrame(name) {
				msg = &HostFrame{}
			} else {
				msg = &WorkerFrame{}
			}
			if err := Unmarshal(stored, msg); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(msg.ProtoReflect().GetUnknown()) != 0 {
				t.Fatalf("decoded with unknown fields: the vector is not a %T", msg)
			}
			if !msg.ProtoReflect().IsValid() || !hasBody(msg) {
				t.Fatalf("decoded to an empty frame")
			}
			again, err := Marshal(msg)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !bytes.Equal(again, stored) {
				t.Fatalf("re-encoding differs\n got %x\nwant %x", again, stored)
			}
		})
	}
}

func hasBody(msg proto.Message) bool {
	switch m := msg.(type) {
	case *HostFrame:
		return m.Body != nil
	case *WorkerFrame:
		return m.Body != nil
	}
	return false
}

// TestVectorValues spot-checks decoded values against what the Rust test
// that wrote the vectors (crates/ingest_models/tests/channel_v1_vectors.rs)
// put in them, and builds two of them from Go values.
func TestVectorValues(t *testing.T) {
	vectors := readVectors(t)
	var init HostFrame
	if err := Unmarshal(vectors["host_init"], &init); err != nil {
		t.Fatal(err)
	}
	in := init.GetInit()
	if in.GetContractVersion() != Version || in.GetConnector() != "fakechat" || len(in.GetStoreKey()) != 32 ||
		in.GetSettings()["device_name"] != "Lynx" || in.GetSettings()["history_messages"] != "500" {
		t.Fatalf("host_init decoded to unexpected values")
	}
	if in.GetHostStoredSeq() != 0 {
		t.Fatalf("host_init carries host_stored_seq %d", in.GetHostStoredSeq())
	}
	// host_init_resume (Init.host_stored_seq, field 8) exists from the
	// contract change that added the field on.
	if raw, ok := vectors["host_init_resume"]; ok {
		var resume HostFrame
		if err := Unmarshal(raw, &resume); err != nil {
			t.Fatal(err)
		}
		r := resume.GetInit()
		if r.GetHostStoredSeq() != 41 || r.GetPrincipal() != "acct-1" || len(r.GetStoreKey()) != 32 || len(r.GetSettings()) != 0 {
			t.Fatalf("host_init_resume decoded to unexpected values")
		}
	}

	built := &WorkerFrame{Body: &WorkerFrame_Hello{Hello: &Hello{
		ContractVersion:  Version,
		WorkerName:       "chat_fakechat",
		WorkerVersion:    "0.1.0",
		SourceCommit:     "0123456789abcdef0123456789abcdef01234567",
		Licence:          "AGPL-3.0-or-later",
		UpstreamVersions: map[string]string{"maunium.net/go/mautrix": "v0.33.0"},
		StoreEncrypted:   true,
		LastUnackedSeq:   41,
	}}}
	got, err := Marshal(built)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, vectors["worker_hello"]) {
		t.Fatalf("worker_hello built in Go differs\n got %x\nwant %x", got, vectors["worker_hello"])
	}

	part1 := &Message{
		ChatId: "group-1", MessageId: "m-100", PartId: "1", PartIndex: 1, SenderId: "u-carol",
		TimestampMs: 1_794_999_990_000,
		Content: &MessageContent{
			Kind: ContentKind_CONTENT_KIND_TEXT, Text: "see this", FormattedHtml: "see <b>this</b>",
			Mentions: []*Mention{{PersonId: "u-bob", ImpliedByReply: true}},
		},
		ReplyTo: &MessageRef{ChatId: "group-1", MessageId: "m-90"},
	}
	built = &WorkerFrame{Body: &WorkerFrame_Event{Event: &QueuedEvent{Seq: 43, Event: &ChatEvent{
		EventId: "msg:group-1:m-100:1", EmittedAtMs: 1_795_000_000_000,
		Kind: &ChatEvent_Message{Message: part1},
	}}}}
	got, err = Marshal(built)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, vectors["event_message_part1"]) {
		t.Fatalf("event_message_part1 built in Go differs\n got %x\nwant %x", got, vectors["event_message_part1"])
	}
}

// TestUnknownFieldsTolerated mirrors the Rust test of the same purpose.
func TestUnknownFieldsTolerated(t *testing.T) {
	b, err := Marshal(&HostFrame{Body: &HostFrame_Ping{Ping: &Ping{Nonce: 7}}})
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, 0xa0, 0x38, 0x01) // field 900, varint, 1
	var f HostFrame
	if err := Unmarshal(b, &f); err != nil {
		t.Fatalf("unknown field refused: %v", err)
	}
	if f.GetPing().GetNonce() != 7 {
		t.Fatalf("ping nonce lost")
	}
}

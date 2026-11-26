// SPDX-License-Identifier: AGPL-3.0-or-later

// Package contract holds the Go types of the channel contract
// (ingest.channel.v1), generated from the repository's
// crates/ingest_models/schemas/channel.proto by gen.sh, plus the few helpers
// every user of the contract needs.
package contract

import "google.golang.org/protobuf/proto"

// Version is the contract version this build speaks in Init and Hello.
const Version uint32 = 1

// Marshal encodes m deterministically: map entries in key order, fields in
// number order. This is the encoding the shared test vectors are written in,
// and the one every event_id-bearing event must use so that repeats are
// byte-identical.
func Marshal(m proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}

// Unmarshal decodes b into m. Unknown fields (from a newer peer) are kept and
// otherwise ignored.
func Unmarshal(b []byte, m proto.Message) error {
	return proto.Unmarshal(b, m)
}

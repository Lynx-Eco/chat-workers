#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Regenerates channel.pb.go from the repository's contract file
# (crates/ingest_models/schemas/channel.proto, package ingest.channel.v1).
# The output is committed: a build never needs protoc.
#
# Pinned tools (record any change here and in go/README.md):
#   protoc         libprotoc 31.1 (protoc-bin-vendored-linux-x86_64 3.2.0)
#   protoc-gen-go  v1.36.12  (go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12)
#
# Usage: PROTOC=/path/to/protoc PROTOC_GEN_GO=/path/to/protoc-gen-go ./gen.sh
# The command it runs, from this directory:
#   "$PROTOC" --plugin=protoc-gen-go="$PROTOC_GEN_GO" \
#     --proto_path=../../../crates/ingest_models/schemas \
#     --go_out=. --go_opt=paths=source_relative \
#     --go_opt=Mchannel.proto='lynx.invalid/ingest/chatworker/contract;contract' \
#     channel.proto
set -euo pipefail
cd "$(dirname "$0")"
: "${PROTOC:?set PROTOC to the protoc binary (libprotoc 31.1)}"
want_protoc="libprotoc 31.1"
have_protoc="$("$PROTOC" --version)"
if [ "$have_protoc" != "$want_protoc" ]; then
	echo "gen.sh: need $want_protoc, found $have_protoc" >&2
	exit 1
fi
PROTOC_GEN_GO="${PROTOC_GEN_GO:-$(go env GOPATH)/bin/protoc-gen-go}"
want="protoc-gen-go v1.36.12"
have="$("$PROTOC_GEN_GO" --version)"
if [ "$have" != "$want" ]; then
	echo "gen.sh: need $want, found $have" >&2
	exit 1
fi
"$PROTOC" --plugin=protoc-gen-go="$PROTOC_GEN_GO" \
	--proto_path=../../../crates/ingest_models/schemas \
	--go_out=. --go_opt=paths=source_relative \
	--go_opt=Mchannel.proto='lynx.invalid/ingest/chatworker/contract;contract' \
	channel.proto

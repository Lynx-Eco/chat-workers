#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Builds the chat workers, stripped and stamped, and runs vet and tests.
# Every worker is built with its store encrypted: mattn/go-sqlite3 with
# `-tags libsqlite3`, linked statically against SQLite3 Multiple Ciphers,
# whose amalgamation is fetched at a pinned release and verified against the
# checksum committed in sqlite3mc.sha256 (it is never committed itself).
#
#   ./build.sh build [out-dir]        linux/amd64 workers (default go/bin)
#   ./build.sh lib                    fetch, verify and build libsqlite3.a only
#   ./build.sh vet                    go vet ./... (encrypted build)
#   ./build.sh test [go test args]    go test ./... -count=1 (encrypted build)
#   ./build.sh env                    print the exports of the encrypted build,
#                                     for: eval "$(./build.sh env)"
#   ./build.sh plain [out-dir]        DEVELOPMENT ONLY: -tags plainstore, the
#                                     driver's own plain SQLite (default
#                                     go/bin-plain). Such a worker refuses to
#                                     start (store_not_encrypted) unless Init
#                                     carries settings["dev_plain_store"]="1".
#   ./build.sh darwin [out-dir]       darwin/arm64, ON A MAC ONLY (untested)
#
# Stamps (-ldflags -X): main.version from VERSION (default 0.1.0),
# main.sourceCommit from `git rev-parse HEAD` (empty outside a checkout),
# with `-dirty` after it when go/ has uncommitted changes (tracked or new,
# not ignored), so such a build never passes the engine's build check.
#
# Environment: SQLITE3MC_CACHE (default go/.sqlite3mc) holds the download and
# the built library; SQLITE3MC_ZIP_FILE uses an already downloaded archive
# (it is verified the same way); CC (default gcc, clang on darwin).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
mod="$here/chatworker"
version="${VERSION:-0.1.0}"
commit="$(git -C "$here" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$commit" ] && [ -n "$(git -C "$here" status --porcelain -- . 2>/dev/null)" ]; then
	commit="$commit-dirty"
fi
# -buildid= : Go's build ID hashes the cgo flags, which name the library's
# directory, so without it two checkouts at different paths differ in those
# bytes alone.
ldflags="-s -w -buildid= -X main.version=$version -X main.sourceCommit=$commit"
# The worker binaries (cmd/<name>).
workers="chat_fakenet chat_telegram chat_whatsapp"

# ---- the pinned SQLite3 Multiple Ciphers release ----
mc_version=2.7.1
mc_sqlite=3.54.1
mc_zip="sqlite3mc-$mc_version-sqlite-$mc_sqlite-amalgamation.zip"
mc_url="https://github.com/utelle/SQLite3MultipleCiphers/releases/download/v$mc_version/$mc_zip"
# The compile-time options of the 2026-11-18 trial: mattn/go-sqlite3's own
# options for its bundled SQLite, plus SQLITE_USE_URI=1 (the key and cipher
# are URI parameters of the open). -ffile-prefix-map keeps build paths out of
# the objects.
mc_defines="-DSQLITE_ENABLE_RTREE -DSQLITE_THREADSAFE=1 -DHAVE_USLEEP=1 -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS -DSQLITE_OMIT_DEPRECATED -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT -DSQLITE_USE_URI=1"
cache="${SQLITE3MC_CACHE:-$here/.sqlite3mc}"
mkdir -p "$cache"
cache="$(cd "$cache" && pwd)"

# mc_lib <target-name> <cc> <extra cflags...>: fetch, verify, build
# $cache/<target-name>/{include,lib/libsqlite3.a}. Rebuilds when the stamp
# (archive checksum, options, compiler) changes, and when the library is not
# the one built: the STAMP file ends with the library's SHA-256, checked
# before every reuse. That SHA-256 sits in the same writable cache as the
# library, so it catches accidental damage and a stale build, not tampering:
# whoever can write the cache can rewrite the library and its STAMP together.
# What guards the source is the amalgamation zip's checksum, committed in
# sqlite3mc.sha256 and checked on every call; the cache itself is trusted.
mc_lib() {
	local target="$1" cc="$2"
	shift 2
	local extra="$*"
	local want
	want="$(awk -v f="$mc_zip" '$2 == f { print $1 }' "$here/sqlite3mc.sha256")"
	[ -n "$want" ] || { echo "no checksum for $mc_zip in sqlite3mc.sha256" >&2; exit 1; }
	mkdir -p "$cache/dl"
	local zip="$cache/dl/$mc_zip"
	if [ -n "${SQLITE3MC_ZIP_FILE:-}" ]; then
		cp "$SQLITE3MC_ZIP_FILE" "$zip.part"
		mv "$zip.part" "$zip"
	elif [ ! -f "$zip" ]; then
		echo "fetching $mc_url" >&2
		curl -fsSL --retry 3 -o "$zip.part" "$mc_url"
		mv "$zip.part" "$zip"
	fi
	local got
	got="$(sha256sum "$zip" | awk '{ print $1 }')"
	if [ "$got" != "$want" ]; then
		echo "checksum mismatch for $mc_zip: got $got, want $want" >&2
		rm -f "$zip"
		exit 1
	fi
	local out="$cache/$target"
	local cflags="-O2 -fPIC $mc_defines -ffile-prefix-map=$out/src=sqlite3mc $extra"
	# The compiler's first line, without a pipe (see check_linux).
	local ccv stamp
	ccv="$("$cc" --version 2>&1)"
	ccv="${ccv%%$'\n'*}"
	stamp="$(printf '%s\n' "$want" "$cflags" "$ccv")"
	if [ -f "$out/lib/libsqlite3.a" ] && [ -f "$out/STAMP" ]; then
		if [ "$(cat "$out/STAMP")" = "$stamp"$'\n'"libsqlite3.a $(lib_sum "$out/lib/libsqlite3.a")" ]; then
			return
		fi
		echo "the cached libsqlite3.a for $target does not match its stamp: rebuilding" >&2
	fi
	rm -rf "$out"
	mkdir -p "$out/src" "$out/include" "$out/lib"
	unzip -q -o "$zip" sqlite3mc_amalgamation.c sqlite3mc_amalgamation.h sqlite3.h sqlite3ext.h -d "$out/src"
	cp "$out/src/sqlite3.h" "$out/src/sqlite3ext.h" "$out/src/sqlite3mc_amalgamation.h" "$out/include/"
	echo "compiling sqlite3mc $mc_version (SQLite $mc_sqlite) for $target: $cc $cflags" >&2
	(cd "$out/src" && "$cc" $cflags -c sqlite3mc_amalgamation.c -o sqlite3mc.o)
	case "$target" in
	darwin-*) libtool -static -o "$out/lib/libsqlite3.a" "$out/src/sqlite3mc.o" ;;
	*) ar rcsD "$out/lib/libsqlite3.a" "$out/src/sqlite3mc.o" ;;
	esac
	rm -f "$out/src/sqlite3mc.o" "$out/src/sqlite3mc_amalgamation.c" "$out/src/shell3mc_amalgamation.c"
	printf '%s\nlibsqlite3.a %s' "$stamp" "$(lib_sum "$out/lib/libsqlite3.a")" >"$out/STAMP"
}

# lib_sum <file>: its SHA-256.
lib_sum() {
	local sum
	sum="$(sha256sum "$1")"
	printf '%s' "${sum%% *}"
}

# The Go environment of the encrypted build for one target. The library
# directory holds only libsqlite3.a, so mattn's `-lsqlite3` resolves to it
# and nothing dynamic; check_linux verifies that on the result.
enc_env() {
	local out="$cache/$1"
	export CGO_ENABLED=1
	export CGO_CFLAGS="-O2 -I$out/include"
	export CGO_LDFLAGS="-L$out/lib -lm"
	export GOFLAGS="${GOFLAGS:+$GOFLAGS }-tags=libsqlite3"
}

# check_linux refuses a binary that names a shared SQLite. readelf's output
# is captured first and then tested: under pipefail, `readelf | grep -q`
# fails when grep exits at its first match and readelf dies of SIGPIPE, so
# such a check passes or fails by accident. A readelf that fails ends the
# build (set -e).
check_linux() {
	local bin="$1" dyn needed
	dyn="$(readelf -d "$bin")"
	needed="$(grep -i 'NEEDED' <<<"$dyn" || true)"
	if grep -qi sqlite <<<"$needed"; then
		echo "$bin links a shared SQLite: refusing" >&2
		printf '%s\n' "$needed" >&2
		exit 1
	fi
	echo "  needs: $(awk '/NEEDED/ { gsub(/[\[\]]/, "", $NF); printf "%s ", $NF }' <<<"$dyn")"
}

build() {
	local goos="$1" goarch="$2" out="$3" tags="$4"
	mkdir -p "$out"
	out="$(cd "$out" && pwd)"
	for w in $workers; do
		(cd "$mod" && GOOS="$goos" GOARCH="$goarch" \
			go build -trimpath -tags "$tags" -ldflags "$ldflags" -o "$out/$w" "./cmd/$w")
		echo "built $out/$w ($(stat -c %s "$out/$w" 2>/dev/null || stat -f %z "$out/$w") bytes)"
		if [ "$goos" = linux ]; then check_linux "$out/$w"; fi
	done
}

cmd="${1:-build}"
shift || true
case "$cmd" in
lib)
	mc_lib linux-amd64 "${CC:-gcc}"
	ls -l "$cache/linux-amd64/lib/libsqlite3.a"
	sha256sum "$cache/linux-amd64/lib/libsqlite3.a"
	;;
build)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	build linux amd64 "${1:-$here/bin}" libsqlite3
	;;
plain)
	echo "DEVELOPMENT BUILD: plain SQLite, not for shipping" >&2
	export CGO_ENABLED=1
	build linux amd64 "${1:-$here/bin-plain}" plainstore
	;;
env)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	printf 'export CGO_ENABLED=1 CGO_CFLAGS=%q CGO_LDFLAGS=%q GOFLAGS=%q\n' "$CGO_CFLAGS" "$CGO_LDFLAGS" "$GOFLAGS"
	;;
vet)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	(cd "$mod" && go vet ./...)
	;;
test)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	(cd "$mod" && go test ./... -count=1 "$@")
	;;
darwin)
	# UNTESTED: written for a Mac (Apple clang, the macOS SDK); no macOS SDK
	# exists on the Linux build host. The same verified amalgamation and
	# options, compiled for arm64 at the deployment target, archived with
	# libtool, and found before mattn's hard-coded -L/opt/homebrew/opt/sqlite/lib
	# because CGO_LDFLAGS puts our -L first; otool -L then refuses any
	# libsqlite3 dylib in the result.
	[ "$(uname -s)" = Darwin ] || { echo "darwin builds run on a Mac" >&2; exit 2; }
	export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-13.0}"
	mc_lib darwin-arm64 "${CC:-clang}" -arch arm64 "-mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
	enc_env darwin-arm64
	export CC="${CC:-clang}" CGO_CFLAGS="$CGO_CFLAGS -arch arm64 -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
	export CGO_LDFLAGS="$CGO_LDFLAGS -arch arm64 -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
	out="${1:-$here/bin-darwin-arm64}"
	build darwin arm64 "$out" libsqlite3
	for w in $workers; do
		deps="$(otool -L "$out/$w")"
		if grep -qi sqlite <<<"$deps"; then
			echo "$out/$w links a shared SQLite: refusing" >&2
			exit 1
		fi
	done
	;;
*)
	echo "usage: $0 build|lib|vet|test|env|plain|darwin [args]" >&2
	exit 2
	;;
esac

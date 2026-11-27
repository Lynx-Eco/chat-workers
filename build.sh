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
#   ./build.sh libsignal              fetch libsignal at the commit pinned in
#                                     libsignal.pin and build libsignal_ffi.a
#                                     (Rust; slow) for chat_signal
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
#
# chat_signal also links libsignal_ffi.a (signalapp/libsignal, Rust, with
# BoringSSL), at the version libsignal.pin names, which must be the one the
# connector's libsignalgo is generated for (checked on every call). The
# library is taken from LYNX_LIBSIGNAL_DIR (a directory holding
# libsignal_ffi.a and no shared libsignal_ffi), else from what
# `./build.sh libsignal` left in LIBSIGNAL_CACHE (default go/.libsignal;
# LIBSIGNAL_SRC uses a checkout you already have, verified at the pinned
# commit). Without either, build, plain, test and env leave chat_signal out
# and say so in one line on stderr (vet needs no library and always checks
# it); LYNX_REQUIRE_SIGNAL=1 makes that an error.
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
# The worker binaries (cmd/<name>). chat_signal is added when its library is
# found (find_libsignal).
workers="chat_fakenet chat_telegram chat_whatsapp chat_slack chat_instagram chat_messenger chat_gmessages chat_gvoice"

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
	if grep -qi signal <<<"$needed"; then
		echo "$bin links a shared libsignal: refusing" >&2
		printf '%s\n' "$needed" >&2
		exit 1
	fi
	echo "  needs: $(awk '/NEEDED/ { gsub(/[\[\]]/, "", $NF); printf "%s ", $NF }' <<<"$dyn")"
}

# ---- the pinned libsignal (chat_signal only) ----
# pin_get <key>: a value of libsignal.pin ("<key> <value>" lines).
pin_get() { awk -v k="$1" '$1 == k { print $2 }' "$here/libsignal.pin"; }
ls_repo="$(pin_get repo)"
ls_version="$(pin_get version)"
ls_commit="$(pin_get commit)"
[ -n "$ls_repo" ] && [ -n "$ls_version" ] && [ ${#ls_commit} -eq 40 ] || {
	echo "libsignal.pin must name repo, version and a 40-hex commit" >&2
	exit 1
}
ls_cache="${LIBSIGNAL_CACHE:-$here/.libsignal}"

# check_libsignal_pin refuses to go on when the connector's libsignalgo is
# generated for another libsignal than libsignal.pin names (its
# signalversion.Version, which the connector's update-ffi.sh writes), so a
# connector bump cannot pass with the old pin. The module is read from the
# module cache (go mod download fetches it only when it is not there, as
# any build would).
check_libsignal_pin() {
	local json dir v
	json="$(cd "$mod" && go mod download -json go.mau.fi/mautrix-signal)"
	dir="$(sed -n 's/^[[:space:]]*"Dir": "\(.*\)",\{0,1\}$/\1/p' <<<"$json")"
	[ -n "$dir" ] || { echo "go.mau.fi/mautrix-signal is not in the module cache" >&2; exit 1; }
	v="$(sed -n 's/^const Version = "\(.*\)"$/\1/p' "$dir/pkg/libsignalgo/signalversion/version.go")"
	if [ "$v" != "$ls_version" ]; then
		echo "libsignal.pin names libsignal $ls_version, but go.mau.fi/mautrix-signal in go.mod is generated for ${v:-an unknown version}: move libsignal.pin (version and commit) with the connector" >&2
		exit 1
	fi
}

# ls_stamp_ok <dir>: <dir>/libsignal_ffi.a is the one `./build.sh libsignal`
# built at the pinned commit and version (its STAMP names them and ends
# with the library's SHA-256). Like the SQLite STAMP, it catches a stale or
# damaged build in a trusted cache, not tampering.
ls_stamp_ok() {
	local d="$1"
	[ -f "$d/libsignal_ffi.a" ] && [ -f "$d/STAMP" ] || return 1
	[ "$(sed -n 1p "$d/STAMP")" = "commit $ls_commit" ] &&
		[ "$(sed -n 2p "$d/STAMP")" = "version $ls_version" ] &&
		[ "$(tail -n 1 "$d/STAMP")" = "libsignal_ffi.a $(lib_sum "$d/libsignal_ffi.a")" ]
}

# ls_build fetches libsignal at the pinned commit into $ls_cache/src (or
# takes LIBSIGNAL_SRC), refuses a checkout at another commit or with local
# changes, and builds libsignal_ffi.a as mautrix-signal's build-rust.sh does
# (cargo build -p libsignal-ffi --profile=release, -crt-static off), with
# Cargo.lock as it is (--locked), into $ls_cache/lib with a STAMP. Nothing
# is fetched or built when the stamped library is there; nothing is fetched
# when the checkout is already at the commit (cargo still needs its crates:
# from the network, or from CARGO_HOME with CARGO_NET_OFFLINE=true).
# Needs git, cargo and rustc (the checkout's rust-toolchain file names the
# version rustup picks; RUSTUP_TOOLCHAIN overrides it), cmake, clang and a
# C++ compiler (BoringSSL, bindgen), and protoc with protobuf's well-known
# .proto files (Debian: protobuf-compiler and libprotobuf-dev; PROTOC names
# another protoc). About 12 minutes of CPU on the build host at -j 3.
ls_build() {
	local out="$ls_cache/lib" src="${LIBSIGNAL_SRC:-$ls_cache/src}"
	mkdir -p "$ls_cache"
	if ls_stamp_ok "$out"; then
		echo "libsignal_ffi.a $ls_version is built: $out" >&2
		return
	fi
	if [ -z "${LIBSIGNAL_SRC:-}" ] && [ "$(git -C "$src" rev-parse -q --verify HEAD 2>/dev/null || true)" != "$ls_commit" ]; then
		echo "fetching $ls_repo at $ls_commit ($ls_version)" >&2
		rm -rf "$src"
		git init -q "$src"
		git -C "$src" fetch -q --depth 1 "$ls_repo" "$ls_commit"
		git -C "$src" checkout -q --detach FETCH_HEAD
	fi
	local got
	got="$(git -C "$src" rev-parse HEAD)"
	if [ "$got" != "$ls_commit" ]; then
		echo "the libsignal checkout $src is at $got, not the pinned $ls_commit" >&2
		exit 1
	fi
	if [ -n "$(git -C "$src" status --porcelain --untracked-files=no)" ]; then
		echo "the libsignal checkout $src has local changes: refusing" >&2
		exit 1
	fi
	local rustv
	rustv="$(cd "$src" && rustc --version)"
	echo "building libsignal_ffi.a $ls_version with $rustv (slow)" >&2
	(cd "$src" && RUSTFLAGS="-Ctarget-feature=-crt-static" RUSTC_WRAPPER="" \
		CARGO_TARGET_DIR="$ls_cache/target" cargo build --locked -p libsignal-ffi --profile=release)
	mkdir -p "$out"
	cp "$ls_cache/target/release/libsignal_ffi.a" "$out/libsignal_ffi.a.part"
	mv "$out/libsignal_ffi.a.part" "$out/libsignal_ffi.a"
	printf 'commit %s\nversion %s\nrustc %s\nlibsignal_ffi.a %s\n' "$ls_commit" "$ls_version" "$rustv" \
		"$(lib_sum "$out/libsignal_ffi.a")" >"$out/STAMP"
}

# find_libsignal: sets sig_ldflags (the linker flags that find
# libsignal_ffi.a) and adds chat_signal to workers when the library is
# found; else sig_ldflags is empty and sig_why says why. With
# LYNX_REQUIRE_SIGNAL=1 a missing library ends the build.
sig_ldflags=""
sig_why=""
find_libsignal() {
	local dir=""
	check_libsignal_pin
	if [ -n "${LYNX_LIBSIGNAL_DIR:-}" ]; then
		if [ -f "$LYNX_LIBSIGNAL_DIR/libsignal_ffi.a" ]; then
			dir="$(cd "$LYNX_LIBSIGNAL_DIR" && pwd)"
		else
			sig_why="LYNX_LIBSIGNAL_DIR=$LYNX_LIBSIGNAL_DIR holds no libsignal_ffi.a"
		fi
	elif ls_stamp_ok "$ls_cache/lib"; then
		dir="$ls_cache/lib"
	elif [ -f "$ls_cache/lib/libsignal_ffi.a" ]; then
		sig_why="$ls_cache/lib/libsignal_ffi.a does not match its STAMP (./build.sh libsignal rebuilds it)"
	else
		sig_why="no libsignal_ffi.a: set LYNX_LIBSIGNAL_DIR or run ./build.sh libsignal"
	fi
	if [ -n "$dir" ]; then
		for f in "$dir"/libsignal_ffi.so "$dir"/libsignal_ffi.dylib; do
			if [ -e "$f" ]; then
				echo "$f is a shared libsignal_ffi beside the static one: the linker would take it; refusing" >&2
				exit 1
			fi
		done
		sig_ldflags="-L$dir"
		local z
		z="$(zlib_dir)" || exit 1
		[ -z "$z" ] || sig_ldflags="$sig_ldflags -L$z"
		workers="$workers chat_signal"
		return
	fi
	if [ "${LYNX_REQUIRE_SIGNAL:-}" = 1 ]; then
		echo "chat_signal is required (LYNX_REQUIRE_SIGNAL=1) but cannot be built: $sig_why" >&2
		exit 1
	fi
	echo "chat_signal skipped: $sig_why" >&2
}

# zlib_dir prints a directory to add to the linker's path so that
# libsignalgo's -lz resolves, or nothing when the linker finds libz itself.
# A system with zlib's runtime library but without its development link
# (Debian: zlib1g without zlib1g-dev) gets a directory in the cache holding
# libz.so -> that library. (The linker drops libz from the binary when
# nothing uses it: it is not among chat_signal's needed libraries.)
zlib_dir() {
	local cc="${CC:-gcc}" so1
	# macOS: the SDK's libz (libz.tbd) is always there.
	[ "$(uname -s)" != Darwin ] || return 0
	if [ "$("$cc" -print-file-name=libz.so)" != libz.so ] || [ "$("$cc" -print-file-name=libz.a)" != libz.a ]; then
		return 0
	fi
	so1="$("$cc" -print-file-name=libz.so.1)"
	if [ "$so1" = libz.so.1 ]; then
		echo "the linker finds no zlib (libz.so, libz.a or libz.so.1), which chat_signal's -lz needs: install zlib's development files (Debian: zlib1g-dev)" >&2
		return 1
	fi
	mkdir -p "$ls_cache/zlib"
	ln -sfn "$so1" "$ls_cache/zlib/libz.so"
	(cd "$ls_cache/zlib" && pwd)
}

# sig_env adds the libsignal flags to the encrypted build's environment.
sig_env() {
	if [ -n "$sig_ldflags" ]; then
		export CGO_LDFLAGS="${CGO_LDFLAGS:-} $sig_ldflags"
	fi
}

# go_pkgs prints the packages a test run covers: all of them, without
# cmd/chat_signal when its library was not found (its tests build the
# binary, which needs it).
go_pkgs() {
	if [ -n "$sig_ldflags" ]; then
		echo ./...
		return
	fi
	(cd "$mod" && go list ./...) | grep -v '/cmd/chat_signal$'
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
libsignal)
	check_libsignal_pin
	ls_build
	ls -l "$ls_cache/lib/libsignal_ffi.a"
	cat "$ls_cache/lib/STAMP"
	;;
build)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	find_libsignal
	sig_env
	build linux amd64 "${1:-$here/bin}" libsqlite3
	;;
plain)
	echo "DEVELOPMENT BUILD: plain SQLite, not for shipping" >&2
	export CGO_ENABLED=1
	find_libsignal
	sig_env
	build linux amd64 "${1:-$here/bin-plain}" plainstore
	;;
env)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	find_libsignal
	sig_env
	printf 'export CGO_ENABLED=1 CGO_CFLAGS=%q CGO_LDFLAGS=%q GOFLAGS=%q\n' "$CGO_CFLAGS" "$CGO_LDFLAGS" "$GOFLAGS"
	;;
vet)
	# vet compiles chat_signal's cgo without linking it: no library needed.
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	check_libsignal_pin
	(cd "$mod" && go vet ./...)
	;;
test)
	mc_lib linux-amd64 "${CC:-gcc}"
	enc_env linux-amd64
	find_libsignal
	sig_env
	# shellcheck disable=SC2046 # one package path per word
	(cd "$mod" && go test $(go_pkgs) -count=1 "$@")
	;;
darwin)
	# UNTESTED: written for a Mac (Apple clang, the macOS SDK); no macOS SDK
	# exists on the Linux build host. The same verified amalgamation and
	# options, compiled for arm64 at the deployment target, archived with
	# libtool, and found before mattn's hard-coded -L/opt/homebrew/opt/sqlite/lib
	# because CGO_LDFLAGS puts our -L first; otool -L then refuses any
	# libsqlite3 dylib in the result.
	# chat_signal, also UNTESTED: built only from a libsignal_ffi.a made on
	# a Mac for arm64 (cargo build -p libsignal-ffi --profile=release with
	# the same MACOSX_DEPLOYMENT_TARGET, as mau.dev's "libsignal macos
	# arm64" job does; `./build.sh libsignal` run on the Mac, or
	# LYNX_LIBSIGNAL_DIR); nothing cross-compiles it from Linux. Its -lz
	# and -lstdc++ resolve against the SDK's libz and libc++'s compatibility
	# stub (not checked); otool -L refuses a libsignal dylib in the result.
	[ "$(uname -s)" = Darwin ] || { echo "darwin builds run on a Mac" >&2; exit 2; }
	export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-13.0}"
	mc_lib darwin-arm64 "${CC:-clang}" -arch arm64 "-mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
	enc_env darwin-arm64
	export CC="${CC:-clang}" CGO_CFLAGS="$CGO_CFLAGS -arch arm64 -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
	export CGO_LDFLAGS="$CGO_LDFLAGS -arch arm64 -mmacosx-version-min=$MACOSX_DEPLOYMENT_TARGET"
	find_libsignal
	sig_env
	out="${1:-$here/bin-darwin-arm64}"
	build darwin arm64 "$out" libsqlite3
	for w in $workers; do
		deps="$(otool -L "$out/$w")"
		if grep -qi sqlite <<<"$deps"; then
			echo "$out/$w links a shared SQLite: refusing" >&2
			exit 1
		fi
		if grep -qi libsignal <<<"$deps"; then
			echo "$out/$w links a shared libsignal: refusing" >&2
			exit 1
		fi
	done
	;;
*)
	echo "usage: $0 build|lib|libsignal|vet|test|env|plain|darwin [args]" >&2
	exit 2
	;;
esac

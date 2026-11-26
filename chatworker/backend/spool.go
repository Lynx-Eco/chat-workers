// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"lynx.invalid/ingest/chatworker/store"
)

// The media spool keeps bytes the connector pushed (uploads, avatars) in
// <state_dir>/media/, encrypted at rest. Each file is
//
//	magic "LXSPOOL2" (8) | salt (32, random per file) | chunks
//
// and each chunk is AES-256-GCM over at most spoolChunk plaintext bytes. The
// file's key is HKDF-SHA256(spool key, salt, "lynx chatworker spool v2 file");
// the spool key is HKDF-SHA256(store key, no salt, spoolKeyInfo). The nonce
// of chunk i is i as 11 big-endian bytes and a last byte that is 1 on the
// final chunk only (the STREAM construction), and every chunk authenticates
// the header as additional data, so chunks cannot be reordered, dropped,
// truncated at a chunk boundary, or moved between files (each file has its
// own key). A file's name is HMAC-SHA256(name key, sha256 of the plaintext),
// so the directory does not reveal digests of known content either.
//
// The file is bound to its name: the final chunk also authenticates the name
// the file is stored under (header | name as additional data). The name is
// known only once every byte has been read, which is when the final chunk is
// sealed; the other chunks are bound to the final one through the file's own
// key. A file renamed to another's name (or copied over it) therefore does
// not decrypt as that other content: open checks the final chunk before any
// byte is read, and put does before it keeps a file already there.
//
// Version 2 (the name binding) reads no version 1 file: the spool is a
// cache, so an old file is a miss (the names changed with the name key's
// info, so old files are not found and are removed at the next eviction; one
// found anyway does not pass open's check).
const (
	spoolMagic     = "LXSPOOL2"
	spoolSaltLen   = 32
	spoolChunk     = 64 << 10
	spoolKeyInfo   = "lynx chatworker spool v1 key"
	spoolNameInfo  = "lynx chatworker spool v2 names"
	spoolFileInfo  = "lynx chatworker spool v2 file"
	spoolHeaderLen = len(spoolMagic) + spoolSaltLen
	spoolSealed    = spoolChunk + 16 // a full chunk with its tag

	// SpoolBudget is the default byte budget of the spool (settings:
	// spool_budget_bytes). When it is exceeded, the oldest files go first;
	// a file being read is never evicted, nor the one just written.
	SpoolBudget = 256 << 20
)

// errSpoolCorrupt: a spooled file does not decrypt (damaged, renamed, of an
// older version, or of another key). It is a miss (errMediaUnknown): the
// host is answered not_found.
var errSpoolCorrupt = fmt.Errorf("spooled file does not decrypt: %w", errMediaUnknown)

type spool struct {
	dir     string
	key     []byte
	nameKey []byte
	budget  int64

	putMu sync.Mutex
	mu    sync.Mutex
	inUse map[string]int
}

func newSpool(dir string, storeKey []byte, budget int64) (*spool, error) {
	k, err := hkdf.Key(sha256.New, storeKey, nil, spoolKeyInfo, 32)
	if err != nil {
		return nil, err
	}
	nk, err := hkdf.Key(sha256.New, storeKey, nil, spoolNameInfo, 32)
	if err != nil {
		return nil, err
	}
	if budget <= 0 {
		budget = SpoolBudget
	}
	return &spool{dir: dir, key: k, nameKey: nk, budget: budget, inUse: map[string]int{}}, nil
}

// name is the file name of the spooled bytes with digest sha.
func (s *spool) name(sha string) string {
	m := hmac.New(sha256.New, s.nameKey)
	m.Write([]byte(sha))
	return hex.EncodeToString(m.Sum(nil))
}

func (s *spool) fileAEAD(salt []byte) (cipher.AEAD, error) {
	fk, err := hkdf.Key(sha256.New, s.key, salt, spoolFileInfo, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(fk)
	clear(fk)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// finalAD is the additional data of a file's final chunk: its header and the
// name it is stored under.
func finalAD(head []byte, name string) []byte {
	return append(bytes.Clone(head), name...)
}

func chunkNonce(nonce []byte, i uint64, final bool) {
	clear(nonce)
	binary.BigEndian.PutUint64(nonce[3:11], i)
	if final {
		nonce[11] = 1
	}
}

// sealWriter encrypts what is written to it into w. Close writes the final
// chunk (empty when the plaintext is empty).
type sealWriter struct {
	w     io.Writer
	aead  cipher.AEAD
	ad    []byte
	buf   []byte
	out   []byte
	nonce []byte
	i     uint64
}

func (s *spool) newSealWriter(w io.Writer) (*sealWriter, error) {
	head := make([]byte, spoolHeaderLen)
	copy(head, spoolMagic)
	if _, err := rand.Read(head[len(spoolMagic):]); err != nil {
		return nil, err
	}
	aead, err := s.fileAEAD(head[len(spoolMagic):])
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(head); err != nil {
		return nil, err
	}
	return &sealWriter{w: w, aead: aead, ad: head, buf: make([]byte, 0, spoolChunk),
		out: make([]byte, 0, spoolChunk+16), nonce: make([]byte, 12)}, nil
}

func (sw *sealWriter) seal(final bool, ad []byte) error {
	chunkNonce(sw.nonce, sw.i, final)
	sw.out = sw.aead.Seal(sw.out[:0], sw.nonce, sw.buf, ad)
	clear(sw.buf)
	sw.buf = sw.buf[:0]
	sw.i++
	_, err := sw.w.Write(sw.out)
	return err
}

func (sw *sealWriter) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		// A full chunk is sealed only once more bytes follow, so that the
		// last chunk is always the one marked final.
		if len(sw.buf) == spoolChunk {
			if err := sw.seal(false, sw.ad); err != nil {
				return n, err
			}
		}
		c := copy(sw.buf[len(sw.buf):spoolChunk], p)
		sw.buf = sw.buf[:len(sw.buf)+c]
		p = p[c:]
		n += c
	}
	return n, nil
}

// finish writes the final chunk, bound to name, the name the file will be
// stored under.
func (sw *sealWriter) finish(name string) error { return sw.seal(true, finalAD(sw.ad, name)) }

// openReader decrypts a spool file read from r.
type openReader struct {
	r       *bufio.Reader
	aead    cipher.AEAD
	ad      []byte
	finalAD []byte
	in      []byte
	plain   []byte
	nonce   []byte
	i       uint64
	done    bool
}

// newOpenReader decrypts the spool file f, stored under name. It first
// checks the file's final chunk, which authenticates the name, so a file
// that is not the one stored under name fails before any byte is read.
func (s *spool) newOpenReader(f *os.File, name string) (*openReader, error) {
	head := make([]byte, spoolHeaderLen)
	if _, err := io.ReadFull(f, head); err != nil || string(head[:len(spoolMagic)]) != spoolMagic {
		return nil, errSpoolCorrupt
	}
	aead, err := s.fileAEAD(head[len(spoolMagic):])
	if err != nil {
		return nil, err
	}
	fad := finalAD(head, name)
	if err := checkFinal(f, aead, fad); err != nil {
		return nil, err
	}
	if _, err := f.Seek(int64(spoolHeaderLen), io.SeekStart); err != nil {
		return nil, err
	}
	return &openReader{r: bufio.NewReaderSize(f, spoolSealed), aead: aead, ad: head, finalAD: fad,
		in: make([]byte, spoolSealed), nonce: make([]byte, 12)}, nil
}

// checkFinal authenticates the final chunk of f (found from the file's size:
// every chunk before it is full) with the additional data ad.
func checkFinal(f *os.File, aead cipher.AEAD, ad []byte) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	return checkFinalAt(f, info.Size(), aead, ad)
}

// checkFinalAt is checkFinal on r, whose size was size when it was looked
// at. A file that is shorter when its final chunk is read (it shrank after
// the Stat) is corrupt, as any other spool file that cannot be read.
func checkFinalAt(r io.ReaderAt, size int64, aead cipher.AEAD, ad []byte) error {
	const sealed = int64(spoolSealed)
	body := size - int64(spoolHeaderLen)
	if body < 16 {
		return errSpoolCorrupt
	}
	n := (body + sealed - 1) / sealed
	last := body - (n-1)*sealed
	if last < 16 {
		return errSpoolCorrupt
	}
	buf := make([]byte, last)
	if _, err := r.ReadAt(buf, int64(spoolHeaderLen)+(n-1)*sealed); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errSpoolCorrupt
		}
		return err
	}
	nonce := make([]byte, 12)
	chunkNonce(nonce, uint64(n-1), true)
	plain, err := aead.Open(buf[:0], nonce, buf, ad)
	clear(plain)
	if err != nil {
		return errSpoolCorrupt
	}
	return nil
}

// valid tells whether the file at path is a spool file stored under name
// that this spool's key decrypts (its final chunk; see newOpenReader).
func (s *spool) valid(path, name string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	_, err = s.newOpenReader(f, name)
	return err == nil
}

func (or *openReader) Read(p []byte) (int, error) {
	for len(or.plain) == 0 {
		if or.done {
			return 0, io.EOF
		}
		n, err := io.ReadFull(or.r, or.in)
		final := false
		switch {
		case err == nil:
			// A full chunk is final only when nothing follows it.
			if _, perr := or.r.Peek(1); perr == io.EOF {
				final = true
			}
		case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
			final = true
		default:
			return 0, err
		}
		if n < 16 {
			return 0, errSpoolCorrupt
		}
		chunkNonce(or.nonce, or.i, final)
		ad := or.ad
		if final {
			ad = or.finalAD
		}
		plain, oerr := or.aead.Open(or.in[:0], or.nonce, or.in[:n], ad)
		if oerr != nil {
			return 0, errSpoolCorrupt
		}
		or.i++
		or.plain = plain
		or.done = final
	}
	c := copy(p, or.plain)
	or.plain = or.plain[c:]
	return c, nil
}

// put encrypts src into the spool, names it by the digest of its bytes,
// records it, and evicts down to the budget. Puts are serialized, so an
// eviction never sees a file without its row. Bytes already spooled are
// only marked as recent.
//
// The row is written through write, the epoch guard of the framework
// instance the bytes came from (instance.write). When it refuses (the store
// was wiped since the instance began: store.ErrWiped), a file this put
// renamed into the spool is removed again and nothing is recorded.
func (s *spool) put(ctx context.Context, st *store.Store, write func(func() error) error, src io.Reader, fileName, mimeType string) (string, error) {
	s.putMu.Lock()
	defer s.putMu.Unlock()
	f, err := tempIn(s.dir, ".spool-*")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	h := sha256.New()
	sw, err := s.newSealWriter(f)
	var size int64
	if err == nil {
		size, err = io.Copy(sw, io.TeeReader(src, h))
	}
	sha := hex.EncodeToString(h.Sum(nil))
	name := s.name(sha)
	if err == nil {
		err = sw.finish(name)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	path := filepath.Join(s.dir, name)
	// A file already there is kept only when it is this content's: one
	// that does not decrypt as such (damaged, renamed, of an older version)
	// is replaced.
	renamed := false
	if s.valid(path, name) {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
	} else if err := os.Rename(tmp, path); err != nil {
		return "", err
	} else {
		renamed = true
	}
	err = write(func() error {
		return st.PutSpool(ctx, store.SpoolEntry{SHA256: sha, MimeType: mimeType, FileName: fileName, Size: size})
	})
	if errors.Is(err, store.ErrWiped) && renamed {
		_ = os.Remove(path)
	}
	if err != nil {
		return "", err
	}
	if err := s.evict(ctx, st, path); err != nil {
		return "", err
	}
	return sha, nil
}

// open returns the decrypted bytes spooled under sha; release must be called
// when done. While open, the file is not evicted.
func (s *spool) open(sha string) (io.Reader, func(), error) {
	name := s.name(sha)
	s.mu.Lock()
	s.inUse[name]++
	s.mu.Unlock()
	release := func() {
		s.mu.Lock()
		if s.inUse[name]--; s.inUse[name] <= 0 {
			delete(s.inUse, name)
		}
		s.mu.Unlock()
	}
	f, err := os.Open(filepath.Join(s.dir, name))
	if err != nil {
		release()
		return nil, nil, errMediaUnknown
	}
	r, err := s.newOpenReader(f, name)
	if err != nil {
		_ = f.Close()
		release()
		return nil, nil, err
	}
	return r, func() { _ = f.Close(); release() }, nil
}

type spoolFile struct {
	path, sha string
	size      int64
	mtime     time.Time
}

// evict removes the oldest spooled files (by modification time) until the
// spool is within its budget, never a file in use or keep. Files no row
// names cannot be served and are removed; rows whose file is gone are
// dropped.
func (s *spool) evict(ctx context.Context, st *store.Store, keep string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := st.ListSpool(ctx)
	if err != nil {
		return err
	}
	byName := make(map[string]string, len(rows))
	for _, sha := range rows {
		byName[s.name(sha)] = sha
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var files []spoolFile
	var total int64
	seen := map[string]bool{}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") || !e.Type().IsRegular() {
			continue
		}
		path := filepath.Join(s.dir, e.Name())
		sha, known := byName[e.Name()]
		if !known {
			if s.inUse[e.Name()] == 0 && path != keep {
				_ = os.Remove(path)
			}
			continue
		}
		seen[e.Name()] = true
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, spoolFile{path: path, sha: sha, size: info.Size(), mtime: info.ModTime()})
		total += info.Size()
	}
	for name, sha := range byName {
		if !seen[name] && s.inUse[name] == 0 && filepath.Join(s.dir, name) != keep {
			_ = st.DeleteSpool(ctx, sha)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime.Before(files[j].mtime) })
	for _, f := range files {
		if total <= s.budget {
			break
		}
		if f.path == keep || s.inUse[filepath.Base(f.path)] > 0 {
			continue
		}
		if err := os.Remove(f.path); err != nil {
			continue
		}
		_ = st.DeleteSpool(ctx, f.sha)
		total -= f.size
	}
	return nil
}

// sweep removes what a crash can leave behind: spool files being written,
// scratch files, and partial transfers.
func sweep(mediaDir, tmpDir, transferDir string) {
	for _, d := range []struct{ dir, prefix string }{{mediaDir, ".spool-"}, {transferDir, ".partial-"}} {
		entries, _ := os.ReadDir(d.dir)
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), d.prefix) {
				_ = os.Remove(filepath.Join(d.dir, e.Name()))
			}
		}
	}
	_ = os.RemoveAll(tmpDir)
}

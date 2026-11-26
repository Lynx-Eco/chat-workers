// SPDX-License-Identifier: AGPL-3.0-or-later

package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/mediaproxy"

	"lynx.invalid/ingest/chatworker/contract"
)

var (
	errTooLarge     = errors.New("too large")
	errNoDirect     = errors.New("connector has no direct media")
	errBadPath      = errors.New("path outside the state directory")
	errMediaUnknown = errors.New("media not known")
)

// cappedWriter fails once more than max bytes were written (max 0: no cap),
// so a cap is enforced while streaming, never after.
type cappedWriter struct {
	w   io.Writer
	max uint64
	n   uint64
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if c.max > 0 && c.n+uint64(len(p)) > c.max {
		return 0, errTooLarge
	}
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

// tempIn makes a temporary file in dir (never /tmp: it is small and in
// memory on the hosts, and media may be large).
func tempIn(dir, pattern string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, pattern)
}

// spoolBytes keeps bytes the connector pushed (an avatar, media of a network
// without direct media) in the encrypted spool and returns their URI.
func (fw *instance) spoolBytes(ctx context.Context, data []byte, fileName, mimeType string) (id.ContentURIString, error) {
	sha, err := fw.spool.put(ctx, fw.st, fw.write, bytes.NewReader(data), fileName, mimeType)
	if err != nil {
		return "", err
	}
	return spoolURI(sha), nil
}

// spoolStream is spoolBytes for UploadMediaStream: the connector writes into
// a scratch file (the callback needs a file), which is encrypted into the
// spool and removed.
func (fw *instance) spoolStream(ctx context.Context, cb bridgev2.FileStreamCallback) (id.ContentURIString, error) {
	f, err := tempIn(filepath.Join(fw.stateDir, "tmp"), "upload-*")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	res, err := cb(f)
	if cerr := f.Close(); err == nil && cerr != nil {
		err = cerr
	}
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", errors.New("upload callback returned no result")
	}
	src := tmp
	if res.ReplacementFile != "" {
		src = res.ReplacementFile
		defer func() { _ = os.Remove(res.ReplacementFile) }()
	}
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	sha, err := fw.spool.put(ctx, fw.st, fw.write, in, res.FileName, res.MimeType)
	_ = in.Close()
	if err != nil {
		return "", err
	}
	return spoolURI(sha), nil
}

// insideStateDir resolves a path the host gave (absolute or relative to the
// state directory) and refuses anything outside it.
func (b *Backend) insideStateDir(p string) (string, error) {
	if p == "" {
		return "", errBadPath
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(b.stateDir, p)
	}
	p = filepath.Clean(p)
	root := filepath.Clean(b.stateDir) + string(filepath.Separator)
	if !strings.HasPrefix(p, root) {
		return "", errBadPath
	}
	return p, nil
}

// copyMedia writes the bytes behind a media URI to w and returns their MIME
// type (when known).
func (b *Backend) copyMedia(ctx context.Context, kind mediaKind, val []byte, w io.Writer) (string, error) {
	switch kind {
	case mediaKindSpool:
		entry, err := b.st.GetSpool(ctx, string(val))
		if err != nil {
			return "", err
		}
		if entry == nil {
			return "", errMediaUnknown
		}
		r, release, err := b.spool.open(entry.SHA256)
		if err == nil {
			defer release()
			_, err = io.Copy(w, r)
		}
		if errors.Is(err, errSpoolCorrupt) {
			// A miss (not_found), not a failure: the spool is a cache.
			b.log.Warn("spool_unreadable")
		}
		return entry.MimeType, err
	case mediaKindOutgoing:
		path, err := b.insideStateDir(string(val))
		if err != nil {
			return "", err
		}
		f, err := os.Open(path)
		if err != nil {
			return "", err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return "", err
	case mediaKindDirect:
		return b.fetchDirect(ctx, networkid.MediaID(val), w)
	}
	return "", errMediaUnknown
}

// fetchDirect downloads direct media from the connector, handling every
// response form the framework defines.
func (b *Backend) fetchDirect(ctx context.Context, mediaID networkid.MediaID, w io.Writer) (string, error) {
	dmn, ok := innerNetwork(b.bridge().Network).(bridgev2.DirectMediableNetwork)
	if !ok {
		return "", errNoDirect
	}
	resp, err := dmn.Download(ctx, mediaID, nil)
	if err != nil {
		return "", err
	}
	switch r := resp.(type) {
	case *mediaproxy.GetMediaResponseData:
		defer r.Reader.Close()
		_, err := io.Copy(w, r.Reader)
		return r.ContentType, err
	case *mediaproxy.GetMediaResponseCallback:
		_, err := r.Callback(w)
		return r.ContentType, err
	case *mediaproxy.GetMediaResponseFile:
		f, err := tempIn(filepath.Join(b.stateDir, "tmp"), "direct-*")
		if err != nil {
			return "", err
		}
		defer func() { _ = os.Remove(f.Name()) }()
		meta, err := r.Callback(f)
		if err != nil {
			_ = f.Close()
			return "", err
		}
		src := f
		if meta != nil && meta.ReplacementFile != "" {
			_ = f.Close()
			rf, err := os.Open(meta.ReplacementFile)
			if err != nil {
				return "", err
			}
			defer func() { _ = os.Remove(meta.ReplacementFile) }()
			src = rf
		} else if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return "", err
		}
		defer src.Close()
		_, err = io.Copy(w, src)
		mime := ""
		if meta != nil {
			mime = meta.ContentType
		}
		return mime, err
	case *mediaproxy.GetMediaResponseURL:
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
		if err != nil {
			return "", err
		}
		client := &http.Client{Timeout: 5 * time.Minute}
		res, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return "", fmt.Errorf("media URL answered %d", res.StatusCode)
		}
		_, err = io.Copy(w, res.Body)
		return res.Header.Get("Content-Type"), err
	}
	return "", errMediaUnknown
}

func (b *Backend) downloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
	kind, val, err := parseMediaURI(uri)
	if err != nil {
		return nil, errNotOurs
	}
	var buf strings.Builder
	if _, err := b.copyMedia(ctx, kind, val, &buf); err != nil {
		return nil, err
	}
	return []byte(buf.String()), nil
}

func (b *Backend) downloadMediaToFile(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo, writable bool, callback func(*os.File) error) error {
	kind, val, err := parseMediaURI(uri)
	if err != nil {
		return errNotOurs
	}
	f, err := tempIn(filepath.Join(b.stateDir, "tmp"), "download-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	if _, err := b.copyMedia(ctx, kind, val, f); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return callback(f)
}

// safeTargetName is a file name inside the transfer directory: no separators,
// no "." or "..".
func safeTargetName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00")
}

// fetchAttachment resolves a handle and writes its bytes to
// <state_dir>/transfer/<target_name>, enforcing the cap while streaming.
func (b *Backend) fetchAttachment(ctx context.Context, cmdID string, f *contract.FetchAttachment) *contract.CommandReply {
	if !safeTargetName(f.GetTargetName()) {
		return rejected(cmdID, "bad_target")
	}
	kind, val, err := parseHandle(f.GetHandle())
	if err != nil {
		return rejected(cmdID, "not_found")
	}
	// The host asked for plaintext bytes here: a transfer file is the one
	// place the worker writes them on purpose. It is created 0600 under a
	// temporary name and renamed only when complete, so a refused or failed
	// transfer leaves nothing.
	tmp, err := tempIn(b.transferDir, ".partial-*")
	if err != nil {
		b.log.Error("transfer_dir_failed")
		return rejected(cmdID, "io_error")
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return rejected(cmdID, "io_error")
	}
	h := sha256.New()
	cw := &cappedWriter{w: io.MultiWriter(tmp, h), max: f.GetMaxBytes()}
	mime, err := b.copyMedia(ctx, kind, val, cw)
	cerr := tmp.Close()
	switch {
	case errors.Is(err, errTooLarge):
		return rejected(cmdID, "too_large")
	case errors.Is(err, errMediaUnknown), errors.Is(err, errNoDirect):
		return rejected(cmdID, "not_found")
	case err != nil:
		b.log.Warn("attachment_fetch_failed")
		return rejected(cmdID, "fetch_failed")
	case cerr != nil:
		return rejected(cmdID, "io_error")
	}
	if err := os.Rename(tmp.Name(), filepath.Join(b.transferDir, f.GetTargetName())); err != nil {
		return rejected(cmdID, "io_error")
	}
	return &contract.CommandReply{
		CommandId:    cmdID,
		Outcome:      contract.Outcome_OUTCOME_OK,
		BytesWritten: cw.n,
		Sha256Hex:    hexSum(h),
		MimeType:     mime,
	}
}

func hexSum(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

// wipeMedia removes the media, transfer and scratch directories.
func (b *Backend) wipeMedia() error {
	return errors.Join(
		os.RemoveAll(b.mediaDir),
		os.RemoveAll(b.transferDir),
		os.RemoveAll(filepath.Join(b.stateDir, "tmp")),
	)
}

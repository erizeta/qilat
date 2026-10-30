// Parts of this file are adapted from Go's net/http/fs.go.
// Copyright 2009 The Go Authors. All rights reserved. Use of that source code is governed by a
// BSD-style license that can be found in the LICENSE-GO file.

package qilat

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// FileServer returns a handler that serves files from fsys, like [net/http.FileServerFS]: Range requests,
// conditional requests, Content-Type detection, directory listings, and index.html. Paths cannot escape fsys.
// Files that implement [io.Seeker] support ranges; an [*os.File] is sent with sendfile on HTTP/1.1.
func FileServer(fsys fs.FS) Handler {
	return fileHandler{ioFS{fsys}}
}

type fileHandler struct{ root fileSystem }

func (f fileHandler) ServeHTTP(w ResponseWriter, r *Request) {
	upath := string(r.Path())
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
	}
	serveFile(w, r, upath, f.root, path.Clean(upath), true)
}

// ServeFile replies with the named file or directory from the operating system, like [net/http.ServeFile].
// Requests whose path contains a ".." element are rejected, in case name was built from the path.
func ServeFile(w ResponseWriter, r *Request, name string) {
	upath := string(r.Path())
	if containsDotDot(upath) {
		serveError(w, "invalid URL path", 400)
		return
	}
	dir, file := filepath.Split(name)
	serveFile(w, r, upath, osDir(dir), file, false)
}

// ServeContent replies with content like [net/http.ServeContent]: it handles Range and conditional requests,
// sets Content-Type from name's extension or by sniffing, and sets Last-Modified unless modtime is zero.
// An ETag set on w before the call is used for If-Match, If-None-Match, and If-Range.
func ServeContent(w ResponseWriter, r *Request, name string, modtime time.Time, content io.ReadSeeker) {
	sizeFunc := func() (int64, error) {
		size, err := content.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, errSeeker
		}
		if _, err := content.Seek(0, io.SeekStart); err != nil {
			return 0, errSeeker
		}
		return size, nil
	}
	serveContent(w, r, name, modtime, sizeFunc, content)
}

// fileSystem opens '/'-separated names, like net/http.FileSystem.
type fileSystem interface {
	Open(name string) (fs.File, error)
}

type ioFS struct{ fsys fs.FS }

func (f ioFS) Open(name string) (fs.File, error) {
	if name == "/" {
		name = "."
	} else {
		name = strings.TrimPrefix(name, "/")
	}
	file, err := f.fsys.Open(name)
	if err != nil {
		return nil, mapOpenError(err, name, '/', func(p string) (fs.FileInfo, error) { return fs.Stat(f.fsys, p) })
	}
	return file, nil
}

// osDir is net/http.Dir: a directory of the native file system, used by ServeFile.
type osDir string

var errInvalidUnsafePath = errors.New("qilat: invalid or unsafe file path")

func (d osDir) Open(name string) (fs.File, error) {
	p := path.Clean("/" + name)[1:]
	if p == "" {
		p = "."
	}
	p, err := filepath.Localize(p)
	if err != nil {
		return nil, errInvalidUnsafePath
	}
	dir := string(d)
	if dir == "" {
		dir = "."
	}
	full := filepath.Join(dir, p)
	f, err := os.Open(full) //nolint:gosec // the name is cleaned and localized above, as in net/http.Dir
	if err != nil {
		return nil, mapOpenError(err, full, filepath.Separator, os.Stat)
	}
	return f, nil
}

// mapOpenError turns errors about opening a file below a non-directory into fs.ErrNotExist.
func mapOpenError(originalErr error, name string, sep rune, stat func(string) (fs.FileInfo, error)) error {
	if errors.Is(originalErr, fs.ErrNotExist) || errors.Is(originalErr, fs.ErrPermission) {
		return originalErr
	}
	parts := strings.Split(name, string(sep))
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		fi, err := stat(strings.Join(parts[:i+1], string(sep)))
		if err != nil {
			return originalErr
		}
		if !fi.IsDir() {
			return fs.ErrNotExist
		}
	}
	return originalErr
}

var (
	errSeeker      = errors.New("seeker can't seek")
	errNoOverlap   = errors.New("invalid range: failed to overlap")
	errMissingSeek = errors.New("io.File missing Seek method")
)

// seekableFile returns f itself when it can seek, so an *os.File keeps its sendfile path.
func seekableFile(f fs.File) io.ReadSeeker {
	if s, ok := f.(io.ReadSeeker); ok {
		return s
	}
	return noSeekFile{f}
}

type noSeekFile struct{ fs.File }

func (noSeekFile) Seek(int64, int) (int64, error) { return 0, errMissingSeek }

const indexPage = "/index.html"

// serveFile serves name from fsys; upath is the request path, used for redirects.
func serveFile(w ResponseWriter, r *Request, upath string, fsys fileSystem, name string, redirect bool) {
	// A relative redirect still works below StripPrefix.
	if strings.HasSuffix(upath, indexPage) {
		localRedirect(w, r, "./")
		return
	}
	f, err := fsys.Open(name)
	if err != nil {
		msg, code := toHTTPError(err)
		serveError(w, msg, code)
		return
	}
	defer func() { _ = f.Close() }()
	d, err := f.Stat()
	if err != nil {
		msg, code := toHTTPError(err)
		serveError(w, msg, code)
		return
	}

	if redirect {
		if d.IsDir() {
			if upath[len(upath)-1] != '/' {
				localRedirect(w, r, path.Base(upath)+"/")
				return
			}
		} else if upath[len(upath)-1] == '/' {
			base := path.Base(upath)
			if base == "/" || base == "." {
				serveError(w, "http: attempting to traverse a non-directory", 500)
				return
			}
			localRedirect(w, r, "../"+base)
			return
		}
	}

	if d.IsDir() {
		if upath == "" || upath[len(upath)-1] != '/' {
			localRedirect(w, r, path.Base(upath)+"/")
			return
		}
		index := strings.TrimSuffix(name, "/") + indexPage
		if ff, err := fsys.Open(index); err == nil {
			defer func() { _ = ff.Close() }()
			if dd, err := ff.Stat(); err == nil {
				d, f = dd, ff
			}
		}
	}

	if d.IsDir() {
		if checkIfModifiedSince(r, d.ModTime()) == condFalse {
			writeNotModified(w)
			return
		}
		setLastModified(w, d.ModTime())
		dirList(w, f)
		return
	}
	sizeFunc := func() (int64, error) { return d.Size(), nil }
	serveContent(w, r, d.Name(), d.ModTime(), sizeFunc, seekableFile(f))
}

func dirList(w ResponseWriter, f fs.File) {
	d, ok := f.(fs.ReadDirFile)
	if !ok {
		Error(w, "Error reading directory", 500)
		return
	}
	entries, err := d.ReadDir(-1)
	if err != nil {
		Error(w, "Error reading directory", 500)
		return
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	w.Header().Set("content-type", "text/html; charset=utf-8")
	_, _ = w.WriteString("<!doctype html>\n<meta name=\"viewport\" content=\"width=device-width\">\n<pre>\n")
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		// '?' and '#' in a name must be escaped to stay part of the path.
		u := url.URL{Path: name}
		_, _ = fmt.Fprintf(w, "<a href=\"%s\">%s</a>\n", u.String(), htmlEscaper.Replace(name))
	}
	_, _ = w.WriteString("</pre>\n")
}

// serveError clears headers the caller set for a successful response, as net/http does since Go 1.23.
func serveError(w ResponseWriter, text string, code int) {
	h := w.Header()
	for _, k := range [...]string{"cache-control", "content-encoding", "etag", "last-modified"} {
		h.Del(k)
	}
	Error(w, text, code)
}

// toHTTPError never returns err's text, which could leak details about the file system.
func toHTTPError(err error) (msg string, code int) {
	switch {
	case errors.Is(err, fs.ErrNotExist), errors.Is(err, errInvalidUnsafePath):
		return "404 page not found", 404
	case errors.Is(err, fs.ErrPermission):
		return "403 Forbidden", 403
	}
	return "500 Internal Server Error", 500
}

// localRedirect does not make newPath absolute, so it works below StripPrefix.
func localRedirect(w ResponseWriter, r *Request, newPath string) {
	// With escaped slashes there is no reliable way to redirect correctly under StripPrefix.
	raw := r.URI().RawPath()
	if bytes.Contains(raw, []byte("%2f")) || bytes.Contains(raw, []byte("%2F")) {
		NotFound(w, r)
		return
	}
	if q := r.URI().RawQuery(); len(q) > 0 {
		newPath += "?" + string(q)
	}
	w.Header().Set("location", newPath)
	w.WriteHeader(301)
}

func containsDotDot(v string) bool {
	if !strings.Contains(v, "..") {
		return false
	}
	for ent := range strings.FieldsFuncSeq(v, func(r rune) bool { return r == '/' || r == '\\' }) {
		if ent == ".." {
			return true
		}
	}
	return false
}

// serveContent serves content, which must be at its start. An empty name means the type is unknown and is
// sniffed; a zero modtime means it is unknown. sizeFunc is called at most once.
func serveContent(w ResponseWriter, r *Request, name string, modtime time.Time, sizeFunc func() (int64, error),
	content io.ReadSeeker) {
	setLastModified(w, modtime)
	done, rangeReq := checkPreconditions(w, r, modtime)
	if done {
		return
	}
	code := 200
	h := w.Header()
	var ctype string
	if h.Has("content-type") {
		ctype = h.Get("content-type")
	} else {
		ctype = mime.TypeByExtension(filepath.Ext(name))
		if ctype == "" {
			var buf [sniffLen]byte
			n, _ := io.ReadFull(content, buf[:])
			ctype = http.DetectContentType(buf[:n])
			if _, err := content.Seek(0, io.SeekStart); err != nil {
				serveError(w, "seeker can't seek", 500)
				return
			}
		}
		h.Set("content-type", ctype)
	}

	size, err := sizeFunc()
	if err != nil {
		serveError(w, err.Error(), 500)
		return
	}
	if size < 0 {
		serveError(w, "negative content size computed", 500)
		return
	}

	sendSize := size
	ranges, err := parseRange(rangeReq, size)
	switch {
	case err == nil:
	case errors.Is(err, errNoOverlap) && size == 0:
		// Some clients send Range on every request; an empty file gets 200 rather than 416.
		ranges = nil
	default:
		if errors.Is(err, errNoOverlap) {
			h.Set("content-range", "bytes */"+strconv.FormatInt(size, 10))
		}
		serveError(w, err.Error(), 416)
		return
	}
	// Ranges adding up to more than the file are probably an attack; serve the whole file instead.
	if sumRangesSize(ranges) > size {
		ranges = nil
	}
	var mw *multipart.Writer
	switch {
	case len(ranges) == 1:
		// RFC 9110 section 14.6: a single range must not be sent as multipart.
		ra := ranges[0]
		if _, err := content.Seek(ra.start, io.SeekStart); err != nil {
			serveError(w, err.Error(), 416)
			return
		}
		sendSize = ra.length
		code = 206
		h.Set("content-range", ra.contentRange(size))
	case len(ranges) > 1:
		sendSize = rangesMIMESize(ranges, ctype, size)
		code = 206
		mw = multipart.NewWriter(w)
		h.Set("content-type", "multipart/byteranges; boundary="+mw.Boundary())
	}
	h.Set("accept-ranges", "bytes")
	// Like net/http, leave the length to a compressing wrapper that set Content-Encoding itself.
	if len(ranges) > 0 || len(h.Peek("content-encoding")) == 0 {
		h.Set("content-length", strconv.FormatInt(sendSize, 10))
	}
	w.WriteHeader(code)
	if r.Method() == "HEAD" {
		return
	}
	if mw == nil {
		_, _ = io.CopyN(w, content, sendSize)
		return
	}
	for _, ra := range ranges {
		part, err := mw.CreatePart(ra.mimeHeader(ctype, size))
		if err != nil {
			return
		}
		if _, err := content.Seek(ra.start, io.SeekStart); err != nil {
			return
		}
		if _, err := io.CopyN(part, content, ra.length); err != nil {
			return
		}
	}
	_ = mw.Close()
}

// scanETag returns the ETag at the start of s and the text after it, or "", "" if there is none.
func scanETag(s string) (etag string, remain string) {
	s = textproto.TrimString(s)
	start := 0
	if strings.HasPrefix(s, "W/") {
		start = 2
	}
	if len(s[start:]) < 2 || s[start] != '"' {
		return "", ""
	}
	for i := start + 1; i < len(s); i++ {
		c := s[i]
		switch {
		case c == 0x21 || c >= 0x23 && c <= 0x7E || c >= 0x80:
		case c == '"':
			return s[:i+1], s[i+1:]
		default:
			return "", ""
		}
	}
	return "", ""
}

func etagStrongMatch(a, b string) bool { return a == b && a != "" && a[0] == '"' }

func etagWeakMatch(a, b string) bool {
	return strings.TrimPrefix(a, "W/") == strings.TrimPrefix(b, "W/")
}

// condResult is the result of an RFC 9110 section 13 precondition.
type condResult int

const (
	condNone condResult = iota
	condTrue
	condFalse
)

func checkIfMatch(w ResponseWriter, r *Request) condResult {
	im := r.Header.Get("if-match")
	if im == "" {
		return condNone
	}
	for {
		im = textproto.TrimString(im)
		if len(im) == 0 {
			break
		}
		if im[0] == ',' {
			im = im[1:]
			continue
		}
		if im[0] == '*' {
			return condTrue
		}
		etag, remain := scanETag(im)
		if etag == "" {
			break
		}
		if etagStrongMatch(etag, w.Header().Get("etag")) {
			return condTrue
		}
		im = remain
	}
	return condFalse
}

func checkIfUnmodifiedSince(r *Request, modtime time.Time) condResult {
	ius := r.Header.Get("if-unmodified-since")
	if ius == "" || isZeroTime(modtime) {
		return condNone
	}
	t, err := http.ParseTime(ius)
	if err != nil {
		return condNone
	}
	// Last-Modified has whole seconds, so modtime is compared at that precision.
	if modtime.Truncate(time.Second).Compare(t) <= 0 {
		return condTrue
	}
	return condFalse
}

func checkIfNoneMatch(w ResponseWriter, r *Request) condResult {
	buf := r.Header.Get("if-none-match")
	if buf == "" {
		return condNone
	}
	for {
		buf = textproto.TrimString(buf)
		if len(buf) == 0 {
			break
		}
		if buf[0] == ',' {
			buf = buf[1:]
			continue
		}
		if buf[0] == '*' {
			return condFalse
		}
		etag, remain := scanETag(buf)
		if etag == "" {
			break
		}
		if etagWeakMatch(etag, w.Header().Get("etag")) {
			return condFalse
		}
		buf = remain
	}
	return condTrue
}

func checkIfModifiedSince(r *Request, modtime time.Time) condResult {
	if m := r.Method(); m != "GET" && m != "HEAD" {
		return condNone
	}
	ims := r.Header.Get("if-modified-since")
	if ims == "" || isZeroTime(modtime) {
		return condNone
	}
	t, err := http.ParseTime(ims)
	if err != nil {
		return condNone
	}
	if modtime.Truncate(time.Second).Compare(t) <= 0 {
		return condFalse
	}
	return condTrue
}

func checkIfRange(w ResponseWriter, r *Request, modtime time.Time) condResult {
	if m := r.Method(); m != "GET" && m != "HEAD" {
		return condNone
	}
	ir := r.Header.Get("if-range")
	if ir == "" {
		return condNone
	}
	if etag, _ := scanETag(ir); etag != "" {
		if etagStrongMatch(etag, w.Header().Get("etag")) {
			return condTrue
		}
		return condFalse
	}
	// If-Range may also hold a date (golang.org/issue/8367).
	if modtime.IsZero() {
		return condFalse
	}
	t, err := http.ParseTime(ir)
	if err != nil {
		return condFalse
	}
	if t.Unix() == modtime.Unix() {
		return condTrue
	}
	return condFalse
}

var unixEpochTime = time.Unix(0, 0)

func isZeroTime(t time.Time) bool { return t.IsZero() || t.Equal(unixEpochTime) }

func setLastModified(w ResponseWriter, modtime time.Time) {
	if !isZeroTime(modtime) {
		w.Header().Set("last-modified", modtime.UTC().Format(http.TimeFormat))
	}
}

// writeNotModified keeps only the metadata that RFC 9110 section 15.4.5 allows on a 304.
func writeNotModified(w ResponseWriter) {
	h := w.Header()
	h.Del("content-type")
	h.Del("content-length")
	h.Del("content-encoding")
	if len(h.Peek("etag")) > 0 {
		h.Del("last-modified")
	}
	w.WriteHeader(304)
}

// checkPreconditions follows RFC 9110 section 13.2.2 and reports whether it already replied (304 or 412).
// rangeHeader is the Range to serve, or "" when there is none or If-Range does not match.
func checkPreconditions(w ResponseWriter, r *Request, modtime time.Time) (done bool, rangeHeader string) {
	ch := checkIfMatch(w, r)
	if ch == condNone {
		ch = checkIfUnmodifiedSince(r, modtime)
	}
	if ch == condFalse {
		w.WriteHeader(412)
		return true, ""
	}
	switch checkIfNoneMatch(w, r) {
	case condFalse:
		if m := r.Method(); m == "GET" || m == "HEAD" {
			writeNotModified(w)
		} else {
			w.WriteHeader(412)
		}
		return true, ""
	case condNone:
		if checkIfModifiedSince(r, modtime) == condFalse {
			writeNotModified(w)
			return true, ""
		}
	}
	rangeHeader = r.Header.Get("range")
	if rangeHeader != "" && checkIfRange(w, r, modtime) == condFalse {
		rangeHeader = ""
	}
	return false, rangeHeader
}

type httpRange struct{ start, length int64 }

func (r httpRange) contentRange(size int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", r.start, r.start+r.length-1, size)
}

func (r httpRange) mimeHeader(contentType string, size int64) textproto.MIMEHeader {
	return textproto.MIMEHeader{
		"Content-Range": {r.contentRange(size)},
		"Content-Type":  {contentType},
	}
}

// maxContentRanges is net/http's default limit; a Range header with more ranges is ignored.
const maxContentRanges = 200

var errInvalidRange = errors.New("invalid range")

// parseRange parses an RFC 9110 section 14.2 Range header for content of the given size.
func parseRange(s string, size int64) ([]httpRange, error) {
	if s == "" {
		return nil, nil
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errInvalidRange
	}
	if strings.Count(s[len(b):], ",")+1 > maxContentRanges {
		return nil, nil
	}
	var ranges []httpRange
	noOverlap := false
	for ra := range strings.SplitSeq(s[len(b):], ",") {
		ra = textproto.TrimString(ra)
		if ra == "" {
			continue
		}
		start, end, ok := strings.Cut(ra, "-")
		if !ok {
			return nil, errInvalidRange
		}
		start, end = textproto.TrimString(start), textproto.TrimString(end)
		var r httpRange
		if start == "" {
			// A suffix range: the last end bytes.
			if end == "" || end[0] == '-' {
				return nil, errInvalidRange
			}
			i, err := strconv.ParseInt(end, 10, 64)
			if i < 0 || err != nil {
				return nil, errInvalidRange
			}
			i = min(i, size)
			r.start = size - i
			r.length = size - r.start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, errInvalidRange
			}
			if i >= size {
				noOverlap = true
				continue
			}
			r.start = i
			if end == "" {
				r.length = size - r.start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					return nil, errInvalidRange
				}
				i = min(i, size-1)
				r.length = i - r.start + 1
			}
		}
		ranges = append(ranges, r)
	}
	if noOverlap && len(ranges) == 0 {
		return nil, errNoOverlap
	}
	return ranges, nil
}

type countingWriter int64

func (w *countingWriter) Write(p []byte) (int, error) {
	*w += countingWriter(len(p))
	return len(p), nil
}

// rangesMIMESize returns the length of the multipart/byteranges body for ranges.
func rangesMIMESize(ranges []httpRange, contentType string, contentSize int64) int64 {
	var w countingWriter
	mw := multipart.NewWriter(&w)
	var size int64
	for _, ra := range ranges {
		_, _ = mw.CreatePart(ra.mimeHeader(contentType, contentSize))
		size += ra.length
	}
	_ = mw.Close()
	return size + int64(w)
}

func sumRangesSize(ranges []httpRange) (size int64) {
	for _, ra := range ranges {
		size += ra.length
	}
	return size
}

package qilat

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode"
)

// pattern is a parsed ServeMux pattern: [METHOD ][HOST]/[PATH].
// The syntax, precedence, and conflict rules are those of net/http.ServeMux since Go 1.22.
type pattern struct {
	str    string
	method string
	host   string
	// A trailing slash is stored as an anonymous multi wildcard, and {$} as the literal segment "/".
	segments []segment
	names    []string // wildcard names in path order, indexed like the matches of a request
	loc      string   // where the pattern was registered, for conflict messages
}

type segment struct {
	s     string // literal (unescaped), wildcard name, or "/" for {$}
	wild  bool
	multi bool // {name...} or a trailing slash
}

func (p *pattern) String() string { return p.str }

func (p *pattern) lastSegment() segment { return p.segments[len(p.segments)-1] }

func parsePattern(s string) (_ *pattern, err error) {
	if s == "" {
		return nil, errors.New("empty pattern")
	}
	off := 0
	defer func() {
		if err != nil {
			err = fmt.Errorf("at offset %d: %w", off, err)
		}
	}()

	method, rest, found := "", s, false
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		method, rest, found = s[:i], strings.TrimLeft(s[i+1:], " \t"), true
	}
	if method != "" && !validName(method) {
		return nil, fmt.Errorf("invalid method %q", method)
	}
	p := &pattern{str: s, method: method}
	if found {
		off = len(method) + 1
	}
	i := strings.IndexByte(rest, '/')
	if i < 0 {
		return nil, errors.New("host/path missing /")
	}
	p.host, rest = rest[:i], rest[i:]
	if j := strings.IndexByte(p.host, '{'); j >= 0 {
		off += j
		return nil, errors.New("host contains '{' (missing initial '/'?)")
	}
	off += i
	// Request paths are cleaned before matching, so such a pattern could only ever match CONNECT.
	if method != "" && method != "CONNECT" && rest != cleanPath(rest) {
		return nil, errors.New("non-CONNECT pattern with unclean path can never match")
	}

	for len(rest) > 0 {
		rest = rest[1:]
		off = len(s) - len(rest)
		if rest == "" {
			p.segments = append(p.segments, segment{wild: true, multi: true})
			break
		}
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			i = len(rest)
		}
		var seg string
		seg, rest = rest[:i], rest[i:]
		if i := strings.IndexByte(seg, '{'); i < 0 {
			p.segments = append(p.segments, segment{s: unescapeSegment(seg)})
			continue
		} else if i != 0 {
			return nil, errors.New("bad wildcard segment (must start with '{')")
		}
		if seg[len(seg)-1] != '}' {
			return nil, errors.New("bad wildcard segment (must end with '}')")
		}
		name := seg[1 : len(seg)-1]
		if name == "$" {
			if rest != "" {
				return nil, errors.New("{$} not at end")
			}
			p.segments = append(p.segments, segment{s: "/"})
			break
		}
		name, multi := strings.CutSuffix(name, "...")
		if multi && rest != "" {
			return nil, errors.New("{...} wildcard not at end")
		}
		if name == "" {
			return nil, errors.New("empty wildcard")
		}
		if !validWildcardName(name) {
			return nil, fmt.Errorf("bad wildcard name %q", name)
		}
		for _, n := range p.names {
			if n == name {
				return nil, fmt.Errorf("duplicate wildcard name %q", name)
			}
		}
		p.names = append(p.names, name)
		p.segments = append(p.segments, segment{s: name, wild: true, multi: multi})
	}
	return p, nil
}

// validWildcardName reports whether s is a Go identifier.
func validWildcardName(s string) bool {
	for i, c := range s {
		if !unicode.IsLetter(c) && c != '_' && (i == 0 || !unicode.IsDigit(c)) {
			return false
		}
	}
	return s != ""
}

// unescapeSegment percent-decodes a pattern segment, keeping it unchanged if an escape is invalid.
func unescapeSegment(s string) string {
	if strings.IndexByte(s, '%') < 0 || !validEscapes([]byte(s)) {
		return s
	}
	return string(appendUnescape(nil, []byte(s), false))
}

// cleanPath returns the canonical form of p: rooted, without . and .. elements or repeated slashes,
// keeping a trailing slash.
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	np := path.Clean(p)
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}
	return np
}

// relationship describes how the requests matched by one pattern relate to those matched by another.
type relationship uint8

const (
	equivalent   relationship = iota // both match the same requests
	moreGeneral                      // p1 matches everything p2 does, and more
	moreSpecific                     // p2 matches everything p1 does, and more
	disjoint                         // no request matches both
	overlaps                         // some request matches both, but neither is more specific
)

// conflictsWith reports whether some request matches both patterns while neither takes precedence.
// A pattern with a host always beats one without.
func (p *pattern) conflictsWith(q *pattern) bool {
	if p.host != q.host {
		return false
	}
	rel := p.compareMethods(q)
	if rel == disjoint {
		return false
	}
	rel = combine(rel, p.comparePaths(q))
	return rel == equivalent || rel == overlaps
}

// compareMethods treats an empty method as matching everything, and GET as matching HEAD too.
func (p *pattern) compareMethods(q *pattern) relationship {
	switch {
	case p.method == q.method:
		return equivalent
	case p.method == "":
		return moreGeneral
	case q.method == "":
		return moreSpecific
	case p.method == "GET" && q.method == "HEAD":
		return moreGeneral
	case q.method == "GET" && p.method == "HEAD":
		return moreSpecific
	}
	return disjoint
}

func (p *pattern) comparePaths(q *pattern) relationship {
	pMulti, qMulti := p.lastSegment().multi, q.lastSegment().multi
	if len(p.segments) != len(q.segments) && !pMulti && !qMulti {
		return disjoint
	}
	rel := equivalent
	n := min(len(p.segments), len(q.segments))
	for i := range n {
		if rel = combine(rel, compareSegments(p.segments[i], q.segments[i])); rel == disjoint {
			return disjoint
		}
	}
	switch {
	case len(p.segments) == len(q.segments):
		return rel
	case len(p.segments) < len(q.segments) && pMulti:
		return combine(rel, moreGeneral)
	case len(q.segments) < len(p.segments) && qMulti:
		return combine(rel, moreSpecific)
	}
	return disjoint
}

func compareSegments(a, b segment) relationship {
	switch {
	case a.multi && b.multi:
		return equivalent
	case a.multi:
		return moreGeneral
	case b.multi:
		return moreSpecific
	case a.wild && b.wild:
		return equivalent
	case a.wild:
		if b.s == "/" { // a single wildcard never matches the trailing slash of {$}
			return disjoint
		}
		return moreGeneral
	case b.wild:
		if a.s == "/" {
			return disjoint
		}
		return moreSpecific
	case a.s == b.s:
		return equivalent
	}
	return disjoint
}

// combine merges the relationships of two disjoint parts of a pair of patterns.
func combine(r1, r2 relationship) relationship {
	switch r1 {
	case equivalent:
		return r2
	case disjoint:
		return disjoint
	case overlaps:
		if r2 == disjoint {
			return disjoint
		}
		return overlaps
	}
	switch r2 {
	case equivalent:
		return r1
	case inverse(r1):
		return overlaps
	}
	return r2
}

func inverse(r relationship) relationship {
	switch r {
	case moreGeneral:
		return moreSpecific
	case moreSpecific:
		return moreGeneral
	}
	return r
}

func describeConflict(p, q *pattern) string {
	mrel := p.compareMethods(q)
	prel := p.comparePaths(q)
	switch {
	case combine(mrel, prel) == equivalent:
		return fmt.Sprintf("%s matches the same requests as %s", p, q)
	case prel == overlaps:
		return fmt.Sprintf("%s and %s both match some paths, like %q, but neither is more specific than the other",
			p, q, commonPath(p, q))
	case mrel == moreGeneral:
		return fmt.Sprintf("%s matches more methods than %s, but has a more specific path pattern", p, q)
	}
	return fmt.Sprintf("%s matches fewer methods than %s, but has a more general path pattern", p, q)
}

// commonPath returns a path matched by both patterns, which must overlap.
func commonPath(p, q *pattern) string {
	var b strings.Builder
	n := min(len(p.segments), len(q.segments))
	for i := range n {
		if s := p.segments[i]; s.wild {
			writeSegment(&b, q.segments[i])
		} else {
			writeSegment(&b, s)
		}
	}
	for _, s := range p.segments[n:] {
		writeSegment(&b, s)
	}
	for _, s := range q.segments[n:] {
		writeSegment(&b, s)
	}
	return b.String()
}

func writeSegment(b *strings.Builder, s segment) {
	b.WriteByte('/')
	if !s.multi && s.s != "/" {
		b.WriteString(s.s)
	}
}

// patternIndex narrows conflict checks at registration so registering n routes is not O(n²) in practice.
type patternIndex struct {
	// segments maps a position and literal (or "" for a single wildcard) to the patterns with it there.
	// Patterns ending in a multi wildcard are kept in multis instead, since they can match any longer path.
	segments map[indexKey][]*pattern
	multis   []*pattern
}

type indexKey struct {
	pos int
	s   string
}

func (idx *patternIndex) add(p *pattern) {
	if p.lastSegment().multi {
		idx.multis = append(idx.multis, p)
		return
	}
	if idx.segments == nil {
		idx.segments = make(map[indexKey][]*pattern)
	}
	for pos, seg := range p.segments {
		key := indexKey{pos: pos}
		if !seg.wild {
			key.s = seg.s
		}
		idx.segments[key] = append(idx.segments[key], p)
	}
}

// candidates calls f on every registered pattern that might conflict with p, and possibly others.
func (idx *patternIndex) candidates(p *pattern, f func(*pattern) error) error {
	apply := func(ps []*pattern) error {
		for _, q := range ps {
			if err := f(q); err != nil {
				return err
			}
		}
		return nil
	}
	if err := apply(idx.multis); err != nil {
		return err
	}
	// Only another {$} pattern at the same position can match the same trailing-slash paths.
	if p.lastSegment().s == "/" {
		return apply(idx.segments[indexKey{pos: len(p.segments) - 1, s: "/"}])
	}
	// A conflicting pattern has p's literal or a wildcard at every literal position of p;
	// check only the position with the fewest such patterns.
	var lits, wilds []*pattern
	best := -1
	for pos, seg := range p.segments {
		if seg.multi {
			break
		}
		if seg.wild {
			continue
		}
		l, w := idx.segments[indexKey{pos: pos, s: seg.s}], idx.segments[indexKey{pos: pos}]
		if best < 0 || len(l)+len(w) < best {
			lits, wilds, best = l, w, len(l)+len(w)
		}
	}
	if best >= 0 {
		if err := apply(lits); err != nil {
			return err
		}
		return apply(wilds)
	}
	for _, ps := range idx.segments {
		if err := apply(ps); err != nil {
			return err
		}
	}
	return nil
}

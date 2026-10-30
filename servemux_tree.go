package qilat

import (
	"bytes"
	"slices"

	"github.com/erizeta/qilat/internal/bytesconv"
)

// muxTree is a decision tree branching on host, then method, then each path segment.
// Literal children are tried before wildcards, which registration guarantees is the precedence order.
type muxTree struct {
	hosts   map[string]*hostNode
	anyHost *hostNode
}

type hostNode struct {
	methods nodeMap
	any     *muxNode // patterns without a method
}

type muxNode struct {
	pat     *pattern // set on leaves
	handler Handler

	children nodeMap  // literal segments; "/" is the trailing slash of {$}
	wild     *muxNode // single wildcard
	multi    *muxNode // multi wildcard or trailing slash
}

// nodeMap uses a slice for the usual handful of children, where it beats a map lookup.
type nodeMap struct {
	s []nodeEntry
	m map[string]*muxNode
}

type nodeEntry struct {
	key string
	n   *muxNode
}

const maxSliceChildren = 8

func (m *nodeMap) find(key string) *muxNode {
	if m.m != nil {
		return m.m[key]
	}
	for i := range m.s {
		if m.s[i].key == key {
			return m.s[i].n
		}
	}
	return nil
}

func (m *nodeMap) add(key string, n *muxNode) {
	if m.m == nil && len(m.s) < maxSliceChildren {
		m.s = append(m.s, nodeEntry{key, n})
		return
	}
	if m.m == nil {
		m.m = make(map[string]*muxNode, len(m.s)+1)
		for _, e := range m.s {
			m.m[e.key] = e.n
		}
		m.s = nil
	}
	m.m[key] = n
}

func (m *nodeMap) each(f func(key string, n *muxNode)) {
	for _, e := range m.s {
		f(e.key, e.n)
	}
	for k, n := range m.m {
		f(k, n)
	}
}

func (t *muxTree) add(p *pattern, h Handler) {
	hn := t.anyHost
	if p.host == "" {
		if hn == nil {
			hn = new(hostNode)
			t.anyHost = hn
		}
	} else {
		if t.hosts == nil {
			t.hosts = make(map[string]*hostNode)
		}
		if hn = t.hosts[p.host]; hn == nil {
			hn = new(hostNode)
			t.hosts[p.host] = hn
		}
	}
	var n *muxNode
	if p.method == "" {
		if hn.any == nil {
			hn.any = new(muxNode)
		}
		n = hn.any
	} else if n = hn.methods.find(p.method); n == nil {
		n = new(muxNode)
		hn.methods.add(p.method, n)
	}
	for _, seg := range p.segments {
		n = n.child(seg)
	}
	if n.pat != nil {
		panic("qilat: internal error: ServeMux leaf registered twice")
	}
	n.pat, n.handler = p, h
}

func (n *muxNode) child(seg segment) *muxNode {
	switch {
	case seg.multi:
		if n.multi == nil {
			n.multi = new(muxNode)
		}
		return n.multi
	case seg.wild, seg.s == "":
		// Like net/http, an empty literal (from "//" in a CONNECT pattern) shares the wildcard child.
		if n.wild == nil {
			n.wild = new(muxNode)
		}
		return n.wild
	}
	c := n.children.find(seg.s)
	if c == nil {
		c = new(muxNode)
		n.children.add(seg.s, c)
	}
	return c
}

// matchScratch holds per-request buffers so matching does not allocate.
type matchScratch struct {
	raw     [][]byte // wildcard matches, still percent-encoded
	slashed [][]byte // wildcard matches of the trailing-slash attempt, which are discarded
	seg     []byte   // the current segment, decoded
	path    []byte   // the path with a trailing slash appended
}

// match returns the leaf matching host, method, and path, and the wildcard values appended to matches.
func (t *muxTree) match(host []byte, method string, path []byte, matches [][]byte, sc *matchScratch) (*muxNode, [][]byte) {
	if len(host) > 0 && t.hosts != nil {
		if l, m := t.hosts[string(host)].match(method, path, matches, sc); l != nil {
			return l, m
		}
	}
	return t.anyHost.match(method, path, matches, sc)
}

func (h *hostNode) match(method string, path []byte, matches [][]byte, sc *matchScratch) (*muxNode, [][]byte) {
	if h == nil {
		return nil, nil
	}
	if l, m := h.methods.find(method).match(path, matches, sc); l != nil {
		return l, m
	}
	if method == "HEAD" {
		if l, m := h.methods.find("GET").match(path, matches, sc); l != nil {
			return l, m
		}
	}
	return h.any.match(path, matches, sc)
}

func (n *muxNode) match(path []byte, matches [][]byte, sc *matchScratch) (*muxNode, [][]byte) {
	if n == nil {
		return nil, nil
	}
	if len(path) == 0 {
		if n.pat == nil {
			return nil, nil
		}
		return n, matches
	}
	raw, rest := firstSegment(path)
	seg := raw
	if bytes.IndexByte(raw, '%') >= 0 {
		sc.seg = appendUnescape(sc.seg[:0], raw, false)
		seg = sc.seg
	}
	// sc.seg is reused by deeper calls, so decide everything that depends on seg now.
	slash := string(seg) == "/"
	var lit *muxNode
	if len(seg) == 0 {
		lit = n.wild
	} else {
		lit = n.children.find(bytesconv.B2S(seg))
	}
	if l, m := lit.match(rest, matches, sc); l != nil {
		return l, m
	}
	if !slash {
		if l, m := n.wild.match(rest, append(matches, raw), sc); l != nil {
			return l, m
		}
	}
	if c := n.multi; c != nil {
		if c.pat.lastSegment().s != "" { // a trailing slash is an anonymous wildcard
			matches = append(matches, path[1:])
		}
		return c, matches
	}
	return nil, nil
}

// firstSegment splits a path starting with '/' into its first segment, still encoded, and the rest.
// A lone "/" is the trailing-slash segment.
func firstSegment(path []byte) (seg, rest []byte) {
	if len(path) == 1 {
		return path, nil
	}
	path = path[1:]
	i := bytes.IndexByte(path, '/')
	if i < 0 {
		return path, nil
	}
	return path[:i], path[i:]
}

// exactMatch reports whether n matched path without a multi wildcard consuming a non-empty remainder.
func exactMatch(n *muxNode, path []byte) bool {
	if n == nil {
		return false
	}
	if !n.pat.lastSegment().multi {
		return true
	}
	if len(path) > 0 && path[len(path)-1] != '/' {
		return false
	}
	return len(n.pat.segments) == bytes.Count(path, []byte("/"))
}

// allowedMethods returns the sorted methods that would match host and path.
func (t *muxTree) allowedMethods(host, path []byte, sc *matchScratch) []string {
	var ms []string
	collect := func(h *hostNode) {
		if h == nil {
			return
		}
		h.methods.each(func(method string, n *muxNode) {
			if l, _ := n.match(path, sc.raw[:0], sc); l != nil && !slices.Contains(ms, method) {
				ms = append(ms, method)
			}
		})
	}
	if len(host) > 0 && t.hosts != nil {
		collect(t.hosts[string(host)])
	}
	collect(t.anyHost)
	if slices.Contains(ms, "GET") && !slices.Contains(ms, "HEAD") {
		ms = append(ms, "HEAD")
	}
	slices.Sort(ms)
	return ms
}

// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details <https://www.gnu.org/licenses/>.

package hx

import (
	"bytes"
)

// ExtractedEntry is a leaf lifted out of the tree by pruning.
type ExtractedEntry struct {
	Key   []byte // raw key bytes
	Value []byte
}

// aggExcludesEpoch reports whether an aggregate indicator proves that no
// leaf with the evict epoch exists below — the CanSkip condition of
// Algorithm 1 phase 1.
func aggExcludesEpoch(agg uint8, evict byte) bool {
	if evict == 0 {
		return agg == aggOne // all ones: no zero epoch below
	}
	return agg == aggZero // all zeros: no one epoch below
}

// ExtractDomain removes every leaf under the domain prefix whose epoch equals
// evictEpoch and returns the extracted entries in path order. This is
// Algorithm 1 phases 1/2/4 of the paper:
//
//	phase 1 (CanSkip): a subtree whose aggregate indicator excludes the evict
//		epoch is never resolved or descended — zero node accesses.
//	phase 2 (extraction): matching leaves are collected and detached.
//	phase 4 (shrink): internal nodes reduced to a single branch merge with
//		the remaining child; aggregate indicators are recomputed bottom-up.
//
// Phase 3 (compaction/mounting) is driven by the caller: the returned entries
// feed the bucket mounting logic in the mpt layer.
//
// The prefix is a hex-nibble path without terminator. When the logical domain
// is bit-granular (depth not a multiple of 4), the prefix covers only the
// whole nibbles of the domain and inDomain applies the trailing bits: it
// receives the full leaf key and reports whether the leaf belongs to the
// target domain. A nil inDomain selects the whole prefix subtree.
func (t *Trie) ExtractDomain(prefix []byte, evictEpoch byte, inDomain func(key []byte) bool) ([]ExtractedEntry, error) {
	if t.committed {
		return nil, ErrCommitted
	}
	var out []ExtractedEntry
	n, _, err := t.extract(t.root, nil, prefix, evictEpoch, inDomain, &out)
	if err != nil {
		return nil, err
	}
	t.root = n
	return out, nil
}

// extract walks node n at absolute nibble path `path`, removing in-domain
// leaves with the evict epoch. It returns the replacement node (nil if the
// subtree vanished) and whether anything changed.
func (t *Trie) extract(n node, path []byte, domain []byte, evict byte, inDomain func([]byte) bool, out *[]ExtractedEntry) (node, bool, error) {
	inside := len(path) >= len(domain)
	switch n := n.(type) {
	case nil:
		return nil, false, nil

	case valueNode:
		// Branch value slot: unreachable for fixed-width keys; never evicted.
		return n, false, nil

	case hashNode:
		// Reaching a hashNode means the parent already decided this subtree
		// may contain targets (or it is on the navigation path). Resolve.
		resolved, err := t.resolveAndTrack(n, path)
		if err != nil {
			return nil, false, err
		}
		nn, changed, err := t.extract(resolved, path, domain, evict, inDomain, out)
		if err != nil {
			return nil, false, err
		}
		if !changed {
			return n, false, nil
		}
		if nn == nil {
			t.tracer.onDelete(path)
			return nil, true, nil
		}
		// The subtree root was rewritten at the same path.
		t.tracer.onDelete(path)
		t.tracer.onInsert(path)
		return nn, true, nil

	case *shortNode:
		if !inside {
			// Navigation mode: match the remaining domain prefix against the
			// compressed segment.
			rest := domain[len(path):]
			if len(rest) >= len(n.Key) {
				if !bytes.Equal(rest[:len(n.Key)], n.Key) {
					return n, false, nil // domain does not intersect this subtree
				}
			} else {
				if !bytes.Equal(n.Key[:len(rest)], rest) {
					return n, false, nil
				}
				// Domain boundary cuts through this segment: the whole
				// subtree rooted here is inside the domain — switch to
				// extraction mode at this very node.
				if _, isLeaf := n.Val.(valueNode); isLeaf {
					return t.extractLeaf(n, path, evict, inDomain, out)
				}
				// Extension: its aggregate is trustworthy for the whole
				// subtree — CanSkip applies here too.
				if aggExcludesEpoch(n.Agg, evict) {
					return n, false, nil
				}
				return t.extractShortChild(n, path, domain, evict, inDomain, out)
			}
			// Fully consumed: descend (leaf check happens at the recursion).
			if _, isLeaf := n.Val.(valueNode); isLeaf {
				// Leaf path covers the domain prefix: in-domain by construction.
				return t.extractLeaf(n, path, evict, inDomain, out)
			}
			return t.extractShortChild(n, path, domain, evict, inDomain, out)
		}
		// Extraction mode.
		if _, isLeaf := n.Val.(valueNode); isLeaf {
			return t.extractLeaf(n, path, evict, inDomain, out)
		}
		if aggExcludesEpoch(n.Agg, evict) {
			return n, false, nil // CanSkip: do not touch the subtree
		}
		return t.extractShortChild(n, path, domain, evict, inDomain, out)

	case *fullNode:
		if !inside {
			// Navigation: follow the single nibble towards the domain.
			idx := domain[len(path)]
			child := n.Children[idx]
			if child == nil {
				return n, false, nil // empty domain
			}
			nc, changed, err := t.extract(child, concat(path, idx), domain, evict, inDomain, out)
			if err != nil {
				return nil, false, err
			}
			if !changed {
				return n, false, nil
			}
			cpy := n.copy()
			// The copy must be dirtied BEFORE any child swap: n.copy() carries
			// the clean cached hash of a resolved node, and an undirtied mutated
			// copy would be re-stored under its OLD hash while the child's
			// deletion marker hits the database — a dangling reference (the D2
			// nib3 missing-node failure at scale).
			cpy.flags = t.newFlag()
			cpy.Children[idx] = nc
			if nc == nil {
				t.tracer.onDelete(concat(path, idx))
				// The slot aggregate must be cleared too: shrinkFull keeps a
				// branch with >=2 children as-is, and a stale slot agg would
				// be persisted under the root while the child is gone (the
				// extraction-mode loop already refreshes nil slots).
				cpy.refreshAgg(int(idx))
				return t.shrinkFull(cpy, path)
			}
			cpy.refreshAgg(int(idx))
			return cpy, true, nil
		}
		// Extraction mode: per-child CanSkip against the parent's aggregate
		// slots — skipped children are not even resolved.
		cpy := n
		changed := false
		for i := 0; i < 16; i++ {
			child := n.Children[i]
			if child == nil {
				continue
			}
			if aggExcludesEpoch(n.getAgg(i), evict) {
				continue // CanSkip: zero access to this subtree
			}
			nc, childChanged, err := t.extract(child, concat(path, byte(i)), domain, evict, inDomain, out)
			if err != nil {
				return nil, false, err
			}
			if !childChanged {
				continue
			}
			if !changed {
				cpy = n.copy()
				changed = true
			}
			cpy.Children[i] = nc
			if nc == nil {
				t.tracer.onDelete(concat(path, byte(i)))
			}
			cpy.refreshAgg(i) // nil child -> slot becomes aggEmpty
		}
		if !changed {
			return n, false, nil
		}
		cpy.flags = t.newFlag()
		return t.shrinkFull(cpy, path)

	default:
		panic("hx: invalid node type in extract")
	}
}

// extractLeaf evicts a leaf whose epoch matches and whose key passes the
// bit-domain filter (nil filter = the whole prefix subtree), recording the
// entry.
func (t *Trie) extractLeaf(n *shortNode, path []byte, evict byte, inDomain func([]byte) bool, out *[]ExtractedEntry) (node, bool, error) {
	if n.Epoch != evict {
		return n, false, nil
	}
	full := concat(path, n.Key...)
	key := hexToKeybytes(full)
	if inDomain != nil && !inDomain(key) {
		return n, false, nil
	}
	*out = append(*out, ExtractedEntry{
		Key:   key,
		Value: append([]byte(nil), n.Val.(valueNode)...),
	})
	t.tracer.onDelete(path)
	return nil, true, nil
}

// extractShortChild recurses into an extension's child and re-wraps,
// merging with a shortNode child (path compression) when the child itself
// became short.
func (t *Trie) extractShortChild(n *shortNode, path []byte, domain []byte, evict byte, inDomain func([]byte) bool, out *[]ExtractedEntry) (node, bool, error) {
	childPath := concat(path, n.Key...)
	nc, changed, err := t.extract(n.Val, childPath, domain, evict, inDomain, out)
	if err != nil {
		return nil, false, err
	}
	if !changed {
		return n, false, nil
	}
	if nc == nil {
		// The entire subtree below vanished. Stubs mounted here keep a
		// stub-only branch alive (bucket mounting must stay addressable);
		// without stubs the short node goes with its subtree.
		if len(n.Stubs) > 0 {
			t.tracer.onDelete(childPath)
			t.tracer.onInsert(path)
			return &fullNode{Stubs: n.Stubs, flags: t.newFlag()}, true, nil
		}
		t.tracer.onDelete(path)
		return nil, true, nil
	}
	if child, ok := nc.(*shortNode); ok {
		// Merging into a leaf would land n's stubs on a leaf, which the
		// mounting rules forbid — keep the extension above it instead.
		if _, isVal := child.Val.(valueNode); isVal && len(n.Stubs) > 0 {
			fresh := &shortNode{Key: n.Key, Val: nc, Agg: subtreeAggOf(nc), Stubs: n.Stubs, flags: t.newFlag()}
			t.tracer.onInsert(path)
			return fresh, true, nil
		}
		// Merge the two compressed segments; child metadata wins. Stubs ride
		// the highest node of the merge.
		t.tracer.onDelete(path)
		t.tracer.onDelete(childPath)
		t.tracer.onInsert(path)
		return t.foldShortChain(&shortNode{
			Key:   concat(n.Key, child.Key...),
			Val:   child.Val,
			Epoch: child.Epoch,
			Agg:   child.Agg,
			Stubs: mergeStubs(n.Stubs, child.Stubs),
			flags: t.newFlag(),
		}, concat(childPath, child.Key...)), true, nil
	}
	fresh := &shortNode{Key: n.Key, Val: nc, Agg: subtreeAggOf(nc), Stubs: n.Stubs, flags: t.newFlag()}
	t.tracer.onInsert(path)
	return fresh, true, nil
}

// shrinkFull implements Algorithm 1 phase 4 (path shrink): a branch reduced
// to a single remaining child merges with it, recomputing the aggregate.
// Mirrors the delete-path reduction, including the path-invariance property
// (no descendant ever changes its node path).
func (t *Trie) shrinkFull(n *fullNode, path []byte) (node, bool, error) {
	pos := -1
	count := 0
	for i, child := range n.Children {
		if child != nil {
			pos = i
			count++
		}
	}
	if count >= 2 {
		// Pass-through: a descendant below changed, so this branch must be
		// rehashed even though its shape is unchanged — a clean cached hash
		// here would resurrect the removed child's reference.
		n.flags = t.newFlag()
		return n, true, nil
	}
	if count == 0 {
		if len(n.Stubs) > 0 {
			// A stub-only branch: everything below was evicted, but mounted
			// buckets keep it alive (root catch-all included). Never remove.
			n.flags = t.newFlag()
			return n, true, nil
		}
		// Domain subtree fully evicted (or only the value slot left, which
		// cannot happen for fixed-width keys — treat as removal either way).
		t.tracer.onDelete(path)
		return nil, true, nil
	}
	if pos == 16 {
		// Lone branch value: keep the node (unreachable for fixed-width keys).
		n.flags = t.newFlag() // descendant changed: force rehash (see above)
		return n, true, nil
	}
	child := n.Children[pos]
	childPath := concat(path, byte(pos))
	if hn, ok := child.(hashNode); ok {
		// The last surviving child was skipped by CanSkip; resolving it here
		// is one node read, not a subtree scan — required to decide the merge.
		resolved, err := t.resolveAndTrack(hn, childPath)
		if err != nil {
			return nil, false, err
		}
		child = resolved
	}
	if short, ok := child.(*shortNode); ok {
		// Merging into a leaf would land n's stubs on a leaf, which the
		// mounting rules forbid — keep the branch above it instead.
		if _, isVal := short.Val.(valueNode); isVal && len(n.Stubs) > 0 {
			n.flags = t.newFlag() // descendant changed: force rehash
			return n, true, nil
		}
		t.tracer.onDelete(path)
		t.tracer.onDelete(childPath)
		t.tracer.onInsert(path)
		return t.foldShortChain(&shortNode{
			Key:   concat([]byte{byte(pos)}, short.Key...),
			Val:   short.Val,
			Epoch: short.Epoch,
			Agg:   short.Agg,
			Stubs: mergeStubs(n.Stubs, short.Stubs),
			flags: t.newFlag(),
		}, concat(childPath, short.Key...)), true, nil
	}
	// Wrap: the child (fullNode) keeps living at childPath — the short node
	// references it there. Do NOT mark childPath deleted (that would delete
	// the blob out from under a live reference — the D2 nib3 dangling-node
	// failure). The node at `path` is simply overwritten by the committer.
	return &shortNode{Key: []byte{byte(pos)}, Val: child, Agg: subtreeAggOf(child), Stubs: n.Stubs, flags: t.newFlag()}, true, nil
}

// foldShortChain flattens a merged short node whose Val is itself a
// shortNode. Adjacent short segments violate the canonical MPT form the
// geth-derived hasher/committer rely on (the committer never collapses
// short->short links, so an unflattened chain is embedded raw — with the
// epoch/agg/stub fields it exceeds the 32-byte embedding limit and the
// decoder rejects it: the D2 nib3 "oversized embedded node" failure).
// innerPath is the trie path of the currently absorbed Val node; every
// folded node's path is marked deleted. Deepest node's metadata wins.
func (t *Trie) foldShortChain(merged *shortNode, innerPath []byte) *shortNode {
	for {
		inner, ok := merged.Val.(*shortNode)
		if !ok {
			return merged
		}
		t.tracer.onDelete(innerPath)
		merged = &shortNode{
			Key:   concat(merged.Key, inner.Key...),
			Val:   inner.Val,
			Epoch: inner.Epoch,
			Agg:   inner.Agg,
			Stubs: mergeStubs(merged.Stubs, inner.Stubs),
			flags: t.newFlag(),
		}
		innerPath = concat(innerPath, inner.Key...)
	}
}

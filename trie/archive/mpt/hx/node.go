// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package hx is a fork of geth's hexary MPT (trie package) extended with the
// AMT paper's node metadata (route B of plan item D1):
//
//   - Leaf nodes carry a 1-bit epoch (engineering: one byte; the paper's
//     formalization calls it 1 bit, see 4.Design.tex node definitions).
//     v_leaf = <epoch, path, h(V)> — the value is stored inline, and the
//     leaf hash commits to epoch, path and value together.
//   - Branch (full) nodes carry a 2-bit-per-child aggregate epoch indicator
//     (16 children x 2 bits = uint32) encoding for each child subtree whether
//     its leaves are "all epoch 0", "all epoch 1" or "mixed". This powers the
//     O(1) subtree skip (CanSkip) of the paper's Algorithm 1 phase 1.
//   - Branch nodes carry a stubList: compact archival commitments
//     <Path, CF, C_ECMH, Count> mounted at the node sharing the longest common
//     prefix with the pruned batch. Stubs are serialized into the node blob,
//     so the state root covers archive commitments ("hot and cold in one
//     tree", paper section 4).
//
// Extension (short, non-leaf) nodes carry a 2-bit subtree aggregate so CanSkip
// can fire above the first branch of a domain subtree.
package hx

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

type node interface {
	cache() (hashNode, bool)
	encode(w rlp.EncoderBuffer)
	fstring(string) string
}

type (
	fullNode struct {
		Children [17]node // Actual trie node data; slot 16 is the branch value
		EpochAgg uint32   // 2 bits per child slot 0..15: empty/all0/all1/mixed
		Stubs    []*Stub  // side-mounted archival commitments (paper stubList)
		flags    nodeFlag
	}
	shortNode struct {
		Key   []byte
		Val   node
		Epoch byte    // leaf only: 1-bit lifecycle indicator (0/1)
		Agg   uint8   // extension only: 2-bit aggregate of the subtree below
		Stubs []*Stub // rarely used: archive mounts when no branch node exists
		flags nodeFlag
	}
	hashNode  []byte
	valueNode []byte
)

// Aggregate indicator values (2 bits), paper encoding: "all 1s"=10,
// "all 0s"=01, "mixed"=11. 00 means the child slot is empty and never
// participates in CanSkip decisions.
const (
	aggEmpty = uint8(0)
	aggZero  = uint8(1)
	aggOne   = uint8(2)
	aggMixed = uint8(3)
)

// epochToAgg maps a leaf epoch bit to its 2-bit aggregate form.
func epochToAgg(epoch byte) uint8 {
	if epoch == 0 {
		return aggZero
	}
	return aggOne
}

// getAgg extracts the 2-bit aggregate of child slot i.
func (n *fullNode) getAgg(i int) uint8 {
	return uint8((n.EpochAgg >> uint(2*i)) & 0x3)
}

// setAgg writes the 2-bit aggregate of child slot i.
func (n *fullNode) setAgg(i int, a uint8) {
	n.EpochAgg &^= 0x3 << uint(2*i)
	n.EpochAgg |= uint32(a&0x3) << uint(2*i)
}

// combineAgg folds a set of child aggregates into one: equal non-empty
// indicators propagate, any disagreement yields "mixed".
func combineAgg(agg uint8, seen bool, next uint8) (uint8, bool) {
	if next == aggEmpty {
		return agg, seen
	}
	if !seen {
		return next, true
	}
	if agg == next {
		return agg, true
	}
	return aggMixed, true
}

// subtreeAggOf reports the 2-bit aggregate of the subtree rooted at n.
// It is only meaningful for nodes that are fully in memory (freshly built or
// resolved during the current operation); an unresolved hashNode reports
// aggMixed, the conservative answer that never wrongly skips a subtree.
func subtreeAggOf(n node) uint8 {
	switch n := n.(type) {
	case nil:
		return aggEmpty
	case *shortNode:
		if _, isVal := n.Val.(valueNode); isVal {
			return epochToAgg(n.Epoch)
		}
		return n.Agg
	case *fullNode:
		agg, seen := uint8(0), false
		for i := 0; i < 16; i++ {
			agg, seen = combineAgg(agg, seen, n.getAgg(i))
		}
		if !seen {
			return aggEmpty
		}
		return agg
	case hashNode:
		return aggMixed // conservative: unknown without loading
	default:
		return aggMixed
	}
}

// recomputeAggLocked refreshes slot i's aggregate from the (in-memory) child.
func (n *fullNode) refreshAgg(i int) {
	n.setAgg(i, subtreeAggOf(n.Children[i]))
}

// Stub is the compact archival commitment mounted on a branch node
// (paper: B = <Path, CF, C_ECMH, Count>). The physical preimage set lives in
// the archive KV store; only this metadata enters the trie and therefore the
// state root.
type Stub struct {
	Path       []byte   // bucket path relative to the mounting node, HEX nibbles (no terminator)
	Filter     []byte   // serialized Cuckoo filter
	Commitment [33]byte // ECMH commitment (compressed secp256k1 point)
	Count      uint32   // element count
}

func (s *Stub) copy() *Stub {
	c := *s
	return &c
}

// nodeFlag contains caching-related metadata about a node.
type nodeFlag struct {
	hash  hashNode // cached hash of the node (may be nil)
	dirty bool     // whether the node has changes that must be written to the database
}

func (n *fullNode) cache() (hashNode, bool)  { return n.flags.hash, n.flags.dirty }
func (n *shortNode) cache() (hashNode, bool) { return n.flags.hash, n.flags.dirty }
func (n hashNode) cache() (hashNode, bool)   { return nil, true }
func (n valueNode) cache() (hashNode, bool)  { return nil, true }

func (n *fullNode) copy() *fullNode {
	c := *n
	if n.Stubs != nil {
		c.Stubs = make([]*Stub, len(n.Stubs))
		for i, s := range n.Stubs {
			c.Stubs[i] = s.copy()
		}
	}
	return &c
}
func (n *shortNode) copy() *shortNode {
	cpy := *n
	if n.Stubs != nil {
		cpy.Stubs = append([]*Stub(nil), n.Stubs...)
	}
	return &cpy
}

var indices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

// Pretty printing (test diagnostics only).
func (n *fullNode) String() string  { return n.fstring("") }
func (n *shortNode) String() string { return n.fstring("") }
func (n hashNode) String() string   { return n.fstring("") }
func (n valueNode) String() string  { return n.fstring("") }

func (n *fullNode) fstring(ind string) string {
	resp := fmt.Sprintf("[agg=%08x stubs=%d\n%s  ", n.EpochAgg, len(n.Stubs), ind)
	for i, node := range &n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", indices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", indices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *shortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: epoch=%d agg=%d %v} ", n.Key, n.Epoch, n.Agg, n.Val.fstring(ind+"  "))
}
func (n hashNode) fstring(ind string) string  { return fmt.Sprintf("<%x> ", []byte(n)) }
func (n valueNode) fstring(ind string) string { return fmt.Sprintf("%x ", []byte(n)) }

const hashLen = len(common.Hash{})

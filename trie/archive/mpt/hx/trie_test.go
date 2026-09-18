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

package hx

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	gethtrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"

	"fmt"
)

// memNodeDB is a hash-addressed in-memory node store, mirroring how the mpt
// layer's hash backend persists nodes (no reference counting, no blob
// decoding — both production backends, pathdb and mpt's nodeDatabase, are
// blob-agnostic; triedb/hashdb is not used by this package and would reject
// the extended encoding in its child-gathering pass).
type memNodeDB struct {
	nodes map[common.Hash][]byte
}

func newTestDB() *memNodeDB {
	return &memNodeDB{nodes: make(map[common.Hash][]byte)}
}

func (d *memNodeDB) NodeReader(common.Hash) (database.NodeReader, error) {
	return d, nil
}

func (d *memNodeDB) Node(_ common.Hash, _ []byte, hash common.Hash) ([]byte, error) {
	blob, ok := d.nodes[hash]
	if !ok {
		return nil, fmt.Errorf("hx test: node %x not found", hash)
	}
	return blob, nil
}

func (d *memNodeDB) commit(t *testing.T, tr *Trie) common.Hash {
	t.Helper()
	root, set := tr.Commit()
	if set != nil {
		for _, n := range set.Nodes {
			if n.IsDeleted() {
				delete(d.nodes, n.Hash)
				continue
			}
			d.nodes[n.Hash] = n.Blob
		}
	}
	return root
}

func randKeys(r *rand.Rand, n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		k := make([]byte, 32)
		r.Read(k)
		keys[i] = k
	}
	return keys
}

func collectAll(t *testing.T, tr *Trie) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	if err := tr.CollectLeaves(func(k, v []byte) bool {
		out[string(k)] = bytes.Clone(v)
		return true
	}); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return out
}

// TestHXBasicCRUD exercises insert/update/delete round-trips on the forked tree.
func TestHXBasicCRUD(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	tdb := newTestDB()
	tr := NewEmpty(tdb)

	keys := randKeys(r, 64)
	vals := make(map[string][]byte)
	for i, k := range keys {
		v := []byte{byte(i), byte(i >> 8), 0xab}
		vals[string(k)] = v
		if err := tr.Update(k, v); err != nil {
			t.Fatal(err)
		}
	}
	for k, want := range vals {
		got, err := tr.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %x: got %x want %x", k, got, want)
		}
	}
	// Same-value rewrite must still refresh the epoch.
	tr.SetEpochPolicy(func(key []byte) byte { return 1 })
	first := keys[0]
	if err := tr.Update(first, vals[string(first)]); err != nil {
		t.Fatal(err)
	}
	leaf := findLeaf(t, tr, first)
	if leaf.Epoch != 1 {
		t.Fatalf("same-value rewrite did not refresh epoch: %d", leaf.Epoch)
	}
	// Update value, delete others.
	if err := tr.Update(keys[1], []byte{0xff}); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys[2:] {
		if err := tr.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := tr.Get(keys[1]); !bytes.Equal(got, []byte{0xff}) {
		t.Fatalf("post-delete get: %x", got)
	}
	for _, k := range keys[2:] {
		if got, _ := tr.Get(k); got != nil {
			t.Fatalf("deleted key %x still present: %x", k, got)
		}
	}
}

// findLeaf resolves the path and returns the leaf shortNode for assertions.
func findLeaf(t *testing.T, tr *Trie, key []byte) *shortNode {
	t.Helper()
	n := tr.root
	pos := 0
	hex := keybytesToHex(key)
	for {
		switch nn := n.(type) {
		case *shortNode:
			if !bytes.HasPrefix(hex[pos:], nn.Key) {
				t.Fatalf("path diverged at %d", pos)
			}
			pos += len(nn.Key)
			if v, ok := nn.Val.(valueNode); ok {
				_ = v
				return nn
			}
			n = nn.Val
		case *fullNode:
			n = nn.Children[hex[pos]]
			pos++
		case hashNode:
			resolved, err := tr.resolveAndTrack(nn, hex[:pos])
			if err != nil {
				t.Fatal(err)
			}
			n = resolved
		default:
			t.Fatalf("bad node %T", n)
		}
	}
}

// TestHXContentEquivalenceGeth verifies the fork stores exactly what geth's
// native trie stores for the same op sequence (roots differ by design: the
// encoding carries epoch metadata).
func TestHXContentEquivalenceGeth(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	tdb := newTestDB()
	mine := NewEmpty(tdb)
	ref := gethtrie.NewEmpty(triedb.NewDatabase(rawdb.NewMemoryDatabase(), triedb.HashDefaults))

	keys := randKeys(r, 200)
	deleted := make(map[string]bool)
	for i, k := range keys {
		v := make([]byte, 33)
		r.Read(v)
		if i%5 == 4 {
			if err := mine.Delete(k); err != nil {
				t.Fatal(err)
			}
			if err := ref.Delete(k); err != nil {
				t.Fatal(err)
			}
			deleted[string(k)] = true
			continue
		}
		if err := mine.Update(k, v); err != nil {
			t.Fatal(err)
		}
		if err := ref.Update(k, v); err != nil {
			t.Fatal(err)
		}
	}
	_ = ref.Hash()
	it := ref.MustNodeIterator(nil)
	refIt := gethtrie.NewIterator(it)
	refVals := make(map[string][]byte)
	for refIt.Next() {
		refVals[string(refIt.Key)] = bytes.Clone(refIt.Value)
	}
	mineVals := collectAll(t, mine)
	if len(mineVals) != len(refVals) {
		t.Fatalf("content size mismatch: mine %d, geth %d", len(mineVals), len(refVals))
	}
	for k, v := range refVals {
		if !bytes.Equal(mineVals[k], v) {
			t.Fatalf("value mismatch at %x", k)
		}
	}
	_ = deleted
}

// TestHXDeterministicRoot is the determinism red line: identical op sequences
// on independent trees must produce byte-identical roots.
func TestHXDeterministicRoot(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	keys := randKeys(r, 120)
	policy := func(key []byte) byte { return key[0] & 1 }

	roots := make([]common.Hash, 2)
	for run := 0; run < 2; run++ {
		tdb := newTestDB()
		tr := NewEmpty(tdb)
		tr.SetEpochPolicy(policy)
		for i, k := range keys {
			v := []byte{byte(i)}
			if err := tr.Update(k, v); err != nil {
				t.Fatal(err)
			}
			if i%7 == 6 {
				if err := tr.Delete(keys[i/2]); err != nil {
					t.Fatal(err)
				}
			}
		}
		roots[run] = tdb.commit(t, tr)
	}
	if roots[0] != roots[1] {
		t.Fatalf("non-deterministic roots: %x vs %x", roots[0], roots[1])
	}
}

// TestHXReloadAfterCommit commits, reopens from the persisted root, and checks
// content plus epoch metadata survive the round trip through the database.
func TestHXReloadAfterCommit(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	tdb := newTestDB()
	tr := NewEmpty(tdb)
	tr.SetEpochPolicy(func(key []byte) byte { return key[31] & 1 })

	keys := randKeys(r, 300)
	for i, k := range keys {
		if err := tr.Update(k, []byte{byte(i), 0x01}); err != nil {
			t.Fatal(err)
		}
	}
	root := tdb.commit(t, tr)
	if root == types.EmptyRootHash {
		t.Fatal("empty root after inserts")
	}

	reopened, err := New(root, common.Hash{}, tdb)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	for i, k := range keys {
		got, err := reopened.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte{byte(i), 0x01}) {
			t.Fatalf("key %d: got %x", i, got)
		}
		leaf := findLeaf(t, reopened, k)
		if want := k[31] & 1; leaf.Epoch != want {
			t.Fatalf("key %d epoch %d, want %d", i, leaf.Epoch, want)
		}
	}
	// A second commit cycle after reopen must also work.
	if err := reopened.Delete(keys[0]); err != nil {
		t.Fatal(err)
	}
	root2 := tdb.commit(t, reopened)
	reopened2, err := New(root2, common.Hash{}, tdb)
	if err != nil {
		t.Fatalf("reopen2: %v", err)
	}
	if got, _ := reopened2.Get(keys[0]); got != nil {
		t.Fatalf("deleted key present after reload: %x", got)
	}
	if got, _ := reopened2.Get(keys[1]); !bytes.Equal(got, []byte{1, 0x01}) {
		t.Fatalf("survivor mismatch: %x", got)
	}
}

// TestHXEpochAggregatePropagation checks the 2-bit branch aggregates track the
// leaf epoch distribution (acceptance: TestMPTEpochBitmapAggregatePropagation
// at the mpt layer builds on this).
func TestHXEpochAggregatePropagation(t *testing.T) {
	tdb := newTestDB()
	tr := NewEmpty(tdb)

	// Policy: epoch = top bit of the key. Then child 0x0..0x7 subtrees of the
	// root branch are "all 0" and 0x8..0xf are "all 1", as far as populated.
	tr.SetEpochPolicy(func(key []byte) byte { return key[0] >> 7 })
	r := rand.New(rand.NewSource(5))
	keys := randKeys(r, 400)
	for i, k := range keys {
		if err := tr.Update(k, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	root, ok := tr.root.(*fullNode)
	if !ok {
		t.Fatalf("root is %T, want *fullNode", tr.root)
	}
	for i := 0; i < 16; i++ {
		agg := root.getAgg(i)
		if root.Children[i] == nil {
			if agg != aggEmpty {
				t.Fatalf("empty child %d has agg %d", i, agg)
			}
			continue
		}
		want := aggZero
		if i >= 8 {
			want = aggOne
		}
		if agg != want {
			t.Fatalf("child %d agg %d, want %d", i, agg, want)
		}
	}
	// Delete every epoch-0 key: all populated slots must become "all 1".
	for _, k := range keys {
		if k[0]>>7 == 0 {
			if err := tr.Delete(k); err != nil {
				t.Fatal(err)
			}
		}
	}
	root, ok = tr.root.(*fullNode)
	if !ok {
		t.Fatalf("post-delete root is %T", tr.root)
	}
	for i := 0; i < 16; i++ {
		agg := root.getAgg(i)
		if root.Children[i] == nil {
			continue
		}
		if agg != aggOne {
			t.Fatalf("post-delete child %d agg %d, want all-ones", i, agg)
		}
	}
}

// TestHXSortedCollect ensures CollectLeaves yields every key exactly once.
func TestHXSortedCollect(t *testing.T) {
	r := rand.New(rand.NewSource(6))
	tdb := newTestDB()
	tr := NewEmpty(tdb)
	keys := randKeys(r, 150)
	for _, k := range keys {
		if err := tr.Update(k, k[:4]); err != nil {
			t.Fatal(err)
		}
	}
	root := tdb.commit(t, tr)
	reopened, err := New(root, common.Hash{}, tdb)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	vals := collectAll(t, reopened)
	if len(vals) != len(keys) {
		t.Fatalf("collected %d, want %d", len(vals), len(keys))
	}
	var sorted []string
	for k := range vals {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, ks := range sorted {
		if !bytes.Equal(vals[ks], []byte(ks)[:4]) {
			t.Fatalf("value mismatch at %x", ks)
		}
	}
}

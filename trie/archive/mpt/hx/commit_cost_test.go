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
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	gethtrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// TestCommitCostCompare times the per-batch commit of the hx trie against the
// stock geth MPT over the identical operation stream, mimicking the trace
// harness lifecycle: ops -> Commit -> persist nodeset -> reopen from root.
//
// Background: in the 20.2B trace run the AMT side spent ~41ms per 4,000-op
// batch inside CommitToBatch while the stock MPT reference (B0) spent
// ~0.68ms per batch in trie.Commit. This test isolates the committer itself
// from the mpt staging/backend layer: if hx matches stock here, the 60x gap
// lives above this package.
func TestCommitCostCompare(t *testing.T) {
	const (
		preload     = 200_000
		batches     = 100
		opsPerBatch = 1_000
		keyLen      = 32
		valLen      = 32
	)
	rng := rand.New(rand.NewSource(7))
	keys := make([][]byte, preload+batches*opsPerBatch)
	vals := make([][]byte, preload+batches*opsPerBatch)
	for i := range keys {
		k := make([]byte, keyLen)
		v := make([]byte, valLen)
		rng.Read(k)
		rng.Read(v)
		keys[i], vals[i] = k, v
	}

	// ---- leg 1: hx trie ----
	hxDB := newTestDB()
	tr := NewEmpty(hxDB)
	hxRoot := common.Hash{}
	for i := 0; i < preload; i++ {
		if err := tr.Update(keys[i], vals[i]); err != nil {
			t.Fatalf("hx preload update: %v", err)
		}
	}
	hxRoot = hxDB.commit(t, tr)

	var hxCommit []time.Duration
	for b := 0; b < batches; b++ {
		tr, err := New(hxRoot, common.Hash{}, hxDB)
		if err != nil {
			t.Fatalf("hx reopen: %v", err)
		}
		for i := 0; i < opsPerBatch; i++ {
			idx := preload + b*opsPerBatch + i
			// 75% rewrite of an existing key, 25% brand-new key
			if i%4 != 0 {
				idx = rng.Intn(preload)
			}
			if err := tr.Update(keys[idx], vals[idx]); err != nil {
				t.Fatalf("hx update: %v", err)
			}
		}
		start := time.Now()
		hxRoot = hxDB.commit(t, tr)
		hxCommit = append(hxCommit, time.Since(start))
	}

	// ---- leg 2: stock geth MPT ----
	tdb := triedb.NewDatabase(rawdb.NewMemoryDatabase(), triedb.HashDefaults)
	ref := gethtrie.NewEmpty(tdb)
	refRoot := common.Hash{}
	for i := 0; i < preload; i++ {
		if err := ref.Update(keys[i], vals[i]); err != nil {
			t.Fatalf("ref preload update: %v", err)
		}
	}
	refRoot, refSet := ref.Commit(false)
	if err := tdb.Update(refRoot, common.Hash{}, 0, trienode.NewWithNodeSet(refSet), nil); err != nil {
		t.Fatalf("ref tdb update: %v", err)
	}
	if err := tdb.Commit(refRoot, false); err != nil {
		t.Fatalf("ref tdb commit: %v", err)
	}

	var refCommit []time.Duration
	for b := 0; b < batches; b++ {
		ref, err := gethtrie.New(gethtrie.TrieID(refRoot), tdb)
		if err != nil {
			t.Fatalf("ref reopen: %v", err)
		}
		for i := 0; i < opsPerBatch; i++ {
			idx := preload + b*opsPerBatch + i
			if i%4 != 0 {
				idx = rng.Intn(preload)
			}
			if err := ref.Update(keys[idx], vals[idx]); err != nil {
				t.Fatalf("ref update: %v", err)
			}
		}
		start := time.Now()
		newRoot, set := ref.Commit(false)
		refCommit = append(refCommit, time.Since(start))
		if err := tdb.Update(newRoot, refRoot, uint64(b+1), trienode.NewWithNodeSet(set), nil); err != nil {
			t.Fatalf("ref tdb update: %v", err)
		}
		if err := tdb.Commit(newRoot, false); err != nil {
			t.Fatalf("ref tdb commit: %v", err)
		}
		refRoot = newRoot
	}

	stats := func(ds []time.Duration) (avg, p50, p95 time.Duration) {
		sorted := make([]time.Duration, len(ds))
		copy(sorted, ds)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		var sum time.Duration
		for _, d := range ds {
			sum += d
		}
		return sum / time.Duration(len(ds)), sorted[len(sorted)/2], sorted[len(sorted)*95/100]
	}
	ha, hp50, hp95 := stats(hxCommit)
	ra, rp50, rp95 := stats(refCommit)
	t.Logf("hx    commit/batch: avg=%v p50=%v p95=%v", ha, hp50, hp95)
	t.Logf("stock commit/batch: avg=%v p50=%v p95=%v", ra, rp50, rp95)
	t.Logf("hx/stock ratio: avg=%.1fx p50=%.1fx", float64(ha)/float64(ra), float64(hp50)/float64(rp50))
}

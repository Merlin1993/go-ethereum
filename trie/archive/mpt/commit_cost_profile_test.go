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

package mpt

import (
	"encoding/binary"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	stocktrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

// TestCommitCostProfile reproduces the trace-harness commit loop against the
// path backend and captures a CPU profile of the commit section, so the
// 41ms/batch Commit_ms observed in the 20.2B trace run can be attributed to
// concrete functions instead of guessed at.
//
// Run:  go test ./trie/archive/mpt -run TestCommitCostProfile -v
// View: go tool pprof -top /tmp/mpt_commit.prof
func TestCommitCostProfile(t *testing.T) {
	if os.Getenv("MPT_COMMIT_PROFILE") == "" {
		t.Skip("set MPT_COMMIT_PROFILE=1 to run the commit-cost profile")
	}
	preload := 200_000
	batches := 100
	const opsPerBatch = 4_000
	if v := os.Getenv("MPT_COMMIT_PRELOAD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			preload = n
		}
	}
	if v := os.Getenv("MPT_COMMIT_BATCHES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			batches = n
		}
	}
	ldb, err := leveldb.New(t.TempDir(), 512, 256, "mpt-commit-profile", false)
	if err != nil {
		t.Fatalf("open bench database: %v", err)
	}
	t.Cleanup(func() { ldb.Close() })
	store := &levelStore{ldb}
	cfg := benchConfig(BackendPath)
	cfg.ShardDepthBits = 19
	cfg.ForceCommitEveryBatches = 25
	tr, err := New(nil, store, cfg)
	if err != nil {
		t.Fatalf("new trie: %v", err)
	}
	keys, values := benchKeys(preload + batches*opsPerBatch)

	// Preload in 4K chunks with a commit each, mirroring the harness cadence.
	for i := 0; i < preload; i += opsPerBatch {
		batch := store.NewBatch()
		for j := i; j < i+opsPerBatch; j++ {
			if err := tr.Put(keys[j], values[j]); err != nil {
				t.Fatalf("preload put: %v", err)
			}
		}
		if _, err := tr.CommitToBatch(batch, true); err != nil {
			t.Fatalf("preload commit: %v", err)
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("preload write: %v", err)
		}
		batch.Reset()
	}

	prof, err := os.Create(os.Getenv("MPT_COMMIT_PROFILE"))
	if err != nil {
		t.Fatalf("create profile: %v", err)
	}
	defer prof.Close()

	rng := rand.New(rand.NewSource(11))
	var commitDs []time.Duration
	var updateDs []time.Duration
	if err := pprof.StartCPUProfile(prof); err != nil {
		t.Fatalf("start profile: %v", err)
	}
	for b := 0; b < batches; b++ {
		if b > 0 { // production harness prunes one shard per batch, before ops
			if err := tr.PruneNextShard(); err != nil {
				t.Fatalf("prune: %v", err)
			}
		}
		for i := 0; i < opsPerBatch; i++ {
			idx := preload + b*opsPerBatch + i
			if i%4 != 0 { // 75% reads (production trace mix), 25% writes
				if _, err := tr.Get(keys[rng.Intn(preload)]); err != nil {
					t.Fatalf("get: %v", err)
				}
				continue
			}
			if err := tr.Put(keys[idx], values[idx]); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
		batch := store.NewBatch()
		start := time.Now()
		if _, err := tr.CommitToBatch(batch, true); err != nil {
			t.Fatalf("commit: %v", err)
		}
		commitDs = append(commitDs, time.Since(start))
		ws := time.Now()
		if err := batch.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		updateDs = append(updateDs, time.Since(ws))
		batch.Reset()
	}
	pprof.StopCPUProfile()

	sort.Slice(commitDs, func(i, j int) bool { return commitDs[i] < commitDs[j] })
	var sum time.Duration
	for _, d := range commitDs {
		sum += d
	}
	var wsum time.Duration
	for _, d := range updateDs {
		wsum += d
	}
	t.Logf("CommitToBatch/batch: avg=%v p50=%v p95=%v (profile -> %s)",
		sum/time.Duration(len(commitDs)), commitDs[len(commitDs)/2], commitDs[len(commitDs)*95/100], os.Getenv("MPT_COMMIT_PROFILE"))
	t.Logf("batch.Write/batch: avg=%v", wsum/time.Duration(len(updateDs)))
	wr, wn, buf, diff := tr.PathStats()
	t.Logf("AMT pathdb written: %.1fMB in %d writes (buffered %.1fMB, diff %.1fMB); per-op %.1fB",
		float64(wr)/1e6, wn, float64(buf)/1e6, float64(diff)/1e6,
		float64(wr)/float64(preload+batches*opsPerBatch))
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("AMT heap: inuse=%.1fMB alloc=%.1fMB sys=%.1fMB nextGC=%.1fMB numGC=%d",
		float64(ms.HeapInuse)/1e6, float64(ms.HeapAlloc)/1e6, float64(ms.Sys)/1e6, float64(ms.NextGC)/1e6, ms.NumGC)
	if hp, err := os.Create(os.Getenv("MPT_COMMIT_PROFILE") + ".amt.heap"); err == nil {
		pprof.WriteHeapProfile(hp)
		hp.Close()
	}

	if os.Getenv("MPT_COMMIT_STOCK") == "" {
		return
	}
	// Micro-isolation: replay a flush-sized burst (2.7M x ~105B, the shape of
	// one 256MB pathdb buffer flush) through the exact adapter chain and time
	// the Put loop separately from leveldb's batch.Write.
	{
		ldb3, err := leveldb.New(t.TempDir(), 512, 256, "mpt-commit-profile-micro", false)
		if err != nil {
			t.Fatalf("open micro database: %v", err)
		}
		t.Cleanup(func() { ldb3.Close() })
		counter3 := &writeCounter{}
		disk3 := rawdb.NewDatabase(&kvStoreAdapter{store: &levelStore{ldb3}, counter: counter3})
		table3 := rawdb.NewTable(disk3, pathTablePrefix)
		burst := table3.NewBatch()
		key := make([]byte, 40)
		val := make([]byte, 105)
		const burstN = 2_700_000
		putStart := time.Now()
		for i := 0; i < burstN; i++ {
			binary.BigEndian.PutUint64(key, uint64(i)*2654435761)
			if err := burst.Put(key, val); err != nil {
				t.Fatalf("micro put: %v", err)
			}
		}
		putDur := time.Since(putStart)
		writeStart := time.Now()
		if err := burst.Write(); err != nil {
			t.Fatalf("micro write: %v", err)
		}
		writeDur := time.Since(writeStart)
		t.Logf("micro burst: %d puts in %v (%.1fns/put), batch.Write %v (%.1fMB/s)",
			burstN, putDur, float64(putDur.Nanoseconds())/burstN, writeDur, float64(burstN*145)/1e6/writeDur.Seconds())
	}
	// B0-shaped leg: stock geth trie + stock pathdb over an identical op
	// stream, with the same counted adapter so write volumes are comparable.
	ldb2, err := leveldb.New(t.TempDir(), 512, 256, "mpt-commit-profile-stock", false)
	if err != nil {
		t.Fatalf("open stock database: %v", err)
	}
	t.Cleanup(func() { ldb2.Close() })
	tdb2, counter2, err := openPathDatabase(&levelStore{ldb2}, cfg.CleanCacheBytes, cfg.WriteBufferBytes)
	if err != nil {
		t.Fatalf("open stock pathdb: %v", err)
	}
	ref := stocktrie.NewEmpty(tdb2)
	refRoot := types.EmptyRootHash
	stockUpdate := func(batch int) time.Duration {
		start := time.Now()
		newRoot, set := ref.Commit(false)
		if err := tdb2.Update(newRoot, refRoot, uint64(batch), trienode.NewWithNodeSet(set), triedb.NewStateSet()); err != nil {
			t.Fatalf("stock tdb update: %v", err)
		}
		if err := tdb2.Commit(newRoot, false); err != nil {
			t.Fatalf("stock tdb commit: %v", err)
		}
		refRoot = newRoot
		var err error
		if ref, err = stocktrie.New(stocktrie.TrieID(refRoot), tdb2); err != nil { // committed tries are single-use (mirrors B0)
			t.Fatalf("stock reopen: %v", err)
		}
		return time.Since(start)
	}
	for i := 0; i < preload; i++ {
		if err := ref.Update(keys[i], values[i]); err != nil {
			t.Fatalf("stock preload update: %v", err)
		}
		if (i+1)%opsPerBatch == 0 {
			stockUpdate(i / opsPerBatch)
		}
	}
	var stockDs []time.Duration
	for b := 0; b < batches; b++ {
		for i := 0; i < opsPerBatch; i++ {
			idx := preload + b*opsPerBatch + i
			if i%4 != 0 {
				if _, err := ref.Get(keys[rng.Intn(preload)]); err != nil {
					t.Fatalf("stock get: %v", err)
				}
				continue
			}
			if err := ref.Update(keys[idx], values[idx]); err != nil {
				t.Fatalf("stock update: %v", err)
			}
		}
		stockDs = append(stockDs, stockUpdate(preload/opsPerBatch+b))
	}
	sort.Slice(stockDs, func(i, j int) bool { return stockDs[i] < stockDs[j] })
	var ssum time.Duration
	for _, d := range stockDs {
		ssum += d
	}
	wr2, wn2 := counter2.totals()
	t.Logf("stock commit+tdb/batch: avg=%v p50=%v p95=%v", ssum/time.Duration(len(stockDs)), stockDs[len(stockDs)/2], stockDs[len(stockDs)*95/100])
	t.Logf("stock pathdb written: %.1fMB in %d writes; per-op %.1fB",
		float64(wr2)/1e6, wn2, float64(wr2)/float64(preload+batches*opsPerBatch))
	var ms2 runtime.MemStats
	runtime.ReadMemStats(&ms2)
	t.Logf("stock heap: inuse=%.1fMB alloc=%.1fMB sys=%.1fMB nextGC=%.1fMB numGC=%d",
		float64(ms2.HeapInuse)/1e6, float64(ms2.HeapAlloc)/1e6, float64(ms2.Sys)/1e6, float64(ms2.NextGC)/1e6, ms2.NumGC)
	if hp, err := os.Create(os.Getenv("MPT_COMMIT_PROFILE") + ".stock.heap"); err == nil {
		pprof.WriteHeapProfile(hp)
		hp.Close()
	}
}

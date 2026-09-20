package mpt

// Regression stress for the 2026-09-18 D2 dangling-node bug: hx prune's
// shrinkFull "wrap" branch emitted onDelete(childPath) while the child
// fullNode kept living at that exact path, so pathdb deleted a blob that the
// committed tree still referenced (remote nib3 run died at batch 4802 with
// "missing trie node ... loc: dirty, blob: nil"). The same fix pass also
// dirtied previously clean mutated copies in prune.go (stale cached-hash
// class — silent ghost leaves on the hash backend).
//
// Driver-shaped: pathdb backend, one prune per commit, forced pathdb flushes,
// read-activation on, deterministic mixed put/get/delete workload. Every batch
// asserts the zero-pollution invariant that a pending deletion is never
// reachable in the in-memory tree, and fully resolves the committed tree.

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

func TestMPTPathDBPruneStressDanglingRegression(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrieWithBackend(t, db, BackendPath, true)
	tr.config.ForceCommitEveryBatches = 5
	tr.config.DomainNibbles = 1 // 16 domains: one cycle per 16 batches

	rng := rand.New(rand.NewPCG(0xdeadbeef, 20260918))
	mkKey := func() []byte {
		// Deterministic key source (crand would make failures unrepeatable).
		k := make([]byte, 32)
		for i := 0; i < 32; i += 8 {
			v := rng.Uint64()
			for j := 0; j < 8; j++ {
				k[i+j] = byte(v >> (8 * j))
			}
		}
		return k
	}
	mkVal := func(tag int) []byte {
		v := make([]byte, 32)
		for i := range v {
			v[i] = byte(tag + i)
		}
		return v
	}
	keys := map[string][]byte{}

	// Seed a pool so re-writes/re-reads of the same keys are likely.
	pool := make([][]byte, 0, 4096)
	for i := 0; i < 512; i++ {
		pool = append(pool, mkKey())
	}
	pick := func() []byte { return pool[rng.IntN(len(pool))] }

	const batches = 600 // 16 domains -> ~37 full prune cycles
	for batch := 0; batch < batches; batch++ {
		ops := 200
		for i := 0; i < ops; i++ {
			switch rng.IntN(10) {
			case 0, 1, 2, 3: // put (new or rewrite)
				k := pick()
				if rng.IntN(4) == 0 {
					k = mkKey()
					pool = append(pool, k)
				}
				v := mkVal(batch)
				if err := tr.Put(k, v); err != nil {
					t.Fatalf("batch %d put: %v", batch, err)
				}
				keys[string(k)] = v
			case 4: // delete
				k := pick()
				if err := tr.Delete(k); err != nil {
					t.Fatalf("batch %d delete: %v", batch, err)
				}
				delete(keys, string(k))
			default: // get (hot, archived, or absent)
				k := pick()
				if rng.IntN(5) == 0 {
					k = mkKey() // absent probe
				}
				got, err := tr.Get(k)
				if err == ErrNotFound {
					if _, ok := keys[string(k)]; ok {
						dumpKeyWhereabouts(t, tr, k, batch)
						t.Fatalf("batch %d get: live key reported missing", batch)
					}
					continue
				}
				if err != nil {
					t.Fatalf("batch %d get: %v", batch, err)
				}
				want, ok := keys[string(k)]
				if !ok {
					t.Fatalf("batch %d get: ghost value for absent key", batch)
				}
				if string(got) != string(want) {
					t.Fatalf("batch %d get: value mismatch", batch)
				}
			}
		}
		// Zero-pollution invariant: a pending deletion must never be
		// reachable in the in-memory tree (checked resolve-free).
		if bad := danglingScan(t, tr, fmt.Sprintf("batch %d post-ops", batch)); len(bad) > 0 {
			t.Fatalf("batch %d post-ops dangling: %x", batch, bad)
		}
		if err := tr.PruneNextShard(); err != nil {
			t.Fatalf("batch %d prune: %v", batch, err)
		}
		if bad := danglingScan(t, tr, fmt.Sprintf("batch %d post-prune", batch)); len(bad) > 0 {
			t.Fatalf("batch %d post-prune dangling: %x", batch, bad)
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatalf("batch %d commit: %v", batch, err)
		}
		// Integrity gate: resolve the ENTIRE committed tree + stubs.
		hot, err := tr.hotTrieLocked()
		if err != nil {
			t.Fatalf("batch %d reopen: %v", batch, err)
		}
		leaves := 0
		if err := hot.CollectLeaves(func(key, value []byte) bool { leaves++; return true }); err != nil {
			t.Fatalf("batch %d post-commit walk: %v", batch, err)
		}
		stubCount := 0
		if err := hot.AllStubs(func(p []byte, s *gethtrie.Stub) { stubCount++ }); err != nil {
			t.Fatalf("batch %d post-commit stub walk: %v", batch, err)
		}
		// The gate's reads pollute the handle (tracer accessList + resolved
		// caching); drop it so the check never perturbs the next batch.
		tr.mu.Lock()
		tr.hot = nil
		tr.mu.Unlock()
		if batch%100 == 0 {
			fmt.Printf("batch %d ok (pool=%d live=%d leaves=%d stubs=%d)\n", batch, len(pool), len(keys), leaves, stubCount)
		}
	}
}

// dumpKeyWhereabouts prints where a lost key actually lives (hot / buckets).
func dumpKeyWhereabouts(t *testing.T, tr *Trie, k []byte, batch int) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	hot, _ := tr.hotTrieLocked()
	hv, herr := hot.Get(k)
	fmt.Printf("LOST KEY %x (batch %d): hot-present=%v hot-err=%v buckets=%d\n", k[:8], batch, hv != nil, herr, len(tr.buckets))
	fmt.Printf("  stubs actually on the key route:\n")
	_ = hot.StubsOnPath(k, func(np []byte, s *gethtrie.Stub) bool {
		fmt.Printf("    route stub at node=%x stubPath=%x count=%d\n", np, s.Path, s.Count)
		return false
	})
	for sp, b := range tr.buckets {
		if err := tr.loadBucketLocked(b); err != nil {
			fmt.Printf("  bucket stub=%x load-err=%v\n", []byte(sp), err)
			continue
		}
		_, hit := b.entries[string(k)]
		fhit := b.filter != nil && b.filter.Lookup(k)
		if hit || fhit {
			fmt.Printf("  bucket stub=%x mount=%x count=%d entry=%v filter=%v\n", []byte(sp), b.mount, b.count, hit, fhit)
		}
	}
}

// danglingScan returns pending-deletion paths still reachable in the live hot
// handle (resolve-free).
func danglingScan(t *testing.T, tr *Trie, where string) [][]byte {
	t.Helper()
	hot, err := tr.hotTrieLocked()
	if err != nil {
		t.Fatalf("%s reopen: %v", where, err)
	}
	return hot.DebugCheckDangling()
}

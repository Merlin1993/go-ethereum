package mpt

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// pipelineOracleKey builds deterministic 32-byte keys; the top nibble (the
// D=4 domain) is spread evenly so pruning has real work every cycle.
func pipelineOracleKey(rng *rand.Rand) []byte {
	key := make([]byte, 32)
	rng.Read(key)
	return key
}

// TestMPTIntermediateCommitPreservesRoot is the semantics oracle for the
// paper's per-batch pipeline (prune → intermediate root → operations →
// root): splitting the batch into two commits must produce exactly the same
// final root and the same readable state as the single-commit ordering
// (prune → operations → root) over an identical deterministic op sequence.
func TestMPTIntermediateCommitPreservesRoot(t *testing.T) {
	const (
		preload     = 2048
		batches     = 40 // 2.5 full rotations at D=4 (16 domains)
		opsPerBatch = 256
	)
	rng := rand.New(rand.NewSource(42))
	keys := make([][]byte, 0, preload+batches*opsPerBatch)
	for i := 0; i < preload+batches*opsPerBatch; i++ {
		keys = append(keys, pipelineOracleKey(rng))
	}
	// Deterministic per-batch op scripts shared by both variants.
	type op struct {
		kind int // 0=put fresh key, 1=put existing, 2=read, 3=delete
		key  []byte
		val  []byte
	}
	scripts := make([][]op, batches)
	opRng := rand.New(rand.NewSource(7))
	for b := 0; b < batches; b++ {
		script := make([]op, 0, opsPerBatch)
		for i := 0; i < opsPerBatch; i++ {
			kind := opRng.Intn(4)
			var key []byte
			switch kind {
			case 0:
				key = keys[preload+b*opsPerBatch+i] // fresh, never reused
			default:
				key = keys[opRng.Intn(preload+b*opsPerBatch)] // revisit
			}
			script = append(script, op{kind: kind, key: key, val: []byte(fmt.Sprintf("v-%d-%d", b, i))})
		}
		scripts[b] = script
	}

	run := func(backend string, intermediate bool) ([]byte, map[string][]byte) {
		db := &testStore{memorydb.New()}
		tr := newTestTrieWithBackend(t, db, backend, false)
		batch := db.NewBatch()
		for i := 0; i < preload; i++ {
			if err := tr.Put(keys[i], []byte(fmt.Sprintf("init-%d", i))); err != nil {
				t.Fatalf("preload: %v", err)
			}
		}
		if _, err := tr.CommitToBatch(batch, true); err != nil {
			t.Fatalf("preload commit: %v", err)
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("preload write: %v", err)
		}
		batch.Reset()
		for b := 0; b < batches; b++ {
			if err := tr.PruneNextShard(); err != nil {
				t.Fatalf("batch %d prune: %v", b, err)
			}
			if intermediate {
				ib := db.NewBatch()
				if _, err := tr.CommitToBatch(ib, true); err != nil {
					t.Fatalf("batch %d intermediate commit: %v", b, err)
				}
				if err := ib.Write(); err != nil {
					t.Fatalf("batch %d intermediate write: %v", b, err)
				}
			}
			for _, o := range scripts[b] {
				var err error
				switch o.kind {
				case 0, 1:
					err = tr.Put(o.key, o.val)
				case 2:
					_, err = tr.Get(o.key)
				case 3:
					err = tr.Delete(o.key)
				}
				// Reads/deletes of keys the script already deleted (or never
				// wrote) legitimately miss; the oracle compares final state,
				// not per-op success.
				if err != nil && !strings.Contains(err.Error(), "not found") {
					t.Fatalf("batch %d op: %v", b, err)
				}
			}
			if _, err := tr.CommitToBatch(batch, true); err != nil {
				t.Fatalf("batch %d commit: %v", b, err)
			}
			if err := batch.Write(); err != nil {
				t.Fatalf("batch %d write: %v", b, err)
			}
			batch.Reset()
		}
		// Sample reads across every domain, including archived/deleted keys;
		// both variants must observe identical outcomes, errors included.
		reads := make(map[string][]byte)
		for i := 0; i < preload+batches*opsPerBatch; i += 97 {
			v, err := tr.Get(keys[i])
			if err != nil {
				reads[string(keys[i])] = []byte("ERR:" + err.Error())
				continue
			}
			reads[string(keys[i])] = v
		}
		finalRoot, err := tr.Hash()
		if err != nil {
			t.Fatalf("final hash: %v", err)
		}
		return finalRoot, reads
	}

	for _, backend := range []string{"", BackendPath} {
		rootSingle, readsSingle := run(backend, false)
		rootSplit, readsSplit := run(backend, true)
		if !bytes.Equal(rootSingle, rootSplit) {
			t.Fatalf("backend %q final root mismatch: single-commit %x vs split-commit %x", backend, rootSingle, rootSplit)
		}
		for k, v := range readsSingle {
			if !bytes.Equal(v, readsSplit[k]) {
				t.Fatalf("backend %q read mismatch for key %x: single=%x split=%x", backend, k, v, readsSplit[k])
			}
		}
	}
}

// TestMPTPrefetchPreservesRoot exercises the background pre-warm
// (Config.AsyncPrune) on the path backend: the same deterministic workload
// with prefetch on vs off must end at the same root and serve the same
// reads. The prefetch walker is read-only, so this must hold trivially — the
// test exists to catch accidental state coupling between the walker and the
// main trie.
func TestMPTPrefetchPreservesRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("path backend test")
	}
	const (
		preload     = 4096
		batches     = 24
		opsPerBatch = 256
	)
	rng := rand.New(rand.NewSource(99))
	keys := make([][]byte, 0, preload+batches*opsPerBatch)
	for i := 0; i < preload+batches*opsPerBatch; i++ {
		keys = append(keys, pipelineOracleKey(rng))
	}

	run := func(prefetch bool) []byte {
		db := &testStore{memorydb.New()}
		tr, err := New(nil, db, &Config{
			ShardDepthBits: 4,
			CuckooBuckets:  32,
			CuckooSlots:    4,
			Backend:        BackendPath,
			AsyncPrune:     prefetch,
		})
		if err != nil {
			t.Fatalf("new trie (prefetch=%v): %v", prefetch, err)
		}
		batch := db.NewBatch()
		for i := 0; i < preload; i++ {
			if err := tr.Put(keys[i], []byte(fmt.Sprintf("init-%d", i))); err != nil {
				t.Fatalf("preload: %v", err)
			}
		}
		if _, err := tr.CommitToBatch(batch, true); err != nil {
			t.Fatalf("preload commit: %v", err)
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("preload write: %v", err)
		}
		batch.Reset()
		opRng := rand.New(rand.NewSource(3))
		for b := 0; b < batches; b++ {
			if err := tr.PruneNextShard(); err != nil {
				t.Fatalf("batch %d prune: %v", b, err)
			}
			for i := 0; i < opsPerBatch; i++ {
				idx := preload + b*opsPerBatch + i
				switch i % 4 {
				case 0:
					if err := tr.Put(keys[idx], []byte(fmt.Sprintf("v-%d", idx))); err != nil {
						t.Fatalf("put: %v", err)
					}
				case 3:
					if err := tr.Delete(keys[opRng.Intn(idx)]); err != nil && !strings.Contains(err.Error(), "not found") {
						t.Fatalf("delete: %v", err)
					}
				default:
					if _, err := tr.Get(keys[opRng.Intn(idx)]); err != nil && !strings.Contains(err.Error(), "not found") {
						t.Fatalf("get: %v", err)
					}
				}
			}
			if _, err := tr.CommitToBatch(batch, true); err != nil {
				t.Fatalf("batch %d commit: %v", b, err)
			}
			if err := batch.Write(); err != nil {
				t.Fatalf("batch %d write: %v", b, err)
			}
			batch.Reset()
		}
		// Drain the single-flight prefetch before comparing so no goroutine
		// outlives the trie.
		for i := 0; i < 1000; i++ {
			tr.prefetchMu.Lock()
			busy := tr.prefetchBusy
			tr.prefetchMu.Unlock()
			if !busy {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		tr.prefetchMu.Lock()
		stillBusy := tr.prefetchBusy
		tr.prefetchMu.Unlock()
		if stillBusy {
			t.Fatalf("prefetch still running after 5s (prefetch=%v)", prefetch)
		}
		finalRoot, err := tr.Hash()
		if err != nil {
			t.Fatalf("final hash: %v", err)
		}
		return finalRoot
	}

	rootOff := run(false)
	rootOn := run(true)
	if !bytes.Equal(rootOff, rootOn) {
		t.Fatalf("final root mismatch: prefetch off %x vs on %x", rootOff, rootOn)
	}
}

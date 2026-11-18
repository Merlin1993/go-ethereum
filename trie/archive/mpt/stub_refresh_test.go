package mpt

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// A forced refresh after every redemption models the previous eager stub
// updates. Both schedules must commit to the same root and bucket contents.
func TestDeferredStubRefreshMatchesEagerRedemption(t *testing.T) {
	for _, backend := range []string{BackendHash, BackendPath} {
		for _, audit := range []bool{false, true} {
			t.Run(backend+map[bool]string{false: "/no_audit", true: "/audit"}[audit], func(t *testing.T) {
				keys := [][]byte{{0x00, 0x11}, {0x00, 0x12}, {0x00, 0x13}}
				makeTrie := func() *Trie {
					tr := newTestTrieWithBackend(t, &testStore{memorydb.New()}, backend, true)
					for i, key := range keys {
						if err := tr.Put(key, []byte{byte(i + 1)}); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := tr.Commit(); err != nil {
						t.Fatal(err)
					}
					expirePrune(t, tr, 0)
					if _, err := tr.Commit(); err != nil {
						t.Fatal(err)
					}
					tr.config.VerifyAtCommit = audit
					return tr
				}
				deferred, eager := makeTrie(), makeTrie()
				initialMeta := bytes.Clone(bucketOfKey(t, deferred, keys[0]).metaBytes)
				for i, key := range keys[:2] {
					for _, tr := range []*Trie{deferred, eager} {
						got, err := tr.Get(key)
						if err != nil || !bytes.Equal(got, []byte{byte(i + 1)}) {
							t.Fatalf("Get(%x) = %x, %v", key, got, err)
						}
					}
					// Publish each intermediate stub in the eager reference.
					eager.mu.Lock()
					err := eager.flushPendingStubsLocked()
					eager.mu.Unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
				if b := bucketOfKey(t, deferred, keys[2]); !b.stubPending || !bytes.Equal(b.metaBytes, initialMeta) {
					t.Fatal("deferred bucket published an intermediate stub")
				}
				for _, tr := range []*Trie{deferred, eager} {
					if got, err := tr.Get(keys[0]); err != nil || !bytes.Equal(got, []byte{1}) {
						t.Fatalf("repeated hot Get = %x, %v", got, err)
					}
				}
				rootDeferred, err := deferred.Commit()
				if err != nil {
					t.Fatal(err)
				}
				rootEager, err := eager.Commit()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(rootDeferred, rootEager) {
					t.Fatalf("first committed roots differ: deferred=%x eager=%x", rootDeferred, rootEager)
				}
				for _, tr := range []*Trie{deferred, eager} {
					if got, err := tr.Get(keys[2]); err != nil || !bytes.Equal(got, []byte{3}) {
						t.Fatalf("last redemption = %x, %v", got, err)
					}
				}
				rootDeferred, err = deferred.Commit()
				if err != nil {
					t.Fatal(err)
				}
				rootEager, err = eager.Commit()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(rootDeferred, rootEager) || len(deferred.buckets) != len(eager.buckets) {
					t.Fatalf("empty-bucket final state differs: deferred=%x eager=%x", rootDeferred, rootEager)
				}
			})
		}
	}
}

func TestDeferredStubRefreshBeforePrune(t *testing.T) {
	keys := [][]byte{{0x00, 0x11}, {0x00, 0x12}}
	makeTrie := func() *Trie {
		tr := newTestTrie(t, &testStore{memorydb.New()}, true)
		for i, key := range keys {
			if err := tr.Put(key, []byte{byte(i + 1)}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatal(err)
		}
		expirePrune(t, tr, 0)
		if _, err := tr.Commit(); err != nil {
			t.Fatal(err)
		}
		return tr
	}
	deferred, eager := makeTrie(), makeTrie()
	for _, tr := range []*Trie{deferred, eager} {
		if _, err := tr.Get(keys[0]); err != nil {
			t.Fatal(err)
		}
	}
	eager.mu.Lock()
	err := eager.flushPendingStubsLocked()
	eager.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range []*Trie{deferred, eager} {
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
	rootDeferred, err := deferred.Commit()
	if err != nil {
		t.Fatal(err)
	}
	rootEager, err := eager.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rootDeferred, rootEager) {
		t.Fatalf("prune roots differ: deferred=%x eager=%x", rootDeferred, rootEager)
	}
}

// BenchmarkRedemptionStubRefresh includes both the read promotions and the
// resulting commit. The eager case publishes each intermediate stub, matching
// the former update schedule without changing the production code path.
func BenchmarkRedemptionStubRefresh(b *testing.B) {
	keys := make([][]byte, 8)
	for i := range keys {
		keys[i] = []byte{0x00, byte(0x10 + i)}
	}
	for _, eager := range []bool{false, true} {
		name := "deferred"
		if eager {
			name = "eager"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tr, err := New(nil, &testStore{memorydb.New()}, &Config{
					ShardDepthBits: 4, CuckooBuckets: 32, CuckooSlots: 4,
					ActivateArchivedKeyOnRead: true, Backend: BackendHash,
				})
				if err != nil {
					b.Fatal(err)
				}
				for j, key := range keys {
					if err := tr.Put(key, []byte{byte(j + 1)}); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := tr.Commit(); err != nil {
					b.Fatal(err)
				}
				for domain := 0; domain < 17; domain++ {
					if err := tr.PruneNextShard(); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := tr.Commit(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for _, key := range keys {
					if _, err := tr.Get(key); err != nil {
						b.Fatal(err)
					}
					if eager {
						tr.mu.Lock()
						err := tr.flushPendingStubsLocked()
						tr.mu.Unlock()
						if err != nil {
							b.Fatal(err)
						}
					}
				}
				if _, err := tr.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

package mpt

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// shardBitKey13 builds a 32-byte key for the D=13 boundary tests: the top 12
// bits are prefix12, bit 12 (the MSB of the fourth nibble — the D=13 boundary
// bit) is boundaryBit, bits 13..15 are low3, and the tail is a distinct
// per-seq pattern.
func shardBitKey13(prefix12 uint16, boundaryBit, low3, seq int) []byte {
	key := make([]byte, 32)
	key[0] = byte(prefix12 >> 4)
	key[1] = byte(prefix12&0xf)<<4 | byte(boundaryBit&1)<<3 | byte(low3&0x7)
	key[29] = byte(seq >> 8)
	key[30] = byte(seq)
	key[31] = byte(seq*7 + 1)
	return key
}

// TestMPTShardBitBoundarySplitD13 is the bit-granularity acceptance test at a
// non-nibble-aligned depth: with ShardDepthBits=13 the 13th key bit splits one
// nibble subtree into the adjacent domains 0x400/0x401, so pruning one must
// archive exactly its own leaves and never touch the other's. Extraction
// navigates the shared 3-nibble prefix; the per-leaf bit filter does the
// split, and the epoch rule still applies inside the target domain.
func TestMPTShardBitBoundarySplitD13(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got := tr.domainCount(); got != 8192 {
		t.Fatalf("domainCount = %d, want 2^13 = 8192", got)
	}

	// A key written while the rotation pointer is still at 0 lands on the
	// "not yet pruned" side and gets the opposite-of-base epoch (1): it must
	// survive its own domain's prune as a same-domain fresh leaf.
	kFreshA := shardBitKey13(0x200, 0, 5, 999)
	if err := tr.Put(kFreshA, []byte("fresh-A")); err != nil {
		t.Fatal(err)
	}

	// Aim the epoch schedule so writes to domain 0x400 take the base bit (0)
	// and writes to 0x401 take the opposite bit (1): prune index 0x401 puts
	// 0x400 on the "already pruned" side and 0x401 on the "not yet pruned"
	// side (white-box; the production schedule reaches the same state by
	// rotation).
	tr.pruneDomainIdx = 0x401

	const prefix12 = 0x200 // nibbles [2,0,...]: the shared boundary subtree
	var keysA, keysB [][]byte
	for i := 0; i < 4; i++ {
		kA := shardBitKey13(prefix12, 0, i, i)      // domain 0x400, boundary nibble 0+i
		kB := shardBitKey13(prefix12, 1, i, 1000+i) // domain 0x401, boundary nibble 8+i
		vA := []byte(fmt.Sprintf("a-%d", i))
		vB := []byte(fmt.Sprintf("b-%d", i))
		if err := tr.Put(kA, vA); err != nil {
			t.Fatal(err)
		}
		if err := tr.Put(kB, vB); err != nil {
			t.Fatal(err)
		}
		keysA = append(keysA, kA)
		keysB = append(keysB, kB)
	}
	// Routing and epoch sanity before the prune.
	for _, k := range keysA {
		if got := tr.domainID(k); got != 0x400 {
			t.Fatalf("A key %x routes to domain %d, want 0x400", k[:4], got)
		}
		if epoch, ok := leafEpoch(t, tr, k); !ok || epoch != 0 {
			t.Fatalf("A key %x: epoch %d ok %v, want base bit 0", k[:4], epoch, ok)
		}
	}
	for _, k := range keysB {
		if got := tr.domainID(k); got != 0x401 {
			t.Fatalf("B key %x routes to domain %d, want 0x401", k[:4], got)
		}
		if epoch, ok := leafEpoch(t, tr, k); !ok || epoch != 1 {
			t.Fatalf("B key %x: epoch %d ok %v, want 1", k[:4], epoch, ok)
		}
	}

	// Prune domain 0x400 only.
	tr.pruneDomainIdx = 0x400
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}

	// The target domain's expired leaves are archived...
	for i, k := range keysA {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || !fromArchive {
			t.Fatalf("A key %d after prune: fromArchive=%v err=%v; want an archive hit", i, fromArchive, err)
		}
	}
	// ... and only they moved: exactly the four epoch-0 A leaves are in the
	// archive (the fresh A leaf and every B leaf stayed hot).
	b := bucketOfKey(t, tr, keysA[0])
	if b == nil {
		t.Fatal("no bucket claims the pruned A keys")
	}
	if b.count != len(keysA) {
		t.Fatalf("archived count = %d, want exactly the %d expired A leaves", b.count, len(keysA))
	}
	if _, fromArchive, err := tr.GetValueRef(kFreshA); err != nil || fromArchive {
		t.Fatalf("fresh A leaf after prune: fromArchive=%v err=%v; want a hot hit (epoch 1 survives)", fromArchive, err)
	}
	for i, k := range keysB {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || fromArchive {
			t.Fatalf("B key %d after prune: fromArchive=%v err=%v; want a hot hit (sibling domain untouched)", i, fromArchive, err)
		}
		if epoch, ok := leafEpoch(t, tr, k); !ok || epoch != 1 {
			t.Fatalf("B key %d after prune: epoch %d ok %v, want a live epoch-1 leaf", i, epoch, ok)
		}
	}
	// The boundary subtree still commits and the archive round-trips through
	// a reload.
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range keysA {
		got, err := reloaded.Get(k)
		if err != nil || !bytes.Equal(got, []byte(fmt.Sprintf("a-%d", i))) {
			t.Fatalf("reloaded A key %d: %q err %v", i, got, err)
		}
	}
	for i, k := range keysB {
		got, err := reloaded.Get(k)
		if err != nil || !bytes.Equal(got, []byte(fmt.Sprintf("b-%d", i))) {
			t.Fatalf("reloaded B key %d: %q err %v", i, got, err)
		}
	}
	if got, err := reloaded.Get(kFreshA); err != nil || !bytes.Equal(got, []byte("fresh-A")) {
		t.Fatalf("reloaded fresh A: %q err %v", got, err)
	}
}

// TestMPTShardBitDeterministicRootD13 pins the determinism red line at a
// non-nibble-aligned depth: two independent runs over the same operation
// stream and prune schedule must produce byte-identical roots. The key source
// is a seeded math/rand PCG (never crypto/rand) so a failure is repeatable.
func TestMPTShardBitDeterministicRootD13(t *testing.T) {
	run := func() []byte {
		db := &testStore{memorydb.New()}
		tr, err := New(nil, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewPCG(0x5eed13, 20260923))
		mkKey := func() []byte {
			k := make([]byte, 32)
			// Small leading byte so the low domains actually get pruned
			// mid-run; the rest is uniform.
			k[0] = byte(rng.IntN(8))
			for i := 8; i < 32; i += 8 {
				v := rng.Uint64()
				for j := 0; j < 8; j++ {
					k[i+j] = byte(v >> (8 * j))
				}
			}
			return k
		}
		pool := make([][]byte, 0, 256)
		for i := 0; i < 128; i++ {
			pool = append(pool, mkKey())
		}
		for batch := 0; batch < 40; batch++ {
			for i := 0; i < 50; i++ {
				switch rng.IntN(4) {
				case 0, 1: // put (new or rewrite)
					k := pool[rng.IntN(len(pool))]
					if rng.IntN(5) == 0 {
						k = mkKey()
						pool = append(pool, k)
					}
					if err := tr.Put(k, []byte(fmt.Sprintf("b%d-i%d", batch, i))); err != nil {
						t.Fatal(err)
					}
				case 2: // delete
					if err := tr.Delete(pool[rng.IntN(len(pool))]); err != nil {
						t.Fatal(err)
					}
				default: // get (hot, archived, or absent)
					_, _ = tr.Get(pool[rng.IntN(len(pool))])
				}
			}
			if batch%2 == 1 {
				if err := tr.PruneNextShard(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tr.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		return tr.Root()
	}
	a, b := run(), run()
	if !bytes.Equal(a, b) {
		t.Fatalf("non-deterministic roots at D=13: %x vs %x", a, b)
	}
}

// TestMPTShardBitCommitReloadWalkD13 covers commit -> reload at a
// non-nibble-aligned depth: the persisted schedule (base bit, domain index)
// survives the reopen, every live key still resolves hot or archived, the
// aggregate epoch indicators stay truthful, and a no-op recommit reproduces
// the root.
func TestMPTShardBitCommitReloadWalkD13(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	live := map[string][]byte{}
	put := func(k []byte, tag string) {
		v := []byte(fmt.Sprintf("v-%s", tag))
		if err := tr.Put(k, v); err != nil {
			t.Fatal(err)
		}
		live[string(k)] = v
	}

	// Schedule at 1: domain-0 writes take the base bit (0), everything else
	// the opposite (1).
	tr.pruneDomainIdx = 1
	// Three keys in domain 0 (top 13 bits all zero) plus boundary pairs in
	// assorted prefixes and a deterministic rng batch.
	for i := 0; i < 3; i++ {
		put(shardBitKey13(0x000, 0, i, i), fmt.Sprintf("d0-%d", i))
	}
	for p, prefix12 := range []uint16{0x200, 0xabc, 0xfff} {
		for bit := 0; bit < 2; bit++ {
			put(shardBitKey13(prefix12, bit, p, 3000+8*p+bit), fmt.Sprintf("p%d-b%d", p, bit))
		}
	}
	rng := rand.New(rand.NewPCG(0xb17, 13))
	for i := 0; i < 64; i++ {
		k := make([]byte, 32)
		k[0] = byte(rng.IntN(8))
		for j := 8; j < 32; j += 8 {
			v := rng.Uint64()
			for q := 0; q < 8; q++ {
				k[j+q] = byte(v >> (8 * q))
			}
		}
		put(k, fmt.Sprintf("rng-%d", i))
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}

	// Prune domain 0: the three d0 keys expire (epoch 0 == base bit 0).
	tr.pruneDomainIdx = 0
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	wantIdx, wantBase := tr.pruneDomainIdx, tr.baseBit

	reloaded, err := New(root, db, &Config{ShardDepthBits: 13, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.pruneDomainIdx != wantIdx || reloaded.baseBit != wantBase {
		t.Fatalf("schedule did not persist: got (idx %d, base %d), want (%d, %d)",
			reloaded.pruneDomainIdx, reloaded.baseBit, wantIdx, wantBase)
	}
	for k, want := range live {
		got, err := reloaded.Get([]byte(k))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reloaded Get(%x) = %q, %v; want %q", []byte(k)[:4], got, err, want)
		}
	}
	// The pruned keys are archive hits; everything else is hot.
	for i := 0; i < 3; i++ {
		k := shardBitKey13(0x000, 0, i, i)
		if _, fromArchive, err := reloaded.GetValueRef(k); err != nil || !fromArchive {
			t.Fatalf("d0 key %d after reload: fromArchive=%v err=%v; want an archive hit", i, fromArchive, err)
		}
	}
	if _, fromArchive, err := reloaded.GetValueRef(shardBitKey13(0xabc, 1, 1, 3009)); err != nil || fromArchive {
		t.Fatalf("unpruned key after reload: fromArchive=%v err=%v; want a hot hit", fromArchive, err)
	}
	if err := reloaded.ensureHotLocked(); err != nil {
		t.Fatal(err)
	}
	if err := reloaded.hot.VerifyAggregates(); err != nil {
		t.Fatalf("aggregates after reload: %v", err)
	}
	// A no-op recommit reproduces the committed root byte-for-byte.
	again, err := reloaded.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, root) {
		t.Fatalf("recommit after reload: %x != committed %x", again, root)
	}
}

// TestMPTShardDepthBits19Sanity pins the target validation tier D=19
// (524,288 domains): routing samples, and a real prune of domain 0 split from
// its sibling domain 1 — the two share the all-zero 4-nibble navigation
// prefix and differ only in bit 18, so the split is exercised entirely by the
// per-leaf bit filter.
func TestMPTShardDepthBits19Sanity(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{ShardDepthBits: 19, CuckooBuckets: 32, CuckooSlots: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got := tr.domainCount(); got != 524288 {
		t.Fatalf("domainCount = %d, want 2^19 = 524288", got)
	}
	if got := tr.domainID(bytes.Repeat([]byte{0xff}, 32)); got != 0x7ffff {
		t.Fatalf("domainID(all-ones) = %d, want 0x7ffff", got)
	}

	mk := func(bit18, seq int) []byte {
		k := make([]byte, 32)
		k[2] = byte(bit18) << 5 // bit 18 splits domains 0 and 1
		k[29] = byte(seq >> 8)
		k[30] = byte(seq)
		k[31] = byte(seq*3 + 1)
		return k
	}
	if got := tr.domainID(mk(0, 0)); got != 0 {
		t.Fatalf("domainID(bit18=0 key) = %d, want 0", got)
	}
	if got := tr.domainID(mk(1, 0)); got != 1 {
		t.Fatalf("domainID(bit18=1 key) = %d, want 1", got)
	}

	tr.pruneDomainIdx = 1 // domain 0 writes take base bit 0, domain 1 takes 1
	var keysA, keysB [][]byte
	for i := 0; i < 3; i++ {
		kA, kB := mk(0, i), mk(1, 100+i)
		if err := tr.Put(kA, []byte(fmt.Sprintf("A%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := tr.Put(kB, []byte(fmt.Sprintf("B%d", i))); err != nil {
			t.Fatal(err)
		}
		keysA = append(keysA, kA)
		keysB = append(keysB, kB)
	}
	tr.pruneDomainIdx = 0
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	for i, k := range keysA {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || !fromArchive {
			t.Fatalf("D19 A key %d: fromArchive=%v err=%v; want an archive hit", i, fromArchive, err)
		}
	}
	for i, k := range keysB {
		if _, fromArchive, err := tr.GetValueRef(k); err != nil || fromArchive {
			t.Fatalf("D19 B key %d: fromArchive=%v err=%v; want a hot hit", i, fromArchive, err)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
}

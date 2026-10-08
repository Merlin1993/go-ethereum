package mpt

// ---------------------------------------------------------------------------
// 2026-10-06 D=12 validation failure (batch 12915): "mpt archive: ECMH
// commitment mismatch on bucket ..." under MPT_ECMH_VERIFY=1 after ~3.15
// prune rotations. Root cause: the v3 write path dropped the archived-copy
// release (EVM read-before-write ruling), so a blind rewrite of an archived
// key re-enters the archive on the next rotation. bucketAppendLocked's
// overwrite branch added the new value's ECMH item without subtracting the
// old one (and double-counted the entry), breaking the commitment == payload
// multiset invariant; the multi-hit probe also returned the first route hit
// (oldest copy) and promoted it, resurrecting stale values and leaving the
// newer duplicate behind. The tests below script exactly that workload.
// ---------------------------------------------------------------------------

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
)

// verifyAllBucketsLocked recomputes every resident bucket's commitment from
// its payload and asserts count == payload size: the per-bucket form of the
// "commitment equals payload multiset" invariant.
func verifyAllBucketsLocked(t *testing.T, tr *Trie) {
	t.Helper()
	for _, b := range tr.buckets {
		if b.payloadGone {
			continue
		}
		if err := tr.loadBucketLocked(b); err != nil {
			t.Fatalf("load bucket %x: %v", b.path, err)
		}
		if b.count != len(b.entries) {
			t.Fatalf("bucket %x: count=%d but payload holds %d entries", b.path, b.count, len(b.entries))
		}
		if err := tr.verifyBucketLocked(b); err != nil {
			t.Fatalf("bucket %x: %v", b.path, err)
		}
	}
}

// archivedCopiesLocked reports how many archive buckets genuinely hold key.
func archivedCopiesLocked(t *testing.T, tr *Trie, key []byte) int {
	t.Helper()
	copies := 0
	for _, b := range tr.buckets {
		if b.payloadGone {
			continue
		}
		if err := tr.loadBucketLocked(b); err != nil {
			t.Fatalf("load bucket %x: %v", b.path, err)
		}
		if _, ok := b.entries[string(key)]; ok {
			copies++
		}
	}
	return copies
}

// TestMPTBlindWriteSameBucketCommitment scripts the minimal corruption: one
// key archived, blindly rewritten, and re-archived into the SAME bucket
// (single-entry coverage prefix matches the rewrite exactly), then audited.
func TestMPTBlindWriteSameBucketCommitment(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)

	key := make([]byte, 32)
	key[0] = 0x0a // high nibble 0: prune domain 0 at depth 4
	v1 := bytes.Repeat([]byte{0x11}, 32)
	v2 := bytes.Repeat([]byte{0x22}, 32)

	if err := tr.Put(key, v1); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0) // archives key -> v1

	// Blind rewrite: no read, so the archived v1 copy survives underneath.
	if err := tr.Put(key, v2); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0) // re-archives key -> v2 into the same bucket

	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, key); copies != 1 {
		t.Fatalf("want exactly 1 archived copy after same-bucket rewrite, got %d", copies)
	}
	verifyAllBucketsLocked(t, tr) // pre-fix: ECMH commitment mismatch here
	tr.mu.Unlock()

	// The probe must surface the newest value, and read-activation must
	// leave no archived duplicate behind.
	got, err := tr.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Get returned stale value %x, want %x", got, v2)
	}
	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, key); copies != 0 {
		t.Fatalf("promotion left %d archived copies behind", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
}

// TestMPTBlindWriteCrossBucketDuplicate forces the duplicate into a SECOND
// bucket (first one full) and asserts the probe redeems the newest copy
// (bucket sequence discriminator) while promotion cleans every hit bucket.
func TestMPTBlindWriteCrossBucketDuplicate(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits:            4,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		BucketCapacity:            2, // two entries fill a bucket
		ActivateArchivedKeyOnRead: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	domain0Key := func(tag byte) []byte {
		k := make([]byte, 32)
		k[0] = tag & 0x0f // high nibble 0: prune domain 0
		k[31] = tag
		return k
	}
	k1, k2, k3 := domain0Key(1), domain0Key(2), domain0Key(3)
	v1 := bytes.Repeat([]byte{0x11}, 32)
	v2 := bytes.Repeat([]byte{0x22}, 32)
	w1 := bytes.Repeat([]byte{0x33}, 32)
	w3 := bytes.Repeat([]byte{0x44}, 32)

	// Bucket 1: {k1: v1, k2: w1} — at capacity.
	if err := tr.Put(k1, v1); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(k2, w1); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	// Blind rewrite of k1 plus a fresh key: bucket 1 is full, so the next
	// rotation materializes bucket 2 holding {k1: v2, k3: w3} — k1 now
	// lives in two buckets with different values.
	if err := tr.Put(k1, v2); err != nil {
		t.Fatal(err)
	}
	if err := tr.Put(k3, w3); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, k1); copies != 2 {
		t.Fatalf("setup: want k1 duplicated across 2 buckets, got %d", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()

	// The probe must return the NEWEST copy (v2), and promotion must clean
	// the duplicate from every hit bucket without breaking any commitment.
	got, err := tr.Get(k1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Get returned stale value %x, want newest %x", got, v2)
	}
	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, k1); copies != 0 {
		t.Fatalf("promotion left %d archived copies of k1 behind", copies)
	}
	if copies := archivedCopiesLocked(t, tr, k2); copies != 1 {
		t.Fatalf("unrelated key k2 lost: %d copies", copies)
	}
	if copies := archivedCopiesLocked(t, tr, k3); copies != 1 {
		t.Fatalf("unrelated key k3 lost: %d copies", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()

	// The other keys still redeem their own values.
	if got, err := tr.Get(k2); err != nil || !bytes.Equal(got, w1) {
		t.Fatalf("k2: got %x err %v, want %x", got, err, w1)
	}
	if got, err := tr.Get(k3); err != nil || !bytes.Equal(got, w3) {
		t.Fatalf("k3: got %x err %v, want %x", got, err, w3)
	}
	tr.mu.Lock()
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
}

// TestMPTBlindDeleteThenRewrite covers the twin violation: a blind delete
// leaves the archived copy behind, a blind rewrite then re-archives the key
// into the same bucket — the overwrite path must still keep commitment ==
// payload multiset.
func TestMPTBlindDeleteThenRewrite(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)

	key := make([]byte, 32)
	key[0] = 0x05 // domain 0
	key[31] = 0x99
	v1 := bytes.Repeat([]byte{0x11}, 32)
	v2 := bytes.Repeat([]byte{0x22}, 32)

	if err := tr.Put(key, v1); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	// Blind delete: archived v1 copy survives underneath.
	if err := tr.Delete(key); err != nil {
		t.Fatal(err)
	}
	// Blind rewrite of the same key, then re-archive.
	if err := tr.Put(key, v2); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)

	tr.mu.Lock()
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()

	got, err := tr.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v2) {
		t.Fatalf("Get returned stale value %x, want %x", got, v2)
	}
	tr.mu.Lock()
	if copies := archivedCopiesLocked(t, tr, key); copies != 0 {
		t.Fatalf("promotion left %d archived copies behind", copies)
	}
	verifyAllBucketsLocked(t, tr)
	tr.mu.Unlock()
}

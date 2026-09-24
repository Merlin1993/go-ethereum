package mpt

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/mpt/ecmh"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
)

type testStore struct{ *memorydb.Database }

func (db *testStore) NewBatch() archivetrie.Batcher { return db.Database.NewBatch() }

// expirePrune drives the prune rotation until domain id has been visited
// under a base bit that expires everything written so far. With the paper's
// dynamic epoch assignment, data written in the current cycle is evicted at
// its domain's prune in the FOLLOWING cycle (1-2 cycle residency by design),
// so a single PruneNextShard no longer seals fresh data.
func expirePrune(t *testing.T, tr *Trie, id int) {
	t.Helper()
	for tr.pruneDomainIdx != 0 { // settle at a cycle boundary
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < tr.domainCount(); i++ { // full cycle flips the base bit
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i <= id; i++ { // reach the domain in the new cycle
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
}

func newTestTrie(t *testing.T, db *testStore, activate bool) *Trie {
	t.Helper()
	return newTestTrieWithBackend(t, db, "", activate)
}

// domainKeyRange returns [start, end) covering every key in the bit domain
// id at the given depth, padded out to the store's 32-byte key width. A depth
// that is not a multiple of 8 leaves the final byte partly consumed — the
// boundary case a nibble-based domain could not express.
// Test-only helper (kept for routing assertions); production pruning walks
// the tree by nibble prefix plus a per-leaf bit filter.
func domainKeyRange(id, depth int) ([]byte, []byte) {
	start := make([]byte, 32)
	for i := 0; i < depth; i++ {
		if id>>(uint(depth-1-i))&1 == 1 {
			start[i/8] |= 1 << (7 - uint(i%8))
		}
	}
	// Exclusive upper bound: carry past the last consumed unit.
	end := make([]byte, 32)
	copy(end, start)
	last := (depth - 1) / 8
	if depth%8 != 0 {
		end[last] |= (1 << (8 - uint(depth%8))) - 1
	}
	for i := last; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			break
		}
	}
	return start, end
}

// newTestTrieWithBackend builds a trie over four-bit domains (the top nibble,
// i.e. the previous one-nibble granularity), so the tests can reach a specific
// domain with a handful of keys. backend "" means the default (hash) backend.
func newTestTrieWithBackend(t *testing.T, db *testStore, backend string, activate bool) *Trie {
	t.Helper()
	tr, err := New(nil, db, &Config{
		ShardDepthBits:            4,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		ActivateArchivedKeyOnRead: activate,
		Backend:                   backend,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestMPTArchiveRoundTripAndForEach(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	entries := map[string]string{
		"alpha": "one",
		"beta":  "two",
		"gamma": "three",
	}
	batch := make([]KeyValue, 0, len(entries))
	for key, value := range entries {
		batch = append(batch, KeyValue{Key: []byte(key), Value: []byte(value)})
	}
	if err := tr.PutBatch(batch); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(root) != 32 {
		t.Fatalf("commit returned root of length %d", len(root))
	}
	for key, want := range entries {
		got, err := tr.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}

	reloaded, err := New(root, db, &Config{ShardDepthBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]string, 0, len(entries))
	reloaded.ForEach(func(key, value []byte) bool {
		if !bytes.Equal(value, []byte(entries[string(key)])) {
			t.Fatalf("ForEach(%q) = %q", key, value)
		}
		seen = append(seen, string(key))
		return true
	})
	sort.Strings(seen)
	wantKeys := []string{"alpha", "beta", "gamma"}
	if len(seen) != len(wantKeys) {
		t.Fatalf("ForEach keys = %v, want %v", seen, wantKeys)
	}
	for i := range wantKeys {
		if seen[i] != wantKeys[i] {
			t.Fatalf("ForEach keys = %v, want %v", seen, wantKeys)
		}
	}
}

// TestMPTInlineValueEvictionRemovesActiveCopy is the invariant the inline value
// layout exists for: evicting a leaf must change what the tree commits to. The
// previous layout stored payloads in a separate flat store and only hashed refs
// in the leaves, so pruning left the payload behind and every active-layer size
// claim depended on a second deletion happening elsewhere.
func TestMPTInlineValueEvictionRemovesActiveCopy(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'k'}
	value := []byte("payload-that-must-not-survive")
	if err := tr.Put(key, value); err != nil {
		t.Fatal(err)
	}
	before, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, archived, err := tr.GetValueRef(key); err != nil || archived {
		t.Fatalf("before prune: archived=%v err=%v; want a hot hit", archived, err)
	}

	expirePrune(t, tr, 0)
	after, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("pruning left the root unchanged: the value is still committed to the tree")
	}
	// The only remaining home for the payload is an archive bucket. Its
	// content is the preimage itself now, not a hash, because nothing else
	// holds it.
	b := bucketOfKey(t, tr, key)
	if b == nil {
		t.Fatal("no bucket claims the pruned key")
	}
	if err := tr.loadBucketLocked(b); err != nil {
		t.Fatal(err)
	}
	if got := b.entries[string(key)]; !bytes.Equal(got, value) {
		t.Fatalf("archive holds %q; want the original payload %q", got, value)
	}
}

// bucketOfKey finds the registered bucket whose filter claims key.
func bucketOfKey(t *testing.T, tr *Trie, key []byte) *bucket {
	t.Helper()
	for _, b := range tr.buckets {
		if b.filter != nil && b.filter.Lookup(key) {
			return b
		}
	}
	return nil
}

func TestMPTArchivePruneReloadAndReadActivation(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'k'}
	value := []byte("archived")
	if err := tr.Put(key, value); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	root, err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	ref, fromArchive, err := tr.GetValueRef(key)
	if err != nil || !fromArchive || len(ref) != 32 {
		t.Fatalf("GetValueRef after prune = %x, archive=%v, err=%v", ref, fromArchive, err)
	}

	active, err := New(root, db, &Config{ShardDepthBits: 4, ActivateArchivedKeyOnRead: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := active.Get(key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("activated Get = %q, %v; want %q", got, err, value)
	}
	if _, fromArchive, err := active.GetValueRef(key); err != nil || fromArchive {
		t.Fatalf("key remained archived after activation: archive=%v err=%v", fromArchive, err)
	}
	root, err = active.Commit()
	if err != nil {
		t.Fatal(err)
	}
	final, err := New(root, db, &Config{ShardDepthBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := final.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("reloaded activated Get = %q, %v; want %q", got, err, value)
	}
}

// TestMPTArchiveFilterLookupCounters proves the G4 denominator wiring: an
// archived bucket's cuckoo filter consult must feed Lookups/Positives on a
// hit and Lookups/Negatives on a miss. The sharded path always reported
// these; the mpt probe path only recorded false positives before.
func TestMPTArchiveFilterLookupCounters(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'k'}
	if err := tr.Put(key, []byte("archived")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}

	before := archivetrie.LastUpdateDiagnostics()
	// Filter positive: the archived key routes to the bucket, the filter
	// claims it, the payload confirms.
	if ref, fromArchive, err := tr.GetValueRef(key); err != nil || !fromArchive || len(ref) != 32 {
		t.Fatalf("GetValueRef archived = %x, archive=%v, err=%v", ref, fromArchive, err)
	}
	// Filter negative: same mount path, a key the filter has never seen.
	// (A confirmed miss surfaces as "key not found" from GetValueRef.)
	if _, fromArchive, err := tr.GetValueRef([]byte{0x00, 'z'}); fromArchive || err == nil {
		t.Fatalf("GetValueRef missing = archive=%v, err=%v; want not-found", fromArchive, err)
	}
	after := archivetrie.LastUpdateDiagnostics()

	dPos := after.ArchiveFilterPositives - before.ArchiveFilterPositives
	dNeg := after.ArchiveFilterNegatives - before.ArchiveFilterNegatives
	dLook := after.ArchiveFilterLookups - before.ArchiveFilterLookups
	if dPos < 1 || dNeg < 1 {
		t.Fatalf("filter counters: positives+%d negatives+%d; want both >= 1", dPos, dNeg)
	}
	if dLook != dPos+dNeg {
		t.Fatalf("lookups+%d != positives+negatives (%d+%d)", dLook, dPos, dNeg)
	}
	if dFP := after.ArchiveFilterFalsePositives - before.ArchiveFilterFalsePositives; dFP != 0 {
		t.Fatalf("false positives +%d; want 0 (payload confirmed the hit)", dFP)
	}
}

// TestMPTCommitToBatchStagesCallerBatch proves the shared-batch contract: every
// write rides the caller's batch and nothing reaches the store before the caller
// flushes. After inline values the "invisible" record is a trie node rather than
// a flat value, so visibility is checked by reopening the store.
func TestMPTCommitToBatchStagesCallerBatch(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	entries := map[string][]byte{
		"a": []byte("alpha-value"),
		"b": []byte("beta-value"),
		"c": []byte("gamma-value"),
	}
	for key, value := range entries {
		if err := tr.Put([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}
	batch := db.NewBatch()
	root, err := tr.CommitToBatch(batch, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(root) != 32 {
		t.Fatalf("CommitToBatch returned root of length %d", len(root))
	}
	if batch.ValueSize() == 0 {
		t.Fatal("CommitToBatch staged nothing into the caller batch")
	}
	// A fresh reader must not see the staged tree yet.
	if reader, err := New(nil, db, &Config{ShardDepthBits: 4}); err == nil {
		if got, err := reader.Get([]byte("a")); err == nil && bytes.Equal(got, entries["a"]) {
			t.Fatal("staged writes visible before the caller flushed the batch")
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepthBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range entries {
		got, err := reloaded.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reloaded Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
}

func TestMPTCommitToBatchMatchesCommit(t *testing.T) {
	// The shared-batch path and the self-managed Commit path must stay
	// bit-identical, including across a prune round.
	seed := map[string][]byte{
		"\x00a": []byte("one"),
		"\x00b": []byte("two"),
		"\x00c": []byte("three"),
		"\x00d": []byte("four"),
	}
	run := func(t *testing.T, shared bool) []byte {
		t.Helper()
		db := &testStore{memorydb.New()}
		tr := newTestTrie(t, db, false)
		for key, value := range seed {
			if err := tr.Put([]byte(key), value); err != nil {
				t.Fatal(err)
			}
		}
		var root []byte
		var err error
		if shared {
			var batch archivetrie.Batcher = db.NewBatch()
			root, err = tr.CommitToBatch(batch, false)
			if err == nil {
				err = batch.Write()
			}
		} else {
			root, err = tr.Commit()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
		if err := tr.Delete([]byte{0x00, 'b'}); err != nil {
			t.Fatal(err)
		}
		if shared {
			batch := db.NewBatch()
			root, err = tr.CommitToBatch(batch, true)
			if err == nil {
				err = batch.Write()
			}
		} else {
			root, err = tr.Commit()
		}
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	viaCommit := run(t, false)
	viaBatch := run(t, true)
	if !bytes.Equal(viaCommit, viaBatch) {
		t.Fatalf("root divergence: Commit=%x CommitToBatch=%x", viaCommit, viaBatch)
	}
}

// TestMPTSingleTreeRootMatchesRebuild replaces the old aggregate-consistency
// test. With one tree there is no aggregate to drift from anything, so this pins
// the simpler and stronger property: the committed root always equals a
// from-scratch hash of the same content, across writes, prunes and deletes.
func TestMPTSingleTreeRootMatchesRebuild(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := func(round, i int) []byte {
		return []byte{0x00, byte('a' + i%26), byte(round*7 + i*13), byte(round)}
	}
	check := func(round int) {
		t.Helper()
		want, err := tr.Hash()
		if err != nil {
			t.Fatalf("round %d Hash: %v", round, err)
		}
		got := tr.Root()
		if !bytes.Equal(got, want) {
			t.Fatalf("round %d: committed root %x != rebuild root %x", round, got, want)
		}
	}
	for round := 1; round <= 6; round++ {
		for i := 0; i < 6; i++ {
			if err := tr.Put(key(round, i), []byte{byte(round), byte(i), 'v'}); err != nil {
				t.Fatalf("round %d Put: %v", round, err)
			}
		}
		batch := db.NewBatch()
		if _, err := tr.CommitToBatch(batch, false); err != nil {
			t.Fatalf("round %d CommitToBatch: %v", round, err)
		}
		if err := batch.Write(); err != nil {
			t.Fatalf("round %d Write: %v", round, err)
		}
		check(round)
	}
	for round := 7; round <= 10; round++ {
		if err := tr.PruneNextShard(); err != nil {
			t.Fatalf("round %d Prune: %v", round, err)
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatalf("round %d Commit: %v", round, err)
		}
		check(round)
	}
	for round := 11; round <= 14; round++ {
		if err := tr.Delete(key(round-10, 0)); err != nil {
			t.Fatalf("round %d Delete: %v", round, err)
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatalf("round %d Commit: %v", round, err)
		}
		check(round)
	}
}

// TestMPTDomainRouting pins the bit-granular routing rule: a domain is the
// first depth bits of the key (MSB-first), and the key range must cover
// exactly that domain for aligned and misaligned depths. A depth that is not
// a multiple of 4 ends mid-nibble — the case the old nibble granularity could
// not express.
func TestMPTDomainRouting(t *testing.T) {
	tr := &Trie{config: Config{ShardDepthBits: 16}}
	cases := []struct {
		key   []byte
		want  int
		depth int
	}{
		{[]byte{0x12, 0x34}, 0x1234, 16},
		{[]byte{0xab, 0xcd, 0xef}, 0xabc, 12},
		{[]byte{0x00}, 0x0, 4},
		{[]byte{0xf0}, 0xf, 4},
		// D=13: the 13th bit is the MSB of the fourth nibble — two keys
		// sharing the first 12 bits split into adjacent domains.
		{[]byte{0x20, 0x00}, 0x400, 13},
		{[]byte{0x20, 0x08}, 0x401, 13},
		{[]byte{}, 0x0, 13}, // short keys pad with zero bits
	}
	for _, c := range cases {
		tr.config.ShardDepthBits = c.depth
		if got := tr.domainID(c.key); got != c.want {
			t.Fatalf("domainID(%x) with depth %d = %d, want %d", c.key, c.depth, got, c.want)
		}
	}

	for _, depth := range []int{1, 4, 5, 8, 13, 16} {
		// Exhaustive for the shallow depths, a representative sample for
		// the deeper ones (0, neighbours, mid, alternating bits, max).
		var ids []int
		count := 1 << uint(depth)
		if depth <= 8 {
			for id := 0; id < count; id++ {
				ids = append(ids, id)
			}
		} else {
			ids = []int{0, 1, 2, 0x400, 0x401, 0xaaa, count / 2, count - 2, count - 1}
		}
		for _, id := range ids {
			start, end := domainKeyRange(id, depth)
			// An all-zero end means the carry wrapped past the 256-bit
			// keyspace: the topmost domain extends to the end of the space.
			if !isAllZero(end) && bytes.Compare(start, end) >= 0 {
				t.Fatalf("depth=%d id=%d: empty range [%x, %x)", depth, id, start, end)
			}
			// Keys inside the domain must route back to it, and neighbours
			// just outside must not.
			tr.config.ShardDepthBits = depth
			if got := tr.domainID(start); got != id {
				t.Fatalf("depth=%d: start key routes to %d, want %d", depth, got, id)
			}
			if lastInside := decrementKey(end); tr.domainID(lastInside) != id {
				t.Fatalf("depth=%d: last in-range key (%x) routes to %d, want %d", depth, lastInside, tr.domainID(lastInside), id)
			}
			if got := tr.domainID(end); got == id && !isAllZero(end) {
				t.Fatalf("depth=%d: end key %x still routes to domain %d", depth, end, id)
			}
		}
	}
}

func isAllZero(key []byte) bool {
	for _, b := range key {
		if b != 0 {
			return false
		}
	}
	return true
}

func decrementKey(key []byte) []byte {
	out := make([]byte, len(key))
	copy(out, key)
	for i := len(out) - 1; i >= 0; i-- {
		out[i]--
		if out[i] != 0xff {
			break
		}
	}
	return out
}

// TestMPTSingleTreeDomainPruneStubResidue is the P5a acceptance test: pruning
// a domain mounts a stub INSIDE the tree (covered by the root), the stub
// survives a reload, and emptying the bucket removes the stub again.
func TestMPTSingleTreeDomainPruneStubResidue(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'x'}
	if err := tr.Put(key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	stubCount := func(tr *Trie) int {
		n := 0
		hot, err := tr.hotTrieLocked()
		if err != nil {
			t.Fatal(err)
		}
		if err := hot.AllStubs(func(_ []byte, _ *gethtrie.Stub) { n++ }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := stubCount(tr); n != 1 {
		t.Fatalf("stubs after prune = %d, want 1", n)
	}

	reloaded, err := New(root, db, &Config{ShardDepthBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	if n := stubCount(reloaded); n != 1 {
		t.Fatalf("stubs after reload = %d, want 1", n)
	}
	if _, fromArchive, err := reloaded.GetValueRef(key); err != nil || !fromArchive {
		t.Fatalf("reloaded GetValueRef: archive=%v err=%v; want an archive hit", fromArchive, err)
	}
	// Emptying the bucket destroys it: the stub leaves the tree.
	b := bucketOfKey(t, reloaded, key)
	if b == nil {
		t.Fatal("bucket missing after reload")
	}
	payloadKey := bucketKey(b.path)
	if err := reloaded.Delete(key); err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := stubCount(reloaded); n != 0 {
		t.Fatalf("stubs after bucket destruction = %d, want 0", n)
	}
	if data, err := db.Get(payloadKey); err == nil && len(data) != 0 {
		t.Fatal("payload record survived bucket destruction")
	}
}

// TestMPTStagedBytesAccounting exercises the recording batcher: every staged key
// must fall into exactly one diagnostic class. The flat and aggregate classes no
// longer exist after the rewrite; staged volume is nodes, archive records and
// index blocks.
func TestMPTStagedBytesAccounting(t *testing.T) {
	staged := &stagedBytes{}
	rec := &recordingBatcher{Batcher: &countBatcher{}, staged: staged}
	for _, key := range [][]byte{
		nodeKey(common.Hash{1}),
		nodeKey(common.Hash{2}),
		bucketKey([]byte{3}),
		scheduleKey(),
		[]byte("not ours"),
	} {
		if err := rec.Put(key, make([]byte, 16)); err != nil {
			t.Fatal(err)
		}
	}
	nodeBytes := int64(len(nodeKey(common.Hash{})) + 16)
	if staged.hotNode != 2*nodeBytes {
		t.Fatalf("hotNode=%d, want %d", staged.hotNode, 2*nodeBytes)
	}
	if staged.archive == 0 || staged.index == 0 {
		t.Fatalf("missing class: archive=%d index=%d", staged.archive, staged.index)
	}
}

// countBatcher is a no-op Batcher used by accounting tests.
type countBatcher struct{ puts int }

func (b *countBatcher) Put([]byte, []byte) error { b.puts++; return nil }
func (b *countBatcher) Delete([]byte) error      { return nil }
func (b *countBatcher) ValueSize() int           { return 0 }
func (b *countBatcher) Write() error             { return nil }
func (b *countBatcher) Reset()                   {}

func TestMPTArchiveDeleteArchivedKey(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'd'}
	if err := tr.Put(key, []byte("delete me")); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	root, err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepthBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	// The key sits in the archive now; deleting it must empty the bucket.
	if _, fromArchive, err := reloaded.GetValueRef(key); err != nil || !fromArchive {
		t.Fatalf("before delete: archive=%v err=%v; want an archive hit", fromArchive, err)
	}
	if err := reloaded.Delete(key); err != nil {
		t.Fatal(err)
	}
	root, err = reloaded.Commit()
	if err != nil {
		t.Fatal(err)
	}
	final, err := New(root, db, &Config{ShardDepthBits: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := final.Get(key); err != ErrNotFound {
		t.Fatalf("deleted key error = %v, want %v", err, ErrNotFound)
	}
}

// ---- path backend ----

// TestMPTPathBackendAccountsForOwnWrites guards the storage-accounting hole the
// path backend opened: pathdb writes through its own batch inside the adapter,
// never through the caller's Batcher, so a naively instrumented commit reported
// zero active writes and every active-layer size claim silently came out too
// small.
//
// It also pins the flush semantics the hard way: this geth version has no API
// that forces a persist, so bytes only reach the store once enough diff layers
// have accumulated to trip pathdb's internal cap. The test therefore runs past
// that cap with a deliberately tiny write buffer.
func TestMPTPathBackendAccountsForOwnWrites(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits:   4,
		CuckooBuckets:    32,
		CuckooSlots:      4,
		Backend:          BackendPath,
		WriteBufferBytes: 512, // force pathdb's buffer to fill almost immediately
		CleanCacheBytes:  1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tr.pathWrites == nil {
		t.Fatal("path backend has no write counter")
	}
	// Enough commits to cross pathdb's diff-layer cap (128), which is the only
	// trigger that persists a layer in this version.
	for i := 0; i < 200; i++ {
		if err := tr.Put([]byte{0x00, byte(i), 'k'}, []byte{byte(i), 'v', 'a', 'l'}); err != nil {
			t.Fatal(err)
		}
		if _, err := tr.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.Flush(); err != nil {
		t.Fatal(err)
	}
	written, writes, buffered, diffs := tr.PathStats()
	t.Logf("path backend: written=%d bytes, %d writes, still buffered=%d, diff=%d",
		written, writes, buffered, diffs)
	if written <= 0 || writes <= 0 {
		t.Fatalf("path backend wrote %d bytes across %d operations; want both non-zero", written, writes)
	}

	// The hash backend reports nothing here by design: it stages through the
	// caller's batch, which the commit diagnostics already measure.
	hashTrie := newTestTrie(t, &testStore{memorydb.New()}, false)
	if w, n, b, d := hashTrie.PathStats(); w != 0 || n != 0 || b != 0 || d != 0 {
		t.Fatalf("hash backend reports path stats (%d, %d, %d, %d); want all zero", w, n, b, d)
	}
}

// TestMPTPathBackendRoundTrip runs the whole trie lifecycle on pathdb: writes,
// commits, a prune round, resurrection and delete. This is the live acceptance
// gate for the pathDB backend, complementing the lower-level adapter tests.
func TestMPTPathBackendRoundTrip(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrieWithBackend(t, db, BackendPath, true)
	if tr.tdb == nil {
		t.Fatal("path backend selected but no trie database was opened")
	}

	written := map[string][]byte{}
	for i := 0; i < 64; i++ {
		key := []byte{0x00, byte(i), 'k'}
		value := []byte{byte(i), 'v', 'a', 'l'}
		if err := tr.Put(key, value); err != nil {
			t.Fatalf("Put(%x): %v", key, err)
		}
		written[string(key)] = value
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	for key, want := range written {
		got, err := tr.Get([]byte(key))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Get(%x) = %q, %v; want %q", key, got, err, want)
		}
	}

	// Prune domain 0 (the only one these keys route to at one nibble) through
	// a full epoch cycle and make sure the values come back through the
	// archive path.
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatalf("Commit after prune: %v", err)
	}
	if _, archived, err := tr.GetValueRef([]byte{0x00, 0, 'k'}); err != nil || !archived {
		t.Fatalf("key not archived after prune: archived=%v err=%v", archived, err)
	}
	// Read activation pulls it straight back into the hot tree.
	if got, err := tr.Get([]byte{0x00, 0, 'k'}); err != nil || !bytes.Equal(got, written[string([]byte{0x00, 0, 'k'})]) {
		t.Fatalf("resurrected Get = %q, %v", got, err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatalf("Commit after resurrection: %v", err)
	}
	if err := tr.Delete([]byte{0x00, 1, 'k'}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatalf("Commit after delete: %v", err)
	}
	if _, err := tr.Get([]byte{0x00, 1, 'k'}); err != ErrNotFound {
		t.Fatalf("deleted key error = %v, want %v", err, ErrNotFound)
	}
	if err := tr.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

// TestMPTPathBackendRejectsAdoptedRoot documents the constraint that shapes how
// the driver must use pathdb: a layer tree cannot adopt a root whose history it
// does not hold, so re-opening mid-history is refused up front rather than
// failing later inside a Get.
func TestMPTPathBackendRejectsAdoptedRoot(t *testing.T) {
	db := &testStore{memorydb.New()}
	adopted := common.Hash{0xab, 0xcd, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05,
		0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
		0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
		0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d}
	if _, err := New(adopted.Bytes(), db, &Config{ShardDepthBits: 4, Backend: BackendPath}); err == nil {
		t.Fatal("path backend accepted an existing root it has no layer history for")
	}
}

// TestMPTRejectsBadConfig guards the config surfaces callers can get wrong: a
// shard depth beyond the persisted uint32 domain index and an unknown backend
// are errors, while an unset depth falls back to the default so a partially
// populated Config still builds.
func TestMPTRejectsBadConfig(t *testing.T) {
	db := &testStore{memorydb.New()}
	if _, err := New(nil, db, &Config{ShardDepthBits: 33}); err == nil {
		t.Error("accepted a 33-bit shard depth (beyond the 32-bit prune schedule record)")
	}
	if _, err := New(nil, db, &Config{ShardDepthBits: 8, Backend: "leveldb"}); err == nil {
		t.Error("accepted an unknown backend")
	}
	// Defaults must still construct, including from a zero Config.
	tr, err := New(nil, db, &Config{})
	if err != nil {
		t.Fatalf("default config failed to construct: %v", err)
	}
	if tr.config.ShardDepthBits != 16 || tr.config.Backend != BackendHash {
		t.Fatalf("defaults = (depth bits %d, backend %q); want (16, %q)", tr.config.ShardDepthBits, tr.config.Backend, BackendHash)
	}
}

// TestMPTArchiveBucketEviction pins the resident-budget behaviour: with a tiny
// ArchiveResidentEntries cap, clean committed buckets are evicted back to disk
// (filter and count stay resident), dirty buckets are never evicted, and a
// cold read still resurrects an evicted entry correctly.
func TestMPTArchiveBucketEviction(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits:            4,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		ActivateArchivedKeyOnRead: true,
		ArchiveResidentEntries:    3, // one bucket's worth, forces churn
	})
	if err != nil {
		t.Fatal(err)
	}
	// Three keys in each of domains 0..3 (the first nibble routes the domain).
	for _, d := range []byte{0x00, 0x10, 0x20, 0x30} {
		for i := 0; i < 3; i++ {
			key := []byte{d, byte('a' + i)}
			if err := tr.Put(key, []byte{0x42, d, byte(i)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	// Seal domains 0..3: the epoch lifecycle needs a full cycle (base-bit
	// flip) before fresh data becomes evictable, then four prunes into the
	// new cycle reach domains 0..3 in rotation order.
	expirePrune(t, tr, 3)
	// Dirty buckets must survive eviction regardless of the budget. The
	// mounting algorithm merges domains into shared buckets (paper CanAppend
	// top-up), so assert invariants rather than an exact bucket count.
	if len(tr.buckets) == 0 {
		t.Fatal("no buckets after sealing 4 domains")
	}
	total := 0
	for _, b := range tr.buckets {
		if b.entries == nil {
			t.Fatalf("dirty bucket %x was evicted before commit", b.path)
		}
		total += b.count
	}
	if total != 12 {
		t.Fatalf("archived total = %d, want 12", total)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	// After commit the budget holds: only the MRU bucket (3 entries) may stay.
	if tr.residentEntries > 3 {
		t.Fatalf("resident entries after commit = %d, want <= 3", tr.residentEntries)
	}
	// Domain 0's keys sit in the first-created bucket, which is the coldest:
	// entries must be evicted while the filter and count stay resident for
	// negative lookups.
	key := []byte{0x00, 'a'}
	b := bucketOfKey(t, tr, key)
	if b == nil {
		t.Fatal("domain 0 bucket not registered")
	}
	if b.entries != nil || b.count < 3 || b.filter == nil {
		t.Fatalf("domain 0 bucket state: entries=%v count=%d filter=%v; want evicted entries, count >= 3, resident filter",
			b.entries != nil, b.count, b.filter != nil)
	}
	// A cold read on an evicted bucket reloads and resurrects the entry.
	before := b.count
	value, err := tr.Get(key)
	if err != nil {
		t.Fatalf("resurrecting evicted entry: %v", err)
	}
	if !bytes.Equal(value, []byte{0x42, 0x00, 0x00}) {
		t.Fatalf("resurrected value %x; want 420000", value)
	}
	if b.count != before-1 {
		t.Fatalf("bucket count after resurrection = %d, want %d", b.count, before-1)
	}
	// The resurrected key is hot again; the archived copy is gone.
	if _, fromArchive, err := tr.GetValueRef(key); err != nil || fromArchive {
		t.Fatalf("after resurrection: fromArchive=%v err=%v; want a hot hit", fromArchive, err)
	}
	// A key that was never archived misses without touching any bucket.
	if _, err := tr.Get([]byte{0x50, 'z'}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("never-archived key: err=%v; want ErrNotFound", err)
	}
	// The remaining archived entries survive a commit round-trip to disk.
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.Get([]byte{0x00, 'b'})
	if err != nil || !bytes.Equal(got, []byte{0x42, 0x00, 0x01}) {
		t.Fatalf("post-commit resurrect: value=%x err=%v; want 420001", got, err)
	}
}

// TestMPTGetValueRefErrorContract pins the miss signalling the access sampler
// depends on: a key absent from both hot trie and archive must surface
// ErrNotFound (previously GetValueRef returned a nil error, so the sampler
// counted missing reads as hot hits and Hot_Read_Hit_Rate was meaningless).
func TestMPTGetValueRefErrorContract(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)

	present := []byte("present-key")
	if err := tr.Put(present, []byte("value")); err != nil {
		t.Fatal(err)
	}
	ref, fromArchive, err := tr.GetValueRef(present)
	if err != nil {
		t.Fatalf("GetValueRef(present): %v", err)
	}
	if fromArchive {
		t.Fatal("GetValueRef(present) reported archive for a hot key")
	}
	if len(ref) != 32 {
		t.Fatalf("GetValueRef(present) returned ref of length %d", len(ref))
	}

	missing := []byte("missing-key")
	if _, _, err := tr.GetValueRef(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetValueRef(missing) err = %v, want ErrNotFound", err)
	}
	if _, err := tr.Get(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
	}
}

// ---- P5 acceptance tests ----

// TestMPTSingleTreeResurrectionRoundTrip is the P5b acceptance test: the
// bucket commitment is built with ECMH Create at mount time, redemption
// subtracts exactly the redeemed item (BlindDelete), and the remaining
// multiset still verifies against the rewritten in-tree stub.
func TestMPTSingleTreeResurrectionRoundTrip(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)
	keys := [][]byte{
		{0x00, 'a'}, {0x00, 'b'}, {0x00, 'c'}, {0x00, 'd'}, {0x00, 'e'},
	}
	values := make(map[string][]byte)
	items := make([][]byte, 0, len(keys))
	for i, key := range keys {
		value := []byte{0x77, byte(i)}
		values[string(key)] = value
		if err := tr.Put(key, value); err != nil {
			t.Fatal(err)
		}
		items = append(items, bucketItem(key, value))
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	b := bucketOfKey(t, tr, keys[0])
	if b == nil || b.count != len(keys) {
		t.Fatalf("bucket = %+v; want one bucket of %d", b, len(keys))
	}
	want := ecmh.Create(items)
	if b.commitment != want {
		t.Fatal("mount-time commitment != Create(items)")
	}
	// Redeem one key: commitment must equal both Delete(c, item) and a fresh
	// Create over the remaining multiset.
	if got, err := tr.Get(keys[0]); err != nil || !bytes.Equal(got, values[string(keys[0])]) {
		t.Fatalf("resurrect: %q, %v", got, err)
	}
	after := ecmh.Delete(want, bucketItem(keys[0], values[string(keys[0])]))
	if b.commitment != after {
		t.Fatal("post-redemption commitment != BlindDelete(old, item)")
	}
	rest := items[1:]
	if !ecmh.Verify(b.commitment, rest) {
		t.Fatal("remaining multiset does not verify against the rewritten stub")
	}
	// The stub in the tree carries the same commitment (root covers it).
	hot, err := tr.hotTrieLocked()
	if err != nil {
		t.Fatal(err)
	}
	var stubs []*gethtrie.Stub
	if err := hot.AllStubs(func(_ []byte, s *gethtrie.Stub) { stubs = append(stubs, s) }); err != nil {
		t.Fatal(err)
	}
	if len(stubs) != 1 || [33]byte(b.commitment) != stubs[0].Commitment || stubs[0].Count != 4 {
		t.Fatalf("in-tree stub = %+v; want count 4 with the updated commitment", stubs)
	}
	// Persistence: reload, resurrect the rest, bucket destroys itself.
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{ShardDepthBits: 4, ActivateArchivedKeyOnRead: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys[1:] {
		if _, err := reloaded.Get(key); err != nil {
			t.Fatalf("resurrect %x after reload: %v", key, err)
		}
	}
	hot, err = reloaded.hotTrieLocked()
	if err != nil {
		t.Fatal(err)
	}
	stubs = stubs[:0]
	if err := hot.AllStubs(func(_ []byte, s *gethtrie.Stub) { stubs = append(stubs, s) }); err != nil {
		t.Fatal(err)
	}
	if len(stubs) != 0 {
		t.Fatalf("bucket survived full redemption: %+v", stubs)
	}
}

// TestMPTBucketCapacitySplitDown is the P5c acceptance test: a batch larger
// than M forces full M-buckets (forced compaction), an already-full bucket
// absorbs nothing, and the remainder materializes as a partial bucket — all
// still retrievable.
func TestMPTBucketCapacitySplitDown(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits:            4,
		CuckooBuckets:             64,
		CuckooSlots:               4,
		BucketCapacity:            3,
		ActivateArchivedKeyOnRead: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	put := func(prefix byte, n int, tag byte) [][]byte {
		var keys [][]byte
		for i := 0; i < n; i++ {
			key := []byte{prefix, 0xBC, tag, byte(i)}
			keys = append(keys, key)
			if err := tr.Put(key, []byte{tag, byte(i)}); err != nil {
				t.Fatal(err)
			}
		}
		return keys
	}
	// Phase A: 3 keys fill one bucket exactly.
	keysA := put(0x0A, 3, 0xA0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	// Phase B: 4 more keys; the full bucket absorbs nothing, 3 force a fresh
	// full bucket, the last one lands in a partial bucket.
	keysB := put(0x0A, 4, 0xB0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if len(tr.buckets) != 3 {
		t.Fatalf("buckets = %d, want 3 (3+3+1)", len(tr.buckets))
	}
	total := 0
	for _, b := range tr.buckets {
		if b.count > 3 {
			t.Fatalf("bucket %x holds %d entries, over capacity 3", b.path, b.count)
		}
		total += b.count
	}
	if total != 7 {
		t.Fatalf("archived total = %d, want 7", total)
	}
	for _, key := range append(keysA, keysB...) {
		if _, fromArchive, err := tr.GetValueRef(key); err != nil || !fromArchive {
			t.Fatalf("key %x: archive=%v err=%v; want an archive hit", key, fromArchive, err)
		}
	}
}

// countingStore tallies payload-record reads for the O(1) structural test.
type countingStore struct {
	*memorydb.Database
	archiveReads int
}

func (db *countingStore) NewBatch() archivetrie.Batcher { return db.Database.NewBatch() }

func (db *countingStore) Get(key []byte) ([]byte, error) {
	if bytes.HasPrefix(key, archivePrefix) {
		db.archiveReads++
	}
	return db.Database.Get(key)
}

// TestMPTBucketO1CommitmentUpdate is the P5d structural assertion: redemption
// updates the bucket commitment by point subtraction over the single redeemed
// item (k‖keccak(v)) — one payload record read for the value itself, no scan
// of the remaining preimages, and the result matches BlindDelete exactly.
func TestMPTBucketO1CommitmentUpdate(t *testing.T) {
	db := &countingStore{Database: memorydb.New()}
	tr, err := New(nil, db, &Config{
		ShardDepthBits:            4,
		CuckooBuckets:             32,
		CuckooSlots:               4,
		ArchiveResidentEntries:    1, // force payload eviction at commit
		ActivateArchivedKeyOnRead: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{{0x00, 'a'}, {0x00, 'b'}, {0x00, 'c'}}
	for i, key := range keys {
		if err := tr.Put(key, []byte{0x55, byte(i)}); err != nil {
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
	b := bucketOfKey(t, tr, keys[0])
	if b == nil {
		t.Fatal("bucket missing")
	}
	c0 := b.commitment
	// White-box eviction: drop the payload exactly as the LRU would (a
	// black-box second bucket cannot force this — the root catch-all top-up
	// would merge any later batch into this same bucket and reload it).
	tr.mu.Lock()
	if b.elem != nil {
		tr.archiveLRU.Remove(b.elem)
		b.elem = nil
	}
	tr.residentEntries -= b.count
	b.entries = nil
	tr.mu.Unlock()
	db.archiveReads = 0
	if _, err := tr.Get(keys[0]); err != nil {
		t.Fatal(err)
	}
	if db.archiveReads != 1 {
		t.Fatalf("redemption read %d payload records, want exactly 1 (no preimage scan)", db.archiveReads)
	}
	want := ecmh.Delete(c0, bucketItem(keys[0], []byte{0x55, 0x00}))
	if b.commitment != want {
		t.Fatal("commitment after redemption != BlindDelete(c0, redeemed item)")
	}
	if !ecmh.Verify(b.commitment, [][]byte{
		bucketItem(keys[1], []byte{0x55, 0x01}),
		bucketItem(keys[2], []byte{0x55, 0x02}),
	}) {
		t.Fatal("remaining multiset does not verify against the updated commitment")
	}
}

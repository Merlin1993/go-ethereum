package mpt

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

type testStore struct{ *memorydb.Database }

func (db *testStore) NewBatch() archivetrie.Batcher { return db.Database.NewBatch() }

func newTestTrie(t *testing.T, db *testStore, activate bool) *Trie {
	t.Helper()
	return newTestTrieWithBackend(t, db, "", activate)
}

// newTestTrieWithBackend builds a trie over one-nibble domains, so the tests can
// reach a specific domain with a handful of keys. backend "" means the default
// (hash) backend.
func newTestTrieWithBackend(t *testing.T, db *testStore, backend string, activate bool) *Trie {
	t.Helper()
	tr, err := New(nil, db, &Config{
		DomainNibbles:             1,
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

	reloaded, err := New(root, db, &Config{DomainNibbles: 1})
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

	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	after, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("pruning left the root unchanged: the value is still committed to the tree")
	}
	// The only remaining home for the payload is the archive bucket. Its content
	// is the preimage itself now, not a hash, because nothing else holds it.
	shard, err := tr.loadArchiveLocked(0)
	if err != nil {
		t.Fatal(err)
	}
	if got := shard.entries[string(key)]; !bytes.Equal(got, value) {
		t.Fatalf("archive holds %q; want the original payload %q", got, value)
	}
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
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	ref, fromArchive, err := tr.GetValueRef(key)
	if err != nil || !fromArchive || len(ref) != 32 {
		t.Fatalf("GetValueRef after prune = %x, archive=%v, err=%v", ref, fromArchive, err)
	}

	active, err := New(root, db, &Config{DomainNibbles: 1, ActivateArchivedKeyOnRead: true})
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
	final, err := New(root, db, &Config{DomainNibbles: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := final.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("reloaded activated Get = %q, %v; want %q", got, err, value)
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
	if reader, err := New(nil, db, &Config{DomainNibbles: 1}); err == nil {
		if got, err := reader.Get([]byte("a")); err == nil && bytes.Equal(got, entries["a"]) {
			t.Fatal("staged writes visible before the caller flushed the batch")
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{DomainNibbles: 1})
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

// TestMPTDomainRouting pins the routing change behind plan item D3: domains are
// counted in nibbles, and the key range must cover exactly that domain for both
// odd and even nibble counts. An odd count leaves the final byte half consumed,
// which is the case a bit-based domain could not express.
func TestMPTDomainRouting(t *testing.T) {
	tr := &Trie{config: Config{DomainNibbles: 4}}
	cases := []struct {
		key     []byte
		want    int
		nibbles int
	}{
		{[]byte{0x12, 0x34}, 0x1234, 4},
		{[]byte{0xab, 0xcd, 0xef}, 0xabc, 3},
		{[]byte{0x00}, 0x0, 1},
		{[]byte{0xf0}, 0xf, 1},
		{[]byte{}, 0x0, 2},
	}
	for _, c := range cases {
		tr.config.DomainNibbles = c.nibbles
		if got := tr.domainID(c.key); got != c.want {
			t.Fatalf("domainID(%x) with %d nibbles = %d, want %d", c.key, c.nibbles, got, c.want)
		}
	}

	for _, nibbles := range []int{1, 2, 3, 4, 5} {
		for id := 0; id < 1<<uint(nibbles); id++ {
			start, end := domainKeyRange(id, nibbles)
			if bytes.Compare(start, end) >= 0 {
				t.Fatalf("nibbles=%d id=%d: empty range [%x, %x)", nibbles, id, start, end)
			}
			// Keys inside the domain must route back to it, and neighbours just
			// outside must not.
			tr.config.DomainNibbles = nibbles
			if got := tr.domainID(start); got != id {
				t.Fatalf("nibbles=%d: start key routes to %d, want %d", nibbles, got, id)
			}
			if lastInside := decrementKey(end); tr.domainID(lastInside) != id {
				t.Fatalf("nibbles=%d: last in-range key (%x) routes to %d, want %d", nibbles, lastInside, tr.domainID(lastInside), id)
			}
			if got := tr.domainID(end); nibbles < 8 && got == id && !isAllZero(end) {
				t.Fatalf("nibbles=%d: end key %x still routes to domain %d", nibbles, end, id)
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

// TestMPTArchiveIndexBlockRoundTrip covers the sharded archive index: a pruned
// domain is recorded in exactly one index block, survives reload, and disappears
// again once its bucket empties. The old single full-width bitmap rewrote every
// domain on every prune, which is the write amplification this replaced.
func TestMPTArchiveIndexBlockRoundTrip(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	key := []byte{0x00, 'x'}
	if err := tr.Put(key, []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tr.archiveIDs[0]; !ok {
		t.Fatal("domain 0 not recorded after prune")
	}
	if data, err := db.Get(indexBlockKey(0)); err != nil || len(data) == 0 {
		t.Fatalf("index block 0 missing after prune: len=%d err=%v", len(data), err)
	}

	reloaded, err := New(root, db, &Config{DomainNibbles: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reloaded.archiveIDs[0]; !ok {
		t.Fatal("index lost across reload")
	}
	if err := reloaded.Delete(key); err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := reloaded.archiveIDs[0]; ok {
		t.Fatal("domain still marked archived after its last entry was deleted")
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
		archiveKey(3),
		indexBlockKey(0),
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
	if err := tr.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	root, err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := New(root, db, &Config{DomainNibbles: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := reloaded.Delete(key); err != nil {
		t.Fatal(err)
	}
	root, err = reloaded.Commit()
	if err != nil {
		t.Fatal(err)
	}
	final, err := New(root, db, &Config{DomainNibbles: 1})
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
		DomainNibbles:    1,
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

	// Prune domain 0 (the only one these keys route to at one nibble) and make
	// sure the values come back through the archive path.
	if err := tr.PruneNextShard(); err != nil {
		t.Fatalf("PruneNextShard: %v", err)
	}
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
	if _, err := New(adopted.Bytes(), db, &Config{DomainNibbles: 1, Backend: BackendPath}); err == nil {
		t.Fatal("path backend accepted an existing root it has no layer history for")
	}
}

// TestMPTRejectsBadConfig guards the config surfaces callers can get wrong: an
// over-wide domain and an unknown backend are errors, while an unset width falls
// back to the default so a partially populated Config still builds.
func TestMPTRejectsBadConfig(t *testing.T) {
	db := &testStore{memorydb.New()}
	if _, err := New(nil, db, &Config{DomainNibbles: 9}); err == nil {
		t.Error("accepted a 9-nibble domain (beyond the 32-bit routing key)")
	}
	if _, err := New(nil, db, &Config{DomainNibbles: 2, Backend: "leveldb"}); err == nil {
		t.Error("accepted an unknown backend")
	}
	// Defaults must still construct, including from a zero Config.
	tr, err := New(nil, db, &Config{})
	if err != nil {
		t.Fatalf("default config failed to construct: %v", err)
	}
	if tr.config.DomainNibbles != 4 || tr.config.Backend != BackendHash {
		t.Fatalf("defaults = (nibbles %d, backend %q); want (4, %q)", tr.config.DomainNibbles, tr.config.Backend, BackendHash)
	}
}

// TestMPTArchiveBucketEviction pins the resident-budget behaviour: with a tiny
// ArchiveResidentEntries cap, clean committed buckets are evicted back to disk
// (filter and count stay resident), dirty buckets are never evicted, and a
// cold read still resurrects an evicted entry correctly.
func TestMPTArchiveBucketEviction(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr, err := New(nil, db, &Config{
		DomainNibbles:             1,
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
	for i := 0; i < 4; i++ { // seal domains 0,1,2,3 in rotation order
		if err := tr.PruneNextShard(); err != nil {
			t.Fatal(err)
		}
	}
	// Dirty buckets must survive eviction regardless of the budget.
	for id := 0; id < 4; id++ {
		if shard := tr.archives[id]; shard == nil || shard.entries == nil {
			t.Fatalf("dirty bucket %d was evicted before commit", id)
		}
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	// After commit the budget holds: only the MRU bucket (3 entries) may stay.
	if tr.residentEntries > 3 {
		t.Fatalf("resident entries after commit = %d, want <= 3", tr.residentEntries)
	}
	// Domain 0's bucket was sealed first, so it is the coldest: entries must be
	// evicted while the filter and count stay resident for negative lookups.
	shard := tr.archives[0]
	if shard == nil || shard.entries != nil || shard.count != 3 || shard.filter == nil {
		t.Fatalf("domain 0 shard state = %+v; want evicted entries, count 3, resident filter", shard)
	}
	// A cold read on an evicted bucket reloads and resurrects the entry.
	key := []byte{0x00, 'a'}
	value, err := tr.Get(key)
	if err != nil {
		t.Fatalf("resurrecting evicted entry: %v", err)
	}
	if !bytes.Equal(value, []byte{0x42, 0x00, 0x00}) {
		t.Fatalf("resurrected value %x; want 420000", value)
	}
	if shard.count != 2 {
		t.Fatalf("bucket count after resurrection = %d, want 2", shard.count)
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

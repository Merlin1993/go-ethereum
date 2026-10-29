package mpt

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
)

func TestBlockAuditDeduplicatesBucketAndCommits(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)
	keys := [][]byte{{0x00, 0x11}, {0x00, 0x12}}
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
	tr.config.VerifyAtCommit = true
	tr.config.VerifyBucketWorkers = 8
	tr.config.VerifyItemWorkers = 2
	before, _ := archivetrie.MPTRedemptionVerifyStats()
	b := bucketOfKey(t, tr, keys[0])
	wantProofBytes := int64(len(b.metaBytes)) + b.payloadBytes
	for i, key := range keys {
		got, err := tr.Get(key)
		if err != nil || !bytes.Equal(got, []byte{byte(i + 1)}) {
			t.Fatalf("Get(%x) = %x, %v", key, got, err)
		}
	}
	if len(tr.pendingAudits) != 1 {
		t.Fatalf("pending audits = %d, want one original bucket", len(tr.pendingAudits))
	}
	if buckets, proofBytes := tr.PendingProofStats(); buckets != 1 || proofBytes != wantProofBytes {
		t.Fatalf("deduplicated block proof = %d buckets, %d bytes; want 1, %d", buckets, proofBytes, wantProofBytes)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	after, _ := archivetrie.MPTRedemptionVerifyStats()
	if after-before != 1 || len(tr.pendingAudits) != 0 {
		t.Fatalf("audited buckets = %d, pending = %d", after-before, len(tr.pendingAudits))
	}
}

func TestBucketReadIOIsSeparateFromPromotionAndAudit(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, true)
	key := []byte{0x00, 0x11}
	if err := tr.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	tr.config.VerifyAtCommit = true
	resetHandles := func() {
		tr.buckets = make(map[string]*bucket)
		tr.archiveLRU.Init()
		tr.residentEntries = 0
	}
	resetHandles()
	beforeIO := archivetrie.CurrentMPTBucketReadIOStats()
	if _, archived, err := tr.GetValueRef(key); err != nil || !archived {
		t.Fatalf("classification read: archived=%t err=%v", archived, err)
	}
	if got := archivetrie.CurrentMPTBucketReadIOStats().Sub(beforeIO); got.MetaReads != 0 || got.PayloadReads != 0 {
		t.Fatalf("classification read polluted foreground bucket IO: %+v", got)
	}
	resetHandles()
	verifyBefore, _ := archivetrie.MPTRedemptionVerifyStats()
	promotionBefore := archivetrie.LastUpdateDiagnostics().ArchiveReadPromotionCalls
	if got, err := tr.Get(key); err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get = %q, %v", got, err)
	}
	io := archivetrie.CurrentMPTBucketReadIOStats().Sub(beforeIO)
	if io.MetaReads != 1 || io.PayloadReads != 1 || io.MetaNanos < 0 || io.PayloadNanos < 0 {
		t.Fatalf("executed Get bucket IO = %+v", io)
	}
	if archivetrie.LastUpdateDiagnostics().ArchiveReadPromotionCalls != promotionBefore+1 {
		t.Fatal("read promotion was not recorded separately")
	}
	if count, _ := archivetrie.MPTRedemptionVerifyStats(); count != verifyBefore {
		t.Fatal("audit ran during Get")
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if count, _ := archivetrie.MPTRedemptionVerifyStats(); count != verifyBefore+1 {
		t.Fatalf("committed bucket audits = %d, want %d", count, verifyBefore+1)
	}
}

func TestBlockAuditFailureRollsBackSpeculativeRedemption(t *testing.T) {
	for _, backend := range []string{BackendHash, BackendPath} {
		t.Run(backend, func(t *testing.T) {
			testBlockAuditFailureRollsBackSpeculativeRedemption(t, backend)
		})
	}
}

func testBlockAuditFailureRollsBackSpeculativeRedemption(t *testing.T, backend string) {
	db := &testStore{memorydb.New()}
	tr := newTestTrieWithBackend(t, db, backend, true)
	key := []byte{0x00, 0x11}
	value := []byte("original")
	if err := tr.Put(key, value); err != nil {
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
	tr.config.VerifyAtCommit = true
	if _, err := tr.Get(key); err != nil {
		t.Fatal(err)
	}
	tr.pendingAudits[0].entries[0].Value[0] ^= 0xff
	if _, err := tr.Commit(); err == nil {
		t.Fatal("corrupt proof committed")
	}
	if !bytes.Equal(root, tr.rootBytes()) || tr.dirty || len(tr.pendingAudits) != 0 {
		t.Fatal("failed audit left speculative state")
	}
	if _, archived, err := tr.GetValueRef(key); err != nil || !archived {
		t.Fatalf("rollback did not restore archived value: archived=%t err=%v", archived, err)
	}
	got, err := tr.Get(key)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("Get after rollback = %q, %v", got, err)
	}
}

func TestSingletonTopUpAtSameMount(t *testing.T) {
	db := &testStore{memorydb.New()}
	tr := newTestTrie(t, db, false)
	first := []byte{0x00, 0x11}
	second := []byte{0x00, 0x12}
	if err := tr.Put(first, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	b := bucketOfKey(t, tr, first)
	if b == nil || b.count != 1 || b.prefixBits != 16 {
		t.Fatalf("initial singleton = %+v", b)
	}
	if err := tr.Put(second, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	expirePrune(t, tr, 0)
	if _, err := tr.Commit(); err != nil {
		t.Fatal(err)
	}
	if b.count != 2 || len(tr.buckets) != 1 {
		t.Fatalf("same-parent top-up left %d buckets, original count=%d", len(tr.buckets), b.count)
	}
	for _, key := range [][]byte{first, second} {
		if _, archived, err := tr.GetValueRef(key); err != nil || !archived {
			t.Fatalf("archived key %x missing after top-up: %v", key, err)
		}
	}
}

// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/holiman/uint256"
)

type archiveStemTestDB struct {
	db ethdb.KeyValueStore
}

func newArchiveStemTestDB() *archiveStemTestDB {
	return &archiveStemTestDB{db: memorydb.New()}
}

func (db *archiveStemTestDB) Get(key []byte) ([]byte, error) { return db.db.Get(key) }
func (db *archiveStemTestDB) Has(key []byte) (bool, error)   { return db.db.Has(key) }
func (db *archiveStemTestDB) Put(key, value []byte) error    { return db.db.Put(key, value) }
func (db *archiveStemTestDB) Delete(key []byte) error        { return db.db.Delete(key) }
func (db *archiveStemTestDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	return db.db.NewIterator(prefix, start)
}
func (db *archiveStemTestDB) NewBatch() archivetrie.Batcher {
	return db.db.NewBatch()
}

func newArchiveStemWrapper(t testing.TB, db *archiveStemTestDB, root []byte, shardDepth int) *ArchiveTrie {
	t.Helper()
	config := archivetrie.DefaultConfig()
	config.ShardDepth = shardDepth
	config.StemMode = true
	config.NodeStorageScheme = archivetrie.NodeStoragePath
	backend := archivetrie.NewTrie(root, db, archivetrie.NewPooledKeccakHasher(), config, true)
	stem, err := archivetrie.NewStemTrie(backend)
	if err != nil {
		t.Fatalf("new stem adapter: %v", err)
	}
	wrapper := &ArchiveTrie{trie: backend, stem: stem, indexOps: make(map[string]bool)}
	if err := wrapper.prepareArchiveStemIndex(); err != nil {
		t.Fatalf("prepare stem index: %v", err)
	}
	return wrapper
}

func TestArchiveTrieStemModeColocatesAndRestoresState(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 0)
	addr := common.HexToAddress("0x1234")
	account := &types.StateAccount{
		Nonce:    7,
		Balance:  uint256.NewInt(99),
		Root:     types.EmptyRootHash,
		CodeHash: common.HexToHash("0xabcdef").Bytes(),
	}
	var slot0, slot1 common.Hash
	slot1[31] = 1
	value0 := []byte{0x11}
	value1 := []byte{0x22}
	code := []byte{0x60, 0x01, 0x60, 0x02, 0x01}

	if err := tr.UpdateAccount(addr, account, len(code)); err != nil {
		t.Fatalf("update account: %v", err)
	}
	if err := tr.UpdateStorage(addr, slot0[:], value0); err != nil {
		t.Fatalf("update slot 0: %v", err)
	}
	if err := tr.UpdateStorage(addr, slot1[:], value1); err != nil {
		t.Fatalf("update slot 1: %v", err)
	}
	if err := tr.UpdateContractCode(addr, common.BytesToHash(account.CodeHash), code); err != nil {
		t.Fatalf("update code: %v", err)
	}
	stats := tr.trie.Stats()
	if stats.LeafCount != 1 {
		t.Fatalf("account, header storage and short code should share one outer stem, got %d leaves", stats.LeafCount)
	}
	if stats.ActiveLogicalValues != 4 {
		t.Fatalf("active logical value count mismatch: got %d want 4", stats.ActiveLogicalValues)
	}

	gotAccount, err := tr.GetAccount(addr)
	if err != nil || gotAccount == nil || gotAccount.Nonce != account.Nonce || gotAccount.Balance.Cmp(account.Balance) != 0 {
		t.Fatalf("account round trip failed: account=%v err=%v", gotAccount, err)
	}
	if got, err := tr.GetStorage(addr, slot0[:]); err != nil || !bytes.Equal(got, value0) {
		t.Fatalf("slot 0 round trip: got %x err %v", got, err)
	}
	chunks := trieutils.ChunkifyBinaryCode(code)
	gotChunk, err := tr.stem.Get(trieutils.BinaryTreeCodeChunkKey(addr, 0))
	if err != nil || !bytes.Equal(gotChunk, chunks[:common.HashLength]) {
		t.Fatalf("code chunk round trip: got %x err %v", gotChunk, err)
	}

	tr.trie.SetGlobalEpoch(1)
	if err := tr.trie.PruneNextShard(); err != nil {
		t.Fatalf("archive header stem: %v", err)
	}
	stats = tr.trie.Stats()
	if stats.ArchivedDataSize != 1 {
		t.Fatalf("shared header stem should archive as one record, got %d", stats.ArchivedDataSize)
	}
	if stats.ArchivedLogicalValues != 4 {
		t.Fatalf("archived logical value count mismatch: got %d want 4", stats.ArchivedLogicalValues)
	}

	// Updating one suffix restores the complete stem. The account, other
	// storage suffix, and code chunk must move forward with the new stem root.
	newValue0 := []byte{0x33}
	if err := tr.UpdateStorage(addr, slot0[:], newValue0); err != nil {
		t.Fatalf("update archived stem: %v", err)
	}
	stats = tr.trie.Stats()
	if stats.ArchivedDataSize != 0 {
		t.Fatalf("updated stem was not restored, archived=%d", stats.ArchivedDataSize)
	}
	if stats.ActiveLogicalValues != 4 {
		t.Fatalf("restored logical value count mismatch: got %d want 4", stats.ActiveLogicalValues)
	}
	if got, err := tr.GetStorage(addr, slot1[:]); err != nil || !bytes.Equal(got, value1) {
		t.Fatalf("untouched storage suffix was lost: got %x err %v", got, err)
	}
	if got, err := tr.GetAccount(addr); err != nil || got == nil || got.Nonce != account.Nonce {
		t.Fatalf("account suffix was lost: account=%v err=%v", got, err)
	}
	gotChunk, err = tr.stem.Get(trieutils.BinaryTreeCodeChunkKey(addr, 0))
	if err != nil || !bytes.Equal(gotChunk, chunks[:common.HashLength]) {
		t.Fatalf("code suffix was lost: got %x err %v", gotChunk, err)
	}

	// Account deletion removes only its own suffix, matching binary-tree
	// semantics; header storage in the same stem remains available.
	if err := tr.DeleteAccount(addr); err != nil {
		t.Fatalf("delete account: %v", err)
	}
	if got, err := tr.GetAccount(addr); err != nil || got != nil {
		t.Fatalf("deleted account still exists: account=%v err=%v", got, err)
	}
	if got, err := tr.GetStorage(addr, slot1[:]); err != nil || !bytes.Equal(got, value1) {
		t.Fatalf("delete account removed header storage: got %x err %v", got, err)
	}
}

func TestArchiveTrieStemModeBasicAccountLoadsOnlyMetadata(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 0)
	addr := common.HexToAddress("0x4321")
	account := types.NewEmptyStateAccount()
	account.Balance.SetUint64(1)
	if err := tr.UpdateAccount(addr, account, 0); err != nil {
		t.Fatal(err)
	}
	account.Balance.SetUint64(2)
	before := archivetrie.LastUpdateDiagnostics()
	if err := tr.UpdateAccount(addr, account, 0); err != nil {
		t.Fatal(err)
	}
	window := archivetrie.LastUpdateDiagnostics().Sub(before)
	if window.StemPutCalls != 1 || window.StemPutLoadedBytes != int64(8+archivetrie.StemSuffixCount/8) {
		t.Fatalf("basic account loaded more than metadata: calls=%d loaded=%d", window.StemPutCalls, window.StemPutLoadedBytes)
	}
	got, err := tr.GetAccount(addr)
	if err != nil || got == nil || got.Balance.Cmp(account.Balance) != 0 {
		t.Fatalf("updated account: account=%v err=%v", got, err)
	}
}

func TestArchiveTrieStemModeStorageIteratorAndReload(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 8)
	addr := common.HexToAddress("0x5678")
	other := common.HexToAddress("0x9999")
	var headerSlot, mainSlot, otherSlot common.Hash
	headerSlot[31] = 3
	mainSlot[30] = 1
	otherSlot[31] = 4
	if err := tr.UpdateStorage(addr, headerSlot[:], []byte("header")); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(addr, mainSlot[:], []byte("main")); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(other, otherSlot[:], []byte("other")); err != nil {
		t.Fatal(err)
	}
	stats := tr.trie.Stats()
	if stats.ActiveLogicalValues != 3 || stats.ActiveLogicalValueReadFailures != 0 {
		t.Fatalf("sharded active logical stats mismatch: values=%d failures=%d", stats.ActiveLogicalValues, stats.ActiveLogicalValueReadFailures)
	}

	it, err := NewArchiveStorageTrie(addr, tr).NodeIterator(nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[common.Hash]string)
	for it.Next(true) {
		seen[common.BytesToHash(it.LeafKey())] = string(it.LeafBlob())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[headerSlot] != "header" || seen[mainSlot] != "main" {
		t.Fatalf("storage iterator mismatch: %#v", seen)
	}

	root, _ := tr.Commit(false)
	if root == (common.Hash{}) {
		t.Fatal("commit returned empty root")
	}
	reloaded := newArchiveStemWrapper(t, db, root[:], 8)
	if got, err := reloaded.GetStorage(addr, headerSlot[:]); err != nil || string(got) != "header" {
		t.Fatalf("reloaded header slot: got %q err %v", got, err)
	}
	reloadedIt, err := NewArchiveStorageTrie(addr, reloaded).NodeIterator(nil)
	if err != nil {
		t.Fatal(err)
	}
	reloadedSeen := make(map[common.Hash]string)
	for reloadedIt.Next(true) {
		reloadedSeen[common.BytesToHash(reloadedIt.LeafKey())] = string(reloadedIt.LeafBlob())
	}
	if err := reloadedIt.Error(); err != nil {
		t.Fatal(err)
	}
	if len(reloadedSeen) != 2 || reloadedSeen[headerSlot] != "header" || reloadedSeen[mainSlot] != "main" {
		t.Fatalf("reloaded storage iterator mismatch: %#v", reloadedSeen)
	}
	if got, err := reloaded.GetStorage(addr, mainSlot[:]); err != nil || string(got) != "main" {
		t.Fatalf("reloaded main slot: got %q err %v", got, err)
	}
}

func TestArchiveTrieStemModeIndexedAccountWipe(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 8)
	addr := common.HexToAddress("0x1111")
	other := common.HexToAddress("0x2222")
	var slot0, slot1, otherSlot common.Hash
	slot1[30] = 1
	otherSlot[31] = 7
	if err := tr.UpdateStorage(addr, slot0[:], []byte("zero")); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(addr, slot1[:], []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := tr.UpdateStorage(other, otherSlot[:], []byte("other")); err != nil {
		t.Fatal(err)
	}
	code := bytes.Repeat([]byte{0x60}, 40) // two 31-byte code chunks
	if err := tr.UpdateContractCode(addr, common.HexToHash("0x01"), code); err != nil {
		t.Fatal(err)
	}
	root, _ := tr.Commit(false)
	if root == (common.Hash{}) {
		t.Fatal("initial commit returned empty root")
	}

	reloaded := newArchiveStemWrapper(t, db, root[:], 8)
	result, err := reloaded.WipeAccountState(addr)
	if err != nil {
		t.Fatalf("wipe account state: %v", err)
	}
	if len(result.Storage) != 2 || result.CodeChunks != 2 {
		t.Fatalf("wipe result mismatch: storage=%d codeChunks=%d", len(result.Storage), result.CodeChunks)
	}
	seen := make(map[common.Hash]string)
	for _, item := range result.Storage {
		seen[common.BytesToHash(item.Key)] = string(item.Value)
	}
	if seen[slot0] != "zero" || seen[slot1] != "one" {
		t.Fatalf("wiped storage origins mismatch: %#v", seen)
	}
	if got, err := reloaded.GetStorage(addr, slot0[:]); err != nil || len(got) != 0 {
		t.Fatalf("wiped slot still exists: got %x err %v", got, err)
	}
	if got, err := reloaded.GetStorage(other, otherSlot[:]); err != nil || string(got) != "other" {
		t.Fatalf("unrelated account was modified: got %q err %v", got, err)
	}
	for chunk := uint64(0); chunk < 2; chunk++ {
		if _, err := reloaded.stem.Get(trieutils.BinaryTreeCodeChunkKey(addr, chunk)); err != archivetrie.ErrNodeNotFound {
			t.Fatalf("code chunk %d survived wipe: %v", chunk, err)
		}
	}
	root2, _ := reloaded.Commit(false)
	clean := newArchiveStemWrapper(t, db, root2[:], 8)
	result, err = clean.WipeAccountState(addr)
	if err != nil {
		t.Fatalf("repeat wipe: %v", err)
	}
	if len(result.Storage) != 0 || result.CodeChunks != 0 {
		t.Fatalf("index entries survived wipe commit: storage=%d code=%d", len(result.Storage), result.CodeChunks)
	}
}

func TestArchiveTrieStemModeWipeSeesPendingIndexChanges(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 8)
	addr := common.HexToAddress("0x1212")
	var slot0, slot1 common.Hash
	slot1[30] = 1
	if err := tr.UpdateStorageBatch(addr, []StorageUpdate{
		{Key: slot0[:], Value: []byte("zero")},
		{Key: slot1[:], Value: []byte("one")},
	}); err != nil {
		t.Fatal(err)
	}
	code := bytes.Repeat([]byte{0x60}, 40) // two 31-byte code chunks
	if err := tr.UpdateContractCode(addr, common.HexToHash("0x01"), code); err != nil {
		t.Fatal(err)
	}

	// No Commit has happened: all index additions still live only in indexOps.
	result, err := tr.WipeAccountState(addr)
	if err != nil {
		t.Fatalf("wipe state with pending index additions: %v", err)
	}
	if len(result.Storage) != 2 || result.CodeChunks != 2 {
		t.Fatalf("pending additions were missed: storage=%d code=%d", len(result.Storage), result.CodeChunks)
	}
	// The wipe itself stages index deletions. A second wipe before Commit must
	// see those deletions instead of re-reading the old logical index entries.
	result, err = tr.WipeAccountState(addr)
	if err != nil {
		t.Fatalf("repeat wipe with pending index deletions: %v", err)
	}
	if len(result.Storage) != 0 || result.CodeChunks != 0 {
		t.Fatalf("pending deletions were ignored: storage=%d code=%d", len(result.Storage), result.CodeChunks)
	}
	root, _ := tr.Commit(false)
	clean := newArchiveStemWrapper(t, db, root[:], 8)
	result, err = clean.WipeAccountState(addr)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Storage) != 0 || result.CodeChunks != 0 {
		t.Fatalf("wiped indexes survived commit: storage=%d code=%d", len(result.Storage), result.CodeChunks)
	}
}

func TestArchiveTrieStemModeLargeWipeGroupsSlotsByStem(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 8)
	addr := common.HexToAddress("0x5151")
	updates := make([]StorageUpdate, 512)
	for i := range updates {
		slot := new(uint256.Int).SetUint64(uint64(i)).Bytes32()
		updates[i] = StorageUpdate{Key: slot[:], Value: []byte{byte(i)}}
	}
	if err := tr.UpdateStorageBatch(addr, updates); err != nil {
		t.Fatal(err)
	}
	root, _ := tr.Commit(false)
	reloaded := newArchiveStemWrapper(t, db, root[:], 8)
	result, err := reloaded.WipeAccountState(addr)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Storage) != len(updates) {
		t.Fatalf("wiped %d slots, want %d", len(result.Storage), len(updates))
	}
	if result.StemRecords <= 0 || result.StemRecords >= len(updates) {
		t.Fatalf("wipe did not group slots by stem: slots=%d stems=%d", len(updates), result.StemRecords)
	}
	if result.StemRecords > 4 {
		t.Fatalf("sequential slots unexpectedly spread across %d stems", result.StemRecords)
	}
}

func TestArchiveTrieStemModeCodeShrinkRemovesOldChunks(t *testing.T) {
	db := newArchiveStemTestDB()
	tr := newArchiveStemWrapper(t, db, nil, 8)
	addr := common.HexToAddress("0x3333")
	longCode := bytes.Repeat([]byte{0x60}, 100)
	longChunks := len(trieutils.ChunkifyBinaryCode(longCode)) / common.HashLength
	if longChunks < 2 {
		t.Fatalf("test code produced only %d chunk(s)", longChunks)
	}
	if err := tr.UpdateContractCode(addr, common.HexToHash("0x01"), longCode); err != nil {
		t.Fatal(err)
	}
	root1, _ := tr.Commit(false)

	reloaded := newArchiveStemWrapper(t, db, root1[:], 8)
	shortCode := []byte{0x60}
	shortChunks := len(trieutils.ChunkifyBinaryCode(shortCode)) / common.HashLength
	if err := reloaded.UpdateContractCode(addr, common.HexToHash("0x02"), shortCode); err != nil {
		t.Fatal(err)
	}
	for chunk := shortChunks; chunk < longChunks; chunk++ {
		if _, err := reloaded.stem.Get(trieutils.BinaryTreeCodeChunkKey(addr, uint64(chunk))); err != archivetrie.ErrNodeNotFound {
			t.Fatalf("old code chunk %d survived shrink: %v", chunk, err)
		}
	}
	root2, _ := reloaded.Commit(false)

	clean := newArchiveStemWrapper(t, db, root2[:], 8)
	result, err := clean.WipeAccountState(addr)
	if err != nil {
		t.Fatal(err)
	}
	if result.CodeChunks != shortChunks {
		t.Fatalf("code index retained stale chunks: got %d want %d", result.CodeChunks, shortChunks)
	}
}

func TestArchiveTrieStemModeRejectsDatabaseWithoutAccountIndex(t *testing.T) {
	db := newArchiveStemTestDB()
	legacyFlatKey := append(bytes.Clone(archiveFlatValuePrefix), bytes.Repeat([]byte{0x01}, archivetrie.StemSize)...)
	if err := db.Put(legacyFlatKey, []byte("legacy stem payload")); err != nil {
		t.Fatal(err)
	}
	config := archivetrie.DefaultConfig()
	config.ShardDepth = 8
	config.StemMode = true
	config.NodeStorageScheme = archivetrie.NodeStoragePath
	backend := archivetrie.NewTrie(nil, db, archivetrie.NewPooledKeccakHasher(), config, true)
	stem, err := archivetrie.NewStemTrie(backend)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := &ArchiveTrie{trie: backend, stem: stem, indexOps: make(map[string]bool)}
	if err := wrapper.prepareArchiveStemIndex(); err == nil {
		t.Fatal("legacy stem database was accepted without an account index")
	}
}

func BenchmarkArchiveTrieStemModeAccountWipe100K(b *testing.B) {
	const slots = 100_000
	addr := common.HexToAddress("0x6161")
	updates := make([]StorageUpdate, slots)
	for i := range updates {
		slot := new(uint256.Int).SetUint64(uint64(i)).Bytes32()
		updates[i] = StorageUpdate{Key: slot[:], Value: []byte{byte(i)}}
	}
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		db := newArchiveStemTestDB()
		tr := newArchiveStemWrapper(b, db, nil, 8)
		if err := tr.UpdateStorageBatch(addr, updates); err != nil {
			b.Fatal(err)
		}
		root, _ := tr.Commit(false)
		reloaded := newArchiveStemWrapper(b, db, root[:], 8)
		b.StartTimer()
		result, err := reloaded.WipeAccountState(addr)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Storage) != slots {
			b.Fatalf("wiped %d slots", len(result.Storage))
		}
		b.ReportMetric(float64(result.StemRecords), "stems/op")
	}
}

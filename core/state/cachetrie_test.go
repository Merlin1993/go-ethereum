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

package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
)

func TestCacheTrieStateDBCommitRead(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{
		CacheTrie:         true,
		CacheTrieWindow:   16,
		CacheTrieMaxItems: 128,
	})
	db := NewDatabase(tdb, nil)

	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	key := common.HexToHash("0x01")
	value := common.HexToHash("0x02")

	state, err := New(types.EmptyRootHash, db)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}
	state.CreateAccount(addr)
	state.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	state.SetState(addr, key, value)

	state.SetBlockNum(1)
	precommitRoot := state.IntermediateRoot(true)
	oldReaderBeforeCommit, err := db.Reader(types.EmptyRootHash)
	if err != nil {
		t.Fatalf("failed to create pre-commit old-root reader: %v", err)
	}
	oldAccountBeforeCommit, err := oldReaderBeforeCommit.Account(addr)
	if err != nil {
		t.Fatalf("failed to read old-root account before commit: %v", err)
	}
	if oldAccountBeforeCommit != nil {
		t.Fatal("staged cachetrie write leaked into the parent root before commit")
	}

	root, err := state.Commit(1, true, true)
	if err != nil {
		t.Fatalf("failed to commit state: %v", err)
	}
	if root != precommitRoot {
		t.Fatalf("commit root mismatch: have %x, want precommit root %x", root, precommitRoot)
	}
	cache := tdb.CacheTrie()
	if cache == nil {
		t.Fatal("cachetrie is not enabled")
	}
	if cachedRoot, ok := cache.Root(); !ok || cachedRoot != root {
		t.Fatalf("cache root mismatch: have %x ok %v, want %x", cachedRoot, ok, root)
	}

	next, err := New(root, db)
	if err != nil {
		t.Fatalf("failed to create next state: %v", err)
	}
	if got := next.GetState(addr, key); got != value {
		t.Fatalf("storage mismatch: have %x, want %x", got, value)
	}
	stats := cache.Stats()
	if stats.AccountHits == 0 {
		t.Fatal("expected account read to hit cachetrie")
	}
	if stats.StorageHits == 0 {
		t.Fatal("expected storage read to hit cachetrie")
	}

	oldReader, err := db.Reader(types.EmptyRootHash)
	if err != nil {
		t.Fatalf("failed to create old-root reader: %v", err)
	}
	oldAccount, err := oldReader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read old-root account: %v", err)
	}
	if oldAccount != nil {
		t.Fatal("cachetrie served data for a mismatched historical root")
	}
	if after := cache.Stats(); after.AccountHits != stats.AccountHits || after.StorageHits != stats.StorageHits {
		t.Fatalf("historical read unexpectedly touched cachetrie: before %+v after %+v", stats, after)
	}
}

func TestCacheTrieAsyncCommitReadWithoutForegroundMPT(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{
		CacheTrie:         true,
		CacheTrieWindow:   16,
		CacheTrieMaxItems: 128,
	})
	db := NewDatabase(tdb, nil)

	addr := common.HexToAddress("0x1212121212121212121212121212121212121212")
	key := common.HexToHash("0x01")
	value := common.HexToHash("0x02")

	state, err := New(types.EmptyRootHash, db)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}
	state.SetBlockNum(1)
	state.SetCacheTrieAsync(true)
	state.CreateAccount(addr)
	state.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	state.SetState(addr, key, value)

	root, err := state.Commit(1, true, true)
	if err != nil {
		t.Fatalf("failed to commit async state: %v", err)
	}
	if root != types.EmptyRootHash {
		t.Fatalf("async commit should not advance backing MPT root before merge: have %x", root)
	}
	if state.AccountCommits != 0 || state.StorageCommits != 0 || state.AccountHashes != 0 {
		t.Fatalf("async commit unexpectedly used foreground MPT path: accountCommit=%s storageCommit=%s accountHash=%s", state.AccountCommits, state.StorageCommits, state.AccountHashes)
	}

	next, err := New(root, db)
	if err != nil {
		t.Fatalf("failed to create next state: %v", err)
	}
	if got := next.GetNonce(addr); got != 1 {
		t.Fatalf("nonce mismatch: have %d, want 1", got)
	}
	if got := next.GetState(addr, key); got != value {
		t.Fatalf("storage mismatch: have %x, want %x", got, value)
	}

	noCache := readNoCacheAccount(t, db.(*MPTDatabase), root, addr)
	if noCache != nil {
		t.Fatal("foreground async commit unexpectedly wrote account into backing MPT")
	}
	stats := tdb.CacheTrie().Stats()
	if stats.Accounts != 1 || stats.Storages != 1 {
		t.Fatalf("cachetrie write set mismatch: accounts=%d storages=%d", stats.Accounts, stats.Storages)
	}
}

func TestCacheTrieIntermediateRootDoesNotStageWithoutAsync(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{
		CacheTrie:         true,
		CacheTrieWindow:   16,
		CacheTrieMaxItems: 128,
	})
	db := NewDatabase(tdb, nil)
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")

	state, err := New(types.EmptyRootHash, db)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}
	state.SetBlockNum(1)
	state.CreateAccount(addr)
	state.SetNonce(addr, 1, tracing.NonceChangeUnspecified)
	state.IntermediateRoot(true)

	if stats := tdb.CacheTrie().Stats(); stats.PendingInputs != 0 || stats.Accounts != 0 {
		t.Fatalf("non-async intermediate root staged cachetrie writes: %+v", stats)
	}
}

func TestCacheTrieAsyncCommitMergesBackingMPTOnWatermark(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{
		CacheTrie:         true,
		CacheTrieWindow:   16,
		CacheTrieMaxItems: 2,
	})
	db := NewDatabase(tdb, nil)
	mptdb := db.(*MPTDatabase)

	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addr3 := common.HexToAddress("0x3333333333333333333333333333333333333333")

	root, mptRoot1, swmtRoot1 := commitAsyncAccount(t, db, types.EmptyRootHash, 1, addr1, 1)
	if root != types.EmptyRootHash {
		t.Fatalf("first async block disclosed unexpected global root: have %x", root)
	}
	if root == mptRoot1 {
		t.Fatalf("async commit returned current MPT root instead of disclosed global root: %x", root)
	}
	if swmtRoot1 == (common.Hash{}) {
		t.Fatal("empty swmt root after async write")
	}
	if account := readNoCacheAccount(t, mptdb, types.EmptyRootHash, addr1); account != nil {
		t.Fatalf("async write reached backing MPT before merge: %#v", account)
	}
	if nonce := readNonce(t, db, root, addr1); nonce != 1 {
		t.Fatalf("SWMT read did not shadow backing MPT: have nonce %d", nonce)
	}

	root, _, _ = commitAsyncAccount(t, db, root, 2, addr2, 2)
	if root != types.EmptyRootHash {
		t.Fatalf("second async block disclosed root before prune: have %x", root)
	}
	root, _, _ = commitAsyncAccount(t, db, root, 3, addr3, 3)
	if root == types.EmptyRootHash {
		t.Fatal("high watermark did not disclose completed backing merge root")
	}
	if account := readNoCacheAccount(t, mptdb, root, addr1); account == nil || account.Nonce != 1 {
		t.Fatalf("merged account not visible in backing MPT: %#v", account)
	}
	if nonce := readNonce(t, db, root, addr2); nonce != 2 {
		t.Fatalf("live SWMT account 2 not readable after prune: have nonce %d", nonce)
	}
	if nonce := readNonce(t, db, root, addr3); nonce != 3 {
		t.Fatalf("live SWMT account 3 not readable after prune: have nonce %d", nonce)
	}
}

func TestCacheTrieMergePreservesBackingStorageRoot(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{CacheTrie: true})
	db := NewMPTDatabase(tdb, nil)

	addr := common.HexToAddress("0x4444444444444444444444444444444444444444")
	slot := common.HexToHash("0x01")
	value := common.HexToHash("0x02")
	unmaterializedRoot := common.HexToHash("0x1234")
	account := types.NewEmptyStateAccount()
	account.Nonce = 7
	account.Root = unmaterializedRoot

	root, err := db.mergeCacheTrieInputs(types.EmptyRootHash, 1, []cachetrie.MergeInput{
		{
			Type:       cachetrie.StorageState,
			Address:    addr,
			StorageKey: &slot,
			Storage:    value,
		},
		{
			Type:    cachetrie.AccountState,
			Address: addr,
			Account: account,
		},
	})
	if err != nil {
		t.Fatalf("failed to merge cachetrie inputs: %v", err)
	}
	reader, err := (&mptNoCacheDatabase{db: db}).Reader(root)
	if err != nil {
		t.Fatalf("failed to create no-cache reader: %v", err)
	}
	mergedAccount, err := reader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read merged account: %v", err)
	}
	if mergedAccount == nil {
		t.Fatal("merged account missing")
	}
	if mergedAccount.Nonce != account.Nonce {
		t.Fatalf("merged nonce mismatch: have %d, want %d", mergedAccount.Nonce, account.Nonce)
	}
	if mergedAccount.Root == unmaterializedRoot {
		t.Fatalf("merge wrote unmaterialized SWMT storage root %x into backing MPT", unmaterializedRoot)
	}
	mergedStorage, err := reader.Storage(addr, slot)
	if err != nil {
		t.Fatalf("failed to read merged storage: %v", err)
	}
	if mergedStorage != value {
		t.Fatalf("merged storage mismatch: have %x, want %x", mergedStorage, value)
	}
}

func TestCacheTrieStorageDependencyDoesNotOverwriteBackingAccount(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{CacheTrie: true})
	db := NewMPTDatabase(tdb, nil)

	addr := common.HexToAddress("0x5555555555555555555555555555555555555555")
	slot := common.HexToHash("0x01")
	value := common.HexToHash("0x02")

	state, err := New(types.EmptyRootHash, db)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}
	state.CreateAccount(addr)
	state.SetNonce(addr, 2, tracing.NonceChangeUnspecified)
	state.SetBalance(addr, uint256.NewInt(10), tracing.BalanceChangeUnspecified)
	root, err := state.Commit(1, true, false)
	if err != nil {
		t.Fatalf("failed to commit seed account: %v", err)
	}

	staleAccount := types.NewEmptyStateAccount()
	staleAccount.Nonce = 1
	staleAccount.Balance = uint256.NewInt(1)
	root, err = db.mergeCacheTrieInputs(root, 2, []cachetrie.MergeInput{
		{
			Type:       cachetrie.AccountState,
			Address:    addr,
			Account:    staleAccount,
			Dependency: true,
		},
		{
			Type:       cachetrie.StorageState,
			Address:    addr,
			StorageKey: &slot,
			Storage:    value,
		},
	})
	if err != nil {
		t.Fatalf("failed to merge dependency input: %v", err)
	}
	reader, err := (&mptNoCacheDatabase{db: db}).Reader(root)
	if err != nil {
		t.Fatalf("failed to create no-cache reader: %v", err)
	}
	account, err := reader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read merged account: %v", err)
	}
	if account == nil {
		t.Fatal("merged account missing")
	}
	if account.Nonce != 2 {
		t.Fatalf("dependency overwrote nonce: have %d want 2", account.Nonce)
	}
	if account.Balance.Cmp(uint256.NewInt(10)) != 0 {
		t.Fatalf("dependency overwrote balance: have %s want 10", account.Balance)
	}
	mergedStorage, err := reader.Storage(addr, slot)
	if err != nil {
		t.Fatalf("failed to read merged storage: %v", err)
	}
	if mergedStorage != value {
		t.Fatalf("merged storage mismatch: have %x, want %x", mergedStorage, value)
	}
}

func commitAsyncAccount(t *testing.T, db Database, root common.Hash, block uint64, addr common.Address, nonce uint64) (common.Hash, common.Hash, common.Hash) {
	t.Helper()

	state, err := New(root, db)
	if err != nil {
		t.Fatalf("failed to create state: %v", err)
	}
	state.SetBlockNum(block)
	state.SetCacheTrieAsync(true)
	state.CreateAccount(addr)
	state.SetNonce(addr, nonce, tracing.NonceChangeUnspecified)

	mptRoot := state.IntermediateRoot(true)
	globalRoot, swmtRoot, ok := state.CacheTrieRoots()
	if !ok {
		t.Fatal("cachetrie roots unavailable")
	}
	committedRoot, err := state.Commit(block, true, true)
	if err != nil {
		t.Fatalf("failed to commit async state: %v", err)
	}
	if globalRoot != committedRoot {
		t.Fatalf("preview/commit global root mismatch: preview %x commit %x", globalRoot, committedRoot)
	}
	return committedRoot, mptRoot, swmtRoot
}

func readNonce(t *testing.T, db Database, root common.Hash, addr common.Address) uint64 {
	t.Helper()

	state, err := New(root, db)
	if err != nil {
		t.Fatalf("failed to create reader state: %v", err)
	}
	return state.GetNonce(addr)
}

func readNoCacheAccount(t *testing.T, db *MPTDatabase, root common.Hash, addr common.Address) *types.StateAccount {
	t.Helper()

	reader, err := (&mptNoCacheDatabase{db: db}).Reader(root)
	if err != nil {
		t.Fatalf("failed to create no-cache reader: %v", err)
	}
	account, err := reader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read no-cache account: %v", err)
	}
	return account
}

func TestCacheTrieSkipsHashedStorageKeys(t *testing.T) {
	disk := rawdb.NewMemoryDatabase()
	tdb := triedb.NewDatabase(disk, &triedb.Config{CacheTrie: true})
	db := NewMPTDatabase(tdb, nil)

	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addrHash := crypto.Keccak256Hash(addr.Bytes())
	slotHash := crypto.Keccak256Hash(common.HexToHash("0x01").Bytes())

	db.updateCacheTrie(&StateUpdate{
		Root:           common.HexToHash("0x01"),
		BlockNumber:    1,
		StorageKeyType: StorageKeyHashed,
		Storages: map[common.Hash]map[common.Hash]common.Hash{
			addrHash: {slotHash: common.HexToHash("0x02")},
		},
		StoragesOrigin: map[common.Address]map[common.Hash]common.Hash{
			addr: {slotHash: common.Hash{}},
		},
	})
	if stats := tdb.CacheTrie().Stats(); stats.Storages != 0 {
		t.Fatalf("hashed storage keys should not be cached, have %d entries", stats.Storages)
	}
}

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

package cachetrie

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

func TestLowWatermarkDefaultAndOverride(t *testing.T) {
	if got := NewCacheTrie(0, 16, 10).Stats().LowWatermark; got != 8 {
		t.Fatalf("default low watermark mismatch: have %d want 8", got)
	}
	if got := NewCacheTrie(0, 16, 10, 3).Stats().LowWatermark; got != 3 {
		t.Fatalf("custom low watermark mismatch: have %d want 3", got)
	}
	if got := NewCacheTrie(0, 16, 10, 20).Stats().LowWatermark; got != 10 {
		t.Fatalf("capped low watermark mismatch: have %d want 10", got)
	}
}

func TestApplyClearsOnOriginMismatch(t *testing.T) {
	cache := NewCacheTrie(0, 16, 128)
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	rootA := common.HexToHash("0x01")
	rootB := common.HexToHash("0x02")
	rootC := common.HexToHash("0x03")

	cache.Apply(1, common.Hash{}, rootA, map[common.Address]*types.StateAccount{
		addr: {Nonce: 1, Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()},
	}, nil)
	if _, ok := cache.Account(addr); !ok {
		t.Fatal("expected account to be cached")
	}
	cache.Apply(2, rootB, rootC, nil, nil)
	if _, ok := cache.Account(addr); ok {
		t.Fatal("stale account survived origin mismatch")
	}
	if root, ok := cache.Root(); !ok || root != rootC {
		t.Fatalf("unexpected cache root: have %x ok %v, want %x", root, ok, rootC)
	}
}

func TestStagedWritesPublishOnlyAtNewRoot(t *testing.T) {
	cache := NewCacheTrie(0, 16, 128)
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	origin := common.HexToHash("0x01")
	root := common.HexToHash("0x02")
	acct := &types.StateAccount{Nonce: 1, Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()}

	cache.Apply(1, common.Hash{}, origin, nil, nil)
	cache.Begin(2, origin)
	cache.StageAccount(addr, acct)

	if got, ok := cache.Account(addr); ok || got != nil {
		t.Fatalf("staged account leaked into committed root: have %#v ok %v", got, ok)
	}
	cache.Begin(2, origin)
	cache.Publish(2, origin, root, nil, nil)

	got, ok := cache.Account(addr)
	if !ok || got == nil || got.Nonce != acct.Nonce {
		t.Fatalf("published staged account not readable: have %#v ok %v", got, ok)
	}
	if cachedRoot, ok := cache.Root(); !ok || cachedRoot != root {
		t.Fatalf("cache root mismatch: have %x ok %v, want %x", cachedRoot, ok, root)
	}
}

func TestIncrementalSWMTRootMatchesFullAggregation(t *testing.T) {
	cache := NewCacheTrie(0, 16, 128)
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	slotA := common.HexToHash("0x01")
	slotB := common.HexToHash("0x02")
	rootA := common.HexToHash("0x01")
	rootB := common.HexToHash("0x02")
	rootC := common.HexToHash("0x03")
	acct := &types.StateAccount{Nonce: 1, Balance: uint256.NewInt(1), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()}

	cache.PublishRoots(1, common.Hash{}, rootA,
		map[common.Address]*types.StateAccount{addr: acct},
		map[common.Address]map[common.Hash]common.Hash{addr: {
			slotA: common.HexToHash("0x0a"),
			slotB: common.HexToHash("0x0b"),
		}},
	)
	assertIncrementalRootConsistent(t, cache)
	if stats := cache.Stats(); stats.Storages != 2 {
		t.Fatalf("storage count mismatch after publish: have %d want 2", stats.Storages)
	}

	cache.PublishRoots(2, common.Hash{}, rootB,
		nil,
		map[common.Address]map[common.Hash]common.Hash{addr: {
			slotA: common.HexToHash("0x0c"),
		}},
	)
	assertIncrementalRootConsistent(t, cache)
	if got, ok := cache.Storage(addr, slotA); !ok || got != common.HexToHash("0x0c") {
		t.Fatalf("updated storage mismatch: have %x ok %v", got, ok)
	}

	cache.PublishRoots(3, common.Hash{}, rootC, map[common.Address]*types.StateAccount{addr: nil}, nil)
	assertIncrementalRootConsistent(t, cache)
	if stats := cache.Stats(); stats.Storages != 0 {
		t.Fatalf("storage count mismatch after account tombstone: have %d want 0", stats.Storages)
	}
}

func TestWatermarkPipelinePrunesOnlyPreviousCompleteBits(t *testing.T) {
	cache := NewCacheTrie(0, 16, 4)
	root := common.HexToHash("0x01")
	addrA := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	addrX := common.HexToAddress("0xabaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	addrB := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	addrC := common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	acct := func(nonce uint64) *types.StateAccount {
		return &types.StateAccount{Nonce: nonce, Balance: uint256.NewInt(nonce), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()}
	}

	cache.Apply(1, common.Hash{}, root, map[common.Address]*types.StateAccount{addrA: acct(1), addrX: acct(10)}, nil)
	cache.Apply(2, root, root, map[common.Address]*types.StateAccount{addrB: acct(2)}, nil)

	stats := cache.Stats()
	if !stats.Pipeline || stats.PendingBits == 0 || stats.PendingInputs == 0 {
		t.Fatalf("low watermark did not start pending merge: %+v", stats)
	}
	firstPending := cache.pendingMerge
	if firstPending == nil {
		t.Fatal("missing pending merge")
	}
	<-firstPending.done
	if got, ok := cache.Account(addrA); !ok || got == nil || got.Nonce != 1 {
		t.Fatalf("pending merge input was pruned before high watermark: have %#v ok %v", got, ok)
	}

	cache.Apply(3, root, root, map[common.Address]*types.StateAccount{addrA: acct(4)}, nil)
	cache.Apply(4, root, root, map[common.Address]*types.StateAccount{addrC: acct(3)}, nil)

	got, ok := cache.Account(addrA)
	if !ok || got == nil || got.Nonce != 4 {
		t.Fatalf("rewritten key was pruned by old pending bit: have %#v ok %v", got, ok)
	}
	if got, ok := cache.Account(addrX); ok || got != nil {
		t.Fatalf("old complete-bit account survived pruning: have %#v ok %v", got, ok)
	}
	if got, ok := cache.Account(addrB); !ok || got == nil || got.Nonce != 2 {
		t.Fatalf("non-pending complete-bit account was pruned: have %#v ok %v", got, ok)
	}
	if stats := cache.Stats(); stats.PendingBits == 0 || stats.PendingInputs == 0 {
		t.Fatalf("high watermark did not start next pending merge: %+v", stats)
	}
}

func TestStoragePendingInputCarriesAccountDependency(t *testing.T) {
	cache := NewCacheTrie(0, 16, 128)
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	slot := common.HexToHash("0x01")

	accountAtStorageWrite := types.NewEmptyStateAccount()
	accountAtStorageWrite.Nonce = 1
	cache.PublishRoots(1, common.Hash{}, common.HexToHash("0x01"),
		map[common.Address]*types.StateAccount{addr: accountAtStorageWrite},
		map[common.Address]map[common.Hash]common.Hash{addr: {slot: common.HexToHash("0x02")}},
	)

	newerAccount := types.NewEmptyStateAccount()
	newerAccount.Nonce = 2
	cache.PublishRoots(2, common.Hash{}, common.HexToHash("0x02"),
		map[common.Address]*types.StateAccount{addr: newerAccount},
		nil,
	)

	inputs := cache.mergeInputsForBitLocked(bitForBlock(1))
	var accountInput, storageInput bool
	for _, input := range inputs {
		switch input.Type {
		case AccountState:
			if input.Address == addr && input.Account != nil && input.Account.Nonce == accountAtStorageWrite.Nonce {
				accountInput = true
			}
		case StorageState:
			if input.Address == addr && input.StorageKey != nil && *input.StorageKey == slot {
				storageInput = true
			}
		}
	}
	if !storageInput {
		t.Fatal("missing storage input")
	}
	if !accountInput {
		t.Fatal("storage input did not carry its account dependency")
	}
}

func TestWriteWaitsWhenHighWatermarkPendingMergeIncomplete(t *testing.T) {
	cache := NewCacheTrie(0, 16, 2, 1)
	root := common.HexToHash("0x01")
	release := make(chan struct{})
	started := make(chan struct{})
	cache.SetMergeFunc(func(root common.Hash, block uint64, inputs []MergeInput) (common.Hash, error) {
		close(started)
		<-release
		return root, nil
	})
	acct := func(nonce uint64) *types.StateAccount {
		return &types.StateAccount{Nonce: nonce, Balance: uint256.NewInt(nonce), Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()}
	}
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addr3 := common.HexToAddress("0x3333333333333333333333333333333333333333")

	cache.Apply(1, common.Hash{}, root, map[common.Address]*types.StateAccount{addr1: acct(1)}, nil)
	cache.Apply(2, root, root, map[common.Address]*types.StateAccount{addr2: acct(2)}, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pending merge did not start")
	}

	done := make(chan struct{})
	go func() {
		cache.Begin(3, root)
		cache.StageAccount(addr3, acct(3))
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("write passed high watermark while previous merge was incomplete")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("write did not resume after pending merge completed")
	}
	if stats := cache.Stats(); stats.WriteWaitCount == 0 || stats.WriteWaitElapsed == 0 {
		t.Fatalf("write wait statistics were not recorded: %+v", stats)
	}
}

func assertIncrementalRootConsistent(t *testing.T, cache *CacheTrie) {
	t.Helper()

	cache.mu.RLock()
	accounts, storages := cache.copyLiveLocked()
	want := swmtRootFromCopies(cache.root, accounts, storages)
	have := cache.swmtRootLocked(cache.root)
	cache.mu.RUnlock()
	if have != want {
		t.Fatalf("incremental SWMT root mismatch: have %x want %x", have, want)
	}
}

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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

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

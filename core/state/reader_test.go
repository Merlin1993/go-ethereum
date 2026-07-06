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
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

type accountReaderStub struct {
	account *types.StateAccount
}

func (r *accountReaderStub) Account(common.Address) (*types.StateAccount, error) {
	return r.account, nil
}

func (r *accountReaderStub) Storage(common.Address, common.Hash) (common.Hash, error) {
	return common.Hash{}, nil
}

func TestStateReaderWithCacheReturnsAccountCopies(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	origin := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(2),
		Root:     types.EmptyRootHash,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	reader := newStateReaderWithCache(&accountReaderStub{account: origin})

	first, err := reader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read account: %v", err)
	}
	first.Nonce = 99
	first.Balance.SetUint64(99)
	first.CodeHash[0] ^= 0xff

	second, err := reader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read cached account: %v", err)
	}
	if second.Nonce != origin.Nonce {
		t.Fatalf("cached account nonce was mutated through caller-owned copy: have %d want %d", second.Nonce, origin.Nonce)
	}
	if second.Balance.Cmp(origin.Balance) != 0 {
		t.Fatalf("cached account balance was mutated through caller-owned copy: have %v want %v", second.Balance, origin.Balance)
	}
	if got, want := second.CodeHash, origin.CodeHash; !bytes.Equal(got, want) {
		t.Fatalf("cached account code hash was mutated through caller-owned copy: have %x want %x", got, want)
	}
}

func TestCacheTrieAccountReadKeepsBackingStorageRoot(t *testing.T) {
	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	stateRoot := common.HexToHash("0x1234")
	backingRoot := common.HexToHash("0x1111")
	swmtRoot := common.HexToHash("0x2222")
	backing := &types.StateAccount{
		Nonce:    1,
		Balance:  uint256.NewInt(2),
		Root:     backingRoot,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	cached := &types.StateAccount{
		Nonce:    2,
		Balance:  uint256.NewInt(3),
		Root:     swmtRoot,
		CodeHash: types.EmptyCodeHash.Bytes(),
	}
	cache := cachetrie.NewCacheTrie(0, 16, 128)
	cache.Publish(1, stateRoot, stateRoot, map[common.Address]*types.StateAccount{addr: cached}, nil)
	reader, err := newMultiStateReader(newCacheTrieReader(cache), &accountReaderStub{account: backing})
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	acct, err := reader.Account(addr)
	if err != nil {
		t.Fatalf("failed to read account: %v", err)
	}
	if acct.Nonce != cached.Nonce {
		t.Fatalf("account metadata did not come from cachetrie: have nonce %d want %d", acct.Nonce, cached.Nonce)
	}
	if acct.Root != backingRoot {
		t.Fatalf("storage root should come from backing MPT: have %s want %s", acct.Root, backingRoot)
	}
}

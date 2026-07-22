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

import "github.com/ethereum/go-ethereum/common"

// StorageUpdate is one original storage-slot mutation. Trie implementations
// may expose UpdateStorageBatch as an optional fast path; the standard Trie
// interface remains unchanged.
type StorageUpdate struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// StorageWipeItem is one original storage slot removed while wiping an
// account from a unified state trie. Key is the unhashed 32-byte slot and
// Value is the unencoded storage value returned by Trie.GetStorage.
type StorageWipeItem struct {
	Key   []byte
	Value []byte
}

// AccountStateWipeResult describes the address-owned data removed from a
// unified state trie. Traditional account/storage tries do not need this
// operation because deleting a storage-trie root makes the whole subtrie
// unreachable; unified binary tries must remove the address-owned leaves.
type AccountStateWipeResult struct {
	Address     common.Address
	Storage     []StorageWipeItem
	CodeChunks  int
	StemRecords int

	IndexScanNanos  int64
	StemDeleteNanos int64
	IndexStageNanos int64
}

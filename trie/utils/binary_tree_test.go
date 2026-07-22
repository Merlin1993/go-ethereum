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

package utils_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/bintrie"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
)

func TestArchiveBinaryKeyMappingMatchesBinaryTrie(t *testing.T) {
	addresses := []common.Address{
		common.HexToAddress("0x01"),
		common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
	}
	slots := []common.Hash{
		{},
		common.HexToHash("0x3f"),
		common.HexToHash("0x40"),
		common.HexToHash("0x123456"),
		common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
	}
	for _, addr := range addresses {
		if got, want := trieutils.BinaryTreeBasicDataKey(addr), bintrie.GetBinaryTreeKeyBasicData(addr); !bytes.Equal(got, want) {
			t.Fatalf("basic-data key mismatch for %x: got %x want %x", addr, got, want)
		}
		for _, slot := range slots {
			got := trieutils.BinaryTreeStorageSlotKey(addr, slot[:])
			want := bintrie.GetBinaryTreeKeyStorageSlot(addr, slot[:])
			if !bytes.Equal(got, want) {
				t.Fatalf("storage key mismatch for %x/%x: got %x want %x", addr, slot, got, want)
			}
		}
		for _, chunk := range []uint64{0, 1, 127, 128, 1024} {
			got := trieutils.BinaryTreeCodeChunkKey(addr, chunk)
			var offset [32]byte
			binary.BigEndian.PutUint64(offset[24:], 128+chunk)
			want := bintrie.GetBinaryTreeKey(addr, offset[:])
			if !bytes.Equal(got, want) {
				t.Fatalf("code key mismatch for %x/%d: got %x want %x", addr, chunk, got, want)
			}
		}
	}
}

func TestArchiveBinaryCodeChunkingMatchesBinaryTrie(t *testing.T) {
	code := append([]byte{0x60, 0x01, 0x7f}, bytes.Repeat([]byte{0xaa}, 90)...)
	got := trieutils.ChunkifyBinaryCode(code)
	want := bintrie.ChunkifyCode(code)
	if !bytes.Equal(got, want) {
		t.Fatalf("code chunking mismatch:\ngot  %x\nwant %x", got, want)
	}
}

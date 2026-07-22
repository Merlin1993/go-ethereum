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

package utils

import (
	"bytes"
	"crypto/sha256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

const (
	BinaryBasicDataLeafKey = byte(0)
	binaryCodeOffset       = uint64(128)
)

// BinaryTreeKey derives the 32-byte key used by the repository's unified
// binary trie. The first 31 bytes form the stem and the final byte is the
// suffix. offset must be a 32-byte big-endian value.
func BinaryTreeKey(addr common.Address, offset []byte) []byte {
	return binaryTreeKey(addr, offset, false)
}

func binaryTreeKey(addr common.Address, offset []byte, overflow bool) []byte {
	var input [64]byte
	copy(input[12:32], addr[:])
	copy(input[33:64], offset[:31])
	if overflow {
		input[32] = 1
	}
	hash := sha256.Sum256(input[:])
	hash[31] = offset[31]
	return hash[:]
}

// BinaryTreeBasicDataKey returns the account-header key. ASCT's stem adapter
// stores the account RLP at suffix 0 of this stem.
func BinaryTreeBasicDataKey(addr common.Address) []byte {
	var offset [32]byte
	offset[31] = BinaryBasicDataLeafKey
	return BinaryTreeKey(addr, offset[:])
}

// BinaryTreeStorageSlotKey maps an Ethereum storage slot to the same 31+1 key
// layout used by trie/bintrie. Slots 0..63 share the account header stem.
func BinaryTreeStorageSlotKey(addr common.Address, slot []byte) []byte {
	var (
		offset [32]byte
		normal = common.BytesToHash(slot)
		zero   common.Hash
	)
	if bytes.Equal(normal[:31], zero[:31]) && normal[31] < 64 {
		offset[31] = 64 + normal[31]
		return BinaryTreeKey(addr, offset[:])
	}
	// MAIN_STORAGE_OFFSET is 1<<248. Carry out of the 32-byte value is
	// represented separately because the tree-key shifter has one spare byte.
	overflow := normal[0] == 0xff
	copy(offset[:], normal[:])
	offset[0]++
	return binaryTreeKey(addr, offset[:], overflow)
}

// BinaryTreeCodeChunkKey maps one 31-byte EVM code chunk into the binary key
// space. Chunks 0..127 occupy suffixes 128..255 of the account header stem.
func BinaryTreeCodeChunkKey(addr common.Address, chunk uint64) []byte {
	offset := new(uint256.Int).SetUint64(binaryCodeOffset + chunk).Bytes32()
	return BinaryTreeKey(addr, offset[:])
}

// ChunkifyBinaryCode splits EVM code into 32-byte tree values: one metadata
// byte followed by 31 code bytes.
func ChunkifyBinaryCode(code []byte) []byte {
	const (
		chunkDataSize = 31
		chunkSize     = 32
		push1         = byte(0x60)
		push32        = byte(0x7f)
	)
	var (
		chunkOffset = 0
		chunkCount  = len(code) / chunkDataSize
		codeOffset  = 0
	)
	if len(code)%chunkDataSize != 0 {
		chunkCount++
	}
	chunks := make([]byte, chunkCount*chunkSize)
	for i := 0; i < chunkCount; i++ {
		end := min(len(code), chunkDataSize*(i+1))
		copy(chunks[i*chunkSize+1:], code[chunkDataSize*i:end])
		if chunkOffset > chunkDataSize {
			chunks[i*chunkSize] = chunkDataSize
			chunkOffset = 1
			continue
		}
		chunks[chunkSize*i] = byte(chunkOffset)
		chunkOffset = 0
		for ; codeOffset < end; codeOffset++ {
			if code[codeOffset] >= push1 && code[codeOffset] <= push32 {
				codeOffset += int(code[codeOffset]-push1) + 1
				if codeOffset+1 >= chunkDataSize*(i+1) {
					codeOffset++
					chunkOffset = codeOffset - chunkDataSize*(i+1)
					break
				}
			}
		}
	}
	return chunks
}

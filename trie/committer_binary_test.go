// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package trie

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestGatherBinaryRootBranchChildren(t *testing.T) {
	left := common.HexToHash("0x11")
	right := common.HexToHash("0x22")
	node := make([]byte, 2+2*common.HashLength)
	node[0] = 0xd4
	node[1] = 7
	copy(node[2:2+common.HashLength], left[:])
	copy(node[2+common.HashLength:], right[:])

	var children []common.Hash
	ForGatherBinaryChildren(node, func(hash common.Hash) {
		children = append(children, hash)
	})
	if len(children) != 2 || children[0] != left || children[1] != right {
		t.Fatalf("binary root children mismatch: %x", children)
	}
}

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

package trienode

import "github.com/ethereum/go-ethereum/common"

// NewNodeWithPrev constructs a node while accepting the upstream previous-value
// argument. This branch's NodeSet does not track previous blobs, so prev is ignored.
func NewNodeWithPrev(hash common.Hash, blob []byte, _ []byte) *Node {
	return New(hash, blob)
}

// NewDeletedWithPrev constructs a deleted node while accepting the upstream
// previous-value argument. This branch's NodeSet does not track previous blobs.
func NewDeletedWithPrev(_ []byte) *Node {
	return NewDeleted()
}

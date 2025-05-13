// Copyright 2022 The go-ethereum Authors
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
	"github.com/ethereum/go-ethereum/rlp"
)

func cacheNodeToBytes(n cacheNode) []byte {
	w := rlp.NewEncoderBuffer(nil)
	n.encode(w)
	result := w.ToBytes()
	w.Flush()
	return result
}

func (n *cacheFullNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	for _, c := range n.Children {
		if c != nil {
			c.encode(w)
		} else {
			w.Write(rlp.EmptyString)
		}
	}
	w.ListEnd(offset)
}

func (n *cacheFullNodeEncoder) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	for _, c := range n.Children {
		if c == nil {
			w.Write(rlp.EmptyString)
		} else if len(c) < 32 {
			w.Write(c) // rawNode
		} else {
			w.WriteBytes(c) // hashNode
		}
	}
	w.ListEnd(offset)
}

func (n *cacheShortNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key)
	if n.Val != nil {
		n.Val.encode(w)
	} else {
		w.Write(rlp.EmptyString)
	}
	w.ListEnd(offset)
}

func (n *cacheExtNodeEncoder) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key)

	if n.Val == nil {
		w.Write(rlp.EmptyString)
	} else if len(n.Val) < 32 {
		w.Write(n.Val) // rawNode
	} else {
		w.WriteBytes(n.Val) // hashNode
	}
	w.ListEnd(offset)
}

func (n *cacheLeafNodeEncoder) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key) // Compact format key
	w.WriteBytes(n.Val) // Value node, must be non-nil
	w.ListEnd(offset)
}

func (n cacheHashNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

func (n cacheValueNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

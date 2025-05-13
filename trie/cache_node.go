// Copyright 2014 The go-ethereum Authors
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
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

var cacheIndices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

type cacheNode interface {
	cache() (cacheHashNode, bool)
	encode(w rlp.EncoderBuffer)
	fstring(string) string
}

type (
	cacheFullNode struct {
		Children [17]cacheNode // Actual trie node data to encode/decode (needs custom encoder)
		flags    cacheNodeFlag
		window   int
		size     int
	}
	cacheShortNode struct {
		Key    []byte
		Val    cacheNode
		flags  cacheNodeFlag
		window int
		size   int
	}
	cacheHashNode  []byte
	cacheValueNode []byte

	// cacheFullNodeEncoder is a type used exclusively for encoding fullNode.
	cacheFullNodeEncoder struct {
		Children [17][]byte
	}

	// cacheExtNodeEncoder is a type used exclusively for encoding extension node.
	cacheExtNodeEncoder struct {
		Key []byte
		Val []byte
	}

	// cacheLeafNodeEncoder is a type used exclusively for encoding leaf node.
	cacheLeafNodeEncoder struct {
		Key []byte
		Val []byte
	}
)

// cacheNilValueNode is used when collapsing internal trie nodes for hashing, since
// unset children need to serialize correctly.
var cacheNilValueNode = cacheValueNode(nil)

// EncodeRLP encodes a full node into the consensus RLP format.
func (n *cacheFullNode) EncodeRLP(w io.Writer) error {
	eb := rlp.NewEncoderBuffer(w)
	n.encode(eb)
	return eb.Flush()
}

func (n *cacheFullNode) copy() *cacheFullNode   { copy := *n; return &copy }
func (n *cacheShortNode) copy() *cacheShortNode { copy := *n; return &copy }

// cacheNodeFlag contains caching-related metadata about a node.
type cacheNodeFlag struct {
	hash  cacheHashNode // cached hash of the node (may be nil)
	dirty bool          // whether the node has changes that must be written to the database
}

func (n *cacheFullNode) cache() (cacheHashNode, bool)  { return n.flags.hash, n.flags.dirty }
func (n *cacheShortNode) cache() (cacheHashNode, bool) { return n.flags.hash, n.flags.dirty }
func (n cacheHashNode) cache() (cacheHashNode, bool)   { return nil, true }
func (n cacheValueNode) cache() (cacheHashNode, bool)  { return nil, true }

// Pretty printing.
func (n *cacheFullNode) String() string  { return n.fstring("") }
func (n *cacheShortNode) String() string { return n.fstring("") }
func (n cacheHashNode) String() string   { return n.fstring("") }
func (n cacheValueNode) String() string  { return n.fstring("") }

func (n *cacheFullNode) fstring(ind string) string {
	resp := fmt.Sprintf("[\n%s  ", ind)
	for i, node := range &n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", cacheIndices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", cacheIndices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *cacheShortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", n.Key, n.Val.fstring(ind+"  "))
}
func (n cacheHashNode) fstring(ind string) string {
	return fmt.Sprintf("<%x> ", []byte(n))
}
func (n cacheValueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}

// cacheDecodeNode parses the RLP encoding of a trie node. It will deep-copy the passed
// byte slice for decoding, so it's safe to modify the byte slice afterwards. The-
// decode performance of this function is not optimal, but it is suitable for most
// scenarios with low performance requirements and hard to determine whether the
// byte slice be modified or not.
func cacheDecodeNode(hash, buf []byte) (cacheNode, error) {
	return cacheDecodeNodeUnsafe(hash, common.CopyBytes(buf))
}

// cacheDecodeNodeUnsafe parses the RLP encoding of a trie node. The passed byte slice
// will be directly referenced by node without bytes deep copy, so the input MUST
// not be changed after.
func cacheDecodeNodeUnsafe(hash, buf []byte) (cacheNode, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := cacheDecodeShort(hash, elems)
		return n, cacheWrapError(err, "short")
	case 17:
		n, err := cacheDecodeFull(hash, elems)
		return n, cacheWrapError(err, "full")
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

func cacheDecodeShort(hash, elems []byte) (cacheNode, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	flag := cacheNodeFlag{hash: cacheHashNode(hash)}
	key := compactToHex(kbuf)
	if hasTerm(key) {
		// value node
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid value node: %v", err)
		}
		return &cacheShortNode{key, cacheValueNode(val), flag, 0, 0}, nil
	}
	r, _, err := cacheDecodeRef(rest)
	if err != nil {
		return nil, cacheWrapError(err, "val")
	}
	return &cacheShortNode{key, r, flag, 0, 0}, nil
}

func cacheDecodeFull(hash, elems []byte) (*cacheFullNode, error) {
	n := &cacheFullNode{flags: cacheNodeFlag{hash: cacheHashNode(hash)}, window: 0, size: 0}
	for i := 0; i < 16; i++ {
		cld, rest, err := cacheDecodeRef(elems)
		if err != nil {
			return n, cacheWrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i], elems = cld, rest
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = cacheValueNode(val)
	}
	return n, nil
}

func cacheDecodeRef(buf []byte) (cacheNode, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := cacheDecodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return cacheHashNode(val), rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

// wraps a decoding error with information about the path to the
// invalid child node (for debugging encoding issues).
type cacheDecodeError struct {
	what  error
	stack []string
}

func cacheWrapError(err error, ctx string) error {
	if err == nil {
		return nil
	}
	if decErr, ok := err.(*cacheDecodeError); ok {
		decErr.stack = append(decErr.stack, ctx)
		return decErr
	}
	return &cacheDecodeError{err, []string{ctx}}
}

func (err *cacheDecodeError) Error() string {
	return fmt.Sprintf("%v (decode path: %s)", err.what, strings.Join(err.stack, "<-"))
}

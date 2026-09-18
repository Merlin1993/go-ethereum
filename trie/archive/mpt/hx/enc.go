// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package hx

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/rlp"
)

// Node blob layout (RLP, versioned by shape; the blob is what the node hash
// commits to, so epoch bits and stubs are covered by the state root):
//
//	fullNode:  [ c0, ..., c16, epochAgg, stubList ]              (19 elements)
//	             ci = embedded raw node (<32B) or 32B child hash or empty
//	             epochAgg = uint64 (16 slots x 2 bits)
//	             stubList = [ [pathCompact, filter, commitment33, count], ... ]
//	shortNode: [ compactKey, valOrRef, meta ]                    (3 elements)
//	          or [ compactKey, valOrRef, meta, stubList ]        (4, rare)
//	             leaf (key has terminator): val = value bytes, meta = epoch (0/1)
//	             extension: val = child ref, meta = subtree aggregate (1..3)
//	             stubList only when stubs hang on a short node (root catch-all
//	             or a compressed path with no branch at the mount point)

func nodeToBytes(n node) []byte {
	w := rlp.NewEncoderBuffer(nil)
	n.encode(w)
	result := w.ToBytes()
	w.Flush()
	return result
}

// encode stubs into the buffer as an RLP list. Stub.Path is an opaque bucket
// identifier (mount path + discriminator), stored raw — membership is decided
// by the filter, not by path prefix matching.
func encodeStubs(w rlp.EncoderBuffer, stubs []*Stub) {
	off := w.List()
	for _, s := range stubs {
		soff := w.List()
		w.WriteBytes(s.Path)
		w.WriteBytes(s.Filter)
		w.WriteBytes(s.Commitment[:])
		w.WriteUint64(uint64(s.Count))
		w.ListEnd(soff)
	}
	w.ListEnd(off)
}

// hexToCompactNoTerm packs a HEX nibble path (no terminator) into compact
// form without the leaf flag. Kept for future key-prefix-encoded identifiers.
func hexToCompactNoTerm(hex []byte) []byte {
	buf := make([]byte, len(hex)/2+1)
	if len(hex)&1 == 1 {
		buf[0] |= 1 << 4 // odd flag
		buf[0] |= hex[0]
		hex = hex[1:]
	}
	decodeNibbles(hex, buf[1:])
	return buf
}

// compactToHexNoTerm unpacks a compact path written by hexToCompactNoTerm.
func compactToHexNoTerm(compact []byte) []byte {
	if len(compact) == 0 {
		return nil
	}
	base := keybytesToHex(compact)
	base = base[:len(base)-1] // drop synthetic terminator
	if compact[0]&0x10 != 0 {
		return base[1:] // odd flag: first nibble lives in the flag byte
	}
	return base[2:]
}

func (n *fullNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	for _, c := range n.Children {
		if c != nil {
			c.encode(w)
		} else {
			w.Write(rlp.EmptyString)
		}
	}
	w.WriteUint64(uint64(n.EpochAgg))
	encodeStubs(w, n.Stubs)
	w.ListEnd(offset)
}

// fullnodeEncoder mirrors geth's collapsed encoding: children already
// resolved to embedded blobs or hashes.
type fullnodeEncoder struct {
	Children [17][]byte
	EpochAgg uint32
	Stubs    []*Stub
}

func (n *fullnodeEncoder) encode(w rlp.EncoderBuffer) {
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
	w.WriteUint64(uint64(n.EpochAgg))
	encodeStubs(w, n.Stubs)
	w.ListEnd(offset)
}

func (n *shortNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key) // compact format
	if n.Val != nil {
		n.Val.encode(w)
	} else {
		w.Write(rlp.EmptyString)
	}
	if hasTerm(compactToHex(n.Key)) {
		w.WriteUint64(uint64(n.Epoch))
	} else {
		w.WriteUint64(uint64(n.Agg))
	}
	if len(n.Stubs) > 0 {
		encodeStubs(w, n.Stubs)
	}
	w.ListEnd(offset)
}

// shortNodeEncoder is the collapsed form used by hasher/committer.
type shortNodeEncoder struct {
	Key  []byte // compact format
	Val  []byte // raw blob (<32B) or hash
	Meta uint64 // epoch (leaf) or aggregate (extension)
}

func (n *shortNodeEncoder) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key)
	if n.Val == nil {
		w.Write(rlp.EmptyString)
	} else if len(n.Val) < 32 {
		w.Write(n.Val)
	} else {
		w.WriteBytes(n.Val)
	}
	w.WriteUint64(n.Meta)
	w.ListEnd(offset)
}

func (n hashNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

func (n valueNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

// decodeNode parses the RLP encoding of a trie node, deep-copying the input.
func decodeNode(hash, buf []byte) (node, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("hx decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 3, 4:
		n, err := decodeShort(hash, elems)
		return n, wrapError(err, "short")
	case 19:
		n, err := decodeFull(hash, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("hx: invalid number of list elements: %v", c)
	}
}

func decodeShort(hash, elems []byte) (node, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	key := compactToHex(kbuf)
	flag := nodeFlag{hash: hash}
	var sn *shortNode
	if hasTerm(key) {
		val, r, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid value node: %v", err)
		}
		rest = r
		meta, r, err := rlp.SplitUint64(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid leaf epoch: %v", err)
		}
		rest = r
		sn = &shortNode{Key: key, Val: valueNode(val), Epoch: byte(meta), flags: flag}
	} else {
		ref, r, err := decodeRef(rest)
		if err != nil {
			return nil, wrapError(err, "val")
		}
		rest = r
		meta, r, err := rlp.SplitUint64(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid extension aggregate: %v", err)
		}
		rest = r
		sn = &shortNode{Key: key, Val: ref, Agg: uint8(meta), flags: flag}
	}
	if len(rest) > 0 { // optional stub list
		stubs, r, err := rlp.SplitList(rest)
		if err != nil {
			return nil, fmt.Errorf("hx: invalid short stub list: %v", err)
		}
		rest = r
		for len(stubs) > 0 {
			var raw []byte
			raw, stubs, err = rlp.SplitList(stubs)
			if err != nil {
				return nil, fmt.Errorf("hx: invalid stub entry: %v", err)
			}
			stub, err := decodeStub(raw)
			if err != nil {
				return nil, err
			}
			sn.Stubs = append(sn.Stubs, stub)
		}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("hx: trailing short node bytes")
	}
	return sn, nil
}

func decodeFull(hash, elems []byte) (*fullNode, error) {
	n := &fullNode{flags: nodeFlag{hash: hash}}
	var err error
	for i := 0; i < 16; i++ {
		var cld node
		cld, elems, err = decodeRef(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i] = cld
	}
	// Slot 16 (branch value): unused for fixed-width state keys but decoded
	// for completeness.
	val, rest, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = valueNode(val)
	}
	agg, rest, err := rlp.SplitUint64(rest)
	if err != nil {
		return n, fmt.Errorf("hx: invalid epoch aggregate: %v", err)
	}
	n.EpochAgg = uint32(agg)
	stubs, _, err := rlp.SplitList(rest)
	if err != nil {
		return n, fmt.Errorf("hx: invalid stub list: %v", err)
	}
	for len(stubs) > 0 {
		var raw []byte
		raw, stubs, err = rlp.SplitList(stubs)
		if err != nil {
			return n, fmt.Errorf("hx: invalid stub entry: %v", err)
		}
		stub, err := decodeStub(raw)
		if err != nil {
			return n, err
		}
		n.Stubs = append(n.Stubs, stub)
	}
	return n, nil
}

func decodeStub(elems []byte) (*Stub, error) {
	path, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, fmt.Errorf("hx: invalid stub path: %v", err)
	}
	filter, rest, err := rlp.SplitString(rest)
	if err != nil {
		return nil, fmt.Errorf("hx: invalid stub filter: %v", err)
	}
	commit, rest, err := rlp.SplitString(rest)
	if err != nil {
		return nil, fmt.Errorf("hx: invalid stub commitment: %v", err)
	}
	if len(commit) != 33 {
		return nil, fmt.Errorf("hx: stub commitment length %d, want 33", len(commit))
	}
	count, rest, err := rlp.SplitUint64(rest)
	if err != nil {
		return nil, fmt.Errorf("hx: invalid stub count: %v", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("hx: trailing stub bytes")
	}
	s := &Stub{
		Path:   bytes.Clone(path),
		Filter: bytes.Clone(filter),
		Count:  uint32(count),
	}
	copy(s.Commitment[:], commit)
	return s, nil
}

func decodeRef(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 'embedded' node reference. The encoding must be smaller
		// than a hash in order to be valid.
		if size := len(buf) - len(rest); size > hashLen {
			err := fmt.Errorf("hx: oversized embedded node (size is %d bytes, want size < %d)", size, hashLen)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// empty node
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return hashNode(val), rest, nil
	default:
		return nil, nil, fmt.Errorf("hx: invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

// decodeError wraps a decoding error with the path to the invalid child.
type decodeError struct {
	what  error
	stack []string
}

func wrapError(err error, ctx string) error {
	if err == nil {
		return nil
	}
	if decErr, ok := err.(*decodeError); ok {
		decErr.stack = append(decErr.stack, ctx)
		return decErr
	}
	return &decodeError{err, []string{ctx}}
}

func (err *decodeError) Error() string {
	return fmt.Sprintf("%v (decode path: %s)", err.what, strings.Join(err.stack, "<-"))
}

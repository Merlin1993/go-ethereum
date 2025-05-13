// Copyright 2023 The go-ethereum Authors
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

package cacheTrie

import (
	"fmt"
	"io"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

var nodeIndices = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "[17]"}

// cacheNode接口定义了缓存节点的基本操作
type cacheNode interface {
	cache() (HashNode, bool)
	encode(w rlp.EncoderBuffer)
	fstring(string) string
}

// 各种节点类型
type (
	// 全节点，包含17个子节点
	FullNode struct {
		Children [17]cacheNode // 16个子节点加一个值节点
		flags    nodeFlag
		window   int // 缓存窗口，用于跟踪节点在哪些区块被访问
		size     int // 记录此节点子树中包含的键值对数量
	}

	// 短节点，用于压缩路径
	ShortNode struct {
		Key    []byte
		Val    cacheNode
		flags  nodeFlag
		window int
		size   int
	}

	// 哈希节点，存储节点的哈希值
	HashNode []byte

	// 值节点，存储实际的值
	ValueNode []byte
)

// 节点标志，用于存储节点的哈希和状态
type nodeFlag struct {
	hash  HashNode // 缓存的哈希值
	dirty bool     // 标记节点是否被修改过
}

// 返回节点的哈希值和dirty标志
func (n *FullNode) cache() (HashNode, bool)  { return n.flags.hash, n.flags.dirty }
func (n *ShortNode) cache() (HashNode, bool) { return n.flags.hash, n.flags.dirty }
func (n HashNode) cache() (HashNode, bool)   { return nil, true }
func (n ValueNode) cache() (HashNode, bool)  { return nil, true }

// 复制节点
func (n *FullNode) copy() *FullNode   { copy := *n; return &copy }
func (n *ShortNode) copy() *ShortNode { copy := *n; return &copy }

// 节点的字符串表示
func (n *FullNode) String() string  { return n.fstring("") }
func (n *ShortNode) String() string { return n.fstring("") }
func (n HashNode) String() string   { return n.fstring("") }
func (n ValueNode) String() string  { return n.fstring("") }

// 格式化输出节点信息，用于调试
func (n *FullNode) fstring(ind string) string {
	resp := fmt.Sprintf("[\n%s  ", ind)
	for i, node := range &n.Children {
		if node == nil {
			resp += fmt.Sprintf("%s: <nil> ", nodeIndices[i])
		} else {
			resp += fmt.Sprintf("%s: %v", nodeIndices[i], node.fstring(ind+"  "))
		}
	}
	return resp + fmt.Sprintf("\n%s] ", ind)
}

func (n *ShortNode) fstring(ind string) string {
	return fmt.Sprintf("{%x: %v} ", n.Key, n.Val.fstring(ind+"  "))
}

func (n HashNode) fstring(ind string) string {
	return fmt.Sprintf("<%x> ", []byte(n))
}

func (n ValueNode) fstring(ind string) string {
	return fmt.Sprintf("%x ", []byte(n))
}

// NilValueNode 用于表示空值节点
var NilValueNode = ValueNode(nil)

// 节点编码相关方法
func (n *FullNode) encode(w rlp.EncoderBuffer) {
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

func (n *ShortNode) encode(w rlp.EncoderBuffer) {
	offset := w.List()
	w.WriteBytes(n.Key)
	if n.Val != nil {
		n.Val.encode(w)
	} else {
		w.Write(rlp.EmptyString)
	}
	w.ListEnd(offset)
}

func (n HashNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

func (n ValueNode) encode(w rlp.EncoderBuffer) {
	w.WriteBytes(n)
}

// EncodeRLP 将全节点编码为RLP格式
func (n *FullNode) EncodeRLP(w io.Writer) error {
	eb := rlp.NewEncoderBuffer(w)
	n.encode(eb)
	return eb.Flush()
}

// nodeToBytes 将节点编码为字节数组
func nodeToBytes(n cacheNode) []byte {
	w := rlp.NewEncoderBuffer(nil)
	n.encode(w)
	result := w.ToBytes()
	w.Flush()
	return result
}

// decodeNode 解码RLP数据为节点
func decodeNode(hash, buf []byte) (cacheNode, error) {
	return decodeNodeUnsafe(hash, common.CopyBytes(buf))
}

// decodeNodeUnsafe 直接解码RLP数据为节点，不拷贝输入
func decodeNodeUnsafe(hash, buf []byte) (cacheNode, error) {
	if len(buf) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	elems, _, err := rlp.SplitList(buf)
	if err != nil {
		return nil, fmt.Errorf("decode error: %v", err)
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		n, err := decodeShort(hash, elems)
		return n, wrapError(err, "short")
	case 17:
		n, err := decodeFull(hash, elems)
		return n, wrapError(err, "full")
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

// 解码短节点
func decodeShort(hash, elems []byte) (cacheNode, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	flag := nodeFlag{hash: HashNode(hash)}
	key := compactToHex(kbuf)
	if hasTerm(key) {
		// 值节点
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid value node: %v", err)
		}
		return &ShortNode{key, ValueNode(val), flag, 0, 0}, nil
	}
	r, _, err := decodeRef(rest)
	if err != nil {
		return nil, wrapError(err, "val")
	}
	return &ShortNode{key, r, flag, 0, 0}, nil
}

// 解码全节点
func decodeFull(hash, elems []byte) (*FullNode, error) {
	n := &FullNode{flags: nodeFlag{hash: HashNode(hash)}, window: 0, size: 0}
	for i := 0; i < 16; i++ {
		cld, rest, err := decodeRef(elems)
		if err != nil {
			return n, wrapError(err, fmt.Sprintf("[%d]", i))
		}
		n.Children[i], elems = cld, rest
	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return n, err
	}
	if len(val) > 0 {
		n.Children[16] = ValueNode(val)
	}
	return n, nil
}

// 解码节点引用
func decodeRef(buf []byte) (cacheNode, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, buf, err
	}
	switch {
	case kind == rlp.List:
		// 嵌入式节点引用
		if size := len(buf) - len(rest); size > 32 {
			err := fmt.Errorf("oversized embedded node (size is %d bytes, want size < %d)", size, 32)
			return nil, buf, err
		}
		n, err := decodeNode(nil, buf)
		return n, rest, err
	case kind == rlp.String && len(val) == 0:
		// 空节点
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return HashNode(val), rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 or 32)", len(val))
	}
}

// 错误包装
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

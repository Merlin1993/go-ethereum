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
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// CacheTrie相关错误
var (
	// ErrNotFound 表示所请求的trie节点在缓存中未找到
	ErrNotFound = errors.New("trie node not found")

	// ErrCommitted 表示树已提交
	ErrCommitted = errors.New("trie already committed")
)

// MissingNodeError 当请求的节点在缓存中未找到时返回
type MissingNodeError struct {
	NodeHash common.Hash // 请求的节点的哈希值
	Path     []byte      // 到达缺失节点的Hex-编码路径
	err      error       // 底层存储错误
}

func (err *MissingNodeError) Error() string {
	if err.err != nil {
		return fmt.Sprintf("missing trie node %x (path %x): %v", err.NodeHash, err.Path, err.err)
	}
	return fmt.Sprintf("missing trie node %x (path %x)", err.NodeHash, err.Path)
}

// 返回底层错误，如果有
func (err *MissingNodeError) Unwrap() error {
	return err.err
}

// Copyright 2024 The go-ethereum Authors
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
	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// StateTrieInterface 定义状态trie的接口
// 这里我们重新定义接口是因为我们需要在trie包中使用，但core/state.Trie在trie包中不可见
type StateTrieInterface interface {
	// GetKey returns the sha3 preimage of a hashed key that was previously used
	// to store a value.
	GetKey([]byte) []byte

	// GetAccount abstracts an account read from the trie.
	GetAccount(address common.Address) (*types.StateAccount, error)

	// GetStorage returns the value for key stored in the trie.
	GetStorage(addr common.Address, key []byte) ([]byte, error)

	// UpdateAccount abstracts an account write to the trie.
	UpdateAccount(address common.Address, account *types.StateAccount, codeLen int) error

	// UpdateAccountRLP updates account with RLP-encoded data
	UpdateAccountRLP(address common.Address, account []byte, codeLen int) error

	// UpdateStorage associates key with value in the trie.
	UpdateStorage(addr common.Address, key, value []byte) error

	// DeleteAccount abstracts an account deletion from the trie.
	DeleteAccount(address common.Address) error

	// DeleteStorage removes any existing value for key from the trie.
	DeleteStorage(addr common.Address, key []byte) error

	// UpdateContractCode abstracts code write to the trie.
	UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error

	// Hash returns the root hash of the trie.
	Hash() common.Hash

	// Commit collects all dirty nodes in the trie and replace them with the
	// corresponding node hash.
	Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet)

	// Witness returns a set containing all trie nodes that have been accessed.
	Witness() map[string]struct{}

	// NodeIterator returns an iterator that returns nodes of the trie.
	NodeIterator(startKey []byte) (NodeIterator, error)

	// Prove constructs a Merkle proof for key.
	Prove(key []byte, proofDb ethdb.KeyValueWriter) error

	// IsVerkle returns true if the trie is verkle-tree based
	IsVerkle() bool
}

// CacheProxyTrie 包装任何trie实现并提供缓存功能
// 它实现了StateTrieInterface接口，可以透明地替换底层的trie
type CacheProxyTrie struct {
	underlying StateTrieInterface   // 底层的trie实现（StateTrie或VerkleTrie）
	cache      *cachetrie.CacheTrie // 缓存层
	useCache   bool                 // 是否启用缓存
}

// NewCacheProxyTrie 创建一个新的缓存代理trie
func NewCacheProxyTrie(underlying StateTrieInterface, cache *cachetrie.CacheTrie) *CacheProxyTrie {
	return &CacheProxyTrie{
		underlying: underlying,
		cache:      cache,
		useCache:   false, // 默认关闭
	}
}

// GetKey 返回哈希键的原始预映像
func (t *CacheProxyTrie) GetKey(hash []byte) []byte {
	return t.underlying.GetKey(hash)
}

// GetAccount 获取账户信息，优先从缓存获取
func (t *CacheProxyTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	// 如果有缓存，先从缓存中尝试获取
	if t.cache != nil {
		cacheNode, err := t.cache.Get(address.Bytes())
		if err == nil && cacheNode != nil {
			if valueNode, ok := cacheNode.(cachetrie.ValueNode); ok {
				if len(valueNode.Data) > 0 {
					ret := new(types.StateAccount)
					if err := rlp.DecodeBytes(valueNode.Data, ret); err == nil {
						return ret, nil
					}
				} else {
					return nil, nil
				}
			}
		}
	}

	// 如果缓存中没有或无缓存，从底层trie获取
	account, err := t.underlying.GetAccount(address)
	if err != nil {
		return nil, err
	}

	// 如果启用useCache且获取成功，将结果写入缓存（标记为非新内容）
	if t.useCache && t.cache != nil && account != nil {
		data, err := rlp.EncodeToBytes(account)
		if err == nil {
			t.cache.Update(address.Bytes(), data, false)
		}
	}

	return account, nil
}

// GetStorage 获取存储槽值，优先从缓存获取
func (t *CacheProxyTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	// 如果有缓存，先从缓存中尝试获取
	if t.cache != nil {
		cacheNode, err := t.cache.GetWithAddress(addr, key)
		if err == nil && cacheNode != nil {
			if valueNode, ok := cacheNode.(cachetrie.ValueNode); ok {
				if len(valueNode.Data) > 0 {
					content := valueNode.Data
					// 提取RLP编码中的实际内容
					_, actualContent, _, err := rlp.Split(content)
					if err != nil {
						return content, nil // 如果解码失败，直接返回原始内容
					}
					return actualContent, nil
				} else {
					return nil, nil
				}
			}
		}
	}

	// 如果缓存中没有或无缓存，从底层trie获取
	value, err := t.underlying.GetStorage(addr, key)
	if err != nil {
		return nil, err
	}

	// 如果启用useCache且获取成功，将结果写入缓存（标记为非新内容）
	if t.useCache && t.cache != nil && len(value) > 0 {
		// 使用RLP编码存储值
		encoded, _ := rlp.EncodeToBytes(value)
		t.cache.UpdateWithAddress(addr, key, encoded, false)
	}

	return value, nil
}

// UpdateAccount 更新账户信息
func (t *CacheProxyTrie) UpdateAccount(address common.Address, account *types.StateAccount, codeLen int) error {
	// 如果有缓存，直接更新缓存而不更新底层trie
	if t.cache != nil {
		data, err := rlp.EncodeToBytes(account)
		if err != nil {
			return err
		}
		return t.cache.Update(address.Bytes(), data, true)
	}

	// 如果无缓存，更新底层trie
	return t.underlying.UpdateAccount(address, account, codeLen)
}

// UpdateAccountRLP 使用RLP编码的账户数据更新账户
func (t *CacheProxyTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	// 如果有缓存，直接更新缓存而不更新底层trie
	if t.cache != nil {
		return t.cache.Update(address.Bytes(), account, true)
	}

	// 如果无缓存，更新底层trie
	return t.underlying.UpdateAccountRLP(address, account, codeLen)
}

// UpdateStorage 更新存储槽
func (t *CacheProxyTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	// 如果有缓存，直接更新缓存而不更新底层trie
	if t.cache != nil {
		// 使用RLP编码存储值
		encoded, _ := rlp.EncodeToBytes(value)
		return t.cache.UpdateWithAddress(addr, key, encoded, true)
	}

	// 如果无缓存，更新底层trie
	return t.underlying.UpdateStorage(addr, key, value)
}

// DeleteAccount 删除账户
func (t *CacheProxyTrie) DeleteAccount(address common.Address) error {
	// 如果有缓存，直接从缓存删除而不操作底层trie
	if t.cache != nil {
		return t.cache.Delete(address.Bytes())
	}

	// 如果无缓存，从底层trie删除
	return t.underlying.DeleteAccount(address)
}

// DeleteStorage 删除存储槽
func (t *CacheProxyTrie) DeleteStorage(addr common.Address, key []byte) error {
	// 如果有缓存，直接从缓存删除而不操作底层trie
	if t.cache != nil {
		return t.cache.DeleteWithAddress(addr, key)
	}

	// 如果无缓存，从底层trie删除
	return t.underlying.DeleteStorage(addr, key)
}

// UpdateContractCode 更新合约代码
func (t *CacheProxyTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return nil
}

// Hash 返回trie的根哈希
func (t *CacheProxyTrie) Hash() common.Hash {
	return t.underlying.Hash()
}

// Commit 提交所有修改
func (t *CacheProxyTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	return t.underlying.Commit(collectLeaf)
}

// Witness 返回见证数据
func (t *CacheProxyTrie) Witness() map[string]struct{} {
	return t.underlying.Witness()
}

// NodeIterator 返回节点迭代器
func (t *CacheProxyTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	return t.underlying.NodeIterator(startKey)
}

// Prove 构造Merkle证明
func (t *CacheProxyTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return t.underlying.Prove(key, proofDb)
}

// IsVerkle 返回是否为Verkle trie
func (t *CacheProxyTrie) IsVerkle() bool {
	return t.underlying.IsVerkle()
}

// Copy 返回trie的拷贝
func (t *CacheProxyTrie) Copy() *CacheProxyTrie {
	var cacheCopy *cachetrie.CacheTrie
	if t.cache != nil {
		// Note: CacheTrie目前可能没有Copy方法，这里先设为nil
		// 在实际使用中可能需要根据具体情况处理
		cacheCopy = t.cache
	}

	// 根据底层trie类型进行复制
	var underlyingCopy StateTrieInterface
	switch ut := t.underlying.(type) {
	case *StateTrie:
		underlyingCopy = ut.Copy()
	case *VerkleTrie:
		underlyingCopy = ut.Copy()
	default:
		// 对于其他类型，尝试直接复制（可能需要类型断言）
		underlyingCopy = t.underlying
	}

	return NewCacheProxyTrie(underlyingCopy, cacheCopy)
}

// GetCache 返回缓存实例（用于调试或统计）
func (t *CacheProxyTrie) GetCache() *cachetrie.CacheTrie {
	return t.cache
}

// SetCacheEnabled 设置是否启用缓存
func (t *CacheProxyTrie) SetCacheEnabled(enabled bool) {
	t.useCache = enabled
}

// GetUnderlying 返回底层trie实例
func (t *CacheProxyTrie) GetUnderlying() StateTrieInterface {
	return t.underlying
}

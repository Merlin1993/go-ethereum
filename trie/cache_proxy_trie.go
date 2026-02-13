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
	"sync/atomic"

	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// StateTrieInterface defines the interface for state trie
// We redefine this interface here because we need to use it in the trie package,
// but core/state.Trie is not visible in the trie package
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

// CacheProxyTrie wraps any trie implementation and provides caching functionality
// It implements the StateTrieInterface and can transparently replace the underlying trie
type CacheProxyTrie struct {
	underlying StateTrieInterface   // underlying trie implementation (StateTrie or VerkleTrie)
	cache      *cachetrie.CacheTrie // cache layer
	useCache   bool                 // whether caching is enabled
}

// NewCacheProxyTrie creates a new cache proxy trie
func NewCacheProxyTrie(underlying StateTrieInterface, cache *cachetrie.CacheTrie) *CacheProxyTrie {
	return &CacheProxyTrie{
		underlying: underlying,
		cache:      cache,
		useCache:   false, // disabled by default
	}
}

// GetKey returns the original preimage of a hashed key
func (t *CacheProxyTrie) GetKey(hash []byte) []byte {
	return t.underlying.GetKey(hash)
}

// GetAccount retrieves account information, prioritizing cache access
func (t *CacheProxyTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	// If cache is available, try to get from cache first
	if t.cache != nil {
		cacheNode, err := t.cache.Get(address.Bytes())
		if err == nil && cacheNode != nil {
			if valueNode, ok := cacheNode.(cachetrie.ValueNode); ok {
				if len(valueNode.Data) > 0 {
					ret := new(types.StateAccount)
					if err := rlp.DecodeBytes(valueNode.Data, ret); err == nil {
						atomic.AddInt64(&common.CacheAccountHit, 1)
						atomic.AddInt64(&common.CacheAccountReadSize, int64(len(valueNode.Data)))
						return ret, nil
					}
				} else {
					return nil, nil
				}
			}
		}
	}

	// If no cache or cache miss, get from underlying trie
	account, err := t.underlying.GetAccount(address)
	if err != nil {
		return nil, err
	}

	// If useCache is enabled and successful, write to cache (marked as non-new content)
	if t.useCache && t.cache != nil && account != nil {
		data, err := rlp.EncodeToBytes(account)
		if err == nil {
			t.cache.Update(address.Bytes(), data, false)
		}
	}

	return account, nil
}

// GetStorage retrieves storage slot value, prioritizing cache access
func (t *CacheProxyTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	// If cache is available, try to get from cache first
	if t.cache != nil {
		cacheNode, err := t.cache.GetWithAddress(addr, key)
		if err == nil && cacheNode != nil {
			if valueNode, ok := cacheNode.(cachetrie.ValueNode); ok {
				if len(valueNode.Data) > 0 {
					content := valueNode.Data
					// Extract actual content from RLP-encoded data
					_, actualContent, _, err := rlp.Split(content)
					if err != nil {
						atomic.AddInt64(&common.CacheStorageHit, 1)
						atomic.AddInt64(&common.CacheStorageReadSize, int64(len(content)))
						return content, nil // If decoding fails, return original content directly
					}
					atomic.AddInt64(&common.CacheStorageHit, 1)
					atomic.AddInt64(&common.CacheStorageReadSize, int64(len(content)))
					return actualContent, nil
				} else {
					return nil, nil
				}
			}
		}
	}

	// If no cache or cache miss, get from underlying trie
	value, err := t.underlying.GetStorage(addr, key)
	if err != nil {
		return nil, err
	}

	// If useCache is enabled and successful, write to cache (marked as non-new content)
	if t.useCache && t.cache != nil && len(value) > 0 {
		// Use RLP encoding to store the value
		encoded, _ := rlp.EncodeToBytes(value)
		t.cache.UpdateWithAddress(addr, key, encoded, false)
	}

	return value, nil
}

// UpdateAccount updates account information
func (t *CacheProxyTrie) UpdateAccount(address common.Address, account *types.StateAccount, codeLen int) error {
	// If cache is available, update cache directly without updating underlying trie
	if t.cache != nil {
		data, err := rlp.EncodeToBytes(account)
		if err != nil {
			return err
		}
		atomic.AddInt64(&common.CacheAccountWriteSize, int64(len(data)))
		return t.cache.Update(address.Bytes(), data, true)
	}

	// If no cache, update underlying trie
	return t.underlying.UpdateAccount(address, account, codeLen)
}

// UpdateAccountRLP updates account with RLP-encoded data
func (t *CacheProxyTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	// If cache is available, update cache directly without updating underlying trie
	if t.cache != nil {
		atomic.AddInt64(&common.CacheAccountWriteSize, int64(len(account)))
		return t.cache.Update(address.Bytes(), account, true)
	}

	// If no cache, update underlying trie
	return t.underlying.UpdateAccountRLP(address, account, codeLen)
}

// UpdateStorage updates storage slot
func (t *CacheProxyTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	// If cache is available, update cache directly without updating underlying trie
	if t.cache != nil {
		// Use RLP encoding to store the value
		encoded, _ := rlp.EncodeToBytes(value)
		atomic.AddInt64(&common.CacheStorageWriteSize, int64(len(encoded)))
		return t.cache.UpdateWithAddress(addr, key, encoded, true)
	}

	// If no cache, update underlying trie
	return t.underlying.UpdateStorage(addr, key, value)
}

// DeleteAccount deletes account
func (t *CacheProxyTrie) DeleteAccount(address common.Address) error {
	// If cache is available, delete from cache directly without operating underlying trie
	if t.cache != nil {
		return t.cache.Delete(address.Bytes())
	}

	// If no cache, delete from underlying trie
	return t.underlying.DeleteAccount(address)
}

// DeleteStorage deletes storage slot
func (t *CacheProxyTrie) DeleteStorage(addr common.Address, key []byte) error {
	// If cache is available, delete from cache directly without operating underlying trie
	if t.cache != nil {
		return t.cache.DeleteWithAddress(addr, key)
	}

	// If no cache, delete from underlying trie
	return t.underlying.DeleteStorage(addr, key)
}

// UpdateContractCode updates contract code
func (t *CacheProxyTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return nil
}

// Hash returns the root hash of the trie
func (t *CacheProxyTrie) Hash() common.Hash {
	return t.underlying.Hash()
}

// Commit collects all dirty nodes in the trie and replace them with the
// corresponding node hash.
func (t *CacheProxyTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	return t.underlying.Commit(collectLeaf)
}

// Witness returns a set containing all trie nodes that have been accessed.
func (t *CacheProxyTrie) Witness() map[string]struct{} {
	return t.underlying.Witness()
}

// NodeIterator returns an iterator that returns nodes of the trie.
func (t *CacheProxyTrie) NodeIterator(startKey []byte) (NodeIterator, error) {
	return t.underlying.NodeIterator(startKey)
}

// Prove constructs a Merkle proof for key.
func (t *CacheProxyTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return t.underlying.Prove(key, proofDb)
}

// IsVerkle returns true if the trie is verkle-tree based
func (t *CacheProxyTrie) IsVerkle() bool {
	return t.underlying.IsVerkle()
}

// Copy returns a copy of the trie
func (t *CacheProxyTrie) Copy() *CacheProxyTrie {
	var cacheCopy *cachetrie.CacheTrie
	if t.cache != nil {
		// Note: CacheTrie currently does not have a Copy method,
		// so we set it to nil for now.
		// In a real scenario, this might need to be handled differently
		// depending on the specific CacheTrie implementation.
		cacheCopy = t.cache
	}

	// Copy based on underlying trie type
	var underlyingCopy StateTrieInterface
	switch ut := t.underlying.(type) {
	case *StateTrie:
		underlyingCopy = ut.Copy()
	case *VerkleTrie:
		underlyingCopy = ut.Copy()
	default:
		// For other types, try to copy directly (may require type assertion)
		underlyingCopy = t.underlying
	}

	return NewCacheProxyTrie(underlyingCopy, cacheCopy)
}

// GetCache returns the cache instance (for debugging or statistics)
func (t *CacheProxyTrie) GetCache() *cachetrie.CacheTrie {
	return t.cache
}

// SetCacheEnabled sets whether caching is enabled
func (t *CacheProxyTrie) SetCacheEnabled(enabled bool) {
	t.useCache = enabled
}

// GetUnderlying returns the underlying trie instance
func (t *CacheProxyTrie) GetUnderlying() StateTrieInterface {
	return t.underlying
}

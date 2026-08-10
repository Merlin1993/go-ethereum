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

package archive

import (
	"sync"

	"github.com/ethereum/go-ethereum/common/lru"
)

const (
	// DefaultStemCacheLimit bounds the number of recently used active stems.
	// The cache is intentionally much smaller than the outer node cache because
	// each entry contains suffix values and the sparse eight-level commitment.
	DefaultStemCacheLimit = 64 * 1024

	// DefaultStemCacheBytesLimit is a conservative guard for cloned values and
	// commitment nodes. The accounting deliberately includes map/object
	// overhead estimates instead of counting only payload bytes.
	DefaultStemCacheBytesLimit int64 = 128 * 1024 * 1024

	stemCacheShardCount = 64
)

type cachedStemState struct {
	stem  *Stem
	split bool
	size  int64
}

type stemStateCacheShard struct {
	mu               sync.Mutex
	cache            lru.BasicLRU[string, cachedStemState]
	limit            int
	bytesLimit       int64
	bytes            int64
	hits             int64
	misses           int64
	evictions        int64
	entryEvictions   int64
	byteEvictions    int64
	oversizedRejects int64
}

type stemStateCache struct {
	shards     []stemStateCacheShard
	limit      int
	bytesLimit int64
}

// StemCacheDiagnostics reports the current size and lifetime behavior of the
// active-stem cache.
type StemCacheDiagnostics struct {
	Entries          int64
	Bytes            int64
	EntryLimit       int64
	BytesLimit       int64
	Shards           int64
	Hits             int64
	Misses           int64
	Evictions        int64
	EntryEvictions   int64
	ByteEvictions    int64
	OversizedRejects int64
}

func newStemStateCache(limit int, bytesLimit int64) *stemStateCache {
	switch {
	case limit == 0:
		limit = DefaultStemCacheLimit
	case limit < 0:
		return nil
	}
	if limit <= 0 {
		return nil
	}
	switch {
	case bytesLimit == 0:
		bytesLimit = DefaultStemCacheBytesLimit
	case bytesLimit < 0:
		bytesLimit = 0
	}
	shardCount := stemCacheShardCount
	if limit < stemCacheShardCount*16 {
		shardCount = 1
	}
	cache := &stemStateCache{
		shards:     make([]stemStateCacheShard, shardCount),
		limit:      limit,
		bytesLimit: bytesLimit,
	}
	for i := range cache.shards {
		entryLimit := limit / shardCount
		if i < limit%shardCount {
			entryLimit++
		}
		byteLimit := int64(0)
		if bytesLimit > 0 {
			byteLimit = bytesLimit / int64(shardCount)
			if int64(i) < bytesLimit%int64(shardCount) {
				byteLimit++
			}
		}
		cache.shards[i] = stemStateCacheShard{
			cache:      lru.NewBasicLRU[string, cachedStemState](entryLimit),
			limit:      entryLimit,
			bytesLimit: byteLimit,
		}
	}
	return cache
}

func (c *stemStateCache) shard(key []byte) *stemStateCacheShard {
	return &c.shards[nodeCacheHash(key)%uint64(len(c.shards))]
}

func (c *stemStateCache) get(key []byte) (*Stem, bool, bool) {
	if c == nil || len(key) == 0 {
		return nil, false, false
	}
	shard := c.shard(key)
	shard.mu.Lock()
	entry, ok := shard.cache.Get(string(key))
	if ok {
		shard.hits++
	} else {
		shard.misses++
	}
	shard.mu.Unlock()
	recordStemCacheLookup(ok)
	if !ok {
		return nil, false, false
	}
	return cloneStem(entry.stem), entry.split, true
}

func (c *stemStateCache) add(key []byte, stem *Stem, split bool) {
	if c == nil || len(key) == 0 || stem == nil {
		return
	}
	c.addOwned(key, cloneStem(stem), split)
}

// addOwned stores a stem that the caller will no longer mutate. Update paths
// use this after the backend accepted the new root, avoiding a second clone on
// every successful write.
func (c *stemStateCache) addOwned(key []byte, stem *Stem, split bool) {
	if c == nil || len(key) == 0 || stem == nil {
		return
	}
	k := string(key)
	size := estimateCachedStemBytes(k, stem)
	entry := cachedStemState{stem: stem, split: split, size: size}
	shard := c.shard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.bytesLimit > 0 && size > shard.bytesLimit {
		shard.removeLocked(k)
		shard.oversizedRejects++
		recordStemCacheOversizedReject()
		return
	}
	if old, ok := shard.cache.Peek(k); ok {
		shard.bytes -= old.size
		shard.cache.Remove(k)
	}
	for shard.limit > 0 && shard.cache.Len() >= shard.limit {
		if shard.removeOldestLocked() {
			shard.entryEvictions++
			recordStemCacheEviction(true)
		}
	}
	for shard.bytesLimit > 0 && shard.bytes+size > shard.bytesLimit && shard.cache.Len() > 0 {
		if shard.removeOldestLocked() {
			shard.byteEvictions++
			recordStemCacheEviction(false)
		}
	}
	shard.cache.Add(k, entry)
	shard.bytes += size
}

func (c *stemStateCache) remove(key []byte) {
	if c == nil || len(key) == 0 {
		return
	}
	shard := c.shard(key)
	shard.mu.Lock()
	shard.removeLocked(string(key))
	shard.mu.Unlock()
}

func (c *stemStateCache) clear() {
	if c == nil {
		return
	}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		shard.cache.Purge()
		shard.bytes = 0
		shard.mu.Unlock()
	}
}

func (c *stemStateCache) diagnostics() StemCacheDiagnostics {
	if c == nil {
		return StemCacheDiagnostics{}
	}
	diag := StemCacheDiagnostics{
		EntryLimit: int64(c.limit),
		BytesLimit: c.bytesLimit,
		Shards:     int64(len(c.shards)),
	}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		diag.Entries += int64(shard.cache.Len())
		diag.Bytes += shard.bytes
		diag.Hits += shard.hits
		diag.Misses += shard.misses
		diag.Evictions += shard.evictions
		diag.EntryEvictions += shard.entryEvictions
		diag.ByteEvictions += shard.byteEvictions
		diag.OversizedRejects += shard.oversizedRejects
		shard.mu.Unlock()
	}
	return diag
}

func (s *stemStateCacheShard) removeLocked(key string) {
	if old, ok := s.cache.Peek(key); ok {
		s.bytes -= old.size
		s.cache.Remove(key)
	}
	if s.bytes < 0 {
		s.bytes = 0
	}
}

func (s *stemStateCacheShard) removeOldestLocked() bool {
	_, old, ok := s.cache.RemoveOldest()
	if !ok {
		return false
	}
	s.bytes -= old.size
	s.evictions++
	if s.bytes < 0 {
		s.bytes = 0
	}
	return true
}

func cloneStem(stem *Stem) *Stem {
	if stem == nil {
		return nil
	}
	cloned := &Stem{
		values:  make(map[byte][]byte, len(stem.values)),
		present: stem.present,
		count:   stem.count,
	}
	// Values and commitment hashes are immutable once installed: Stem.Get
	// returns a copy, while updates replace map entries instead of editing the
	// byte slices in place. Copying the maps is therefore sufficient isolation
	// for copy-on-write updates and avoids cloning every sibling value on a hit.
	for suffix, value := range stem.values {
		cloned.values[suffix] = value
	}
	if stem.commitment != nil {
		commitment := &stemCommitment{
			hasher: stem.commitment.hasher,
			empty:  stem.commitment.empty,
			nodes:  make(map[uint16][]byte, len(stem.commitment.nodes)),
		}
		for key, hash := range stem.commitment.nodes {
			commitment.nodes[key] = hash
		}
		cloned.commitment = commitment
	}
	return cloned
}

func estimateCachedStemBytes(key string, stem *Stem) int64 {
	// Base object, map headers and the LRU key/value wrappers.
	size := int64(len(key) + 256)
	for _, value := range stem.values {
		// Map bucket/key overhead plus the cloned value.
		size += int64(64 + len(value))
	}
	if stem.commitment != nil {
		for _, hash := range stem.commitment.nodes {
			// Sparse-node map overhead plus the cloned hash.
			size += int64(64 + len(hash))
		}
	}
	return size
}

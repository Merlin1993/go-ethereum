package archive

import (
	"sync"

	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/trie/archive/ecmh"
)

// DefaultNodeCacheLimit mirrors the archive trie wrapper default. A zero config
// value resolves to this default; negative values disable the cache.
const DefaultNodeCacheLimit = 262144

// DefaultNodeCacheBytesLimit is a hard memory guard for serialized clean-node
// blobs. The entry limit alone is not enough for long path-mode replays because
// archive metadata nodes can vary widely in size.
const DefaultNodeCacheBytesLimit int64 = 512 * 1024 * 1024

// DefaultNodeCacheWarmPathBits disables commit-time eager warming in path mode.
// Positive values are useful for experiments, but current long runs show that
// even root eager warming costs more in commit/LRU churn than it saves. Loaded
// nodes are still cached lazily on demand.
const DefaultNodeCacheWarmPathBits = -2

// DefaultCommitmentPointCacheLimit keeps decoded ECMH commitment-point caching
// disabled by default. Long destructive path runs showed near-zero reuse for
// bucket commitments because merge inputs are usually consumed immediately.
const DefaultCommitmentPointCacheLimit = -1

// DefaultCommitWorkers caps destructive path-mode shard commits to avoid
// turning large dirty-shard windows into LevelDB/GC contention spikes.
const DefaultCommitWorkers = 16

type nodeBlobCache struct {
	mu         sync.Mutex
	cache      lru.BasicLRU[string, []byte]
	limit      int
	bytesLimit int64
	bytes      int64
	hits       int64
	misses     int64
	evictions  int64
}

// NodeCacheDiagnostics contains the current cache size and lifetime lookup
// counters. Counters are maintained under the cache's existing mutex, so they
// add no extra synchronization to the read path.
type NodeCacheDiagnostics struct {
	Entries   int64
	Bytes     int64
	Hits      int64
	Misses    int64
	Evictions int64
}

func newNodeBlobCache(limit int) *nodeBlobCache {
	return newNodeBlobCacheWithBytesLimit(limit, DefaultNodeCacheBytesLimit)
}

func newNodeBlobCacheWithBytesLimit(limit int, bytesLimit int64) *nodeBlobCache {
	switch {
	case limit == 0:
		limit = DefaultNodeCacheLimit
	case limit < 0:
		return nil
	}
	if limit <= 0 {
		return nil
	}
	switch {
	case bytesLimit == 0:
		bytesLimit = DefaultNodeCacheBytesLimit
	case bytesLimit < 0:
		bytesLimit = 0
	}
	return &nodeBlobCache{
		cache:      lru.NewBasicLRU[string, []byte](limit),
		limit:      limit,
		bytesLimit: bytesLimit,
	}
}

func (c *nodeBlobCache) get(key []byte) ([]byte, bool) {
	if c == nil || len(key) == 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.cache.Get(string(key))
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	return data, true
}

func (c *nodeBlobCache) add(key []byte, data []byte) {
	if c == nil || len(key) == 0 || len(data) == 0 {
		return
	}
	k := string(key)
	size := int64(len(k) + len(data))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bytesLimit > 0 && size > c.bytesLimit {
		c.removeStringLocked(k)
		return
	}
	if old, ok := c.cache.Peek(k); ok {
		c.bytes -= int64(len(k) + len(old))
		c.cache.Remove(k)
	}
	for c.limit > 0 && c.cache.Len() >= c.limit {
		c.removeOldestLocked()
	}
	for c.bytesLimit > 0 && c.bytes+size > c.bytesLimit && c.cache.Len() > 0 {
		c.removeOldestLocked()
	}
	c.cache.Add(k, data)
	c.bytes += size
}

func (c *nodeBlobCache) remove(key []byte) {
	if c == nil || len(key) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeStringLocked(string(key))
}

func (c *nodeBlobCache) removeStringLocked(key string) {
	if c == nil {
		return
	}
	if old, ok := c.cache.Peek(key); ok {
		c.bytes -= int64(len(key) + len(old))
		c.cache.Remove(key)
	}
}

func (c *nodeBlobCache) removeOldestLocked() {
	key, value, ok := c.cache.RemoveOldest()
	if !ok {
		return
	}
	c.bytes -= int64(len(key) + len(value))
	c.evictions++
	if c.bytes < 0 {
		c.bytes = 0
	}
}

func (c *nodeBlobCache) stats() (entries int64, bytes int64) {
	diag := c.diagnostics()
	return diag.Entries, diag.Bytes
}

func (c *nodeBlobCache) diagnostics() NodeCacheDiagnostics {
	if c == nil {
		return NodeCacheDiagnostics{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return NodeCacheDiagnostics{
		Entries:   int64(c.cache.Len()),
		Bytes:     c.bytes,
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}

type commitmentPointCache struct {
	cache *lru.Cache[string, *ecmh.Point]
}

func newCommitmentPointCache(limit int) *commitmentPointCache {
	switch {
	case limit == 0:
		limit = DefaultCommitmentPointCacheLimit
	case limit < 0:
		return nil
	}
	if limit <= 0 {
		return nil
	}
	return &commitmentPointCache{cache: lru.NewCache[string, *ecmh.Point](limit)}
}

func (c *commitmentPointCache) get(commitment []byte) (*ecmh.Point, bool) {
	if c == nil || c.cache == nil || len(commitment) == 0 {
		return nil, false
	}
	return c.cache.Get(string(commitment))
}

func (c *commitmentPointCache) add(commitment []byte, point *ecmh.Point) {
	if c == nil || c.cache == nil || len(commitment) == 0 || point == nil {
		return
	}
	c.cache.Add(string(commitment), point)
}

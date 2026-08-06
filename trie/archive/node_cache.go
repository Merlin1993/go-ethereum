package archive

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/trie/archive/ecmh"
)

// DefaultNodeCacheLimit mirrors the archive trie wrapper default. A zero config
// value resolves to this default; negative values disable the cache. The first
// 10M-block path-mode replay exhausted the old 1M-entry cap while accounting
// for only about 78 MiB of serialized keys and values, so use the conservative
// 4M-entry A/B candidate while retaining the independent byte guard below.
const DefaultNodeCacheLimit = 4 * 1024 * 1024

// nodeCacheShardCount keeps unrelated shard workers off the same LRU lock.
// It is deliberately fixed so cache behavior stays comparable across runs.
const nodeCacheShardCount = 64

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

type nodeBlobCacheShard struct {
	mu               sync.Mutex
	cache            lru.BasicLRU[string, []byte]
	limit            int
	bytesLimit       int64
	bytes            int64
	hits             int64
	misses           int64
	evictions        int64
	entryEvictions   int64
	byteEvictions    int64
	oversizedRejects int64
	lockWaitNanos    int64
	lockContentions  int64
}

type nodeBlobCache struct {
	shards      []nodeBlobCacheShard
	limit       int
	bytesLimit  int64
	dbGets      atomic.Int64
	dbGetNanos  atomic.Int64
	dbLoadBytes atomic.Int64
}

// NodeCacheDiagnostics contains the current cache size and lifetime lookup
// counters. Counters are maintained under the cache's existing mutex, so they
// add no extra synchronization to the read path.
type NodeCacheDiagnostics struct {
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
	LockContentions  int64
	LockWaitNanos    int64
	DBGets           int64
	DBGetNanos       int64
	DBLoadBytes      int64
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
	shardCount := nodeCacheShardCount
	// Tiny caches are primarily used by tests and short-lived tools. Keeping
	// them as one LRU preserves exact global eviction semantics and avoids
	// dividing a small byte budget into unusably small pieces.
	if limit < nodeCacheShardCount*16 {
		shardCount = 1
	}
	cache := &nodeBlobCache{
		shards:     make([]nodeBlobCacheShard, shardCount),
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
		cache.shards[i] = nodeBlobCacheShard{
			cache:      lru.NewBasicLRU[string, []byte](entryLimit),
			limit:      entryLimit,
			bytesLimit: byteLimit,
		}
	}
	return cache
}

func nodeCacheHash(key []byte) uint64 {
	// FNV-1a is cheap and, unlike selecting a prefix byte, distributes path
	// storage keys whose leading bytes are intentionally identical.
	const (
		offset64 = uint64(14695981039346656037)
		prime64  = uint64(1099511628211)
	)
	hash := offset64
	for _, b := range key {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

func (c *nodeBlobCache) shard(key []byte) *nodeBlobCacheShard {
	return &c.shards[nodeCacheHash(key)%uint64(len(c.shards))]
}

func (s *nodeBlobCacheShard) lock() {
	if s.mu.TryLock() {
		return
	}
	start := time.Now()
	s.mu.Lock()
	s.lockWaitNanos += time.Since(start).Nanoseconds()
	s.lockContentions++
}

func (c *nodeBlobCache) get(key []byte) ([]byte, bool) {
	if c == nil || len(key) == 0 {
		return nil, false
	}
	shard := c.shard(key)
	shard.lock()
	defer shard.mu.Unlock()
	data, ok := shard.cache.Get(string(key))
	if !ok {
		shard.misses++
		return nil, false
	}
	shard.hits++
	return data, true
}

func (c *nodeBlobCache) add(key []byte, data []byte) {
	if c == nil || len(key) == 0 || len(data) == 0 {
		return
	}
	k := string(key)
	size := int64(len(k) + len(data))
	shard := c.shard(key)
	shard.lock()
	defer shard.mu.Unlock()
	if shard.bytesLimit > 0 && size > shard.bytesLimit {
		shard.removeStringLocked(k)
		shard.oversizedRejects++
		return
	}
	if old, ok := shard.cache.Peek(k); ok {
		shard.bytes -= int64(len(k) + len(old))
		shard.cache.Remove(k)
	}
	for shard.limit > 0 && shard.cache.Len() >= shard.limit {
		if shard.removeOldestLocked() {
			shard.entryEvictions++
		}
	}
	for shard.bytesLimit > 0 && shard.bytes+size > shard.bytesLimit && shard.cache.Len() > 0 {
		if shard.removeOldestLocked() {
			shard.byteEvictions++
		}
	}
	shard.cache.Add(k, data)
	shard.bytes += size
}

func (c *nodeBlobCache) remove(key []byte) {
	if c == nil || len(key) == 0 {
		return
	}
	shard := c.shard(key)
	shard.lock()
	defer shard.mu.Unlock()
	shard.removeStringLocked(string(key))
}

func (s *nodeBlobCacheShard) removeStringLocked(key string) {
	if old, ok := s.cache.Peek(key); ok {
		s.bytes -= int64(len(key) + len(old))
		s.cache.Remove(key)
	}
}

func (s *nodeBlobCacheShard) removeOldestLocked() bool {
	key, value, ok := s.cache.RemoveOldest()
	if !ok {
		return false
	}
	s.bytes -= int64(len(key) + len(value))
	s.evictions++
	if s.bytes < 0 {
		s.bytes = 0
	}
	return true
}

func (c *nodeBlobCache) recordDBGet(elapsed time.Duration, loadedBytes int) {
	if c == nil {
		return
	}
	c.dbGets.Add(1)
	c.dbGetNanos.Add(elapsed.Nanoseconds())
	if loadedBytes > 0 {
		c.dbLoadBytes.Add(int64(loadedBytes))
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
	diag := NodeCacheDiagnostics{
		EntryLimit:  int64(c.limit),
		BytesLimit:  c.bytesLimit,
		Shards:      int64(len(c.shards)),
		DBGets:      c.dbGets.Load(),
		DBGetNanos:  c.dbGetNanos.Load(),
		DBLoadBytes: c.dbLoadBytes.Load(),
	}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.lock()
		diag.Entries += int64(shard.cache.Len())
		diag.Bytes += shard.bytes
		diag.Hits += shard.hits
		diag.Misses += shard.misses
		diag.Evictions += shard.evictions
		diag.EntryEvictions += shard.entryEvictions
		diag.ByteEvictions += shard.byteEvictions
		diag.OversizedRejects += shard.oversizedRejects
		diag.LockContentions += shard.lockContentions
		diag.LockWaitNanos += shard.lockWaitNanos
		shard.mu.Unlock()
	}
	return diag
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

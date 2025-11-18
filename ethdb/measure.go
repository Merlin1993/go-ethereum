package ethdb

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// CacheConfig 定义缓存配置选项
type CacheConfig struct {
	// Enabled 控制是否启用缓存
	Enabled bool
	// Size 缓存大小，仅在Enabled为true时有效
	Size int
}

// DefaultCacheConfig 返回默认的缓存配置
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		Enabled: false,
		Size:    300000, // 100万条目
	}
}

// DisabledCacheConfig 返回禁用缓存的配置
func DisabledCacheConfig() CacheConfig {
	return CacheConfig{
		Enabled: false,
		Size:    0,
	}
}

// AccessStats 记录读/写的次数与耗时（纳秒）。
// - Read: Get
// - Write: Put/Delete/DeleteRange/Batch.Write
// 时间单位为纳秒。
type AccessStats struct {
	ReadCount  uint64
	ReadNanos  uint64
	WriteCount uint64
	WriteNanos uint64
	// 缓存统计
	CacheHits   uint64
	CacheMisses uint64
	CacheSize   uint64
}

// String 实现 fmt.Stringer，使 AccessStats 可直接打印。
func (a AccessStats) String() string {
	hitRate := float64(0)
	if a.CacheHits+a.CacheMisses > 0 {
		hitRate = float64(a.CacheHits) / float64(a.CacheHits+a.CacheMisses) * 100
	}
	return fmt.Sprintf("reads=%d (%.3fms) writes=%d (%.3fms) cache_hits=%d cache_misses=%d hit_rate=%.2f%% cache_size=%d",
		a.ReadCount, float64(a.ReadNanos)/1e6,
		a.WriteCount, float64(a.WriteNanos)/1e6,
		a.CacheHits, a.CacheMisses, hitRate, a.CacheSize,
	)
}

// StatsProvider 暴露查询统计与重置方法的接口。
// 仅由包装器提供；不会影响现有 Database/KeyValueStore 使用。
type StatsProvider interface {
	// 注意：用户已扩展为同时实现 KeyValueStore
	KeyValueStore
	// Stats 返回当前累计的读/写次数与耗时（纳秒）。
	Stats() AccessStats
	// ResetStats 重置累计的读/写耗时与计数。
	ResetStats()
}

// cacheEntry 表示缓存中的一个条目
type cacheEntry struct {
	key   string
	value []byte
	prev  *cacheEntry
	next  *cacheEntry
}

// lruCache 实现LRU缓存
type lruCache struct {
	capacity int
	cache    map[string]*cacheEntry
	head     *cacheEntry
	tail     *cacheEntry
	mu       sync.RWMutex
}

// newLRUCache 创建一个新的LRU缓存
func newLRUCache(capacity int) *lruCache {
	head := &cacheEntry{}
	tail := &cacheEntry{}
	head.next = tail
	tail.prev = head

	return &lruCache{
		capacity: capacity,
		cache:    make(map[string]*cacheEntry),
		head:     head,
		tail:     tail,
	}
}

// get 从缓存中获取值
func (c *lruCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.cache[key]; exists {
		// 移动到头部
		c.moveToHead(entry)
		// 返回值的副本以避免并发修改
		value := make([]byte, len(entry.value))
		copy(value, entry.value)
		return value, true
	}
	return nil, false
}

// put 向缓存中添加或更新值
func (c *lruCache) put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 创建值的副本
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	if entry, exists := c.cache[key]; exists {
		// 更新现有条目
		entry.value = valueCopy
		c.moveToHead(entry)
	} else {
		// 添加新条目
		entry := &cacheEntry{
			key:   key,
			value: valueCopy,
		}

		c.cache[key] = entry
		c.addToHead(entry)

		// 检查是否超过容量
		if len(c.cache) > c.capacity {
			tail := c.removeTail()
			delete(c.cache, tail.key)
		}
	}
}

// delete 从缓存中删除条目
func (c *lruCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.cache[key]; exists {
		c.removeEntry(entry)
		delete(c.cache, key)
	}
}

// size 返回缓存当前大小
func (c *lruCache) size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// clear 清空缓存
func (c *lruCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*cacheEntry)
	c.head.next = c.tail
	c.tail.prev = c.head
}

// 内部方法：移动条目到头部
func (c *lruCache) moveToHead(entry *cacheEntry) {
	c.removeEntry(entry)
	c.addToHead(entry)
}

// 内部方法：添加条目到头部
func (c *lruCache) addToHead(entry *cacheEntry) {
	entry.prev = c.head
	entry.next = c.head.next
	c.head.next.prev = entry
	c.head.next = entry
}

// 内部方法：移除条目
func (c *lruCache) removeEntry(entry *cacheEntry) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev
}

// 内部方法：移除尾部条目
func (c *lruCache) removeTail() *cacheEntry {
	last := c.tail.prev
	c.removeEntry(last)
	return last
}

// measuredKV 是对 KeyValueStore 的包装器，拦截 Get/Put/Delete/DeleteRange/Batches 做次数与耗时统计。
// 其它方法均透传到底层 store。
type measuredKV struct {
	underlying KeyValueStore
	cache      *lruCache

	// 计数与耗时（纳秒），使用原子类型保证并发安全
	readCount   atomic.Uint64
	readNanos   atomic.Uint64
	writeCount  atomic.Uint64
	writeNanos  atomic.Uint64
	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64
}

// WrapWithStats 用统计代理包装一个 KeyValueStore，使用默认缓存配置。
// 返回值实现了 KeyValueStore 与 StatsProvider 接口。
func WrapWithStats(store KeyValueStore) StatsProvider {
	return WrapWithStatsAndConfig(store, DefaultCacheConfig())
}

// WrapWithStatsAndCache 用统计代理和指定大小的缓存包装一个 KeyValueStore。
// 为了向后兼容保留此函数。
func WrapWithStatsAndCache(store KeyValueStore, cacheSize int) StatsProvider {
	config := CacheConfig{
		Enabled: cacheSize > 0,
		Size:    cacheSize,
	}
	return WrapWithStatsAndConfig(store, config)
}

// WrapWithStatsAndConfig 用统计代理和指定配置包装一个 KeyValueStore。
// 这是最灵活的包装函数，允许完全控制缓存行为。
func WrapWithStatsAndConfig(store KeyValueStore, config CacheConfig) StatsProvider {
	var cache *lruCache
	if config.Enabled && config.Size > 0 {
		cache = newLRUCache(config.Size)
	}

	return &measuredKV{
		underlying: store,
		cache:      cache,
	}
}

// 确保 measuredKV 同时实现 StatsProvider
var _ StatsProvider = (*measuredKV)(nil)

// --- KeyValueReader ---
// Has 视为"查询存在性"，根据需求不计入读/写统计，仅透传。
func (m *measuredKV) Has(key []byte) (bool, error) {
	return m.underlying.Has(key)
}

func (m *measuredKV) Get(key []byte) ([]byte, error) {
	// 如果缓存启用，先查缓存
	if m.cache != nil {
		keyStr := string(key)
		if value, found := m.cache.get(keyStr); found {
			m.cacheHits.Add(1)
			m.readCount.Add(1)
			return value, nil
		}
		// 缓存未命中
		m.cacheMisses.Add(1)
	}

	// 查询底层存储
	start := time.Now()
	val, err := m.underlying.Get(key)
	elapsed := time.Since(start)
	m.readCount.Add(1)
	m.readNanos.Add(uint64(elapsed))

	// 如果缓存启用且查询成功，添加到缓存
	if m.cache != nil && err == nil && val != nil {
		keyStr := string(key)
		m.cache.put(keyStr, val)
	}

	return val, err
}

// --- KeyValueWriter ---
func (m *measuredKV) Put(key []byte, value []byte) error {
	start := time.Now()
	err := m.underlying.Put(key, value)
	elapsed := time.Since(start)
	m.writeCount.Add(1)
	m.writeNanos.Add(uint64(elapsed))

	// 如果缓存启用且写入成功，更新缓存
	if m.cache != nil && err == nil {
		m.cache.put(string(key), value)
	}

	return err
}

func (m *measuredKV) Delete(key []byte) error {
	start := time.Now()
	err := m.underlying.Delete(key)
	elapsed := time.Since(start)
	m.writeCount.Add(1)
	m.writeNanos.Add(uint64(elapsed))

	// 如果缓存启用且删除成功，从缓存中移除
	if m.cache != nil && err == nil {
		m.cache.delete(string(key))
	}

	return err
}

// --- KeyValueRangeDeleter ---
func (m *measuredKV) DeleteRange(start, end []byte) error {
	begin := time.Now()
	err := m.underlying.DeleteRange(start, end)
	elapsed := time.Since(begin)
	m.writeCount.Add(1)
	m.writeNanos.Add(uint64(elapsed))

	// 如果缓存启用且范围删除成功，清空整个缓存
	// 范围删除会影响缓存，为了简单起见，清空整个缓存
	// 在实际应用中，可以考虑更精细的缓存失效策略
	if m.cache != nil && err == nil {
		m.cache.clear()
	}

	return err
}

// --- KeyValueStater ---
func (m *measuredKV) Stat() (string, error) { return m.underlying.Stat() }

// --- Batcher ---
func (m *measuredKV) NewBatch() Batch { return measuredBatch{Batch: m.underlying.NewBatch(), owner: m} }
func (m *measuredKV) NewBatchWithSize(size int) Batch {
	return measuredBatch{Batch: m.underlying.NewBatchWithSize(size), owner: m}
}

// measuredBatch 包装 Batch：
// - 在 Put/Delete 增加写操作计数（不计时）
// - 在 Write 统计一次写操作并计入耗时
// - 其它方法透传
// 注意：Batch 本身是不可并发复用的，但计数聚合到 measuredKV 的原子变量是并发安全的。
type measuredBatch struct {
	Batch
	owner *measuredKV
}

func (b measuredBatch) Put(key []byte, value []byte) error {
	b.owner.writeCount.Add(1)
	return b.Batch.Put(key, value)
}

func (b measuredBatch) Delete(key []byte) error {
	b.owner.writeCount.Add(1)
	return b.Batch.Delete(key)
}

func (b measuredBatch) Write() error {
	start := time.Now()
	err := b.Batch.Write()
	elapsed := time.Since(start)
	b.owner.writeNanos.Add(uint64(elapsed))

	// 如果缓存启用且批量写入成功，清空缓存
	// 批量写入后，为了简单起见，清空缓存
	// 在实际应用中，可以考虑更精细的缓存更新策略
	if b.owner.cache != nil && err == nil {
		b.owner.cache.clear()
	}

	return err
}

// 其它 Batch 方法透传（ValueSize/Reset/Replay 已在内嵌 Batch 上提供）。

// --- Iteratee ---
func (m *measuredKV) NewIterator(prefix []byte, start []byte) Iterator {
	return m.underlying.NewIterator(prefix, start)
}

// --- Compacter ---
func (m *measuredKV) Compact(start []byte, limit []byte) error {
	return m.underlying.Compact(start, limit)
}

// --- io.Closer ---
func (m *measuredKV) Close() error {
	// 如果缓存启用，清空缓存
	if m.cache != nil {
		m.cache.clear()
	}

	if c, ok := m.underlying.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// --- StatsProvider ---
func (m *measuredKV) Stats() AccessStats {
	var cacheSize uint64
	if m.cache != nil {
		cacheSize = uint64(m.cache.size())
	}

	return AccessStats{
		ReadCount:   m.readCount.Load(),
		ReadNanos:   m.readNanos.Load(),
		WriteCount:  m.writeCount.Load(),
		WriteNanos:  m.writeNanos.Load(),
		CacheHits:   m.cacheHits.Load(),
		CacheMisses: m.cacheMisses.Load(),
		CacheSize:   cacheSize,
	}
}

func (m *measuredKV) ResetStats() {
	m.readCount.Store(0)
	m.readNanos.Store(0)
	m.writeCount.Store(0)
	m.writeNanos.Store(0)
	m.cacheHits.Store(0)
	m.cacheMisses.Store(0)
}

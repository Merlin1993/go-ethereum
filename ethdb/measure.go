package ethdb

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// AccessStats 记录读/写的次数与耗时（纳秒）。
// - Read: Get
// - Write: Put/Delete/DeleteRange/Batch.Write
// 时间单位为纳秒。
type AccessStats struct {
	ReadCount  uint64
	ReadNanos  uint64
	WriteCount uint64
	WriteNanos uint64
}

// String 实现 fmt.Stringer，使 AccessStats 可直接打印。
func (a AccessStats) String() string {
	return fmt.Sprintf("reads=%d (%.3fms) writes=%d (%.3fms)",
		a.ReadCount, float64(a.ReadNanos)/1e6,
		a.WriteCount, float64(a.WriteNanos)/1e6,
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

// measuredKV 是对 KeyValueStore 的包装器，拦截 Get/Put/Delete/DeleteRange/Batches 做次数与耗时统计。
// 其它方法均透传到底层 store。
type measuredKV struct {
	underlying KeyValueStore

	// 计数与耗时（纳秒），使用原子类型保证并发安全
	readCount  atomic.Uint64
	readNanos  atomic.Uint64
	writeCount atomic.Uint64
	writeNanos atomic.Uint64
}

// WrapWithStats 用统计代理包装一个 KeyValueStore。
// 返回值实现了 KeyValueStore 与 StatsProvider 接口。
func WrapWithStats(store KeyValueStore) StatsProvider {
	return &measuredKV{underlying: store}
}

// 确保 measuredKV 同时实现 StatsProvider
var _ StatsProvider = (*measuredKV)(nil)

// --- KeyValueReader ---
// Has 视为“查询存在性”，根据需求不计入读/写统计，仅透传。
func (m *measuredKV) Has(key []byte) (bool, error) {
	return m.underlying.Has(key)
}

func (m *measuredKV) Get(key []byte) ([]byte, error) {
	start := time.Now()
	val, err := m.underlying.Get(key)
	elapsed := time.Since(start)
	m.readCount.Add(1)
	m.readNanos.Add(uint64(elapsed))
	return val, err
}

// --- KeyValueWriter ---
func (m *measuredKV) Put(key []byte, value []byte) error {
	start := time.Now()
	err := m.underlying.Put(key, value)
	elapsed := time.Since(start)
	m.writeCount.Add(1)
	m.writeNanos.Add(uint64(elapsed))
	return err
}

func (m *measuredKV) Delete(key []byte) error {
	start := time.Now()
	err := m.underlying.Delete(key)
	elapsed := time.Since(start)
	m.writeCount.Add(1)
	m.writeNanos.Add(uint64(elapsed))
	return err
}

// --- KeyValueRangeDeleter ---
func (m *measuredKV) DeleteRange(start, end []byte) error {
	begin := time.Now()
	err := m.underlying.DeleteRange(start, end)
	elapsed := time.Since(begin)
	m.writeCount.Add(1)
	m.writeNanos.Add(uint64(elapsed))
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
	if c, ok := m.underlying.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// --- StatsProvider ---
func (m *measuredKV) Stats() AccessStats {
	return AccessStats{
		ReadCount:  m.readCount.Load(),
		ReadNanos:  m.readNanos.Load(),
		WriteCount: m.writeCount.Load(),
		WriteNanos: m.writeNanos.Load(),
	}
}

func (m *measuredKV) ResetStats() {
	m.readCount.Store(0)
	m.readNanos.Store(0)
	m.writeCount.Store(0)
	m.writeNanos.Store(0)
}

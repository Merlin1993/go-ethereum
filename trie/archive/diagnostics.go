package archive

import (
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// CommitDiagnostics is a snapshot of the most recent ASCT commit breakdown.
type CommitDiagnostics struct {
	TotalNanos         int64
	CommitToBatchNanos int64
	ShardCommitNanos   int64
	RootHashNanos      int64
	BatchWriteNanos    int64
	AdapterMergeNanos  int64
	TrieDBUpdateNanos  int64
	PruneTotalNanos    int64
	PruneWaitNanos     int64
	PruneShardNanos    int64
	PrunePrefetchNanos int64

	DirtyShards int64

	NodeCacheHits              int64
	NodeCacheMisses            int64
	PathNodeDBGets             int64
	ArchivePromotionChecks     int64
	ArchivePromotionHits       int64
	BucketRecomputes           int64
	CommitmentPointCacheHits   int64
	CommitmentPointCacheMisses int64
	PruneInternalVisits        int64
	PruneHotSkips              int64
	PruneChildHits             int64
	PruneChildSkips            int64
	PruneBulkCollects          int64
	PruneCollectedLeaves       int64
	PruneCollectedStubs        int64
	PruneBuildItems            int64
	PruneBuildBuckets          int64
	PruneArchiveBuildParallels int64
	PrunePathAbsorbedItems     int64
	PruneRootPoolItems         int64
	PruneRootPoolBuckets       int64

	ShardMaxID                int64
	ShardMaxCommitNanos       int64
	ShardMaxLockWaitNanos     int64
	ShardMaxRootCommitNanos   int64
	ShardMaxStaleDeleteNanos  int64
	ShardMaxPendingValueNanos int64
	ShardMaxNodeCount         int64
	ShardMaxStaleSetLen       int64
	ShardMaxPendingValues     int64
	ShardMaxPendingFlatValues int64
	ShardMaxSerializeNanos    int64
	ShardMaxHashNanos         int64
	ShardMaxPersistNanos      int64
	ShardMaxBatchPutNanos     int64
	ShardMaxCacheNanos        int64
	ShardMaxBookkeepNanos     int64
	ShardMaxNodeNanos         int64
	ShardMaxNodeType          int64
	ShardMaxNodeBytes         int64

	RawBatchOps              int64
	RawBatchBytes            int64
	RawShardMaxID            int64
	RawShardMaxOps           int64
	RawShardMaxBytes         int64
	NodeCacheEntries         int64
	NodeCacheBytes           int64
	NodeCacheEntryLimit      int64
	NodeCacheBytesLimit      int64
	NodeCacheShards          int64
	NodeCacheTotalHits       int64
	NodeCacheTotalMisses     int64
	NodeCacheEvictions       int64
	NodeCacheLockContentions int64
	NodeCacheLockWaitNanos   int64
	NodeCacheDBGets          int64
	NodeCacheDBGetNanos      int64
	NodeCacheDBLoadBytes     int64
	RuntimeHeapAlloc         int64
	RuntimeHeapSys           int64
	RuntimeHeapInuse         int64
	RuntimeSys               int64
	RuntimeNumGC             int64
	RuntimePauseTotal        int64
	RuntimeLastPauseNs       int64
}

// HashDiagnostics describes the root calculation performed by the latest
// Trie.Hash call. ShardWorkNanos is the sum of all shard work, while
// ShardWallNanos is the elapsed wall time after running those shards in
// parallel.
type HashDiagnostics struct {
	TotalNanos     int64
	ShardWallNanos int64
	ShardWorkNanos int64
	RootNanos      int64
	DirtyShards    int64
	Workers        int64
	MaxShardID     int64
	MaxShardNanos  int64
	NodeCount      int64
	DirtyNodes     int64
	CleanNodes     int64
	SerializeNanos int64
	HashNanos      int64
}

func (d HashDiagnostics) String() string {
	return fmt.Sprintf(
		"hashTotal=%v hashShardWall=%v hashShardWork=%v hashRootMerge=%v hashDirtyShards=%d hashWorkers=%d hashNodes=%d hashDirtyNodes=%d hashCleanNodes=%d hashSerialize=%v hashCompute=%v hashMaxShardID=%d hashMaxShard=%v",
		time.Duration(d.TotalNanos),
		time.Duration(d.ShardWallNanos),
		time.Duration(d.ShardWorkNanos),
		time.Duration(d.RootNanos),
		d.DirtyShards,
		d.Workers,
		d.NodeCount,
		d.DirtyNodes,
		d.CleanNodes,
		time.Duration(d.SerializeNanos),
		time.Duration(d.HashNanos),
		d.MaxShardID,
		time.Duration(d.MaxShardNanos),
	)
}

// PrunePressureDiagnostics captures the pressure of the most recent pruned
// shard and the heaviest shard seen since the last reset. The detailed count
// fields are populated when path diagnostics are enabled.
type PrunePressureDiagnostics struct {
	LastShardID             int64
	LastTotalNanos          int64
	LastWaitNanos           int64
	LastShardNanos          int64
	LastPrefetchNanos       int64
	LastLockWaitNanos       int64
	LastRootLoadNanos       int64
	LastWalkNanos           int64
	LastFinishNanos         int64
	DetailedCountersEnabled bool
	LastInternalVisits      int64
	LastHotSkips            int64
	LastChildHits           int64
	LastChildSkips          int64
	LastBulkCollects        int64
	LastLeaves              int64
	LastStubs               int64
	LastBuildItems          int64
	LastBuildBuckets        int64
	LastParallelBuilds      int64
	LastPathAbsorbed        int64
	LastRootPoolItems       int64
	LastRootPoolBuckets     int64

	MaxShardID                 int64
	MaxTotalNanos              int64
	MaxWaitNanos               int64
	MaxShardNanos              int64
	MaxPrefetchNanos           int64
	MaxLockWaitNanos           int64
	MaxRootLoadNanos           int64
	MaxWalkNanos               int64
	MaxFinishNanos             int64
	MaxDetailedCountersEnabled bool
	MaxInternalVisits          int64
	MaxHotSkips                int64
	MaxChildHits               int64
	MaxChildSkips              int64
	MaxBulkCollects            int64
	MaxLeaves                  int64
	MaxStubs                   int64
	MaxBuildItems              int64
	MaxBuildBuckets            int64
	MaxParallelBuilds          int64
	MaxPathAbsorbed            int64
	MaxRootPoolItems           int64
	MaxRootPoolBuckets         int64
}

// ShardPruneDiagnostics separates time spent waiting for the shard from the
// actual load, tree walk, and root finishing work.
type ShardPruneDiagnostics struct {
	TotalNanos              int64
	LockWaitNanos           int64
	RootLoadNanos           int64
	WalkNanos               int64
	FinishNanos             int64
	DetailedCountersEnabled bool
}

// ArchiveCumulativeDiagnostics captures run-wide counters that are cheap enough
// to keep enabled during hot replay runs.
type ArchiveCumulativeDiagnostics struct {
	ArchivedLeaves    int64
	FlatValuePuts     int64
	FlatValueDeletes  int64
	FlatValuePutBytes int64
}

// UpdateDiagnostics contains cumulative counters for the hot state-update path.
// Callers take two snapshots and subtract them to obtain one replay window.
type UpdateDiagnostics struct {
	StemPutCalls            int64
	StemPutNoops            int64
	StemPutLoadedBytes      int64
	StemPutEncodedBytes     int64
	StemPutCommitmentHashes int64
	StemPutTotalNanos       int64
	StemPutLoadNanos        int64
	StemPutDecodeNanos      int64
	StemPutEncodeNanos      int64
	StemPutBackendNanos     int64

	StemApplyCalls        int64
	StemApplyUpdates      int64
	StemApplyStems        int64
	StemApplyPuts         int64
	StemApplyDeletes      int64
	StemApplyTotalNanos   int64
	StemApplyLoadNanos    int64
	StemApplyEncodeNanos  int64
	StemApplyBackendNanos int64

	ShardPutCalls          int64
	ShardPutValues         int64
	ShardPutNanos          int64
	ShardPutBatchCalls     int64
	ShardPutBatchValues    int64
	ShardPutBatchShards    int64
	ShardPutBatchWallNanos int64
	ShardPutBatchWorkNanos int64
	ShardDeleteCalls       int64
	ShardDeleteNanos       int64

	FlatValueGets          int64
	FlatValueReadIONanos   int64
	FlatValueReadIOBytes   int64
	FlatValuePuts          int64
	FlatValueDeletes       int64
	FlatValueWriteNanos    int64
	ArchivePromotionChecks int64
	ArchivePromotionHits   int64
}

const latencyHistogramBuckets = 512

// LatencyHistogram is a cumulative, low-overhead log histogram with eight
// buckets per power of two. Percentiles are returned as bucket upper bounds.
type LatencyHistogram struct {
	Buckets [latencyHistogramBuckets]int64
	Count   int64
}

func (h LatencyHistogram) Sub(previous LatencyHistogram) LatencyHistogram {
	var out LatencyHistogram
	out.Count = h.Count - previous.Count
	for i := range out.Buckets {
		out.Buckets[i] = h.Buckets[i] - previous.Buckets[i]
	}
	return out
}

func (h LatencyHistogram) Percentile(percent int) time.Duration {
	if h.Count <= 0 || percent <= 0 {
		return 0
	}
	if percent > 100 {
		percent = 100
	}
	rank := (h.Count*int64(percent) + 99) / 100
	var seen int64
	for i, count := range h.Buckets {
		seen += count
		if seen >= rank {
			return time.Duration(latencyBucketUpperBound(i))
		}
	}
	return time.Duration(latencyBucketUpperBound(latencyHistogramBuckets - 1))
}

func (d UpdateDiagnostics) Sub(previous UpdateDiagnostics) UpdateDiagnostics {
	return UpdateDiagnostics{
		StemPutCalls:            d.StemPutCalls - previous.StemPutCalls,
		StemPutNoops:            d.StemPutNoops - previous.StemPutNoops,
		StemPutLoadedBytes:      d.StemPutLoadedBytes - previous.StemPutLoadedBytes,
		StemPutEncodedBytes:     d.StemPutEncodedBytes - previous.StemPutEncodedBytes,
		StemPutCommitmentHashes: d.StemPutCommitmentHashes - previous.StemPutCommitmentHashes,
		StemPutTotalNanos:       d.StemPutTotalNanos - previous.StemPutTotalNanos,
		StemPutLoadNanos:        d.StemPutLoadNanos - previous.StemPutLoadNanos,
		StemPutDecodeNanos:      d.StemPutDecodeNanos - previous.StemPutDecodeNanos,
		StemPutEncodeNanos:      d.StemPutEncodeNanos - previous.StemPutEncodeNanos,
		StemPutBackendNanos:     d.StemPutBackendNanos - previous.StemPutBackendNanos,
		StemApplyCalls:          d.StemApplyCalls - previous.StemApplyCalls,
		StemApplyUpdates:        d.StemApplyUpdates - previous.StemApplyUpdates,
		StemApplyStems:          d.StemApplyStems - previous.StemApplyStems,
		StemApplyPuts:           d.StemApplyPuts - previous.StemApplyPuts,
		StemApplyDeletes:        d.StemApplyDeletes - previous.StemApplyDeletes,
		StemApplyTotalNanos:     d.StemApplyTotalNanos - previous.StemApplyTotalNanos,
		StemApplyLoadNanos:      d.StemApplyLoadNanos - previous.StemApplyLoadNanos,
		StemApplyEncodeNanos:    d.StemApplyEncodeNanos - previous.StemApplyEncodeNanos,
		StemApplyBackendNanos:   d.StemApplyBackendNanos - previous.StemApplyBackendNanos,
		ShardPutCalls:           d.ShardPutCalls - previous.ShardPutCalls,
		ShardPutValues:          d.ShardPutValues - previous.ShardPutValues,
		ShardPutNanos:           d.ShardPutNanos - previous.ShardPutNanos,
		ShardPutBatchCalls:      d.ShardPutBatchCalls - previous.ShardPutBatchCalls,
		ShardPutBatchValues:     d.ShardPutBatchValues - previous.ShardPutBatchValues,
		ShardPutBatchShards:     d.ShardPutBatchShards - previous.ShardPutBatchShards,
		ShardPutBatchWallNanos:  d.ShardPutBatchWallNanos - previous.ShardPutBatchWallNanos,
		ShardPutBatchWorkNanos:  d.ShardPutBatchWorkNanos - previous.ShardPutBatchWorkNanos,
		ShardDeleteCalls:        d.ShardDeleteCalls - previous.ShardDeleteCalls,
		ShardDeleteNanos:        d.ShardDeleteNanos - previous.ShardDeleteNanos,
		FlatValueGets:           d.FlatValueGets - previous.FlatValueGets,
		FlatValueReadIONanos:    d.FlatValueReadIONanos - previous.FlatValueReadIONanos,
		FlatValueReadIOBytes:    d.FlatValueReadIOBytes - previous.FlatValueReadIOBytes,
		FlatValuePuts:           d.FlatValuePuts - previous.FlatValuePuts,
		FlatValueDeletes:        d.FlatValueDeletes - previous.FlatValueDeletes,
		FlatValueWriteNanos:     d.FlatValueWriteNanos - previous.FlatValueWriteNanos,
		ArchivePromotionChecks:  d.ArchivePromotionChecks - previous.ArchivePromotionChecks,
		ArchivePromotionHits:    d.ArchivePromotionHits - previous.ArchivePromotionHits,
	}
}

func (d PrunePressureDiagnostics) String() string {
	return fmt.Sprintf(
		"pruneLastShardID=%d pruneLastTotal=%v pruneLastShard=%v pruneLastLockWait=%v pruneLastRootLoad=%v pruneLastWalk=%v pruneLastFinish=%v pruneDetailedCounters=%v pruneLastLeaves=%d pruneLastStubs=%d pruneLastBuildItems=%d pruneLastBuildBuckets=%d pruneLastPathAbsorbed=%d pruneLastRootPoolItems=%d pruneLastRootPoolBuckets=%d pruneLastInternalVisits=%d pruneMaxShardID=%d pruneMaxTotal=%v pruneMaxShard=%v pruneMaxLockWait=%v pruneMaxRootLoad=%v pruneMaxWalk=%v pruneMaxFinish=%v pruneMaxDetailedCounters=%v pruneMaxLeaves=%d pruneMaxStubs=%d pruneMaxBuildItems=%d pruneMaxBuildBuckets=%d pruneMaxPathAbsorbed=%d pruneMaxRootPoolItems=%d pruneMaxRootPoolBuckets=%d pruneMaxInternalVisits=%d",
		d.LastShardID,
		time.Duration(d.LastTotalNanos),
		time.Duration(d.LastShardNanos),
		time.Duration(d.LastLockWaitNanos),
		time.Duration(d.LastRootLoadNanos),
		time.Duration(d.LastWalkNanos),
		time.Duration(d.LastFinishNanos),
		d.DetailedCountersEnabled,
		d.LastLeaves,
		d.LastStubs,
		d.LastBuildItems,
		d.LastBuildBuckets,
		d.LastPathAbsorbed,
		d.LastRootPoolItems,
		d.LastRootPoolBuckets,
		d.LastInternalVisits,
		d.MaxShardID,
		time.Duration(d.MaxTotalNanos),
		time.Duration(d.MaxShardNanos),
		time.Duration(d.MaxLockWaitNanos),
		time.Duration(d.MaxRootLoadNanos),
		time.Duration(d.MaxWalkNanos),
		time.Duration(d.MaxFinishNanos),
		d.MaxDetailedCountersEnabled,
		d.MaxLeaves,
		d.MaxStubs,
		d.MaxBuildItems,
		d.MaxBuildBuckets,
		d.MaxPathAbsorbed,
		d.MaxRootPoolItems,
		d.MaxRootPoolBuckets,
		d.MaxInternalVisits,
	)
}

var (
	archiveCumulativeArchivedLeaves    int64
	archiveCumulativeFlatValuePuts     int64
	archiveCumulativeFlatValueDeletes  int64
	archiveCumulativeFlatValuePutBytes int64
	updateStemPutCalls                 int64
	updateStemPutNoops                 int64
	updateStemPutLoadedBytes           int64
	updateStemPutEncodedBytes          int64
	updateStemPutCommitmentHashes      int64
	updateStemPutTotalNanos            int64
	updateStemPutLoadNanos             int64
	updateStemPutDecodeNanos           int64
	updateStemPutEncodeNanos           int64
	updateStemPutBackendNanos          int64
	updateStemApplyCalls               int64
	updateStemApplyUpdates             int64
	updateStemApplyStems               int64
	updateStemApplyPuts                int64
	updateStemApplyDeletes             int64
	updateStemApplyTotalNanos          int64
	updateStemApplyLoadNanos           int64
	updateStemApplyEncodeNanos         int64
	updateStemApplyBackendNanos        int64
	updateShardPutCalls                int64
	updateShardPutValues               int64
	updateShardPutNanos                int64
	updateShardPutBatchCalls           int64
	updateShardPutBatchValues          int64
	updateShardPutBatchShards          int64
	updateShardPutBatchWallNanos       int64
	updateShardPutBatchWorkNanos       int64
	updateShardDeleteCalls             int64
	updateShardDeleteNanos             int64
	updateFlatValueGets                int64
	updateFlatValueReadIONanos         int64
	updateFlatValueReadIOBytes         int64
	updateFlatValuePuts                int64
	updateFlatValueDeletes             int64
	updateFlatValueWriteNanos          int64
	updateArchivePromotionChecks       int64
	updateArchivePromotionHits         int64
	updateStemPutLatencyBuckets        [latencyHistogramBuckets]int64
	updateStemPutLatencyCount          int64
	updateStemApplyLatencyBuckets      [latencyHistogramBuckets]int64
	updateStemApplyLatencyCount        int64

	hashDiagnosticsMu sync.Mutex
	hashDiagnostics   HashDiagnostics
)

func LastUpdateDiagnostics() UpdateDiagnostics {
	return UpdateDiagnostics{
		StemPutCalls:            atomic.LoadInt64(&updateStemPutCalls),
		StemPutNoops:            atomic.LoadInt64(&updateStemPutNoops),
		StemPutLoadedBytes:      atomic.LoadInt64(&updateStemPutLoadedBytes),
		StemPutEncodedBytes:     atomic.LoadInt64(&updateStemPutEncodedBytes),
		StemPutCommitmentHashes: atomic.LoadInt64(&updateStemPutCommitmentHashes),
		StemPutTotalNanos:       atomic.LoadInt64(&updateStemPutTotalNanos),
		StemPutLoadNanos:        atomic.LoadInt64(&updateStemPutLoadNanos),
		StemPutDecodeNanos:      atomic.LoadInt64(&updateStemPutDecodeNanos),
		StemPutEncodeNanos:      atomic.LoadInt64(&updateStemPutEncodeNanos),
		StemPutBackendNanos:     atomic.LoadInt64(&updateStemPutBackendNanos),
		StemApplyCalls:          atomic.LoadInt64(&updateStemApplyCalls),
		StemApplyUpdates:        atomic.LoadInt64(&updateStemApplyUpdates),
		StemApplyStems:          atomic.LoadInt64(&updateStemApplyStems),
		StemApplyPuts:           atomic.LoadInt64(&updateStemApplyPuts),
		StemApplyDeletes:        atomic.LoadInt64(&updateStemApplyDeletes),
		StemApplyTotalNanos:     atomic.LoadInt64(&updateStemApplyTotalNanos),
		StemApplyLoadNanos:      atomic.LoadInt64(&updateStemApplyLoadNanos),
		StemApplyEncodeNanos:    atomic.LoadInt64(&updateStemApplyEncodeNanos),
		StemApplyBackendNanos:   atomic.LoadInt64(&updateStemApplyBackendNanos),
		ShardPutCalls:           atomic.LoadInt64(&updateShardPutCalls),
		ShardPutValues:          atomic.LoadInt64(&updateShardPutValues),
		ShardPutNanos:           atomic.LoadInt64(&updateShardPutNanos),
		ShardPutBatchCalls:      atomic.LoadInt64(&updateShardPutBatchCalls),
		ShardPutBatchValues:     atomic.LoadInt64(&updateShardPutBatchValues),
		ShardPutBatchShards:     atomic.LoadInt64(&updateShardPutBatchShards),
		ShardPutBatchWallNanos:  atomic.LoadInt64(&updateShardPutBatchWallNanos),
		ShardPutBatchWorkNanos:  atomic.LoadInt64(&updateShardPutBatchWorkNanos),
		ShardDeleteCalls:        atomic.LoadInt64(&updateShardDeleteCalls),
		ShardDeleteNanos:        atomic.LoadInt64(&updateShardDeleteNanos),
		FlatValueGets:           atomic.LoadInt64(&updateFlatValueGets),
		FlatValueReadIONanos:    atomic.LoadInt64(&updateFlatValueReadIONanos),
		FlatValueReadIOBytes:    atomic.LoadInt64(&updateFlatValueReadIOBytes),
		FlatValuePuts:           atomic.LoadInt64(&updateFlatValuePuts),
		FlatValueDeletes:        atomic.LoadInt64(&updateFlatValueDeletes),
		FlatValueWriteNanos:     atomic.LoadInt64(&updateFlatValueWriteNanos),
		ArchivePromotionChecks:  atomic.LoadInt64(&updateArchivePromotionChecks),
		ArchivePromotionHits:    atomic.LoadInt64(&updateArchivePromotionHits),
	}
}

func LastStemPutLatencyHistogram() LatencyHistogram {
	return snapshotLatencyHistogram(&updateStemPutLatencyBuckets, &updateStemPutLatencyCount)
}

func LastStemApplyLatencyHistogram() LatencyHistogram {
	return snapshotLatencyHistogram(&updateStemApplyLatencyBuckets, &updateStemApplyLatencyCount)
}

func recordStemPutDiagnostics(noop bool, loadedBytes, encodedBytes, commitmentHashes int, total, load, decode, encode, backend time.Duration) {
	atomic.AddInt64(&updateStemPutCalls, 1)
	if noop {
		atomic.AddInt64(&updateStemPutNoops, 1)
	}
	atomic.AddInt64(&updateStemPutLoadedBytes, int64(loadedBytes))
	atomic.AddInt64(&updateStemPutEncodedBytes, int64(encodedBytes))
	atomic.AddInt64(&updateStemPutCommitmentHashes, int64(commitmentHashes))
	atomic.AddInt64(&updateStemPutTotalNanos, total.Nanoseconds())
	atomic.AddInt64(&updateStemPutLoadNanos, load.Nanoseconds())
	atomic.AddInt64(&updateStemPutDecodeNanos, decode.Nanoseconds())
	atomic.AddInt64(&updateStemPutEncodeNanos, encode.Nanoseconds())
	atomic.AddInt64(&updateStemPutBackendNanos, backend.Nanoseconds())
	recordLatency(&updateStemPutLatencyBuckets, &updateStemPutLatencyCount, total)
}

func recordStemApplyDiagnostics(updates, stems, puts, deletes int, total, load, encode, backend time.Duration) {
	atomic.AddInt64(&updateStemApplyCalls, 1)
	atomic.AddInt64(&updateStemApplyUpdates, int64(updates))
	atomic.AddInt64(&updateStemApplyStems, int64(stems))
	atomic.AddInt64(&updateStemApplyPuts, int64(puts))
	atomic.AddInt64(&updateStemApplyDeletes, int64(deletes))
	atomic.AddInt64(&updateStemApplyTotalNanos, total.Nanoseconds())
	atomic.AddInt64(&updateStemApplyLoadNanos, load.Nanoseconds())
	atomic.AddInt64(&updateStemApplyEncodeNanos, encode.Nanoseconds())
	atomic.AddInt64(&updateStemApplyBackendNanos, backend.Nanoseconds())
	recordLatency(&updateStemApplyLatencyBuckets, &updateStemApplyLatencyCount, total)
}

func snapshotLatencyHistogram(buckets *[latencyHistogramBuckets]int64, count *int64) LatencyHistogram {
	var out LatencyHistogram
	out.Count = atomic.LoadInt64(count)
	for i := range out.Buckets {
		out.Buckets[i] = atomic.LoadInt64(&buckets[i])
	}
	return out
}

func recordLatency(buckets *[latencyHistogramBuckets]int64, count *int64, elapsed time.Duration) {
	index := latencyBucketIndex(elapsed.Nanoseconds())
	atomic.AddInt64(&buckets[index], 1)
	atomic.AddInt64(count, 1)
}

func latencyBucketIndex(nanos int64) int {
	if nanos <= 1 {
		return 0
	}
	value := uint64(nanos)
	exponent := bits.Len64(value) - 1
	base := uint64(1) << exponent
	sub := int((value - base) * 8 / base)
	if sub > 7 {
		sub = 7
	}
	index := exponent*8 + sub
	if index >= latencyHistogramBuckets {
		return latencyHistogramBuckets - 1
	}
	return index
}

func latencyBucketUpperBound(index int) int64 {
	if index <= 0 {
		return 1
	}
	exponent := index / 8
	sub := index % 8
	if exponent >= 62 {
		return int64(^uint64(0) >> 1)
	}
	base := uint64(1) << exponent
	upper := base + base*uint64(sub+1)/8
	if upper > uint64(^uint64(0)>>1) {
		return int64(^uint64(0) >> 1)
	}
	return int64(upper)
}

func recordShardPutDiagnostics(values int, elapsed time.Duration) {
	atomic.AddInt64(&updateShardPutCalls, 1)
	atomic.AddInt64(&updateShardPutValues, int64(values))
	atomic.AddInt64(&updateShardPutNanos, elapsed.Nanoseconds())
}

func recordShardPutBatchDiagnostics(values, shards int, wall, work time.Duration) {
	atomic.AddInt64(&updateShardPutBatchCalls, 1)
	atomic.AddInt64(&updateShardPutBatchValues, int64(values))
	atomic.AddInt64(&updateShardPutBatchShards, int64(shards))
	atomic.AddInt64(&updateShardPutBatchWallNanos, wall.Nanoseconds())
	atomic.AddInt64(&updateShardPutBatchWorkNanos, work.Nanoseconds())
}

func recordShardDeleteDiagnostics(elapsed time.Duration) {
	atomic.AddInt64(&updateShardDeleteCalls, 1)
	atomic.AddInt64(&updateShardDeleteNanos, elapsed.Nanoseconds())
}

func recordFlatValueGet() { atomic.AddInt64(&updateFlatValueGets, 1) }

func recordFlatValueReadIO(elapsed time.Duration, bytes int) {
	atomic.AddInt64(&updateFlatValueReadIONanos, elapsed.Nanoseconds())
	atomic.AddInt64(&updateFlatValueReadIOBytes, int64(bytes))
}

func recordFlatValueWrite(puts, deletes int, elapsed time.Duration) {
	atomic.AddInt64(&updateFlatValuePuts, int64(puts))
	atomic.AddInt64(&updateFlatValueDeletes, int64(deletes))
	atomic.AddInt64(&updateFlatValueWriteNanos, elapsed.Nanoseconds())
}

func recordHashDiagnostics(diag HashDiagnostics) {
	hashDiagnosticsMu.Lock()
	hashDiagnostics = diag
	hashDiagnosticsMu.Unlock()
}

// LastHashDiagnostics returns the breakdown of the latest root calculation.
func LastHashDiagnostics() HashDiagnostics {
	hashDiagnosticsMu.Lock()
	defer hashDiagnosticsMu.Unlock()
	return hashDiagnostics
}

// ResetHashDiagnostics clears the latest root calculation breakdown.
func ResetHashDiagnostics() {
	hashDiagnosticsMu.Lock()
	hashDiagnostics = HashDiagnostics{}
	hashDiagnosticsMu.Unlock()
}

// ResetArchiveCumulativeDiagnostics clears run-wide archive counters. Replay
// tests call this once at startup; normal operation may leave them accumulating.
func ResetArchiveCumulativeDiagnostics() {
	atomic.StoreInt64(&archiveCumulativeArchivedLeaves, 0)
	atomic.StoreInt64(&archiveCumulativeFlatValuePuts, 0)
	atomic.StoreInt64(&archiveCumulativeFlatValueDeletes, 0)
	atomic.StoreInt64(&archiveCumulativeFlatValuePutBytes, 0)
}

// LastArchiveCumulativeDiagnostics returns the current run-wide archive counters.
func LastArchiveCumulativeDiagnostics() ArchiveCumulativeDiagnostics {
	return ArchiveCumulativeDiagnostics{
		ArchivedLeaves:    atomic.LoadInt64(&archiveCumulativeArchivedLeaves),
		FlatValuePuts:     atomic.LoadInt64(&archiveCumulativeFlatValuePuts),
		FlatValueDeletes:  atomic.LoadInt64(&archiveCumulativeFlatValueDeletes),
		FlatValuePutBytes: atomic.LoadInt64(&archiveCumulativeFlatValuePutBytes),
	}
}

func recordFlatValuePut(byteLen int) {
	atomic.AddInt64(&archiveCumulativeFlatValuePuts, 1)
	if byteLen > 0 {
		atomic.AddInt64(&archiveCumulativeFlatValuePutBytes, int64(byteLen))
	}
}

func recordFlatValueDelete() {
	atomic.AddInt64(&archiveCumulativeFlatValueDeletes, 1)
}

// ShardCommitDiagnostics captures the inside of one shard commit.
type ShardCommitDiagnostics struct {
	TotalNanos        int64
	LockWaitNanos     int64
	RootCommitNanos   int64
	StaleDeleteNanos  int64
	PendingValueNanos int64

	CommitSerializeNanos int64
	CommitHashNanos      int64
	CommitPersistNanos   int64
	CommitBatchPutNanos  int64
	CommitCacheNanos     int64
	CommitBookkeepNanos  int64
	CommitMaxNodeNanos   int64
	CommitMaxNodeType    int64
	CommitMaxNodeBytes   int64
	DirtyNodeCount       int64
	CleanNodeCount       int64
	PathRelocationCount  int64
	PersistedNodeCount   int64

	NodeCount               int64
	StaleSetLen             int64
	PendingFlatValuePuts    int64
	PendingFlatValueDeletes int64
	RootWasNil              bool
}

func (d ShardCommitDiagnostics) PendingValuesTotal() int64 {
	return d.PendingFlatValuePuts + d.PendingFlatValueDeletes
}

func (d ShardCommitDiagnostics) PendingFlatValuesTotal() int64 {
	return d.PendingFlatValuePuts + d.PendingFlatValueDeletes
}

func (d ShardCommitDiagnostics) String() string {
	return fmt.Sprintf(
		"shardTotal=%v shardLockWait=%v shardRootCommit=%v shardCommitSerialize=%v shardCommitHash=%v shardCommitPersist=%v shardCommitBatchPut=%v shardCommitCache=%v shardCommitBookkeep=%v shardCommitMaxNode=%v shardCommitMaxNodeType=%d shardCommitMaxNodeBytes=%d shardDirtyNodes=%d shardCleanNodes=%d shardPathRelocations=%d shardPersistedNodes=%d shardStaleDeletes=%v shardPendingValues=%v shardNodeCount=%d shardStaleSetLen=%d shardPendingValuesCount=%d shardPendingFlatValuesCount=%d shardRootNil=%v",
		time.Duration(d.TotalNanos),
		time.Duration(d.LockWaitNanos),
		time.Duration(d.RootCommitNanos),
		time.Duration(d.CommitSerializeNanos),
		time.Duration(d.CommitHashNanos),
		time.Duration(d.CommitPersistNanos),
		time.Duration(d.CommitBatchPutNanos),
		time.Duration(d.CommitCacheNanos),
		time.Duration(d.CommitBookkeepNanos),
		time.Duration(d.CommitMaxNodeNanos),
		d.CommitMaxNodeType,
		d.CommitMaxNodeBytes,
		d.DirtyNodeCount,
		d.CleanNodeCount,
		d.PathRelocationCount,
		d.PersistedNodeCount,
		time.Duration(d.StaleDeleteNanos),
		time.Duration(d.PendingValueNanos),
		d.NodeCount,
		d.StaleSetLen,
		d.PendingValuesTotal(),
		d.PendingFlatValuesTotal(),
		d.RootWasNil,
	)
}

type commitWorkStats struct {
	serializeNanos  int64
	hashNanos       int64
	persistNanos    int64
	batchPutNanos   int64
	cacheNanos      int64
	bookkeepNanos   int64
	maxNodeNanos    int64
	maxNodeType     int64
	maxNodeBytes    int64
	dirtyNodes      int64
	cleanNodes      int64
	pathRelocations int64
	persistedNodes  int64
}

func (s *commitWorkStats) addNode(duration time.Duration, nodeType byte, bytes int) {
	if s == nil {
		return
	}
	if duration.Nanoseconds() > s.maxNodeNanos {
		s.maxNodeNanos = duration.Nanoseconds()
		s.maxNodeType = int64(nodeType)
		s.maxNodeBytes = int64(bytes)
	}
}

func (s *commitWorkStats) applyTo(diag *ShardCommitDiagnostics) {
	if s == nil || diag == nil {
		return
	}
	diag.CommitSerializeNanos = s.serializeNanos
	diag.CommitHashNanos = s.hashNanos
	diag.CommitPersistNanos = s.persistNanos
	diag.CommitBatchPutNanos = s.batchPutNanos
	diag.CommitCacheNanos = s.cacheNanos
	diag.CommitBookkeepNanos = s.bookkeepNanos
	diag.CommitMaxNodeNanos = s.maxNodeNanos
	diag.CommitMaxNodeType = s.maxNodeType
	diag.CommitMaxNodeBytes = s.maxNodeBytes
	diag.DirtyNodeCount = s.dirtyNodes
	diag.CleanNodeCount = s.cleanNodes
	diag.PathRelocationCount = s.pathRelocations
	diag.PersistedNodeCount = s.persistedNodes
}

type pruneCounterSnapshot struct {
	internalVisits  int64
	hotSkips        int64
	childHits       int64
	childSkips      int64
	bulkCollects    int64
	leaves          int64
	stubs           int64
	buildItems      int64
	buildBuckets    int64
	parallelBuilds  int64
	pathAbsorbed    int64
	rootPoolItems   int64
	rootPoolBuckets int64
}

var (
	commitDiagTotalNanos                 int64
	commitDiagCommitToBatchNanos         int64
	commitDiagShardCommitNanos           int64
	commitDiagRootHashNanos              int64
	commitDiagBatchWriteNanos            int64
	commitDiagAdapterMergeNanos          int64
	commitDiagTrieDBUpdateNanos          int64
	commitDiagPruneTotalNanos            int64
	commitDiagPruneWaitNanos             int64
	commitDiagPruneShardNanos            int64
	commitDiagPrunePrefetchNanos         int64
	commitDiagDirtyShards                int64
	commitDiagNodeCacheHits              int64
	commitDiagNodeCacheMisses            int64
	commitDiagPathNodeDBGets             int64
	commitDiagArchivePromotionChecks     int64
	commitDiagArchivePromotionHits       int64
	commitDiagBucketRecomputes           int64
	commitDiagCommitmentPointCacheHits   int64
	commitDiagCommitmentPointCacheMisses int64
	commitDiagPruneInternalVisits        int64
	commitDiagPruneHotSkips              int64
	commitDiagPruneChildHits             int64
	commitDiagPruneChildSkips            int64
	commitDiagPruneBulkCollects          int64
	commitDiagPruneCollectedLeaves       int64
	commitDiagPruneCollectedStubs        int64
	commitDiagPruneBuildItems            int64
	commitDiagPruneBuildBuckets          int64
	commitDiagPruneArchiveBuildParallels int64
	commitDiagPrunePathAbsorbedItems     int64
	commitDiagPruneRootPoolItems         int64
	commitDiagPruneRootPoolBuckets       int64
	commitDiagShardMaxID                 int64
	commitDiagShardMaxCommitNanos        int64
	commitDiagShardMaxLockWaitNanos      int64
	commitDiagShardMaxRootCommitNanos    int64
	commitDiagShardMaxStaleDeleteNanos   int64
	commitDiagShardMaxPendingValueNanos  int64
	commitDiagShardMaxNodeCount          int64
	commitDiagShardMaxStaleSetLen        int64
	commitDiagShardMaxPendingValues      int64
	commitDiagShardMaxPendingFlatValues  int64
	commitDiagShardMaxSerializeNanos     int64
	commitDiagShardMaxHashNanos          int64
	commitDiagShardMaxPersistNanos       int64
	commitDiagShardMaxBatchPutNanos      int64
	commitDiagShardMaxCacheNanos         int64
	commitDiagShardMaxBookkeepNanos      int64
	commitDiagShardMaxNodeNanos          int64
	commitDiagShardMaxNodeType           int64
	commitDiagShardMaxNodeBytes          int64
	commitDiagRawBatchOps                int64
	commitDiagRawBatchBytes              int64
	commitDiagRawShardMaxID              int64
	commitDiagRawShardMaxOps             int64
	commitDiagRawShardMaxBytes           int64
	commitDiagNodeCacheEntries           int64
	commitDiagNodeCacheBytes             int64
	commitDiagNodeCacheEntryLimit        int64
	commitDiagNodeCacheBytesLimit        int64
	commitDiagNodeCacheShards            int64
	commitDiagNodeCacheTotalHits         int64
	commitDiagNodeCacheTotalMisses       int64
	commitDiagNodeCacheEvictions         int64
	commitDiagNodeCacheLockContentions   int64
	commitDiagNodeCacheLockWaitNanos     int64
	commitDiagNodeCacheDBGets            int64
	commitDiagNodeCacheDBGetNanos        int64
	commitDiagNodeCacheDBLoadBytes       int64
	commitDiagRuntimeHeapAlloc           int64
	commitDiagRuntimeHeapSys             int64
	commitDiagRuntimeHeapInuse           int64
	commitDiagRuntimeSys                 int64
	commitDiagRuntimeNumGC               int64
	commitDiagRuntimePauseTotal          int64
	commitDiagRuntimeLastPauseNs         int64

	prunePressureMu     sync.Mutex
	prunePressureDiag   PrunePressureDiagnostics
	prunePressureMaxSet bool
)

// ResetCommitDiagnostics clears the global commit diagnostic counters.
func ResetCommitDiagnostics() {
	atomic.StoreInt64(&commitDiagTotalNanos, 0)
	atomic.StoreInt64(&commitDiagCommitToBatchNanos, 0)
	atomic.StoreInt64(&commitDiagShardCommitNanos, 0)
	atomic.StoreInt64(&commitDiagRootHashNanos, 0)
	atomic.StoreInt64(&commitDiagBatchWriteNanos, 0)
	atomic.StoreInt64(&commitDiagAdapterMergeNanos, 0)
	atomic.StoreInt64(&commitDiagTrieDBUpdateNanos, 0)
	atomic.StoreInt64(&commitDiagPruneTotalNanos, 0)
	atomic.StoreInt64(&commitDiagPruneWaitNanos, 0)
	atomic.StoreInt64(&commitDiagPruneShardNanos, 0)
	atomic.StoreInt64(&commitDiagPrunePrefetchNanos, 0)
	atomic.StoreInt64(&commitDiagDirtyShards, 0)
	atomic.StoreInt64(&commitDiagNodeCacheHits, 0)
	atomic.StoreInt64(&commitDiagNodeCacheMisses, 0)
	atomic.StoreInt64(&commitDiagPathNodeDBGets, 0)
	atomic.StoreInt64(&commitDiagArchivePromotionChecks, 0)
	atomic.StoreInt64(&commitDiagArchivePromotionHits, 0)
	atomic.StoreInt64(&commitDiagBucketRecomputes, 0)
	atomic.StoreInt64(&commitDiagCommitmentPointCacheHits, 0)
	atomic.StoreInt64(&commitDiagCommitmentPointCacheMisses, 0)
	atomic.StoreInt64(&commitDiagPruneInternalVisits, 0)
	atomic.StoreInt64(&commitDiagPruneHotSkips, 0)
	atomic.StoreInt64(&commitDiagPruneChildHits, 0)
	atomic.StoreInt64(&commitDiagPruneChildSkips, 0)
	atomic.StoreInt64(&commitDiagPruneBulkCollects, 0)
	atomic.StoreInt64(&commitDiagPruneCollectedLeaves, 0)
	atomic.StoreInt64(&commitDiagPruneCollectedStubs, 0)
	atomic.StoreInt64(&commitDiagPruneBuildItems, 0)
	atomic.StoreInt64(&commitDiagPruneBuildBuckets, 0)
	atomic.StoreInt64(&commitDiagPruneArchiveBuildParallels, 0)
	atomic.StoreInt64(&commitDiagPrunePathAbsorbedItems, 0)
	atomic.StoreInt64(&commitDiagPruneRootPoolItems, 0)
	atomic.StoreInt64(&commitDiagPruneRootPoolBuckets, 0)
	atomic.StoreInt64(&commitDiagShardMaxID, 0)
	atomic.StoreInt64(&commitDiagShardMaxCommitNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxLockWaitNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxRootCommitNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxStaleDeleteNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxPendingValueNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxNodeCount, 0)
	atomic.StoreInt64(&commitDiagShardMaxStaleSetLen, 0)
	atomic.StoreInt64(&commitDiagShardMaxPendingValues, 0)
	atomic.StoreInt64(&commitDiagShardMaxPendingFlatValues, 0)
	atomic.StoreInt64(&commitDiagShardMaxSerializeNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxHashNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxPersistNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxBatchPutNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxCacheNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxBookkeepNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxNodeNanos, 0)
	atomic.StoreInt64(&commitDiagShardMaxNodeType, 0)
	atomic.StoreInt64(&commitDiagShardMaxNodeBytes, 0)
	atomic.StoreInt64(&commitDiagRawBatchOps, 0)
	atomic.StoreInt64(&commitDiagRawBatchBytes, 0)
	atomic.StoreInt64(&commitDiagRawShardMaxID, 0)
	atomic.StoreInt64(&commitDiagRawShardMaxOps, 0)
	atomic.StoreInt64(&commitDiagRawShardMaxBytes, 0)
	atomic.StoreInt64(&commitDiagNodeCacheEntries, 0)
	atomic.StoreInt64(&commitDiagNodeCacheBytes, 0)
	atomic.StoreInt64(&commitDiagNodeCacheEntryLimit, 0)
	atomic.StoreInt64(&commitDiagNodeCacheBytesLimit, 0)
	atomic.StoreInt64(&commitDiagNodeCacheShards, 0)
	atomic.StoreInt64(&commitDiagNodeCacheTotalHits, 0)
	atomic.StoreInt64(&commitDiagNodeCacheTotalMisses, 0)
	atomic.StoreInt64(&commitDiagNodeCacheEvictions, 0)
	atomic.StoreInt64(&commitDiagNodeCacheLockContentions, 0)
	atomic.StoreInt64(&commitDiagNodeCacheLockWaitNanos, 0)
	atomic.StoreInt64(&commitDiagNodeCacheDBGets, 0)
	atomic.StoreInt64(&commitDiagNodeCacheDBGetNanos, 0)
	atomic.StoreInt64(&commitDiagNodeCacheDBLoadBytes, 0)
	atomic.StoreInt64(&commitDiagRuntimeHeapAlloc, 0)
	atomic.StoreInt64(&commitDiagRuntimeHeapSys, 0)
	atomic.StoreInt64(&commitDiagRuntimeHeapInuse, 0)
	atomic.StoreInt64(&commitDiagRuntimeSys, 0)
	atomic.StoreInt64(&commitDiagRuntimeNumGC, 0)
	atomic.StoreInt64(&commitDiagRuntimePauseTotal, 0)
	atomic.StoreInt64(&commitDiagRuntimeLastPauseNs, 0)
}

func recordCommitDiagnostics(total, commitToBatch, batchWrite int64, dirtyShards int) {
	atomic.StoreInt64(&commitDiagTotalNanos, total)
	atomic.StoreInt64(&commitDiagCommitToBatchNanos, commitToBatch)
	atomic.StoreInt64(&commitDiagBatchWriteNanos, batchWrite)
	atomic.StoreInt64(&commitDiagDirtyShards, int64(dirtyShards))
}

func recordCommitToBatchDiagnostics(shardCommit, rootHash int64) {
	atomic.StoreInt64(&commitDiagShardCommitNanos, shardCommit)
	atomic.StoreInt64(&commitDiagRootHashNanos, rootHash)
}

func recordPruneDiagnostics(total, wait, shard, prefetch int64) {
	atomic.StoreInt64(&commitDiagPruneTotalNanos, total)
	atomic.StoreInt64(&commitDiagPruneWaitNanos, wait)
	atomic.StoreInt64(&commitDiagPruneShardNanos, shard)
	atomic.StoreInt64(&commitDiagPrunePrefetchNanos, prefetch)
}

func snapshotPruneCounters() pruneCounterSnapshot {
	return pruneCounterSnapshot{
		internalVisits: atomic.LoadInt64(&commitDiagPruneInternalVisits),
		hotSkips:       atomic.LoadInt64(&commitDiagPruneHotSkips),
		childHits:      atomic.LoadInt64(&commitDiagPruneChildHits),
		childSkips:     atomic.LoadInt64(&commitDiagPruneChildSkips),
		bulkCollects:   atomic.LoadInt64(&commitDiagPruneBulkCollects),
		leaves:         atomic.LoadInt64(&commitDiagPruneCollectedLeaves),
		stubs:          atomic.LoadInt64(&commitDiagPruneCollectedStubs),
		buildItems:     atomic.LoadInt64(&commitDiagPruneBuildItems),
		buildBuckets:   atomic.LoadInt64(&commitDiagPruneBuildBuckets),
		parallelBuilds: atomic.LoadInt64(&commitDiagPruneArchiveBuildParallels),
		pathAbsorbed:   atomic.LoadInt64(&commitDiagPrunePathAbsorbedItems),
		rootPoolItems:  atomic.LoadInt64(&commitDiagPruneRootPoolItems),
		rootPoolBuckets: atomic.LoadInt64(
			&commitDiagPruneRootPoolBuckets,
		),
	}
}

func recordPruneShardPressure(shardID int, total, wait, shard, prefetch int64, phases ShardPruneDiagnostics, before, after pruneCounterSnapshot) {
	current := PrunePressureDiagnostics{
		LastShardID:             int64(shardID),
		LastTotalNanos:          total,
		LastWaitNanos:           wait,
		LastShardNanos:          shard,
		LastPrefetchNanos:       prefetch,
		LastLockWaitNanos:       phases.LockWaitNanos,
		LastRootLoadNanos:       phases.RootLoadNanos,
		LastWalkNanos:           phases.WalkNanos,
		LastFinishNanos:         phases.FinishNanos,
		DetailedCountersEnabled: phases.DetailedCountersEnabled,
		LastInternalVisits:      after.internalVisits - before.internalVisits,
		LastHotSkips:            after.hotSkips - before.hotSkips,
		LastChildHits:           after.childHits - before.childHits,
		LastChildSkips:          after.childSkips - before.childSkips,
		LastBulkCollects:        after.bulkCollects - before.bulkCollects,
		LastLeaves:              after.leaves - before.leaves,
		LastStubs:               after.stubs - before.stubs,
		LastBuildItems:          after.buildItems - before.buildItems,
		LastBuildBuckets:        after.buildBuckets - before.buildBuckets,
		LastParallelBuilds:      after.parallelBuilds - before.parallelBuilds,
		LastPathAbsorbed:        after.pathAbsorbed - before.pathAbsorbed,
		LastRootPoolItems:       after.rootPoolItems - before.rootPoolItems,
		LastRootPoolBuckets:     after.rootPoolBuckets - before.rootPoolBuckets,
	}

	prunePressureMu.Lock()
	defer prunePressureMu.Unlock()
	prunePressureDiag.LastShardID = current.LastShardID
	prunePressureDiag.LastTotalNanos = current.LastTotalNanos
	prunePressureDiag.LastWaitNanos = current.LastWaitNanos
	prunePressureDiag.LastShardNanos = current.LastShardNanos
	prunePressureDiag.LastPrefetchNanos = current.LastPrefetchNanos
	prunePressureDiag.LastLockWaitNanos = current.LastLockWaitNanos
	prunePressureDiag.LastRootLoadNanos = current.LastRootLoadNanos
	prunePressureDiag.LastWalkNanos = current.LastWalkNanos
	prunePressureDiag.LastFinishNanos = current.LastFinishNanos
	prunePressureDiag.DetailedCountersEnabled = current.DetailedCountersEnabled
	prunePressureDiag.LastInternalVisits = current.LastInternalVisits
	prunePressureDiag.LastHotSkips = current.LastHotSkips
	prunePressureDiag.LastChildHits = current.LastChildHits
	prunePressureDiag.LastChildSkips = current.LastChildSkips
	prunePressureDiag.LastBulkCollects = current.LastBulkCollects
	prunePressureDiag.LastLeaves = current.LastLeaves
	prunePressureDiag.LastStubs = current.LastStubs
	prunePressureDiag.LastBuildItems = current.LastBuildItems
	prunePressureDiag.LastBuildBuckets = current.LastBuildBuckets
	prunePressureDiag.LastParallelBuilds = current.LastParallelBuilds
	prunePressureDiag.LastPathAbsorbed = current.LastPathAbsorbed
	prunePressureDiag.LastRootPoolItems = current.LastRootPoolItems
	prunePressureDiag.LastRootPoolBuckets = current.LastRootPoolBuckets
	if prunePressureMaxSet && current.LastShardNanos <= prunePressureDiag.MaxShardNanos {
		return
	}
	prunePressureMaxSet = true
	prunePressureDiag.MaxShardID = current.LastShardID
	prunePressureDiag.MaxTotalNanos = current.LastTotalNanos
	prunePressureDiag.MaxWaitNanos = current.LastWaitNanos
	prunePressureDiag.MaxShardNanos = current.LastShardNanos
	prunePressureDiag.MaxPrefetchNanos = current.LastPrefetchNanos
	prunePressureDiag.MaxLockWaitNanos = current.LastLockWaitNanos
	prunePressureDiag.MaxRootLoadNanos = current.LastRootLoadNanos
	prunePressureDiag.MaxWalkNanos = current.LastWalkNanos
	prunePressureDiag.MaxFinishNanos = current.LastFinishNanos
	prunePressureDiag.MaxDetailedCountersEnabled = current.DetailedCountersEnabled
	prunePressureDiag.MaxInternalVisits = current.LastInternalVisits
	prunePressureDiag.MaxHotSkips = current.LastHotSkips
	prunePressureDiag.MaxChildHits = current.LastChildHits
	prunePressureDiag.MaxChildSkips = current.LastChildSkips
	prunePressureDiag.MaxBulkCollects = current.LastBulkCollects
	prunePressureDiag.MaxLeaves = current.LastLeaves
	prunePressureDiag.MaxStubs = current.LastStubs
	prunePressureDiag.MaxBuildItems = current.LastBuildItems
	prunePressureDiag.MaxBuildBuckets = current.LastBuildBuckets
	prunePressureDiag.MaxParallelBuilds = current.LastParallelBuilds
	prunePressureDiag.MaxPathAbsorbed = current.LastPathAbsorbed
	prunePressureDiag.MaxRootPoolItems = current.LastRootPoolItems
	prunePressureDiag.MaxRootPoolBuckets = current.LastRootPoolBuckets
}

// ResetPrunePressureDiagnostics clears the per-window prune pressure snapshot.
func ResetPrunePressureDiagnostics() {
	prunePressureMu.Lock()
	prunePressureDiag = PrunePressureDiagnostics{}
	prunePressureMaxSet = false
	prunePressureMu.Unlock()
}

// LastPrunePressureDiagnostics returns the latest per-shard prune pressure snapshot.
func LastPrunePressureDiagnostics() PrunePressureDiagnostics {
	prunePressureMu.Lock()
	defer prunePressureMu.Unlock()
	return prunePressureDiag
}

// RecordWrapperCommitDiagnostics records the state.Trie wrapper commit path.
func RecordWrapperCommitDiagnostics(total, shardCommit, rootHash, adapterMerge, trieDBUpdate, batchWrite time.Duration, dirtyShards int) {
	atomic.StoreInt64(&commitDiagTotalNanos, total.Nanoseconds())
	atomic.StoreInt64(&commitDiagCommitToBatchNanos, total.Nanoseconds())
	atomic.StoreInt64(&commitDiagShardCommitNanos, shardCommit.Nanoseconds())
	atomic.StoreInt64(&commitDiagRootHashNanos, rootHash.Nanoseconds())
	atomic.StoreInt64(&commitDiagAdapterMergeNanos, adapterMerge.Nanoseconds())
	atomic.StoreInt64(&commitDiagTrieDBUpdateNanos, trieDBUpdate.Nanoseconds())
	atomic.StoreInt64(&commitDiagBatchWriteNanos, batchWrite.Nanoseconds())
	atomic.StoreInt64(&commitDiagDirtyShards, int64(dirtyShards))
}

func RecordWrapperShardCommitMax(shardID int, diag ShardCommitDiagnostics) {
	atomic.StoreInt64(&commitDiagShardMaxID, int64(shardID))
	atomic.StoreInt64(&commitDiagShardMaxCommitNanos, diag.TotalNanos)
	atomic.StoreInt64(&commitDiagShardMaxLockWaitNanos, diag.LockWaitNanos)
	atomic.StoreInt64(&commitDiagShardMaxRootCommitNanos, diag.RootCommitNanos)
	atomic.StoreInt64(&commitDiagShardMaxStaleDeleteNanos, diag.StaleDeleteNanos)
	atomic.StoreInt64(&commitDiagShardMaxPendingValueNanos, diag.PendingValueNanos)
	atomic.StoreInt64(&commitDiagShardMaxNodeCount, diag.NodeCount)
	atomic.StoreInt64(&commitDiagShardMaxStaleSetLen, diag.StaleSetLen)
	atomic.StoreInt64(&commitDiagShardMaxPendingValues, diag.PendingValuesTotal())
	atomic.StoreInt64(&commitDiagShardMaxPendingFlatValues, diag.PendingFlatValuesTotal())
	atomic.StoreInt64(&commitDiagShardMaxSerializeNanos, diag.CommitSerializeNanos)
	atomic.StoreInt64(&commitDiagShardMaxHashNanos, diag.CommitHashNanos)
	atomic.StoreInt64(&commitDiagShardMaxPersistNanos, diag.CommitPersistNanos)
	atomic.StoreInt64(&commitDiagShardMaxBatchPutNanos, diag.CommitBatchPutNanos)
	atomic.StoreInt64(&commitDiagShardMaxCacheNanos, diag.CommitCacheNanos)
	atomic.StoreInt64(&commitDiagShardMaxBookkeepNanos, diag.CommitBookkeepNanos)
	atomic.StoreInt64(&commitDiagShardMaxNodeNanos, diag.CommitMaxNodeNanos)
	atomic.StoreInt64(&commitDiagShardMaxNodeType, diag.CommitMaxNodeType)
	atomic.StoreInt64(&commitDiagShardMaxNodeBytes, diag.CommitMaxNodeBytes)
}

func RecordWrapperResourceDiagnostics(rawOps, rawBytes, rawMaxShardID, rawMaxOps, rawMaxBytes, nodeCacheEntries, nodeCacheBytes int64, heapAlloc, heapSys, heapInuse, runtimeSys, numGC, pauseTotal, lastPause uint64) {
	atomic.StoreInt64(&commitDiagRawBatchOps, rawOps)
	atomic.StoreInt64(&commitDiagRawBatchBytes, rawBytes)
	atomic.StoreInt64(&commitDiagRawShardMaxID, rawMaxShardID)
	atomic.StoreInt64(&commitDiagRawShardMaxOps, rawMaxOps)
	atomic.StoreInt64(&commitDiagRawShardMaxBytes, rawMaxBytes)
	atomic.StoreInt64(&commitDiagNodeCacheEntries, nodeCacheEntries)
	atomic.StoreInt64(&commitDiagNodeCacheBytes, nodeCacheBytes)
	atomic.StoreInt64(&commitDiagRuntimeHeapAlloc, int64(heapAlloc))
	atomic.StoreInt64(&commitDiagRuntimeHeapSys, int64(heapSys))
	atomic.StoreInt64(&commitDiagRuntimeHeapInuse, int64(heapInuse))
	atomic.StoreInt64(&commitDiagRuntimeSys, int64(runtimeSys))
	atomic.StoreInt64(&commitDiagRuntimeNumGC, int64(numGC))
	atomic.StoreInt64(&commitDiagRuntimePauseTotal, int64(pauseTotal))
	atomic.StoreInt64(&commitDiagRuntimeLastPauseNs, int64(lastPause))
}

// RecordNodeCacheDiagnostics records the lifetime counters of the caches used
// by the wrapper and the active archive trie.
func RecordNodeCacheDiagnostics(diag NodeCacheDiagnostics) {
	atomic.StoreInt64(&commitDiagNodeCacheEntries, diag.Entries)
	atomic.StoreInt64(&commitDiagNodeCacheBytes, diag.Bytes)
	atomic.StoreInt64(&commitDiagNodeCacheEntryLimit, diag.EntryLimit)
	atomic.StoreInt64(&commitDiagNodeCacheBytesLimit, diag.BytesLimit)
	atomic.StoreInt64(&commitDiagNodeCacheShards, diag.Shards)
	atomic.StoreInt64(&commitDiagNodeCacheTotalHits, diag.Hits)
	atomic.StoreInt64(&commitDiagNodeCacheTotalMisses, diag.Misses)
	atomic.StoreInt64(&commitDiagNodeCacheEvictions, diag.Evictions)
	atomic.StoreInt64(&commitDiagNodeCacheLockContentions, diag.LockContentions)
	atomic.StoreInt64(&commitDiagNodeCacheLockWaitNanos, diag.LockWaitNanos)
	atomic.StoreInt64(&commitDiagNodeCacheDBGets, diag.DBGets)
	atomic.StoreInt64(&commitDiagNodeCacheDBGetNanos, diag.DBGetNanos)
	atomic.StoreInt64(&commitDiagNodeCacheDBLoadBytes, diag.DBLoadBytes)
}

func recordNodeCacheLookupIfEnabled(config *Config, hit bool) {
	if config == nil || !config.EnablePathDiagnostics {
		return
	}
	if hit {
		atomic.AddInt64(&commitDiagNodeCacheHits, 1)
		return
	}
	atomic.AddInt64(&commitDiagNodeCacheMisses, 1)
}

func recordPathDBGetIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPathNodeDBGets, 1)
	}
}

func recordArchivePromotionCheckIfEnabled(config *Config, hit bool) {
	atomic.AddInt64(&updateArchivePromotionChecks, 1)
	if hit {
		atomic.AddInt64(&updateArchivePromotionHits, 1)
	}
	if config == nil || !config.EnablePathDiagnostics {
		return
	}
	atomic.AddInt64(&commitDiagArchivePromotionChecks, 1)
	if hit {
		atomic.AddInt64(&commitDiagArchivePromotionHits, 1)
	}
}

func recordBucketRecomputeIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagBucketRecomputes, 1)
	}
}

func recordCommitmentPointCacheLookupIfEnabled(config *Config, hit bool) {
	if config == nil || !config.EnablePathDiagnostics {
		return
	}
	if hit {
		atomic.AddInt64(&commitDiagCommitmentPointCacheHits, 1)
		return
	}
	atomic.AddInt64(&commitDiagCommitmentPointCacheMisses, 1)
}

func recordPruneInternalVisitIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPruneInternalVisits, 1)
	}
}

func recordPruneHotSkipIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPruneHotSkips, 1)
	}
}

func recordPruneChildDecisionIfEnabled(config *Config, hit bool) {
	if config == nil || !config.EnablePathDiagnostics {
		return
	}
	if hit {
		atomic.AddInt64(&commitDiagPruneChildHits, 1)
		return
	}
	atomic.AddInt64(&commitDiagPruneChildSkips, 1)
}

func recordPruneBulkCollectIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPruneBulkCollects, 1)
	}
}

func recordPruneCollectedLeafIfEnabled(config *Config) {
	atomic.AddInt64(&archiveCumulativeArchivedLeaves, 1)
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPruneCollectedLeaves, 1)
	}
}

func recordPruneCollectedStubIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPruneCollectedStubs, 1)
	}
}

func recordPruneBuildBucketIfEnabled(config *Config, items int) {
	if config == nil || !config.EnablePathDiagnostics {
		return
	}
	atomic.AddInt64(&commitDiagPruneBuildBuckets, 1)
	atomic.AddInt64(&commitDiagPruneBuildItems, int64(items))
}

func recordPruneArchiveBuildParallelIfEnabled(config *Config) {
	if config != nil && config.EnablePathDiagnostics {
		atomic.AddInt64(&commitDiagPruneArchiveBuildParallels, 1)
	}
}

func recordPrunePathAbsorbedIfEnabled(config *Config, items int) {
	if config != nil && config.EnablePathDiagnostics && items > 0 {
		atomic.AddInt64(&commitDiagPrunePathAbsorbedItems, int64(items))
	}
}

func recordPruneRootPoolIfEnabled(config *Config, items, buckets int) {
	if config == nil || !config.EnablePathDiagnostics || items <= 0 {
		return
	}
	atomic.AddInt64(&commitDiagPruneRootPoolItems, int64(items))
	if buckets > 0 {
		atomic.AddInt64(&commitDiagPruneRootPoolBuckets, int64(buckets))
	}
}

// LastCommitDiagnostics returns the most recent ASCT commit diagnostic snapshot.
func LastCommitDiagnostics() CommitDiagnostics {
	return CommitDiagnostics{
		TotalNanos:                 atomic.LoadInt64(&commitDiagTotalNanos),
		CommitToBatchNanos:         atomic.LoadInt64(&commitDiagCommitToBatchNanos),
		ShardCommitNanos:           atomic.LoadInt64(&commitDiagShardCommitNanos),
		RootHashNanos:              atomic.LoadInt64(&commitDiagRootHashNanos),
		BatchWriteNanos:            atomic.LoadInt64(&commitDiagBatchWriteNanos),
		AdapterMergeNanos:          atomic.LoadInt64(&commitDiagAdapterMergeNanos),
		TrieDBUpdateNanos:          atomic.LoadInt64(&commitDiagTrieDBUpdateNanos),
		PruneTotalNanos:            atomic.LoadInt64(&commitDiagPruneTotalNanos),
		PruneWaitNanos:             atomic.LoadInt64(&commitDiagPruneWaitNanos),
		PruneShardNanos:            atomic.LoadInt64(&commitDiagPruneShardNanos),
		PrunePrefetchNanos:         atomic.LoadInt64(&commitDiagPrunePrefetchNanos),
		DirtyShards:                atomic.LoadInt64(&commitDiagDirtyShards),
		NodeCacheHits:              atomic.LoadInt64(&commitDiagNodeCacheHits),
		NodeCacheMisses:            atomic.LoadInt64(&commitDiagNodeCacheMisses),
		PathNodeDBGets:             atomic.LoadInt64(&commitDiagPathNodeDBGets),
		ArchivePromotionChecks:     atomic.LoadInt64(&commitDiagArchivePromotionChecks),
		ArchivePromotionHits:       atomic.LoadInt64(&commitDiagArchivePromotionHits),
		BucketRecomputes:           atomic.LoadInt64(&commitDiagBucketRecomputes),
		CommitmentPointCacheHits:   atomic.LoadInt64(&commitDiagCommitmentPointCacheHits),
		CommitmentPointCacheMisses: atomic.LoadInt64(&commitDiagCommitmentPointCacheMisses),
		PruneInternalVisits:        atomic.LoadInt64(&commitDiagPruneInternalVisits),
		PruneHotSkips:              atomic.LoadInt64(&commitDiagPruneHotSkips),
		PruneChildHits:             atomic.LoadInt64(&commitDiagPruneChildHits),
		PruneChildSkips:            atomic.LoadInt64(&commitDiagPruneChildSkips),
		PruneBulkCollects:          atomic.LoadInt64(&commitDiagPruneBulkCollects),
		PruneCollectedLeaves:       atomic.LoadInt64(&commitDiagPruneCollectedLeaves),
		PruneCollectedStubs:        atomic.LoadInt64(&commitDiagPruneCollectedStubs),
		PruneBuildItems:            atomic.LoadInt64(&commitDiagPruneBuildItems),
		PruneBuildBuckets:          atomic.LoadInt64(&commitDiagPruneBuildBuckets),
		PruneArchiveBuildParallels: atomic.LoadInt64(&commitDiagPruneArchiveBuildParallels),
		PrunePathAbsorbedItems:     atomic.LoadInt64(&commitDiagPrunePathAbsorbedItems),
		PruneRootPoolItems:         atomic.LoadInt64(&commitDiagPruneRootPoolItems),
		PruneRootPoolBuckets:       atomic.LoadInt64(&commitDiagPruneRootPoolBuckets),
		ShardMaxID:                 atomic.LoadInt64(&commitDiagShardMaxID),
		ShardMaxCommitNanos:        atomic.LoadInt64(&commitDiagShardMaxCommitNanos),
		ShardMaxLockWaitNanos:      atomic.LoadInt64(&commitDiagShardMaxLockWaitNanos),
		ShardMaxRootCommitNanos:    atomic.LoadInt64(&commitDiagShardMaxRootCommitNanos),
		ShardMaxStaleDeleteNanos:   atomic.LoadInt64(&commitDiagShardMaxStaleDeleteNanos),
		ShardMaxPendingValueNanos:  atomic.LoadInt64(&commitDiagShardMaxPendingValueNanos),
		ShardMaxNodeCount:          atomic.LoadInt64(&commitDiagShardMaxNodeCount),
		ShardMaxStaleSetLen:        atomic.LoadInt64(&commitDiagShardMaxStaleSetLen),
		ShardMaxPendingValues:      atomic.LoadInt64(&commitDiagShardMaxPendingValues),
		ShardMaxPendingFlatValues:  atomic.LoadInt64(&commitDiagShardMaxPendingFlatValues),
		ShardMaxSerializeNanos:     atomic.LoadInt64(&commitDiagShardMaxSerializeNanos),
		ShardMaxHashNanos:          atomic.LoadInt64(&commitDiagShardMaxHashNanos),
		ShardMaxPersistNanos:       atomic.LoadInt64(&commitDiagShardMaxPersistNanos),
		ShardMaxBatchPutNanos:      atomic.LoadInt64(&commitDiagShardMaxBatchPutNanos),
		ShardMaxCacheNanos:         atomic.LoadInt64(&commitDiagShardMaxCacheNanos),
		ShardMaxBookkeepNanos:      atomic.LoadInt64(&commitDiagShardMaxBookkeepNanos),
		ShardMaxNodeNanos:          atomic.LoadInt64(&commitDiagShardMaxNodeNanos),
		ShardMaxNodeType:           atomic.LoadInt64(&commitDiagShardMaxNodeType),
		ShardMaxNodeBytes:          atomic.LoadInt64(&commitDiagShardMaxNodeBytes),
		RawBatchOps:                atomic.LoadInt64(&commitDiagRawBatchOps),
		RawBatchBytes:              atomic.LoadInt64(&commitDiagRawBatchBytes),
		RawShardMaxID:              atomic.LoadInt64(&commitDiagRawShardMaxID),
		RawShardMaxOps:             atomic.LoadInt64(&commitDiagRawShardMaxOps),
		RawShardMaxBytes:           atomic.LoadInt64(&commitDiagRawShardMaxBytes),
		NodeCacheEntries:           atomic.LoadInt64(&commitDiagNodeCacheEntries),
		NodeCacheBytes:             atomic.LoadInt64(&commitDiagNodeCacheBytes),
		NodeCacheEntryLimit:        atomic.LoadInt64(&commitDiagNodeCacheEntryLimit),
		NodeCacheBytesLimit:        atomic.LoadInt64(&commitDiagNodeCacheBytesLimit),
		NodeCacheShards:            atomic.LoadInt64(&commitDiagNodeCacheShards),
		NodeCacheTotalHits:         atomic.LoadInt64(&commitDiagNodeCacheTotalHits),
		NodeCacheTotalMisses:       atomic.LoadInt64(&commitDiagNodeCacheTotalMisses),
		NodeCacheEvictions:         atomic.LoadInt64(&commitDiagNodeCacheEvictions),
		NodeCacheLockContentions:   atomic.LoadInt64(&commitDiagNodeCacheLockContentions),
		NodeCacheLockWaitNanos:     atomic.LoadInt64(&commitDiagNodeCacheLockWaitNanos),
		NodeCacheDBGets:            atomic.LoadInt64(&commitDiagNodeCacheDBGets),
		NodeCacheDBGetNanos:        atomic.LoadInt64(&commitDiagNodeCacheDBGetNanos),
		NodeCacheDBLoadBytes:       atomic.LoadInt64(&commitDiagNodeCacheDBLoadBytes),
		RuntimeHeapAlloc:           atomic.LoadInt64(&commitDiagRuntimeHeapAlloc),
		RuntimeHeapSys:             atomic.LoadInt64(&commitDiagRuntimeHeapSys),
		RuntimeHeapInuse:           atomic.LoadInt64(&commitDiagRuntimeHeapInuse),
		RuntimeSys:                 atomic.LoadInt64(&commitDiagRuntimeSys),
		RuntimeNumGC:               atomic.LoadInt64(&commitDiagRuntimeNumGC),
		RuntimePauseTotal:          atomic.LoadInt64(&commitDiagRuntimePauseTotal),
		RuntimeLastPauseNs:         atomic.LoadInt64(&commitDiagRuntimeLastPauseNs),
	}
}

func (d CommitDiagnostics) String() string {
	return fmt.Sprintf(
		"total=%v commitToBatch=%v shardCommit=%v rootHash=%v batchWrite=%v adapterMerge=%v trieDBUpdate=%v pruneTotal=%v pruneWait=%v pruneShard=%v prunePrefetchStart=%v dirtyShards=%d nodeCacheHits=%d nodeCacheMisses=%d pathNodeDBGets=%d archivePromotionChecks=%d archivePromotionHits=%d bucketRecomputes=%d commitmentPointCacheHits=%d commitmentPointCacheMisses=%d pruneInternalVisits=%d pruneHotSkips=%d pruneChildHits=%d pruneChildSkips=%d pruneBulkCollects=%d pruneCollectedLeaves=%d pruneCollectedStubs=%d pruneBuildItems=%d pruneBuildBuckets=%d pruneArchiveBuildParallels=%d prunePathAbsorbedItems=%d pruneRootPoolItems=%d pruneRootPoolBuckets=%d shardMaxID=%d shardMaxCommit=%v shardMaxLockWait=%v shardMaxRootCommit=%v shardMaxSerialize=%v shardMaxHash=%v shardMaxPersist=%v shardMaxBatchPut=%v shardMaxCache=%v shardMaxBookkeep=%v shardMaxNode=%v shardMaxNodeType=%d shardMaxNodeBytes=%d shardMaxStaleDeletes=%v shardMaxPendingValues=%v shardMaxNodeCount=%d shardMaxStaleSetLen=%d shardMaxPendingValuesCount=%d shardMaxPendingFlatValuesCount=%d rawBatchOps=%d rawBatchBytes=%d rawShardMaxID=%d rawShardMaxOps=%d rawShardMaxBytes=%d nodeCacheEntries=%d nodeCacheBytes=%d nodeCacheTotalHits=%d nodeCacheTotalMisses=%d nodeCacheEvictions=%d heapAlloc=%d heapSys=%d heapInuse=%d runtimeSys=%d numGC=%d pauseTotal=%v lastPause=%v",
		time.Duration(d.TotalNanos),
		time.Duration(d.CommitToBatchNanos),
		time.Duration(d.ShardCommitNanos),
		time.Duration(d.RootHashNanos),
		time.Duration(d.BatchWriteNanos),
		time.Duration(d.AdapterMergeNanos),
		time.Duration(d.TrieDBUpdateNanos),
		time.Duration(d.PruneTotalNanos),
		time.Duration(d.PruneWaitNanos),
		time.Duration(d.PruneShardNanos),
		time.Duration(d.PrunePrefetchNanos),
		d.DirtyShards,
		d.NodeCacheHits,
		d.NodeCacheMisses,
		d.PathNodeDBGets,
		d.ArchivePromotionChecks,
		d.ArchivePromotionHits,
		d.BucketRecomputes,
		d.CommitmentPointCacheHits,
		d.CommitmentPointCacheMisses,
		d.PruneInternalVisits,
		d.PruneHotSkips,
		d.PruneChildHits,
		d.PruneChildSkips,
		d.PruneBulkCollects,
		d.PruneCollectedLeaves,
		d.PruneCollectedStubs,
		d.PruneBuildItems,
		d.PruneBuildBuckets,
		d.PruneArchiveBuildParallels,
		d.PrunePathAbsorbedItems,
		d.PruneRootPoolItems,
		d.PruneRootPoolBuckets,
		d.ShardMaxID,
		time.Duration(d.ShardMaxCommitNanos),
		time.Duration(d.ShardMaxLockWaitNanos),
		time.Duration(d.ShardMaxRootCommitNanos),
		time.Duration(d.ShardMaxSerializeNanos),
		time.Duration(d.ShardMaxHashNanos),
		time.Duration(d.ShardMaxPersistNanos),
		time.Duration(d.ShardMaxBatchPutNanos),
		time.Duration(d.ShardMaxCacheNanos),
		time.Duration(d.ShardMaxBookkeepNanos),
		time.Duration(d.ShardMaxNodeNanos),
		d.ShardMaxNodeType,
		d.ShardMaxNodeBytes,
		time.Duration(d.ShardMaxStaleDeleteNanos),
		time.Duration(d.ShardMaxPendingValueNanos),
		d.ShardMaxNodeCount,
		d.ShardMaxStaleSetLen,
		d.ShardMaxPendingValues,
		d.ShardMaxPendingFlatValues,
		d.RawBatchOps,
		d.RawBatchBytes,
		d.RawShardMaxID,
		d.RawShardMaxOps,
		d.RawShardMaxBytes,
		d.NodeCacheEntries,
		d.NodeCacheBytes,
		d.NodeCacheTotalHits,
		d.NodeCacheTotalMisses,
		d.NodeCacheEvictions,
		d.RuntimeHeapAlloc,
		d.RuntimeHeapSys,
		d.RuntimeHeapInuse,
		d.RuntimeSys,
		d.RuntimeNumGC,
		time.Duration(d.RuntimePauseTotal),
		time.Duration(d.RuntimeLastPauseNs),
	)
}

// ArchiveFilterFPStats summarizes direct Cuckoo filter false-positive sampling.
type ArchiveFilterFPStats struct {
	BucketCount     int64
	SampledBuckets  int64
	Samples         int64 // Backward-compatible alias for NegativeQueries.
	FilterChecks    int64
	FilterPositives int64
	PositiveQueries int64
	TruePositives   int64
	NegativeQueries int64
	FalsePositives  int64
	Rate            float64
	Groups          []ArchiveFilterFPGroup

	groups map[string]*ArchiveFilterFPGroup
}

// ArchiveFilterFPGroup separates the synthetic workload by the factors that
// most directly affect Cuckoo-filter pressure.
type ArchiveFilterFPGroup struct {
	Dimension       string
	Group           string
	BucketCount     int64
	SampledBuckets  int64
	FilterChecks    int64
	FilterPositives int64
	PositiveQueries int64
	TruePositives   int64
	NegativeQueries int64
	FalsePositives  int64
	Rate            float64
}

// SampleArchiveFilterFalsePositives samples non-member suffixes directly against
// archive bucket filters. It is a diagnostic helper and does not affect roots.
func (t *Trie) SampleArchiveFilterFalsePositives(samplesPerBucket int, seed int64) *ArchiveFilterFPStats {
	stats := &ArchiveFilterFPStats{groups: make(map[string]*ArchiveFilterFPGroup)}
	if t == nil || samplesPerBucket <= 0 {
		return stats
	}
	rng := rand.New(rand.NewSource(seed))

	t.shardsMu.RLock()
	shards := make([]*Shard, len(t.shards))
	copy(shards, t.shards)
	t.shardsMu.RUnlock()

	seen := make(map[int]struct{}, len(shards))
	for i, shard := range shards {
		if shard == nil {
			continue
		}
		seen[i] = struct{}{}
		shard.sampleArchiveFilterFPIsolated(stats, rng, samplesPerBucket)
	}

	if roots, err := t.allShardRoots(); err == nil {
		for id, root := range roots {
			if id < 0 || id >= len(shards) {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			shardID := id
			shard := newStatsShardView(shardID, t.db, t.hasher, t.config, nil, root, t.pruning, func() byte {
				if shardID < t.pruneShardIdx {
					return t.globalEpochBit
				}
				return t.globalEpochBit ^ 1
			})
			shard.sampleArchiveFilterFP(stats, rng, samplesPerBucket)
		}
	}

	stats.Samples = stats.NegativeQueries
	if stats.NegativeQueries > 0 {
		stats.Rate = float64(stats.FalsePositives) / float64(stats.NegativeQueries)
	}
	stats.finalizeGroups()
	return stats
}

func (s *Shard) sampleArchiveFilterFPIsolated(stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	s.sampleArchiveFilterFPWithCache(stats, rng, samplesPerBucket, nil)
}

func (s *Shard) sampleArchiveFilterFP(stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	s.sampleArchiveFilterFPWithCache(stats, rng, samplesPerBucket, s.nodeCache)
}

func (s *Shard) sampleArchiveFilterFPWithCache(stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int, nodeCache *nodeBlobCache) {
	if s == nil || stats == nil || rng == nil || samplesPerBucket <= 0 {
		return
	}
	s.mu.RLock()
	root := s.root
	rootHash := append([]byte(nil), s.rootHash...)
	s.mu.RUnlock()

	if root == nil && len(rootHash) > 0 {
		view := newStatsShardView(s.id, s.db, s.hasher, s.config, nodeCache, rootHash, s.pruning, s.globalEpochBit)
		loaded, err := view.loadNodeAtPath(rootHash, nil, 0)
		if err == nil && loaded != nil {
			view.sampleArchiveFilterFPNode(loaded, nil, 0, stats, rng, samplesPerBucket)
		}
		return
	}
	s.sampleArchiveFilterFPNode(root, nil, 0, stats, rng, samplesPerBucket)
}

func (s *Shard) sampleArchiveFilterFPNode(node Node, path []byte, pathBits int, stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *InternalNode:
		for _, bucket := range n.StubList {
			s.sampleArchiveFilterFPBucket(bucket, stats, rng, samplesPerBucket)
		}
		leftPath, leftBits := s.childStoragePath(path, pathBits, n, 0)
		if n.Left != nil {
			s.sampleArchiveFilterFPNode(n.Left, leftPath, leftBits, stats, rng, samplesPerBucket)
		} else if len(n.LeftHash) > 0 {
			loaded, _ := s.loadNodeAtPath(n.LeftHash, leftPath, leftBits)
			if loaded != nil {
				s.sampleArchiveFilterFPNode(loaded, leftPath, leftBits, stats, rng, samplesPerBucket)
			}
		}
		rightPath, rightBits := s.childStoragePath(path, pathBits, n, 1)
		if n.Right != nil {
			s.sampleArchiveFilterFPNode(n.Right, rightPath, rightBits, stats, rng, samplesPerBucket)
		} else if len(n.RightHash) > 0 {
			loaded, _ := s.loadNodeAtPath(n.RightHash, rightPath, rightBits)
			if loaded != nil {
				s.sampleArchiveFilterFPNode(loaded, rightPath, rightBits, stats, rng, samplesPerBucket)
			}
		}
	case *ArchiveBucketNode:
		s.sampleArchiveFilterFPBucket(n, stats, rng, samplesPerBucket)
	}
}

func (s *Shard) sampleArchiveFilterFPBucket(bucket *ArchiveBucketNode, stats *ArchiveFilterFPStats, rng *rand.Rand, samplesPerBucket int) {
	if bucket == nil || bucket.Count == 0 {
		return
	}
	stats.BucketCount++
	filter := s.archiveBucketFilter(bucket)
	if filter == nil {
		return
	}
	keys, err := s.bucketKeys(bucket)
	if err != nil || len(keys) == 0 {
		return
	}
	existing := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		existing[string(archiveItemKey(key.SuffixBits, key.Suffix))] = struct{}{}
	}

	stats.SampledBuckets++
	groups := stats.filterGroups(s.id, bucket)
	for _, group := range groups {
		group.SampledBuckets++
	}
	knownMember := archiveItemKey(keys[0].SuffixBits, keys[0].Suffix)
	stats.PositiveQueries++
	stats.FilterChecks++
	for _, group := range groups {
		group.PositiveQueries++
		group.FilterChecks++
	}
	if filter.Lookup(knownMember) {
		stats.TruePositives++
		stats.FilterPositives++
		for _, group := range groups {
			group.TruePositives++
			group.FilterPositives++
		}
	}
	for i := 0; i < samplesPerBucket; i++ {
		template := keys[rng.Intn(len(keys))]
		keyWithLen, ok := randomNonMemberArchiveSuffix(rng, template.SuffixBits, existing)
		if !ok {
			continue
		}
		stats.NegativeQueries++
		stats.FilterChecks++
		for _, group := range groups {
			group.NegativeQueries++
			group.FilterChecks++
		}
		if filter.Lookup(keyWithLen) {
			stats.FalsePositives++
			stats.FilterPositives++
			for _, group := range groups {
				group.FalsePositives++
				group.FilterPositives++
			}
		}
	}
}

func (s *ArchiveFilterFPStats) filterGroups(shardID int, bucket *ArchiveBucketNode) []*ArchiveFilterFPGroup {
	density := archiveFilterDensityGroup(bucket.Count)
	depth := fmt.Sprintf("%d-%d", bucket.PathBits/32*32, bucket.PathBits/32*32+31)
	shard := fmt.Sprintf("%d", shardID)
	groups := []*ArchiveFilterFPGroup{
		s.filterGroup("density", density),
		s.filterGroup("path_depth", depth),
		s.filterGroup("shard", shard),
	}
	for _, group := range groups {
		group.BucketCount++
	}
	return groups
}

func (s *ArchiveFilterFPStats) filterGroup(dimension, group string) *ArchiveFilterFPGroup {
	key := dimension + "\x00" + group
	if current := s.groups[key]; current != nil {
		return current
	}
	current := &ArchiveFilterFPGroup{Dimension: dimension, Group: group}
	s.groups[key] = current
	return current
}

func (s *ArchiveFilterFPStats) finalizeGroups() {
	if len(s.groups) == 0 {
		return
	}
	s.Groups = make([]ArchiveFilterFPGroup, 0, len(s.groups))
	for _, group := range s.groups {
		if group.NegativeQueries > 0 {
			group.Rate = float64(group.FalsePositives) / float64(group.NegativeQueries)
		}
		s.Groups = append(s.Groups, *group)
	}
	sort.Slice(s.Groups, func(i, j int) bool {
		if s.Groups[i].Dimension != s.Groups[j].Dimension {
			return s.Groups[i].Dimension < s.Groups[j].Dimension
		}
		return s.Groups[i].Group < s.Groups[j].Group
	})
	s.groups = nil
}

func archiveFilterDensityGroup(count uint64) string {
	switch {
	case count <= 10:
		return "1-10"
	case count <= 30:
		return "11-30"
	case count <= 60:
		return "31-60"
	case count <= 100:
		return "61-100"
	default:
		return "101+"
	}
}

func randomNonMemberArchiveSuffix(rng *rand.Rand, suffixBits int, existing map[string]struct{}) ([]byte, bool) {
	if suffixBits < 0 {
		return nil, false
	}
	byteLen := (suffixBits + 7) / 8
	for attempt := 0; attempt < 32; attempt++ {
		suffix := make([]byte, byteLen)
		for i := range suffix {
			suffix[i] = byte(rng.Intn(256))
		}
		if rem := suffixBits % 8; rem != 0 && len(suffix) > 0 {
			suffix[len(suffix)-1] &= byte(0xff << (8 - rem))
		}
		keyWithLen := archiveItemKey(suffixBits, suffix)
		if _, ok := existing[string(keyWithLen)]; !ok {
			return keyWithLen, true
		}
	}
	return nil, false
}

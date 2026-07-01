package archive

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// CommitDiagnostics is a snapshot of the most recent ASCT commit breakdown.
type CommitDiagnostics struct {
	TotalNanos         int64
	CommitToBatchNanos int64
	ShardCommitNanos   int64
	TopTreeNanos       int64
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

	TopTreeDirtyChildren        int64
	TopTreeMaxChildPrefix       int64
	TopTreeMaxChildComputeNanos int64
	TopTreeMaxChildOps          int64
	TopTreeMaxChildBytes        int64
	TopTreeMaxApplyPrefix       int64
	TopTreeMaxApplyNanos        int64
	TopTreeOps                  int64
	TopTreeBytes                int64

	RawBatchOps        int64
	RawBatchBytes      int64
	RawShardMaxID      int64
	RawShardMaxOps     int64
	RawShardMaxBytes   int64
	NodeCacheEntries   int64
	NodeCacheBytes     int64
	RuntimeHeapAlloc   int64
	RuntimeHeapSys     int64
	RuntimeHeapInuse   int64
	RuntimeSys         int64
	RuntimeNumGC       int64
	RuntimePauseTotal  int64
	RuntimeLastPauseNs int64
}

// PrunePressureDiagnostics captures the pressure of the most recent pruned
// shard and the heaviest shard seen since the last reset. The detailed count
// fields are populated when path diagnostics are enabled.
type PrunePressureDiagnostics struct {
	LastShardID        int64
	LastTotalNanos     int64
	LastWaitNanos      int64
	LastShardNanos     int64
	LastPrefetchNanos  int64
	LastInternalVisits int64
	LastHotSkips       int64
	LastChildHits      int64
	LastChildSkips     int64
	LastBulkCollects   int64
	LastLeaves         int64
	LastStubs          int64
	LastBuildItems     int64
	LastBuildBuckets   int64
	LastParallelBuilds int64

	MaxShardID        int64
	MaxTotalNanos     int64
	MaxWaitNanos      int64
	MaxShardNanos     int64
	MaxPrefetchNanos  int64
	MaxInternalVisits int64
	MaxHotSkips       int64
	MaxChildHits      int64
	MaxChildSkips     int64
	MaxBulkCollects   int64
	MaxLeaves         int64
	MaxStubs          int64
	MaxBuildItems     int64
	MaxBuildBuckets   int64
	MaxParallelBuilds int64
}

func (d PrunePressureDiagnostics) String() string {
	return fmt.Sprintf(
		"pruneLastShardID=%d pruneLastTotal=%v pruneLastShard=%v pruneLastLeaves=%d pruneLastStubs=%d pruneLastBuildItems=%d pruneLastBuildBuckets=%d pruneLastInternalVisits=%d pruneMaxShardID=%d pruneMaxTotal=%v pruneMaxShard=%v pruneMaxLeaves=%d pruneMaxStubs=%d pruneMaxBuildItems=%d pruneMaxBuildBuckets=%d pruneMaxInternalVisits=%d",
		d.LastShardID,
		time.Duration(d.LastTotalNanos),
		time.Duration(d.LastShardNanos),
		d.LastLeaves,
		d.LastStubs,
		d.LastBuildItems,
		d.LastBuildBuckets,
		d.LastInternalVisits,
		d.MaxShardID,
		time.Duration(d.MaxTotalNanos),
		time.Duration(d.MaxShardNanos),
		d.MaxLeaves,
		d.MaxStubs,
		d.MaxBuildItems,
		d.MaxBuildBuckets,
		d.MaxInternalVisits,
	)
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
		"shardTotal=%v shardLockWait=%v shardRootCommit=%v shardCommitSerialize=%v shardCommitHash=%v shardCommitPersist=%v shardCommitBatchPut=%v shardCommitCache=%v shardCommitBookkeep=%v shardCommitMaxNode=%v shardCommitMaxNodeType=%d shardCommitMaxNodeBytes=%d shardStaleDeletes=%v shardPendingValues=%v shardNodeCount=%d shardStaleSetLen=%d shardPendingValuesCount=%d shardPendingFlatValuesCount=%d shardRootNil=%v",
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
	serializeNanos int64
	hashNanos      int64
	persistNanos   int64
	batchPutNanos  int64
	cacheNanos     int64
	bookkeepNanos  int64
	maxNodeNanos   int64
	maxNodeType    int64
	maxNodeBytes   int64
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
}

type pruneCounterSnapshot struct {
	internalVisits int64
	hotSkips       int64
	childHits      int64
	childSkips     int64
	bulkCollects   int64
	leaves         int64
	stubs          int64
	buildItems     int64
	buildBuckets   int64
	parallelBuilds int64
}

var (
	commitDiagTotalNanos                 int64
	commitDiagCommitToBatchNanos         int64
	commitDiagShardCommitNanos           int64
	commitDiagTopTreeNanos               int64
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
	commitDiagTopTreeDirtyChildren       int64
	commitDiagTopTreeMaxChildPrefix      int64
	commitDiagTopTreeMaxChildCompute     int64
	commitDiagTopTreeMaxChildOps         int64
	commitDiagTopTreeMaxChildBytes       int64
	commitDiagTopTreeMaxApplyPrefix      int64
	commitDiagTopTreeMaxApplyNanos       int64
	commitDiagTopTreeOps                 int64
	commitDiagTopTreeBytes               int64
	commitDiagRawBatchOps                int64
	commitDiagRawBatchBytes              int64
	commitDiagRawShardMaxID              int64
	commitDiagRawShardMaxOps             int64
	commitDiagRawShardMaxBytes           int64
	commitDiagNodeCacheEntries           int64
	commitDiagNodeCacheBytes             int64
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
	atomic.StoreInt64(&commitDiagTopTreeNanos, 0)
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
	atomic.StoreInt64(&commitDiagTopTreeDirtyChildren, 0)
	atomic.StoreInt64(&commitDiagTopTreeMaxChildPrefix, 0)
	atomic.StoreInt64(&commitDiagTopTreeMaxChildCompute, 0)
	atomic.StoreInt64(&commitDiagTopTreeMaxChildOps, 0)
	atomic.StoreInt64(&commitDiagTopTreeMaxChildBytes, 0)
	atomic.StoreInt64(&commitDiagTopTreeMaxApplyPrefix, 0)
	atomic.StoreInt64(&commitDiagTopTreeMaxApplyNanos, 0)
	atomic.StoreInt64(&commitDiagTopTreeOps, 0)
	atomic.StoreInt64(&commitDiagTopTreeBytes, 0)
	atomic.StoreInt64(&commitDiagRawBatchOps, 0)
	atomic.StoreInt64(&commitDiagRawBatchBytes, 0)
	atomic.StoreInt64(&commitDiagRawShardMaxID, 0)
	atomic.StoreInt64(&commitDiagRawShardMaxOps, 0)
	atomic.StoreInt64(&commitDiagRawShardMaxBytes, 0)
	atomic.StoreInt64(&commitDiagNodeCacheEntries, 0)
	atomic.StoreInt64(&commitDiagNodeCacheBytes, 0)
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

func recordCommitToBatchDiagnostics(shardCommit, topTree int64) {
	atomic.StoreInt64(&commitDiagShardCommitNanos, shardCommit)
	atomic.StoreInt64(&commitDiagTopTreeNanos, topTree)
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
	}
}

func recordPruneShardPressure(shardID int, total, wait, shard, prefetch int64, before, after pruneCounterSnapshot) {
	current := PrunePressureDiagnostics{
		LastShardID:        int64(shardID),
		LastTotalNanos:     total,
		LastWaitNanos:      wait,
		LastShardNanos:     shard,
		LastPrefetchNanos:  prefetch,
		LastInternalVisits: after.internalVisits - before.internalVisits,
		LastHotSkips:       after.hotSkips - before.hotSkips,
		LastChildHits:      after.childHits - before.childHits,
		LastChildSkips:     after.childSkips - before.childSkips,
		LastBulkCollects:   after.bulkCollects - before.bulkCollects,
		LastLeaves:         after.leaves - before.leaves,
		LastStubs:          after.stubs - before.stubs,
		LastBuildItems:     after.buildItems - before.buildItems,
		LastBuildBuckets:   after.buildBuckets - before.buildBuckets,
		LastParallelBuilds: after.parallelBuilds - before.parallelBuilds,
	}

	prunePressureMu.Lock()
	defer prunePressureMu.Unlock()
	prunePressureDiag.LastShardID = current.LastShardID
	prunePressureDiag.LastTotalNanos = current.LastTotalNanos
	prunePressureDiag.LastWaitNanos = current.LastWaitNanos
	prunePressureDiag.LastShardNanos = current.LastShardNanos
	prunePressureDiag.LastPrefetchNanos = current.LastPrefetchNanos
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
	if prunePressureMaxSet && current.LastShardNanos <= prunePressureDiag.MaxShardNanos {
		return
	}
	prunePressureMaxSet = true
	prunePressureDiag.MaxShardID = current.LastShardID
	prunePressureDiag.MaxTotalNanos = current.LastTotalNanos
	prunePressureDiag.MaxWaitNanos = current.LastWaitNanos
	prunePressureDiag.MaxShardNanos = current.LastShardNanos
	prunePressureDiag.MaxPrefetchNanos = current.LastPrefetchNanos
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
func RecordWrapperCommitDiagnostics(total, shardCommit, topTree, adapterMerge, trieDBUpdate, batchWrite time.Duration, dirtyShards int) {
	atomic.StoreInt64(&commitDiagTotalNanos, total.Nanoseconds())
	atomic.StoreInt64(&commitDiagCommitToBatchNanos, total.Nanoseconds())
	atomic.StoreInt64(&commitDiagShardCommitNanos, shardCommit.Nanoseconds())
	atomic.StoreInt64(&commitDiagTopTreeNanos, topTree.Nanoseconds())
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

func recordTopTreeDetailDiagnostics(dirtyChildren, maxChildPrefix int, maxChildCompute time.Duration, maxChildOps, maxChildBytes int, maxApplyPrefix int, maxApply time.Duration, totalOps, totalBytes int) {
	atomic.StoreInt64(&commitDiagTopTreeDirtyChildren, int64(dirtyChildren))
	atomic.StoreInt64(&commitDiagTopTreeMaxChildPrefix, int64(maxChildPrefix))
	atomic.StoreInt64(&commitDiagTopTreeMaxChildCompute, maxChildCompute.Nanoseconds())
	atomic.StoreInt64(&commitDiagTopTreeMaxChildOps, int64(maxChildOps))
	atomic.StoreInt64(&commitDiagTopTreeMaxChildBytes, int64(maxChildBytes))
	atomic.StoreInt64(&commitDiagTopTreeMaxApplyPrefix, int64(maxApplyPrefix))
	atomic.StoreInt64(&commitDiagTopTreeMaxApplyNanos, maxApply.Nanoseconds())
	atomic.StoreInt64(&commitDiagTopTreeOps, int64(totalOps))
	atomic.StoreInt64(&commitDiagTopTreeBytes, int64(totalBytes))
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

// LastCommitDiagnostics returns the most recent ASCT commit diagnostic snapshot.
func LastCommitDiagnostics() CommitDiagnostics {
	return CommitDiagnostics{
		TotalNanos:                 atomic.LoadInt64(&commitDiagTotalNanos),
		CommitToBatchNanos:         atomic.LoadInt64(&commitDiagCommitToBatchNanos),
		ShardCommitNanos:           atomic.LoadInt64(&commitDiagShardCommitNanos),
		TopTreeNanos:               atomic.LoadInt64(&commitDiagTopTreeNanos),
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
		TopTreeDirtyChildren:       atomic.LoadInt64(&commitDiagTopTreeDirtyChildren),
		TopTreeMaxChildPrefix:      atomic.LoadInt64(&commitDiagTopTreeMaxChildPrefix),
		TopTreeMaxChildComputeNanos: atomic.LoadInt64(
			&commitDiagTopTreeMaxChildCompute,
		),
		TopTreeMaxChildOps:    atomic.LoadInt64(&commitDiagTopTreeMaxChildOps),
		TopTreeMaxChildBytes:  atomic.LoadInt64(&commitDiagTopTreeMaxChildBytes),
		TopTreeMaxApplyPrefix: atomic.LoadInt64(&commitDiagTopTreeMaxApplyPrefix),
		TopTreeMaxApplyNanos:  atomic.LoadInt64(&commitDiagTopTreeMaxApplyNanos),
		TopTreeOps:            atomic.LoadInt64(&commitDiagTopTreeOps),
		TopTreeBytes:          atomic.LoadInt64(&commitDiagTopTreeBytes),
		RawBatchOps:           atomic.LoadInt64(&commitDiagRawBatchOps),
		RawBatchBytes:         atomic.LoadInt64(&commitDiagRawBatchBytes),
		RawShardMaxID:         atomic.LoadInt64(&commitDiagRawShardMaxID),
		RawShardMaxOps:        atomic.LoadInt64(&commitDiagRawShardMaxOps),
		RawShardMaxBytes:      atomic.LoadInt64(&commitDiagRawShardMaxBytes),
		NodeCacheEntries:      atomic.LoadInt64(&commitDiagNodeCacheEntries),
		NodeCacheBytes:        atomic.LoadInt64(&commitDiagNodeCacheBytes),
		RuntimeHeapAlloc:      atomic.LoadInt64(&commitDiagRuntimeHeapAlloc),
		RuntimeHeapSys:        atomic.LoadInt64(&commitDiagRuntimeHeapSys),
		RuntimeHeapInuse:      atomic.LoadInt64(&commitDiagRuntimeHeapInuse),
		RuntimeSys:            atomic.LoadInt64(&commitDiagRuntimeSys),
		RuntimeNumGC:          atomic.LoadInt64(&commitDiagRuntimeNumGC),
		RuntimePauseTotal:     atomic.LoadInt64(&commitDiagRuntimePauseTotal),
		RuntimeLastPauseNs:    atomic.LoadInt64(&commitDiagRuntimeLastPauseNs),
	}
}

func (d CommitDiagnostics) String() string {
	return fmt.Sprintf(
		"total=%v commitToBatch=%v shardCommit=%v topTree=%v batchWrite=%v adapterMerge=%v trieDBUpdate=%v pruneTotal=%v pruneWait=%v pruneShard=%v prunePrefetchStart=%v dirtyShards=%d nodeCacheHits=%d nodeCacheMisses=%d pathNodeDBGets=%d archivePromotionChecks=%d archivePromotionHits=%d bucketRecomputes=%d commitmentPointCacheHits=%d commitmentPointCacheMisses=%d pruneInternalVisits=%d pruneHotSkips=%d pruneChildHits=%d pruneChildSkips=%d pruneBulkCollects=%d pruneCollectedLeaves=%d pruneCollectedStubs=%d pruneBuildItems=%d pruneBuildBuckets=%d pruneArchiveBuildParallels=%d shardMaxID=%d shardMaxCommit=%v shardMaxLockWait=%v shardMaxRootCommit=%v shardMaxSerialize=%v shardMaxHash=%v shardMaxPersist=%v shardMaxBatchPut=%v shardMaxCache=%v shardMaxBookkeep=%v shardMaxNode=%v shardMaxNodeType=%d shardMaxNodeBytes=%d shardMaxStaleDeletes=%v shardMaxPendingValues=%v shardMaxNodeCount=%d shardMaxStaleSetLen=%d shardMaxPendingValuesCount=%d shardMaxPendingFlatValuesCount=%d topTreeDirtyChildren=%d topTreeMaxChildPrefix=%d topTreeMaxChildCompute=%v topTreeMaxChildOps=%d topTreeMaxChildBytes=%d topTreeMaxApplyPrefix=%d topTreeMaxApply=%v topTreeOps=%d topTreeBytes=%d rawBatchOps=%d rawBatchBytes=%d rawShardMaxID=%d rawShardMaxOps=%d rawShardMaxBytes=%d nodeCacheEntries=%d nodeCacheBytes=%d heapAlloc=%d heapSys=%d heapInuse=%d runtimeSys=%d numGC=%d pauseTotal=%v lastPause=%v",
		time.Duration(d.TotalNanos),
		time.Duration(d.CommitToBatchNanos),
		time.Duration(d.ShardCommitNanos),
		time.Duration(d.TopTreeNanos),
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
		d.TopTreeDirtyChildren,
		d.TopTreeMaxChildPrefix,
		time.Duration(d.TopTreeMaxChildComputeNanos),
		d.TopTreeMaxChildOps,
		d.TopTreeMaxChildBytes,
		d.TopTreeMaxApplyPrefix,
		time.Duration(d.TopTreeMaxApplyNanos),
		d.TopTreeOps,
		d.TopTreeBytes,
		d.RawBatchOps,
		d.RawBatchBytes,
		d.RawShardMaxID,
		d.RawShardMaxOps,
		d.RawShardMaxBytes,
		d.NodeCacheEntries,
		d.NodeCacheBytes,
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
	BucketCount    int64
	SampledBuckets int64
	Samples        int64
	FalsePositives int64
	Rate           float64
}

// SampleArchiveFilterFalsePositives samples non-member suffixes directly against
// archive bucket filters. It is a diagnostic helper and does not affect roots.
func (t *Trie) SampleArchiveFilterFalsePositives(samplesPerBucket int, seed int64) *ArchiveFilterFPStats {
	stats := &ArchiveFilterFPStats{}
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

	if t.topTree != nil {
		roots := make(map[int][]byte)
		t.topTree.ForEachShardRoot(func(id int, hash []byte) {
			if id < 0 || id >= len(shards) {
				return
			}
			if _, ok := seen[id]; ok {
				return
			}
			roots[id] = hash
		})
		for id, root := range roots {
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

	if stats.Samples > 0 {
		stats.Rate = float64(stats.FalsePositives) / float64(stats.Samples)
	}
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
	for i := 0; i < samplesPerBucket; i++ {
		template := keys[rng.Intn(len(keys))]
		keyWithLen, ok := randomNonMemberArchiveSuffix(rng, template.SuffixBits, existing)
		if !ok {
			continue
		}
		stats.Samples++
		if filter.Lookup(keyWithLen) {
			stats.FalsePositives++
		}
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

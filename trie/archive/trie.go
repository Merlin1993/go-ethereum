package archive

import (
	"bytes"
	"runtime"
	"sync"
	"time"
)

// Trie represents a Binary Merkle Patricia Trie that shards its key space.
type Trie struct {
	db     KVStore
	hasher Hasher
	config *Config

	shards            []*Shard
	shardsMu          sync.RWMutex
	nodeCache         *nodeBlobCache
	pointCache        *commitmentPointCache
	prunePrefetchMu   sync.Mutex
	prunePrefetchIdx  int
	prunePrefetchDone chan struct{}
	asyncPruneMu      sync.Mutex
	asyncPrune        *asyncPruneJob

	// TopTree handles the hierarchical root hash computation
	topTree *TopTree

	// Epoch management for pruning
	pruneShardIdx  int
	globalEpochBit byte
	pruning        bool

	dirtyShards        map[int]struct{}
	archiveDirtyShards map[int]struct{}
	dirtyShardList     []int
	archiveShardList   []int
}

type asyncPruneJob struct {
	idx           int
	epoch         byte
	done          chan struct{}
	err           error
	pruneNanos    int64
	before        pruneCounterSnapshot
	after         pruneCounterSnapshot
	launchNanos   int64
	prefetchNanos int64
}

// KeyValue is one ordered trie write used by PutBatch.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// NewTrie creates a new Archive Trie with the given database and configuration.
func NewTrie(root []byte, db KVStore, hasher Hasher, config *Config, pruning bool) *Trie {
	if config == nil {
		config = DefaultConfig()
	}
	t := &Trie{
		db:                 db,
		hasher:             hasher,
		config:             config,
		shards:             make([]*Shard, 1<<config.ShardDepth),
		nodeCache:          newNodeBlobCacheWithBytesLimit(config.NodeCacheLimit, config.NodeCacheBytesLimit),
		pointCache:         newCommitmentPointCache(config.CommitmentPointCacheLimit),
		prunePrefetchIdx:   -1,
		globalEpochBit:     0,
		pruning:            pruning,
		dirtyShards:        make(map[int]struct{}),
		archiveDirtyShards: make(map[int]struct{}),
	}
	t.topTree = NewTopTree(hasher, nil, config.ShardDepth, config.UsePathStorage())

	if len(root) > 0 {
		t.Load(root)
	}
	return t
}

func (t *Trie) SetFlatReader(reader FlatValueReader) {
	if t.config != nil {
		t.config.FlatReader = reader
	}
}

func (t *Trie) GetDirtyShards() []int {
	t.shardsMu.RLock()
	defer t.shardsMu.RUnlock()
	list := make([]int, 0, len(t.dirtyShards))
	list = append(list, t.dirtyShardList...)
	return list
}

func (t *Trie) GetShardRoot(id int) []byte {
	shardRoot, _ := t.topTree.GetShardRoot(id, t.db)
	return shardRoot
}

func (t *Trie) CommitShardToBatch(id int, batch Batcher, destructive bool) ([]byte, error) {
	if err := t.finishAsyncPruneForShard(id); err != nil {
		return nil, err
	}
	shard, err := t.getOrCreateShard(id)
	if err != nil {
		return nil, err
	}
	return shard.CommitToBatch(batch, destructive)
}

func (t *Trie) CommitShardToBatchWithDiagnostics(id int, batch Batcher, destructive bool) ([]byte, ShardCommitDiagnostics, error) {
	if err := t.finishAsyncPruneForShard(id); err != nil {
		return nil, ShardCommitDiagnostics{}, err
	}
	shard, err := t.getOrCreateShard(id)
	if err != nil {
		return nil, ShardCommitDiagnostics{}, err
	}
	return shard.CommitToBatchWithDiagnostics(batch, destructive)
}

func (t *Trie) CommitTopTreeToBatch(shardRoots map[int][]byte, dirtyShards []int, batch Batcher) ([]byte, error) {
	rootHash, err := t.topTree.Compute(shardRoots, dirtyShards, batch)
	if err != nil {
		return nil, err
	}
	t.shardsMu.Lock()
	for _, id := range dirtyShards {
		delete(t.dirtyShards, id)
	}
	t.filterDirtyShardListLocked()
	t.shardsMu.Unlock()
	return rootHash, nil
}

func (t *Trie) GetShardID(key []byte) int {
	bits := t.config.ShardDepth
	if bits <= 0 {
		return 0
	}
	id := 0
	for i := 0; i < bits; i++ {
		byteIdx := i / 8
		bitIdx := 7 - (i % 8)
		if byteIdx < len(key) {
			if (key[byteIdx] & (1 << bitIdx)) != 0 {
				id |= (1 << (bits - 1 - i))
			}
		}
	}
	return id
}

func (t *Trie) markDirtyShard(id int) {
	t.shardsMu.Lock()
	t.markDirtyShardLocked(id)
	t.shardsMu.Unlock()
}

func (t *Trie) markArchiveDirtyShard(id int) {
	t.shardsMu.Lock()
	t.markArchiveDirtyShardLocked(id)
	t.shardsMu.Unlock()
}

func (t *Trie) markDirtyShardLocked(id int) {
	if _, ok := t.dirtyShards[id]; ok {
		return
	}
	t.dirtyShards[id] = struct{}{}
	t.dirtyShardList = append(t.dirtyShardList, id)
}

func (t *Trie) markArchiveDirtyShardLocked(id int) {
	if _, ok := t.archiveDirtyShards[id]; ok {
		return
	}
	t.archiveDirtyShards[id] = struct{}{}
	t.archiveShardList = append(t.archiveShardList, id)
}

func (t *Trie) filterDirtyShardListLocked() {
	out := t.dirtyShardList[:0]
	for _, id := range t.dirtyShardList {
		if _, ok := t.dirtyShards[id]; ok {
			out = append(out, id)
		}
	}
	t.dirtyShardList = out
}

func (t *Trie) filterArchiveShardListLocked() {
	out := t.archiveShardList[:0]
	for _, id := range t.archiveShardList {
		if _, ok := t.archiveDirtyShards[id]; ok {
			out = append(out, id)
		}
	}
	t.archiveShardList = out
}

func (t *Trie) finishAsyncPruneForShard(id int) error {
	t.asyncPruneMu.Lock()
	job := t.asyncPrune
	pending := job != nil && job.idx == id
	t.asyncPruneMu.Unlock()
	if !pending {
		return nil
	}
	return t.finishAsyncPrune()
}

// FinishAsyncPrune applies a pending asynchronous prune before callers read or
// commit roots.
func (t *Trie) FinishAsyncPrune() error {
	return t.finishAsyncPrune()
}

func (t *Trie) finishAsyncPrune() error {
	t.asyncPruneMu.Lock()
	job := t.asyncPrune
	t.asyncPruneMu.Unlock()
	if job == nil {
		return nil
	}

	waitStart := time.Now()
	<-job.done
	waitNanos := time.Since(waitStart).Nanoseconds()

	t.asyncPruneMu.Lock()
	if t.asyncPrune != job {
		t.asyncPruneMu.Unlock()
		return nil
	}
	if job.err == nil {
		t.pruneShardIdx = (job.idx + 1) % (1 << t.config.ShardDepth)
	}
	t.asyncPrune = nil
	t.asyncPruneMu.Unlock()

	prefetchNanos := job.prefetchNanos
	if job.err == nil {
		prefetchStart := time.Now()
		t.startPrunePrefetch(t.pruneShardIdx)
		prefetchNanos += time.Since(prefetchStart).Nanoseconds()
	}
	totalNanos := job.launchNanos + waitNanos
	recordPruneDiagnostics(totalNanos, waitNanos, job.pruneNanos, prefetchNanos)
	recordPruneShardPressure(job.idx, totalNanos, waitNanos, job.pruneNanos, prefetchNanos, job.before, job.after)
	return job.err
}

// Load loads the Trie state from the database using a given root hash.
func (t *Trie) Load(rootHash []byte) error {
	if len(rootHash) == 0 {
		return nil
	}
	if err := t.finishAsyncPrune(); err != nil {
		return err
	}

	// If current root is already what we're loading, skip reset
	current, _ := t.Hash()
	if bytes.Equal(current, rootHash) {
		return nil
	}

	// Load the TopTree from DB
	if err := t.topTree.Load(rootHash, t.db); err != nil {
		return err
	}

	// [FIX] Invalidate all loaded shards so they pick up new roots from TopTree on next access
	t.shardsMu.Lock()
	defer t.shardsMu.Unlock()
	for i, s := range t.shards {
		if s != nil {
			shardRoot, _ := t.topTree.GetShardRoot(i, t.db)
			s.Reset(shardRoot)
		}
	}
	return nil
}

func (t *Trie) getOrCreateShard(id int) (*Shard, error) {
	t.shardsMu.RLock()
	shard := t.shards[id]
	t.shardsMu.RUnlock()

	if shard != nil {
		return shard, nil
	}

	t.shardsMu.Lock()
	defer t.shardsMu.Unlock()

	if t.shards[id] != nil {
		return t.shards[id], nil
	}

	// Try to get existing shard root from TopTree
	shardRoot, _ := t.topTree.GetShardRoot(id, t.db)

	s, err := newShard(id, t.db, t.hasher, t.config, shardRoot, t.pruning, func() byte {
		// [Rolling Epoch] 为了防止“提前加热”导致无法归档，每个分片只有在正式被剪枝后才切换到新的 GlobalBit。
		// 在当前周期内尚未被剪枝的分片应继续使用旧位的补。
		if id < t.pruneShardIdx {
			return t.globalEpochBit
		}
		return t.globalEpochBit ^ 1
	}, t.nodeCache, t.pointCache)
	if err != nil {
		return nil, err
	}
	t.shards[id] = s
	return s, nil
}

// Get finds the value for a given key.
func (t *Trie) Get(key []byte) ([]byte, error) {
	shardID := t.GetShardID(key)
	if err := t.finishAsyncPruneForShard(shardID); err != nil {
		return nil, err
	}
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}
	val, err := shard.Get(key)
	if err == nil && shard.HasPendingArchiveWrites() {
		t.markDirtyShard(shardID)
		t.markArchiveDirtyShard(shardID)
	}
	return val, err
}

// Put updates or inserts a value for a given key.
func (t *Trie) Put(key []byte, value []byte) error {
	shardID := t.GetShardID(key)
	if err := t.finishAsyncPruneForShard(shardID); err != nil {
		return err
	}
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.markDirtyShard(shardID)
	if err := shard.Put(key, value); err != nil {
		return err
	}
	if shard.HasPendingArchiveWrites() {
		t.markArchiveDirtyShard(shardID)
	}
	return nil
}

// PutBatch applies ordered writes in parallel across independent shards.
// Writes targeting the same shard retain their original order.
func (t *Trie) PutBatch(entries []KeyValue) error {
	if len(entries) == 0 {
		return nil
	}
	type shardWrites struct {
		id      int
		entries []KeyValue
	}
	groupIndex := make(map[int]int)
	groups := make([]shardWrites, 0)
	for _, entry := range entries {
		id := t.GetShardID(entry.Key)
		index, ok := groupIndex[id]
		if !ok {
			index = len(groups)
			groupIndex[id] = index
			groups = append(groups, shardWrites{id: id})
		}
		groups[index].entries = append(groups[index].entries, entry)
	}
	for _, group := range groups {
		if err := t.finishAsyncPruneForShard(group.id); err != nil {
			return err
		}
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(groups) {
		workers = len(groups)
	}
	jobs := make(chan int, len(groups))
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				group := groups[index]
				shard, err := t.getOrCreateShard(group.id)
				if err == nil {
					t.markDirtyShard(group.id)
					err = shard.PutBatch(group.entries)
					if err == nil && shard.HasPendingArchiveWrites() {
						t.markArchiveDirtyShard(group.id)
					}
				}
				errs[index] = err
			}
		}()
	}
	for index := range groups {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a key and its value from the Trie.
func (t *Trie) Delete(key []byte) error {
	shardID := t.GetShardID(key)
	if err := t.finishAsyncPruneForShard(shardID); err != nil {
		return err
	}
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.markDirtyShard(shardID)
	t.markArchiveDirtyShard(shardID)
	return shard.Delete(key)
}

// BatchDelete is an alias for Delete, used by some tests.
func (t *Trie) BatchDelete(key []byte) error {
	return t.Delete(key)
}

// Redundant GetShardID removed.

// Hash returns the root hash of the Trie.
func (t *Trie) Hash() ([]byte, error) {
	if err := t.finishAsyncPrune(); err != nil {
		return nil, err
	}
	shardRoots := make(map[int][]byte)
	dirtyShards := make([]int, 0)

	// We only need to compute roots for shards that have changed
	// or were previously loaded but not committed.
	// For simplicity, we can use dirtyShards here too, but Hash() doesn't clear them.
	t.shardsMu.RLock()
	for _, i := range t.dirtyShardList {
		shardRoots[i] = make([]byte, 32) // Default to empty
		s := t.shards[i]
		if s != nil {
			h, err := s.Hash()
			if err != nil {
				t.shardsMu.RUnlock()
				return nil, err
			}
			if len(h) > 0 {
				shardRoots[i] = h
				dirtyShards = append(dirtyShards, i)
			} else {
				dirtyShards = append(dirtyShards, i)
			}
		} else {
			dirtyShards = append(dirtyShards, i)
		}
	}
	t.shardsMu.RUnlock()

	return t.topTree.Compute(shardRoots, dirtyShards, nil)
}

// Commit persists any dirty shards to the database.
func (t *Trie) Commit() ([]byte, error) {
	totalStart := time.Now()
	if err := t.finishAsyncPrune(); err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), 0, 0, 0, 0, 0, 0)
		return nil, err
	}
	dirtyShards, archiveDirtyShards, flushShards := t.diagnosticCommitShardCounts()
	batch := t.db.NewBatch()
	defer batch.Reset()

	// Flush archives FIRST before committing shards (which might clear dirty set)
	flushStart := time.Now()
	if err := t.FlushArchives(); err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), time.Since(flushStart).Nanoseconds(), 0, 0, dirtyShards, archiveDirtyShards, flushShards)
		return nil, err
	}
	flushDuration := time.Since(flushStart)

	commitToBatchStart := time.Now()
	rootHash, err := t.CommitToBatch(batch, true)
	if err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), flushDuration.Nanoseconds(), time.Since(commitToBatchStart).Nanoseconds(), 0, dirtyShards, archiveDirtyShards, flushShards)
		return nil, err
	}
	commitToBatchDuration := time.Since(commitToBatchStart)

	batchWriteStart := time.Now()
	if err := batch.Write(); err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), flushDuration.Nanoseconds(), commitToBatchDuration.Nanoseconds(), time.Since(batchWriteStart).Nanoseconds(), dirtyShards, archiveDirtyShards, flushShards)
		return nil, err
	}
	batchWriteDuration := time.Since(batchWriteStart)
	recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), flushDuration.Nanoseconds(), commitToBatchDuration.Nanoseconds(), batchWriteDuration.Nanoseconds(), dirtyShards, archiveDirtyShards, flushShards)

	return rootHash, nil
}

func (t *Trie) diagnosticCommitShardCounts() (dirtyShards, archiveDirtyShards, flushShards int) {
	t.shardsMu.RLock()
	defer t.shardsMu.RUnlock()
	return len(t.dirtyShards), len(t.archiveDirtyShards), len(t.archiveDirtyShards)
}

// CommitToBatch commits the trie state to a given batcher.
func (t *Trie) CommitToBatch(batch Batcher, destructive bool) ([]byte, error) {
	if err := t.finishAsyncPrune(); err != nil {
		return nil, err
	}
	shardRoots := make(map[int][]byte)
	dirtyShards := make([]int, 0)
	shardCommitStart := time.Now()

	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		errs  []error
		errMu sync.Mutex
	)

	t.shardsMu.RLock()
	dirtyShardsList := append([]int(nil), t.dirtyShardList...)
	t.shardsMu.RUnlock()

	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers < 1 {
		numWorkers = 1
	}
	if numWorkers > len(dirtyShardsList) {
		numWorkers = len(dirtyShardsList)
	}

	// Parallel commit shards
	shardChan := make(chan int, len(dirtyShardsList))
	for _, i := range dirtyShardsList {
		shardChan <- i
	}
	close(shardChan)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Worker-local batch captures operation order, including stale deletes.
			workerBatch := &memBatcher{ops: make([]memBatchOp, 0, 5000)}
			localRoots := make(map[int][]byte)
			localDirtyShards := make([]int, 0)

			for i := range shardChan {
				s := t.shards[i]
				if s != nil {
					h, err := s.CommitToBatch(workerBatch, destructive)
					if err != nil {
						errMu.Lock()
						errs = append(errs, err)
						errMu.Unlock()
						return
					}
					if len(h) > 0 {
						localRoots[i] = h
					} else {
						localRoots[i] = make([]byte, 32)
					}
					localDirtyShards = append(localDirtyShards, i)
				} else {
					localRoots[i] = make([]byte, 32)
					localDirtyShards = append(localDirtyShards, i)
				}
			}

			// Merge back to global state
			mu.Lock()
			for shardID, root := range localRoots {
				shardRoots[shardID] = root
			}
			dirtyShards = append(dirtyShards, localDirtyShards...)
			for _, item := range workerBatch.ops {
				var err error
				if item.delete {
					err = batch.Delete(item.k)
				} else {
					err = batch.Put(item.k, item.v)
				}
				if err != nil {
					mu.Unlock()
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
					return
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		recordCommitToBatchDiagnostics(time.Since(shardCommitStart).Nanoseconds(), 0)
		return nil, errs[0]
	}
	shardCommitDuration := time.Since(shardCommitStart)

	topTreeStart := time.Now()
	rootHash, err := t.topTree.Compute(shardRoots, dirtyShards, batch)
	if err != nil {
		recordCommitToBatchDiagnostics(shardCommitDuration.Nanoseconds(), time.Since(topTreeStart).Nanoseconds())
		return nil, err
	}
	topTreeDuration := time.Since(topTreeStart)
	recordCommitToBatchDiagnostics(shardCommitDuration.Nanoseconds(), topTreeDuration.Nanoseconds())

	t.shardsMu.Lock()
	t.dirtyShards = make(map[int]struct{})
	t.dirtyShardList = nil
	t.shardsMu.Unlock()

	return rootHash, nil
}

const prunePrefetchNodeLimit = 512

func (t *Trie) waitPrunePrefetch(idx int) {
	t.prunePrefetchMu.Lock()
	if t.prunePrefetchIdx != idx || t.prunePrefetchDone == nil {
		t.prunePrefetchMu.Unlock()
		return
	}
	done := t.prunePrefetchDone
	t.prunePrefetchIdx = -1
	t.prunePrefetchDone = nil
	t.prunePrefetchMu.Unlock()
	<-done
}

func (t *Trie) startPrunePrefetch(idx int) {
	if t.config == nil || !t.config.UsePathStorage() || t.nodeCache == nil {
		return
	}
	root, _ := t.topTree.GetShardRoot(idx, t.db)
	if len(root) == 0 {
		return
	}
	done := make(chan struct{})
	epoch := t.globalEpochBit
	t.prunePrefetchMu.Lock()
	t.prunePrefetchIdx = idx
	t.prunePrefetchDone = done
	t.prunePrefetchMu.Unlock()

	go func(rootHash []byte) {
		defer close(done)
		shard := newStatsShardView(idx, t.db, t.hasher, t.config, t.nodeCache, rootHash, t.pruning, func() byte {
			return epoch
		})
		visited := 0
		shard.prefetchNodeAtPath(rootHash, nil, 0, &visited, prunePrefetchNodeLimit)
	}(append([]byte(nil), root...))
}

// PruneNextShard prunes the next shard in cycle.
func (t *Trie) PruneNextShard() error {
	totalStart := time.Now()
	if t.config != nil && t.config.AsyncPrune {
		if err := t.finishAsyncPrune(); err != nil {
			return err
		}
		idx := t.pruneShardIdx
		waitStart := time.Now()
		t.waitPrunePrefetch(idx)
		waitDur := time.Since(waitStart)
		if idx == 0 {
			t.globalEpochBit ^= 1
		}

		shard, err := t.getOrCreateShard(idx)
		if err != nil {
			recordPruneDiagnostics(time.Since(totalStart).Nanoseconds(), waitDur.Nanoseconds(), 0, 0)
			return err
		}

		t.shardsMu.Lock()
		t.markDirtyShardLocked(idx)
		t.markArchiveDirtyShardLocked(idx)
		t.shardsMu.Unlock()

		job := &asyncPruneJob{
			idx:         idx,
			epoch:       t.globalEpochBit,
			done:        make(chan struct{}),
			before:      snapshotPruneCounters(),
			launchNanos: time.Since(totalStart).Nanoseconds(),
		}
		t.asyncPruneMu.Lock()
		if t.asyncPrune != nil {
			t.asyncPruneMu.Unlock()
			return t.finishAsyncPrune()
		}
		t.asyncPrune = job
		t.asyncPruneMu.Unlock()

		go func() {
			pruneStart := time.Now()
			job.err = shard.Prune(job.epoch)
			job.pruneNanos = time.Since(pruneStart).Nanoseconds()
			job.after = snapshotPruneCounters()
			close(job.done)
		}()
		return nil
	}

	idx := t.pruneShardIdx
	waitStart := time.Now()
	t.waitPrunePrefetch(idx)
	waitDur := time.Since(waitStart)
	if idx == 0 {
		t.globalEpochBit ^= 1
	}

	shard, err := t.getOrCreateShard(idx)
	if err != nil {
		recordPruneDiagnostics(time.Since(totalStart).Nanoseconds(), waitDur.Nanoseconds(), 0, 0)
		return err
	}

	t.shardsMu.Lock()
	t.markDirtyShardLocked(idx)
	t.markArchiveDirtyShardLocked(idx)
	t.shardsMu.Unlock()

	pruneBefore := snapshotPruneCounters()
	shardPruneStart := time.Now()
	err = shard.Prune(t.globalEpochBit)
	shardPruneDur := time.Since(shardPruneStart)
	pruneAfter := snapshotPruneCounters()
	if err != nil {
		totalDur := time.Since(totalStart)
		recordPruneDiagnostics(totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), 0)
		recordPruneShardPressure(idx, totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), 0, pruneBefore, pruneAfter)
		return err
	}

	t.pruneShardIdx = (idx + 1) % (1 << t.config.ShardDepth)
	prefetchStart := time.Now()
	t.startPrunePrefetch(t.pruneShardIdx)
	prefetchDur := time.Since(prefetchStart)
	totalDur := time.Since(totalStart)
	recordPruneDiagnostics(totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), prefetchDur.Nanoseconds())
	recordPruneShardPressure(idx, totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), prefetchDur.Nanoseconds(), pruneBefore, pruneAfter)
	return nil
}

// SetGlobalEpoch sets the global epoch bit, used by tests.
func (t *Trie) SetGlobalEpoch(epoch byte) {
	t.globalEpochBit = epoch
}

// IsDirty returns true if the trie has any uncommitted changes.
func (t *Trie) IsDirty() bool {
	t.shardsMu.RLock()
	defer t.shardsMu.RUnlock()
	for _, s := range t.shards {
		if s != nil && s.root != nil && s.root.IsDirty() {
			return true
		}
	}
	return false
}

// ForEach iterates over all keys and values in the trie.
func (t *Trie) ForEach(fn func(key, value []byte) bool) {
	t.shardsMu.RLock()
	defer t.shardsMu.RUnlock()
	for i, s := range t.shards {
		if s != nil {
			prefix := t.getShardPrefix(i)
			s.ForEach(prefix, t.config.ShardDepth, fn)
		}
	}
}

// ForEachPrefix iterates over keys matching the given bit prefix. It avoids
// touching unrelated shards, which is important for storage wiping.
func (t *Trie) ForEachPrefix(prefix []byte, prefixBits int, fn func(key, value []byte) bool) {
	if prefixBits <= 0 {
		t.ForEach(fn)
		return
	}
	shardDepth := t.config.ShardDepth
	if prefixBits >= shardDepth {
		shardID := t.GetShardID(prefix)
		shard, err := t.getOrCreateShard(shardID)
		if err != nil || shard == nil {
			return
		}
		shardPrefix := t.getShardPrefix(shardID)
		shard.ForEachPrefix(shardPrefix, shardDepth, prefix, prefixBits, fn)
		return
	}

	shardCount := 1 << shardDepth
	for id := 0; id < shardCount; id++ {
		shardPrefix := t.getShardPrefix(id)
		if !bitPrefixesOverlap(shardPrefix, shardDepth, prefix, prefixBits) {
			continue
		}
		shard, err := t.getOrCreateShard(id)
		if err != nil || shard == nil {
			continue
		}
		shard.ForEachPrefix(shardPrefix, shardDepth, prefix, prefixBits, fn)
	}
}

func (t *Trie) getShardPrefix(shardID int) []byte {
	prefix := make([]byte, (t.config.ShardDepth+7)/8)
	for i := 0; i < t.config.ShardDepth; i++ {
		bit := byte((shardID >> (t.config.ShardDepth - 1 - i)) & 1)
		if bit == 1 {
			byteIdx := i / 8
			bitIdx := 7 - (i % 8)
			prefix[byteIdx] |= (1 << bitIdx)
		}
	}
	return prefix
}

// FlushArchives persists all pending archive data to ArchiveDB.
func (t *Trie) FlushArchives() error {
	if err := t.finishAsyncPrune(); err != nil {
		return err
	}

	t.shardsMu.RLock()
	dirtyShardsList := append([]int(nil), t.archiveShardList...)
	t.shardsMu.RUnlock()

	var wg sync.WaitGroup
	errChan := make(chan error, len(dirtyShardsList))

	for _, i := range dirtyShardsList {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s := t.shards[idx]
			if s != nil {
				if err := s.FlushArchives(); err != nil {
					errChan <- err
				}
			}
		}(i)
	}
	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan
	}

	t.shardsMu.Lock()
	for _, i := range dirtyShardsList {
		delete(t.archiveDirtyShards, i)
	}
	t.filterArchiveShardListLocked()
	t.shardsMu.Unlock()
	return nil
}

// Activate moves an archived key back to the hot tree.
func (t *Trie) Activate(key []byte, value []byte) error {
	shardID := t.GetShardID(key)
	if err := t.finishAsyncPruneForShard(shardID); err != nil {
		return err
	}
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.shardsMu.Lock()
	t.markDirtyShardLocked(shardID)
	t.markArchiveDirtyShardLocked(shardID)
	t.shardsMu.Unlock()
	return shard.Activate(key, value)
}

// Close releases any resources held by the trie.
func (t *Trie) Close() error {
	t.shardsMu.Lock()
	defer t.shardsMu.Unlock()
	for i := range t.shards {
		if t.shards[i] != nil {
			t.shards[i].ClearCaches()
			t.shards[i] = nil
		}
	}
	return nil
}

// Database returns the underlying database.
func (t *Trie) Database() KVStore {
	return t.db
}

// Hasher returns the hasher used by the trie.
func (t *Trie) Hasher() Hasher {
	return t.hasher
}

// Config returns the trie configuration.
func (t *Trie) Config() *Config {
	return t.config
}

func (t *Trie) NodeCacheStats() (entries int64, bytes int64) {
	if t == nil || t.nodeCache == nil {
		return 0, 0
	}
	return t.nodeCache.stats()
}

type memBatchOp struct {
	k, v   []byte
	delete bool
}

type memBatcher struct {
	ops       []memBatchOp
	valueSize int
}

func (m *memBatcher) Put(key []byte, value []byte) error {
	m.ops = append(m.ops, memBatchOp{k: key, v: value})
	m.valueSize += len(key) + len(value)
	return nil
}

func (m *memBatcher) Delete(key []byte) error {
	m.ops = append(m.ops, memBatchOp{k: key, delete: true})
	m.valueSize += len(key)
	return nil
}
func (m *memBatcher) Write() error   { return nil }
func (m *memBatcher) Reset()         { m.ops = nil; m.valueSize = 0 }
func (m *memBatcher) ValueSize() int { return m.valueSize }

// GetGlobalEpochBit returns the current global epoch bit.
func (t *Trie) GetGlobalEpochBit() byte {
	return t.globalEpochBit
}

package archive

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// Trie coordinates the shard views of one binary tree. Shards are only fixed
// depth subtrees; their roots are connected by the same binary paths and are
// hashed directly back to one global root.
type Trie struct {
	db     KVStore
	hasher Hasher
	config *Config

	stemViewMu sync.Mutex
	stemView   *StemTrie

	shards   []*Shard
	shardsMu sync.RWMutex
	rootMu   sync.Mutex

	// rootBranch is the lazily loaded binary path above the shard boundary.
	// With ShardDepth zero, rootHash is directly the only shard root.
	rootBranch *rootBranch
	rootHash   []byte

	// nodeCache 缓存序列化节点；pointCache 缓存解码后的 ECMH 点。
	// 这两个缓存被所有 shard 共享，用来降低剪枝和提交时的重复解码/分配。
	nodeCache  *nodeBlobCache
	pointCache *commitmentPointCache

	// 预取下一个 path-storage shard，减少轮到该 shard 剪枝时的加载等待。
	prunePrefetchMu   sync.Mutex
	prunePrefetchIdx  int
	prunePrefetchDone chan struct{}

	// 同一时间只允许一个后台剪枝任务。任何依赖 root 的操作都必须先等待它完成。
	asyncPruneMu sync.Mutex
	asyncPrune   *asyncPruneJob

	// 分片轮转剪枝使用的 epoch 状态。
	pruneShardIdx  int
	globalEpochBit byte
	pruning        bool

	dirtyShards    map[int]struct{}
	dirtyShardList []int
}

// asyncPruneJob 记录一个后台 shard 剪枝任务，以及等待该任务完成时需要统计的诊断信息。
type asyncPruneJob struct {
	idx           int
	epoch         byte
	done          chan struct{}
	err           error
	pruneNanos    int64
	prunePhases   ShardPruneDiagnostics
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
		db:               db,
		hasher:           hasher,
		config:           config,
		shards:           make([]*Shard, 1<<config.ShardDepth),
		nodeCache:        newNodeBlobCacheWithBytesLimit(config.NodeCacheLimit, config.NodeCacheBytesLimit),
		pointCache:       newCommitmentPointCache(config.CommitmentPointCacheLimit),
		prunePrefetchIdx: -1,
		globalEpochBit:   0,
		pruning:          pruning,
		dirtyShards:      make(map[int]struct{}),
	}
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
	shardRoot, _ := t.shardRoot(id)
	return shardRoot
}

func (t *Trie) CommitShardToBatch(id int, batch Batcher, destructive bool) ([]byte, error) {
	if err := t.finishAsyncPrune(); err != nil {
		return nil, err
	}
	shard, err := t.getOrCreateShard(id)
	if err != nil {
		return nil, err
	}
	return shard.CommitToBatch(batch, destructive)
}

func (t *Trie) CommitShardToBatchWithDiagnostics(id int, batch Batcher, destructive bool) ([]byte, ShardCommitDiagnostics, error) {
	if err := t.finishAsyncPrune(); err != nil {
		return nil, ShardCommitDiagnostics{}, err
	}
	shard, err := t.getOrCreateShard(id)
	if err != nil {
		return nil, ShardCommitDiagnostics{}, err
	}
	return shard.CommitToBatchWithDiagnostics(batch, destructive)
}

func (t *Trie) CommitRootToBatch(shardRoots map[int][]byte, dirtyShards []int, batch Batcher) ([]byte, error) {
	rootHash, err := t.computeBinaryRoot(shardRoots, dirtyShards, batch)
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

func (t *Trie) markDirtyShardLocked(id int) {
	if _, ok := t.dirtyShards[id]; ok {
		return
	}
	t.dirtyShards[id] = struct{}{}
	t.dirtyShardList = append(t.dirtyShardList, id)
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
	recordPruneShardPressure(job.idx, totalNanos, waitNanos, job.pruneNanos, prefetchNanos, job.prunePhases, job.before, job.after)
	return job.err
}

// finishAsyncPruneForShard waits only when the pending cleanup owns the shard
// about to be accessed. Unrelated shards remain available while cleanup runs;
// root reads and commits still use finishAsyncPrune as a global barrier.
func (t *Trie) finishAsyncPruneForShard(shardID int) error {
	t.asyncPruneMu.Lock()
	job := t.asyncPrune
	t.asyncPruneMu.Unlock()
	if job == nil || job.idx != shardID {
		return nil
	}
	return t.finishAsyncPrune()
}

// Load loads the Trie state from the database using a given root hash.
func (t *Trie) Load(rootHash []byte) error {
	if len(rootHash) == 0 {
		return nil
	}
	if len(rootHash) != 32 {
		return fmt.Errorf("archive: root hash must be 32 bytes, got %d", len(rootHash))
	}
	if err := t.finishAsyncPrune(); err != nil {
		return err
	}

	// If current root is already what we're loading, skip reset
	current, _ := t.Hash()
	if rootHashesEqual(current, rootHash) {
		return nil
	}

	if err := t.loadBinaryRoot(rootHash); err != nil {
		return err
	}

	// 重置已加载 shard，使它们下次访问时从新 root 的二叉路径读取分片 root。
	t.shardsMu.Lock()
	defer t.shardsMu.Unlock()
	for i, s := range t.shards {
		if s != nil {
			shardRoot, err := t.shardRoot(i)
			if err != nil {
				return err
			}
			s.Reset(shardRoot)
		}
	}
	t.dirtyShards = make(map[int]struct{})
	t.dirtyShardList = nil
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

	// Follow the key prefix directly to the committed shard root.
	shardRoot, rootErr := t.shardRoot(id)
	if rootErr != nil {
		return nil, rootErr
	}

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

// Get 根据 key 定位 shard，并沿该 shard 的热/冷路径读取。
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
	return val, err
}

// Put 把新 value 写成热 leaf。如果该 key 已经在冷桶中，先删除旧冷 entry，
// 保证一个 key 只有一个有效位置。
func (t *Trie) Put(key []byte, value []byte) error {
	start := time.Now()
	defer func() { recordShardPutDiagnostics(1, time.Since(start)) }()
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
	return nil
}

// PutBatch applies ordered writes in parallel across independent shards.
// Writes targeting the same shard retain their original order.
func (t *Trie) PutBatch(entries []KeyValue) error {
	if len(entries) == 0 {
		return nil
	}
	totalStart := time.Now()
	var groupWork []int64
	defer func() {
		var work time.Duration
		for _, nanos := range groupWork {
			work += time.Duration(nanos)
		}
		recordShardPutBatchDiagnostics(len(entries), len(groupWork), time.Since(totalStart), work)
	}()
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
	groupWork = make([]int64, len(groups))
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
				groupStart := time.Now()
				shard, err := t.getOrCreateShard(group.id)
				if err == nil {
					t.markDirtyShard(group.id)
					err = shard.PutBatch(group.entries)
				}
				groupWork[index] = time.Since(groupStart).Nanoseconds()
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

// Delete 同时删除 key 的热状态和冷状态。
func (t *Trie) Delete(key []byte) error {
	start := time.Now()
	defer func() { recordShardDeleteDiagnostics(time.Since(start)) }()
	shardID := t.GetShardID(key)
	if err := t.finishAsyncPruneForShard(shardID); err != nil {
		return err
	}
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.markDirtyShard(shardID)
	return shard.Delete(key)
}

// BatchDelete is an alias for Delete, used by some tests.
func (t *Trie) BatchDelete(key []byte) error {
	return t.Delete(key)
}

// Hash returns the root hash of the Trie.
func (t *Trie) Hash() ([]byte, error) {
	totalStart := time.Now()
	if err := t.finishAsyncPrune(); err != nil {
		recordHashDiagnostics(HashDiagnostics{TotalNanos: time.Since(totalStart).Nanoseconds()})
		return nil, err
	}
	type shardHashResult struct {
		id   int
		root []byte
		diag ShardCommitDiagnostics
		err  error
	}

	// Snapshot both ids and pointers so shard creation never needs to contend
	// with the comparatively expensive hashing work below.
	t.shardsMu.RLock()
	dirtyShards := append([]int(nil), t.dirtyShardList...)
	shards := make([]*Shard, len(dirtyShards))
	for index, id := range dirtyShards {
		shards[index] = t.shards[id]
	}
	t.shardsMu.RUnlock()

	workers := t.parallelWorkerCount(len(dirtyShards))
	results := make([]shardHashResult, len(dirtyShards))
	shardStart := time.Now()
	if workers > 0 {
		jobs := make(chan int, len(dirtyShards))
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for index := range jobs {
					id := dirtyShards[index]
					result := shardHashResult{id: id, root: make([]byte, 32)}
					if shards[index] != nil {
						result.root, result.diag, result.err = shards[index].HashWithDiagnostics()
						if len(result.root) == 0 {
							result.root = make([]byte, 32)
						}
					}
					results[index] = result
				}
			}()
		}
		for index := range dirtyShards {
			jobs <- index
		}
		close(jobs)
		wg.Wait()
	}
	shardWall := time.Since(shardStart)

	diag := HashDiagnostics{
		ShardWallNanos: shardWall.Nanoseconds(),
		DirtyShards:    int64(len(dirtyShards)),
		Workers:        int64(workers),
	}
	shardRoots := make(map[int][]byte, len(dirtyShards))
	for _, result := range results {
		diag.ShardWorkNanos += result.diag.TotalNanos
		diag.NodeCount += result.diag.NodeCount
		diag.DirtyNodes += result.diag.DirtyNodeCount
		diag.CleanNodes += result.diag.CleanNodeCount
		diag.SerializeNanos += result.diag.CommitSerializeNanos
		diag.HashNanos += result.diag.CommitHashNanos
		if result.diag.TotalNanos > diag.MaxShardNanos {
			diag.MaxShardID = int64(result.id)
			diag.MaxShardNanos = result.diag.TotalNanos
		}
		if result.err != nil {
			diag.TotalNanos = time.Since(totalStart).Nanoseconds()
			recordHashDiagnostics(diag)
			return nil, result.err
		}
		shardRoots[result.id] = result.root
	}

	rootStart := time.Now()
	root, err := t.computeBinaryRoot(shardRoots, dirtyShards, nil)
	diag.RootNanos = time.Since(rootStart).Nanoseconds()
	diag.TotalNanos = time.Since(totalStart).Nanoseconds()
	recordHashDiagnostics(diag)
	return root, err
}

func (t *Trie) parallelWorkerCount(items int) int {
	if items <= 0 {
		return 0
	}
	workers := runtime.GOMAXPROCS(0)
	if t != nil && t.config != nil && t.config.CommitWorkers > 0 && t.config.CommitWorkers < workers {
		workers = t.config.CommitWorkers
	}
	if workers < 1 {
		workers = 1
	}
	if workers > items {
		workers = items
	}
	return workers
}

// Commit 把 dirty shard metadata、pending value、stale delete 和全局 root
// 一起写入数据库 batch。
func (t *Trie) Commit() ([]byte, error) {
	totalStart := time.Now()
	if err := t.finishAsyncPrune(); err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), 0, 0, 0)
		return nil, err
	}
	dirtyShards := t.diagnosticCommitShardCounts()
	batch := t.db.NewBatch()
	defer batch.Reset()

	commitToBatchStart := time.Now()
	rootHash, err := t.CommitToBatch(batch, true)
	if err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), time.Since(commitToBatchStart).Nanoseconds(), 0, dirtyShards)
		return nil, err
	}
	commitToBatchDuration := time.Since(commitToBatchStart)

	batchWriteStart := time.Now()
	if err := batch.Write(); err != nil {
		recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), commitToBatchDuration.Nanoseconds(), time.Since(batchWriteStart).Nanoseconds(), dirtyShards)
		return nil, err
	}
	batchWriteDuration := time.Since(batchWriteStart)
	recordCommitDiagnostics(time.Since(totalStart).Nanoseconds(), commitToBatchDuration.Nanoseconds(), batchWriteDuration.Nanoseconds(), dirtyShards)

	return rootHash, nil
}

func (t *Trie) diagnosticCommitShardCounts() int {
	t.shardsMu.RLock()
	defer t.shardsMu.RUnlock()
	return len(t.dirtyShards)
}

// CommitToBatch 并行提交 dirty shard 到 worker 私有缓冲，再串行合并到调用方 batch，
// 最后沿变更分片的二叉路径重算全局 root。
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

	numWorkers := t.parallelWorkerCount(len(dirtyShardsList))

	// 每个 worker 写入自己的 memBatcher，不并发共享调用方 batch。
	// worker 缓冲会在同一把锁下回放，保证底层 batch 安全。
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

	rootStart := time.Now()
	rootHash, err := t.computeBinaryRoot(shardRoots, dirtyShards, batch)
	if err != nil {
		recordCommitToBatchDiagnostics(shardCommitDuration.Nanoseconds(), time.Since(rootStart).Nanoseconds())
		return nil, err
	}
	rootDuration := time.Since(rootStart)
	recordCommitToBatchDiagnostics(shardCommitDuration.Nanoseconds(), rootDuration.Nanoseconds())

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
	root, _ := t.shardRoot(idx)
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

// PruneNextShard 推进轮转剪枝游标，处理下一个 shard。
//
// 启用 AsyncPrune 时，它会启动后台剪枝后返回；后续读取或提交 root 前会先调用
// finishAsyncPrune 等待结果落定。
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
			job.prunePhases, job.err = shard.PruneWithDiagnostics(job.epoch)
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
	t.shardsMu.Unlock()

	pruneBefore := snapshotPruneCounters()
	shardPruneStart := time.Now()
	prunePhases, err := shard.PruneWithDiagnostics(t.globalEpochBit)
	shardPruneDur := time.Since(shardPruneStart)
	pruneAfter := snapshotPruneCounters()
	if err != nil {
		totalDur := time.Since(totalStart)
		recordPruneDiagnostics(totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), 0)
		recordPruneShardPressure(idx, totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), 0, prunePhases, pruneBefore, pruneAfter)
		return err
	}

	t.pruneShardIdx = (idx + 1) % (1 << t.config.ShardDepth)
	prefetchStart := time.Now()
	t.startPrunePrefetch(t.pruneShardIdx)
	prefetchDur := time.Since(prefetchStart)
	totalDur := time.Since(totalStart)
	recordPruneDiagnostics(totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), prefetchDur.Nanoseconds())
	recordPruneShardPressure(idx, totalDur.Nanoseconds(), waitDur.Nanoseconds(), shardPruneDur.Nanoseconds(), prefetchDur.Nanoseconds(), prunePhases, pruneBefore, pruneAfter)
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

// ForEachAll iterates both loaded shards and persisted shards reachable from
// the binary root. Unlike ForEach, it is suitable for recovery and tooling after
// a trie reload, when most shards have not been opened in memory yet.
func (t *Trie) ForEachAll(fn func(key, value []byte) bool) error {
	if err := t.finishAsyncPrune(); err != nil {
		return err
	}
	t.shardsMu.RLock()
	loaded := make([]*Shard, len(t.shards))
	copy(loaded, t.shards)
	t.shardsMu.RUnlock()

	roots, err := t.allShardRoots()
	if err != nil {
		return err
	}
	stopped := false
	visit := func(key, value []byte) bool {
		if stopped {
			return false
		}
		if !fn(key, value) {
			stopped = true
			return false
		}
		return true
	}
	for id, shard := range loaded {
		if shard == nil {
			continue
		}
		delete(roots, id)
		prefix := t.getShardPrefix(id)
		shard.mu.RLock()
		if shard.root != nil {
			shard.forEach(shard.root, prefix, t.config.ShardDepth, visit)
			shard.mu.RUnlock()
			if stopped {
				return nil
			}
			continue
		}
		root := append([]byte(nil), shard.rootHash...)
		shard.mu.RUnlock()
		if len(root) == 0 {
			continue
		}
		view := newStatsShardView(id, t.db, t.hasher, t.config, t.nodeCache, root, t.pruning, shard.globalEpochBit)
		loadedRoot, err := view.loadNodeAtPath(root, nil, 0)
		if err != nil {
			return err
		}
		view.forEach(loadedRoot, prefix, t.config.ShardDepth, visit)
		if stopped {
			return nil
		}
	}
	for id, root := range roots {
		prefix := t.getShardPrefix(id)
		shardID := id
		view := newStatsShardView(id, t.db, t.hasher, t.config, t.nodeCache, root, t.pruning, func() byte {
			if shardID < t.pruneShardIdx {
				return t.globalEpochBit
			}
			return t.globalEpochBit ^ 1
		})
		loadedRoot, err := view.loadNodeAtPath(root, nil, 0)
		if err != nil {
			return err
		}
		view.forEach(loadedRoot, prefix, t.config.ShardDepth, visit)
		if stopped {
			return nil
		}
	}
	return nil
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

func (t *Trie) NodeCacheDiagnostics() NodeCacheDiagnostics {
	if t == nil || t.nodeCache == nil {
		return NodeCacheDiagnostics{}
	}
	return t.nodeCache.diagnostics()
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

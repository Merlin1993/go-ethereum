package binary

import (
	"bytes"
	"sync"
)

// Trie represents a Binary Merkle Patricia Trie that shards its key space.
type Trie struct {
	db     KVStore
	hasher Hasher
	config *Config

	shards   []*Shard
	shardsMu sync.RWMutex

	// TopTree handles the hierarchical root hash computation
	topTree *TopTree

	// Epoch management for pruning
	pruneShardIdx  int
	globalEpochBit byte
	pruning        bool

	dirtyShards        map[int]struct{}
	archiveDirtyShards map[int]struct{}
}

// NewTrie creates a new Binary Trie with the given database and configuration.
func NewTrie(root []byte, db KVStore, hasher Hasher, config *Config, pruning bool) *Trie {
	if config == nil {
		config = DefaultConfig()
	}
	t := &Trie{
		db:                 db,
		hasher:             hasher,
		config:             config,
		shards:             make([]*Shard, 1<<config.ShardDepth),
		globalEpochBit:     0,
		pruning:            pruning,
		dirtyShards:        make(map[int]struct{}),
		archiveDirtyShards: make(map[int]struct{}),
	}
	t.topTree = NewTopTree(hasher, nil, config.ShardDepth)

	if len(root) > 0 {
		t.Load(root)
	}
	return t
}

func (t *Trie) GetDirtyShards() []int {
	t.shardsMu.RLock()
	defer t.shardsMu.RUnlock()
	list := make([]int, 0, len(t.dirtyShards))
	for id := range t.dirtyShards {
		list = append(list, id)
	}
	return list
}

func (t *Trie) GetShardRoot(id int) []byte {
	shardRoot, _ := t.topTree.GetShardRoot(id, t.db)
	return shardRoot
}

func (t *Trie) CommitShardToBatch(id int, batch Batcher, destructive bool) ([]byte, error) {
	shard, err := t.getOrCreateShard(id)
	if err != nil {
		return nil, err
	}
	return shard.CommitToBatch(batch, destructive)
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
	t.dirtyShards[id] = struct{}{}
	t.shardsMu.Unlock()
}

func (t *Trie) markArchiveDirtyShard(id int) {
	t.shardsMu.Lock()
	t.archiveDirtyShards[id] = struct{}{}
	t.shardsMu.Unlock()
}

// Load loads the Trie state from the database using a given root hash.
func (t *Trie) Load(rootHash []byte) error {
	if len(rootHash) == 0 {
		return nil
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

	s, err := NewShard(id, t.db, t.hasher, t.config, shardRoot, t.pruning, func() byte {
		// [Rolling Epoch] 为了防止“提前加热”导致无法归档，每个分片只有在正式被剪枝后才切换到新的 GlobalBit。
		// 在当前周期内尚未被剪枝的分片应继续使用旧位的补。
		if id < t.pruneShardIdx {
			return t.globalEpochBit
		}
		return t.globalEpochBit ^ 1
	})
	if err != nil {
		return nil, err
	}
	t.shards[id] = s
	return s, nil
}

// Get finds the value for a given key.
func (t *Trie) Get(key []byte) ([]byte, error) {
	shardID := t.GetShardID(key)
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

// Delete removes a key and its value from the Trie.
func (t *Trie) Delete(key []byte) error {
	shardID := t.GetShardID(key)
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
	shardRoots := make(map[int][]byte)
	dirtyShards := make([]int, 0)

	// We only need to compute roots for shards that have changed
	// or were previously loaded but not committed.
	// For simplicity, we can use dirtyShards here too, but Hash() doesn't clear them.
	t.shardsMu.RLock()
	for i := range t.dirtyShards {
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
	batch := t.db.NewBatch()
	defer batch.Reset()

	// Flush archives FIRST before committing shards (which might clear dirty set)
	if err := t.FlushArchives(); err != nil {
		return nil, err
	}
	rootHash, err := t.CommitToBatch(batch, true)
	if err != nil {
		return nil, err
	}

	if err := batch.Write(); err != nil {
		return nil, err
	}

	return rootHash, nil
}

// CommitToBatch commits the trie state to a given batcher.
func (t *Trie) CommitToBatch(batch Batcher, destructive bool) ([]byte, error) {
	shardRoots := make(map[int][]byte)
	dirtyShards := make([]int, 0)

	var (
		mu         sync.Mutex
		wg         sync.WaitGroup
		errs       []error
		errMu      sync.Mutex
		numWorkers = 8
	)

	t.shardsMu.RLock()
	dirtyShardsList := make([]int, 0, len(t.dirtyShards))
	for i := range t.dirtyShards {
		dirtyShardsList = append(dirtyShardsList, i)
	}
	t.shardsMu.RUnlock()

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
					mu.Lock()
					if len(h) > 0 {
						shardRoots[i] = h
					} else {
						shardRoots[i] = make([]byte, 32)
					}
					dirtyShards = append(dirtyShards, i)
					mu.Unlock()
				} else {
					mu.Lock()
					shardRoots[i] = make([]byte, 32)
					dirtyShards = append(dirtyShards, i)
					mu.Unlock()
				}
			}

			// Merge back to global state
			mu.Lock()
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
		return nil, errs[0]
	}

	rootHash, err := t.topTree.Compute(shardRoots, dirtyShards, batch)
	if err != nil {
		return nil, err
	}

	t.shardsMu.Lock()
	t.dirtyShards = make(map[int]struct{})
	t.shardsMu.Unlock()

	return rootHash, nil
}

// PruneNextShard prunes the next shard in cycle.
func (t *Trie) PruneNextShard() error {
	idx := t.pruneShardIdx
	if idx == 0 {
		t.globalEpochBit ^= 1
	}

	shard, err := t.getOrCreateShard(idx)
	if err != nil {
		return err
	}

	t.shardsMu.Lock()
	t.dirtyShards[idx] = struct{}{}
	t.archiveDirtyShards[idx] = struct{}{}
	t.shardsMu.Unlock()

	err = shard.Prune(t.globalEpochBit)
	if err != nil {
		return err
	}

	t.pruneShardIdx = (idx + 1) % (1 << t.config.ShardDepth)
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
	t.shardsMu.RLock()
	archiveShards := make(map[int]struct{}, len(t.archiveDirtyShards)+len(t.dirtyShards))
	for i := range t.archiveDirtyShards {
		archiveShards[i] = struct{}{}
	}
	for i := range t.dirtyShards {
		archiveShards[i] = struct{}{}
	}
	dirtyShardsList := make([]int, 0, len(archiveShards))
	for i := range archiveShards {
		dirtyShardsList = append(dirtyShardsList, i)
	}
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
	t.shardsMu.Unlock()
	return nil
}

// Activate moves an archived key back to the hot tree.
func (t *Trie) Activate(key []byte, value []byte) error {
	shardID := t.GetShardID(key)
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.shardsMu.Lock()
	t.dirtyShards[shardID] = struct{}{}
	t.archiveDirtyShards[shardID] = struct{}{}
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

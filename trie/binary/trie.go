package binary

import (
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

	dirtyShards map[int]struct{}
}

// NewTrie creates a new Binary Trie with the given database and configuration.
// Original signature restored for test compatibility.
func NewTrie(root []byte, db KVStore, hasher Hasher, config *Config, pruning bool) *Trie {
	if config == nil {
		config = DefaultConfig()
	}
	t := &Trie{
		db:             db,
		hasher:         hasher,
		config:         config,
		shards:         make([]*Shard, 1<<config.ShardDepth),
		globalEpochBit: 0,
		pruning:        pruning,
		dirtyShards:    make(map[int]struct{}),
	}
	t.topTree = NewTopTree(hasher, nil, config.ShardDepth)

	if len(root) > 0 {
		t.Load(root)
	}
	return t
}

// Load loads the Trie state from the database using a given root hash.
func (t *Trie) Load(rootHash []byte) error {
	if len(rootHash) == 0 {
		return nil
	}
	// Load the TopTree from DB
	return t.topTree.Load(rootHash, t.db)
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
		return t.globalEpochBit
	})
	if err != nil {
		return nil, err
	}
	t.shards[id] = s
	return s, nil
}

// Get finds the value for a given key.
func (t *Trie) Get(key []byte) ([]byte, error) {
	shardID := t.getShardID(key)
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return nil, err
	}
	return shard.Get(key)
}

// Put updates or inserts a value for a given key.
func (t *Trie) Put(key []byte, value []byte) error {
	shardID := t.getShardID(key)
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.shardsMu.Lock()
	t.dirtyShards[shardID] = struct{}{}
	t.shardsMu.Unlock()
	return shard.Put(key, value)
}

// Delete removes a key and its value from the Trie.
func (t *Trie) Delete(key []byte) error {
	shardID := t.getShardID(key)
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.shardsMu.Lock()
	t.dirtyShards[shardID] = struct{}{}
	t.shardsMu.Unlock()
	return shard.Delete(key)
}

// BatchDelete is an alias for Delete, used by some tests.
func (t *Trie) BatchDelete(key []byte) error {
	return t.Delete(key)
}

func (t *Trie) getShardID(key []byte) int {
	id := 0
	for i := 0; i < t.config.ShardDepth; i++ {
		if getBit(key, i) == 1 {
			id |= (1 << (t.config.ShardDepth - 1 - i))
		}
	}
	return id
}

// Hash returns the root hash of the Trie.
func (t *Trie) Hash() ([]byte, error) {
	shardRoots := make(map[int][]byte)
	dirtyShards := make([]int, 0)

	// We only need to compute roots for shards that have changed
	// or were previously loaded but not committed.
	// For simplicity, we can use dirtyShards here too, but Hash() doesn't clear them.
	t.shardsMu.RLock()
	for i := range t.dirtyShards {
		s := t.shards[i]
		if s != nil {
			h, err := s.Hash()
			if err != nil {
				t.shardsMu.RUnlock()
				return nil, err
			}
			if h != nil {
				shardRoots[i] = h
				dirtyShards = append(dirtyShards, i)
			}
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

			// Worker local state
			localShardRoots := make(map[int][]byte)
			localDirtyShards := make([]int, 0)

			// Mock batcher to collect puts
			workerBatch := &memBatcher{puts: make([]memKV, 0, 5000)}

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
					if h != nil {
						localShardRoots[i] = h
						localDirtyShards = append(localDirtyShards, i)
					}
				}
			}

			// Merge back to global state
			mu.Lock()
			for k, v := range localShardRoots {
				shardRoots[k] = v
			}
			dirtyShards = append(dirtyShards, localDirtyShards...)
			for _, item := range workerBatch.puts {
				batch.Put(item.k, item.v)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		return nil, errs[0]
	}

	if destructive {
		t.shardsMu.Lock()
		t.dirtyShards = make(map[int]struct{})
		t.shardsMu.Unlock()
	}

	return t.topTree.Compute(shardRoots, dirtyShards, batch)
}

// PruneNextShard prunes the next shard in cycle.
func (t *Trie) PruneNextShard() error {
	idx := t.pruneShardIdx
	shard, err := t.getOrCreateShard(idx)
	if err != nil {
		return err
	}

	t.shardsMu.Lock()
	t.dirtyShards[idx] = struct{}{}
	t.shardsMu.Unlock()

	err = shard.Prune()

	t.pruneShardIdx = (idx + 1) % (1 << t.config.ShardDepth)
	if t.pruneShardIdx == 0 {
		t.globalEpochBit ^= 1
	}
	return err
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
	dirtyShardsList := make([]int, 0, len(t.dirtyShards))
	for i := range t.dirtyShards {
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
	return nil
}

// Activate moves an archived key back to the hot tree.
func (t *Trie) Activate(key []byte, value []byte) error {
	shardID := t.getShardID(key)
	shard, err := t.getOrCreateShard(shardID)
	if err != nil {
		return err
	}
	t.shardsMu.Lock()
	t.dirtyShards[shardID] = struct{}{}
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

type memKV struct {
	k, v []byte
}

type memBatcher struct {
	puts []memKV
}

func (m *memBatcher) Put(key []byte, value []byte) error {
	m.puts = append(m.puts, memKV{k: key, v: value})
	return nil
}

func (m *memBatcher) Delete(key []byte) error { return nil }
func (m *memBatcher) Write() error            { return nil }
func (m *memBatcher) Reset()                  { m.puts = nil }
func (m *memBatcher) ValueSize() int          { return 0 }

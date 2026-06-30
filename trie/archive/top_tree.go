package archive

import (
	"bytes"
	"errors"
	"sync"
	"time"
)

const (
	// TopTreeHeaderPrefix is the byte prefix for TopNode serialization at level 0.
	TopTreeHeaderPrefix = 0xD0
	topTreeFanoutBits   = 4
	topTreeFanout       = 1 << topTreeFanoutBits
	topTreeFanoutMask   = topTreeFanout - 1
)

var zeroHash = make([]byte, 32)

// TopNode represents an internal node in the top tree.
// At Level < maxLevel, Children points to other TopNodes.
// At Level == maxLevel, the "children" are the actual 32-byte shard hashes.
type TopNode struct {
	Level       int // 0 is the root
	Children    [topTreeFanout]*TopNode
	Hash        []byte // Cached hash of this node
	Dirty       bool   // Check if node needs to be re-hashed
	ShardHashes [topTreeFanout][]byte
}

func newTopNode(level int) *TopNode {
	return &TopNode{
		Level: level,
		Dirty: true,
	}
}

// Serialize encodes the TopNode into bytes for hashing and persistence.
// A TopNode simply serializes its child hashes in order.
// If a child is missing or its hash is nil, a 32-byte zero hash is used.
// Optionally, we append a 1-byte level indicator to distinguish nodes from different levels.
func (n *TopNode) Serialize(shardRoots map[int][]byte, nodePrefix int, maxLevel int) []byte {
	buf := make([]byte, 1+topTreeFanout*32)
	buf[0] = byte(0xD0 + n.Level) // Simple header to prevent collision

	for i := 0; i < topTreeFanout; i++ {
		offset := 1 + i*32
		if n.Level == maxLevel {
			// At maxLevel, the children are the actual shard hashes
			shardID := (nodePrefix << topTreeFanoutBits) | i
			h := shardRoots[shardID]
			if h == nil {
				h = n.ShardHashes[i]
			} else {
				n.ShardHashes[i] = h
			}

			if len(h) == 32 {
				copy(buf[offset:offset+32], h)
			} else {
				copy(buf[offset:offset+32], zeroHash)
			}
		} else {
			// At levels < maxLevel, the children are TopNodes
			child := n.Children[i]
			if child != nil && len(child.Hash) == 32 {
				copy(buf[offset:offset+32], child.Hash)
			} else {
				copy(buf[offset:offset+32], zeroHash)
			}
		}
	}
	return buf
}

// TopTree coordinates the hierarchical top tree structure.
// Instead of storing shard hashes flat, it organizes them in levels
// of wide fanout nodes, updating only the paths that change.
type TopTree struct {
	root       *TopNode
	hasher     Hasher
	db         Batcher
	shardDepth int
	maxLevel   int
	pathStore  bool
}

func NewTopTree(hasher Hasher, db Batcher, shardDepth int, pathStore bool) *TopTree {
	return &TopTree{
		root:       newTopNode(0),
		hasher:     hasher,
		db:         db,
		shardDepth: shardDepth,
		maxLevel:   (shardDepth+topTreeFanoutBits-1)/topTreeFanoutBits - 1,
		pathStore:  pathStore,
	}
}

// Compute dynamically updates the top tree based on the dirty shards and
// computes the new root hash. It only processes paths that are marked dirty.
func (t *TopTree) Compute(shardRoots map[int][]byte, dirtyShards []int, batch Batcher) ([]byte, error) {
	if t.root == nil {
		t.root = newTopNode(0)
	}
	staleHashes := make(map[string][]byte)
	// 1. Mark dirty paths bottom-up
	// We need to trace from the root to the leaves (maxLevel) for every dirty shard,
	// creating nodes if they don't exist and marking them dirty.
	for _, id := range dirtyShards {
		curr := t.root
		curr.Dirty = true

		numDigits := t.maxLevel + 1
		for i := 0; i < t.maxLevel; i++ {
			shift := (numDigits - 1 - i) * topTreeFanoutBits
			idx := (id >> shift) & topTreeFanoutMask

			if curr.Children[idx] == nil {
				curr.Children[idx] = newTopNode(i + 1)
			}
			curr = curr.Children[idx]
			curr.Dirty = true
		}
	}

	// 2. Re-hash the dirty nodes bottom-up (Post-order traversal)
	var err error
	t.root.Hash, err = t.hashRoot(shardRoots, batch, staleHashes)
	if err != nil {
		return nil, err
	}
	if batch != nil {
		t.root.Dirty = false
	}

	if batch != nil && !t.pathStore && len(staleHashes) > 0 {
		for _, hash := range staleHashes {
			if err := batch.Delete(hash); err != nil {
				return nil, err
			}
		}
	}

	return t.root.Hash, nil
}

func (t *TopTree) hashRoot(shardRoots map[int][]byte, batch Batcher, staleHashes map[string][]byte) ([]byte, error) {
	if t.root == nil || !t.root.Dirty || batch == nil || t.maxLevel <= 0 {
		return t.hashNode(t.root, shardRoots, 0, batch, staleHashes)
	}

	oldHash := append([]byte(nil), t.root.Hash...)

	type childResult struct {
		idx          int
		prefix       int
		hash         []byte
		ops          []memBatchOp
		opBytes      int
		computeNanos int64
		stale        map[string][]byte
		err          error
	}

	results := make(chan childResult, topTreeFanout)
	var wg sync.WaitGroup
	dirtyChildren := 0
	for i, child := range t.root.Children {
		if child == nil || !child.Dirty {
			continue
		}
		dirtyChildren++
		wg.Add(1)
		go func(idx int, node *TopNode) {
			defer wg.Done()
			localBatch := &memBatcher{ops: make([]memBatchOp, 0, 512)}
			localStale := make(map[string][]byte)
			childPrefix := idx
			computeStart := time.Now()
			h, err := t.hashNode(node, shardRoots, childPrefix, localBatch, localStale)
			results <- childResult{
				idx:          idx,
				prefix:       childPrefix,
				hash:         h,
				ops:          localBatch.ops,
				opBytes:      localBatch.valueSize,
				computeNanos: time.Since(computeStart).Nanoseconds(),
				stale:        localStale,
				err:          err,
			}
		}(i, child)
	}

	wg.Wait()
	close(results)

	var (
		maxChildPrefix  int
		maxChildCompute time.Duration
		maxChildOps     int
		maxChildBytes   int
		maxApplyPrefix  int
		maxApply        time.Duration
		totalOps        int
		totalBytes      int
	)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		childCompute := time.Duration(res.computeNanos)
		if childCompute > maxChildCompute {
			maxChildCompute = childCompute
			maxChildPrefix = res.prefix
			maxChildOps = len(res.ops)
			maxChildBytes = res.opBytes
		}
		totalOps += len(res.ops)
		totalBytes += res.opBytes
		child := t.root.Children[res.idx]
		child.Hash = res.hash
		child.Dirty = false
		applyStart := time.Now()
		for _, op := range res.ops {
			var err error
			if op.delete {
				err = batch.Delete(op.k)
			} else {
				err = batch.Put(op.k, op.v)
			}
			if err != nil {
				return nil, err
			}
		}
		applyDuration := time.Since(applyStart)
		if applyDuration > maxApply {
			maxApply = applyDuration
			maxApplyPrefix = res.prefix
		}
		for key, hash := range res.stale {
			staleHashes[key] = hash
		}
	}

	data := t.root.Serialize(shardRoots, 0, t.maxLevel)
	t.root.Hash = t.hasher.Hash(data)
	t.root.Dirty = false
	key := t.root.Hash
	if t.pathStore {
		key = pathTopNodeKey(t.root.Level, 0)
	}
	if err := batch.Put(key, data); err != nil {
		return nil, err
	}
	totalOps++
	totalBytes += len(key) + len(data)
	recordTopTreeDetailDiagnostics(dirtyChildren, maxChildPrefix, maxChildCompute, maxChildOps, maxChildBytes, maxApplyPrefix, maxApply, totalOps, totalBytes)
	if !t.pathStore && len(oldHash) > 0 && !bytes.Equal(oldHash, t.root.Hash) && !bytes.Equal(oldHash, zeroHash) {
		staleHashes[string(oldHash)] = oldHash
	}
	return t.root.Hash, nil
}

// hashNode recursively computes the hash of a dirty TopNode and writes it to DB.
func (t *TopTree) hashNode(n *TopNode, shardRoots map[int][]byte, prefix int, batch Batcher, staleHashes map[string][]byte) ([]byte, error) {
	if !n.Dirty {
		return n.Hash, nil
	}

	oldHash := append([]byte(nil), n.Hash...)

	for i := 0; i < topTreeFanout; i++ {
		child := n.Children[i]
		if child != nil {
			childPrefix := (prefix << topTreeFanoutBits) | i
			h, err := t.hashNode(child, shardRoots, childPrefix, batch, staleHashes)
			if err != nil {
				return nil, err
			}
			child.Hash = h
			if batch != nil {
				child.Dirty = false
			}
		}
	}

	// Now serialize this node (incorporating its children's hashes)
	data := n.Serialize(shardRoots, prefix, t.maxLevel)
	n.Hash = t.hasher.Hash(data)
	if batch != nil {
		n.Dirty = false
	}

	// Persist the internal top tree node to the database batch
	if batch != nil {
		key := n.Hash
		if t.pathStore {
			key = pathTopNodeKey(n.Level, prefix)
		}
		if err := batch.Put(key, data); err != nil {
			return nil, err
		}
		if !t.pathStore && len(oldHash) > 0 && !bytes.Equal(oldHash, n.Hash) && !bytes.Equal(oldHash, zeroHash) {
			staleHashes[string(oldHash)] = oldHash
		}
	}

	return n.Hash, nil
}

func (t *TopTree) ForEachShardRoot(fn func(id int, hash []byte)) {
	t.forEachShardRoot(t.root, 0, fn)
}

func (t *TopTree) forEachShardRoot(n *TopNode, prefix int, fn func(id int, hash []byte)) {
	if n == nil {
		return
	}
	if n.Level == t.maxLevel {
		for i, h := range n.ShardHashes {
			if len(h) > 0 && !bytes.Equal(h, zeroHash) {
				fn((prefix<<topTreeFanoutBits)|i, append([]byte(nil), h...))
			}
		}
		return
	}
	for i, child := range n.Children {
		t.forEachShardRoot(child, (prefix<<topTreeFanoutBits)|i, fn)
	}
}

// Load recursively reconstructs the TopTree structure from a given root hash.
func (t *TopTree) Load(rootHash []byte, db KVStore) error {
	if len(rootHash) == 0 {
		return nil
	}
	newRoot, err := t.loadNode(rootHash, 0, 0, db)
	if err != nil {
		return err
	}
	t.root = newRoot
	return nil
}

func (t *TopTree) loadNode(hash []byte, level int, prefix int, db KVStore) (*TopNode, error) {
	if len(hash) == 0 || bytes.Equal(hash, make([]byte, 32)) {
		return nil, nil
	}
	key := hash
	if t.pathStore {
		key = pathTopNodeKey(level, prefix)
	}
	data, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	if t.pathStore && !bytes.Equal(t.hasher.Hash(data), hash) {
		return nil, errors.New("top node hash mismatch")
	}

	// Basic validation of header
	if data[0] != byte(0xD0+level) {
		return nil, errors.New("invalid top node header")
	}
	if len(data) < 1+topTreeFanout*32 {
		return nil, errors.New("invalid top node length")
	}

	n := newTopNode(level)
	n.Hash = hash
	n.Dirty = false

	if level < t.maxLevel {
		for i := 0; i < topTreeFanout; i++ {
			childHash := data[1+i*32 : 1+(i+1)*32]
			childPrefix := (prefix << topTreeFanoutBits) | i
			child, err := t.loadNode(childHash, level+1, childPrefix, db)
			if err != nil {
				return nil, err
			}
			n.Children[i] = child
		}
	} else {
		// At max level, children are just 32-byte hashes stored in data
		for i := 0; i < topTreeFanout; i++ {
			shardHash := data[1+i*32 : 1+(i+1)*32]
			if !bytes.Equal(shardHash, zeroHash) {
				n.ShardHashes[i] = append([]byte{}, shardHash...)
			}
		}
	}

	return n, nil
}

// GetShardRoot retrieves the persisted hash for a given shard ID.
func (t *TopTree) GetShardRoot(id int, db KVStore) ([]byte, error) {
	curr := t.root
	if curr == nil {
		return nil, nil
	}

	numDigits := t.maxLevel + 1
	for i := 0; i < t.maxLevel; i++ {
		shift := (numDigits - 1 - i) * topTreeFanoutBits
		idx := (id >> shift) & topTreeFanoutMask
		curr = curr.Children[idx]
		if curr == nil {
			return nil, nil
		}
	}

	idx := id & topTreeFanoutMask
	h := curr.ShardHashes[idx]
	if len(h) == 0 || bytes.Equal(h, zeroHash) {
		return nil, nil
	}
	return append([]byte(nil), h...), nil
}

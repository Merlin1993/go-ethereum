package binary

import (
	"bytes"
	"errors"
)

const (
	// TopTreeHeaderPrefix is the byte prefix for TopNode serialization at level 0.
	TopTreeHeaderPrefix = 0xD0
)

var zeroHash = make([]byte, 32)

// TopNode represents an internal node in the 16-ary top tree.
// At Level < maxLevel, Children points to other TopNodes.
// At Level == maxLevel, the "children" are the actual 32-byte shard hashes.
type TopNode struct {
	Level       int          // 0 is the root
	Children    [16]*TopNode // Pointer to child nodes (nil if empty)
	Hash        []byte       // Cached hash of this node
	Dirty       bool         // Check if node needs to be re-hashed
	ShardHashes [16][]byte   // [NEW] Cache shard hashes at maxLevel
}

func newTopNode(level int) *TopNode {
	return &TopNode{
		Level: level,
		Dirty: true,
	}
}

// Serialize encodes the TopNode into bytes for hashing and persistence.
// A TopNode simply serializes its 16 child hashes in order.
// If a child is missing or its hash is nil, a 32-byte zero hash is used.
// Optionally, we append a 1-byte level indicator to distinguish nodes from different levels.
func (n *TopNode) Serialize(shardRoots map[int][]byte, nodePrefix int, maxLevel int) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(0xD0 + n.Level)) // Simple header to prevent collision

	for i := 0; i < 16; i++ {
		if n.Level == maxLevel {
			// At maxLevel, the children are the actual shard hashes
			shardID := (nodePrefix << 4) | i
			h := shardRoots[shardID]
			if h == nil {
				h = n.ShardHashes[i]
			} else {
				n.ShardHashes[i] = h
			}

			if h != nil {
				buf.Write(h)
			} else {
				buf.Write(zeroHash)
			}
		} else {
			// At levels < maxLevel, the children are TopNodes
			child := n.Children[i]
			if child != nil && len(child.Hash) == 32 {
				buf.Write(child.Hash)
			} else {
				buf.Write(zeroHash)
			}
		}
	}
	return buf.Bytes()
}

// TopTree coordinates the hierarchical 16-ary tree structure.
// Instead of storing shard hashes flat, it organizes them in levels
// of 16-ary nodes, updating only the O(log_16 N) paths that change.
type TopTree struct {
	root       *TopNode
	hasher     Hasher
	db         Batcher
	shardDepth int
	maxLevel   int
}

func NewTopTree(hasher Hasher, db Batcher, shardDepth int) *TopTree {
	return &TopTree{
		root:       newTopNode(0),
		hasher:     hasher,
		db:         db,
		shardDepth: shardDepth,
		maxLevel:   (shardDepth+3)/4 - 1,
	}
}

// Compute dynamically updates the 16-ary tree based on the dirty shards and
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

		// Extract nibbles based on shardDepth
		numNibbles := t.maxLevel + 1
		for i := 0; i < t.maxLevel; i++ {
			shift := (numNibbles - 1 - i) * 4
			idx := (id >> shift) & 0x0F

			if curr.Children[idx] == nil {
				curr.Children[idx] = newTopNode(i + 1)
			}
			curr = curr.Children[idx]
			curr.Dirty = true
		}
	}

	// 2. Re-hash the dirty nodes bottom-up (Post-order traversal)
	var err error
	t.root.Hash, err = t.hashNode(t.root, shardRoots, 0, batch, staleHashes)
	if err != nil {
		return nil, err
	}
	if batch != nil {
		t.root.Dirty = false
	}

	if batch != nil && len(staleHashes) > 0 {
		liveHashes := make(map[string]struct{})
		t.collectLiveHashes(t.root, liveHashes)
		for key, hash := range staleHashes {
			if _, live := liveHashes[key]; live {
				continue
			}
			if err := batch.Delete(hash); err != nil {
				return nil, err
			}
		}
	}

	return t.root.Hash, nil
}

// hashNode recursively computes the hash of a dirty TopNode and writes it to DB.
func (t *TopTree) hashNode(n *TopNode, shardRoots map[int][]byte, prefix int, batch Batcher, staleHashes map[string][]byte) ([]byte, error) {
	if !n.Dirty {
		return n.Hash, nil
	}

	oldHash := append([]byte(nil), n.Hash...)

	for i := 0; i < 16; i++ {
		child := n.Children[i]
		if child != nil {
			childPrefix := (prefix << 4) | i
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

	// Persist the internal 16-ary node to the database batch
	if batch != nil {
		if err := batch.Put(n.Hash, data); err != nil {
			return nil, err
		}
		if len(oldHash) > 0 && !bytes.Equal(oldHash, n.Hash) && !bytes.Equal(oldHash, zeroHash) {
			staleHashes[string(oldHash)] = oldHash
		}
	}

	return n.Hash, nil
}

func (t *TopTree) collectLiveHashes(n *TopNode, live map[string]struct{}) {
	if n == nil {
		return
	}
	if len(n.Hash) > 0 && !bytes.Equal(n.Hash, zeroHash) {
		live[string(n.Hash)] = struct{}{}
	}
	for _, child := range n.Children {
		t.collectLiveHashes(child, live)
	}
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
				fn((prefix<<4)|i, append([]byte(nil), h...))
			}
		}
		return
	}
	for i, child := range n.Children {
		t.forEachShardRoot(child, (prefix<<4)|i, fn)
	}
}

// Load recursively reconstructs the TopTree structure from a given root hash.
func (t *TopTree) Load(rootHash []byte, db KVStore) error {
	if len(rootHash) == 0 {
		return nil
	}
	newRoot, err := t.loadNode(rootHash, 0, db)
	if err != nil {
		return err
	}
	t.root = newRoot
	return nil
}

func (t *TopTree) loadNode(hash []byte, level int, db KVStore) (*TopNode, error) {
	if len(hash) == 0 || bytes.Equal(hash, make([]byte, 32)) {
		return nil, nil
	}
	data, err := db.Get(hash)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	// Basic validation of header
	if data[0] != byte(0xD0+level) {
		return nil, errors.New("invalid top node header")
	}

	n := newTopNode(level)
	n.Hash = hash
	n.Dirty = false

	if level < t.maxLevel {
		for i := 0; i < 16; i++ {
			childHash := data[1+i*32 : 1+(i+1)*32]
			child, err := t.loadNode(childHash, level+1, db)
			if err != nil {
				return nil, err
			}
			n.Children[i] = child
		}
	} else {
		// At max level, children are just 32-byte hashes stored in data
		for i := 0; i < 16; i++ {
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

	numNibbles := t.maxLevel + 1
	for i := 0; i < t.maxLevel; i++ {
		shift := (numNibbles - 1 - i) * 4
		idx := (id >> shift) & 0x0F
		curr = curr.Children[idx]
		if curr == nil {
			return nil, nil
		}
	}

	// At max level, we need to read the shard hash from the node's serialized data
	if len(curr.Hash) == 0 {
		return nil, nil
	}
	data, err := db.Get(curr.Hash)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	idx := id & 0x0F
	h := data[1+idx*32 : 1+(idx+1)*32]
	if bytes.Equal(h, make([]byte, 32)) {
		return nil, nil
	}
	return h, nil
}

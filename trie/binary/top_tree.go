package binary

import (
	"bytes"
)

const (
	// TopTreeHeaderPrefix is the byte prefix for TopNode serialization at level 0.
	TopTreeHeaderPrefix = 0xD0
)

// TopNode represents an internal node in the 16-ary top tree.
// At Level < maxLevel, Children points to other TopNodes.
// At Level == maxLevel, the "children" are the actual 32-byte shard hashes.
type TopNode struct {
	Level    int          // 0 is the root
	Children [16]*TopNode // Pointer to child nodes (nil if empty)
	Hash     []byte       // Cached hash of this node
	Dirty    bool         // Check if node needs to be re-hashed
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
			if h != nil {
				buf.Write(h)
			} else {
				buf.Write(make([]byte, 32)) // Empty shard
			}
		} else {
			// At levels < maxLevel, the children are TopNodes
			child := n.Children[i]
			if child != nil && len(child.Hash) == 32 {
				buf.Write(child.Hash)
			} else {
				buf.Write(make([]byte, 32)) // Empty child
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
	t.root.Hash, err = t.hashNode(t.root, shardRoots, 0, batch)
	if err != nil {
		return nil, err
	}
	t.root.Dirty = false

	return t.root.Hash, nil
}

// hashNode recursively computes the hash of a dirty TopNode and writes it to DB.
func (t *TopTree) hashNode(n *TopNode, shardRoots map[int][]byte, prefix int, batch Batcher) ([]byte, error) {
	if !n.Dirty {
		return n.Hash, nil
	}

	for i := 0; i < 16; i++ {
		child := n.Children[i]
		if child != nil {
			childPrefix := (prefix << 4) | i
			h, err := t.hashNode(child, shardRoots, childPrefix, batch)
			if err != nil {
				return nil, err
			}
			child.Hash = h
			child.Dirty = false
		}
	}

	// Now serialize this node (incorporating its children's hashes)
	data := n.Serialize(shardRoots, prefix, t.maxLevel)
	n.Hash = t.hasher.Hash(data)
	n.Dirty = false

	// Persist the internal 16-ary node to the database batch
	if batch != nil {
		if err := batch.Put(n.Hash, data); err != nil {
			return nil, err
		}
	}

	return n.Hash, nil
}

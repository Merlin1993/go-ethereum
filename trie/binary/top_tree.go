package binary

import (
	"bytes"
)

const (
	// TopTreeDepth is the number of layers in the 16-ary top tree.
	// Since 16^4 = 65536, a depth of 4 layers covers all possible 16-bit shard IDs.
	TopTreeDepth = 4

	// MaxShards is the total capacity of the top tree
	MaxShards = 65536
)

// TopNode represents an internal node in the 16-ary top tree.
// At Level < 3, Children points to other TopNodes.
// At Level == 3, the "children" are the actual 32-byte shard hashes.
type TopNode struct {
	Level    int          // 0 is the root, 3 is the lowest internal node
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
func (n *TopNode) Serialize(shardRoots map[int][]byte, nodePrefix int) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(0xD0 + n.Level)) // Simple header to prevent collision

	for i := 0; i < 16; i++ {
		if n.Level == 3 {
			// At level 3, the children are the actual shard hashes
			shardID := (nodePrefix << 4) | i
			h := shardRoots[shardID]
			if h != nil {
				buf.Write(h)
			} else {
				buf.Write(make([]byte, 32)) // Empty shard
			}
		} else {
			// At levels 0-2, the children are TopNodes
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
// Instead of storing 65536 hashes flat, it organizes them in 4 levels
// of 16-ary nodes, updating only the O(log_16 N) paths that change.
type TopTree struct {
	root   *TopNode
	hasher Hasher
	db     Batcher
}

func NewTopTree(hasher Hasher, db Batcher) *TopTree {
	return &TopTree{
		root:   newTopNode(0),
		hasher: hasher,
		db:     db,
	}
}

// Compute dynamically updates the 16-ary tree based on the dirty shards and
// computes the new root hash. It only processes paths that are marked dirty.
func (t *TopTree) Compute(shardRoots map[int][]byte, dirtyShards []int, batch Batcher) ([]byte, error) {
	// 1. Mark dirty paths bottom-up
	// We need to trace from the root to the leaves (Level 3) for every dirty shard,
	// creating nodes if they don't exist and marking them dirty.
	for _, id := range dirtyShards {
		curr := t.root
		curr.Dirty = true

		// The path consists of 4 nibbles: id = N0 N1 N2 N3
		path := []int{
			(id >> 12) & 0x0F,
			(id >> 8) & 0x0F,
			(id >> 4) & 0x0F,
			id & 0x0F,
		}

		for i := 0; i < 3; i++ { // Trace down to Level 2 (children of Level 2 are Level 3 nodes)
			idx := path[i]
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
	data := n.Serialize(shardRoots, prefix)
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

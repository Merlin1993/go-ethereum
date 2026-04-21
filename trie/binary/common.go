package binary

import (
	"sync"

	"errors"
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	// ErrNodeNotFound is returned if a node hash could not be found in the database.
	ErrNodeNotFound = errors.New("node not found")
)

// Hasher is the interface that wraps the basic Hash method.
// 该接口抽象哈希算法，可支持 Keccak-256、SHA-256 等不同实现。
type Hasher interface {
	// Hash calculates the hash of the given data.
	// 计算并返回输入数据的哈希值。
	Hash(data []byte) []byte
}

// KVStore is a simple key-value store interface for the underlying database.
// 抽象底层键值数据库的读写接口。
type KVStore interface {
	// Put inserts the given value into the key-value store.
	// 写入键值数据。
	Put(key []byte, value []byte) error

	// Delete removes the value associated with the given key.
	// 删除指定键对应的数据。
	Delete(key []byte) error

	// Get retrieves the value associated with the given key.
	// 读取指定键对应的数据。
	Get(key []byte) ([]byte, error)

	// NewBatch creates a batch operation.
	NewBatch() Batcher
}

// Batcher interface for database batch operations
// 数据库批处理接口定义。
type Batcher interface {
	// 批处理写入
	Put(key []byte, value []byte) error
	// 批处理删除
	Delete(key []byte) error
	// 提交批次
	Write() error
	// 重置批次
	Reset()
	// ValueSize returns the approximate size of the batch
	ValueSize() int
}

// SafeBatcher wraps a Batcher with a mutex to allow concurrent Put/Delete calls.
type SafeBatcher struct {
	mu sync.Mutex
	b  Batcher
}

func NewSafeBatcher(b Batcher) *SafeBatcher {
	return &SafeBatcher{b: b}
}

func (sb *SafeBatcher) Put(key []byte, value []byte) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.Put(key, value)
}

func (sb *SafeBatcher) Delete(key []byte) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.Delete(key)
}

func (sb *SafeBatcher) Write() error {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.Write()
}

func (sb *SafeBatcher) Reset() {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.b.Reset()
}

func (sb *SafeBatcher) ValueSize() int {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.b.ValueSize()
}

var (
	keccakPool = &sync.Pool{}
)

// PooledKeccakHasher uses a global sync.Pool to reuse KeccakState objects, reducing allocation overhead.
type PooledKeccakHasher struct{}

// NewPooledKeccakHasher returns a PooledKeccakHasher instance.
func NewPooledKeccakHasher() *PooledKeccakHasher {
	return &PooledKeccakHasher{}
}

// Hash calculates the hash of the given data using the global pooled KeccakState.
func (h *PooledKeccakHasher) Hash(data []byte) []byte {
	raw := keccakPool.Get()
	var sha crypto.KeccakState
	if raw == nil {
		sha = crypto.NewKeccakState()
	} else {
		sha = raw.(crypto.KeccakState)
	}
	defer keccakPool.Put(sha)

	sha.Reset()
	sha.Write(data)
	hash := make([]byte, 32)
	sha.Read(hash)
	return hash
}

// NodePool manages reuse of Trie nodes to reduce GC pressure.
type NodePool struct {
	internalPool sync.Pool
	leafPool     sync.Pool
}

func NewNodePool() *NodePool {
	return &NodePool{}
}

func (p *NodePool) GetInternal() *InternalNode {
	raw := p.internalPool.Get()
	if raw == nil {
		n := &InternalNode{}
		n.Reset()
		return n
	}
	n := raw.(*InternalNode)
	n.Reset()
	return n
}

func (p *NodePool) PutInternal(n *InternalNode) {
	if n != nil {
		p.internalPool.Put(n)
	}
}

func (p *NodePool) GetLeaf() *LeafNode {
	raw := p.leafPool.Get()
	if raw == nil {
		n := &LeafNode{}
		n.Reset()
		return n
	}
	n := raw.(*LeafNode)
	n.Reset()
	return n
}

func (p *NodePool) PutLeaf(n *LeafNode) {
	if n != nil {
		p.leafPool.Put(n)
	}
}

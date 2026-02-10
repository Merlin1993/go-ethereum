package binary

import (
	"sync"

	"github.com/ethereum/go-ethereum/crypto"
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

// PooledKeccakHasher uses a sync.Pool to reuse KeccakState objects, reducing allocation overhead.
type PooledKeccakHasher struct {
	pool *sync.Pool
}

// NewPooledKeccakHasher creates a new PooledKeccakHasher.
func NewPooledKeccakHasher() *PooledKeccakHasher {
	return &PooledKeccakHasher{pool: &sync.Pool{}}
}

// Hash calculates the hash of the given data using a pooled KeccakState.
func (h *PooledKeccakHasher) Hash(data []byte) []byte {
	raw := h.pool.Get()
	var sha crypto.KeccakState
	if raw == nil {
		sha = crypto.NewKeccakState()
	} else {
		sha = raw.(crypto.KeccakState)
	}
	defer h.pool.Put(sha)

	sha.Reset()
	sha.Write(data)
	hash := make([]byte, 32)
	sha.Read(hash)
	return hash
}

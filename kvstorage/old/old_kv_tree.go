package kvsrotage

import (
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
	"hash"
	"sync"
)

// tree构造类
type KVTree struct {
	db          ethdb.Database
	deleteCache map[common.Hash]struct{} // 记录所有待删除的bindingKey
	heightCache map[uint64][]common.Hash // 记录哪些高度有待删除的数据
}

type UpdateKV struct {
	Key    common.Hash
	Height uint64
	Value  []byte // 可以为nil
}

// BindingInfo 存储绑定关系信息
type BindingInfo struct {
	Hash    common.Hash // 兄弟节点的哈希
	IsRight bool        // true表示当前节点是右节点，false表示是左节点
}

func bindingInfo2Bytes(bi *BindingInfo) []byte {
	result := make([]byte, 33)
	if bi.IsRight {
		result[0] = 0x01
	} else {
		result[0] = 0x00
	}
	copy(result[1:], bi.Hash[:])
	return result
}

func bytes2BindingInfo(data []byte) *BindingInfo {
	right := false
	if data[0] == 0x01 {
		right = true
	}
	hash := common.BytesToHash(data[1:33])
	return &BindingInfo{
		Hash:    hash,
		IsRight: right,
	}
}

func NewKVTree(db ethdb.Database) KVTree {
	return KVTree{
		db:          db,
		deleteCache: make(map[common.Hash]struct{}),
		heightCache: make(map[uint64][]common.Hash),
	}
}

func encode(data interface{}) ([]byte, error) {
	return rlp.EncodeToBytes(data)
}

func hashData(data []byte) common.Hash {
	return crypto.Keccak256Hash(data)
}

// 持久化的方法
func (kvt *KVTree) addDB(key []byte, value []byte) {
	kvt.db.Put(key, value)
}

func (kvt *KVTree) deleteDB(key []byte) {
	kvt.db.Delete(key)
}

// 计算绑定关系的key：hash(hash+Height)
func calcBindingKey(hash common.Hash, height uint64) common.Hash {
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, height)
	return hashData(append(hash.Bytes(), heightBytes...))
}

// 计算父节点哈希，考虑左右顺序
func calcParentHash(left, right common.Hash) common.Hash {
	combined, _ := encode([]common.Hash{left, right})
	return hashData(combined)
}

// GenerateHash 生成Merkle树并返回根哈希
func (kvt *KVTree) GenerateHash(h uint64, kvs []*UpdateKV) (common.Hash, map[common.Hash][]byte) {
	if len(kvs) == 0 {
		return common.Hash{}, nil
	}

	treeCache := make(map[common.Hash][]byte)
	// 第一步：计算所有叶子节点的哈希
	currentLevel := make([]common.Hash, 0, len(kvs))
	for _, kv := range kvs {
		data, _ := encode(kv)
		hash := hashData(data)
		currentLevel = append(currentLevel, hash)
	}

	// 第二步：逐层构建Merkle树
	for len(currentLevel) > 1 {
		nextLevel := make([]common.Hash, 0, (len(currentLevel)+1)/2)
		for i := 0; i < len(currentLevel); i += 2 {
			var left, right common.Hash
			left = currentLevel[i]

			// 处理最后一个单独的节点
			if i+1 >= len(currentLevel) {
				bindingKey := calcBindingKey(left, h)
				info := &BindingInfo{Hash: common.Hash{}, IsRight: false}
				infoBytes := bindingInfo2Bytes(info)
				treeCache[bindingKey] = infoBytes
				nextLevel = append(nextLevel, left)
				continue
			}

			right = currentLevel[i+1]
			// 存储双向绑定关系到缓存，记录左右位置
			bindingKey1 := calcBindingKey(left, h)
			bindingKey2 := calcBindingKey(right, h)

			// 左节点绑定右节点
			info1 := &BindingInfo{Hash: right, IsRight: false}
			infoBytes1 := bindingInfo2Bytes(info1)
			treeCache[bindingKey1] = infoBytes1

			// 右节点绑定左节点
			info2 := &BindingInfo{Hash: left, IsRight: true}
			infoBytes2 := bindingInfo2Bytes(info2)
			treeCache[bindingKey2] = infoBytes2

			parentHash := calcParentHash(left, right)
			nextLevel = append(nextLevel, parentHash)
		}
		currentLevel = nextLevel
	}

	return currentLevel[0], treeCache
}

// DeleteBind 将待删除的绑定关系添加到缓存，并递归查找和删除父节点
func (kvt *KVTree) DeleteBind(ch uint64, kv *UpdateKV) {
	oh := kv.Height
	data, _ := encode(kv)
	hash := hashData(data)
	bindingKey := calcBindingKey(hash, oh)

	// 如果已经在删除缓存中，直接返回
	if _, exists := kvt.deleteCache[bindingKey]; exists {
		return
	}

	// 记录待删除的绑定关系
	kvt.deleteBindingCache(ch, bindingKey)

	// 递归查找和删除父节点
	current := hash
	for {
		// 获取当前节点的绑定信息
		currentBindingKey := calcBindingKey(current, oh)
		infoBytes, err := kvt.db.Get(currentBindingKey.Bytes())
		if err != nil || len(infoBytes) == 0 {
			break
		}

		var info = bytes2BindingInfo(infoBytes)

		// 检查是否是单节点
		if info.Hash == (common.Hash{}) {
			// 单节点情况，继续向上查找父节点
			current = hashData(current.Bytes())
			bindingKey = calcBindingKey(current, oh)

			kvt.deleteBindingCache(ch, bindingKey)
			continue
		}

		// 检查兄弟节点是否已被删除
		siblingBindingKey := calcBindingKey(info.Hash, oh)

		// 先检查删除缓存
		if _, siblingInCache := kvt.deleteCache[siblingBindingKey]; siblingInCache {
			// 计算父节点时考虑左右顺序
			var parentHash common.Hash
			if info.IsRight {
				parentHash = calcParentHash(info.Hash, current)
			} else {
				parentHash = calcParentHash(current, info.Hash)
			}
			current = parentHash
			bindingKey = calcBindingKey(current, oh)

			kvt.deleteBindingCache(ch, bindingKey)
			continue
		}

		// 如果缓存中未标记删除，检查数据库
		siblingBytes, err := kvt.db.Get(siblingBindingKey.Bytes())
		if err != nil || len(siblingBytes) == 0 {
			// 计算父节点时考虑左右顺序
			var parentHash common.Hash
			if info.IsRight {
				parentHash = calcParentHash(info.Hash, current)
			} else {
				parentHash = calcParentHash(current, info.Hash)
			}
			current = parentHash
			bindingKey = calcBindingKey(current, oh)
			kvt.deleteBindingCache(ch, bindingKey)
			continue
		}

		break
	}
}

func (kvt *KVTree) deleteBindingCache(ch uint64, bindingKey common.Hash) {
	kvt.deleteCache[bindingKey] = struct{}{}
	if _, exist := kvt.heightCache[ch]; !exist {
		kvt.heightCache[ch] = make([]common.Hash, 0)
	}
	for _, existingKey := range kvt.heightCache[ch] {
		if existingKey == bindingKey {
			return
		}
	}
	kvt.heightCache[ch] = append(kvt.heightCache[ch], bindingKey)
}

// DeleteHeight 删除指定高度的所有缓存的绑定关系
func (kvt *KVTree) DeleteHeight(height uint64) {
	if _, exists := kvt.heightCache[height]; !exists {
		return
	}

	// 遍历所有待删除的绑定关系
	for _, key := range kvt.heightCache[height] {
		kvt.deleteDB(key.Bytes())
		delete(kvt.deleteCache, key)
	}

	delete(kvt.heightCache, height)
}

// Proof 根据UpdateKV构造证明并返回根哈希
func (kvt *KVTree) Proof(kv *UpdateKV) common.Hash {
	data, _ := encode(kv)
	current := hashData(data)
	height := kv.Height

	for {
		// 计算当前节点的绑定key
		bindingKey := calcBindingKey(current, height)

		// 先检查是否在删除缓存中
		if _, exists := kvt.deleteCache[bindingKey]; exists {
			return common.Hash{}
		}

		// 检查绑定缓存
		var infoBytes []byte

		var err error
		infoBytes, err = kvt.db.Get(bindingKey.Bytes())
		if err != nil || len(infoBytes) == 0 {
			break
		}

		var info BindingInfo
		if err := rlp.DecodeBytes(infoBytes, &info); err != nil {
			break
		}

		// 检查是否是单节点
		if info.Hash == (common.Hash{}) {
			current = hashData(current.Bytes())
			continue
		}

		// 计算父节点时考虑左右顺序
		if info.IsRight {
			current = calcParentHash(info.Hash, current)
		} else {
			current = calcParentHash(current, info.Hash)
		}
	}
	return current
}

// ----------------------------------------------------------------------------------------------
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type hasher struct {
	sha keccakState
}

var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{
			sha: sha3.NewLegacyKeccak256().(keccakState),
		}
	},
}

func newHasher() *hasher {
	h := hasherPool.Get().(*hasher)
	return h
}

func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

func HashKey(key []byte) common.Hash {
	h := newHasher()
	h.sha.Reset()
	h.sha.Write(key)
	n := common.Hash{}
	h.sha.Sum(n[:0])
	returnHasherToPool(h)
	return n
}

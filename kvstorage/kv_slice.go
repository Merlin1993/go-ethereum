package kvsrotage

import (
	"bytes"
	"github.com/ethereum/go-ethereum/common"
	"sort"
)

// 一份kv存储的切片，用于某个区块的状态存取
type KVSlice struct {
	kd            *KVDatabase
	parent        common.Hash //缓存父状态
	currentHeight uint64
	state         map[common.Hash][]byte
	merkleTree    []byte                 // 绑定的Merkle Tree
	keyIndices    map[common.Hash]uint32 // 存储每个key对应的索引
}

func NewState(kd *KVDatabase, parentRoot common.Hash, currentHeight uint64) *KVSlice {
	st := &KVSlice{
		kd:            kd,
		parent:        parentRoot,
		currentHeight: currentHeight,
		state:         make(map[common.Hash][]byte),
		keyIndices:    make(map[common.Hash]uint32),
	}
	return st
}

// 读取余额，如果读取不到就从父状态中读取，如果没有父状态，就从世界状态中读取，并缓存
func (s *KVSlice) getState(key []byte) []byte {
	hashKey := HashKey(key)
	if v, ok := s.state[hashKey]; ok {
		return v
	}
	return s.getCommitedState(key)
}

func (s *KVSlice) getCommitedState(key []byte) []byte {
	parentState := s.kd.getParentState(s.parent)
	if parentState != nil {
		v := parentState.getState(key)
		return v
	}
	hashKey := HashKey(key)
	if v, ok := s.kd.getValue(hashKey[:]); ok {
		return v
	} else {
		return nil
	}
}

func (s *KVSlice) WriteState(key []byte, value []byte) {
	hashKey := HashKey(key)
	s.state[hashKey] = value
}

func (s *KVSlice) Copy() *KVSlice {
	st := make(map[common.Hash][]byte)
	for k, v := range s.state {
		st[k] = v
	}

	// 复制索引映射
	indices := make(map[common.Hash]uint32)
	for k, idx := range s.keyIndices {
		indices[k] = idx
	}

	//bindings可以直接复制，是因为他不会进行修改
	return &KVSlice{
		kd:            s.kd,
		parent:        s.parent,
		currentHeight: s.currentHeight,
		state:         st,
		merkleTree:    s.merkleTree,
		keyIndices:    indices,
	}
}

func (s *KVSlice) knotBlock(rootHash common.Hash) {
	cpy := s.Copy()
	s.kd.setDirtyState(rootHash, cpy)
}

// Hash 计算KV列表的哈希值
func (s *KVSlice) Hash() common.Hash {
	if s.state == nil {
		return common.Hash{}
	}

	// 将map中的key收集到切片中
	keys := make([]common.Hash, 0, len(s.state))
	for k := range s.state {
		keys = append(keys, k)
	}

	// 对byte切片进行排序
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i].Bytes(), keys[j].Bytes()) < 0
	})

	// 根据排序后的key构建UpdateKV切片
	sortKV := make([]*UpdateKV, 0, len(keys))
	for i, key := range keys {
		sortKV = append(sortKV, &UpdateKV{
			Key:    key,
			Height: s.currentHeight,
			Value:  s.state[key],
			Index:  uint32(i), // 设置索引
		})
		// 保存键对应的索引，以便后续在Commit中使用
		s.keyIndices[key] = uint32(i)
	}

	hash, mt := s.kd.GenerateHash(s.currentHeight, sortKV)

	s.merkleTree = mt
	// 计算并返回哈希值
	return hash
}

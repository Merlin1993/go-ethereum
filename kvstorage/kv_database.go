package kvsrotage

import (
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/ethdb"
	"sync"
)

// 数据库的实现适配类
type KVDatabase struct {
	db ethdb.Database
	KVTree
	dirty map[common.Hash]*KVSlice //未确定的区块的状态,通过状态的根来映射
	lock  sync.RWMutex
	cache *lru.Cache[string, []byte] // 添加LRU缓存
}

func NewKVDatabase(db ethdb.Database) *KVDatabase {
	ws := &KVDatabase{
		db:     db,
		KVTree: NewKVTree(db),
		dirty:  make(map[common.Hash]*KVSlice),
		cache:  lru.NewCache[string, []byte](100000), // 创建一个容量为10000的LRU缓存
	}
	return ws
}

func (kd *KVDatabase) CreateDBSnapshot(parentRoot common.Hash, currentHeight uint64) (*KVSnapshot, error) {
	st := NewState(kd, parentRoot, currentHeight)
	stateSnapshot := NewKVSnapshot(st)
	return stateSnapshot, nil
}

// 缓存已经使用过世界状态切片
func (kd *KVDatabase) setDirtyState(rootHash common.Hash, st *KVSlice) {
	kd.dirty[rootHash] = st
}

func (kd *KVDatabase) DiskDB() ethdb.Database {
	return kd.db
}

// 返回一个深拷贝
func (kd *KVDatabase) CopySnapshot(snapshot *KVSlice) *KVSlice {
	return snapshot.Copy()
}

type StoreKV struct {
	Height uint64
	Index  uint32
	Value  []byte
}

func storeKV2Bytes(s *StoreKV) []byte {
	// 创建一个字节数组，长度为8（uint64的字节数）加上Value的长度
	result := make([]byte, 4+8+len(s.Value))
	// 将Height转换为字节并写入结果数组
	binary.BigEndian.PutUint64(result[:8], s.Height)
	binary.BigEndian.PutUint32(result[8:12], s.Index)
	// 将Value写入结果数组
	copy(result[12:], s.Value)
	return result
}

func bytes2StoreKV(data []byte) *StoreKV {
	// 从字节数组中提取Height
	height := binary.BigEndian.Uint64(data[:8])
	index := binary.BigEndian.Uint32(data[8:12])
	// 提取Value
	value := data[12:]
	return &StoreKV{
		Height: height,
		Index:  index,
		Value:  value,
	}
}

const doRead = false
const doDelete = false
const doProof = false
const cacheHeight = 0

func (kd *KVDatabase) Commit(rootHash common.Hash) error {
	kd.lock.Lock()
	defer kd.lock.Unlock()
	dirty := kd.dirty[rootHash]

	batch := kd.db.NewBatch()
	if dirty != nil {
		listUpdatedKV := make([]*UpdateKV, 0, len(dirty.state))
		for k, v := range dirty.state {
			if doRead {
				oldData, _ := kd.getStoredValue(k.Bytes())
				if len(oldData) != 0 {
					skv := bytes2StoreKV(oldData)
					listUpdatedKV = append(listUpdatedKV, &UpdateKV{
						Key:    k,
						Height: skv.Height,
						Index:  skv.Index,
						Value:  skv.Value,
					})
				}
			}

			storeData := storeKV2Bytes(&StoreKV{
				Height: dirty.currentHeight,
				Index:  dirty.keyIndices[k],
				Value:  v,
			})
			batch.Put(k[:], storeData)

			// 更新缓存
			kd.cache.Add(string(k[:]), storeData)
		}
		delete(kd.dirty, rootHash)
		//补充记录MT的数据
		if doProof {
			batch.Put(calcTreeKey(dirty.currentHeight), dirty.merkleTree)
		}

		if doDelete {
			//先预删除旧数据
			kd.DeleteBind(dirty.currentHeight, listUpdatedKV)

			//再删除之前的数据
			kd.DeleteHeight(dirty.currentHeight - cacheHeight)
		}
	}
	err := batch.Write()
	if err != nil {
		return err
	}
	return nil
}

func (kd *KVDatabase) getParentState(parentHash common.Hash) *KVSlice {
	parentState := kd.dirty[parentHash]
	return parentState
}

func (kd *KVDatabase) getValue(key []byte) ([]byte, bool) {
	sv, _ := kd.getStoredValue(key)
	if len(sv) >= 12 { // 确保数据长度足够
		skv := bytes2StoreKV(sv)
		// 将解析后的值添加到缓存中
		kd.cache.Add(string(key), sv)
		return skv.Value, true
	}

	return nil, false
}

func (kd *KVDatabase) getStoredValue(key []byte) ([]byte, bool) {
	if value, ok := kd.cache.Get(string(key)); ok {
		return value, true
	}

	// 如果缓存中没有，则从数据库中获取
	v, err := kd.db.Get(key)
	if err != nil {
		return nil, false
	}
	kd.cache.Add(string(key), v)
	return v, true
}

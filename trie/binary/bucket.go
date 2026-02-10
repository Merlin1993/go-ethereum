package binary

import (
	"bytes"
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/binary/cuckoo"
	"github.com/ethereum/go-ethereum/trie/binary/ecmh"
)

// ArchivedKV 存储归档在桶内的数据项
type ArchivedKV struct {
	Suffix     []byte // 数据的原始后缀（相对于桶前缀）
	SuffixBits int    // 后缀的位数
	Value      []byte // 数据的具体值哈希
}

// serializeArchivedKV 将 ArchivedKV 列表序列化为二进制数据
func (s *Shard) serializeArchivedKV(items []ArchivedKV) ([]byte, error) {
	var buf bytes.Buffer
	scratch := make([]byte, binary.MaxVarintLen64)

	nBits := binary.PutUvarint(scratch, uint64(len(items)))
	buf.Write(scratch[:nBits])

	for _, kv := range items {
		nBits = binary.PutUvarint(scratch, uint64(kv.SuffixBits))
		buf.Write(scratch[:nBits])
		buf.Write(kv.Suffix)
		buf.WriteByte(byte(len(kv.Value)))
		buf.Write(kv.Value)
	}
	return buf.Bytes(), nil
}

// deserializeArchivedKV 从二进制数据反序列化为 ArchivedKV 列表
func (s *Shard) deserializeArchivedKV(data []byte) ([]ArchivedKV, error) {
	reader := bytes.NewReader(data)
	count, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}

	items := make([]ArchivedKV, 0, count)
	for i := uint64(0); i < count; i++ {
		suffixBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}
		suffixLen := (int(suffixBits) + 7) / 8
		suffix := make([]byte, suffixLen)
		if _, err := reader.Read(suffix); err != nil {
			return nil, err
		}

		valLen, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		val := make([]byte, int(valLen))
		if _, err := reader.Read(val); err != nil {
			return nil, err
		}

		items = append(items, ArchivedKV{
			Suffix:     suffix,
			SuffixBits: int(suffixBits),
			Value:      val,
		})
	}
	return items, nil
}

// blindAppendToBucket 实现“盲追加”：只更新元数据（过滤器、ECMH、Count），无需加载原始数据。
func (s *Shard) blindAppendToBucket(bucket *ArchiveBucketNode, newItems []ArchivedKV) {
	// 1. 增量更新布谷鸟过滤器
	filter := cuckoo.New()
	if len(bucket.Filter) > 0 {
		filter.Decode(bucket.Filter)
	}
	for _, it := range newItems {
		filter.Insert(it.Suffix)
	}
	bucket.Filter = filter.Encode()

	// 2. 增量更新 ECMH 承诺
	hashes := make([]common.Hash, 0, len(newItems))
	for _, it := range newItems {
		// K + Hash(V)
		h := crypto.Keccak256Hash(append(it.Suffix, it.Value...))
		hashes = append(hashes, h)
	}
	committer := ecmh.New()
	newCommitment, _ := committer.Add(bucket.Commitment, hashes)
	bucket.Commitment = newCommitment

	// 3. 更新计数
	bucket.Count += uint64(len(newItems))

	// 4. 记录追加任务
	oldHash := bucket.Hash()

	// 清除旧哈希以重新计算元数据哈希
	bucket.SetHash(nil)
	meta, _ := bucket.Serialize()
	newHash := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(newHash)

	// [优化]：如果 oldHash 已经在 pending 队列中，直接在内存中合并，保持 pending 状态扁平
	if data, ok := s.pendingArchives[string(oldHash)]; ok {
		// 已经在全量缓存中
		items, _ := s.deserializeArchivedKV(data)
		items = append(items, newItems...)
		newData, _ := s.serializeArchivedKV(items)
		s.pendingArchives[string(newHash)] = newData
		delete(s.pendingArchives, string(oldHash))
	} else if task, ok := s.pendingAppends[string(oldHash)]; ok {
		// 已经在追加缓存中，合并到该任务
		task.newItems = append(task.newItems, newItems...)
		s.pendingAppends[string(newHash)] = task
		delete(s.pendingAppends, string(oldHash))
	} else {
		// 全新追加任务
		s.pendingAppends[string(newHash)] = appendTask{
			oldHash:  oldHash,
			newItems: newItems,
		}
	}
}

// recomputeBucket 重新计算桶的承诺部分 (Filter, ECMH, Count)
func (s *Shard) recomputeBucket(bucket *ArchiveBucketNode, items []ArchivedKV) {
	filter := cuckoo.New()
	var hashes []common.Hash

	for _, item := range items {
		// 这里使用相对于分片起始深度的全路径位进行过滤和承诺
		filter.Insert(item.Suffix)

		// ECMH: K + Hash(V)
		h := crypto.Keccak256Hash(append(item.Suffix, item.Value...))
		hashes = append(hashes, h)
	}

	bucket.Filter = filter.Encode()
	bucket.Count = uint64(len(items))

	// ECMH 承诺
	comm, _ := s.ecmh.Add(nil, hashes)
	bucket.Commitment = comm

	// 记录待入库的原始数据
	// 提前计算桶在 Commit 后的哈希，用于 pendingArchives 索引
	// 注意：哈希前必须清除老的 hash 字段，确保哈希只针对元数据内容
	bucket.SetHash(nil)
	meta, _ := bucket.Serialize()
	h := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(h)

	bucketData, _ := s.serializeArchivedKV(items)
	s.pendingArchives[string(h)] = bucketData
}

package binary

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"time"

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
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()

	// 1. 增量更新布谷鸟过滤器
	if bucket.cachedFilter != nil {
		for _, it := range newItems {
			keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
			bucket.cachedFilter.Insert(keyWithLen)
		}
		bucket.Filter = bucket.cachedFilter.Encode()
	} else {
		filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
		if len(bucket.Filter) > 0 {
			filter.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots)
		}
		for _, it := range newItems {
			keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
			filter.Insert(keyWithLen)
		}
		bucket.Filter = filter.Encode()
	}

	// 2. 增量更新 ECMH 承诺
	hashes := make([]common.Hash, 0, len(newItems))
	for _, it := range newItems {
		// [FIX] 一致性：ECMH 必须包含 SuffixBits
		keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
		h := crypto.Keccak256Hash(append(keyWithLen, it.Value...))
		hashes = append(hashes, h)
	}
	committer := ecmh.New()
	newCommitment, _ := committer.Add(bucket.Commitment, hashes)
	bucket.Commitment = newCommitment

	// 3. 更新计数
	bucket.Count += uint64(len(newItems))

	// 4. 更新缓存的数据项（如果已加载）
	if bucket.cachedItems != nil {
		bucket.cachedItems = append(bucket.cachedItems, newItems...)
	}

	// 5. 记录追加任务
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

// blindDeleteFromBucket 实现“盲删除”：增量更新元数据（过滤器、ECMH、Count），无需加载原始数据。
func (s *Shard) blindDeleteFromBucket(bucket *ArchiveBucketNode, deleteItems []ArchivedKV) {
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()

	// 1. 增量更新布谷鸟过滤器
	if bucket.cachedFilter != nil {
		for _, it := range deleteItems {
			keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
			bucket.cachedFilter.Delete(keyWithLen)
		}
		bucket.Filter = bucket.cachedFilter.Encode()
	} else {
		filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
		if len(bucket.Filter) > 0 {
			filter.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots)
		}
		for _, it := range deleteItems {
			keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
			filter.Delete(keyWithLen)
		}
		bucket.Filter = filter.Encode()
	}

	// 2. 增量更新 ECMH 承诺 (减法)
	hashes := make([]common.Hash, 0, len(deleteItems))
	for _, it := range deleteItems {
		// [FIX] 一致性：ECMH 必须包含 SuffixBits
		keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
		h := crypto.Keccak256Hash(append(keyWithLen, it.Value...))
		hashes = append(hashes, h)
	}
	committer := ecmh.New()
	newCommitment, _ := committer.Delete(bucket.Commitment, hashes)
	bucket.Commitment = newCommitment

	// 3. 更新计数
	bucket.Count -= uint64(len(deleteItems))

	// 4. 更新缓存的数据项（如果已加载）
	if bucket.cachedItems != nil {
		newItems := make([]ArchivedKV, 0, len(bucket.cachedItems))
		for _, it := range bucket.cachedItems {
			found := false
			for _, del := range deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		bucket.cachedItems = newItems
	}

	// 4. 记录删除任务
	oldHash := bucket.Hash()

	// 清除旧哈希以重新计算元数据哈希
	bucket.SetHash(nil)
	meta, _ := bucket.Serialize()
	newHash := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(newHash)

	// [优化]：如果 oldHash 已经在 pendingArchives 队列中 (说明是本批次新创建的桶)，直接处理
	if data, ok := s.pendingArchives[string(oldHash)]; ok {
		items, _ := s.deserializeArchivedKV(data)
		// 简单过滤掉要删除的项
		newItems := make([]ArchivedKV, 0, len(items))
		for _, it := range items {
			found := false
			for _, del := range deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		newData, _ := s.serializeArchivedKV(newItems)
		s.pendingArchives[string(newHash)] = newData
		delete(s.pendingArchives, string(oldHash))
	} else if task, ok := s.pendingAppends[string(oldHash)]; ok {
		// 已经在追加缓存中，尝试从待追加项中移除
		newItems := make([]ArchivedKV, 0, len(task.newItems))
		for _, it := range task.newItems {
			found := false
			for _, del := range deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		task.newItems = newItems
		s.pendingAppends[string(newHash)] = task
		delete(s.pendingAppends, string(oldHash))
	} else if task, ok := s.pendingDeletes[string(oldHash)]; ok {
		// 已经在删除缓存中，累加删除任务
		task.deleteItems = append(task.deleteItems, deleteItems...)
		s.pendingDeletes[string(newHash)] = task
		delete(s.pendingDeletes, string(oldHash))
	} else {
		// 全新删除任务
		s.pendingDeletes[string(newHash)] = deleteTask{
			oldHash:     oldHash,
			deleteItems: deleteItems,
		}
	}
}

// recomputeBucket 重新计算桶的承诺部分 (Filter, ECMH, Count)
func (s *Shard) recomputeBucket(bucket *ArchiveBucketNode, items []ArchivedKV) {
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()
	bucket.dirty = true

	filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
	var hashes []common.Hash

	for _, item := range items {
		// Include SuffixBits to avoid ambiguity (e.g. 1-bit '1' vs 8-bit '10000000')
		keyWithLen := append([]byte{byte(item.SuffixBits)}, item.Suffix...)
		filter.Insert(keyWithLen)

		// ECMH: K + Hash(V)
		h := crypto.Keccak256Hash(append(keyWithLen, item.Value...))
		hashes = append(hashes, h)
	}

	bucket.Filter = filter.Encode()
	bucket.Count = uint64(len(items))

	// 更新缓存
	bucket.cachedFilter = filter
	bucket.cachedItems = items

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
	if s.pendingArchives == nil {
		s.pendingArchives = make(map[string][]byte)
	}
	s.pendingArchives[string(h)] = bucketData
}

// verifyBucket 验证桶的 ECMH 承诺是否正确。返回布尔值及验证耗时（纳秒）。
func (s *Shard) verifyBucket(bucket *ArchiveBucketNode) (bool, int64) {
	start := time.Now()
	bucket.cacheMu.RLock()
	items := bucket.cachedItems
	bucket.cacheMu.RUnlock()

	if items == nil {
		bucketData, err := s.getBucketData(bucket.Hash())
		if err != nil {
			return false, time.Since(start).Nanoseconds()
		}
		items, err = s.deserializeArchivedKV(bucketData)
		if err != nil {
			return false, time.Since(start).Nanoseconds()
		}
		// 不需要在这里写回缓存，因为 load 过程通常已经处理了缓存。
	}

	hashes := make([]common.Hash, 0, len(items))
	for _, it := range items {
		// [FIX] 一致性：ECMH 必须包含 SuffixBits
		keyWithLen := append([]byte{byte(it.SuffixBits)}, it.Suffix...)
		h := crypto.Keccak256Hash(append(keyWithLen, it.Value...))
		hashes = append(hashes, h)
	}

	committer := ecmh.New()
	ok := committer.Verify(hashes, bucket.Commitment)
	dur := time.Since(start).Nanoseconds()

	// Track Max time
	for {
		oldMax := atomic.LoadInt64(&common.BinaryProofVerifTimeMax)
		if dur <= oldMax || atomic.CompareAndSwapInt64(&common.BinaryProofVerifTimeMax, oldMax, dur) {
			break
		}
	}

	// Track proof sizes (bucket size)
	if ok {
		var bucketSize int64
		for _, it := range items {
			bucketSize += int64(len(it.Suffix) + len(it.Value))
		}
		bucketSize += 32 // Commitment
		bucketSize += int64(len(bucket.Filter))

		atomic.AddInt64(&common.BinaryTotalProofSize, bucketSize)
		atomic.AddInt64(&common.BinaryBlockProofSize, bucketSize)

		common.BinaryStatsMu.Lock()
		common.BinaryItemProofSizes = append(common.BinaryItemProofSizes, bucketSize)
		if bucketSize > common.BinaryItemProofSizeMax {
			common.BinaryItemProofSizeMax = bucketSize
		}
		if common.BinaryItemProofSizeMin == 0 || bucketSize < common.BinaryItemProofSizeMin {
			common.BinaryItemProofSizeMin = bucketSize
		}
		common.BinaryStatsMu.Unlock()
	}

	return ok, dur
}

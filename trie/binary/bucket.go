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
	size := uvarintLen(uint64(len(items)))
	for _, kv := range items {
		size += uvarintLen(uint64(kv.SuffixBits))
		size += len(kv.Suffix) + 1 + len(kv.Value)
	}

	buf := make([]byte, 0, size)
	var scratch [binary.MaxVarintLen64]byte

	nBits := binary.PutUvarint(scratch[:], uint64(len(items)))
	buf = append(buf, scratch[:nBits]...)

	for _, kv := range items {
		nBits = binary.PutUvarint(scratch[:], uint64(kv.SuffixBits))
		buf = append(buf, scratch[:nBits]...)
		buf = append(buf, kv.Suffix...)
		buf = append(buf, byte(len(kv.Value)))
		buf = append(buf, kv.Value...)
	}
	return buf, nil
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
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

func (s *Shard) shouldCacheArchivedItems(count int) bool {
	switch limit := s.config.ArchiveItemCacheLimit; {
	case limit < 0:
		return true
	case limit == 0:
		return false
	default:
		return count <= limit
	}
}

func archiveItemKey(suffixBits int, suffix []byte) []byte {
	return appendArchiveItemKey(make([]byte, 0, binary.MaxVarintLen64+len(suffix)), suffixBits, suffix)
}

func appendArchiveItemKey(dst []byte, suffixBits int, suffix []byte) []byte {
	if suffixBits < 0 {
		panic("archiveItemKey: negative suffix bits")
	}
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(suffixBits))
	dst = append(dst[:0], scratch[:n]...)
	dst = append(dst, suffix...)
	return dst
}

func appendArchiveItemHashInput(dst []byte, keyWithLen []byte, value []byte) []byte {
	dst = append(dst[:0], keyWithLen...)
	dst = append(dst, value...)
	return dst
}

func (s *Shard) ensureBucketHash(bucket *ArchiveBucketNode) []byte {
	if bucket == nil {
		return nil
	}
	if h := bucket.Hash(); len(h) > 0 {
		return h
	}
	meta, err := bucket.Serialize()
	if err != nil {
		return nil
	}
	h := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(h)
	return h
}

// blindAppendToBucket 实现“盲追加”：只更新元数据（过滤器、ECMH、Count），无需加载原始数据。
func (s *Shard) blindAppendToBucket(bucket *ArchiveBucketNode, newItems []ArchivedKV) {
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()

	oldHash := s.ensureBucketHash(bucket)

	var keyBuf []byte
	var hashBuf []byte

	// 1. 增量更新布谷鸟过滤器
	if bucket.cachedFilter != nil {
		for _, it := range newItems {
			keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
			bucket.cachedFilter.Insert(keyWithLen)
			keyBuf = keyWithLen
		}
		bucket.Filter = bucket.cachedFilter.Encode()
	} else {
		filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
		if len(bucket.Filter) > 0 {
			filter.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots)
		}
		for _, it := range newItems {
			keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
			filter.Insert(keyWithLen)
			keyBuf = keyWithLen
		}
		bucket.Filter = filter.Encode()
	}

	// 2. 增量更新 ECMH 承诺
	hashes := make([]common.Hash, 0, len(newItems))
	for _, it := range newItems {
		// [FIX] 一致性：ECMH 必须包含 SuffixBits
		keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
		hashInput := appendArchiveItemHashInput(hashBuf, keyWithLen, it.Value)
		h := crypto.Keccak256Hash(hashInput)
		hashes = append(hashes, h)
		keyBuf = keyWithLen
		hashBuf = hashInput
	}
	committer := ecmh.New()
	newCommitment, _ := committer.Add(bucket.Commitment, hashes)
	bucket.Commitment = newCommitment

	// 3. 更新计数
	bucket.Count += uint64(len(newItems))

	// 4. 更新缓存的数据项（如果已加载）
	if bucket.cachedItems != nil {
		cachedItems := append(bucket.cachedItems, newItems...)
		if s.shouldCacheArchivedItems(len(cachedItems)) {
			bucket.cachedItems = cachedItems
		} else {
			bucket.cachedItems = nil
		}
	}

	// 5. 记录追加任务
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

	oldHash := s.ensureBucketHash(bucket)

	// 1. 增量更新布谷鸟过滤器
	var keyBuf []byte
	var hashBuf []byte

	if bucket.cachedFilter != nil {
		for _, it := range deleteItems {
			keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
			bucket.cachedFilter.Delete(keyWithLen)
			keyBuf = keyWithLen
		}
		bucket.Filter = bucket.cachedFilter.Encode()
	} else {
		filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
		if len(bucket.Filter) > 0 {
			filter.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots)
		}
		for _, it := range deleteItems {
			keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
			filter.Delete(keyWithLen)
			keyBuf = keyWithLen
		}
		bucket.Filter = filter.Encode()
	}

	// 2. 增量更新 ECMH 承诺 (减法)
	hashes := make([]common.Hash, 0, len(deleteItems))
	for _, it := range deleteItems {
		// [FIX] 一致性：ECMH 必须包含 SuffixBits
		keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
		hashInput := appendArchiveItemHashInput(hashBuf, keyWithLen, it.Value)
		h := crypto.Keccak256Hash(hashInput)
		hashes = append(hashes, h)
		keyBuf = keyWithLen
		hashBuf = hashInput
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
		if s.shouldCacheArchivedItems(len(newItems)) {
			bucket.cachedItems = newItems
		} else {
			bucket.cachedItems = nil
		}
	}

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
	hashes := make([]common.Hash, 0, len(items))
	var keyBuf []byte
	var hashBuf []byte

	for _, item := range items {
		// Include SuffixBits to avoid ambiguity (e.g. 1-bit '1' vs 8-bit '10000000')
		keyWithLen := appendArchiveItemKey(keyBuf, item.SuffixBits, item.Suffix)
		filter.Insert(keyWithLen)

		// ECMH: K + Hash(V)
		hashInput := appendArchiveItemHashInput(hashBuf, keyWithLen, item.Value)
		h := crypto.Keccak256Hash(hashInput)
		hashes = append(hashes, h)
		keyBuf = keyWithLen
		hashBuf = hashInput
	}

	bucket.Filter = filter.Encode()
	bucket.Count = uint64(len(items))

	// 更新缓存
	bucket.cachedFilter = filter
	if s.shouldCacheArchivedItems(len(items)) {
		bucket.cachedItems = items
	} else {
		bucket.cachedItems = nil
	}

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
		bucketData, err := s.getBucketData(s.ensureBucketHash(bucket))
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
	var keyBuf []byte
	var hashBuf []byte
	for _, it := range items {
		// [FIX] 一致性：ECMH 必须包含 SuffixBits
		keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
		hashInput := appendArchiveItemHashInput(hashBuf, keyWithLen, it.Value)
		h := crypto.Keccak256Hash(hashInput)
		hashes = append(hashes, h)
		keyBuf = keyWithLen
		hashBuf = hashInput
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

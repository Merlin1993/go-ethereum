package archive

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	"github.com/ethereum/go-ethereum/trie/archive/ecmh"
)

// ArchivedKey stores the proof entry tracked by a minimalist archive bucket.
// The real value lives in the flat snapshot/value layer; ValueRef is the
// key-bound value commitment used by ECMH.
type ArchivedKey struct {
	Suffix     []byte
	SuffixBits int
	ValueRef   []byte
}

// ArchivedKV is the in-memory construction item used while rebuilding buckets.
// Value stores the key-bound valueRef used to update ECMH.
type ArchivedKV struct {
	Suffix     []byte // 数据的原始后缀（相对于桶前缀）
	SuffixBits int    // 后缀的位数
	Value      []byte // 数据的具体值哈希
}

func archivedKeyFromKV(kv ArchivedKV) ArchivedKey {
	return ArchivedKey{
		Suffix:     common.CopyBytes(kv.Suffix),
		SuffixBits: kv.SuffixBits,
		ValueRef:   common.CopyBytes(kv.Value),
	}
}

func (s *Shard) bucketCommitmentPoint(bucket *ArchiveBucketNode) (*ecmh.Point, error) {
	if bucket == nil {
		return nil, nil
	}
	bucket.cacheMu.RLock()
	point := bucket.cachedCommitmentPoint
	bucket.cacheMu.RUnlock()
	if point != nil {
		return point, nil
	}
	if s.pointCache != nil {
		if point, ok := s.pointCache.get(bucket.Commitment); ok {
			recordCommitmentPointCacheLookupIfEnabled(s.config, true)
			bucket.cacheMu.Lock()
			if bucket.cachedCommitmentPoint != nil {
				point = bucket.cachedCommitmentPoint
			} else {
				bucket.cachedCommitmentPoint = point
			}
			bucket.cacheMu.Unlock()
			return point, nil
		}
		recordCommitmentPointCacheLookupIfEnabled(s.config, false)
	}
	point, err := s.ecmh.DecodePoint(bucket.Commitment)
	if err != nil {
		return nil, err
	}
	if s.pointCache != nil {
		s.pointCache.add(bucket.Commitment, point)
	}
	bucket.cacheMu.Lock()
	if bucket.cachedCommitmentPoint != nil {
		point = bucket.cachedCommitmentPoint
	} else {
		bucket.cachedCommitmentPoint = point
	}
	bucket.cacheMu.Unlock()
	return point, nil
}

func (s *Shard) setBucketCommitmentPoint(bucket *ArchiveBucketNode, point *ecmh.Point) {
	if bucket == nil {
		return
	}
	bucket.cacheMu.Lock()
	bucket.cachedCommitmentPoint = point
	bucket.cacheMu.Unlock()
}

func (s *Shard) clearBucketCommitmentPoint(bucket *ArchiveBucketNode) {
	if bucket == nil {
		return
	}
	bucket.cacheMu.Lock()
	bucket.cachedCommitmentPoint = nil
	bucket.cacheMu.Unlock()
}

func archivedKVFromKey(key ArchivedKey) ArchivedKV {
	return ArchivedKV{
		Suffix:     common.CopyBytes(key.Suffix),
		SuffixBits: key.SuffixBits,
		Value:      common.CopyBytes(key.ValueRef),
	}
}

func archivedKeyEqual(a, b ArchivedKey) bool {
	return a.SuffixBits == b.SuffixBits && bytes.Equal(a.Suffix, b.Suffix)
}

func archivedKeyMatchesKV(key ArchivedKey, kv ArchivedKV) bool {
	return key.SuffixBits == kv.SuffixBits && bytes.Equal(key.Suffix, kv.Suffix)
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

var valueRefDomain = []byte{'B', 'V', 'R', '1'}

func valueRefForKeyValue(key []byte, value []byte) []byte {
	var scratch [binary.MaxVarintLen64]byte
	buf := make([]byte, 0, len(valueRefDomain)+2*binary.MaxVarintLen64+len(key)+len(value))
	buf = append(buf, valueRefDomain...)
	n := binary.PutUvarint(scratch[:], uint64(len(key)))
	buf = append(buf, scratch[:n]...)
	buf = append(buf, key...)
	n = binary.PutUvarint(scratch[:], uint64(len(value)))
	buf = append(buf, scratch[:n]...)
	buf = append(buf, value...)
	h := crypto.Keccak256Hash(buf)
	return h.Bytes()
}

func (s *Shard) archivePointHash(bucket *ArchiveBucketNode, key ArchivedKey, valueRef []byte, keyBuf []byte, hashBuf []byte) (common.Hash, []byte, []byte) {
	fullKey, fullBits := s.prependPath(key.Suffix, key.SuffixBits, bucket.Path, bucket.PathBits)
	keyWithLen := appendArchiveItemKey(keyBuf, fullBits, fullKey)
	hashInput := appendArchiveItemHashInput(hashBuf, keyWithLen, valueRef)
	return crypto.Keccak256Hash(hashInput), keyWithLen, hashInput
}

func (s *Shard) bucketKeys(bucket *ArchiveBucketNode) ([]ArchivedKey, error) {
	if bucket == nil {
		return nil, nil
	}
	if len(bucket.Keys) > 0 || bucket.Count == 0 {
		return bucket.Keys, nil
	}
	data, err := s.getBucketData(s.ensureBucketHash(bucket))
	if err != nil {
		return nil, err
	}
	items, err := s.deserializeArchivedKV(data)
	if err != nil {
		return nil, err
	}
	keys := make([]ArchivedKey, len(items))
	for i, item := range items {
		keys[i] = archivedKeyFromKV(item)
	}
	bucket.Keys = keys
	return keys, nil
}

func (s *Shard) ensureBucketKeyList(bucket *ArchiveBucketNode) error {
	if bucket == nil || len(bucket.Keys) > 0 || bucket.Count == 0 {
		return nil
	}
	_, err := s.bucketKeys(bucket)
	return err
}

func (s *Shard) bucketItemsWithValueRefs(bucket *ArchiveBucketNode) ([]ArchivedKV, error) {
	if bucket == nil {
		return nil, nil
	}
	bucket.cacheMu.RLock()
	if bucket.cachedItems != nil && len(bucket.cachedItems) == int(bucket.Count) {
		items := make([]ArchivedKV, len(bucket.cachedItems))
		copy(items, bucket.cachedItems)
		bucket.cacheMu.RUnlock()
		return items, nil
	}
	bucket.cacheMu.RUnlock()

	if len(bucket.Keys) > 0 || bucket.Count == 0 {
		items := make([]ArchivedKV, 0, len(bucket.Keys))
		for _, key := range bucket.Keys {
			items = append(items, archivedKVFromKey(key))
		}
		return items, nil
	}
	data, err := s.getBucketData(s.ensureBucketHash(bucket))
	if err != nil {
		return nil, err
	}
	items, err := s.deserializeArchivedKV(data)
	if err != nil {
		return nil, err
	}
	return items, nil
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
func (s *Shard) blindAppendToBucket(bucket *ArchiveBucketNode, newItems []ArchivedKV) bool {
	limit := s.config.ResolveArchiveBucketSize()
	if limit > 0 && bucket.Count+uint64(len(newItems)) > uint64(limit) {
		return false
	}

	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()
	if err := s.ensureBucketKeyList(bucket); err != nil {
		return false
	}

	oldHash := s.ensureBucketHash(bucket)
	if s.pruning && (s.config == nil || s.config.PhysicalDelete) && len(oldHash) > 0 {
		s.staleSet[string(oldHash)] = struct{}{}
	}

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
	cacheItems := bucket.cachedItems != nil
	var valuedItems []ArchivedKV
	if cacheItems {
		valuedItems = make([]ArchivedKV, 0, len(newItems))
	}
	for _, it := range newItems {
		key := archivedKeyFromKV(it)
		h, nextKeyBuf, nextHashBuf := s.archivePointHash(bucket, key, it.Value, keyBuf, hashBuf)
		hashes = append(hashes, h)
		if cacheItems {
			valuedItems = append(valuedItems, ArchivedKV{
				Suffix:     common.CopyBytes(it.Suffix),
				SuffixBits: it.SuffixBits,
				Value:      common.CopyBytes(it.Value),
			})
		}
		keyBuf = nextKeyBuf
		hashBuf = nextHashBuf
	}
	newCommitment, point, _ := s.ecmh.AddWithPoint(bucket.Commitment, hashes)
	bucket.Commitment = newCommitment
	bucket.cachedCommitmentPoint = point

	// 3. 更新计数
	bucket.Count += uint64(len(newItems))
	for _, it := range newItems {
		bucket.Keys = append(bucket.Keys, archivedKeyFromKV(it))
	}

	// 4. 更新缓存的数据项（如果已加载）
	if cacheItems {
		cachedItems := append(bucket.cachedItems, valuedItems...)
		if s.shouldCacheArchivedItems(len(cachedItems)) {
			bucket.cachedItems = cachedItems
		} else {
			bucket.cachedItems = nil
		}
	}

	// 5. 记录追加任务
	// 清除旧哈希以重新计算元数据哈希
	bucket.SetHash(nil)
	bucket.SetDirty(true)
	bucket.invalidateMetaCache()
	meta, _ := bucket.Serialize()
	newHash := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(newHash)

	return true
}

// blindDeleteFromBucket 实现“盲删除”：增量更新元数据（过滤器、ECMH、Count），无需加载原始数据。
func (s *Shard) blindDeleteFromBucket(bucket *ArchiveBucketNode, deleteItems []ArchivedKV) {
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()
	if err := s.ensureBucketKeyList(bucket); err != nil {
		return
	}

	oldHash := s.ensureBucketHash(bucket)
	if s.pruning && (s.config == nil || s.config.PhysicalDelete) && len(oldHash) > 0 {
		s.staleSet[string(oldHash)] = struct{}{}
	}

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
		key := archivedKeyFromKV(it)
		h, nextKeyBuf, nextHashBuf := s.archivePointHash(bucket, key, it.Value, keyBuf, hashBuf)
		hashes = append(hashes, h)
		keyBuf = nextKeyBuf
		hashBuf = nextHashBuf
	}
	committer := ecmh.New()
	newCommitment, _ := committer.Delete(bucket.Commitment, hashes)
	bucket.Commitment = newCommitment
	bucket.cachedCommitmentPoint = nil

	// 3. 更新计数
	bucket.Count -= uint64(len(deleteItems))
	if len(bucket.Keys) > 0 {
		newKeys := make([]ArchivedKey, 0, len(bucket.Keys))
		for _, key := range bucket.Keys {
			found := false
			for _, del := range deleteItems {
				if archivedKeyMatchesKV(key, del) {
					found = true
					break
				}
			}
			if !found {
				newKeys = append(newKeys, key)
			}
		}
		bucket.Keys = newKeys
	}

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
	bucket.SetDirty(true)
	bucket.invalidateMetaCache()
	meta, _ := bucket.Serialize()
	newHash := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(newHash)
}

// recomputeBucket 重新计算桶的承诺部分 (Filter, ECMH, Count)
func (s *Shard) recomputeBucket(bucket *ArchiveBucketNode, items []ArchivedKV) {
	recordBucketRecomputeIfEnabled(s.config)
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()
	bucket.dirty = true

	filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
	hashes := make([]common.Hash, 0, len(items))
	keys := make([]ArchivedKey, 0, len(items))
	cacheItems := s.shouldCacheArchivedItems(len(items))
	var cachedItems []ArchivedKV
	if cacheItems {
		cachedItems = make([]ArchivedKV, 0, len(items))
	}
	var keyBuf []byte
	var hashBuf []byte

	for _, item := range items {
		// Include SuffixBits to avoid ambiguity (e.g. 1-bit '1' vs 8-bit '10000000')
		keyWithLen := appendArchiveItemKey(keyBuf, item.SuffixBits, item.Suffix)
		filter.Insert(keyWithLen)

		key := archivedKeyFromKV(item)
		h, _, nextHashBuf := s.archivePointHash(bucket, key, item.Value, keyBuf, hashBuf)
		hashes = append(hashes, h)
		keys = append(keys, key)
		if cacheItems {
			cachedItems = append(cachedItems, ArchivedKV{
				Suffix:     common.CopyBytes(item.Suffix),
				SuffixBits: item.SuffixBits,
				Value:      common.CopyBytes(item.Value),
			})
		}
		keyBuf = keyWithLen
		hashBuf = nextHashBuf
	}

	bucket.Filter = filter.Encode()
	bucket.Count = uint64(len(items))
	bucket.Keys = keys

	// 更新缓存
	bucket.cachedFilter = filter
	if cacheItems {
		bucket.cachedItems = cachedItems
	} else {
		bucket.cachedItems = nil
	}

	// ECMH 承诺
	comm, point, _ := s.ecmh.AddWithPoint(nil, hashes)
	bucket.Commitment = comm
	bucket.cachedCommitmentPoint = point

	// 记录待入库的原始数据
	// 提前计算桶在 Commit 后的哈希，用于 pendingArchives 索引
	// 注意：哈希前必须清除老的 hash 字段，确保哈希只针对元数据内容
	bucket.SetHash(nil)
	bucket.invalidateMetaCache()
	meta, _ := bucket.Serialize()
	h := append([]byte{}, s.hasher.Hash(meta)...)
	bucket.SetHash(h)
}

// verifyBucket 验证桶的 ECMH 承诺是否正确。返回布尔值及验证耗时（纳秒）。
func (s *Shard) verifyBucket(bucket *ArchiveBucketNode) (bool, int64) {
	start := time.Now()
	bucket.cacheMu.RLock()
	items := bucket.cachedItems
	bucket.cacheMu.RUnlock()

	if items == nil {
		var err error
		items, err = s.bucketItemsWithValueRefs(bucket)
		if err != nil {
			return false, time.Since(start).Nanoseconds()
		}
		// 不需要在这里写回缓存，因为 load 过程通常已经处理了缓存。
	}

	hashes := make([]common.Hash, 0, len(items))
	var keyBuf []byte
	var hashBuf []byte
	for _, it := range items {
		key := archivedKeyFromKV(it)
		h, nextKeyBuf, nextHashBuf := s.archivePointHash(bucket, key, it.Value, keyBuf, hashBuf)
		hashes = append(hashes, h)
		keyBuf = nextKeyBuf
		hashBuf = nextHashBuf
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

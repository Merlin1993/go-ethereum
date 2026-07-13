package archive

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	return key.SuffixBits == kv.SuffixBits &&
		bytes.Equal(key.Suffix, kv.Suffix) &&
		(len(kv.Value) == 0 || bytes.Equal(key.ValueRef, kv.Value))
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
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

type archiveDedupeEntry struct {
	item        ArchivedKV
	fullKey     []byte
	flatRef     []byte
	flatChecked bool
}

func (s *Shard) deduplicateArchiveItems(items []ArchivedKV, bucketPath []byte, bucketBits int) []ArchivedKV {
	if len(items) < 2 {
		return items
	}
	seen := make(map[string]int, len(items))
	entries := make([]archiveDedupeEntry, 0, len(items))
	duplicated := false
	for _, item := range items {
		fullKey, fullBits := s.prependPath(item.Suffix, item.SuffixBits, bucketPath, bucketBits)
		fullKey = s.prefixBits(fullKey, fullBits, nil)
		id := string(archiveItemKey(fullBits, fullKey))
		if idx, ok := seen[id]; ok {
			duplicated = true
			entry := &entries[idx]
			if s.preferArchiveDuplicate(item, entry) {
				entry.item = item
			}
			continue
		}
		seen[id] = len(entries)
		entries = append(entries, archiveDedupeEntry{
			item:    item,
			fullKey: fullKey,
		})
	}
	if !duplicated {
		return items
	}
	out := make([]ArchivedKV, len(entries))
	for i := range entries {
		out[i] = entries[i].item
	}
	return out
}

func (s *Shard) preferArchiveDuplicate(candidate ArchivedKV, current *archiveDedupeEntry) bool {
	flatRef, ok := s.archiveDedupeFlatRef(current)
	if ok {
		candidateMatches := bytes.Equal(candidate.Value, flatRef)
		currentMatches := bytes.Equal(current.item.Value, flatRef)
		if candidateMatches != currentMatches {
			return candidateMatches
		}
	}
	return true
}

func (s *Shard) archiveDedupeFlatRef(entry *archiveDedupeEntry) ([]byte, bool) {
	if entry == nil {
		return nil, false
	}
	if entry.flatChecked {
		return entry.flatRef, len(entry.flatRef) > 0
	}
	entry.flatChecked = true
	value, err := s.getFlatValue(entry.fullKey)
	if err != nil || value == nil {
		return nil, false
	}
	entry.flatRef = valueRefForKeyValue(entry.fullKey, value)
	return entry.flatRef, true
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
	if len(bucket.Keys) == int(bucket.Count) {
		return bucket.Keys, nil
	}
	if bucket.Count == 0 && len(bucket.Keys) == 0 {
		return nil, nil
	}
	return nil, errors.New("archive bucket key list is missing or incomplete")
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
	keys, err := s.bucketKeys(bucket)
	if err != nil {
		return nil, err
	}
	items := make([]ArchivedKV, 0, len(keys))
	for _, key := range keys {
		items = append(items, archivedKVFromKey(key))
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
	if len(newItems) == 0 {
		return true
	}
	limit := s.config.ResolveArchiveBucketSize()
	if err := s.ensureBucketKeyList(bucket); err != nil {
		return false
	}
	existingItems := make([]ArchivedKV, 0, len(bucket.Keys)+len(newItems))
	for _, key := range bucket.Keys {
		existingItems = append(existingItems, archivedKVFromKey(key))
	}
	combinedItems := append(existingItems, newItems...)
	dedupedItems := s.deduplicateArchiveItems(combinedItems, bucket.Path, bucket.PathBits)
	if limit > 0 && len(dedupedItems) > limit {
		return false
	}
	if len(dedupedItems) != len(combinedItems) {
		oldHash := s.ensureBucketHash(bucket)
		if s.pruning && (s.config == nil || s.config.PhysicalDelete) && len(oldHash) > 0 {
			s.staleSet[string(oldHash)] = struct{}{}
		}
		s.recomputeBucket(bucket, dedupedItems)
		return true
	}
	if limit > 0 && bucket.Count+uint64(len(newItems)) > uint64(limit) {
		return false
	}

	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()

	oldHash := s.ensureBucketHash(bucket)
	if s.pruning && (s.config == nil || s.config.PhysicalDelete) && len(oldHash) > 0 {
		s.staleSet[string(oldHash)] = struct{}{}
	}

	var keyBuf []byte
	var hashBuf []byte

	// 1. 增量更新布谷鸟过滤器
	filterOK := false
	if bucket.cachedFilter != nil {
		filterOK = true
		for _, it := range newItems {
			keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
			if err := bucket.cachedFilter.Insert(keyWithLen); err != nil {
				filterOK = false
				break
			}
			keyBuf = keyWithLen
		}
		if filterOK {
			bucket.Filter = bucket.cachedFilter.Encode()
		}
	} else {
		if len(bucket.Filter) > 0 {
			filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
			if err := filter.Decode(bucket.Filter, s.config.CuckooBuckets, s.config.CuckooSlots); err == nil {
				filterOK = true
				for _, it := range newItems {
					keyWithLen := appendArchiveItemKey(keyBuf, it.SuffixBits, it.Suffix)
					if err := filter.Insert(keyWithLen); err != nil {
						filterOK = false
						break
					}
					keyBuf = keyWithLen
				}
				if filterOK {
					bucket.Filter = filter.Encode()
					bucket.cachedFilter = filter
				}
			}
		}
	}
	if !filterOK {
		bucket.Filter = nil
		bucket.cachedFilter = nil
	}

	// 2. 增量更新 ECMH 承诺
	hashes := make([]common.Hash, 0, len(newItems))
	for _, it := range newItems {
		key := archivedKeyFromKV(it)
		h, nextKeyBuf, nextHashBuf := s.archivePointHash(bucket, key, it.Value, keyBuf, hashBuf)
		hashes = append(hashes, h)
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

	// 4. 清除旧哈希以重新计算元数据哈希
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
	if err := s.ensureBucketKeyList(bucket); err != nil {
		bucket.cacheMu.Unlock()
		return
	}

	matchedDeletes := make([]ArchivedKV, 0, len(deleteItems))
	var newKeys []ArchivedKey
	if len(bucket.Keys) > 0 {
		newKeys = make([]ArchivedKey, 0, len(bucket.Keys))
		for _, key := range bucket.Keys {
			found := false
			for _, del := range deleteItems {
				if archivedKeyMatchesKV(key, del) {
					matchedDeletes = append(matchedDeletes, ArchivedKV{
						Suffix:     common.CopyBytes(key.Suffix),
						SuffixBits: key.SuffixBits,
						Value:      common.CopyBytes(key.ValueRef),
					})
					found = true
					break
				}
			}
			if !found {
				newKeys = append(newKeys, key)
			}
		}
	} else {
		matchedDeletes = append(matchedDeletes, deleteItems...)
	}
	if len(matchedDeletes) == 0 {
		bucket.cacheMu.Unlock()
		return
	}

	oldHash := s.ensureBucketHash(bucket)
	if s.pruning && (s.config == nil || s.config.PhysicalDelete) && len(oldHash) > 0 {
		s.staleSet[string(oldHash)] = struct{}{}
	}
	remainingItems := make([]ArchivedKV, 0, len(newKeys))
	for _, key := range newKeys {
		remainingItems = append(remainingItems, archivedKVFromKey(key))
	}
	bucket.cacheMu.Unlock()
	s.recomputeBucket(bucket, remainingItems)
	return
}

// recomputeBucket 重新计算桶的承诺部分 (Filter, ECMH, Count)
func (s *Shard) recomputeBucket(bucket *ArchiveBucketNode, items []ArchivedKV) {
	recordBucketRecomputeIfEnabled(s.config)
	items = s.deduplicateArchiveItems(items, bucket.Path, bucket.PathBits)
	bucket.cacheMu.Lock()
	defer bucket.cacheMu.Unlock()
	bucket.dirty = true

	filter := cuckoo.New(s.config.CuckooBuckets, s.config.CuckooSlots)
	filterOK := true
	hashes := make([]common.Hash, 0, len(items))
	keys := make([]ArchivedKey, 0, len(items))
	var keyBuf []byte
	var hashBuf []byte

	for _, item := range items {
		// Include SuffixBits to avoid ambiguity (e.g. 1-bit '1' vs 8-bit '10000000')
		keyWithLen := appendArchiveItemKey(keyBuf, item.SuffixBits, item.Suffix)
		if err := filter.Insert(keyWithLen); err != nil {
			filterOK = false
		}

		key := archivedKeyFromKV(item)
		h, _, nextHashBuf := s.archivePointHash(bucket, key, item.Value, keyBuf, hashBuf)
		hashes = append(hashes, h)
		keys = append(keys, key)
		keyBuf = keyWithLen
		hashBuf = nextHashBuf
	}

	if filterOK {
		bucket.Filter = filter.Encode()
		bucket.cachedFilter = filter
	} else {
		bucket.Filter = nil
		bucket.cachedFilter = nil
	}
	bucket.Count = uint64(len(items))
	bucket.Keys = keys

	// ECMH 承诺
	comm, point, _ := s.ecmh.AddWithPoint(nil, hashes)
	bucket.Commitment = comm
	bucket.cachedCommitmentPoint = point

	// 提前计算桶在 Commit 后的哈希，便于后续增量修改登记旧节点 stale。
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
	items, err := s.bucketItemsWithValueRefs(bucket)
	if err != nil {
		return false, time.Since(start).Nanoseconds()
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

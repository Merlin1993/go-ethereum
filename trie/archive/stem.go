// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package archive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"sort"
	"sync"
	"time"
)

const (
	// StemKeySize is the size of a binary-tree state key: a 31-byte stem and
	// one suffix byte.
	StemKeySize = 32
	// StemSize is the number of bytes routed through the outer binary trie.
	StemSize = StemKeySize - 1
	// StemSuffixCount follows Ethereum's 256-value stem layout.
	StemSuffixCount = 256
	// StemProofDepth is the depth of the binary value tree inside a stem.
	StemProofDepth = 8
)

var (
	ErrInvalidStemKey   = errors.New("archive: stem key must be 32 bytes")
	ErrInvalidStem      = errors.New("archive: invalid stem encoding")
	ErrNilStemTrie      = errors.New("archive: nil stem trie backend")
	stemEncodingMagic   = [8]byte{'A', 'S', 'C', 'T', 'S', 'T', 'M', 1}
	stemMetadataMagic   = [8]byte{'A', 'S', 'C', 'T', 'S', 'T', 'M', 2}
	stemEmptyLeafDomain = []byte("ASCT_STEM_EMPTY_V1")
	stemValueLeafDomain = []byte("ASCT_STEM_VALUE_V1")
	stemInternalDomain  = []byte("ASCT_STEM_BRANCH_V1")
)

type stemTripleHasher interface {
	HashTriple(first, second, third []byte) []byte
}

// Stem holds up to 256 values. The outer archive trie sees one ValuesRoot
// under the corresponding 31-byte stem key, so pruning and activation
// naturally operate on the whole stem.
type Stem struct {
	values     map[byte][]byte
	present    [StemSuffixCount / 8]byte
	count      int
	commitment *stemCommitment
}

// stemCommitment caches only non-empty nodes of the fixed eight-level binary
// tree. A suffix update consequently changes at most one leaf and eight
// branches instead of rebuilding all 256 leaves and 255 branches.
type stemCommitment struct {
	hasher    Hasher
	empty     [StemProofDepth + 1][]byte
	nodes     map[uint16][]byte
	hashCount int
}

// StemProof is a binary proof inside one stem. Siblings are ordered from the
// leaf level to the stem root.
type StemProof struct {
	Suffix   byte
	Value    []byte
	Exists   bool
	Siblings [StemProofDepth][]byte
}

// NewStem returns an empty 256-suffix stem.
func NewStem() *Stem {
	return &Stem{values: make(map[byte][]byte)}
}

// Len returns the number of populated suffixes.
func (s *Stem) Len() int {
	if s == nil {
		return 0
	}
	return s.count
}

// Get returns a copy of the value stored at suffix.
func (s *Stem) Get(suffix byte) ([]byte, bool) {
	if s == nil || !s.has(suffix) {
		return nil, false
	}
	return bytes.Clone(s.values[suffix]), true
}

// Put sets one suffix. An empty value is still a present value; callers must
// use Delete when they want an absent suffix.
func (s *Stem) Put(suffix byte, value []byte) {
	s.put(suffix, value)
}

func (s *Stem) put(suffix byte, value []byte) bool {
	if s == nil {
		return false
	}
	if s.has(suffix) && bytes.Equal(s.values[suffix], value) {
		return false
	}
	if !s.has(suffix) {
		s.setPresent(suffix, true)
		s.count++
	}
	if s.values == nil {
		s.values = make(map[byte][]byte)
	}
	s.values[suffix] = bytes.Clone(value)
	if s.commitment != nil {
		s.commitment.setLeaf(suffix, stemValueHash(value, s.commitment.hasher))
	}
	return true
}

// Delete removes one suffix and reports whether it was present.
func (s *Stem) Delete(suffix byte) bool {
	if s == nil || !s.has(suffix) {
		return false
	}
	s.setPresent(suffix, false)
	delete(s.values, suffix)
	s.count--
	if s.commitment != nil {
		s.commitment.setLeaf(suffix, nil)
	}
	return true
}

// ValuesRoot returns the root of the eight-level binary value tree inside the
// stem. It is independent of the outer ASCT shape.
func (s *Stem) ValuesRoot(hasher Hasher) []byte {
	s.ensureCommitment(hasher)
	return bytes.Clone(s.commitment.hashAt(StemProofDepth, 0))
}

// Prove builds an existence or non-existence proof for one suffix.
func (s *Stem) Prove(suffix byte, hasher Hasher) StemProof {
	proof := StemProof{Suffix: suffix}
	if value, ok := s.Get(suffix); ok {
		proof.Exists = true
		proof.Value = value
	}
	s.ensureCommitment(hasher)
	index := int(suffix)
	for depth := 0; depth < StemProofDepth; depth++ {
		proof.Siblings[depth] = bytes.Clone(s.commitment.hashAt(depth, index^1))
		index /= 2
	}
	return proof
}

// VerifyStemProof verifies a proof against a ValuesRoot.
func VerifyStemProof(root []byte, proof StemProof, hasher Hasher) bool {
	if hasher == nil || len(root) == 0 {
		return false
	}
	var current []byte
	if proof.Exists {
		current = stemValueHash(proof.Value, hasher)
	} else {
		if len(proof.Value) != 0 {
			return false
		}
		current = hasher.Hash(stemEmptyLeafDomain)
	}
	for depth := 0; depth < StemProofDepth; depth++ {
		sibling := proof.Siblings[depth]
		if len(sibling) == 0 {
			return false
		}
		if ((proof.Suffix >> depth) & 1) == 0 {
			current = stemBranchHash(current, sibling, hasher)
		} else {
			current = stemBranchHash(sibling, current, hasher)
		}
	}
	return bytes.Equal(current, root)
}

func (s *Stem) has(suffix byte) bool {
	return (s.present[int(suffix)/8] & (byte(1) << (suffix % 8))) != 0
}

func (s *Stem) setPresent(suffix byte, present bool) {
	mask := byte(1) << (suffix % 8)
	if present {
		s.present[int(suffix)/8] |= mask
	} else {
		s.present[int(suffix)/8] &^= mask
	}
}

func (s *Stem) ensureCommitment(hasher Hasher) {
	if s.commitment != nil && sameHasher(s.commitment.hasher, hasher) {
		return
	}
	s.commitment = buildStemCommitment(s, hasher, nil)
}

func newStemCommitment(hasher Hasher, reusableEmpty *[StemProofDepth + 1][]byte) *stemCommitment {
	commitment := &stemCommitment{
		hasher: hasher,
		nodes:  make(map[uint16][]byte),
	}
	if reusableEmpty != nil {
		commitment.empty = *reusableEmpty
	} else {
		commitment.empty = stemEmptyRoots(hasher)
		commitment.hashCount += 1 + StemProofDepth
	}
	return commitment
}

func buildStemCommitment(stem *Stem, hasher Hasher, reusableEmpty *[StemProofDepth + 1][]byte) *stemCommitment {
	commitment := newStemCommitment(hasher, reusableEmpty)
	if stem == nil || stem.count == 0 {
		return commitment
	}
	// The populated indices stay sorted, so every level can collapse them to
	// unique parent indices in place. This avoids scanning all 255 possible
	// branches for the common one-value account stem.
	var populated [StemSuffixCount]uint16
	populatedCount := 0
	for i := 0; i < StemSuffixCount; i++ {
		if !stem.has(byte(i)) {
			continue
		}
		commitment.nodes[stemCommitmentKey(0, i)] = stemValueHash(stem.values[byte(i)], hasher)
		commitment.hashCount++
		populated[populatedCount] = uint16(i)
		populatedCount++
	}
	for depth := 0; depth < StemProofDepth; depth++ {
		parentCount := 0
		lastParent := -1
		for i := 0; i < populatedCount; i++ {
			parent := int(populated[i]) / 2
			if parent == lastParent {
				continue
			}
			left := commitment.hashAt(depth, parent*2)
			right := commitment.hashAt(depth, parent*2+1)
			commitment.nodes[stemCommitmentKey(depth+1, parent)] = stemBranchHash(left, right, hasher)
			commitment.hashCount++
			populated[parentCount] = uint16(parent)
			parentCount++
			lastParent = parent
		}
		populatedCount = parentCount
	}
	return commitment
}

func stemEmptyRoots(hasher Hasher) [StemProofDepth + 1][]byte {
	var empty [StemProofDepth + 1][]byte
	empty[0] = hasher.Hash(stemEmptyLeafDomain)
	for depth := 1; depth <= StemProofDepth; depth++ {
		empty[depth] = stemBranchHash(empty[depth-1], empty[depth-1], hasher)
	}
	return empty
}

func (c *stemCommitment) hashAt(depth, index int) []byte {
	if hash, ok := c.nodes[stemCommitmentKey(depth, index)]; ok {
		return hash
	}
	return c.empty[depth]
}

func (c *stemCommitment) setLeaf(suffix byte, hash []byte) {
	index := int(suffix)
	if len(hash) == 0 {
		delete(c.nodes, stemCommitmentKey(0, index))
	} else {
		c.nodes[stemCommitmentKey(0, index)] = hash
		c.hashCount++
	}
	for depth := 0; depth < StemProofDepth; depth++ {
		parent := index / 2
		left, leftOK := c.nodes[stemCommitmentKey(depth, parent*2)]
		right, rightOK := c.nodes[stemCommitmentKey(depth, parent*2+1)]
		parentKey := stemCommitmentKey(depth+1, parent)
		if !leftOK && !rightOK {
			delete(c.nodes, parentKey)
		} else {
			if !leftOK {
				left = c.empty[depth]
			}
			if !rightOK {
				right = c.empty[depth]
			}
			c.nodes[parentKey] = stemBranchHash(left, right, c.hasher)
			c.hashCount++
		}
		index = parent
	}
}

func stemCommitmentKey(depth, index int) uint16 {
	return uint16(depth<<8 | index)
}

func sameHasher(left, right Hasher) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) || !leftType.Comparable() {
		return false
	}
	// A different hasher instance, even of the same algorithm, safely causes a
	// rebuild. The production hashers are pointers, so this is the hot path.
	return left == right
}

func stemValueHash(value []byte, hasher Hasher) []byte {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	if segmented, ok := hasher.(stemTripleHasher); ok {
		return segmented.HashTriple(stemValueLeafDomain, size[:], value)
	}
	data := make([]byte, 0, len(stemValueLeafDomain)+len(size)+len(value))
	data = append(data, stemValueLeafDomain...)
	data = append(data, size[:]...)
	data = append(data, value...)
	return hasher.Hash(data)
}

func stemBranchHash(left, right []byte, hasher Hasher) []byte {
	if segmented, ok := hasher.(stemTripleHasher); ok {
		return segmented.HashTriple(stemInternalDomain, left, right)
	}
	data := make([]byte, 0, len(stemInternalDomain)+len(left)+len(right))
	data = append(data, stemInternalDomain...)
	data = append(data, left...)
	data = append(data, right...)
	return hasher.Hash(data)
}

func encodeStem(stem *Stem, hasher Hasher) []byte {
	root := stem.ValuesRoot(hasher)
	capacity := len(stemEncodingMagic) + len(root) + len(stem.present) + stem.count*2
	for i := 0; i < StemSuffixCount; i++ {
		if stem.has(byte(i)) {
			capacity += len(stem.values[byte(i)])
		}
	}
	data := make([]byte, 0, capacity)
	data = append(data, stemEncodingMagic[:]...)
	data = append(data, root...)
	data = append(data, stem.present[:]...)
	var size [binary.MaxVarintLen64]byte
	for i := 0; i < StemSuffixCount; i++ {
		if !stem.has(byte(i)) {
			continue
		}
		value := stem.values[byte(i)]
		n := binary.PutUvarint(size[:], uint64(len(value)))
		data = append(data, size[:n]...)
		data = append(data, value...)
	}
	return data
}

func decodeStem(data []byte, hasher Hasher) (*Stem, error) {
	return decodeStemWithEmpty(data, hasher, nil)
}

func decodeStemWithEmpty(data []byte, hasher Hasher, reusableEmpty *[StemProofDepth + 1][]byte) (*Stem, error) {
	headerSize := len(stemEncodingMagic) + 32 + StemSuffixCount/8
	if len(data) < headerSize || !bytes.Equal(data[:len(stemEncodingMagic)], stemEncodingMagic[:]) {
		return nil, ErrInvalidStem
	}
	wantRoot := data[len(stemEncodingMagic) : len(stemEncodingMagic)+32]
	offset := len(stemEncodingMagic) + 32
	stem := NewStem()
	copy(stem.present[:], data[offset:offset+len(stem.present)])
	offset += len(stem.present)
	for _, word := range stem.present {
		stem.count += bits.OnesCount8(word)
	}
	for i := 0; i < StemSuffixCount; i++ {
		if !stem.has(byte(i)) {
			continue
		}
		size, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return nil, ErrInvalidStem
		}
		offset += n
		if size > uint64(len(data)-offset) {
			return nil, ErrInvalidStem
		}
		end := offset + int(size)
		stem.values[byte(i)] = bytes.Clone(data[offset:end])
		offset = end
	}
	stem.commitment = buildStemCommitment(stem, hasher, reusableEmpty)
	if offset != len(data) || !bytes.Equal(stem.ValuesRoot(hasher), wantRoot) {
		return nil, ErrInvalidStem
	}
	return stem, nil
}

// stemEncodedValueCount reads only the fixed stem header. Statistics use it
// instead of rebuilding the 256-leaf value root for every stem.
func stemEncodedValueCount(data []byte) (int, error) {
	if len(data) == len(stemMetadataMagic)+StemSuffixCount/8 &&
		bytes.Equal(data[:len(stemMetadataMagic)], stemMetadataMagic[:]) {
		count := 0
		for _, word := range data[len(stemMetadataMagic):] {
			count += bits.OnesCount8(word)
		}
		return count, nil
	}
	headerSize := len(stemEncodingMagic) + 32 + StemSuffixCount/8
	if len(data) < headerSize || !bytes.Equal(data[:len(stemEncodingMagic)], stemEncodingMagic[:]) {
		return 0, ErrInvalidStem
	}
	offset := len(stemEncodingMagic) + 32
	count := 0
	for _, word := range data[offset : offset+StemSuffixCount/8] {
		count += bits.OnesCount8(word)
	}
	return count, nil
}

func encodeStemMetadata(stem *Stem) []byte {
	data := make([]byte, 0, len(stemMetadataMagic)+len(stem.present))
	data = append(data, stemMetadataMagic[:]...)
	data = append(data, stem.present[:]...)
	return data
}

func decodeStemMetadata(data []byte) (*Stem, error) {
	if len(data) != len(stemMetadataMagic)+StemSuffixCount/8 ||
		!bytes.Equal(data[:len(stemMetadataMagic)], stemMetadataMagic[:]) {
		return nil, ErrInvalidStem
	}
	stem := NewStem()
	copy(stem.present[:], data[len(stemMetadataMagic):])
	for _, word := range stem.present {
		stem.count += bits.OnesCount8(word)
	}
	return stem, nil
}

func joinStemKey(stemKey []byte, suffix byte) []byte {
	key := make([]byte, StemKeySize)
	copy(key, stemKey)
	key[StemSize] = suffix
	return key
}

// StemTrie adapts a normal ASCT to 32-byte binary stem keys. Only the 31-byte
// stem and its ValuesRoot are inserted into the outer trie. The suffix bitmap
// and values are stored separately, while the whole stem remains the unit of
// aging, archiving, and activation.
type StemTrie struct {
	backend *Trie
	locks   [256]sync.Mutex
	empty   [StemProofDepth + 1][]byte
}

// StemUpdate is one logical suffix mutation used by ApplyBatch.
type StemUpdate struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// StemDeleteBatchResult contains the values removed by DeleteBatchWithValues.
// Values are aligned with the input keys and StemCount is the number of outer
// stem records that were loaded.
type StemDeleteBatchResult struct {
	Values    [][]byte
	StemCount int
}

// NewStemTrie wraps an existing trie. The backend must be dedicated to stem
// keys; mixing direct backend writes with StemTrie writes is unsupported.
func NewStemTrie(backend *Trie) (*StemTrie, error) {
	if backend == nil || backend.hasher == nil {
		return nil, ErrNilStemTrie
	}
	backend.stemViewMu.Lock()
	defer backend.stemViewMu.Unlock()
	if backend.stemView == nil {
		backend.stemView = &StemTrie{
			backend: backend,
			empty:   stemEmptyRoots(backend.hasher),
		}
	}
	return backend.stemView, nil
}

// Backend returns the outer archive trie used by this stem view.
func (t *StemTrie) Backend() *Trie {
	if t == nil {
		return nil
	}
	return t.backend
}

// Get returns one suffix value without activating an archived stem.
func (t *StemTrie) Get(key []byte) ([]byte, error) {
	stemKey, suffix, err := splitStemKey(key)
	if err != nil {
		return nil, err
	}
	lock := &t.locks[stemKey[0]]
	lock.Lock()
	defer lock.Unlock()
	stem, err := t.loadStem(stemKey)
	if err != nil {
		return nil, err
	}
	value, ok := stem.Get(suffix)
	if !ok {
		return nil, ErrNodeNotFound
	}
	return value, nil
}

// Put updates one suffix. If the stem is archived, this reads its current
// suffixes, updates the commitment, and writes the stem back as one hot leaf.
func (t *StemTrie) Put(key, value []byte) error {
	return t.put(key, value, false)
}

// ReplaceStem replaces the entire stem with one suffix. Callers may use this
// only when they know no sibling suffix is logically present. Unlike Put, it
// does not load the old stem first.
func (t *StemTrie) ReplaceStem(key, value []byte) error {
	return t.put(key, value, true)
}

func (t *StemTrie) put(key, value []byte, replace bool) error {
	totalStart := time.Now()
	var loadTime, decodeTime, encodeTime, backendTime time.Duration
	var loadedBytes, encodedBytes, commitmentHashes int
	var noop bool
	defer func() {
		recordStemPutDiagnostics(noop, loadedBytes, encodedBytes, commitmentHashes, time.Since(totalStart), loadTime, decodeTime, encodeTime, backendTime)
	}()
	stemKey, suffix, err := splitStemKey(key)
	if err != nil {
		return err
	}
	lock := &t.locks[stemKey[0]]
	lock.Lock()
	defer lock.Unlock()

	var (
		stem       *Stem
		oldStem    *Stem
		split      bool
		wasPresent bool
	)
	loadStart := time.Now()
	if replace {
		oldStem, split, loadedBytes, err = t.loadStoredStem(stemKey, false)
	} else {
		stem, split, loadedBytes, err = t.loadStoredStem(stemKey, true)
	}
	loadTime = time.Since(loadStart)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return err
	}
	if replace || stem == nil {
		stem = &Stem{
			values:     make(map[byte][]byte),
			commitment: newStemCommitment(t.backend.hasher, &t.empty),
		}
	}
	if !replace {
		wasPresent = stem.has(suffix)
	}
	noop = !stem.put(suffix, value)

	encodeStart := time.Now()
	root := stem.ValuesRoot(t.backend.hasher)
	commitmentHashes = stem.commitment.hashCount
	encodeTime = time.Since(encodeStart)

	backendStart := time.Now()
	// The common path writes one value and, at most, one metadata record.
	// Legacy migration may grow this slice once to hold all existing suffixes.
	flatPuts := make([]KeyValue, 0, 2)
	flatDeletes := make([][]byte, 0)
	if replace && split && oldStem != nil {
		for i := 0; i < StemSuffixCount; i++ {
			oldSuffix := byte(i)
			if oldSuffix == suffix || !oldStem.has(oldSuffix) {
				continue
			}
			flatDeletes = append(flatDeletes, joinStemKey(stemKey, oldSuffix))
		}
	}
	writeMetadata := !split
	if split {
		if replace {
			writeMetadata = oldStem == nil || oldStem.Len() != 1 || !oldStem.has(suffix)
		} else {
			writeMetadata = !wasPresent
		}
	}
	if writeMetadata {
		metadata := encodeStemMetadata(stem)
		flatPuts = append(flatPuts, KeyValue{Key: stemKey, Value: metadata})
		encodedBytes += len(metadata)
	}
	if !split && !replace {
		for i := 0; i < StemSuffixCount; i++ {
			itemSuffix := byte(i)
			if !stem.has(itemSuffix) {
				continue
			}
			itemValue := stem.values[itemSuffix]
			flatPuts = append(flatPuts, KeyValue{Key: joinStemKey(stemKey, itemSuffix), Value: itemValue})
			encodedBytes += len(itemValue)
		}
	} else if !noop || replace {
		flatPuts = append(flatPuts, KeyValue{Key: key, Value: value})
		encodedBytes += len(value)
	}
	if err := t.backend.StageFlatBatch(flatPuts, flatDeletes); err != nil {
		backendTime = time.Since(backendStart)
		return err
	}
	err = t.backend.PutValueRef(stemKey, root)
	backendTime = time.Since(backendStart)
	return err
}

// PutBatch applies ordered suffix updates and writes each affected stem only
// once. Updates to the same stem retain their input order.
func (t *StemTrie) PutBatch(entries []KeyValue) error {
	updates := make([]StemUpdate, len(entries))
	for i, entry := range entries {
		updates[i] = StemUpdate{Key: entry.Key, Value: entry.Value}
	}
	return t.ApplyBatch(updates)
}

// ApplyBatch applies ordered puts and deletes, loading and encoding every
// affected stem at most once.
func (t *StemTrie) ApplyBatch(updates []StemUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	totalStart := time.Now()
	var loadTime, encodeTime, backendTime time.Duration
	var stemCount, putCount, deleteCount int
	defer func() {
		recordStemApplyDiagnostics(len(updates), stemCount, putCount, deleteCount, time.Since(totalStart), loadTime, encodeTime, backendTime)
	}()
	type stemWrites struct {
		key     []byte
		updates []StemUpdate
	}
	byStem := make(map[string]int)
	groups := make([]stemWrites, 0)
	lockIDs := make(map[int]struct{})
	for _, update := range updates {
		stemKey, suffix, err := splitStemKey(update.Key)
		if err != nil {
			return err
		}
		id, ok := byStem[string(stemKey)]
		if !ok {
			id = len(groups)
			byStem[string(stemKey)] = id
			groups = append(groups, stemWrites{key: bytes.Clone(stemKey)})
		}
		groups[id].updates = append(groups[id].updates, StemUpdate{
			Key:    []byte{suffix},
			Value:  bytes.Clone(update.Value),
			Delete: update.Delete,
		})
		lockIDs[int(stemKey[0])] = struct{}{}
	}
	stemCount = len(groups)
	orderedLocks := make([]int, 0, len(lockIDs))
	for id := range lockIDs {
		orderedLocks = append(orderedLocks, id)
	}
	sort.Ints(orderedLocks)
	for _, id := range orderedLocks {
		t.locks[id].Lock()
	}
	defer func() {
		for i := len(orderedLocks) - 1; i >= 0; i-- {
			t.locks[orderedLocks[i]].Unlock()
		}
	}()

	refPuts := make([]KeyValue, 0, len(groups))
	outerDeletes := make([][]byte, 0)
	flatPuts := make([]KeyValue, 0, len(updates)+len(groups))
	flatDeletes := make([][]byte, 0)
	for _, group := range groups {
		loadStart := time.Now()
		stem, split, _, err := t.loadStoredStem(group.key, true)
		loadTime += time.Since(loadStart)
		if err != nil && !errors.Is(err, ErrNodeNotFound) {
			return err
		}
		existed := err == nil
		if stem == nil {
			stem = &Stem{
				values:     make(map[byte][]byte),
				commitment: newStemCommitment(t.backend.hasher, &t.empty),
			}
		}
		beforePresent := stem.present
		beforeValues := make(map[byte][]byte)
		touched := make(map[byte]struct{})
		for _, update := range group.updates {
			suffix := update.Key[0]
			if _, ok := touched[suffix]; !ok {
				if value, present := stem.Get(suffix); present {
					beforeValues[suffix] = value
				}
				touched[suffix] = struct{}{}
			}
		}
		for _, update := range group.updates {
			if update.Delete {
				stem.Delete(update.Key[0])
			} else {
				stem.put(update.Key[0], update.Value)
			}
		}
		if stem.Len() == 0 {
			if existed {
				outerDeletes = append(outerDeletes, group.key)
				if split {
					for i := 0; i < StemSuffixCount; i++ {
						suffix := byte(i)
						if beforePresent[i/8]&(byte(1)<<(suffix%8)) != 0 {
							flatDeletes = append(flatDeletes, joinStemKey(group.key, suffix))
						}
					}
				}
			}
			continue
		}
		encodeStart := time.Now()
		root := stem.ValuesRoot(t.backend.hasher)
		if !split {
			flatPuts = append(flatPuts, KeyValue{Key: group.key, Value: encodeStemMetadata(stem)})
			for i := 0; i < StemSuffixCount; i++ {
				suffix := byte(i)
				if stem.has(suffix) {
					flatPuts = append(flatPuts, KeyValue{Key: joinStemKey(group.key, suffix), Value: bytes.Clone(stem.values[suffix])})
				}
			}
		} else {
			for suffix := range touched {
				value, present := stem.Get(suffix)
				oldValue, wasPresent := beforeValues[suffix]
				switch {
				case present && (!wasPresent || !bytes.Equal(value, oldValue)):
					flatPuts = append(flatPuts, KeyValue{Key: joinStemKey(group.key, suffix), Value: value})
				case !present && wasPresent:
					flatDeletes = append(flatDeletes, joinStemKey(group.key, suffix))
				}
			}
			if !bytes.Equal(beforePresent[:], stem.present[:]) {
				flatPuts = append(flatPuts, KeyValue{Key: group.key, Value: encodeStemMetadata(stem)})
			}
		}
		encodeTime += time.Since(encodeStart)
		refPuts = append(refPuts, KeyValue{Key: group.key, Value: root})
	}
	putCount = len(refPuts)
	deleteCount = len(outerDeletes)
	backendStart := time.Now()
	if err := t.backend.StageFlatBatch(flatPuts, flatDeletes); err != nil {
		backendTime += time.Since(backendStart)
		return err
	}
	if err := t.backend.PutValueRefBatch(refPuts); err != nil {
		backendTime += time.Since(backendStart)
		return err
	}
	for _, key := range outerDeletes {
		if err := t.backend.Delete(key); err != nil {
			backendTime += time.Since(backendStart)
			return err
		}
	}
	backendTime += time.Since(backendStart)
	return nil
}

// DeleteBatchWithValues removes a set of suffixes while returning their old
// values. Every affected stem is loaded, decoded and written at most once.
// This is used for account destruction, where the caller needs the old values
// for history bookkeeping but must not perform one outer-trie read per slot.
func (t *StemTrie) DeleteBatchWithValues(keys [][]byte) (StemDeleteBatchResult, error) {
	result := StemDeleteBatchResult{Values: make([][]byte, len(keys))}
	if len(keys) == 0 {
		return result, nil
	}
	type stemDeletes struct {
		key       []byte
		suffixes  []byte
		positions []int
	}
	byStem := make(map[string]int)
	groups := make([]stemDeletes, 0)
	lockIDs := make(map[int]struct{})
	for position, key := range keys {
		stemKey, suffix, err := splitStemKey(key)
		if err != nil {
			return StemDeleteBatchResult{}, err
		}
		id, ok := byStem[string(stemKey)]
		if !ok {
			id = len(groups)
			byStem[string(stemKey)] = id
			groups = append(groups, stemDeletes{key: bytes.Clone(stemKey)})
		}
		groups[id].suffixes = append(groups[id].suffixes, suffix)
		groups[id].positions = append(groups[id].positions, position)
		lockIDs[int(stemKey[0])] = struct{}{}
	}
	orderedLocks := make([]int, 0, len(lockIDs))
	for id := range lockIDs {
		orderedLocks = append(orderedLocks, id)
	}
	sort.Ints(orderedLocks)
	for _, id := range orderedLocks {
		t.locks[id].Lock()
	}
	defer func() {
		for i := len(orderedLocks) - 1; i >= 0; i-- {
			t.locks[orderedLocks[i]].Unlock()
		}
	}()

	result.StemCount = len(groups)
	refPuts := make([]KeyValue, 0, len(groups))
	outerDeletes := make([][]byte, 0, len(groups))
	flatPuts := make([]KeyValue, 0, len(groups))
	flatDeletes := make([][]byte, 0, len(keys))
	for _, group := range groups {
		stem, split, _, err := t.loadStoredStem(group.key, true)
		if err != nil {
			return StemDeleteBatchResult{}, err
		}
		// Read every requested value before applying deletes so duplicate input
		// keys retain the same behavior as separate Get calls.
		for i, suffix := range group.suffixes {
			value, ok := stem.Get(suffix)
			if !ok {
				return StemDeleteBatchResult{}, ErrNodeNotFound
			}
			result.Values[group.positions[i]] = value
		}
		for _, suffix := range group.suffixes {
			stem.Delete(suffix)
		}
		if stem.Len() == 0 {
			outerDeletes = append(outerDeletes, group.key)
			if split {
				seen := make(map[byte]struct{})
				for _, suffix := range group.suffixes {
					if _, ok := seen[suffix]; ok {
						continue
					}
					seen[suffix] = struct{}{}
					flatDeletes = append(flatDeletes, joinStemKey(group.key, suffix))
				}
			}
			continue
		}
		if split {
			seen := make(map[byte]struct{})
			for _, suffix := range group.suffixes {
				if _, ok := seen[suffix]; ok {
					continue
				}
				seen[suffix] = struct{}{}
				flatDeletes = append(flatDeletes, joinStemKey(group.key, suffix))
			}
			flatPuts = append(flatPuts, KeyValue{Key: group.key, Value: encodeStemMetadata(stem)})
		} else {
			flatPuts = append(flatPuts, KeyValue{Key: group.key, Value: encodeStemMetadata(stem)})
			for i := 0; i < StemSuffixCount; i++ {
				suffix := byte(i)
				if stem.has(suffix) {
					flatPuts = append(flatPuts, KeyValue{Key: joinStemKey(group.key, suffix), Value: bytes.Clone(stem.values[suffix])})
				}
			}
		}
		refPuts = append(refPuts, KeyValue{Key: group.key, Value: stem.ValuesRoot(t.backend.hasher)})
	}
	if err := t.backend.StageFlatBatch(flatPuts, flatDeletes); err != nil {
		return StemDeleteBatchResult{}, err
	}
	if err := t.backend.PutValueRefBatch(refPuts); err != nil {
		return StemDeleteBatchResult{}, err
	}
	for _, key := range outerDeletes {
		if err := t.backend.Delete(key); err != nil {
			return StemDeleteBatchResult{}, err
		}
	}
	return result, nil
}

// Delete removes one suffix. The outer stem leaf is deleted only when its last
// suffix is removed.
func (t *StemTrie) Delete(key []byte) error {
	stemKey, suffix, err := splitStemKey(key)
	if err != nil {
		return err
	}
	lock := &t.locks[stemKey[0]]
	lock.Lock()
	defer lock.Unlock()

	stem, split, _, err := t.loadStoredStem(stemKey, true)
	if errors.Is(err, ErrNodeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !stem.Delete(suffix) {
		return nil
	}
	if stem.Len() == 0 {
		if split {
			if err := t.backend.StageFlatDelete(key); err != nil {
				return err
			}
		}
		return t.backend.Delete(stemKey)
	}
	flatPuts := []KeyValue{{Key: stemKey, Value: encodeStemMetadata(stem)}}
	var flatDeletes [][]byte
	if split {
		flatDeletes = append(flatDeletes, bytes.Clone(key))
	} else {
		for i := 0; i < StemSuffixCount; i++ {
			itemSuffix := byte(i)
			if stem.has(itemSuffix) {
				flatPuts = append(flatPuts, KeyValue{Key: joinStemKey(stemKey, itemSuffix), Value: bytes.Clone(stem.values[itemSuffix])})
			}
		}
	}
	if err := t.backend.StageFlatBatch(flatPuts, flatDeletes); err != nil {
		return err
	}
	return t.backend.PutValueRef(stemKey, stem.ValuesRoot(t.backend.hasher))
}

// Activate restores the entire archived stem containing key to the hot tree.
// The suffix only identifies the stem; all 256 positions move together.
func (t *StemTrie) Activate(key []byte) error {
	stemKey, _, err := splitStemKey(key)
	if err != nil {
		return err
	}
	lock := &t.locks[stemKey[0]]
	lock.Lock()
	defer lock.Unlock()

	stem, split, _, err := t.loadStoredStem(stemKey, true)
	if err != nil {
		return err
	}
	if !split {
		flatPuts := []KeyValue{{Key: stemKey, Value: encodeStemMetadata(stem)}}
		for i := 0; i < StemSuffixCount; i++ {
			suffix := byte(i)
			if stem.has(suffix) {
				flatPuts = append(flatPuts, KeyValue{Key: joinStemKey(stemKey, suffix), Value: bytes.Clone(stem.values[suffix])})
			}
		}
		if err := t.backend.StageFlatBatch(flatPuts, nil); err != nil {
			return err
		}
	}
	return t.backend.ActivateValueRef(stemKey, stem.ValuesRoot(t.backend.hasher))
}

// Prove returns the current ValuesRoot and an eight-hash suffix proof.
func (t *StemTrie) Prove(key []byte) ([]byte, StemProof, error) {
	stemKey, suffix, err := splitStemKey(key)
	if err != nil {
		return nil, StemProof{}, err
	}
	lock := &t.locks[stemKey[0]]
	lock.Lock()
	defer lock.Unlock()
	stem, err := t.loadStem(stemKey)
	if err != nil {
		return nil, StemProof{}, err
	}
	return stem.ValuesRoot(t.backend.hasher), stem.Prove(suffix, t.backend.hasher), nil
}

// ForEach visits logical 32-byte keys rather than encoded outer stem records.
func (t *StemTrie) ForEach(fn func(key, value []byte) bool) error {
	if t == nil || t.backend == nil {
		return ErrNilStemTrie
	}
	var iterErr error
	err := t.backend.ForEachAllValueRefs(func(stemKey, _ []byte) bool {
		lock := &t.locks[stemKey[0]]
		lock.Lock()
		defer lock.Unlock()
		stem, err := t.loadStem(stemKey)
		if err != nil {
			iterErr = err
			return false
		}
		for i := 0; i < StemSuffixCount; i++ {
			value, ok := stem.Get(byte(i))
			if !ok {
				continue
			}
			key := make([]byte, StemKeySize)
			copy(key, stemKey)
			key[StemSize] = byte(i)
			if !fn(key, value) {
				return false
			}
		}
		return true
	})
	if err != nil && iterErr == nil {
		iterErr = err
	}
	return iterErr
}

func (t *StemTrie) loadStem(stemKey []byte) (*Stem, error) {
	stem, _, _, err := t.loadStoredStem(stemKey, true)
	return stem, err
}

// loadStoredStem loads either the split layout or the legacy single-blob
// layout. Split stems keep only a bitmap under the 31-byte stem key and store
// each value under its complete 32-byte key.
func (t *StemTrie) loadStoredStem(stemKey []byte, loadValues bool) (*Stem, bool, int, error) {
	if t == nil || t.backend == nil {
		return nil, false, 0, ErrNilStemTrie
	}
	payload, err := t.backend.GetFlatValue(stemKey)
	if err != nil {
		return nil, false, 0, err
	}
	loadedBytes := len(payload)
	if len(payload) >= len(stemMetadataMagic) && bytes.Equal(payload[:len(stemMetadataMagic)], stemMetadataMagic[:]) {
		stem, err := decodeStemMetadata(payload)
		if err != nil {
			return nil, true, loadedBytes, fmt.Errorf("%w for %x", err, stemKey)
		}
		if !loadValues {
			return stem, true, loadedBytes, nil
		}
		valueRef, _, err := t.backend.GetValueRef(stemKey)
		if err != nil {
			return nil, true, loadedBytes, err
		}
		for i := 0; i < StemSuffixCount; i++ {
			suffix := byte(i)
			if !stem.has(suffix) {
				continue
			}
			value, err := t.backend.GetFlatValue(joinStemKey(stemKey, suffix))
			if err != nil {
				return nil, true, loadedBytes, fmt.Errorf("%w: missing suffix %d for %x", ErrInvalidStem, suffix, stemKey)
			}
			loadedBytes += len(value)
			stem.values[suffix] = bytes.Clone(value)
		}
		stem.commitment = buildStemCommitment(stem, t.backend.hasher, &t.empty)
		if !bytes.Equal(stem.ValuesRoot(t.backend.hasher), valueRef) {
			return nil, true, loadedBytes, fmt.Errorf("%w: root mismatch for %x", ErrInvalidStem, stemKey)
		}
		return stem, true, loadedBytes, nil
	}
	valueRef, _, err := t.backend.GetValueRef(stemKey)
	if err != nil {
		return nil, false, loadedBytes, err
	}
	if !bytes.Equal(valueRefForKeyValue(stemKey, payload), valueRef) {
		return nil, false, loadedBytes, fmt.Errorf("%w: legacy value reference mismatch for %x", ErrInvalidStem, stemKey)
	}
	stem, err := decodeStemWithEmpty(payload, t.backend.hasher, &t.empty)
	if err != nil {
		return nil, false, loadedBytes, fmt.Errorf("%w for %x", err, stemKey)
	}
	return stem, false, loadedBytes, nil
}

func splitStemKey(key []byte) ([]byte, byte, error) {
	if len(key) != StemKeySize {
		return nil, 0, ErrInvalidStemKey
	}
	return key[:StemSize], key[StemSize], nil
}

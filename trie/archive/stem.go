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
	"sort"
	"sync"
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
	stemEmptyLeafDomain = []byte("ASCT_STEM_EMPTY_V1")
	stemValueLeafDomain = []byte("ASCT_STEM_VALUE_V1")
	stemInternalDomain  = []byte("ASCT_STEM_BRANCH_V1")
)

// Stem holds up to 256 values. The outer archive trie only sees one encoded
// Stem value under the corresponding 31-byte stem key, so pruning and
// activation naturally operate on the whole stem.
type Stem struct {
	values  [StemSuffixCount][]byte
	present [StemSuffixCount / 8]byte
	count   int
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
	return &Stem{}
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
	return bytes.Clone(s.values[int(suffix)]), true
}

// Put sets one suffix. An empty value is still a present value; callers must
// use Delete when they want an absent suffix.
func (s *Stem) Put(suffix byte, value []byte) {
	if s == nil {
		return
	}
	if !s.has(suffix) {
		s.setPresent(suffix, true)
		s.count++
	}
	s.values[int(suffix)] = bytes.Clone(value)
}

// Delete removes one suffix and reports whether it was present.
func (s *Stem) Delete(suffix byte) bool {
	if s == nil || !s.has(suffix) {
		return false
	}
	s.setPresent(suffix, false)
	s.values[int(suffix)] = nil
	s.count--
	return true
}

// ValuesRoot returns the root of the eight-level binary value tree inside the
// stem. It is independent of the outer ASCT shape.
func (s *Stem) ValuesRoot(hasher Hasher) []byte {
	level := s.leafHashes(hasher)
	for len(level) > 1 {
		level = stemParentLevel(level, hasher)
	}
	return bytes.Clone(level[0])
}

// Prove builds an existence or non-existence proof for one suffix.
func (s *Stem) Prove(suffix byte, hasher Hasher) StemProof {
	proof := StemProof{Suffix: suffix}
	if value, ok := s.Get(suffix); ok {
		proof.Exists = true
		proof.Value = value
	}
	level := s.leafHashes(hasher)
	index := int(suffix)
	for depth := 0; depth < StemProofDepth; depth++ {
		proof.Siblings[depth] = bytes.Clone(level[index^1])
		level = stemParentLevel(level, hasher)
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

func (s *Stem) leafHashes(hasher Hasher) [][]byte {
	level := make([][]byte, StemSuffixCount)
	empty := hasher.Hash(stemEmptyLeafDomain)
	for i := range level {
		if s != nil && s.has(byte(i)) {
			level[i] = stemValueHash(s.values[i], hasher)
		} else {
			level[i] = empty
		}
	}
	return level
}

func stemValueHash(value []byte, hasher Hasher) []byte {
	data := make([]byte, 0, len(stemValueLeafDomain)+8+len(value))
	data = append(data, stemValueLeafDomain...)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	data = append(data, size[:]...)
	data = append(data, value...)
	return hasher.Hash(data)
}

func stemBranchHash(left, right []byte, hasher Hasher) []byte {
	data := make([]byte, 0, len(stemInternalDomain)+len(left)+len(right))
	data = append(data, stemInternalDomain...)
	data = append(data, left...)
	data = append(data, right...)
	return hasher.Hash(data)
}

func stemParentLevel(level [][]byte, hasher Hasher) [][]byte {
	parents := make([][]byte, len(level)/2)
	for i := range parents {
		parents[i] = stemBranchHash(level[i*2], level[i*2+1], hasher)
	}
	return parents
}

func encodeStem(stem *Stem, hasher Hasher) []byte {
	root := stem.ValuesRoot(hasher)
	capacity := len(stemEncodingMagic) + len(root) + len(stem.present) + stem.count*2
	for i := 0; i < StemSuffixCount; i++ {
		if stem.has(byte(i)) {
			capacity += len(stem.values[i])
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
		n := binary.PutUvarint(size[:], uint64(len(stem.values[i])))
		data = append(data, size[:n]...)
		data = append(data, stem.values[i]...)
	}
	return data
}

func decodeStem(data []byte, hasher Hasher) (*Stem, error) {
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
		stem.values[i] = bytes.Clone(data[offset:end])
		offset = end
	}
	if offset != len(data) || !bytes.Equal(stem.ValuesRoot(hasher), wantRoot) {
		return nil, ErrInvalidStem
	}
	return stem, nil
}

// stemEncodedValueCount reads only the fixed stem header. Statistics use it
// instead of rebuilding the 256-leaf value root for every stem.
func stemEncodedValueCount(data []byte) (int, error) {
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

// StemTrie adapts a normal ASCT to 32-byte binary stem keys. Only the 31-byte
// stem is inserted into the outer trie. Its 256 suffix values are encoded as
// one value, making the stem the unit of aging, archiving, and activation.
type StemTrie struct {
	backend *Trie
	locks   [256]sync.Mutex
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
		backend.stemView = &StemTrie{backend: backend}
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
// payload, updates it, and writes the complete stem back as one hot leaf.
func (t *StemTrie) Put(key, value []byte) error {
	stemKey, suffix, err := splitStemKey(key)
	if err != nil {
		return err
	}
	lock := &t.locks[stemKey[0]]
	lock.Lock()
	defer lock.Unlock()

	stem, err := t.loadStem(stemKey)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return err
	}
	if stem == nil {
		stem = NewStem()
	}
	stem.Put(suffix, value)
	return t.backend.Put(stemKey, encodeStem(stem, t.backend.hasher))
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

	rawPuts := make([]KeyValue, 0, len(groups))
	rawDeletes := make([][]byte, 0)
	for _, group := range groups {
		stem, err := t.loadStem(group.key)
		if err != nil && !errors.Is(err, ErrNodeNotFound) {
			return err
		}
		if stem == nil {
			stem = NewStem()
		}
		for _, update := range group.updates {
			if update.Delete {
				stem.Delete(update.Key[0])
			} else {
				stem.Put(update.Key[0], update.Value)
			}
		}
		if stem.Len() == 0 {
			if err == nil {
				rawDeletes = append(rawDeletes, group.key)
			}
			continue
		}
		rawPuts = append(rawPuts, KeyValue{Key: group.key, Value: encodeStem(stem, t.backend.hasher)})
	}
	if err := t.backend.PutBatch(rawPuts); err != nil {
		return err
	}
	for _, key := range rawDeletes {
		if err := t.backend.Delete(key); err != nil {
			return err
		}
	}
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
	rawPuts := make([]KeyValue, 0, len(groups))
	rawDeletes := make([][]byte, 0, len(groups))
	for _, group := range groups {
		stem, err := t.loadStem(group.key)
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
			rawDeletes = append(rawDeletes, group.key)
			continue
		}
		rawPuts = append(rawPuts, KeyValue{Key: group.key, Value: encodeStem(stem, t.backend.hasher)})
	}
	if err := t.backend.PutBatch(rawPuts); err != nil {
		return StemDeleteBatchResult{}, err
	}
	for _, key := range rawDeletes {
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

	stem, err := t.loadStem(stemKey)
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
		return t.backend.Delete(stemKey)
	}
	return t.backend.Put(stemKey, encodeStem(stem, t.backend.hasher))
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

	payload, err := t.backend.Get(stemKey)
	if err != nil {
		return err
	}
	if _, err := decodeStem(payload, t.backend.hasher); err != nil {
		return err
	}
	return t.backend.Activate(stemKey, payload)
}

// Prove returns the current ValuesRoot and an eight-hash suffix proof.
func (t *StemTrie) Prove(key []byte) ([]byte, StemProof, error) {
	stemKey, suffix, err := splitStemKey(key)
	if err != nil {
		return nil, StemProof{}, err
	}
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
	err := t.backend.ForEachAll(func(stemKey, payload []byte) bool {
		stem, err := decodeStem(payload, t.backend.hasher)
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
	if t == nil || t.backend == nil {
		return nil, ErrNilStemTrie
	}
	payload, err := t.backend.Get(stemKey)
	if err != nil {
		return nil, err
	}
	stem, err := decodeStem(payload, t.backend.hasher)
	if err != nil {
		return nil, fmt.Errorf("%w for %x", err, stemKey)
	}
	return stem, nil
}

func splitStemKey(key []byte) ([]byte, byte, error) {
	if len(key) != StemKeySize {
		return nil, 0, ErrInvalidStemKey
	}
	return key[:StemSize], key[StemSize], nil
}

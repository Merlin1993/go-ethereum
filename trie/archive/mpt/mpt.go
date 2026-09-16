// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.

// Package mpt provides the A6 fallback implementation: a hexary MPT hot
// layer with the archive package's shard-oriented cold storage semantics.
// It is deliberately a separate package so the parent trie package can use
// the native MPT without creating an import cycle.
package mpt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	gethtrie "github.com/ethereum/go-ethereum/trie"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

var (
	ErrNotFound  = errors.New("mpt archive: key not found")
	archiveMagic = [4]byte{'M', 'A', 'R', 'C'}
	indexMagic   = [4]byte{'M', 'I', 'D', 'X'}
)

const (
	recordVersion = byte(1)
	maxRecordSize = 1 << 30
)

var (
	nodePrefix    = []byte{'M', 'P', 'T', 'N', 1}
	flatPrefix    = []byte{'M', 'P', 'T', 'F', 1}
	archivePrefix = []byte{'M', 'P', 'T', 'A', 1}
	indexKey      = []byte{'M', 'P', 'T', 'I', 1}
)

// Config controls the MPT fallback. ShardDepth is a routing prefix in bits;
// the hot tree itself remains the native 16-way geth MPT.
type Config struct {
	ShardDepth                int
	CuckooBuckets             int
	CuckooSlots               int
	ActivateArchivedKeyOnRead bool
	// NodeCacheBytes bounds the in-memory cache of serialized trie nodes
	// shared by all tries over one store. Zero uses defaultNodeCacheBytes.
	NodeCacheBytes int64
}

// defaultNodeCacheBytes is the fallback node cache budget (see Config).
const defaultNodeCacheBytes = 64 << 20

// KeyValue is one ordered MPT write used by PutBatch.
type KeyValue struct {
	Key   []byte
	Value []byte
}

func defaultConfig() *Config {
	return &Config{ShardDepth: 16, CuckooBuckets: 32, CuckooSlots: 4}
}

// Trie is a shard-routed MPT hot layer backed by archived shard records.
// Values are kept in flat records and MPT leaves contain their Keccak refs.
type Trie struct {
	mu     sync.Mutex
	db     archivetrie.KVStore
	config Config
	ndb    *nodeDatabase

	shards    map[int]*gethtrie.Trie
	shardRoot map[int][]byte
	dirty     map[int]struct{}

	// aggregate is the persistent shard-root trie opened from the last
	// committed root. Commit stages only this batch's shard-root updates
	// into it (T2 incremental aggregate, 2026-09-16); it replaces the
	// per-batch full rebuild that cost O(all shards x depth) hashing.
	aggregate *gethtrie.Trie

	archives          map[int]*archiveShard
	archiveIDs        map[int]struct{}
	archiveIndexDirty bool

	pendingValues map[string][]byte
	pendingDelete map[string]struct{}
	root          []byte
	pruneShardIdx int
}

type archiveShard struct {
	entries map[string][]byte
	filter  *cuckoo.Filter
	dirty   bool
	loaded  bool
}

// New constructs an MPT archive trie. The root is the aggregate MPT root
// returned by Commit; nil and EmptyRootHash both mean an empty trie.
func New(root []byte, db archivetrie.KVStore, config *Config) (*Trie, error) {
	if db == nil {
		return nil, errors.New("mpt archive: nil database")
	}
	cfg := *defaultConfig()
	if config != nil {
		cfg = *config
		if cfg.CuckooBuckets == 0 {
			cfg.CuckooBuckets = 32
		}
		if cfg.CuckooSlots == 0 {
			cfg.CuckooSlots = 4
		}
	}
	if cfg.ShardDepth < 0 || cfg.ShardDepth > 30 {
		return nil, fmt.Errorf("mpt archive: shard depth %d out of range", cfg.ShardDepth)
	}
	if cfg.NodeCacheBytes == 0 {
		cfg.NodeCacheBytes = defaultNodeCacheBytes
	}
	t := &Trie{
		db:            db,
		config:        cfg,
		ndb:           &nodeDatabase{store: db, cache: newNodeCache(cfg.NodeCacheBytes)},
		shards:        make(map[int]*gethtrie.Trie),
		shardRoot:     make(map[int][]byte),
		dirty:         make(map[int]struct{}),
		archives:      make(map[int]*archiveShard),
		archiveIDs:    make(map[int]struct{}),
		pendingValues: make(map[string][]byte),
		pendingDelete: make(map[string]struct{}),
		root:          normalizeRoot(root),
	}
	if err := t.loadArchiveIndex(); err != nil {
		return nil, err
	}
	if err := t.loadShardRoots(); err != nil {
		return nil, err
	}
	return t, nil
}

// Root returns the last committed aggregate root, or nil for an empty trie.
func (t *Trie) Root() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return bytes.Clone(t.root)
}

func normalizeRoot(root []byte) []byte {
	if len(root) != common.HashLength || bytes.Equal(root, types.EmptyRootHash.Bytes()) {
		return nil
	}
	return bytes.Clone(root)
}

func prefixed(prefix, value []byte) []byte {
	key := make([]byte, len(prefix)+len(value))
	copy(key, prefix)
	copy(key[len(prefix):], value)
	return key
}

func nodeKey(hash common.Hash) []byte { return prefixed(nodePrefix, hash.Bytes()) }
func flatKey(key []byte) []byte       { return prefixed(flatPrefix, key) }

func optionalGet(store archivetrie.KVStore, key []byte) ([]byte, error) {
	value, err := store.Get(key)
	if err != nil && isMissingError(err) {
		return nil, nil
	}
	return value, err
}

func isMissingError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not found") || strings.Contains(message, "notfound")
}

func archiveKey(id int) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(id))
	return prefixed(archivePrefix, encoded[:])
}

func shardKey(id int) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(id))
	return encoded[:]
}

func parseShardKey(key []byte) (int, bool) {
	if len(key) != 4 {
		return 0, false
	}
	return int(binary.BigEndian.Uint32(key)), true
}

func (t *Trie) shardID(key []byte) int {
	id := 0
	for i := 0; i < t.config.ShardDepth; i++ {
		if i/8 < len(key) && key[i/8]&(1<<uint(7-i%8)) != 0 {
			id |= 1 << uint(t.config.ShardDepth-1-i)
		}
	}
	return id
}

func (t *Trie) loadShardRoots() error {
	if len(t.root) == 0 {
		return nil
	}
	aggregate, err := gethtrie.New(gethtrie.TrieID(common.BytesToHash(t.root)), t.ndb)
	if err != nil {
		return err
	}
	it, err := aggregate.NodeIterator(nil)
	if err != nil {
		return err
	}
	iter := gethtrie.NewIterator(it)
	for iter.Next() {
		id, ok := parseShardKey(iter.Key)
		if !ok || len(iter.Value) != common.HashLength {
			return errors.New("mpt archive: invalid aggregate shard record")
		}
		if root := normalizeRoot(iter.Value); root != nil {
			t.shardRoot[id] = root
		}
	}
	return iter.Err
}

func (t *Trie) loadArchiveIndex() error {
	data, err := optionalGet(t.db, indexKey)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	return decodeArchiveIndex(data, func(id int) {
		t.archiveIDs[id] = struct{}{}
	})
}

func (t *Trie) hotLocked(id int) (*gethtrie.Trie, error) {
	if hot := t.shards[id]; hot != nil {
		return hot, nil
	}
	var hot *gethtrie.Trie
	var err error
	if root := t.shardRoot[id]; root != nil {
		hot, err = gethtrie.New(gethtrie.TrieID(common.BytesToHash(root)), t.ndb)
	} else {
		hot = gethtrie.NewEmpty(t.ndb)
	}
	if err != nil {
		return nil, err
	}
	t.shards[id] = hot
	return hot, nil
}

func (t *Trie) markDirtyLocked(id int) { t.dirty[id] = struct{}{} }

// ensureAggregateLocked opens (or lazily reopens) the persistent shard-root
// aggregate at the last committed root. Commit stages only the batch's dirty
// shard roots into this trie, so each commit rehashes only the touched
// aggregate paths instead of rebuilding all shard entries (T2, 2026-09-16).
func (t *Trie) ensureAggregateLocked() error {
	if t.aggregate != nil {
		return nil
	}
	if len(t.root) == 0 {
		t.aggregate = gethtrie.NewEmpty(t.ndb)
		return nil
	}
	aggregate, err := gethtrie.New(gethtrie.TrieID(common.BytesToHash(t.root)), t.ndb)
	if err != nil {
		return err
	}
	t.aggregate = aggregate
	return nil
}

func (t *Trie) loadArchiveLocked(id int) (*archiveShard, error) {
	if shard := t.archives[id]; shard != nil && shard.loaded {
		return shard, nil
	}
	shard := &archiveShard{entries: make(map[string][]byte), loaded: true}
	data, err := optionalGet(t.db, archiveKey(id))
	if err != nil {
		return nil, err
	}
	if len(data) != 0 {
		if err := t.decodeArchive(data, shard); err != nil {
			return nil, err
		}
	}
	t.archives[id] = shard
	return shard, nil
}

func (t *Trie) decodeArchive(data []byte, shard *archiveShard) error {
	if len(data) < 17 || len(data) > maxRecordSize || !bytes.Equal(data[:4], archiveMagic[:]) || data[4] != recordVersion {
		return errors.New("mpt archive: invalid archive record")
	}
	filterLen := int(binary.BigEndian.Uint32(data[5:9]))
	count := int(binary.BigEndian.Uint32(data[9:13]))
	offset := 13
	if filterLen < 0 || offset+filterLen+4 > len(data) {
		return errors.New("mpt archive: invalid archive filter length")
	}
	if filterLen > 0 {
		filter := cuckoo.New(0, 0)
		if err := filter.Decode(data[offset:offset+filterLen], 0, 0); err != nil {
			return err
		}
		shard.filter = filter
	}
	offset += filterLen
	for i := 0; i < count; i++ {
		if offset+8 > len(data) {
			return errors.New("mpt archive: truncated archive entry")
		}
		keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		refLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += 8
		if keyLen < 0 || refLen != common.HashLength || offset+keyLen+refLen > len(data) {
			return errors.New("mpt archive: invalid archive entry")
		}
		key := bytes.Clone(data[offset : offset+keyLen])
		offset += keyLen
		ref := bytes.Clone(data[offset : offset+refLen])
		offset += refLen
		shard.entries[string(key)] = ref
	}
	if offset != len(data) {
		return errors.New("mpt archive: trailing archive bytes")
	}
	return nil
}

func (t *Trie) rebuildFilter(shard *archiveShard) {
	if len(shard.entries) == 0 {
		shard.filter = nil
		return
	}
	filter := cuckoo.New(t.config.CuckooBuckets, t.config.CuckooSlots)
	for key := range shard.entries {
		if err := filter.Insert([]byte(key)); err != nil {
			// The exact map remains authoritative when a deliberately tiny
			// diagnostic filter fills up.
			shard.filter = nil
			return
		}
	}
	shard.filter = filter
}

func (t *Trie) removeArchivedLocked(id int, key []byte) (bool, error) {
	shard, err := t.loadArchiveLocked(id)
	if err != nil {
		return false, err
	}
	if _, ok := shard.entries[string(key)]; !ok {
		return false, nil
	}
	delete(shard.entries, string(key))
	t.rebuildFilter(shard)
	shard.dirty = true
	if len(shard.entries) == 0 {
		delete(t.archiveIDs, id)
	}
	t.archiveIndexDirty = true
	return true, nil
}

func (t *Trie) archiveLookupLocked(id int, key []byte) ([]byte, bool, error) {
	shard, err := t.loadArchiveLocked(id)
	if err != nil {
		return nil, false, err
	}
	if shard.filter != nil && !shard.filter.Lookup(key) {
		return nil, false, nil
	}
	ref, ok := shard.entries[string(key)]
	if !ok {
		return nil, false, nil
	}
	return bytes.Clone(ref), true, nil
}

func (t *Trie) valueLocked(key []byte) ([]byte, error) {
	name := string(key)
	if value, ok := t.pendingValues[name]; ok {
		return bytes.Clone(value), nil
	}
	if _, deleted := t.pendingDelete[name]; deleted {
		return nil, ErrNotFound
	}
	value, err := optionalGet(t.db, flatKey(key))
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, ErrNotFound
	}
	return value, nil
}

// Get returns a value from the MPT hot layer or an archived shard. An archive
// hit can optionally promote the key into the hot MPT.
func (t *Trie) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.shardID(key)
	hot, err := t.hotLocked(id)
	if err != nil {
		return nil, err
	}
	ref, err := hot.Get(key)
	fromArchive := false
	if err != nil {
		return nil, err
	}
	if len(ref) == 0 {
		ref, fromArchive, err = t.archiveLookupLocked(id, key)
		if err != nil || !fromArchive {
			return nil, ErrNotFound
		}
	}
	value, err := t.valueLocked(key)
	if err != nil {
		return nil, err
	}
	if len(ref) == common.HashLength && !bytes.Equal(ref, crypto.Keccak256Hash(value).Bytes()) {
		return nil, errors.New("mpt archive: value reference verification failed")
	}
	if fromArchive && t.config.ActivateArchivedKeyOnRead {
		if _, err := t.removeArchivedLocked(id, key); err != nil {
			return nil, err
		}
		if err := hot.Update(key, ref); err != nil {
			return nil, err
		}
		t.markDirtyLocked(id)
	}
	return value, nil
}

// GetValueRef returns the hot or archived commitment without reading flat data.
func (t *Trie) GetValueRef(key []byte) ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.shardID(key)
	hot, err := t.hotLocked(id)
	if err != nil {
		return nil, false, err
	}
	ref, err := hot.Get(key)
	if err != nil {
		return nil, false, err
	}
	if len(ref) != 0 {
		return bytes.Clone(ref), false, nil
	}
	ref, ok, err := t.archiveLookupLocked(id, key)
	return ref, ok, err
}

// Put inserts or replaces a value in the hot MPT and stages its flat record.
func (t *Trie) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.shardID(key)
	hot, err := t.hotLocked(id)
	if err != nil {
		return err
	}
	if _, err := t.removeArchivedLocked(id, key); err != nil {
		return err
	}
	ref := crypto.Keccak256Hash(value).Bytes()
	if err := hot.Update(key, ref); err != nil {
		return err
	}
	name := string(key)
	t.pendingValues[name] = bytes.Clone(value)
	delete(t.pendingDelete, name)
	t.markDirtyLocked(id)
	return nil
}

// PutBatch applies writes in input order. The order is significant when a key
// appears more than once in the batch, matching the native trie update model.
func (t *Trie) PutBatch(entries []KeyValue) error {
	for _, entry := range entries {
		if err := t.Put(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a key from both the hot MPT and its archive record.
func (t *Trie) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.shardID(key)
	hot, err := t.hotLocked(id)
	if err != nil {
		return err
	}
	if err := hot.Delete(key); err != nil {
		return err
	}
	if _, err := t.removeArchivedLocked(id, key); err != nil {
		return err
	}
	name := string(key)
	delete(t.pendingValues, name)
	t.pendingDelete[name] = struct{}{}
	t.markDirtyLocked(id)
	return nil
}

// DeleteBatch removes keys in input order.
func (t *Trie) DeleteBatch(keys [][]byte) error {
	for _, key := range keys {
		if err := t.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// PruneNextShard archives the current hot contents of the next round-robin
// shard. The actual record write is deferred until Commit.
func (t *Trie) PruneNextShard() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	shardCount := 1 << uint(t.config.ShardDepth)
	id := t.pruneShardIdx
	t.pruneShardIdx = (t.pruneShardIdx + 1) % shardCount
	hot, err := t.hotLocked(id)
	if err != nil {
		return err
	}
	entries := make(map[string][]byte)
	it, err := hot.NodeIterator(nil)
	if err != nil {
		return err
	}
	iter := gethtrie.NewIterator(it)
	for iter.Next() {
		entries[string(iter.Key)] = bytes.Clone(iter.Value)
	}
	if err := iter.Err; err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	archiveShard, err := t.loadArchiveLocked(id)
	if err != nil {
		return err
	}
	for key, ref := range entries {
		archiveShard.entries[key] = ref
	}
	t.rebuildFilter(archiveShard)
	archiveShard.dirty = true
	archiveShard.loaded = true
	t.archiveIDs[id] = struct{}{}
	t.archiveIndexDirty = true
	t.shards[id] = gethtrie.NewEmpty(t.ndb)
	delete(t.shardRoot, id)
	t.markDirtyLocked(id)
	return nil
}

// Hash returns the aggregate root without writing any nodes.
func (t *Trie) Hash() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	roots, err := t.currentShardRootsLocked()
	if err != nil {
		return nil, err
	}
	aggregate, err := t.buildAggregate(roots)
	if err != nil {
		return nil, err
	}
	return normalizeRoot(aggregate.Hash().Bytes()), nil
}

func (t *Trie) currentShardRootsLocked() (map[int][]byte, error) {
	roots := make(map[int][]byte, len(t.shardRoot))
	for id, root := range t.shardRoot {
		if root != nil {
			roots[id] = bytes.Clone(root)
		}
	}
	for id := range t.dirty {
		hot := t.shards[id]
		if hot == nil {
			delete(roots, id)
			continue
		}
		root := normalizeRoot(hot.Hash().Bytes())
		if root == nil {
			delete(roots, id)
		} else {
			roots[id] = root
		}
	}
	return roots, nil
}

func (t *Trie) buildAggregate(roots map[int][]byte) (*gethtrie.Trie, error) {
	aggregate := gethtrie.NewEmpty(t.ndb)
	ids := make([]int, 0, len(roots))
	for id := range roots {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		if err := aggregate.Update(shardKey(id), roots[id]); err != nil {
			return nil, err
		}
	}
	return aggregate, nil
}

func writeNodeSet(batch archivetrie.Batcher, set *trienode.NodeSet) error {
	if set == nil {
		return nil
	}
	for _, node := range set.Nodes {
		if node == nil || node.IsDeleted() {
			continue
		}
		if err := batch.Put(nodeKey(node.Hash), node.Blob); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) encodeArchive(shard *archiveShard) []byte {
	keys := make([]string, 0, len(shard.entries))
	for key := range shard.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	filter := []byte(nil)
	if shard.filter != nil {
		filter = shard.filter.Encode()
	}
	size := 13 + len(filter)
	for _, key := range keys {
		size += 8 + len(key) + len(shard.entries[key])
	}
	data := make([]byte, size)
	copy(data[:4], archiveMagic[:])
	data[4] = recordVersion
	binary.BigEndian.PutUint32(data[5:9], uint32(len(filter)))
	binary.BigEndian.PutUint32(data[9:13], uint32(len(keys)))
	offset := 13
	copy(data[offset:], filter)
	offset += len(filter)
	for _, key := range keys {
		ref := shard.entries[key]
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(key)))
		binary.BigEndian.PutUint32(data[offset+4:offset+8], uint32(len(ref)))
		offset += 8
		copy(data[offset:], key)
		offset += len(key)
		copy(data[offset:], ref)
		offset += len(ref)
	}
	return data
}

// indexVersionBitmap is the second encoding of the archive index: a fixed-size
// bitmap over all shard ids (2^ShardDepth bits = 4 KiB at depth 15). The v1
// list encoding grew to ~130 KiB once most shards were archived and was
// rewritten in full on every batch, which dominated commit cost in B3m2.
// Readers still accept v1; writers always emit v2.
const (
	indexVersionList   = byte(1)
	indexVersionBitmap = byte(2)
)

func (t *Trie) encodeArchiveIndex() []byte {
	shards := 1 << uint(t.config.ShardDepth)
	data := make([]byte, 9+(shards+7)/8)
	copy(data[:4], indexMagic[:])
	data[4] = indexVersionBitmap
	binary.BigEndian.PutUint32(data[5:9], uint32(shards))
	for id := range t.archiveIDs {
		if id >= 0 && id < shards {
			data[9+id/8] |= 1 << uint(id%8)
		}
	}
	return data
}

// decodeArchiveIndex accepts both index encodings and reports every archived
// shard id through insert.
func decodeArchiveIndex(data []byte, insert func(id int)) error {
	if len(data) < 9 || !bytes.Equal(data[:4], indexMagic[:]) {
		return errors.New("mpt archive: invalid archive index")
	}
	switch data[4] {
	case indexVersionList:
		count := int(binary.BigEndian.Uint32(data[5:9]))
		if count < 0 || len(data) != 9+count*4 {
			return errors.New("mpt archive: invalid archive index length")
		}
		for offset := 9; offset < len(data); offset += 4 {
			insert(int(binary.BigEndian.Uint32(data[offset : offset+4])))
		}
		return nil
	case indexVersionBitmap:
		shards := int(binary.BigEndian.Uint32(data[5:9]))
		if shards <= 0 || len(data) != 9+(shards+7)/8 {
			return errors.New("mpt archive: invalid archive bitmap length")
		}
		for id := 0; id < shards; id++ {
			if data[9+id/8]&(1<<uint(id%8)) != 0 {
				insert(id)
			}
		}
		return nil
	}
	return errors.New("mpt archive: unknown archive index version")
}

// stagedBytes is the per-commit split of staged batch volume by record class,
// reported to archive.LastCommitDiagnostics so hot-layer commit cost can be
// attributed (nodes vs archive records vs flat values vs index) without
// changing any call site's flush semantics.
type stagedBytes struct {
	hotNode, aggregateNode, archive, flat, index int64
}

func (s *stagedBytes) add(key []byte, value []byte, nodeAsAggregate bool) {
	if len(key) < 4 || key[0] != 'M' || key[1] != 'P' || key[2] != 'T' {
		return
	}
	n := int64(len(key) + len(value))
	switch key[3] {
	case 'N':
		if nodeAsAggregate {
			s.aggregateNode += n
		} else {
			s.hotNode += n
		}
	case 'A':
		s.archive += n
	case 'I':
		s.index += n
	case 'F':
		s.flat += n
	}
}

// recordingBatcher is a pass-through Batcher that tallies staged bytes.
type recordingBatcher struct {
	archivetrie.Batcher
	staged          *stagedBytes
	nodeAsAggregate bool
}

func (r *recordingBatcher) Put(key, value []byte) error {
	r.staged.add(key, value, r.nodeAsAggregate)
	return r.Batcher.Put(key, value)
}

func (r *recordingBatcher) Delete(key []byte) error {
	r.staged.add(key, nil, r.nodeAsAggregate)
	return r.Batcher.Delete(key)
}

// CommitToBatch stages dirty MPT nodes, archive records, flat values and the
// aggregate shard-root MPT into the CALLER's batch and returns the new root.
// The writes become durable only when the caller flushes the batch with
// Write; the caller owns the batch lifecycle. The trie's in-memory state
// advances as if committed, so a staged batch must not be discarded after a
// successful call. destructive is accepted for hot-layer interface parity and
// is a no-op: committed shard tries are already released.
func (t *Trie) CommitToBatch(batch archivetrie.Batcher, destructive bool) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitToBatchLocked(batch)
}

// Commit persists dirty MPT nodes, archive records, flat values and the
// aggregate shard-root MPT in one self-managed database batch. Drivers that
// already own a shared batch should call CommitToBatch instead so the write
// rides the driver's flush (T1 commit-merge, 2026-09-16).
func (t *Trie) Commit() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	batch := t.db.NewBatch()
	defer batch.Reset()
	root, err := t.commitToBatchLocked(batch)
	if err != nil {
		return nil, err
	}
	if err := batch.Write(); err != nil {
		return nil, err
	}
	return root, nil
}

func (t *Trie) commitToBatchLocked(batch archivetrie.Batcher) ([]byte, error) {
	staged := &stagedBytes{}
	rec := &recordingBatcher{Batcher: batch, staged: staged}
	defer func() {
		archivetrie.SetMPTCommitStagedBytes(staged.hotNode, staged.aggregateNode, staged.archive, staged.flat, staged.index)
		archivetrie.SetMPTNodeCacheStats(t.ndb.cache.stats())
	}()
	if len(t.dirty) == 0 && len(t.pendingValues) == 0 && len(t.pendingDelete) == 0 && !t.archiveIndexDirty {
		return bytes.Clone(t.root), nil
	}
	if err := t.ensureAggregateLocked(); err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(t.dirty))
	for id := range t.dirty {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		hot := t.shards[id]
		if hot == nil {
			delete(t.shardRoot, id)
			if err := t.aggregate.Delete(shardKey(id)); err != nil {
				return nil, err
			}
			continue
		}
		root, nodes := hot.Commit(false)
		if err := writeNodeSet(rec, nodes); err != nil {
			return nil, err
		}
		if normalized := normalizeRoot(root.Bytes()); normalized != nil {
			t.shardRoot[id] = normalized
			if err := t.aggregate.Update(shardKey(id), normalized); err != nil {
				return nil, err
			}
		} else {
			delete(t.shardRoot, id)
			if err := t.aggregate.Delete(shardKey(id)); err != nil {
				return nil, err
			}
		}
		t.shards[id] = nil
	}
	aggregateRoot, nodes := t.aggregate.Commit(false)
	// A committed geth trie is single-use; drop the handle so the next
	// commit reopens lazily from aggregateRoot. Reopening hashes nothing —
	// interior nodes come from the node cache — so the incremental cost
	// stays proportional to this batch's dirty shards.
	t.aggregate = nil
	rec.nodeAsAggregate = true
	err := writeNodeSet(rec, nodes)
	rec.nodeAsAggregate = false
	if err != nil {
		return nil, err
	}
	for id, shard := range t.archives {
		if !shard.dirty {
			continue
		}
		if len(shard.entries) == 0 {
			if err := rec.Delete(archiveKey(id)); err != nil {
				return nil, err
			}
		} else if err := rec.Put(archiveKey(id), t.encodeArchive(shard)); err != nil {
			return nil, err
		}
		shard.dirty = false
	}
	if t.archiveIndexDirty {
		if len(t.archiveIDs) == 0 {
			if err := rec.Delete(indexKey); err != nil {
				return nil, err
			}
		} else if err := rec.Put(indexKey, t.encodeArchiveIndex()); err != nil {
			return nil, err
		}
		t.archiveIndexDirty = false
	}
	keys := make([]string, 0, len(t.pendingValues))
	for key := range t.pendingValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := rec.Put(flatKey([]byte(key)), t.pendingValues[key]); err != nil {
			return nil, err
		}
	}
	deleteKeys := make([]string, 0, len(t.pendingDelete))
	for key := range t.pendingDelete {
		deleteKeys = append(deleteKeys, key)
	}
	sort.Strings(deleteKeys)
	for _, key := range deleteKeys {
		if err := rec.Delete(flatKey([]byte(key))); err != nil {
			return nil, err
		}
	}
	t.pendingValues = make(map[string][]byte)
	t.pendingDelete = make(map[string]struct{})
	t.dirty = make(map[int]struct{})
	t.root = normalizeRoot(aggregateRoot.Bytes())
	return bytes.Clone(t.root), nil
}

// ForEach visits all hot and archived values in key order.
func (t *Trie) ForEach(fn func(key, value []byte) bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	values := make(map[string][]byte)
	ids := make(map[int]struct{}, len(t.shardRoot)+len(t.shards)+len(t.archiveIDs))
	for id := range t.shardRoot {
		ids[id] = struct{}{}
	}
	for id := range t.shards {
		ids[id] = struct{}{}
	}
	for id := range t.archiveIDs {
		ids[id] = struct{}{}
	}
	for id := range ids {
		hot, err := t.hotLocked(id)
		if err == nil {
			if it, iterErr := hot.NodeIterator(nil); iterErr == nil {
				iter := gethtrie.NewIterator(it)
				for iter.Next() {
					if value, valueErr := t.valueLocked(iter.Key); valueErr == nil {
						values[string(iter.Key)] = value
					}
				}
			}
		}
	}
	for id := range t.archiveIDs {
		shard, err := t.loadArchiveLocked(id)
		if err != nil {
			continue
		}
		for key := range shard.entries {
			if value, valueErr := t.valueLocked([]byte(key)); valueErr == nil {
				values[key] = value
			}
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !fn([]byte(key), values[key]) {
			return
		}
	}
}

// nodeDatabase is a read-through adapter from geth's trie node accessor onto
// the archive KVStore. Committed tries are single-use (T2 drops the aggregate
// handle every commit and hot shards are released after pruning), so readers
// reopen them constantly; the embedded size-limited cache absorbs those
// reads. It is shared by every Trie over the same store (one per New call),
// so a per-instance cache would thrash the moment a run opens many tries.
type nodeDatabase struct {
	store archivetrie.KVStore
	cache *nodeCache
}

// nodeCache is a small content-addressed LRU of serialized trie nodes.
// Zero/negative sizes disable caching.
type nodeCache struct {
	enabled bool
	c       *lru.SizeConstrainedCache[common.Hash, []byte]
	gets    atomic.Int64
	hits    atomic.Int64
}

func newNodeCache(maxBytes int64) *nodeCache {
	if maxBytes <= 0 {
		return &nodeCache{}
	}
	return &nodeCache{enabled: true, c: lru.NewSizeConstrainedCache[common.Hash, []byte](uint64(maxBytes))}
}

func (nc *nodeCache) get(hash common.Hash) ([]byte, bool) {
	if nc == nil || !nc.enabled {
		return nil, false
	}
	nc.gets.Add(1)
	if blob, ok := nc.c.Get(hash); ok {
		nc.hits.Add(1)
		return blob, true
	}
	return nil, false
}

func (nc *nodeCache) put(hash common.Hash, blob []byte) {
	if nc == nil || !nc.enabled {
		return
	}
	nc.c.Add(hash, blob)
}

// stats reports cumulative cache gets and hits for diagnostics.
func (nc *nodeCache) stats() (gets, hits int64) {
	if nc == nil {
		return 0, 0
	}
	return nc.gets.Load(), nc.hits.Load()
}

func (d *nodeDatabase) NodeReader(common.Hash) (database.NodeReader, error) {
	return nodeReader{store: d.store, cache: d.cache}, nil
}

type nodeReader struct {
	store archivetrie.KVStore
	cache *nodeCache
}

// Node is read-through over the shared node cache: content-addressed, so a
// hit is always valid regardless of which trie asked.
func (r nodeReader) Node(_ common.Hash, _ []byte, hash common.Hash) ([]byte, error) {
	if blob, ok := r.cache.get(hash); ok {
		return blob, nil
	}
	blob, err := r.store.Get(nodeKey(hash))
	if err != nil {
		return nil, err
	}
	if len(blob) == 0 {
		return nil, fmt.Errorf("mpt archive: node %x not found", hash)
	}
	r.cache.put(hash, blob)
	return blob, nil
}

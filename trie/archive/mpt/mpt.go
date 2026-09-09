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

	"github.com/ethereum/go-ethereum/common"
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
}

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
	t := &Trie{
		db:            db,
		config:        cfg,
		ndb:           &nodeDatabase{store: db},
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
	if len(data) < 9 || !bytes.Equal(data[:4], indexMagic[:]) || data[4] != recordVersion {
		return errors.New("mpt archive: invalid archive index")
	}
	count := int(binary.BigEndian.Uint32(data[5:9]))
	if count < 0 || len(data) != 9+count*4 {
		return errors.New("mpt archive: invalid archive index length")
	}
	for offset := 9; offset < len(data); offset += 4 {
		t.archiveIDs[int(binary.BigEndian.Uint32(data[offset:offset+4]))] = struct{}{}
	}
	return nil
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

func (t *Trie) encodeArchiveIndex() []byte {
	ids := make([]int, 0, len(t.archiveIDs))
	for id := range t.archiveIDs {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	data := make([]byte, 9+len(ids)*4)
	copy(data[:4], indexMagic[:])
	data[4] = recordVersion
	binary.BigEndian.PutUint32(data[5:9], uint32(len(ids)))
	for i, id := range ids {
		binary.BigEndian.PutUint32(data[9+i*4:13+i*4], uint32(id))
	}
	return data
}

// Commit persists dirty MPT nodes, archive records, flat values and the
// aggregate shard-root MPT in one database batch.
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
	if len(t.dirty) == 0 && len(t.pendingValues) == 0 && len(t.pendingDelete) == 0 && !t.archiveIndexDirty {
		return bytes.Clone(t.root), nil
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
			continue
		}
		root, nodes := hot.Commit(false)
		if err := writeNodeSet(batch, nodes); err != nil {
			return nil, err
		}
		if normalized := normalizeRoot(root.Bytes()); normalized != nil {
			t.shardRoot[id] = normalized
		} else {
			delete(t.shardRoot, id)
		}
		t.shards[id] = nil
	}
	roots := make(map[int][]byte, len(t.shardRoot))
	for id, root := range t.shardRoot {
		roots[id] = bytes.Clone(root)
	}
	aggregate, err := t.buildAggregate(roots)
	if err != nil {
		return nil, err
	}
	aggregateRoot, nodes := aggregate.Commit(false)
	if err := writeNodeSet(batch, nodes); err != nil {
		return nil, err
	}
	for id, shard := range t.archives {
		if !shard.dirty {
			continue
		}
		if len(shard.entries) == 0 {
			if err := batch.Delete(archiveKey(id)); err != nil {
				return nil, err
			}
		} else if err := batch.Put(archiveKey(id), t.encodeArchive(shard)); err != nil {
			return nil, err
		}
		shard.dirty = false
	}
	if t.archiveIndexDirty {
		if len(t.archiveIDs) == 0 {
			if err := batch.Delete(indexKey); err != nil {
				return nil, err
			}
		} else if err := batch.Put(indexKey, t.encodeArchiveIndex()); err != nil {
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
		if err := batch.Put(flatKey([]byte(key)), t.pendingValues[key]); err != nil {
			return nil, err
		}
	}
	deleteKeys := make([]string, 0, len(t.pendingDelete))
	for key := range t.pendingDelete {
		deleteKeys = append(deleteKeys, key)
	}
	sort.Strings(deleteKeys)
	for _, key := range deleteKeys {
		if err := batch.Delete(flatKey([]byte(key))); err != nil {
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

type nodeDatabase struct{ store archivetrie.KVStore }

func (d *nodeDatabase) NodeReader(common.Hash) (database.NodeReader, error) {
	return nodeReader{store: d.store}, nil
}

type nodeReader struct{ store archivetrie.KVStore }

func (r nodeReader) Node(_ common.Hash, _ []byte, hash common.Hash) ([]byte, error) {
	blob, err := r.store.Get(nodeKey(hash))
	if err != nil {
		return nil, err
	}
	if len(blob) == 0 {
		return nil, fmt.Errorf("mpt archive: node %x not found", hash)
	}
	return blob, nil
}

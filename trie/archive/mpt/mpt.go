// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package mpt provides the hexary-MPT hot layer used by the AMT experiments: a
// single 16-way tree over logical prefix domains, with cold data moved into
// archive buckets on a round-robin domain schedule.
//
// It deliberately lives in its own package so the parent trie package can use
// the native MPT without creating an import cycle.
//
// Shape as of the 2026-09-17 rewrite (see
// .agent/amt_pathdb_singletree_plan_20260917.md):
//
//   - ONE tree, not one tree per shard. A domain is only a key-prefix routing
//     concept; there is no per-domain subtree and no aggregate shard-root trie.
//     Those two structures were the measured write-path bottleneck: every batch
//     paid O(shards x depth) hashing to rebuild the aggregate.
//   - Values are inline in the leaves (a real MPT), so evicting a leaf really
//     deletes the active copy. The previous design kept payloads in a separate
//     flat store keyed by the value hash, which made any active-layer size claim
//     depend on a second copy being deleted elsewhere.
//   - The backend is selectable: "hashdb" keeps the previous content-addressed
//     node store, "pathdb" routes through triedb.PathDatabase, which addresses
//     nodes by (owner, path) and thus overwrites in place instead of writing a
//     new hash-addressed record per dirty node per commit.
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
	"time"

	"container/list"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	archivetrie "github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
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

	// indexBlockDomains is how many domains one archive-index bitmap covers.
	// The previous design rewrote a single bitmap over every domain on each
	// batch; at 2^20 domains that is 128 KiB of pure write amplification per
	// prune. Sharding the index means a prune touches 512 bytes.
	indexBlockDomains = 4096
)

// Backend selectors for Config.Backend.
const (
	BackendHash = "hashdb"
	BackendPath = "pathdb"
)

var (
	nodePrefix     = []byte{'M', 'P', 'T', 'N', 1}
	archivePrefix  = []byte{'M', 'P', 'T', 'A', 1}
	indexPrefix    = []byte{'M', 'P', 'T', 'I', 1}
	schedulePrefix = []byte{'M', 'P', 'T', 'S', 1}
)

// scheduleVersion is the on-disk version of the prune-schedule record.
const scheduleVersion = byte(1)

// Config controls the MPT hot layer. DomainNibbles replaces the old ShardDepth:
// domains are now counted in hex nibbles rather than bits, because in a single
// hexary tree a domain boundary has to land on a nibble boundary or it cannot be
// mapped onto a key range at all.
type Config struct {
	DomainNibbles int

	CuckooBuckets int
	CuckooSlots   int

	ActivateArchivedKeyOnRead bool

	// Backend is BackendHash or BackendPath (default BackendHash).
	Backend string

	// NodeCacheBytes bounds the content-addressed node cache used by the hash
	// backend. Zero uses defaultNodeCacheBytes. The path backend does not use
	// it: pathdb owns its own clean cache.
	NodeCacheBytes int64

	// CleanCacheBytes / WriteBufferBytes map onto pathdb.Config. Defaults pick
	// the top of pathdb's allowed range, because the winning behaviour is to let
	// dirty nodes accumulate across batches rather than forcing one sync write
	// per batch.
	CleanCacheBytes  int
	WriteBufferBytes int

	// ForceCommitEveryBatches asks pathdb to flush to its disk layer every N
	// commits. 0 (default) leaves the decision to pathdb, which flushes when its
	// write buffer fills or when the diff-layer limit caps the tree.
	ForceCommitEveryBatches int

	// ArchiveResidentEntries bounds how many archived entries stay memory-
	// resident across all buckets. Clean buckets beyond the budget are
	// evicted back to disk (their Cuckoo filters stay resident, so negative
	// lookups never reload). Without the bound a long run accumulates every
	// archived value in RAM. Zero uses defaultArchiveResidentEntries.
	ArchiveResidentEntries int
}

// defaultNodeCacheBytes is the fallback node cache budget for the hash backend.
const defaultNodeCacheBytes = 64 << 20

const (
	defaultCleanCacheBytes  = 64 << 20
	defaultWriteBufferBytes = 256 << 20
)

// defaultArchiveResidentEntries caps the resident archive working set at
// ~4M entries (order of a few hundred MB with map overhead).
const defaultArchiveResidentEntries = 1 << 22

// KeyValue is one ordered MPT write used by PutBatch.
type KeyValue struct {
	Key   []byte
	Value []byte
}

func defaultConfig() *Config {
	return &Config{
		DomainNibbles: 4,
		CuckooBuckets: 32,
		CuckooSlots:   4,
		Backend:       BackendHash,
	}
}

// Trie is a single hexary state tree with logical prefix domains backed by
// archived domain buckets. Hot values live in the leaves; archived values live
// in archive records keyed by domain.
type Trie struct {
	mu     sync.Mutex
	db     archivetrie.KVStore
	config Config
	ndb    database.NodeDatabase

	// tdb is non-nil only for the path backend. It is the same object ndb points
	// at, retained because the batch path needs its Update/Commit methods.
	// pathWrites tallies everything it writes: pathdb owns its own batching, so
	// those bytes never appear in the caller's batch and would otherwise vanish
	// from every storage-size claim.
	tdb        *triedb.Database
	pathWrites *writeCounter

	// hot is the working tree (forked hexary trie with the paper's epoch
	// bitmap and stubList node fields). A committed trie is single-use, so it
	// is dropped after every commit and reopened lazily from the head root.
	hot   *gethtrie.Trie
	root  common.Hash
	block uint64

	dirty bool

	archives          map[int]*archiveShard
	archiveIDs        map[int]struct{}
	dirtyIndexBlocks  map[int]struct{}
	commitsSinceFlush int

	// archiveLRU orders resident buckets least-recently-used first;
	// residentEntries tallies their entries so eviction can enforce
	// Config.ArchiveResidentEntries without walking the maps.
	archiveLRU      *list.List
	residentEntries int

	// opTrace caches the MPT_OP_TRACE gate so the hot path pays one field
	// load per instrumented section (plan item T4).
	opTrace bool

	// Epoch schedule (P4b, paper's dynamic assignment rule). baseBit is the
	// globally alternating cycle bit G. The rotation is strictly sequential,
	// so "domain d already pruned this cycle" is exactly d < pruneDomainIdx;
	// no per-domain bitmap is needed. The pair (baseBit, pruneDomainIdx) is
	// persisted at commit so a reload interprets leaf epochs correctly.
	baseBit        byte
	pruneDomainIdx int
	scheduleDirty  bool
}

type archiveShard struct {
	// entries is nil when the bucket has been evicted back to disk; filter
	// and count stay resident so negative lookups never touch the disk.
	entries map[string][]byte
	filter  *cuckoo.Filter
	count   int
	dirty   bool          // uncommitted mutation; dirty buckets are never evicted
	elem    *list.Element // non-nil exactly while entries is resident
}

// New constructs the trie. root must be nil or the canonical empty-tree hash:
// pathdb registers one layer per Update, so a mid-history root cannot be adopted
// without also adopting its journal. The experiment always rebuilds from
// genesis, which is what this restriction encodes.
func New(root []byte, db archivetrie.KVStore, config *Config) (*Trie, error) {
	if db == nil {
		return nil, errors.New("mpt archive: nil database")
	}
	cfg := *defaultConfig()
	if config != nil {
		cfg = *config
		if cfg.CuckooBuckets <= 0 {
			cfg.CuckooBuckets = 32
		}
		if cfg.CuckooSlots <= 0 {
			cfg.CuckooSlots = 4
		}
		if cfg.Backend == "" {
			cfg.Backend = BackendHash
		}
		// An unset width falls back to the default rather than failing: callers
		// legitimately pass a partially populated Config.
		if cfg.DomainNibbles <= 0 {
			cfg.DomainNibbles = 4
		}
	}
	if cfg.DomainNibbles > 8 {
		return nil, fmt.Errorf("mpt archive: domain nibble count %d exceeds the 32-byte routing key (max 8)", cfg.DomainNibbles)
	}
	if cfg.Backend != BackendHash && cfg.Backend != BackendPath {
		return nil, fmt.Errorf("mpt archive: unknown backend %q", cfg.Backend)
	}
	if cfg.CleanCacheBytes == 0 {
		cfg.CleanCacheBytes = defaultCleanCacheBytes
	}
	if cfg.WriteBufferBytes == 0 {
		cfg.WriteBufferBytes = defaultWriteBufferBytes
	}
	if cfg.NodeCacheBytes == 0 {
		cfg.NodeCacheBytes = defaultNodeCacheBytes
	}
	if cfg.ArchiveResidentEntries <= 0 {
		cfg.ArchiveResidentEntries = defaultArchiveResidentEntries
	}

	t := &Trie{
		db:               db,
		config:           cfg,
		root:             normalizeRootHash(root),
		archives:         make(map[int]*archiveShard),
		archiveIDs:       make(map[int]struct{}),
		dirtyIndexBlocks: make(map[int]struct{}),
		archiveLRU:       list.New(),
		opTrace:          opTraceGate.Load(),
	}
	switch cfg.Backend {
	case BackendPath:
		// A pathdb layer tree only knows roots it registered itself. Refusing to
		// adopt a foreign root here turns a confusing "layer missing" during a
		// later Get into an immediate construction error.
		if t.root != (common.Hash{}) {
			return nil, errors.New("mpt archive: path backend cannot adopt an existing root; it has no layer history for it")
		}
		tdb, counter, err := openPathDatabase(db, cfg.CleanCacheBytes, cfg.WriteBufferBytes)
		if err != nil {
			return nil, err
		}
		t.tdb = tdb
		t.pathWrites = counter
		t.ndb = tdb
	default:
		t.ndb = &nodeDatabase{store: db, cache: newNodeCache(cfg.NodeCacheBytes)}
	}
	if err := t.loadArchiveIndex(); err != nil {
		return nil, err
	}
	if err := t.loadSchedule(); err != nil {
		return nil, err
	}
	return t, nil
}

// scheduleKey is the singleton KV slot for the prune-schedule record.
func scheduleKey() []byte { return schedulePrefix }

// encodeSchedule serializes (version, baseBit, pruneDomainIdx).
func (t *Trie) encodeSchedule() []byte {
	data := make([]byte, 6)
	data[0] = scheduleVersion
	data[1] = t.baseBit & 1
	binary.BigEndian.PutUint32(data[2:6], uint32(t.pruneDomainIdx))
	return data
}

// loadSchedule restores the prune schedule persisted by the last commit. A
// missing record means genesis: baseBit 0, domain 0. A record whose domain
// index no longer fits the configured domain count (domain depth changed
// between runs) is reset rather than trusted.
func (t *Trie) loadSchedule() error {
	data, err := optionalGet(t.db, scheduleKey())
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	if len(data) != 6 || data[0] != scheduleVersion {
		return errors.New("mpt archive: invalid prune schedule record")
	}
	t.baseBit = data[1] & 1
	idx := int(binary.BigEndian.Uint32(data[2:6]))
	if idx < 0 || idx >= t.domainCount() {
		t.baseBit = 0
		idx = 0
	}
	t.pruneDomainIdx = idx
	return nil
}

// epochForKeyLocked implements the paper's dynamic assignment: writes landing
// in a domain already pruned this cycle get the current base bit; writes to
// not-yet-pruned domains get the opposite bit. This keeps fresh data alive
// through its domain's next one-to-two prunings with a single lifecycle bit.
func (t *Trie) epochForKeyLocked(key []byte) byte {
	if t.domainID(key) < t.pruneDomainIdx {
		return t.baseBit
	}
	return 1 - t.baseBit
}

// Root returns the last committed root, or nil for an empty trie.
func (t *Trie) Root() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == (common.Hash{}) || t.root == types.EmptyRootHash {
		return nil
	}
	return t.root.Bytes()
}

func normalizeRootHash(root []byte) common.Hash {
	if len(root) != common.HashLength || bytes.Equal(root, types.EmptyRootHash.Bytes()) {
		return common.Hash{}
	}
	return common.BytesToHash(root)
}

func prefixed(prefix, value []byte) []byte {
	key := make([]byte, len(prefix)+len(value))
	copy(key, prefix)
	copy(key[len(prefix):], value)
	return key
}

func nodeKey(hash common.Hash) []byte { return prefixed(nodePrefix, hash.Bytes()) }

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

func indexBlockKey(block int) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(block))
	return prefixed(indexPrefix, encoded[:])
}

// domainCount is the number of logical domains: 16^DomainNibbles.
func (t *Trie) domainCount() int { return 1 << uint(4*t.config.DomainNibbles) }

// domainID routes a key to its logical domain from the leading nibbles. It is
// purely a scheduling label: unlike the previous per-shard tries, no tree
// structure follows from it.
func (t *Trie) domainID(key []byte) int {
	id := 0
	for i := 0; i < t.config.DomainNibbles; i++ {
		var nib byte
		if idx := i / 2; idx < len(key) {
			if i%2 == 0 {
				nib = key[idx] >> 4
			} else {
				nib = key[idx] & 0x0f
			}
		}
		id = id<<4 | int(nib)
	}
	return id
}

// ensureHotLocked opens the working tree at the current head root. A committed
// trie is single-use, so this runs after every commit and after a prune
// that invalidates the handle. The epoch policy is (re)installed every time:
// it reads the live schedule fields, so the closure never goes stale.
func (t *Trie) ensureHotLocked() error {
	if t.hot != nil {
		return nil
	}
	if t.root == (common.Hash{}) {
		t.hot = gethtrie.NewEmpty(t.ndb)
		t.hot.SetEpochPolicy(t.epochForKeyLocked)
		return nil
	}
	hot, err := gethtrie.New(t.root, common.Hash{}, t.ndb)
	if err != nil {
		return err
	}
	hot.SetEpochPolicy(t.epochForKeyLocked)
	t.hot = hot
	return nil
}

func (t *Trie) markDirtyLocked() { t.dirty = true }

func (t *Trie) markDomainArchivedLocked(id int) {
	t.archiveIDs[id] = struct{}{}
	t.dirtyIndexBlocks[id/indexBlockDomains] = struct{}{}
}

func (t *Trie) markDomainUnarchivedLocked(id int) {
	delete(t.archiveIDs, id)
	t.dirtyIndexBlocks[id/indexBlockDomains] = struct{}{}
}

func (t *Trie) loadArchiveIndex() error {
	blocks := (t.domainCount() + indexBlockDomains - 1) / indexBlockDomains
	for block := 0; block < blocks; block++ {
		data, err := optionalGet(t.db, indexBlockKey(block))
		if err != nil {
			return err
		}
		if len(data) == 0 {
			continue
		}
		if err := decodeIndexBlock(data, block, func(id int) {
			t.archiveIDs[id] = struct{}{}
		}); err != nil {
			return err
		}
	}
	return nil
}

func decodeIndexBlock(data []byte, block int, insert func(id int)) error {
	if len(data) < 9 || !bytes.Equal(data[:4], indexMagic[:]) || data[4] != indexVersionBitmap {
		return errors.New("mpt archive: invalid archive index block")
	}
	count := int(binary.BigEndian.Uint32(data[5:9]))
	if count <= 0 || len(data) != 9+(count+7)/8 {
		return errors.New("mpt archive: invalid archive index block length")
	}
	for i := 0; i < count; i++ {
		if data[9+i/8]&(1<<uint(i%8)) != 0 {
			insert(block*indexBlockDomains + i)
		}
	}
	return nil
}

const indexVersionBitmap = byte(2)

func (t *Trie) encodeIndexBlock(block int) []byte {
	count := indexBlockDomains
	if remain := t.domainCount() - block*indexBlockDomains; remain < count {
		count = remain
	}
	data := make([]byte, 9+(count+7)/8)
	copy(data[:4], indexMagic[:])
	data[4] = indexVersionBitmap
	binary.BigEndian.PutUint32(data[5:9], uint32(count))
	base := block * indexBlockDomains
	for i := 0; i < count; i++ {
		if _, ok := t.archiveIDs[base+i]; ok {
			data[9+i/8] |= 1 << uint(i%8)
		}
	}
	return data
}

// shardForLocked returns the in-memory handle for a domain without loading
// entry data. The second return reports whether a bucket record exists (or is
// being built). Domains absent from the archive index never get a handle, so
// lookups on never-archived domains cost one map probe and no disk read.
func (t *Trie) shardForLocked(id int) (*archiveShard, bool) {
	if shard := t.archives[id]; shard != nil {
		return shard, true
	}
	if _, ok := t.archiveIDs[id]; !ok {
		return nil, false
	}
	shard := &archiveShard{}
	t.archives[id] = shard
	return shard, true
}

// makeResidentLocked registers a bucket whose entries map is in memory and
// enforces the resident-entry budget.
func (t *Trie) makeResidentLocked(shard *archiveShard) {
	if shard.elem == nil {
		shard.elem = t.archiveLRU.PushBack(shard)
		t.residentEntries += shard.count
	} else {
		t.archiveLRU.MoveToBack(shard.elem)
	}
	t.evictArchivesLocked()
}

func (t *Trie) touchResidentLocked(shard *archiveShard) {
	if shard.elem != nil {
		t.archiveLRU.MoveToBack(shard.elem)
	}
}

// evictArchivesLocked drops entry maps of clean least-recently-used buckets
// until the resident budget holds. Filters and counts stay resident. Dirty
// buckets are skipped (their in-memory entries are the only uncommitted
// copy); if everything is dirty the budget is exceeded rather than losing
// data.
func (t *Trie) evictArchivesLocked() {
	limit := t.config.ArchiveResidentEntries
	// Protect the entry-time MRU bucket by identity, not by position: skipping
	// dirty buckets rotates them to the back, so a position check against the
	// live Back() would stop protecting the bucket the caller just loaded and
	// nil its map mid-write.
	mru := t.archiveLRU.Back()
	// The scan budget is the list length at entry: evictions shrink the list,
	// so comparing against the live Len() would cut the scan short.
	budget := t.archiveLRU.Len()
	scanned := 0
	for t.residentEntries > limit && scanned < budget {
		elem := t.archiveLRU.Front()
		if elem == nil {
			return
		}
		shard := elem.Value.(*archiveShard)
		scanned++
		if shard.dirty || elem == mru {
			t.archiveLRU.MoveToBack(elem)
			continue
		}
		t.archiveLRU.Remove(elem)
		shard.elem = nil
		t.residentEntries -= shard.count
		shard.entries = nil
		if t.opTrace {
			opTrace.evictions.Add(1)
		}
	}
}

// loadArchiveLocked ensures the bucket's entries are resident, reading the
// record from disk on first use or after an eviction.
func (t *Trie) loadArchiveLocked(id int) (*archiveShard, error) {
	shard := t.archives[id]
	if shard == nil {
		if _, ok := t.archiveIDs[id]; !ok {
			// No record exists: start an empty resident bucket (the prune
			// path is the only caller that creates buckets).
			shard = &archiveShard{entries: make(map[string][]byte)}
			t.archives[id] = shard
			t.makeResidentLocked(shard)
			return shard, nil
		}
		shard = &archiveShard{}
		t.archives[id] = shard
	}
	if shard.entries != nil {
		t.touchResidentLocked(shard)
		return shard, nil
	}
	loadStart := t.opTraceStart()
	data, err := optionalGet(t.db, archiveKey(id))
	if err != nil {
		return nil, err
	}
	shard.entries = make(map[string][]byte)
	shard.count = 0
	if len(data) != 0 {
		if err := t.decodeArchive(data, shard); err != nil {
			return nil, err
		}
	}
	opTraceAdd(&opTrace.loadNanos, loadStart)
	if !loadStart.IsZero() {
		opTrace.loads.Add(1)
	}
	t.makeResidentLocked(shard)
	return shard, nil
}

// decodeArchive reads a bucket record. Entries carry the value preimage, not a
// hash: since the hot layer now stores values inline, the archive is the only
// remaining copy once a leaf is evicted, and resurrection has to recover the
// actual payload.
func (t *Trie) decodeArchive(data []byte, shard *archiveShard) error {
	if len(data) < 17 || len(data) > maxRecordSize || !bytes.Equal(data[:4], archiveMagic[:]) || data[4] != recordVersion {
		return errors.New("mpt archive: invalid archive record")
	}
	filterLen := int(binary.BigEndian.Uint32(data[5:9]))
	count := int(binary.BigEndian.Uint32(data[9:13]))
	shard.count = count
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
		valLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += 8
		if keyLen <= 0 || valLen < 0 || offset+keyLen+valLen > len(data) {
			return errors.New("mpt archive: invalid archive entry")
		}
		key := bytes.Clone(data[offset : offset+keyLen])
		offset += keyLen
		value := bytes.Clone(data[offset : offset+valLen])
		offset += valLen
		shard.entries[string(key)] = value
	}
	if offset != len(data) {
		return errors.New("mpt archive: trailing archive bytes")
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
		value := shard.entries[key]
		binary.BigEndian.PutUint32(data[offset:offset+4], uint32(len(key)))
		binary.BigEndian.PutUint32(data[offset+4:offset+8], uint32(len(value)))
		offset += 8
		copy(data[offset:], key)
		offset += len(key)
		copy(data[offset:], value)
		offset += len(value)
	}
	return data
}

func (t *Trie) rebuildFilter(shard *archiveShard) {
	if len(shard.entries) == 0 {
		shard.filter = nil
		return
	}
	filter := cuckoo.New(t.config.CuckooBuckets, t.config.CuckooSlots)
	for key := range shard.entries {
		if err := filter.Insert([]byte(key)); err != nil {
			// The exact map stays authoritative when a deliberately tiny
			// diagnostic filter fills up.
			shard.filter = nil
			return
		}
	}
	shard.filter = filter
}

func (t *Trie) removeArchivedLocked(id int, key []byte) (bool, error) {
	if _, ok := t.archiveIDs[id]; !ok {
		return false, nil
	}
	shard, err := t.loadArchiveLocked(id)
	if err != nil {
		return false, err
	}
	if _, ok := shard.entries[string(key)]; !ok {
		return false, nil
	}
	delete(shard.entries, string(key))
	shard.count--
	t.residentEntries--
	t.rebuildFilter(shard)
	shard.dirty = true
	t.touchResidentLocked(shard)
	if shard.count == 0 {
		t.markDomainUnarchivedLocked(id)
	}
	return true, nil
}

// archiveLookupLocked times the probe when the op-trace gate is open; the
// wrapper keeps the gated-off path to one field check with no defer.
func (t *Trie) archiveLookupLocked(id int, key []byte) ([]byte, bool, error) {
	if !t.opTrace {
		return t.archiveLookupInnerLocked(id, key)
	}
	opTrace.probes.Add(1)
	start := time.Now()
	value, ok, err := t.archiveLookupInnerLocked(id, key)
	opTrace.probeNanos.Add(int64(time.Since(start)))
	return value, ok, err
}

// archiveLookupInnerLocked returns the archived preimage for a key. The filter
// is a pure negative filter: a miss proves absence without loading the bucket's
// entries from disk; a hit must still be confirmed against the exact map.
func (t *Trie) archiveLookupInnerLocked(id int, key []byte) ([]byte, bool, error) {
	shard, ok := t.shardForLocked(id)
	if !ok {
		return nil, false, nil
	}
	if shard.filter != nil && !shard.filter.Lookup(key) {
		return nil, false, nil
	}
	if shard.entries == nil {
		if _, err := t.loadArchiveLocked(id); err != nil {
			return nil, false, err
		}
	}
	value, ok := shard.entries[string(key)]
	if !ok {
		return nil, false, nil
	}
	return bytes.Clone(value), true, nil
}

// Get returns a value from the hot tree or from an archived domain bucket.
func (t *Trie) Get(key []byte) ([]byte, error) {
	lockStart := t.opTraceStart()
	t.mu.Lock()
	opTraceAdd(&opTrace.lockNanos, lockStart)
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return nil, err
	}
	if t.opTrace {
		opTrace.ops.Add(1)
	}
	hotStart := t.opTraceStart()
	value, err := t.hot.Get(key)
	opTraceAdd(&opTrace.hotNanos, hotStart)
	if err != nil {
		return nil, err
	}
	if len(value) != 0 {
		return bytes.Clone(value), nil
	}
	if t.opTrace {
		opTrace.hotMisses.Add(1)
	}
	id := t.domainID(key)
	value, fromArchive, err := t.archiveLookupLocked(id, key)
	if err != nil || !fromArchive {
		return nil, ErrNotFound
	}
	if t.config.ActivateArchivedKeyOnRead {
		// Mirror the binary layer's promotion timing: only the promotion
		// itself (bucket removal + hot reinsert) is measured, not the lookup.
		promotionStart := time.Now()
		if _, err := t.removeArchivedLocked(id, key); err != nil {
			archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), false)
			return nil, err
		}
		if err := t.hot.Update(key, value); err != nil {
			archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), false)
			return nil, err
		}
		t.markDirtyLocked()
		archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), true)
	}
	return value, nil
}

// GetValueRef returns the value commitment without promoting anything, together
// with whether the value came from the archive. With inline values the
// commitment is the payload's keccak, matching how the parent driver classifies
// hot versus cold accesses.
func (t *Trie) GetValueRef(key []byte) ([]byte, bool, error) {
	lockStart := t.opTraceStart()
	t.mu.Lock()
	opTraceAdd(&opTrace.lockNanos, lockStart)
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return nil, false, err
	}
	if t.opTrace {
		opTrace.ops.Add(1)
	}
	hotStart := t.opTraceStart()
	value, err := t.hot.Get(key)
	opTraceAdd(&opTrace.hotNanos, hotStart)
	if err != nil {
		return nil, false, err
	}
	if len(value) != 0 {
		return crypto.Keccak256(value), false, nil
	}
	if t.opTrace {
		opTrace.hotMisses.Add(1)
	}
	archived, ok, err := t.archiveLookupLocked(t.domainID(key), key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, ErrNotFound
	}
	return crypto.Keccak256(archived), true, nil
}

// Put writes a value into the hot tree, releasing any archived copy first.
func (t *Trie) Put(key, value []byte) error {
	lockStart := t.opTraceStart()
	t.mu.Lock()
	opTraceAdd(&opTrace.lockNanos, lockStart)
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	if t.opTrace {
		opTrace.ops.Add(1)
	}
	removeStart := t.opTraceStart()
	if _, err := t.removeArchivedLocked(t.domainID(key), key); err != nil {
		return err
	}
	opTraceAdd(&opTrace.removeNanos, removeStart)
	if !removeStart.IsZero() {
		opTrace.removes.Add(1)
	}
	hotStart := t.opTraceStart()
	if err := t.hot.Update(key, value); err != nil {
		return err
	}
	opTraceAdd(&opTrace.hotNanos, hotStart)
	t.markDirtyLocked()
	return nil
}

func (t *Trie) PutBatch(entries []KeyValue) error {
	for _, entry := range entries {
		if err := t.Put(entry.Key, entry.Value); err != nil {
			return err
		}
	}
	return nil
}

// Delete removes a key from the hot tree and from any archived copy.
func (t *Trie) Delete(key []byte) error {
	lockStart := t.opTraceStart()
	t.mu.Lock()
	opTraceAdd(&opTrace.lockNanos, lockStart)
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	if t.opTrace {
		opTrace.ops.Add(1)
	}
	hotStart := t.opTraceStart()
	if err := t.hot.Delete(key); err != nil {
		return err
	}
	opTraceAdd(&opTrace.hotNanos, hotStart)
	removeStart := t.opTraceStart()
	if _, err := t.removeArchivedLocked(t.domainID(key), key); err != nil {
		return err
	}
	opTraceAdd(&opTrace.removeNanos, removeStart)
	if !removeStart.IsZero() {
		opTrace.removes.Add(1)
	}
	t.markDirtyLocked()
	return nil
}

func (t *Trie) DeleteBatch(keys [][]byte) error {
	for _, key := range keys {
		if err := t.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// PruneNextShard archives every hot leaf of the next round-robin domain. The
// name is kept for interface parity with the driver's hot-layer contract; the
// unit it advances over is a logical domain, not a subtree.
//
// Currently the whole domain is sealed as one bucket. Per-key expiry based on
// the epoch bitmap is specified by the paper's Algorithm 1 and is tracked as
// P4 in the plan; it requires the forked node structure to carry epoch bits.
func (t *Trie) PruneNextShard() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pruneNextDomainLocked()
}

func (t *Trie) pruneNextDomainLocked() error {
	id := t.pruneDomainIdx
	// The schedule advances only on success; cycle rollover flips the global
	// base bit, which is what makes last cycle's writes expire under the
	// epoch == baseBit eviction rule.
	defer func() {
		t.pruneDomainIdx = (id + 1) % t.domainCount()
		if t.pruneDomainIdx == 0 {
			t.baseBit ^= 1
		}
		t.scheduleDirty = true
	}()
	if err := t.ensureHotLocked(); err != nil {
		return err
	}
	// Algorithm 1 (epoch-aware pruning): extract leaves whose epoch equals the
	// current base bit. Fresh leaves (opposite bit) survive; skipped subtrees
	// are never touched (CanSkip). Bucketing still uses per-domain shards —
	// stub mounting replaces this in P5a.
	prefix := make([]byte, t.config.DomainNibbles)
	for i := range prefix {
		shift := 4 * (len(prefix) - 1 - i)
		prefix[i] = byte((id >> shift) & 0xf)
	}
	extracted, err := t.hot.ExtractDomain(prefix, t.baseBit)
	if err != nil {
		return err
	}
	if len(extracted) == 0 {
		return nil
	}
	shard, err := t.loadArchiveLocked(id)
	if err != nil {
		return err
	}
	for _, e := range extracted {
		shard.entries[string(e.Key)] = e.Value
	}
	t.residentEntries += len(shard.entries) - shard.count
	shard.count = len(shard.entries)
	t.rebuildFilter(shard)
	shard.dirty = true
	t.markDomainArchivedLocked(id)

	// The extracted leaves are gone from the hot tree; no per-key deletes
	// needed — ExtractDomain already detached and shrank the paths.
	t.markDirtyLocked()
	return nil
}

// Hash reports the tree root without writing anything.
func (t *Trie) Hash() ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureHotLocked(); err != nil {
		return nil, err
	}
	hash := t.hot.Hash()
	if hash == (common.Hash{}) || hash == types.EmptyRootHash {
		return nil, nil
	}
	return hash.Bytes(), nil
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

// stagedBytes is the per-commit split of staged volume by record class, reported
// to archive.LastCommitDiagnostics so hot-layer commit cost stays attributable
// without changing any call site's flush semantics. The flat and aggregate
// classes are gone after this rewrite and are always reported as zero.
type stagedBytes struct {
	hotNode, archive, index int64
}

func (s *stagedBytes) add(key []byte, value []byte) {
	if len(key) < 4 || key[0] != 'M' || key[1] != 'P' || key[2] != 'T' {
		return
	}
	n := int64(len(key) + len(value))
	switch key[3] {
	case 'N':
		s.hotNode += n
	case 'A':
		s.archive += n
	case 'I', 'S':
		s.index += n
	}
}

// recordingBatcher is a pass-through Batcher that tallies staged bytes.
type recordingBatcher struct {
	archivetrie.Batcher
	staged *stagedBytes
}

func (r *recordingBatcher) Put(key, value []byte) error {
	r.staged.add(key, value)
	return r.Batcher.Put(key, value)
}

func (r *recordingBatcher) Delete(key []byte) error {
	r.staged.add(key, nil)
	return r.Batcher.Delete(key)
}

// CommitToBatch stages everything dirty into the CALLER's batch and returns the
// new root. Durability is the caller's: it flushes its own batch. The trie's
// in-memory state advances as if committed, so a staged batch must never be
// discarded after a successful call.
//
// destructive is accepted for hot-layer interface parity and is a no-op.
func (t *Trie) CommitToBatch(batch archivetrie.Batcher, destructive bool) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitToBatchLocked(batch)
}

// Commit stages everything dirty into a self-managed batch and writes it. Use
// CommitToBatch when the caller already owns a shared batch.
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
		// Fold any pathdb writes made while staging into the same class the hash
		// backend charges to. Without this the path backend reports zero active
		// writes and the G2 saving looks better than it is.
		if t.pathWrites != nil {
			written, _ := t.pathWrites.take()
			staged.hotNode += written
		}
		archivetrie.SetMPTCommitStagedBytes(staged.hotNode, 0, staged.archive, 0, staged.index)
		if cacheStats, ok := t.ndb.(*nodeDatabase); ok {
			archivetrie.SetMPTNodeCacheStats(cacheStats.cache.stats())
		} else {
			archivetrie.SetMPTNodeCacheStats(0, 0)
		}
	}()
	if !t.dirty && len(t.dirtyIndexBlocks) == 0 {
		return t.rootBytes(), nil
	}
	if t.dirty {
		if err := t.ensureHotLocked(); err != nil {
			return nil, err
		}
		newRoot, nodes := t.hot.Commit()
		// A committed trie is single-use; drop it so the next switch to a
		// write path reopens from the new root.
		t.hot = nil
		if err := t.stageNodesLocked(rec, newRoot, nodes); err != nil {
			return nil, err
		}
		t.root = newRoot
		t.block++
		t.dirty = false
	}
	for id, shard := range t.archives {
		if !shard.dirty {
			continue
		}
		if shard.count == 0 {
			if err := rec.Delete(archiveKey(id)); err != nil {
				return nil, err
			}
		} else if err := rec.Put(archiveKey(id), t.encodeArchive(shard)); err != nil {
			return nil, err
		}
		shard.dirty = false
	}
	for block := range t.dirtyIndexBlocks {
		data := t.encodeIndexBlock(block)
		if emptyIndexBlock(data) {
			if err := rec.Delete(indexBlockKey(block)); err != nil {
				return nil, err
			}
		} else if err := rec.Put(indexBlockKey(block), data); err != nil {
			return nil, err
		}
	}
	t.dirtyIndexBlocks = make(map[int]struct{})
	if t.scheduleDirty {
		if err := rec.Put(scheduleKey(), t.encodeSchedule()); err != nil {
			return nil, err
		}
		t.scheduleDirty = false
	}
	// Committed buckets are clean again: release resident entries over budget.
	t.evictArchivesLocked()
	return t.rootBytes(), nil
}

func emptyIndexBlock(data []byte) bool {
	for _, b := range data[9:] {
		if b != 0 {
			return false
		}
	}
	return true
}

// stageNodesLocked routes dirty nodes to the active backend.
//
// The hash backend writes every node under its own hash and must therefore be
// handed the caller's batch. The path backend instead records them as layer-tree
// updates; pathdb owns its own buffering and flushes through the adapter when
// its write buffer fills or when a forced commit asks for it. That difference is
// the point of the backend switch: no per-node permanent record for each commit.
func (t *Trie) stageNodesLocked(rec *recordingBatcher, newRoot common.Hash, nodes *trienode.NodeSet) error {
	if t.tdb == nil {
		return writeNodeSet(rec, nodes)
	}
	if nodes == nil || len(nodes.Nodes) == 0 {
		return nil
	}
	parent := t.parentLocked()
	if newRoot == parent {
		// pathdb rejects a layer whose parent equals itself; nothing changed in
		// the tree, so there is no layer to register.
		return nil
	}
	if err := t.tdb.Update(newRoot, parent, t.block, trienode.NewWithNodeSet(nodes), triedb.NewStateSet()); err != nil {
		return err
	}
	t.commitsSinceFlush++
	if n := t.config.ForceCommitEveryBatches; n > 0 && t.commitsSinceFlush >= n {
		if err := t.tdb.Commit(newRoot, false); err != nil {
			return err
		}
		t.commitsSinceFlush = 0
	}
	return nil
}

// parentLocked is the root the next layer must attach to. pathdb bottoms out on
// the canonical empty root rather than the zero hash; see the adapter notes.
func (t *Trie) parentLocked() common.Hash {
	if t.root == (common.Hash{}) {
		return types.EmptyRootHash
	}
	return t.root
}

func (t *Trie) rootBytes() []byte {
	if t.root == (common.Hash{}) {
		return nil
	}
	return t.root.Bytes()
}

// Flush asks the path backend to flatten as far as this geth version allows.
//
// It is deliberately NOT documented as a durable flush, because it cannot be
// one: triedb.Database.Commit caps the layer tree at common.VerkleLayerCount
// (1), which stops at one remaining diff layer rather than persisting to disk.
// Actual writes land when the dirty buffer reaches WriteBufferSize, or when the
// tree caps at its diff-layer limit — both inside pathdb, neither forcible from
// the public API.
//
// So callers must not treat "Flush returned nil" as "everything is on disk".
// They should read PathStats instead, which reports both halves.
func (t *Trie) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tdb == nil || t.root == (common.Hash{}) {
		return nil
	}
	t.commitsSinceFlush = 0
	return t.tdb.Commit(t.root, false)
}

// PathStats reports write volume for the path backend: written/writes are the
// cumulative bytes and operations handed to the store since open; buffered and
// diff are what pathdb still holds in memory. A run can legitimately end with
// a large unsettled buffer, so any active-layer size claim has to add both —
// reading only the written half understates the active layer exactly when it
// matters most, which is the G2 saving measurement.
//
// All zeros for the hash backend, which stages through the caller's batch and
// is therefore already accounted for by the commit diagnostics.
func (t *Trie) PathStats() (written, writes, buffered, diff int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tdb == nil {
		return 0, 0, 0, 0
	}
	written, writes = t.pathWrites.totals()
	diffs, nodes, _ := t.tdb.Size()
	return written, writes, int64(nodes), int64(diffs)
}

// ForEach visits every hot and archived value in key order.
func (t *Trie) ForEach(fn func(key, value []byte) bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	values := make(map[string][]byte)
	if hot, err := t.hotTrieLocked(); err == nil {
		_ = hot.CollectLeaves(func(key, value []byte) bool {
			values[string(key)] = bytes.Clone(value)
			return true
		})
	}
	for id := range t.archiveIDs {
		shard, err := t.loadArchiveLocked(id)
		if err != nil {
			continue
		}
		for key, value := range shard.entries {
			values[key] = value
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

// hotTrieLocked returns a readable handle without disturbing state. It never
// commits, so the current handle stays usable afterwards.
func (t *Trie) hotTrieLocked() (*gethtrie.Trie, error) {
	if t.hot != nil {
		return t.hot, nil
	}
	if t.root == (common.Hash{}) {
		return gethtrie.NewEmpty(t.ndb), nil
	}
	return gethtrie.New(t.root, common.Hash{}, t.ndb)
}

// nodeDatabase is the hash-addressed node store used by the hash backend: a
// read-through adapter from geth's trie node accessor onto the archive KVStore,
// fronted by a size-limited content-addressed cache.
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

// Node is read-through over the shared node cache: content-addressed, so a hit
// is always valid regardless of which trie asked.
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

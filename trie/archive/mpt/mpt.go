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
	"os"
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
	"github.com/ethereum/go-ethereum/trie/archive/mpt/ecmh"
	gethtrie "github.com/ethereum/go-ethereum/trie/archive/mpt/hx"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// ecmhVerifyGate enables the audit-mode redemption check (C5): with
// MPT_ECMH_VERIFY=1 every promotion recomputes the bucket commitment from the
// full payload and compares it against the in-tree stub. Formal runs leave it
// off; the audit cost never lands in generated-path timing.
var ecmhVerifyGate = os.Getenv("MPT_ECMH_VERIFY") == "1"

var (
	ErrNotFound  = errors.New("mpt archive: key not found")
	archiveMagic = [4]byte{'M', 'A', 'R', 'C'}
)

const (
	recordVersion = byte(2)
	maxRecordSize = 1 << 30

	// defaultBucketCapacity is the paper's bucket capacity M: a mount point
	// absorbs up to M entries per bucket; larger batches force compaction
	// (fresh M-sized buckets) and the remainder pushes down by longest common
	// prefix before materializing a partial bucket.
	defaultBucketCapacity = 100

	// DefaultBucketCapacity exports M for experiment metadata reporting.
	DefaultBucketCapacity = defaultBucketCapacity
)

// Backend selectors for Config.Backend.
const (
	BackendHash = "hashdb"
	BackendPath = "pathdb"
)

var (
	nodePrefix     = []byte{'M', 'P', 'T', 'N', 1}
	archivePrefix  = []byte{'M', 'P', 'T', 'A', 1}
	schedulePrefix = []byte{'M', 'P', 'T', 'S', 1}
)

// scheduleVersion is the on-disk version of the prune-schedule record: v2 adds
// the bucket sequence counter used to mint unique bucket identifiers.
const scheduleVersion = byte(2)

// Config controls the MPT hot layer. ShardDepthBits is the paper's shard
// granularity: the state space is partitioned into 2^ShardDepthBits logical
// domains (the paper's shards), a domain being the first ShardDepthBits bits
// of the key, read MSB-first from the big-endian byte stream. Bit granularity
// matches the paper's K = 2^k exactly and tunes K in 2x steps; the previous
// nibble granularity (16^N domains) forced 16x steps.
type Config struct {
	ShardDepthBits int

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

	// BucketCapacity is the paper's M: maximum entries per archive bucket.
	// Zero uses defaultBucketCapacity (100).
	BucketCapacity int
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
		ShardDepthBits: 16, // 65,536 domains, the previous 4-nibble default
		CuckooBuckets:  32,
		CuckooSlots:    4,
		Backend:        BackendHash,
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

	// buckets is the archive registry keyed by bucket identifier (stub.Path).
	// Bucket metadata (filter/commitment/count) lives in the tree's stub
	// lists, covered by the root; only the payload records live in the KV
	// store under archivePrefix. bucketSeq mints unique identifiers and is
	// persisted in the schedule record.
	buckets           map[string]*bucket
	bucketSeq         uint64
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

// bucket is the in-memory handle for one archive bucket (paper: Stub Bucket
// B = <Path, CF, C_ECMH, Count>). The authoritative metadata is the stub
// serialized into the tree; this handle caches the decoded filter/commitment
// and, while resident, the payload entries.
type bucket struct {
	path        []byte // identifier: mount path + sequence discriminator
	mount       []byte // nibble path of the node carrying the stub
	entries     map[string][]byte
	filter      *cuckoo.Filter
	commitment  ecmh.Commitment
	count       int
	dirty       bool          // uncommitted mutation; dirty buckets are never evicted
	payloadGone bool          // count hit zero: delete the record at commit
	elem        *list.Element // non-nil exactly while entries is resident
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
		// An unset depth falls back to the default rather than failing: callers
		// legitimately pass a partially populated Config.
		if cfg.ShardDepthBits <= 0 {
			cfg.ShardDepthBits = 16
		}
	}
	// The prune schedule persists the domain index as a uint32, so the domain
	// count 2^ShardDepthBits must stay within what that record can address.
	if cfg.ShardDepthBits > 32 {
		return nil, fmt.Errorf("mpt archive: shard depth %d bits exceeds the 32-bit prune schedule record (max 32)", cfg.ShardDepthBits)
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
	if cfg.BucketCapacity <= 0 {
		cfg.BucketCapacity = defaultBucketCapacity
	}

	t := &Trie{
		db:         db,
		config:     cfg,
		root:       normalizeRootHash(root),
		buckets:    make(map[string]*bucket),
		archiveLRU: list.New(),
		opTrace:    opTraceGate.Load(),
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
	if err := t.loadSchedule(); err != nil {
		return nil, err
	}
	return t, nil
}

// scheduleKey is the singleton KV slot for the prune-schedule record.
func scheduleKey() []byte { return schedulePrefix }

// encodeSchedule serializes (version, baseBit, pruneDomainIdx, bucketSeq).
func (t *Trie) encodeSchedule() []byte {
	data := make([]byte, 14)
	data[0] = scheduleVersion
	data[1] = t.baseBit & 1
	binary.BigEndian.PutUint32(data[2:6], uint32(t.pruneDomainIdx))
	binary.BigEndian.PutUint64(data[6:14], t.bucketSeq)
	return data
}

// loadSchedule restores the prune schedule persisted by the last commit. A
// missing record means genesis: baseBit 0, domain 0. A record whose domain
// index no longer fits the configured domain count (domain depth changed
// between runs) is reset rather than trusted. The legacy 6-byte v1 record
// (pre-stub-tree archives) loads with bucketSeq 0.
func (t *Trie) loadSchedule() error {
	data, err := optionalGet(t.db, scheduleKey())
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	switch {
	case len(data) == 14 && data[0] == 2:
		t.bucketSeq = binary.BigEndian.Uint64(data[6:14])
	case len(data) == 6 && data[0] == 1:
		// v1: no bucket sequence; any v1 bucket records are orphaned by the
		// move to in-tree stubs and simply never referenced again.
	default:
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

// bucketKey maps a bucket identifier onto its payload record slot.
func bucketKey(path []byte) []byte { return prefixed(archivePrefix, path) }

// domainCount is the number of logical domains: 2^ShardDepthBits.
func (t *Trie) domainCount() int { return 1 << uint(t.config.ShardDepthBits) }

// domainID routes a key to its logical domain: the first ShardDepthBits bits
// of the key, MSB-first. It is purely a scheduling label: no tree structure
// follows from it.
func (t *Trie) domainID(key []byte) int { return domainIDOf(key, t.config.ShardDepthBits) }

// domainIDOf is domainID against an explicit depth. Bits past the end of the
// key read as zero, so short keys pad with zero bits — the same rule in epoch
// assignment and in the prune-time domain filter, which is what keeps the two
// consistent for keys shorter than the domain prefix.
func domainIDOf(key []byte, depth int) int {
	id := 0
	for i := 0; i < depth; i++ {
		var bit byte
		if idx := i / 8; idx < len(key) {
			bit = (key[idx] >> (7 - uint(i%8))) & 1
		}
		id = id<<1 | int(bit)
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

// bucketItem is the ECMH multiset element for one entry: k ‖ keccak(v).
func bucketItem(key, value []byte) []byte {
	item := make([]byte, 0, len(key)+32)
	item = append(item, key...)
	return append(item, crypto.Keccak256(value)...)
}

// nibbleLCPLen returns the length of the longest common key prefix of the
// batch, in nibbles (paper's push-down criterion).
func nibbleLCPLen(entries []gethtrie.ExtractedEntry) int {
	if len(entries) == 0 {
		return 0
	}
	lcp := len(entries[0].Key) * 2
	for _, e := range entries[1:] {
		if n := commonNibblePrefix(entries[0].Key, e.Key); n < lcp {
			lcp = n
		}
	}
	return lcp
}

func commonNibblePrefix(a, b []byte) int {
	n := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] == b[i] {
			n += 2
			continue
		}
		if a[i]>>4 == b[i]>>4 {
			n++
		}
		return n
	}
	return n
}

// keyNibblesPrefix returns the first n nibbles of a raw key.
func keyNibblesPrefix(key []byte, n int) []byte {
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			out[i] = key[i/2] >> 4
		} else {
			out[i] = key[i/2] & 0x0f
		}
	}
	return out
}

// bucketForStubLocked returns the registry handle for a stub, decoding the
// filter and commitment on first sight. The stub serialized in the tree is
// authoritative; this handle is a cache of it plus the resident payload.
func (t *Trie) bucketForStubLocked(mountPath []byte, st *gethtrie.Stub) (*bucket, error) {
	if b := t.buckets[string(st.Path)]; b != nil {
		return b, nil
	}
	filter := cuckoo.New(0, 0)
	if len(st.Filter) > 0 {
		if err := filter.Decode(st.Filter, 0, 0); err != nil {
			return nil, err
		}
	}
	b := &bucket{
		path:       bytes.Clone(st.Path),
		mount:      bytes.Clone(mountPath),
		filter:     filter,
		commitment: ecmh.Commitment(st.Commitment),
		count:      int(st.Count),
	}
	t.buckets[string(st.Path)] = b
	return b, nil
}

// stub renders the handle back into its in-tree form after a mutation.
func (b *bucket) stub() *gethtrie.Stub {
	return &gethtrie.Stub{
		Path:       bytes.Clone(b.path),
		Filter:     b.filter.Encode(),
		Commitment: [33]byte(b.commitment),
		Count:      uint32(b.count),
	}
}

// makeResidentLocked registers a bucket whose entries map is in memory and
// enforces the resident-entry budget.
func (t *Trie) makeResidentLocked(b *bucket) {
	if b.elem == nil {
		b.elem = t.archiveLRU.PushBack(b)
		t.residentEntries += b.count
	} else {
		t.archiveLRU.MoveToBack(b.elem)
	}
	t.evictArchivesLocked(true)
}

func (t *Trie) touchResidentLocked(b *bucket) {
	if b.elem != nil {
		t.archiveLRU.MoveToBack(b.elem)
	}
}

// evictArchivesLocked drops entry maps of clean least-recently-used buckets
// until the resident budget holds. Filters, commitments and counts stay
// resident (they live in the tree). Dirty buckets are skipped (their
// in-memory entries are the only uncommitted copy); if everything is dirty
// the budget is exceeded rather than losing data. protectMRU spares the
// entry-time most-recent bucket: callers that just loaded a bucket for a
// mutation must not see its map nilled mid-write. Quiescent points (end of
// commit) pass false so the budget holds exactly.
func (t *Trie) evictArchivesLocked(protectMRU bool) {
	limit := t.config.ArchiveResidentEntries
	// Protect the entry-time MRU bucket by identity, not by position: skipping
	// dirty buckets rotates them to the back, so a position check against the
	// live Back() would stop protecting the bucket the caller just loaded and
	// nil its map mid-write.
	var mru *list.Element
	if protectMRU {
		mru = t.archiveLRU.Back()
	}
	// The scan budget is the list length at entry: evictions shrink the list,
	// so comparing against the live Len() would cut the scan short.
	budget := t.archiveLRU.Len()
	scanned := 0
	for t.residentEntries > limit && scanned < budget {
		elem := t.archiveLRU.Front()
		if elem == nil {
			return
		}
		b := elem.Value.(*bucket)
		scanned++
		if b.dirty || elem == mru {
			t.archiveLRU.MoveToBack(elem)
			continue
		}
		t.archiveLRU.Remove(elem)
		b.elem = nil
		t.residentEntries -= b.count
		b.entries = nil
		if t.opTrace {
			opTrace.evictions.Add(1)
		}
	}
}

// loadBucketLocked ensures the bucket's entries are resident, reading the
// payload record from disk on first use or after an eviction. The payload
// count is cross-checked against the in-tree stub.
func (t *Trie) loadBucketLocked(b *bucket) error {
	if b.entries != nil {
		t.touchResidentLocked(b)
		return nil
	}
	loadStart := t.opTraceStart()
	data, err := optionalGet(t.db, bucketKey(b.path))
	if err != nil {
		return err
	}
	b.entries = make(map[string][]byte)
	if len(data) != 0 {
		if err := t.decodeBucket(data, b); err != nil {
			return err
		}
	}
	if len(b.entries) != b.count {
		return fmt.Errorf("mpt archive: bucket %x payload holds %d entries, stub says %d", b.path, len(b.entries), b.count)
	}
	opTraceAdd(&opTrace.loadNanos, loadStart)
	if !loadStart.IsZero() {
		opTrace.loads.Add(1)
	}
	t.makeResidentLocked(b)
	return nil
}

// decodeBucket reads a bucket payload record. Entries carry the value
// preimage, not a hash: since the hot layer stores values inline, the archive
// is the only remaining copy once a leaf is evicted, and resurrection has to
// recover the actual payload. Filter and commitment are NOT in the record —
// they live in the tree's stub (paper: the root covers the commitment).
func (t *Trie) decodeBucket(data []byte, b *bucket) error {
	if len(data) < 9 || len(data) > maxRecordSize || !bytes.Equal(data[:4], archiveMagic[:]) || data[4] != recordVersion {
		return errors.New("mpt archive: invalid bucket record")
	}
	count := int(binary.BigEndian.Uint32(data[5:9]))
	if count != b.count {
		return fmt.Errorf("mpt archive: bucket %x payload count %d, stub says %d", b.path, count, b.count)
	}
	offset := 9
	for i := 0; i < count; i++ {
		if offset+8 > len(data) {
			return errors.New("mpt archive: truncated bucket entry")
		}
		keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		valLen := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += 8
		if keyLen <= 0 || valLen < 0 || offset+keyLen+valLen > len(data) {
			return errors.New("mpt archive: invalid bucket entry")
		}
		key := bytes.Clone(data[offset : offset+keyLen])
		offset += keyLen
		value := bytes.Clone(data[offset : offset+valLen])
		offset += valLen
		b.entries[string(key)] = value
	}
	if offset != len(data) {
		return errors.New("mpt archive: trailing bucket bytes")
	}
	return nil
}

func (t *Trie) encodeBucket(b *bucket) []byte {
	keys := make([]string, 0, len(b.entries))
	for key := range b.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	size := 9
	for _, key := range keys {
		size += 8 + len(key) + len(b.entries[key])
	}
	data := make([]byte, size)
	copy(data[:4], archiveMagic[:])
	data[4] = recordVersion
	binary.BigEndian.PutUint32(data[5:9], uint32(len(keys)))
	offset := 9
	for _, key := range keys {
		value := b.entries[key]
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

// errBucketSaturated signals that a bucket's cuckoo filter cannot absorb
// another entry. Insertion failures are probabilistic well below the filter's
// hard capacity (exponentially likely above ~7/8 load), so saturation is a
// rotation signal, not corruption: the bucket keeps its absorbed prefix and a
// fresh sibling bucket takes the remainder (paper: forced compaction).
var errBucketSaturated = errors.New("mpt archive: bucket saturated")

// cuckooSoftCap bounds how many entries a bucket's filter may hold before
// top-up stops; insertion failures get exponentially likely above ~7/8 load.
func (t *Trie) cuckooSoftCap() int {
	return t.config.CuckooBuckets * t.config.CuckooSlots * 7 / 8
}

// createBucketLocked mounts a fresh bucket holding entries at nodePath
// (paper: forced compaction and partial-bucket materialization). It returns
// the number of entries the bucket absorbed; the filter may saturate before
// the batch runs out, in which case the bucket holds the absorbed prefix and
// the caller rotates a fresh sibling for the remainder. This also caps
// BucketCapacity at the filter's true slot count when M exceeds it.
func (t *Trie) createBucketLocked(nodePath []byte, entries []gethtrie.ExtractedEntry) (int, error) {
	filter := cuckoo.New(t.config.CuckooBuckets, t.config.CuckooSlots)
	items := make([][]byte, 0, len(entries))
	payload := make(map[string][]byte, len(entries))
	consumed := 0
	for _, e := range entries {
		if err := filter.Insert(e.Key); err != nil {
			if consumed == 0 {
				// An empty filter always accepts the first insert; refusing
				// one is real corruption, not saturation.
				return 0, fmt.Errorf("mpt archive: cuckoo overflow in bucket of %d entries: %w", len(entries), err)
			}
			break
		}
		payload[string(e.Key)] = bytes.Clone(e.Value)
		items = append(items, bucketItem(e.Key, e.Value))
		consumed++
	}
	if consumed < len(entries) {
		// The failed insert permuted the fresh filter's slots; rebuild it
		// from the absorbed entries so every key stays probe-visible.
		filter = rebuildFilter(payload, t.config.CuckooBuckets, t.config.CuckooSlots)
	}
	path := make([]byte, len(nodePath)+8)
	copy(path, nodePath)
	binary.BigEndian.PutUint64(path[len(nodePath):], t.bucketSeq)
	t.bucketSeq++
	t.scheduleDirty = true // bucketSeq persists in the schedule record
	b := &bucket{
		path:       path,
		entries:    payload,
		filter:     filter,
		commitment: ecmh.Create(items),
		count:      consumed,
		dirty:      true,
	}
	mp, err := t.hot.MountStub(nodePath, b.stub())
	if err != nil {
		return 0, err
	}
	b.mount = mp
	t.buckets[string(path)] = b
	t.makeResidentLocked(b)
	t.markDirtyLocked()
	return consumed, nil
}

// rebuildFilter reconstructs a cuckoo filter holding exactly the given keys.
// A failed Insert permutes occupied slots and drops the last evicted
// fingerprint (the kick loop returns ErrFull still holding it), which would
// surface as a probe false negative for a key the bucket actually holds.
// Rebuilding from the authoritative entries restores every fingerprint; map
// iteration order randomizes the kick pattern per attempt.
func rebuildFilter(entries map[string][]byte, buckets, slots int) *cuckoo.Filter {
	var filter *cuckoo.Filter
	for attempt := 0; attempt < 8; attempt++ {
		filter = cuckoo.New(buckets, slots)
		ok := true
		for key := range entries {
			if err := filter.Insert([]byte(key)); err != nil {
				ok = false
				break
			}
		}
		if ok {
			return filter
		}
	}
	// Saturation this persistent is a pathological filter geometry; keep the
	// last build rather than fail the batch.
	return filter
}

// bucketAppendLocked tops up an existing bucket (paper CanAppend): filter,
// commitment and count update O(1) per entry — BlindAppend touches no other
// bucket content. On filter saturation it keeps the absorbed prefix, reports
// the consumed count and returns errBucketSaturated so the caller rotates a
// fresh sibling bucket for the remainder instead of failing the batch.
func (t *Trie) bucketAppendLocked(b *bucket, entries []gethtrie.ExtractedEntry) (int, error) {
	if err := t.loadBucketLocked(b); err != nil {
		return 0, err
	}
	consumed := 0
	saturated := false
	for _, e := range entries {
		if err := b.filter.Insert(e.Key); err != nil {
			saturated = true
			break
		}
		b.entries[string(e.Key)] = bytes.Clone(e.Value)
		b.commitment = ecmh.BlindAppend(b.commitment, bucketItem(e.Key, e.Value))
		consumed++
	}
	if consumed == 0 {
		if saturated {
			return 0, errBucketSaturated
		}
		return 0, nil // empty batch: nothing to rewrite
	}
	b.count += consumed
	t.residentEntries += consumed
	b.dirty = true
	t.touchResidentLocked(b)
	if saturated {
		// The failed insert permuted the filter's slots; rebuild it from the
		// authoritative entries so every key stays probe-visible.
		b.filter = rebuildFilter(b.entries, t.config.CuckooBuckets, t.config.CuckooSlots)
	}
	if err := t.hot.ReplaceStub(b.mount, b.path, b.stub()); err != nil {
		return consumed, err
	}
	if saturated {
		return consumed, errBucketSaturated
	}
	return consumed, nil
}

// bucketDeleteLocked removes one entry (paper BlindDelete): the commitment
// updates by point subtraction over k‖keccak(v) alone, the filter drops the
// fingerprint, and the stub is rewritten in place. A bucket reaching Count
// zero is destroyed — the stub leaves the tree and the payload record is
// deleted at the next commit.
func (t *Trie) bucketDeleteLocked(b *bucket, key, value []byte) error {
	b.filter.Delete(key)
	b.commitment = ecmh.Delete(b.commitment, bucketItem(key, value))
	delete(b.entries, string(key))
	b.count--
	t.residentEntries--
	b.dirty = true
	t.touchResidentLocked(b)
	if b.count == 0 {
		if err := t.hot.ReplaceStub(b.mount, b.path, nil); err != nil {
			return err
		}
		b.payloadGone = true
		b.entries = nil
		if b.elem != nil {
			t.archiveLRU.Remove(b.elem)
			b.elem = nil
		}
		return nil
	}
	return t.hot.ReplaceStub(b.mount, b.path, b.stub())
}

// mountEntriesLocked hangs the extracted batch on the tree (paper "Stub
// Mounting"): existing non-full buckets at the mount point absorb first, a
// remaining batch of at least M forces fresh full buckets, the remainder
// pushes down by longest common prefix, and whatever cannot descend
// materializes as a partial bucket at the deepest reachable node — the root
// catch-all when the domain subtree has shrunk away entirely.
func (t *Trie) mountEntriesLocked(domainPrefix []byte, entries []gethtrie.ExtractedEntry) error {
	capacity := t.config.BucketCapacity
	softCap := t.cuckooSoftCap()
	nodePath := domainPrefix
	rest := entries
	for len(rest) > 0 {
		mp, err := t.hot.MountPoint(nodePath)
		if err != nil {
			return err
		}
		nodePath = mp
		stubs, err := t.hot.StubsAt(nodePath)
		if err != nil {
			return err
		}
		for _, st := range stubs {
			if len(rest) == 0 {
				break
			}
			b, err := t.bucketForStubLocked(nodePath, st)
			if err != nil {
				return err
			}
			room := min(capacity-b.count, softCap-b.count)
			if room <= 0 {
				continue
			}
			n := min(room, len(rest))
			consumed, err := t.bucketAppendLocked(b, rest[:n])
			rest = rest[consumed:]
			if err != nil && !errors.Is(err, errBucketSaturated) {
				return err
			}
			// Saturation: the bucket kept its prefix and is full for
			// rotation purposes — fall through to the next stub at this
			// mount, exactly like the room<=0 case.
		}
		if len(rest) == 0 {
			break
		}
		for len(rest) >= capacity {
			consumed, err := t.createBucketLocked(nodePath, rest[:capacity])
			if err != nil {
				return err
			}
			// createBucketLocked consumes at least one entry (an empty
			// filter always accepts the first insert), but possibly fewer
			// than capacity when the filter saturates: the next sibling
			// picks up the remainder.
			rest = rest[consumed:]
		}
		if len(rest) == 0 {
			break
		}
		// Push down by longest common prefix of the remaining keys.
		if lcp := nibbleLCPLen(rest); lcp > len(nodePath) {
			mp2, err := t.hot.MountPoint(keyNibblesPrefix(rest[0].Key, lcp))
			if err != nil {
				return err
			}
			if len(mp2) > len(nodePath) {
				nodePath = mp2
				continue
			}
		}
		consumed, err := t.createBucketLocked(nodePath, rest)
		if err != nil {
			return err
		}
		rest = rest[consumed:]
	}
	return nil
}

// archiveProbeLocked times the probe when the op-trace gate is open; the
// wrapper keeps the gated-off path to one field check with no defer.
func (t *Trie) archiveProbeLocked(key []byte) (*bucket, []byte, bool, error) {
	if !t.opTrace {
		return t.archiveProbeInnerLocked(key)
	}
	opTrace.probes.Add(1)
	start := time.Now()
	b, value, ok, err := t.archiveProbeInnerLocked(key)
	opTrace.probeNanos.Add(int64(time.Since(start)))
	return b, value, ok, err
}

// archiveProbeInnerLocked finds the bucket holding key, if any: the stub
// lists mounted along the key's path are checked sequentially (paper query
// processing), the cuckoo filter is a pure negative filter — a miss proves
// absence without touching the payload record — and a hit is confirmed
// against the exact entries.
func (t *Trie) archiveProbeInnerLocked(key []byte) (*bucket, []byte, bool, error) {
	// Collect EVERY filter-hit bucket on the route: split siblings share one
	// mount node, and a cuckoo false positive (or a genuinely missing entry)
	// in one sibling must not mask the bucket that actually holds the key.
	var hits []*bucket
	var walkErr error
	err := t.hot.StubsOnPath(key, func(mountPath []byte, st *gethtrie.Stub) bool {
		b, err := t.bucketForStubLocked(mountPath, st)
		if err != nil {
			walkErr = err
			return true
		}
		if b.filter != nil {
			if !b.filter.Lookup(key) {
				archivetrie.RecordMPTFilterLookup(false)
				return false
			}
			archivetrie.RecordMPTFilterLookup(true)
		}
		hits = append(hits, b)
		return false // keep walking: later stubs may hold the key
	})
	if err != nil {
		return nil, nil, false, err
	}
	if walkErr != nil {
		return nil, nil, false, walkErr
	}
	for _, b := range hits {
		if err := t.loadBucketLocked(b); err != nil {
			return nil, nil, false, err
		}
		if value, ok := b.entries[string(key)]; ok {
			return b, bytes.Clone(value), true, nil
		}
		// The filter claimed the key but the payload denies it: a genuine
		// cuckoo false positive — feed the G4 telemetry and keep looking.
		archivetrie.RecordMPTFilterFalsePositive()
	}
	return nil, nil, false, nil
}

// removeArchivedEntryLocked deletes key from its archive bucket (Put
// overwrites and Delete), keeping exactly one live copy of every key.
func (t *Trie) removeArchivedEntryLocked(key []byte) (bool, error) {
	b, value, ok, err := t.archiveProbeInnerLocked(key)
	if err != nil || !ok {
		return false, err
	}
	if err := t.bucketDeleteLocked(b, key, value); err != nil {
		return false, err
	}
	return true, nil
}

// verifyBucketLocked is the audit-mode redemption check (C5, gated by
// MPT_ECMH_VERIFY=1): the bucket commitment is recomputed from the full
// payload and compared against the in-tree stub. Excluded from formal-run
// timing and off by default.
func (t *Trie) verifyBucketLocked(b *bucket) error {
	if err := t.loadBucketLocked(b); err != nil {
		return err
	}
	items := make([][]byte, 0, len(b.entries))
	for key, value := range b.entries {
		items = append(items, bucketItem([]byte(key), value))
	}
	if !ecmh.Verify(b.commitment, items) {
		return fmt.Errorf("mpt archive: ECMH commitment mismatch on bucket %x", b.path)
	}
	return nil
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
	b, value, fromArchive, err := t.archiveProbeLocked(key)
	if err != nil {
		return nil, err
	}
	if !fromArchive {
		return nil, ErrNotFound
	}
	if t.config.ActivateArchivedKeyOnRead {
		// Mirror the binary layer's promotion timing: only the promotion
		// itself (bucket removal + hot reinsert) is measured, not the lookup.
		promotionStart := time.Now()
		if ecmhVerifyGate {
			if err := t.verifyBucketLocked(b); err != nil {
				archivetrie.RecordMPTReadPromotion(time.Since(promotionStart), false)
				return nil, err
			}
		}
		if err := t.bucketDeleteLocked(b, key, value); err != nil {
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
	_, archived, ok, err := t.archiveProbeLocked(key)
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
	if _, err := t.removeArchivedEntryLocked(key); err != nil {
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
	if _, err := t.removeArchivedEntryLocked(key); err != nil {
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

// PruneNextShard archives the expiring leaves of the next round-robin domain.
// The name is kept for interface parity with the driver's hot-layer contract;
// the unit it advances over is a logical domain, not a subtree.
//
// Paper Algorithm 1 in full: leaves whose epoch equals the current base bit
// are extracted (CanSkip prunes whole subtrees without visiting them), the
// batch is hung on stub lists along the tree (mountEntriesLocked), and the
// emptied paths shrink.
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
	// Extraction navigates by whole nibbles — the tree's routing unit — so the
	// prefix covers floor(D/4) nibbles. When D is not a multiple of 4 the
	// trailing r = D%4 bits inside the boundary nibble are applied as a
	// per-leaf domain filter: the boundary nibble subtree is shared by up to
	// 2^(4-r) domains, and CanSkip aggregation stays at nibble granularity
	// (coarser at the boundary layer, never incorrect).
	fullNibbles := t.config.ShardDepthBits / 4
	prefix := make([]byte, fullNibbles)
	for i := range prefix {
		shift := uint(t.config.ShardDepthBits - 4*(i+1))
		prefix[i] = byte((id >> shift) & 0xf)
	}
	extracted, err := t.hot.ExtractDomain(prefix, t.baseBit, func(key []byte) bool {
		return domainIDOf(key, t.config.ShardDepthBits) == id
	})
	if err != nil {
		return err
	}
	if len(extracted) == 0 {
		return nil
	}
	// The extracted leaves are gone from the hot tree; no per-key deletes
	// needed — ExtractDomain already detached and shrank the paths.
	return t.mountEntriesLocked(prefix, extracted)
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
	if !t.dirty && !t.scheduleDirty {
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
	for path, b := range t.buckets {
		if !b.dirty {
			continue
		}
		if b.payloadGone {
			if err := rec.Delete(bucketKey(b.path)); err != nil {
				return nil, err
			}
			delete(t.buckets, path)
			continue
		}
		if err := rec.Put(bucketKey(b.path), t.encodeBucket(b)); err != nil {
			return nil, err
		}
		b.dirty = false
	}
	if t.scheduleDirty {
		if err := rec.Put(scheduleKey(), t.encodeSchedule()); err != nil {
			return nil, err
		}
		t.scheduleDirty = false
	}
	// Committed buckets are clean again: release resident entries over budget.
	t.evictArchivesLocked(false)
	return t.rootBytes(), nil
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
		_ = hot.AllStubs(func(mountPath []byte, st *gethtrie.Stub) {
			b, err := t.bucketForStubLocked(mountPath, st)
			if err != nil {
				return
			}
			if err := t.loadBucketLocked(b); err != nil {
				return
			}
			for key, value := range b.entries {
				values[key] = value
			}
		})
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

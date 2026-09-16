// Diagnostic instrumentation for AMT archive valueRef verification failures.
//
// The B3 full replay fails deterministically at batch 164206 (~650M ops) with
// "archive bucket valueRef verification failed". The prune-absorb dedupe fix
// did not move the failure point, so the remaining producer paths for stale
// archive memberships must be separated by evidence. Everything here is
// gated behind AMT_VREF_FAIL_DUMP=1 so normal runs are untouched.
package archive

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

var (
	vRefDebugOnce  sync.Once
	vRefDebugValue bool
	// vRefDebugForced lets in-package tests enable the diagnostics without
	// relying on process env (the env gate stays authoritative in production).
	vRefDebugForced atomic.Bool
)

func vRefDebugEnabled() bool {
	if vRefDebugForced.Load() {
		return true
	}
	vRefDebugOnce.Do(func() {
		vRefDebugValue = os.Getenv("AMT_VREF_FAIL_DUMP") == "1"
	})
	return vRefDebugValue
}

// vRefPhase labels who is mutating archive memberships right now. It is
// stored per shard and read/written under s.mu, so async prune on one shard
// cannot mislabel concurrent writes on another.
func (s *Shard) setVRefPhase(phase string) {
	if s == nil || !vRefDebugEnabled() {
		return
	}
	s.vrefPhase = phase
}

func (s *Shard) currentVRefPhase() string {
	if s.vrefPhase != "" {
		return s.vrefPhase
	}
	return "other"
}

type archivedRefRecord struct {
	kind   string // append | recompute | delete
	phase  string // prune | activate | write | other
	shard  int
	key    []byte // full key bytes the membership claims
	ref    []byte // valueRef stored (empty for delete)
	bucket string // bucket path label
	seq    uint64
}

const vRefRingCap = 1 << 18 // ~262k membership-write events

// vRefWatchKey: when AMT_VREF_WATCH=<hex> names a specific key, every
// membership event for that key is journaled (uncapped, with batch context).
// The generic ring overflowed before capturing the B3 crash key's history,
// so the targeted journal closes that gap.
var (
	vRefWatchOnce sync.Once
	vRefWatchMu   sync.Mutex
	vRefWatchKey  []byte
	vRefWatchLog  []archivedRefRecord
)

func vRefWatchSetup() {
	vRefWatchOnce.Do(func() {
		hexStr := os.Getenv("AMT_VREF_WATCH")
		if len(hexStr) == 0 {
			return
		}
		key := make([]byte, hex.DecodedLen(len(hexStr)))
		if _, err := hex.Decode(key, []byte(hexStr)); err == nil && len(key) > 0 {
			vRefWatchKey = key
			fmt.Fprintf(os.Stderr, "VREF-WATCH armed for key %x\n", key)
		}
	})
}

// vRefContext carries the driver's position (batch/ops) so failure dumps can
// be correlated with the replay progress. Set via SetVRefContext.
var vRefContext atomic.Value // string

func SetVRefContext(ctx string) {
	if vRefDebugEnabled() {
		vRefContext.Store(ctx)
	}
}

func currentVRefContext() string {
	v, ok := vRefContext.Load().(string)
	if ok && v != "" {
		return v
	}
	return "unknown"
}

var (
	vRefMu   sync.Mutex
	vRefRing []archivedRefRecord
	vRefSeq  uint64
)

func vRefBucketLabel(bucket *ArchiveBucketNode) string {
	if bucket == nil {
		return "<nil>"
	}
	path := bucket.Path
	bytesLen := (bucket.PathBits + 7) / 8
	if bytesLen > len(path) {
		bytesLen = len(path)
	}
	return fmt.Sprintf("%x/%db", path[:bytesLen], bucket.PathBits)
}

func vRefRingPush(rec archivedRefRecord) {
	vRefMu.Lock()
	defer vRefMu.Unlock()
	rec.seq = vRefSeq
	vRefSeq++
	if len(vRefRing) < vRefRingCap {
		vRefRing = append(vRefRing, rec)
	} else {
		vRefRing[rec.seq%vRefRingCap] = rec
	}
}

// recordBucketMembershipWrites logs the full keys an archive-membership write
// is about to store. Caller must hold s.mu. Bytes are cloned for the ring.
func (s *Shard) recordBucketMembershipWrites(kind string, bucket *ArchiveBucketNode, items []ArchivedKV) {
	if !vRefDebugEnabled() || len(items) == 0 {
		return
	}
	vRefWatchSetup()
	phase := s.currentVRefPhase()
	label := vRefBucketLabel(bucket)
	ctx := currentVRefContext()
	for _, it := range items {
		fullKey, fullBits := s.prependPath(it.Suffix, it.SuffixBits, bucket.Path, bucket.PathBits)
		if fullBits < len(fullKey)*8 {
			fullKey = s.prefixBits(fullKey, fullBits, nil)
		}
		if vRefWatchKey != nil && bytes.Equal(fullKey, vRefWatchKey) {
			vRefWatchMu.Lock()
			vRefWatchLog = append(vRefWatchLog, archivedRefRecord{
				kind:   kind,
				phase:  phase + "|" + ctx,
				shard:  s.id,
				key:    bytes.Clone(fullKey),
				ref:    bytes.Clone(it.Value),
				bucket: label,
			})
			vRefWatchMu.Unlock()
		}
		vRefRingPush(archivedRefRecord{
			kind:   kind,
			phase:  phase,
			shard:  s.id,
			key:    bytes.Clone(fullKey),
			ref:    bytes.Clone(it.Value),
			bucket: label,
		})
	}
}

// recordBucketMembershipDeletes journals removal of watched-key memberships
// so the dump can distinguish "stale entry never removed" from "removed then
// resurrected". Caller must hold s.mu (or the bucket cacheMu as in
// blindDeleteFromBucket before recompute).
func (s *Shard) recordBucketMembershipDeletes(bucket *ArchiveBucketNode, deleted []ArchivedKV) {
	if !vRefDebugEnabled() || len(deleted) == 0 {
		return
	}
	vRefWatchSetup()
	if vRefWatchKey == nil {
		return
	}
	label := vRefBucketLabel(bucket)
	ctx := currentVRefContext()
	for _, it := range deleted {
		fullKey, fullBits := s.prependPath(it.Suffix, it.SuffixBits, bucket.Path, bucket.PathBits)
		if fullBits < len(fullKey)*8 {
			fullKey = s.prefixBits(fullKey, fullBits, nil)
		}
		if bytes.Equal(fullKey, vRefWatchKey) {
			vRefWatchMu.Lock()
			vRefWatchLog = append(vRefWatchLog, archivedRefRecord{
				kind:   "delete",
				phase:  s.currentVRefPhase() + "|" + ctx,
				shard:  s.id,
				key:    bytes.Clone(fullKey),
				ref:    bytes.Clone(it.Value),
				bucket: label,
			})
			vRefWatchMu.Unlock()
		}
	}
}

// vRefSelfHealCount tracks how many archive reads were repaired from a stale
// cross-bucket membership. The counter is always maintained; journaling the
// individual repair requires AMT_VREF_FAIL_DUMP=1.
var vRefSelfHealCount atomic.Int64

func recordVRefSelfHeal(key, staleRef, healedRef []byte) {
	vRefSelfHealCount.Add(1)
	if !vRefDebugEnabled() {
		return
	}
	vRefPrintf("VREF-HEAL key=%x dropped_stale_ref=%x verified_ref=%x total_heals=%d\n",
		key, staleRef, healedRef, vRefSelfHealCount.Load())
}

// vRefLocation is one structural hit for a key found by walking the shard.
type vRefLocation struct {
	kind   string // hot-leaf | archive-bucket | archive-bucket-unreadable | walk-error
	bucket string
	ref    []byte
	count  uint64
	index  int
}

// dumpArchiveMembershipsFor walks the shard tree (caller holds s.mu) and
// returns every location claiming the key. Descending is keyed off the full
// key bits, so the child prefix bookkeeping is exact by construction.
func (s *Shard) dumpArchiveMembershipsFor(key []byte) ([]vRefLocation, []string) {
	var notes []string
	if s.root == nil && len(s.rootHash) > 0 {
		root, err := s.loadNode(s.rootHash)
		if err != nil {
			return []vRefLocation{{kind: "walk-error", ref: []byte("load shard root: " + err.Error())}}, notes
		}
		s.root = root
	}
	if s.root == nil {
		return []vRefLocation{{kind: "empty-shard"}}, notes
	}
	prefix, prefixBits := s.getShardPrefix()
	var out []vRefLocation
	s.collectKeyLocations(s.root, prefix, prefixBits, key, &out, &notes, 0)
	return out, notes
}

const vRefWalkBudget = 1 << 20

func (s *Shard) collectKeyLocations(node Node, nodePrefix []byte, nodeBits int, key []byte, out *[]vRefLocation, notes *[]string, visited int) int {
	if node == nil || visited > vRefWalkBudget || len(*out) >= 64 {
		return visited
	}
	switch n := node.(type) {
	case *LeafNode:
		visited++
		fullKey, fullBits := s.prependPath(n.Path, n.PathBits, nodePrefix, nodeBits)
		if fullBits == len(key)*8 && bytes.Equal(s.prefixBits(fullKey, fullBits, nil), key) {
			*out = append(*out, vRefLocation{kind: "hot-leaf", ref: bytes.Clone(n.ValueHash)})
		}
	case *ArchiveBucketNode:
		visited++
		s.collectBucketLocations(n, key, out)
	case *InternalNode:
		visited++
		for _, bucket := range n.StubList {
			visited += s.collectBucketLocations(bucket, key, out)
		}
		pathPrefix, pathBits := nodePrefix, nodeBits
		if n.PathBits > 0 {
			if s.commonPrefixLen(n.Path, n.PathBits, key, nodeBits) != n.PathBits {
				return visited
			}
			pathPrefix, pathBits = s.prependPath(n.Path, n.PathBits, nodePrefix, nodeBits)
		}
		var child Node
		var childHash []byte
		bit := s.getBit(key, pathBits)
		if bit == 0 {
			child, childHash = n.Left, n.LeftHash
		} else {
			child, childHash = n.Right, n.RightHash
		}
		if child == nil && len(childHash) > 0 {
			loaded, err := s.loadNode(childHash)
			if err != nil {
				*notes = append(*notes, fmt.Sprintf("load child %x bit=%d: %v", childHash, bit, err))
				return visited
			}
			child = loaded
		}
		if child != nil {
			childPrefix, childBits := s.appendBit(pathPrefix, pathBits, bit)
			visited = s.collectKeyLocations(child, childPrefix, childBits, key, out, notes, visited)
		}
	}
	return visited
}

func (s *Shard) collectBucketLocations(bucket *ArchiveBucketNode, key []byte, out *[]vRefLocation) int {
	if bucket == nil {
		return 0
	}
	visited := 1
	keys, err := s.bucketKeys(bucket)
	if err != nil {
		*out = append(*out, vRefLocation{kind: "archive-bucket-unreadable", bucket: vRefBucketLabel(bucket), ref: []byte(err.Error()), count: bucket.Count})
		return visited
	}
	innerDepth := bucket.PathBits
	keyBits := len(key) * 8
	for index, item := range keys {
		if innerDepth+item.SuffixBits != keyBits {
			continue
		}
		if !s.suffixMatches(key, innerDepth, item.Suffix, item.SuffixBits) {
			continue
		}
		*out = append(*out, vRefLocation{
			kind:   "archive-bucket",
			bucket: vRefBucketLabel(bucket),
			ref:    bytes.Clone(item.ValueRef),
			count:  bucket.Count,
			index:  index,
		})
	}
	return visited
}

var (
	vRefSinkMu    sync.Mutex
	vRefSinkFile  *os.File
	vRefSinkReady bool
)

func vRefOpenSink() {
	if !vRefDebugEnabled() {
		return
	}
	vRefSinkMu.Lock()
	defer vRefSinkMu.Unlock()
	if vRefSinkReady {
		return
	}
	vRefSinkReady = true
	path := os.Getenv("AMT_VREF_DUMP")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "VREF-DUMP sink open %s: %v\n", path, err)
		return
	}
	vRefSinkFile = f
}

// CloseVRefSink releases the dump file handle. Tests use it so temporary
// directories can be removed on Windows.
func CloseVRefSink() {
	vRefSinkMu.Lock()
	defer vRefSinkMu.Unlock()
	if vRefSinkFile != nil {
		vRefSinkFile.Close()
		vRefSinkFile = nil
		vRefSinkReady = false
	}
}

func vRefPrintf(format string, args ...interface{}) {
	vRefSinkMu.Lock()
	defer vRefSinkMu.Unlock()
	if vRefSinkFile != nil {
		fmt.Fprintf(vRefSinkFile, format, args...)
	} else {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

// dumpVRefFailure prints the crime scene: the failing key, every location
// that claims it, and the recent membership-write history for that key.
// Caller must hold s.mu (same lock the failing read already holds).
func dumpVRefFailure(s *Shard, key, storedRef, actualRef []byte, site string) {
	if !vRefDebugEnabled() {
		return
	}
	vRefOpenSink()
	vRefPrintf("VREF-DUMP site=%s shard=%d context=%s key=%x stored_ref=%x actual_ref=%x\n",
		site, s.id, currentVRefContext(), key, storedRef, actualRef)

	locs, notes := s.dumpArchiveMembershipsFor(key)
	for _, loc := range locs {
		switch loc.kind {
		case "archive-bucket":
			verdict := "ORPHAN"
			if len(actualRef) > 0 && bytes.Equal(loc.ref, actualRef) {
				verdict = "MATCHES-CURRENT"
			}
			vRefPrintf("VREF-DUMP   location=archive-bucket bucket=%s key_index=%d bucket_count=%d ref=%x verdict=%s\n",
				loc.bucket, loc.index, loc.count, loc.ref, verdict)
		case "hot-leaf":
			verdict := "differs"
			if bytes.Equal(loc.ref, storedRef) {
				verdict = "same-as-stored"
			}
			if len(actualRef) > 0 && bytes.Equal(loc.ref, actualRef) {
				verdict = "same-as-current"
			}
			vRefPrintf("VREF-DUMP   location=hot-leaf ref=%x verdict=%s\n", loc.ref, verdict)
		default:
			vRefPrintf("VREF-DUMP   location=%s bucket=%s detail=%x\n", loc.kind, loc.bucket, loc.ref)
		}
	}
	for _, note := range notes {
		vRefPrintf("VREF-DUMP   walk-note %s\n", note)
	}

	vRefMu.Lock()
	matched := 0
	var recent []archivedRefRecord
	total := vRefSeq
	for i := len(vRefRing) - 1; i >= 0; i-- {
		rec := vRefRing[i]
		if len(rec.key) == 0 || !bytes.Equal(rec.key, key) {
			continue
		}
		matched++
		if len(recent) < 64 {
			recent = append(recent, rec)
		}
	}
	captured := len(vRefRing)
	vRefMu.Unlock()

	vRefPrintf("VREF-DUMP ring_records=%d ring_matched_for_key=%d ring_total_seq=%d (ring caps at %d and overwrites oldest)\n",
		captured, matched, total, vRefRingCap)
	if bytes.Equal(key, vRefWatchKey) {
		vRefWatchMu.Lock()
		journal := append([]archivedRefRecord(nil), vRefWatchLog...)
		vRefWatchMu.Unlock()
		vRefPrintf("VREF-DUMP watch_journal_events=%d (full lifecycle of watched key, oldest first)\n", len(journal))
		for i, rec := range journal {
			vRefPrintf("VREF-DUMP   watch[%d] kind=%s phase=%s shard=%d bucket=%s ref=%x\n",
				i, rec.kind, rec.phase, rec.shard, rec.bucket, rec.ref)
		}
	}
	for _, rec := range recent {
		alive := "not-in-tree"
		for _, loc := range locs {
			if loc.kind == "archive-bucket" && loc.bucket == rec.bucket {
				if bytes.Equal(loc.ref, rec.ref) {
					alive = "present"
				} else {
					alive = "different-ref"
				}
			}
		}
		vRefPrintf("VREF-DUMP   keyRecord seq=%d kind=%s phase=%s shard=%d bucket=%s ref=%x bucketStill=%s\n",
			rec.seq, rec.kind, rec.phase, rec.shard, rec.bucket, rec.ref, alive)
	}
}

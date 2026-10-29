package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/shirou/gopsutil/process"
)

var (
	traceStressInputDir                   = flag.String("traceStressInputDir", "", "Directory containing state_access_trace_*.csv.gz files")
	traceStressBaseDir                    = flag.String("traceStressBaseDir", "", "Fresh output directory for TestArchiveStemTraceStress")
	traceStressOps                        = flag.Int64("traceStressOps", 100000000, "Maximum ordered trace operations; zero is unlimited when a block boundary is set")
	traceStressBatchSize                  = flag.Int("traceStressBatchSize", 4000, "Trace operations per prune/commit cycle")
	traceStressBatchByBlock               = flag.Bool("traceStressBatchByBlock", false, "Commit once per source block, including blocks without trace records")
	traceStressMetricsBatches             = flag.Int("traceStressMetricsBatches", 250, "Commit batches per metrics row")
	traceStressStartFile                  = flag.Int("traceStressStartFile", 0, "Zero-based sorted trace shard index to start from")
	traceStressFileLimit                  = flag.Int("traceStressFileLimit", 0, "Maximum trace shards to read; zero uses all remaining shards")
	traceStressStartBlock                 = flag.Uint64("traceStressStartBlock", 0, "First inclusive trace block; zero starts at the first selected record")
	traceStressEndBlock                   = flag.Uint64("traceStressEndBlock", 0, "Last inclusive trace block; zero disables an explicit end boundary")
	traceStressBlocks                     = flag.Uint64("traceStressBlocks", 0, "Replay this many blocks from the first selected block; incompatible with traceStressEndBlock")
	traceStressShardDepth                 = flag.Int("traceStressShardDepth", 20, "ASCT shard depth")
	traceStressStemMode                   = flag.Bool("traceStressStemMode", false, "Route 32-byte trace keys through StemTrie; false uses the key-level AMT")
	traceStressStemCacheLimit             = flag.Int("traceStressStemCacheLimit", DefaultStemCacheLimit, "Decoded stem cache entry limit; negative disables")
	traceStressStemCacheMB                = flag.Int("traceStressStemCacheMB", 128, "Decoded stem cache byte limit in MiB; negative disables")
	traceStressNodeCacheLimit             = flag.Int("traceStressNodeCacheLimit", DefaultNodeCacheLimit, "Serialized node cache entry limit; negative disables")
	traceStressNodeCacheMB                = flag.Int("traceStressNodeCacheMB", 512, "Serialized node cache byte limit in MiB; negative disables")
	traceStressCommitWorkers              = flag.Int("traceStressCommitWorkers", 16, "Parallel shard commit workers")
	traceStressAsyncPrune                 = flag.Bool("traceStressAsyncPrune", true, "Overlap one shard prune with each operation batch")
	traceStressPruneEveryBatches          = flag.Int("traceStressPruneEveryBatches", 1, "Advance prune after this many batches; zero disables pruning")
	traceStressPruneEveryBlocks           = flag.Uint64("traceStressPruneEveryBlocks", 0, "Advance one prune shard per this many trace blocks; incompatible with traceStressPruneEveryBatches")
	traceStressDestructiveCommit          = flag.Bool("traceStressDestructiveCommit", true, "Unload committed shard nodes")
	traceStressMPTIntermediateRoot        = flag.Bool("traceStressMPTIntermediateRoot", true, "AMT pipeline (mpt hot layer): commit the intermediate root right after pruning so prune dirt flushes in the archive-maintenance phase instead of bloating the final root's batch writes; prune+intermediate time is excluded from the counted operations+root time")
	traceStressFinalStats                 = flag.Bool("traceStressFinalStats", true, "Run an exact structural scan after the workload")
	traceStressAccessSampleEvery          = flag.Int64("traceStressAccessSampleEvery", 1000, "Sample logical hot/archive access state every N operations; zero disables")
	traceStressActivateArchivedStemOnRead = flag.Bool("traceStressActivateArchivedStemOnRead", false, "Activate an archived stem when a read finds it")
	traceStressActivateArchivedKeyOnRead  = flag.Bool("traceStressActivateArchivedKeyOnRead", false, "Activate an archived AMT key when a read finds it")
	traceStressDisableArchive             = flag.Bool("traceStressDisableArchive", false, "Do not call PruneNextShard during the trace run")
	traceStressHotLayer                   = flag.String("traceStressHotLayer", "mpt", `Hot-layer backend: "amt" (binary key-level trie) or "mpt" (hexary-MPT layer)`)
	traceStressShardDepthBits             = flag.Int("traceStressShardDepthBits", 16, "Logical archive domain width in bits for the MPT hot layer (domains = 2^this)")
	traceStressArchiveResidentEntries     = flag.Int("traceStressArchiveResidentEntries", 0, "MPT hot layer: cap on memory-resident archived entries across all buckets (0 = engine default)")
	traceStressTrieBackend                = flag.String("traceStressTrieBackend", "hashdb", `MPT hot-layer node store: "hashdb" (content-addressed) or "pathdb" (triedb.PathDatabase, path-addressed)`)
	traceStressPathCleanCacheMB           = flag.Int("traceStressPathCleanCacheMB", 64, "pathdb clean cache size in MiB")
	traceStressPathWriteBufferMB          = flag.Int("traceStressPathWriteBufferMB", 256, "pathdb dirty write buffer size in MiB (larger = fewer, fatter flushes)")
	traceStressPathFlushEveryBatches      = flag.Int("traceStressPathFlushEveryBatches", 0, "Force a pathdb flush to disk every N commits; 0 lets pathdb decide")
	traceStressCompactPerWindow           = flag.Bool("traceStressCompactPerWindow", true, "Run a full LevelDB compaction at each metrics-window boundary, BEFORE measuring State_Bytes, so storage reflects live data only (dead bytes reclaimed); compaction time lands in its own column, never in business timing")
	traceStressSplitArchiveDB             = flag.Bool("traceStressSplitArchiveDB", false, "Store MPTA archive payloads in archive_db and all other records in state_db; report both physical directory sizes per window")
	traceStressCuckooBuckets              = flag.Int("traceStressCuckooBuckets", 32, "Archive cuckoo filter bucket count")
	traceStressCuckooSlots                = flag.Int("traceStressCuckooSlots", 4, "Archive cuckoo filter slots per bucket")
	traceStressMPTVerifyAtCommit          = flag.Bool("traceStressMPTVerifyAtCommit", false, "MPT: audit every distinct redeemed bucket before the block's final commit")
	traceStressMPTVerifyBucketWorkers     = flag.Int("traceStressMPTVerifyBucketWorkers", 8, "MPT: concurrent bucket audit workers")
	traceStressMPTVerifyItemWorkers       = flag.Int("traceStressMPTVerifyItemWorkers", 2, "MPT: concurrent item workers per bucket")
	traceStatsBaseDir                     = flag.String("traceStatsBaseDir", "", "Existing TestArchiveStemTraceStress base directory for TestArchiveStemTraceStorageStats")
	traceStatsRoot                        = flag.String("traceStatsRoot", "", "Final trie root for TestArchiveStemTraceStorageStats")
	traceStatsOutput                      = flag.String("traceStatsOutput", "", "Output JSON path for TestArchiveStemTraceStorageStats")
	traceInspectBaseDir                   = flag.String("traceInspectBaseDir", "", "Existing path-storage trace run to inspect read-only")
	traceInspectStem                      = flag.String("traceInspectStem", "", "31-byte hex stem to inspect in an existing trace run")
	traceInspectOutput                    = flag.String("traceInspectOutput", "", "Optional JSON output path for stem inspection")
)

const traceStressHeader = "block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len"

type traceStressKind byte

const (
	traceStressGet traceStressKind = iota
	traceStressPut
	traceStressDelete
)

type traceStressOp struct {
	key       []byte
	value     []byte
	kind      traceStressKind
	operation string
	block     uint64
}

type traceStressReader struct {
	files     []string
	fileIndex int
	file      *os.File
	gz        *gzip.Reader
	csv       *csv.Reader
	current   string
}

func newTraceStressReader(inputDir string, startFile, fileLimit int) (*traceStressReader, error) {
	files, err := filepath.Glob(filepath.Join(inputDir, "state_access_trace_*.csv.gz"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if startFile < 0 || startFile >= len(files) {
		return nil, fmt.Errorf("traceStressStartFile=%d outside %d trace shards", startFile, len(files))
	}
	files = files[startFile:]
	if fileLimit > 0 && fileLimit < len(files) {
		files = files[:fileLimit]
	}
	return &traceStressReader{files: files}, nil
}

func (r *traceStressReader) closeCurrent() error {
	var errs []error
	if r.gz != nil {
		errs = append(errs, r.gz.Close())
	}
	if r.file != nil {
		errs = append(errs, r.file.Close())
	}
	r.file, r.gz, r.csv = nil, nil, nil
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *traceStressReader) Close() error {
	return r.closeCurrent()
}

func (r *traceStressReader) openNext() error {
	if err := r.closeCurrent(); err != nil {
		return err
	}
	if r.fileIndex >= len(r.files) {
		return io.EOF
	}
	path := r.files[r.fileIndex]
	r.fileIndex++
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	gz, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		f.Close()
		return err
	}
	reader := csv.NewReader(bufio.NewReaderSize(gz, 1<<20))
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err != nil {
		gz.Close()
		f.Close()
		return err
	}
	if strings.Join(header, ",") != traceStressHeader {
		gz.Close()
		f.Close()
		return fmt.Errorf("unexpected trace header in %s: %q", path, strings.Join(header, ","))
	}
	r.file, r.gz, r.csv, r.current = f, gz, reader, filepath.Base(path)
	return nil
}

func (r *traceStressReader) Read() (traceStressOp, error) {
	for {
		if r.csv == nil {
			if err := r.openNext(); err != nil {
				return traceStressOp{}, err
			}
		}
		record, err := r.csv.Read()
		if errors.Is(err, io.EOF) {
			if err := r.closeCurrent(); err != nil {
				return traceStressOp{}, err
			}
			continue
		}
		if err != nil {
			return traceStressOp{}, fmt.Errorf("read %s: %w", r.current, err)
		}
		op, err := parseTraceStressRecord(record)
		if err != nil {
			line, _ := r.csv.FieldPos(0)
			return traceStressOp{}, fmt.Errorf("parse %s line %d: %w", r.current, line, err)
		}
		return op, nil
	}
}

func decodeTraceStressHash(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	if len(input) != common.HashLength*2 {
		return nil, fmt.Errorf("want 32-byte hash, got %d hex chars", len(input))
	}
	decoded, err := hex.DecodeString(input)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

var traceStressZeroStorageHash = crypto.Keccak256Hash(make([]byte, common.HashLength)).Hex()

func parseTraceStressRecord(record []string) (traceStressOp, error) {
	if len(record) != 9 {
		return traceStressOp{}, fmt.Errorf("want 9 fields, got %d", len(record))
	}
	block, err := strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return traceStressOp{}, fmt.Errorf("block: %w", err)
	}
	if !common.IsHexAddress(record[4]) {
		return traceStressOp{}, fmt.Errorf("invalid address %q", record[4])
	}
	address := common.HexToAddress(record[4])
	var key []byte
	switch record[3] {
	case "account":
		key = trieutils.BinaryTreeBasicDataKey(address)
	case "storage":
		slot, err := decodeTraceStressHash(record[5])
		if err != nil {
			return traceStressOp{}, fmt.Errorf("storage slot: %w", err)
		}
		key = trieutils.BinaryTreeStorageSlotKey(address, slot)
	case "code":
		chunk, err := strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return traceStressOp{}, fmt.Errorf("code chunk: %w", err)
		}
		key = trieutils.BinaryTreeCodeChunkKey(address, chunk)
	default:
		return traceStressOp{}, fmt.Errorf("unsupported object type %q", record[3])
	}
	op := traceStressOp{key: key, operation: record[6], block: block}
	switch record[6] {
	case "read", "touch":
		op.kind = traceStressGet
	case "delete":
		op.kind = traceStressDelete
	case "write", "create":
		if record[3] == "storage" && strings.EqualFold(record[7], traceStressZeroStorageHash) {
			op.kind = traceStressDelete
			break
		}
		op.kind = traceStressPut
		if record[7] != "" {
			op.value, err = decodeTraceStressHash(record[7])
			if err != nil {
				return traceStressOp{}, fmt.Errorf("value hash: %w", err)
			}
		} else {
			op.value = crypto.Keccak256(key, []byte(record[6]))
		}
	default:
		return traceStressOp{}, fmt.Errorf("unsupported operation %q", record[6])
	}
	return op, nil
}

type traceStressCounts struct {
	Reads   int64 `json:"reads"`
	Touches int64 `json:"touches"`
	Writes  int64 `json:"writes"`
	Creates int64 `json:"creates"`
	Deletes int64 `json:"deletes"`
}

func (c *traceStressCounts) add(operation string) {
	switch operation {
	case "read":
		c.Reads++
	case "touch":
		c.Touches++
	case "write":
		c.Writes++
	case "create":
		c.Creates++
	case "delete":
		c.Deletes++
	}
}

func (c traceStressCounts) total() int64 {
	return c.Reads + c.Touches + c.Writes + c.Creates + c.Deletes
}

type traceStressWindow struct {
	counts                        traceStressCounts
	gets, puts, deletes           int64
	parse, operations             time.Duration
	prune, commit, write          time.Duration
	intermediate                  time.Duration // intermediate-root commit (archive maintenance)
	intermediateWrite             time.Duration // intermediate batch write (archive maintenance)
	finalCommitPhase              [4]int64      // hx/stage/tdbCommit/archiveLoop nanos of the FINAL commit only
	stagedPruneBytes              int64         // staged hot-node bytes of the intermediate commit
	stagedOpsBytes                int64         // staged hot-node bytes of the final commit
	pruneDiag                     [4]int64      // extracted/resolved/extractNanos/mountNanos, window diff
	coldMaintenance               time.Duration
	batchWall                     []time.Duration
	pruneWall                     []time.Duration // per-batch Sharded-Pruning durations; batches that pruned only
	comparativeBatchWall          []time.Duration
	firstBlock, lastBlock         uint64
	stemHits, stemMisses          int64
	stemEvictions                 int64
	rawBatchOps, rawBatchBytes    int64
	mptDiag                       [10]int64 // staged hotNode/aggregate/archive/flat/index/stubMeta bytes, node-cache gets/hits, stubmeta fetches/fetch-bytes
	access                        traceStressAccessStats
	accessSample                  time.Duration
	classificationFilter          traceStressFilterStats
	proofRedemptions              int64         // read-activation redemptions in this window
	proofBytes                    int64         // total redemption-proof bytes in this window
	proofBlocks                   []int64       // completed per-block proof totals in this window
	proofCounts                   []int64       // completed per-block resurrection counts in this window
	proofVerifyCount              int64         // sampled full-node verification audits in this window
	proofVerify                   time.Duration // sampled audit latency in this window
	blockRedemptionsAll           []int64       // exact: every source block, including zeroes
	blockUniqueProofBytesAll      []int64       // one proof per distinct redeemed bucket per block
	blockRedemptionsPositive      []int64       // exact: blocks with at least one redemption
	blockUniqueProofBytesPositive []int64
	blockUniqueBucketsTotal       int64
	proofSizes                    []int64         // exact single-proof size histogram drained at window close (E2 v6)
	proofLatencies                []time.Duration // exact single-proof promotion latency histogram (E2 v6)
	filterProbeLatencies          []time.Duration // exact per-cuckoo-lookup latency histogram (E2 v6)
	bucketOccupancy               []int64         // archive-bucket entry-count snapshot taken at window close (E2 v6)
	bucketEmpty                   int64           // archive buckets with zero entries at window close (E2 v6)
}

type traceStressFilterStats struct {
	Lookups        int64
	Negatives      int64
	Positives      int64
	FalsePositives int64
}

// proofReservoirAdd keeps a uniform random sample of at most cap int64
// values (stream position seen is 1-based), so run-long percentile estimates
// need constant memory.
func proofReservoirAdd(res *[]int64, v, seen int64, cap int, rng *rand.Rand) {
	if len(*res) < cap {
		*res = append(*res, v)
		return
	}
	if j := rng.Int63n(seen); j < int64(cap) {
		(*res)[j] = v
	}
}

// percentileInt64 reads q-quantile off an ascending-sorted slice (linear
// index, matching percentileDuration's convention); empty yields 0.
func percentileInt64(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[min(int(q*float64(len(sorted))), len(sorted)-1)]
}

// sortedInt64Copy returns an ascending-sorted copy of values.
func sortedInt64Copy(values []int64) []int64 {
	out := append([]int64(nil), values...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func meanInt64(values []int64) float64 {
	return traceStressRatio(sumInt64(values), int64(len(values)))
}

func sumInt64(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}
	return total
}

// blockDistributionColumns reports exact window statistics. Each source
// block contributes once to the all-block population, including empty ones.
func blockDistributionColumns(allCounts, allBytes, positiveCounts, positiveBytes []int64, uniqueBuckets int64) []string {
	ac := sortedInt64Copy(allCounts)
	ab := sortedInt64Copy(allBytes)
	pc := sortedInt64Copy(positiveCounts)
	pb := sortedInt64Copy(positiveBytes)
	integer := func(v int64) string { return strconv.FormatInt(v, 10) }
	mean := func(v []int64) string { return strconv.FormatFloat(meanInt64(v), 'f', 6, 64) }
	return []string{
		integer(int64(len(ac))), mean(allCounts), integer(percentileInt64(ac, .50)), integer(percentileInt64(ac, .95)), integer(percentileInt64(ac, .99)),
		mean(allBytes), integer(percentileInt64(ab, .50)), integer(percentileInt64(ab, .95)), integer(percentileInt64(ab, .99)),
		integer(int64(len(pc))), mean(positiveCounts), integer(percentileInt64(pc, .50)), integer(percentileInt64(pc, .95)), integer(percentileInt64(pc, .99)),
		mean(positiveBytes), integer(percentileInt64(pb, .50)), integer(percentileInt64(pb, .95)), integer(percentileInt64(pb, .99)),
		integer(uniqueBuckets), integer(sumInt64(allBytes)),
	}
}

func blockDistributionSummary(counts, proofBytes []int64) map[string]any {
	c := sortedInt64Copy(counts)
	b := sortedInt64Copy(proofBytes)
	return map[string]any{
		"blocks":                   len(counts),
		"redemptions_total":        sumInt64(counts),
		"redemptions_mean":         meanInt64(counts),
		"redemptions_p50":          percentileInt64(c, .50),
		"redemptions_p95":          percentileInt64(c, .95),
		"redemptions_p99":          percentileInt64(c, .99),
		"unique_proof_bytes_total": sumInt64(proofBytes),
		"unique_proof_bytes_mean":  meanInt64(proofBytes),
		"unique_proof_bytes_p50":   percentileInt64(b, .50),
		"unique_proof_bytes_p95":   percentileInt64(b, .95),
		"unique_proof_bytes_p99":   percentileInt64(b, .99),
	}
}

// bucketCapacityMetadata reports the mpt bucket capacity M registered by the
// hot-layer hook package; nil when the hook is absent (amt runs).
func bucketCapacityMetadata() any {
	if TraceMPTBucketCapacity > 0 {
		return TraceMPTBucketCapacity
	}
	return nil
}

func filterStatsFromDiagnostics(d UpdateDiagnostics) traceStressFilterStats {
	return traceStressFilterStats{
		Lookups:        d.ArchiveFilterLookups,
		Negatives:      d.ArchiveFilterNegatives,
		Positives:      d.ArchiveFilterPositives,
		FalsePositives: d.ArchiveFilterFalsePositives,
	}
}

func (s *traceStressFilterStats) add(other traceStressFilterStats) {
	s.Lookups += other.Lookups
	s.Negatives += other.Negatives
	s.Positives += other.Positives
	s.FalsePositives += other.FalsePositives
}

func (s traceStressFilterStats) sub(other traceStressFilterStats) traceStressFilterStats {
	return traceStressFilterStats{
		Lookups:        s.Lookups - other.Lookups,
		Negatives:      s.Negatives - other.Negatives,
		Positives:      s.Positives - other.Positives,
		FalsePositives: s.FalsePositives - other.FalsePositives,
	}
}

func traceStressRatio(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

type traceStressAccessStats struct {
	Samples             int64 `json:"samples"`
	ReadHot             int64 `json:"read_hot"`
	ReadHotNanos        int64 `json:"read_hot_nanos"`
	ReadArchived        int64 `json:"read_archived"`
	ReadArchivedNanos   int64 `json:"read_archived_nanos"`
	ReadMissing         int64 `json:"read_missing"`
	ReadMissingNanos    int64 `json:"read_missing_nanos"`
	WriteHot            int64 `json:"write_hot"`
	WriteHotNanos       int64 `json:"write_hot_nanos"`
	WriteArchived       int64 `json:"write_archived"`
	WriteArchivedNanos  int64 `json:"write_archived_nanos"`
	WriteMissing        int64 `json:"write_missing"`
	WriteMissingNanos   int64 `json:"write_missing_nanos"`
	DeleteHot           int64 `json:"delete_hot"`
	DeleteHotNanos      int64 `json:"delete_hot_nanos"`
	DeleteArchived      int64 `json:"delete_archived"`
	DeleteArchivedNanos int64 `json:"delete_archived_nanos"`
	DeleteMissing       int64 `json:"delete_missing"`
	DeleteMissingNanos  int64 `json:"delete_missing_nanos"`
	Errors              int64 `json:"errors"`
}

func (s *traceStressAccessStats) add(kind traceStressKind, fromArchive bool, sampleErr error, elapsed time.Duration) {
	s.Samples++
	if sampleErr != nil && !errors.Is(sampleErr, ErrNodeNotFound) {
		s.Errors++
		return
	}
	if errors.Is(sampleErr, ErrNodeNotFound) {
		switch kind {
		case traceStressGet:
			s.ReadMissing++
			s.ReadMissingNanos += elapsed.Nanoseconds()
		case traceStressPut:
			s.WriteMissing++
			s.WriteMissingNanos += elapsed.Nanoseconds()
		case traceStressDelete:
			s.DeleteMissing++
			s.DeleteMissingNanos += elapsed.Nanoseconds()
		}
		return
	}
	switch {
	case kind == traceStressGet && fromArchive:
		s.ReadArchived++
		s.ReadArchivedNanos += elapsed.Nanoseconds()
	case kind == traceStressGet:
		s.ReadHot++
		s.ReadHotNanos += elapsed.Nanoseconds()
	case kind == traceStressPut && fromArchive:
		s.WriteArchived++
		s.WriteArchivedNanos += elapsed.Nanoseconds()
	case kind == traceStressPut:
		s.WriteHot++
		s.WriteHotNanos += elapsed.Nanoseconds()
	case kind == traceStressDelete && fromArchive:
		s.DeleteArchived++
		s.DeleteArchivedNanos += elapsed.Nanoseconds()
	case kind == traceStressDelete:
		s.DeleteHot++
		s.DeleteHotNanos += elapsed.Nanoseconds()
	}
}

func classifyTraceStressAccess(hot TraceHotTrie, key []byte) (bool, error) {
	backend, ok := hot.(*Trie)
	if !ok {
		return false, errors.New("stem-mode access classification requires the AMT hot layer")
	}
	if len(key) != StemKeySize {
		return false, ErrInvalidStemKey
	}
	stemKey := key[:StemSize]
	_, fromArchive, err := backend.GetValueRef(stemKey)
	if err != nil {
		return false, err
	}

	// The split bitmap is authoritative even when PhysicalDelete=false leaves
	// an obsolete suffix payload behind. Legacy single-blob stems use the
	// complete-key payload as their membership signal.
	metadata, err := backend.GetFlatValue(stemKey)
	if err != nil {
		return fromArchive, err
	}
	if len(metadata) >= len(stemMetadataMagic) && bytes.Equal(metadata[:len(stemMetadataMagic)], stemMetadataMagic[:]) {
		stem, err := decodeStemMetadata(metadata)
		if err != nil {
			return fromArchive, err
		}
		if !stem.has(key[StemSize]) {
			return fromArchive, ErrNodeNotFound
		}
		return fromArchive, nil
	}
	if _, err := backend.GetFlatValue(key); err != nil {
		return fromArchive, err
	}
	return fromArchive, nil
}

// classifyTraceStressAccessAMT classifies a complete key without reading its
// flat payload. GetValueRef reports whether the reference came from the hot
// leaf or an archive bucket, which is the only distinction AMT needs.
func classifyTraceStressAccessAMT(hot TraceHotTrie, key []byte) (bool, error) {
	if len(key) != common.HashLength {
		return false, ErrInvalidStemKey
	}
	_, fromArchive, err := hot.GetValueRef(key)
	return fromArchive, err
}

// TraceHotTrie is the hot-layer surface TestArchiveStemTraceStress drives.
// *Trie (binary AMT hot layer) implements it directly; the hexary-MPT layer is
// wired through TraceHotTrieNew below.
//
// Flush is deliberately NOT part of this interface. Only the path-backed layer
// needs one, and adding it here would force every existing implementation to
// grow a method that does nothing; the driver probes for it with a type
// assertion instead.
type TraceHotTrie interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	GetValueRef(key []byte) ([]byte, bool, error)
	PruneNextShard() error
	CommitToBatch(batch Batcher, destructive bool) ([]byte, error)
}

// TraceHotTrieSpec carries every hot-layer construction option. It is a struct
// rather than a positional argument list because the set keeps growing with each
// backend experiment, and a positional signature would churn on every addition.
type TraceHotTrieSpec struct {
	Kind                      string
	DB                        KVStore
	ShardDepthBits            int
	CuckooBuckets             int
	CuckooSlots               int
	ActivateArchivedKeyOnRead bool
	Backend                   string
	CleanCacheBytes           int
	WriteBufferBytes          int
	FlushEveryBatches         int
	ArchiveResidentEntries    int
	AsyncPrune                bool
	VerifyAtCommit            bool
	VerifyBucketWorkers       int
	VerifyItemWorkers         int
}

// TraceHotTrieNew constructs the alternative hot layer named by
// -traceStressHotLayer. It is nil inside this package to avoid an import
// cycle (trie/archive/mpt imports trie/archive); the external test package
// trie/archive_test registers the MPT fallback in its init. A nil hook for a
// requested backend must fail the run loudly, never fall back silently.
// Adapters translate "key absent" into ErrNodeNotFound, the sentinel the
// workload treats as a miss.
var TraceHotTrieNew func(spec TraceHotTrieSpec) (TraceHotTrie, error)

func (s *traceStressAccessStats) merge(other traceStressAccessStats) {
	s.Samples += other.Samples
	s.ReadHot += other.ReadHot
	s.ReadHotNanos += other.ReadHotNanos
	s.ReadArchived += other.ReadArchived
	s.ReadArchivedNanos += other.ReadArchivedNanos
	s.ReadMissing += other.ReadMissing
	s.ReadMissingNanos += other.ReadMissingNanos
	s.WriteHot += other.WriteHot
	s.WriteHotNanos += other.WriteHotNanos
	s.WriteArchived += other.WriteArchived
	s.WriteArchivedNanos += other.WriteArchivedNanos
	s.WriteMissing += other.WriteMissing
	s.WriteMissingNanos += other.WriteMissingNanos
	s.DeleteHot += other.DeleteHot
	s.DeleteHotNanos += other.DeleteHotNanos
	s.DeleteArchived += other.DeleteArchived
	s.DeleteArchivedNanos += other.DeleteArchivedNanos
	s.DeleteMissing += other.DeleteMissing
	s.DeleteMissingNanos += other.DeleteMissingNanos
	s.Errors += other.Errors
}

type traceStressSelection struct {
	startBlock      uint64
	endBlock        uint64
	blockCount      uint64
	started         bool
	firstBlock      uint64
	lastBlock       uint64
	nextBlock       uint64
	pending         *traceStressOp
	reachedEOF      bool
	reachedEndBlock bool
}

func (s *traceStressSelection) collect(reader *traceStressReader, limit int, remaining int64) ([]traceStressOp, bool, error) {
	ops := make([]traceStressOp, 0, limit)
	if remaining > 0 && int64(limit) > remaining {
		limit = int(remaining)
	}
	for len(ops) < limit {
		op, err := reader.Read()
		if errors.Is(err, io.EOF) {
			s.reachedEOF = true
			if s.endBlock > 0 && s.lastBlock >= s.endBlock {
				s.reachedEndBlock = true
			}
			return ops, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !s.started {
			if op.block < s.startBlock {
				continue
			}
			s.started = true
			s.firstBlock = op.block
			if s.blockCount > 0 {
				s.endBlock = op.block + s.blockCount - 1
			}
		}
		if s.endBlock > 0 && op.block > s.endBlock {
			s.reachedEndBlock = true
			return ops, true, nil
		}
		ops = append(ops, op)
		s.lastBlock = op.block
		if remaining > 0 && int64(len(ops)) >= remaining {
			return ops, true, nil
		}
	}
	return ops, false, nil
}

// collectBlock returns exactly one source block. A pending record is retained
// across calls, and gaps in the trace produce empty blocks so block-based
// pruning and commits still advance once for every real block number.
func (s *traceStressSelection) collectBlock(reader *traceStressReader) (uint64, []traceStressOp, bool, error) {
	if !s.started {
		for {
			op, err := reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					s.reachedEOF = true
					return 0, nil, true, nil
				}
				return 0, nil, false, err
			}
			if op.block < s.startBlock {
				continue
			}
			s.started = true
			s.firstBlock = op.block
			if s.startBlock > 0 {
				s.firstBlock = s.startBlock
			}
			s.nextBlock = s.firstBlock
			if s.blockCount > 0 {
				s.endBlock = s.firstBlock + s.blockCount - 1
			}
			s.pending = &op
			break
		}
	}
	block := s.nextBlock
	if s.endBlock > 0 && block > s.endBlock {
		s.reachedEndBlock = true
		return 0, nil, true, nil
	}
	var ops []traceStressOp
	for {
		var op traceStressOp
		if s.pending != nil {
			op = *s.pending
			s.pending = nil
		} else {
			var err error
			op, err = reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					s.reachedEOF = true
					s.lastBlock = block
					s.nextBlock = block + 1
					if s.endBlock > 0 && block == s.endBlock {
						s.reachedEndBlock = true
					}
					return block, ops, true, nil
				}
				return 0, nil, false, err
			}
		}
		if op.block < block {
			return 0, nil, false, fmt.Errorf("out-of-order source block %d after block %d", op.block, block)
		}
		if op.block > block {
			s.pending = &op
			s.lastBlock = block
			s.nextBlock = block + 1
			if s.endBlock > 0 && block == s.endBlock {
				s.reachedEndBlock = true
				return block, ops, true, nil
			}
			return block, ops, false, nil
		}
		ops = append(ops, op)
	}
}

func TestTraceStressCollectBlock(t *testing.T) {
	dir := t.TempDir()
	file, err := os.Create(filepath.Join(dir, "state_access_trace_000000001_000000004.csv.gz"))
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(file)
	writer := csv.NewWriter(gz)
	if err := writer.Write(strings.Split(traceStressHeader, ",")); err != nil {
		t.Fatal(err)
	}
	for _, block := range []uint64{1, 2, 4} {
		count := 1
		if block == 2 {
			count = 4101 // A large block must never split at the old 4000-op limit.
		}
		for i := 0; i < count; i++ {
			if err := writer.Write([]string{strconv.FormatUint(block, 10), "0", strconv.Itoa(i), "account", "0x0000000000000000000000000000000000001234", "", "read", common.Hash{}.Hex(), "32"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	selection := traceStressSelection{startBlock: 1, endBlock: 4}
	for _, want := range []struct {
		block uint64
		count int
		end   bool
	}{{1, 1, false}, {2, 4101, false}, {3, 0, false}, {4, 1, true}} {
		block, ops, end, err := selection.collectBlock(reader)
		if err != nil || block != want.block || len(ops) != want.count || end != want.end {
			t.Fatalf("block=%d ops=%d end=%t err=%v; want %+v", block, len(ops), end, err, want)
		}
	}
	if !selection.reachedEndBlock {
		t.Fatal("end block was not reached")
	}
}

func durationMS(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
}

func percentileDuration(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[int(p*float64(len(ordered)-1))]
}

func writeTraceStressJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func TestParseTraceStressRecord(t *testing.T) {
	address := "0x0000000000000000000000000000000000001234"
	valueHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	tests := []struct {
		name   string
		record []string
		kind   traceStressKind
	}{
		{"account read", []string{"1", "0", "0", "account", address, "", "read", valueHash, "32"}, traceStressGet},
		{"storage zero delete", []string{"1", "0", "1", "storage", address, common.Hash{}.Hex(), "write", traceStressZeroStorageHash, "32"}, traceStressDelete},
		{"code write", []string{"1", "0", "2", "code", address, "7", "write", valueHash, "31"}, traceStressPut},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			op, err := parseTraceStressRecord(test.record)
			if err != nil {
				t.Fatal(err)
			}
			if op.kind != test.kind {
				t.Fatalf("kind %d, want %d", op.kind, test.kind)
			}
			if len(op.key) != common.HashLength {
				t.Fatalf("key length %d", len(op.key))
			}
			if op.kind == traceStressPut && len(op.value) != common.HashLength {
				t.Fatalf("value length %d", len(op.value))
			}
		})
	}
}

func TestTraceStressReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state_access_trace_000000001_000000001.csv.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	w := csv.NewWriter(gz)
	address := "0x0000000000000000000000000000000000001234"
	valueHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	for _, row := range [][]string{
		strings.Split(traceStressHeader, ","),
		{"1", "0", "0", "account", address, "", "read", valueHash, "32"},
		{"1", "0", "1", "code", address, "0", "write", valueHash, "31"},
	} {
		if err := w.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, want := range []traceStressKind{traceStressGet, traceStressPut} {
		op, err := r.Read()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if op.kind != want {
			t.Fatalf("read %d kind %d, want %d", i, op.kind, want)
		}
	}
	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("final read error %v, want EOF", err)
	}
}

func TestTraceStressSelection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state_access_trace_000000001_000000003.csv.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	w := csv.NewWriter(gz)
	address := "0x0000000000000000000000000000000000001234"
	valueHash := "0x1111111111111111111111111111111111111111111111111111111111111111"
	if err := w.Write(strings.Split(traceStressHeader, ",")); err != nil {
		t.Fatal(err)
	}
	for _, block := range []string{"1", "2", "3"} {
		for seq, row := range [][]string{
			{"read", "", "32"},
			{"write", valueHash, "32"},
		} {
			record := []string{block, "0", strconv.Itoa(seq), "account", address, "", row[0], row[1], row[2]}
			if err := w.Write(record); err != nil {
				t.Fatal(err)
			}
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	selection := traceStressSelection{startBlock: 2, endBlock: 2}
	ops, complete, err := selection.collect(r, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(ops) != 2 || selection.firstBlock != 2 || selection.lastBlock != 2 || !selection.reachedEndBlock {
		t.Fatalf("unexpected block selection: complete=%v ops=%d first=%d last=%d reachedEnd=%v", complete, len(ops), selection.firstBlock, selection.lastBlock, selection.reachedEndBlock)
	}

	r2, err := newTraceStressReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	selection = traceStressSelection{blockCount: 2}
	ops, complete, err = selection.collect(r2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if complete || len(ops) != 2 || selection.firstBlock != 1 || selection.lastBlock != 1 {
		t.Fatalf("unexpected operation-limit selection: complete=%v ops=%d first=%d last=%d", complete, len(ops), selection.firstBlock, selection.lastBlock)
	}
	ops, complete, err = selection.collect(r2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if complete || len(ops) != 2 || selection.firstBlock != 1 || selection.lastBlock != 2 || selection.endBlock != 2 {
		t.Fatalf("unexpected block-count selection: complete=%v ops=%d first=%d last=%d end=%d", complete, len(ops), selection.firstBlock, selection.lastBlock, selection.endBlock)
	}
	ops, complete, err = selection.collect(r2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(ops) != 0 {
		t.Fatalf("unexpected end-boundary read: complete=%v ops=%d", complete, len(ops))
	}
}

func TestTraceStressAccessClassifier(t *testing.T) {
	db := NewMemoryDBAdapter()
	config := DefaultConfig()
	config.ShardDepth = 8
	config.StemMode = *traceStressStemMode
	backend := NewTrie(nil, db, NewPooledKeccakHasher(), config, true)
	trie, err := NewStemTrie(backend)
	if err != nil {
		t.Fatal(err)
	}
	key := stemTestKey(0x53, 44)
	missingSuffix := stemTestKey(0x53, 45)
	if err := trie.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if _, err := backend.CommitToBatch(batch, false); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	if fromArchive, err := classifyTraceStressAccess(backend, key); err != nil || fromArchive {
		t.Fatalf("hot classification: fromArchive=%v err=%v", fromArchive, err)
	}
	if _, err := classifyTraceStressAccess(backend, missingSuffix); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("missing suffix classification: err=%v", err)
	}

	shardID := backend.GetShardID(key[:StemSize])
	backend.SetGlobalEpoch(0)
	backend.pruneShardIdx = shardID
	if err := backend.PruneNextShard(); err != nil {
		t.Fatal(err)
	}
	trie.cache.clear()
	if fromArchive, err := classifyTraceStressAccess(backend, key); err != nil || !fromArchive {
		t.Fatalf("archive classification: fromArchive=%v err=%v", fromArchive, err)
	}

	if err := trie.Put(key, []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if fromArchive, err := classifyTraceStressAccess(backend, key); err != nil || fromArchive {
		t.Fatalf("post-write classification: fromArchive=%v err=%v", fromArchive, err)
	}
}

// TestArchiveStemTraceStress replays ordered mainnet access rows directly
// against the configured archive trie (AMT by default, or StemTrie when
// traceStressStemMode is enabled). CSV/gzip parsing is outside the measured
// operation path.
func TestArchiveStemTraceStress(t *testing.T) {
	if *traceStressInputDir == "" {
		t.Skip("set -traceStressInputDir to run the trace workload")
	}
	if *traceStressBaseDir == "" {
		t.Fatal("-traceStressBaseDir must name a fresh output directory")
	}
	blockBoundary := *traceStressEndBlock > 0 || *traceStressBlocks > 0
	if *traceStressOps <= 0 && !blockBoundary {
		t.Fatal("traceStressOps must be positive when no end block or block count is set")
	}
	if *traceStressOps == 0 && !blockBoundary {
		t.Fatal("unlimited traceStressOps requires traceStressEndBlock or traceStressBlocks")
	}
	if *traceStressBatchSize <= 0 || *traceStressMetricsBatches <= 0 {
		t.Fatal("traceStressBatchSize and traceStressMetricsBatches must be positive")
	}
	if *traceStressAccessSampleEvery < 0 {
		t.Fatal("traceStressAccessSampleEvery cannot be negative")
	}
	if blockBoundary && *traceStressOps != 0 {
		t.Fatal("use traceStressOps=0 with traceStressEndBlock or traceStressBlocks so a block range is never truncated")
	}
	if *traceStressBatchByBlock && !blockBoundary {
		t.Fatal("traceStressBatchByBlock requires traceStressEndBlock or traceStressBlocks")
	}
	if *traceStressMPTVerifyAtCommit && (!*traceStressBatchByBlock || *traceStressHotLayer != "mpt") {
		t.Fatal("traceStressMPTVerifyAtCommit requires a block-aligned MPT run")
	}
	if *traceStressMPTVerifyAtCommit && (*traceStressMPTVerifyBucketWorkers < 1 || *traceStressMPTVerifyItemWorkers < 1) {
		t.Fatal("MPT verification worker counts must be positive")
	}
	if *traceStressEndBlock > 0 && *traceStressBlocks > 0 {
		t.Fatal("traceStressEndBlock and traceStressBlocks are mutually exclusive")
	}
	if *traceStressStartBlock > 0 && *traceStressEndBlock > 0 && *traceStressStartBlock > *traceStressEndBlock {
		t.Fatal("traceStressStartBlock cannot exceed traceStressEndBlock")
	}
	if *traceStressShardDepth < 1 || *traceStressShardDepth > 30 {
		t.Fatalf("invalid traceStressShardDepth %d", *traceStressShardDepth)
	}
	if *traceStressCuckooBuckets <= 0 || *traceStressCuckooSlots <= 0 {
		t.Fatalf("traceStressCuckooBuckets and traceStressCuckooSlots must be positive")
	}
	if *traceStressPruneEveryBatches < 0 {
		t.Fatal("traceStressPruneEveryBatches cannot be negative")
	}
	if *traceStressPruneEveryBatches > 0 && *traceStressPruneEveryBlocks > 0 {
		t.Fatal("traceStressPruneEveryBatches and traceStressPruneEveryBlocks are mutually exclusive")
	}
	if *traceStressSplitArchiveDB && *traceStressHotLayer != "mpt" {
		t.Fatal("traceStressSplitArchiveDB requires traceStressHotLayer=mpt")
	}
	if *traceStressSplitArchiveDB && !*traceStressCompactPerWindow {
		t.Fatal("traceStressSplitArchiveDB requires traceStressCompactPerWindow=true")
	}
	if _, err := os.Stat(*traceStressBaseDir); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Fatalf("traceStressBaseDir already exists: %s", *traceStressBaseDir)
		}
		t.Fatalf("stat traceStressBaseDir: %v", err)
	}
	stateDir := filepath.Join(*traceStressBaseDir, "state_db")
	archiveDir := filepath.Join(*traceStressBaseDir, "archive_db")
	resultsDir := filepath.Join(*traceStressBaseDir, "results")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if *traceStressSplitArchiveDB {
		if err := os.MkdirAll(archiveDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := newTraceStressReader(*traceStressInputDir, *traceStressStartFile, *traceStressFileLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// One archive round is one prune per rotation unit, spaced by the prune
	// cadence. The binary hot layer rotates over 2^ShardDepth shards; the MPT
	// hot layer rotates over 2^ShardDepthBits logical domains (bit granularity,
	// the paper's K = 2^k). Block-cadence mode (PruneEveryBlocks) has no fixed
	// ops-per-round; this stays a batches-mode estimate as before.
	archiveRoundDomains := int64(1) << uint(*traceStressShardDepth)
	if *traceStressHotLayer == "mpt" {
		archiveRoundDomains = int64(1) << uint(*traceStressShardDepthBits)
	}
	pruneEveryBatches := int64(*traceStressPruneEveryBatches)
	if pruneEveryBatches < 1 {
		pruneEveryBatches = 1
	}
	archivePeriodOps := archiveRoundDomains * pruneEveryBatches * int64(*traceStressBatchSize)
	stemCacheLimitMetadata := any(*traceStressStemCacheLimit)
	stemCacheMBMetadata := any(*traceStressStemCacheMB)
	stemActivationMetadata := any(*traceStressActivateArchivedStemOnRead)
	if !*traceStressStemMode {
		stemCacheLimitMetadata = "not_applicable"
		stemCacheMBMetadata = "not_applicable"
		stemActivationMetadata = "not_applicable"
	}
	metadata := map[string]any{
		"generated_at":              time.Now().Format(time.RFC3339),
		"base_dir":                  *traceStressBaseDir,
		"input_dir":                 *traceStressInputDir,
		"source_files":              reader.files,
		"start_file":                *traceStressStartFile,
		"requested_start_file":      *traceStressStartFile,
		"file_limit":                *traceStressFileLimit,
		"operations":                *traceStressOps,
		"start_block":               *traceStressStartBlock,
		"requested_start_block":     *traceStressStartBlock,
		"effective_start_file":      *traceStressStartFile,
		"effective_start_block":     nil,
		"end_block":                 *traceStressEndBlock,
		"block_count":               *traceStressBlocks,
		"batch_size":                *traceStressBatchSize,
		"batch_by_block":            *traceStressBatchByBlock,
		"metrics_batches":           *traceStressMetricsBatches,
		"shard_depth":               *traceStressShardDepth,
		"stem_mode":                 *traceStressStemMode,
		"stem_cache_limit":          stemCacheLimitMetadata,
		"stem_cache_mb":             stemCacheMBMetadata,
		"node_cache_limit":          *traceStressNodeCacheLimit,
		"node_cache_mb":             *traceStressNodeCacheMB,
		"commit_workers":            *traceStressCommitWorkers,
		"async_prune":               *traceStressAsyncPrune,
		"prune_every_batches":       *traceStressPruneEveryBatches,
		"prune_every_blocks":        *traceStressPruneEveryBlocks,
		"disable_archive":           *traceStressDisableArchive,
		"hot_layer":                 *traceStressHotLayer,
		"shard_depth_bits":          *traceStressShardDepthBits,
		"shard_count":               1 << uint(*traceStressShardDepthBits),
		"epoch_bitmap":              *traceStressHotLayer == "mpt",
		"hot_value_layout":          map[bool]string{true: "inline", false: "flatref"}[*traceStressHotLayer == "mpt"],
		"archive_bucket_capacity":   bucketCapacityMetadata(),
		"trie_backend":              *traceStressTrieBackend,
		"clean_cache_mb":            *traceStressPathCleanCacheMB,
		"write_buffer_mb":           *traceStressPathWriteBufferMB,
		"flush_every_batches":       *traceStressPathFlushEveryBatches,
		"archive_resident_entries":  *traceStressArchiveResidentEntries,
		"archive_period_ops":        archivePeriodOps,
		"cuckoo_buckets":            *traceStressCuckooBuckets,
		"cuckoo_slots":              *traceStressCuckooSlots,
		"mpt_verify_at_commit":      *traceStressMPTVerifyAtCommit,
		"mpt_verify_bucket_workers": *traceStressMPTVerifyBucketWorkers,
		"mpt_verify_item_workers":   *traceStressMPTVerifyItemWorkers,
		"archive_bucket_size": func() int {
			c := DefaultConfig()
			c.CuckooBuckets = *traceStressCuckooBuckets
			c.CuckooSlots = *traceStressCuckooSlots
			return c.ResolveArchiveBucketSize()
		}(),
		"leveldb_cache_mb":               512,
		"leveldb_handles":                256,
		"leveldb":                        map[string]any{"cache_mb": 512, "handles": 256},
		"split_archive_db":               *traceStressSplitArchiveDB,
		"archive_db_dir":                 map[bool]string{true: archiveDir, false: ""}[*traceStressSplitArchiveDB],
		"destructive_commit":             *traceStressDestructiveCommit,
		"access_sample_every":            *traceStressAccessSampleEvery,
		"final_stats":                    *traceStressFinalStats,
		"activate_archived_stem_on_read": stemActivationMetadata,
		"activate_archived_key_on_read":  *traceStressActivateArchivedKeyOnRead,
		"node_storage":                   NodeStoragePath,
		"physical_delete":                false,
		"value_rule":                     "32-byte value_hash; deterministic key/op hash when absent",
		"storage_zero_hash_is_delete":    true,
		"timing_scope":                   "Operations_Per_Sec includes archive bucket reads and read-triggered promotion; excludes parse, prune, access sampling, final stats, and block verification when enabled; raw Commit_ms includes block verification",
		"command":                        strings.Join(os.Args, " "),
	}
	if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "run_status.json"), map[string]any{"status": "running", "started_at": time.Now().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}

	db, err := leveldb.New(stateDir, 512, 256, "trace-stress", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var stateStore ethdb.KeyValueStore = db
	if *traceStressSplitArchiveDB {
		archiveDB, err := leveldb.New(archiveDir, 512, 256, "trace-stress-archive", false)
		if err != nil {
			t.Fatal(err)
		}
		defer archiveDB.Close()
		stateStore = &traceSplitArchiveStore{KeyValueStore: db, archive: archiveDB}
	}
	config := DefaultConfig()
	config.ShardDepth = *traceStressShardDepth
	config.NodeStorageScheme = NodeStoragePath
	config.NodeCacheLimit = *traceStressNodeCacheLimit
	config.NodeCacheBytesLimit = int64(*traceStressNodeCacheMB) * 1024 * 1024
	config.StemMode = *traceStressStemMode
	config.ActivateArchivedStemOnRead = *traceStressActivateArchivedStemOnRead
	config.ActivateArchivedKeyOnRead = *traceStressActivateArchivedKeyOnRead
	config.StemCacheLimit = *traceStressStemCacheLimit
	config.StemCacheBytesLimit = int64(*traceStressStemCacheMB) * 1024 * 1024
	config.CommitWorkers = *traceStressCommitWorkers
	config.AsyncPrune = *traceStressAsyncPrune
	config.PhysicalDelete = false
	config.CuckooBuckets = *traceStressCuckooBuckets
	config.CuckooSlots = *traceStressCuckooSlots
	backend := NewTrie(nil, &stressDBAdapter{stateStore}, NewPooledKeccakHasher(), config, true)
	stateAdapter := &stressDBAdapter{stateStore}
	var hotBackend TraceHotTrie = backend
	switch *traceStressHotLayer {
	case "amt":
	case "mpt":
		if *traceStressStemMode {
			t.Fatal("traceStressHotLayer=mpt requires traceStressStemMode=false")
		}
		if TraceHotTrieNew == nil {
			t.Fatal("traceStressHotLayer=mpt requested but the MPT hot-layer hook is not registered (external test package archive_test missing?)")
		}
		hotBackend, err = TraceHotTrieNew(TraceHotTrieSpec{
			Kind:                      *traceStressHotLayer,
			DB:                        &stressDBAdapter{stateStore},
			ShardDepthBits:            *traceStressShardDepthBits,
			CuckooBuckets:             *traceStressCuckooBuckets,
			CuckooSlots:               *traceStressCuckooSlots,
			ActivateArchivedKeyOnRead: *traceStressActivateArchivedKeyOnRead,
			Backend:                   *traceStressTrieBackend,
			CleanCacheBytes:           *traceStressPathCleanCacheMB << 20,
			WriteBufferBytes:          *traceStressPathWriteBufferMB << 20,
			FlushEveryBatches:         *traceStressPathFlushEveryBatches,
			ArchiveResidentEntries:    *traceStressArchiveResidentEntries,
			AsyncPrune:                *traceStressAsyncPrune,
			VerifyAtCommit:            *traceStressMPTVerifyAtCommit,
			VerifyBucketWorkers:       *traceStressMPTVerifyBucketWorkers,
			VerifyItemWorkers:         *traceStressMPTVerifyItemWorkers,
		})
		if err != nil {
			t.Fatalf("hot layer %q: %v", *traceStressHotLayer, err)
		}
	default:
		t.Fatalf("unknown traceStressHotLayer %q", *traceStressHotLayer)
	}
	var stemTrie *StemTrie
	getValue := hotBackend.Get
	putValue := hotBackend.Put
	deleteValue := hotBackend.Delete
	cacheDiagnostics := func() StemCacheDiagnostics { return StemCacheDiagnostics{} }
	classifyAccess := classifyTraceStressAccessAMT
	if *traceStressStemMode {
		stemTrie, err = NewStemTrie(backend)
		if err != nil {
			t.Fatal(err)
		}
		getValue = stemTrie.Get
		putValue = stemTrie.Put
		deleteValue = stemTrie.Delete
		cacheDiagnostics = stemTrie.CacheDiagnostics
		classifyAccess = classifyTraceStressAccess
	}
	batch := newStressBatcher(backend.db.NewBatch())
	defer batch.Reset()
	intermediateBatch := newStressBatcher(backend.db.NewBatch())
	defer intermediateBatch.Reset()

	metricsPath := filepath.Join(resultsDir, "asct_trace_stress.csv")
	metricsFile, err := os.Create(metricsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metricsFile.Close()
	metrics := csv.NewWriter(metricsFile)
	defer metrics.Flush()

	// T4 op-trace: a separate per-window CSV so the main metrics schema stays
	// untouched. Written only when MPT_OP_TRACE is set and the hot layer
	// registered a probe (OpTraceProbe), and rows are deltas between windows.
	var opTraceWriter *csv.Writer
	var opTraceFile *os.File
	var prevOpTrace map[string]int64
	if os.Getenv("MPT_OP_TRACE") != "" && OpTraceProbe != nil {
		opTraceFile, err = os.Create(filepath.Join(resultsDir, "op_trace.csv"))
		if err != nil {
			t.Fatal(err)
		}
		defer opTraceFile.Close()
		opTraceWriter = csv.NewWriter(opTraceFile)
		defer opTraceWriter.Flush()
		if err := opTraceWriter.Write([]string{"Window_End_Batch", "Ops", "Hot_ns", "Hot_Misses", "Probes", "Probe_ns", "Lock_ns", "Loads", "Load_ns", "Removes", "Remove_ns", "Evictions"}); err != nil {
			t.Fatal(err)
		}
		prevOpTrace = OpTraceProbe()
	}
	header := []string{
		"Window_Start_Batch", "Window_End_Batch", "Total_Batches", "Archive_Round_Completed", "Archive_Round_Progress", "Total_Operations", "First_Block", "Last_Block",
		"Window_Operations", "Reads", "Touches", "Writes", "Creates", "Deletes",
		"Executed_Gets", "Executed_Puts", "Executed_Deletes",
		"StemGet_Hot_Hits", "StemGet_Archive_Hits", "StemGet_Missing", "StemGet_Errors",
		"Access_Samples", "Sampled_Read_Hot", "Sampled_Read_Archived", "Sampled_Read_Missing",
		"Sampled_Write_Hot", "Sampled_Write_Archived", "Sampled_Write_Missing",
		"Sampled_Delete_Hot", "Sampled_Delete_Archived", "Sampled_Delete_Missing",
		"Sampled_Read_Hot_ms", "Sampled_Read_Archived_ms", "Sampled_Read_Missing_ms",
		"Sampled_Write_Hot_ms", "Sampled_Write_Archived_ms", "Sampled_Write_Missing_ms",
		"Sampled_Delete_Hot_ms", "Sampled_Delete_Archived_ms", "Sampled_Delete_Missing_ms",
		"Sampled_Access_Errors", "Hot_Read_Hit_Rate", "Access_Sample_ms",
		"Cold_Maintenance_ms",
		"Archive_Filter_Lookups", "Archive_Filter_Negatives", "Archive_Filter_Positives", "Archive_Filter_False_Positives", "Runtime_Filter_FPR",
		"Mutation_NonCold", "Archive_Promotion_Checks", "Archive_Promotion_Hits",
		"Archive_Read_Promotion_Calls", "Archive_Read_Promotion_Hits", "Archive_Read_Promotion_ms",
		"FlatValue_Gets", "FlatValue_Hits", "FlatValue_Misses",
		"Parse_ms", "Operations_ms", "Prune_Launch_ms", "Commit_ms", "DB_Write_ms", "Measured_Wall_ms",
		"Comparative_Wall_ms", "Operations_Per_Sec", "Batch_P50_ms", "Batch_P95_ms", "Batch_P99_ms", "Batch_Max_ms",
		"Comparative_Batch_P50_ms", "Comparative_Batch_P95_ms", "Comparative_Batch_P99_ms", "Comparative_Batch_Max_ms",
		"State_Bytes", "RSS_Bytes", "Heap_Bytes", "StemCache_Entries", "StemCache_Bytes",
		"StemCache_Window_Hits", "StemCache_Window_Misses", "StemCache_Window_Evictions",
		"StemCache_Total_Hits", "StemCache_Total_Misses", "StemCache_Total_Evictions",
		"Raw_Batch_Ops", "Raw_Batch_Bytes", "NodeCache_Total_Hits", "NodeCache_Total_Misses", "PathNode_DBGets",
		"MPT_Staged_HotNode_Bytes", "MPT_Staged_Aggregate_Bytes", "MPT_Staged_Archive_Bytes", "MPT_Staged_Flat_Bytes", "MPT_Staged_Index_Bytes",
		"MPT_NodeCache_Gets", "MPT_NodeCache_Hits",
		"Archive_Maint_ms", "Intermediate_Root_ms",
		"Prune_Extracted", "Prune_Resolved_Nodes", "Prune_Extract_ms", "Prune_Mount_ms",
		"Commit_Hx_ms", "Commit_Stage_ms", "Commit_TdbCommit_ms", "Commit_ArchiveLoop_ms",
		"Staged_Prune_Bytes", "Staged_Ops_Bytes",
		"Archive_Bytes", "System_Total_Bytes", "Live_Hot_Leaves", "Live_Buckets",
		"MPT_Staged_StubMeta_Bytes", "MPT_StubMeta_Fetches", "MPT_StubMeta_Fetch_Bytes",
		"GC_Pause_ms", "GC_Num",
		"Proof_Redemptions", "Proof_Bytes",
		"Proof_Block_P50_Bytes", "Proof_Block_P95_Bytes", "Proof_Block_P99_Bytes", "Proof_Block_Max_Bytes",
		"Proof_Verify_Count", "Proof_Verify_ms",
		"Prune_P50_ms", "Prune_P95_ms", "Prune_P99_ms", "Prune_Max_ms", "Prune_Spike_Rate",
		"Resurrection_Proof_P50_Bytes", "Resurrection_Proof_P95_Bytes", "Resurrection_Proof_P99_Bytes", "Resurrection_Proof_Max_Bytes",
		"Resurrection_Proof_P50_ms", "Resurrection_Proof_P95_ms", "Resurrection_Proof_P99_ms", "Resurrection_Proof_Max_ms",
		"Bucket_Occupancy_P50", "Bucket_Occupancy_P95", "Bucket_Occupancy_P99", "Bucket_Occupancy_Max", "Bucket_Empty_Count",
		"Filter_Probe_P50_ms", "Filter_Probe_P95_ms", "Filter_Probe_P99_ms", "Filter_Probe_Max_ms",
		"Resurrection_Block_P50", "Resurrection_Block_P95", "Resurrection_Block_P99",
		"Full_Node_DB_Bytes", "Archive_DB_Bytes", "Path_Buffered_Bytes", "Path_Diff_Bytes",
	}
	header = append(header,
		"Block_All_N", "Block_All_Redemptions_Mean", "Block_All_Redemptions_P50", "Block_All_Redemptions_P95", "Block_All_Redemptions_P99",
		"Block_All_Unique_Proof_Mean_Bytes", "Block_All_Unique_Proof_P50_Bytes", "Block_All_Unique_Proof_P95_Bytes", "Block_All_Unique_Proof_P99_Bytes",
		"Block_With_Redemption_N", "Block_With_Redemption_Redemptions_Mean", "Block_With_Redemption_Redemptions_P50", "Block_With_Redemption_Redemptions_P95", "Block_With_Redemption_Redemptions_P99",
		"Block_With_Redemption_Unique_Proof_Mean_Bytes", "Block_With_Redemption_Unique_Proof_P50_Bytes", "Block_With_Redemption_Unique_Proof_P95_Bytes", "Block_With_Redemption_Unique_Proof_P99_Bytes",
		"Block_Unique_Buckets_Total", "Block_Unique_Proof_Bytes_Total",
	)
	if err := metrics.Write(header); err != nil {
		t.Fatal(err)
	}
	metrics.Flush()

	proc, _ := process.NewProcess(int32(os.Getpid()))
	ResetCommitDiagnostics()
	selection := traceStressSelection{
		startBlock: *traceStressStartBlock,
		endBlock:   *traceStressEndBlock,
		blockCount: *traceStressBlocks,
	}
	var (
		totalCounts                   traceStressCounts
		window                        traceStressWindow
		totalParse                    time.Duration
		totalOpsDur                   time.Duration
		totalPrune                    time.Duration
		totalCommit                   time.Duration
		totalWrite                    time.Duration
		totalGets                     int64
		totalPuts                     int64
		totalDeletes                  int64
		totalAccess                   traceStressAccessStats
		totalAccessSample             time.Duration
		totalClassificationFilter     traceStressFilterStats
		totalColdMaintenance          time.Duration
		lastRoot                      []byte
		prevCache                     = cacheDiagnostics()
		initialUpdateDiag             = LastUpdateDiagnostics()
		prevUpdateDiag                = initialUpdateDiag
		prevMPTDiag                   [10]int64
		prevPruneDiag                 [4]int64
		prevGCPauseNs                 uint64
		prevGCNum                     uint32
		started                       = time.Now()
		totalBatches                  int64
		prunesDone                    int64
		nextPruneBlock                uint64
		executedOperations            int64
		proofLastCount                int64
		proofLastBytes                int64
		proofBlockNum                 uint64
		proofBlockAcc                 int64
		proofCountAcc                 int64 // per-block resurrection count for the trailing open block (E2 v6)
		proofBlockOpen                bool
		proofBlockTotal               int64 // completed blocks carrying redemptions
		proofBlockMax                 int64 // global max single-block proof bytes
		proofBlockReservoir           []int64
		proofRedemptionSeen           int64
		proofRedemptionReservoir      []int64
		proofVerifyLastCount          int64
		proofVerifyLastNanos          int64
		proofRng                      = rand.New(rand.NewSource(20261004))
		allBlockRedemptions           []int64
		allBlockUniqueProofBytes      []int64
		positiveBlockRedemptions      []int64
		positiveBlockUniqueProofBytes []int64
		totalUniqueProofBuckets       int64
	)
	pruneOne := func() (time.Duration, error) {
		if *traceStressDisableArchive {
			return 0, nil
		}
		pruneStart := time.Now()
		if err := hotBackend.PruneNextShard(); err != nil {
			return 0, err
		}
		prunesDone++
		return time.Since(pruneStart), nil
	}

	flushWindow := func(batchNumber int64) {
		vc, vn := MPTRedemptionVerifyStats()
		window.proofVerifyCount = vc - proofVerifyLastCount
		window.proofVerify = time.Duration(vn - proofVerifyLastNanos)
		proofVerifyLastCount, proofVerifyLastNanos = vc, vn
		cache := cacheDiagnostics()
		window.stemHits = cache.Hits - prevCache.Hits
		window.stemMisses = cache.Misses - prevCache.Misses
		window.stemEvictions = cache.Evictions - prevCache.Evictions
		prevCache = cache
		measured := window.operations + window.prune + window.intermediate + window.intermediateWrite + window.commit + window.write
		comparative := window.operations + window.commit + window.write
		if *traceStressMPTVerifyAtCommit {
			comparative -= window.proofVerify
		}
		archiveMaint := window.prune + window.intermediate + window.intermediateWrite
		opsPerSec := float64(0)
		if comparative > 0 {
			opsPerSec = float64(window.counts.total()) / comparative.Seconds()
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		var rss uint64
		if proc != nil {
			if info, err := proc.MemoryInfo(); err == nil {
				rss = info.RSS
			}
		}
		// Per-window storage compaction (user ruling 2026-10-12): compact the
		// LevelDB BEFORE measuring State_Bytes so the storage curve reflects
		// live data only — engine dead bytes (tombstones/overwritten versions
		// not yet reclaimed) are reclaimed at every window boundary. Runs at
		// the boundary where no business batch is in flight, so business
		// latency timing is untouched; its own duration is reported in the
		// per-window log line, not in any r/wall metric.
		if *traceStressCompactPerWindow {
			// Split storage must persist all pathdb diff and buffer nodes before
			// recording the physical full-node directory. The legacy Flush leaves
			// one diff layer and may undercount the final state.
			if *traceStressSplitArchiveDB {
				flusher, ok := hotBackend.(interface{ FlushDurable() error })
				if !ok {
					t.Fatal("split archive DB requires a durable hot-layer flush")
				}
				if err := flusher.FlushDurable(); err != nil {
					t.Fatalf("per-window durable hot-layer flush: %v", err)
				}
			} else if flusher, ok := hotBackend.(interface{ Flush() error }); ok {
				if err := flusher.Flush(); err != nil {
					t.Fatalf("per-window hot-layer flush: %v", err)
				}
			}
			compactStart := time.Now()
			if err := stateAdapter.Compact(); err != nil {
				t.Fatalf("per-window compaction: %v", err)
			}
			windowCompactDur := time.Since(compactStart)
			fmt.Printf("[TRACE_STRESS] batch=%d compact_ms=%d\n", batchNumber, windowCompactDur.Milliseconds())
		}
		diag := LastCommitDiagnostics()
		fullNodeDBBytes := getDirSize(stateDir)
		archiveDBBytes := int64(0)
		if *traceStressSplitArchiveDB {
			archiveDBBytes = getDirSize(archiveDir)
		}
		stateBytes := fullNodeDBBytes + archiveDBBytes
		var pathBufferedBytes, pathDiffBytes int64
		if ps, ok := hotBackend.(interface {
			PathStats() (int64, int64, int64, int64)
		}); ok {
			_, _, pathBufferedBytes, pathDiffBytes = ps.PathStats()
			if *traceStressSplitArchiveDB && *traceStressCompactPerWindow && (pathBufferedBytes != 0 || pathDiffBytes != 0) {
				t.Fatalf("per-window durable flush left pathdb bytes: buffer=%d diff=%d", pathBufferedBytes, pathDiffBytes)
			}
		}
		currentUpdateDiag := LastUpdateDiagnostics()
		updateDiag := currentUpdateDiag.Sub(prevUpdateDiag)
		prevUpdateDiag = currentUpdateDiag
		curMPTDiag := [10]int64{diag.MPTStagedHotNodeBytes, diag.MPTStagedAggregateBytes, diag.MPTStagedArchiveBytes, diag.MPTStagedFlatBytes, diag.MPTStagedIndexBytes, diag.MPTNodeCacheGets, diag.MPTNodeCacheHits, diag.MPTStagedStubMetaBytes, diag.MPTStubMetaFetches, diag.MPTStubMetaFetchBytes}
		for i := range curMPTDiag {
			window.mptDiag[i] = curMPTDiag[i] - prevMPTDiag[i]
		}
		prevMPTDiag = curMPTDiag
		curPruneDiag := [4]int64{diag.MPTPruneExtracted, diag.MPTPruneResolvedNodes, diag.MPTPruneExtractNanos, diag.MPTPruneMountNanos}
		for i := range curPruneDiag {
			window.pruneDiag[i] = curPruneDiag[i] - prevPruneDiag[i]
		}
		prevPruneDiag = curPruneDiag
		sampledExistingReads := window.access.ReadHot + window.access.ReadArchived
		workloadFilter := filterStatsFromDiagnostics(updateDiag).sub(window.classificationFilter)
		hotReadRate := traceStressRatio(window.access.ReadHot, sampledExistingReads)
		runtimeFilterFPR := traceStressRatio(workloadFilter.FalsePositives, workloadFilter.Negatives)
		archiveRoundCompleted := prunesDone / archiveRoundDomains
		archiveRoundProgress := prunesDone % archiveRoundDomains
		// Redemption-proof telemetry: sampled full-node audit delta, plus the
		// window's per-block proof percentiles (the trailing open block is
		// carried into the next window, so only completed blocks are ranked).
		proofBlocksSorted := sortedInt64Copy(window.proofBlocks)
		// E2 v6 exact histograms: drain the single resurrection-proof size and
		// latency histograms and the filter-probe (cuckoo lookup) latency
		// histogram at window close — full population, no sampling — then
		// snapshot the archive-bucket occupancy distribution (entry count per
		// bucket plus empty-bucket count; total buckets is the existing
		// Live_Buckets column and is not repeated here). The bucket snapshot
		// goes through the hot-layer hook for the same import-cycle reason the
		// gauge does (mpt imports trie/archive).
		psizes, platns, _, _, _ := TakeMPTRedemptionProofHistogram()
		window.proofSizes = psizes
		window.proofLatencies = make([]time.Duration, len(platns))
		for i, ns := range platns {
			window.proofLatencies[i] = time.Duration(ns)
		}
		fplatns, _, _ := TakeMPTFilterProbeLatencyHistogram()
		window.filterProbeLatencies = make([]time.Duration, len(fplatns))
		for i, ns := range fplatns {
			window.filterProbeLatencies[i] = time.Duration(ns)
		}
		if snapper, ok := hotBackend.(interface {
			BucketOccupancy() ([]int64, int64)
		}); ok {
			window.bucketOccupancy, window.bucketEmpty = snapper.BucketOccupancy()
		}
		// RQ4 per-prune latency: percentiles over this window's Sharded-Pruning
		// batches. Spike rate = share of prune events exceeding 5x the window
		// median. Guard: when the window median is below 0.1ms (idle-rotation
		// prunes that move nothing), 5x-median is noise-adjacent to zero and
		// would flag every event as a spike, so the rate is reported as 0.
		pruneP50 := percentileDuration(window.pruneWall, .50)
		pruneSpikeRate := float64(0)
		if pruneP50 >= 100*time.Microsecond {
			pruneSpikeThreshold := 5 * pruneP50
			pruneSpikes := 0
			for _, d := range window.pruneWall {
				if d > pruneSpikeThreshold {
					pruneSpikes++
				}
			}
			pruneSpikeRate = float64(pruneSpikes) / float64(len(window.pruneWall))
		}
		row := []string{
			strconv.FormatInt(batchNumber-int64(len(window.batchWall))+1, 10), strconv.FormatInt(batchNumber, 10), strconv.FormatInt(batchNumber, 10), strconv.FormatInt(archiveRoundCompleted, 10), strconv.FormatInt(archiveRoundProgress, 10), strconv.FormatInt(totalCounts.total(), 10),
			strconv.FormatUint(window.firstBlock, 10), strconv.FormatUint(window.lastBlock, 10), strconv.FormatInt(window.counts.total(), 10),
			strconv.FormatInt(window.counts.Reads, 10), strconv.FormatInt(window.counts.Touches, 10), strconv.FormatInt(window.counts.Writes, 10), strconv.FormatInt(window.counts.Creates, 10), strconv.FormatInt(window.counts.Deletes, 10),
			strconv.FormatInt(window.gets, 10), strconv.FormatInt(window.puts, 10), strconv.FormatInt(window.deletes, 10),
			strconv.FormatInt(updateDiag.StemGetHotHits, 10), strconv.FormatInt(updateDiag.StemGetArchiveHits, 10), strconv.FormatInt(updateDiag.StemGetMissing, 10), strconv.FormatInt(updateDiag.StemGetErrors, 10),
			strconv.FormatInt(window.access.Samples, 10), strconv.FormatInt(window.access.ReadHot, 10), strconv.FormatInt(window.access.ReadArchived, 10), strconv.FormatInt(window.access.ReadMissing, 10),
			strconv.FormatInt(window.access.WriteHot, 10), strconv.FormatInt(window.access.WriteArchived, 10), strconv.FormatInt(window.access.WriteMissing, 10),
			strconv.FormatInt(window.access.DeleteHot, 10), strconv.FormatInt(window.access.DeleteArchived, 10), strconv.FormatInt(window.access.DeleteMissing, 10),
			durationMS(time.Duration(window.access.ReadHotNanos)), durationMS(time.Duration(window.access.ReadArchivedNanos)), durationMS(time.Duration(window.access.ReadMissingNanos)),
			durationMS(time.Duration(window.access.WriteHotNanos)), durationMS(time.Duration(window.access.WriteArchivedNanos)), durationMS(time.Duration(window.access.WriteMissingNanos)),
			durationMS(time.Duration(window.access.DeleteHotNanos)), durationMS(time.Duration(window.access.DeleteArchivedNanos)), durationMS(time.Duration(window.access.DeleteMissingNanos)),
			strconv.FormatInt(window.access.Errors, 10), strconv.FormatFloat(hotReadRate*100, 'f', 6, 64), durationMS(window.accessSample),
			durationMS(window.coldMaintenance),
			strconv.FormatInt(workloadFilter.Lookups, 10), strconv.FormatInt(workloadFilter.Negatives, 10),
			strconv.FormatInt(workloadFilter.Positives, 10), strconv.FormatInt(workloadFilter.FalsePositives, 10),
			strconv.FormatFloat(runtimeFilterFPR*100, 'f', 6, 64),
			strconv.FormatInt(window.puts+window.deletes-updateDiag.ArchivePromotionHits, 10), strconv.FormatInt(updateDiag.ArchivePromotionChecks, 10), strconv.FormatInt(updateDiag.ArchivePromotionHits, 10),
			strconv.FormatInt(updateDiag.ArchiveReadPromotionCalls, 10), strconv.FormatInt(updateDiag.ArchiveReadPromotionHits, 10), durationMS(time.Duration(updateDiag.ArchiveReadPromotionNanos)),
			strconv.FormatInt(updateDiag.FlatValueGets, 10), strconv.FormatInt(updateDiag.FlatValueGetHits, 10), strconv.FormatInt(updateDiag.FlatValueGetMisses, 10),
			durationMS(window.parse), durationMS(window.operations), durationMS(window.prune), durationMS(window.commit), durationMS(window.write), durationMS(measured),
			durationMS(comparative),
			strconv.FormatFloat(opsPerSec, 'f', 2, 64), durationMS(percentileDuration(window.batchWall, .50)), durationMS(percentileDuration(window.batchWall, .95)), durationMS(percentileDuration(window.batchWall, .99)), durationMS(percentileDuration(window.batchWall, 1)),
			durationMS(percentileDuration(window.comparativeBatchWall, .50)), durationMS(percentileDuration(window.comparativeBatchWall, .95)), durationMS(percentileDuration(window.comparativeBatchWall, .99)), durationMS(percentileDuration(window.comparativeBatchWall, 1)),
			strconv.FormatInt(stateBytes, 10), strconv.FormatUint(rss, 10), strconv.FormatUint(mem.HeapAlloc, 10), strconv.FormatInt(cache.Entries, 10), strconv.FormatInt(cache.Bytes, 10),
			strconv.FormatInt(window.stemHits, 10), strconv.FormatInt(window.stemMisses, 10), strconv.FormatInt(window.stemEvictions, 10), strconv.FormatInt(cache.Hits, 10), strconv.FormatInt(cache.Misses, 10), strconv.FormatInt(cache.Evictions, 10),
			strconv.FormatInt(window.rawBatchOps, 10), strconv.FormatInt(window.rawBatchBytes, 10), strconv.FormatInt(diag.NodeCacheTotalHits, 10), strconv.FormatInt(diag.NodeCacheTotalMisses, 10), strconv.FormatInt(diag.PathNodeDBGets, 10),
			strconv.FormatInt(window.mptDiag[0], 10), strconv.FormatInt(window.mptDiag[1], 10), strconv.FormatInt(window.mptDiag[2], 10),
			strconv.FormatInt(window.mptDiag[3], 10), strconv.FormatInt(window.mptDiag[4], 10),
			strconv.FormatInt(window.mptDiag[5], 10), strconv.FormatInt(window.mptDiag[6], 10),
			strconv.FormatInt(archiveMaint.Milliseconds(), 10), strconv.FormatInt((window.intermediate + window.intermediateWrite).Milliseconds(), 10),
			strconv.FormatInt(window.pruneDiag[0], 10), strconv.FormatInt(window.pruneDiag[1], 10),
			strconv.FormatInt(window.pruneDiag[2]/1e6, 10), strconv.FormatInt(window.pruneDiag[3]/1e6, 10),
			strconv.FormatInt(window.finalCommitPhase[0]/1e6, 10), strconv.FormatInt(window.finalCommitPhase[1]/1e6, 10),
			strconv.FormatInt(window.finalCommitPhase[2]/1e6, 10), strconv.FormatInt(window.finalCommitPhase[3]/1e6, 10),
			strconv.FormatInt(window.stagedPruneBytes, 10), strconv.FormatInt(window.stagedOpsBytes, 10),
			strconv.FormatInt(diag.MPTArchivePayloadBytes, 10), strconv.FormatInt(stateBytes, 10),
			strconv.FormatInt(diag.MPTLiveHotLeaves, 10), strconv.FormatInt(diag.MPTLiveBuckets, 10),
			strconv.FormatInt(window.mptDiag[7], 10), strconv.FormatInt(window.mptDiag[8], 10), strconv.FormatInt(window.mptDiag[9], 10),
			strconv.FormatUint((mem.PauseTotalNs-prevGCPauseNs)/uint64(time.Millisecond), 10), strconv.FormatUint(uint64(mem.NumGC-prevGCNum), 10),
			strconv.FormatInt(window.proofRedemptions, 10), strconv.FormatInt(window.proofBytes, 10),
			strconv.FormatInt(percentileInt64(proofBlocksSorted, .50), 10), strconv.FormatInt(percentileInt64(proofBlocksSorted, .95), 10),
			strconv.FormatInt(percentileInt64(proofBlocksSorted, .99), 10), strconv.FormatInt(percentileInt64(proofBlocksSorted, 1), 10),
			strconv.FormatInt(window.proofVerifyCount, 10), durationMS(window.proofVerify),
			durationMS(pruneP50), durationMS(percentileDuration(window.pruneWall, .95)), durationMS(percentileDuration(window.pruneWall, .99)), durationMS(percentileDuration(window.pruneWall, 1)),
			strconv.FormatFloat(pruneSpikeRate, 'f', 6, 64),
			// E2 v6: exact per-window statistics, appended columns only.
			// Resurrection proofs (full population, no sampling).
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), .50), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), .95), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), .99), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofSizes), 1), 10),
			durationMS(percentileDuration(window.proofLatencies, .50)),
			durationMS(percentileDuration(window.proofLatencies, .95)),
			durationMS(percentileDuration(window.proofLatencies, .99)),
			durationMS(percentileDuration(window.proofLatencies, 1)),
			// Archive-bucket occupancy distribution (window-close snapshot).
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), .50), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), .95), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), .99), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.bucketOccupancy), 1), 10),
			strconv.FormatInt(window.bucketEmpty, 10),
			// Filter probe (cuckoo lookup) latency, exact per-probe histogram.
			durationMS(percentileDuration(window.filterProbeLatencies, .50)),
			durationMS(percentileDuration(window.filterProbeLatencies, .95)),
			durationMS(percentileDuration(window.filterProbeLatencies, .99)),
			durationMS(percentileDuration(window.filterProbeLatencies, 1)),
			// Per-block resurrection counts (completed blocks only).
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofCounts), .50), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofCounts), .95), 10),
			strconv.FormatInt(percentileInt64(sortedInt64Copy(window.proofCounts), .99), 10),
			strconv.FormatInt(fullNodeDBBytes, 10), strconv.FormatInt(archiveDBBytes, 10),
			strconv.FormatInt(pathBufferedBytes, 10), strconv.FormatInt(pathDiffBytes, 10),
		}
		if *traceStressMPTVerifyAtCommit {
			row = append(row, blockDistributionColumns(
				window.blockRedemptionsAll, window.blockUniqueProofBytesAll,
				window.blockRedemptionsPositive, window.blockUniqueProofBytesPositive,
				window.blockUniqueBucketsTotal,
			)...)
		} else {
			row = append(row, make([]string, 20)...)
		}
		prevGCPauseNs = mem.PauseTotalNs
		prevGCNum = mem.NumGC
		if err := metrics.Write(row); err != nil {
			t.Fatal(err)
		}
		metrics.Flush()
		if err := metrics.Error(); err != nil {
			t.Fatal(err)
		}
		if opTraceWriter != nil {
			snap := OpTraceProbe()
			delta := func(key string) int64 { return snap[key] - prevOpTrace[key] }
			if err := opTraceWriter.Write([]string{
				strconv.FormatInt(batchNumber, 10),
				strconv.FormatInt(delta("ops"), 10), strconv.FormatInt(delta("hot_ns"), 10),
				strconv.FormatInt(delta("hot_misses"), 10), strconv.FormatInt(delta("probes"), 10),
				strconv.FormatInt(delta("probe_ns"), 10), strconv.FormatInt(delta("lock_ns"), 10),
				strconv.FormatInt(delta("loads"), 10),
				strconv.FormatInt(delta("load_ns"), 10), strconv.FormatInt(delta("removes"), 10),
				strconv.FormatInt(delta("remove_ns"), 10), strconv.FormatInt(delta("evictions"), 10),
			}); err != nil {
				t.Fatal(err)
			}
			opTraceWriter.Flush()
			prevOpTrace = snap
		}
		fmt.Printf("[TRACE_STRESS] batches=%d/%d ops=%d block=%d cache=%d hits=%d misses=%d full_db=%d archive_db=%d rate=%.0f ops/s\n",
			batchNumber, totalBatches, totalCounts.total(), window.lastBlock, cache.Entries, window.stemHits, window.stemMisses, fullNodeDBBytes, archiveDBBytes, opsPerSec)
		totalAccess.merge(window.access)
		totalAccessSample += window.accessSample
		totalClassificationFilter.add(window.classificationFilter)
		totalColdMaintenance += window.coldMaintenance
		window = traceStressWindow{}
	}

	for batchNumber := int64(1); ; batchNumber++ {
		if vRefDebugEnabled() {
			SetVRefContext(fmt.Sprintf("batch=%d ops=%d", batchNumber, totalCounts.total()))
		}
		parseStart := time.Now()
		remaining := *traceStressOps - totalCounts.total()
		if *traceStressOps == 0 {
			remaining = 0
		}
		var ops []traceStressOp
		var blockNumber uint64
		var reachedBoundary bool
		var err error
		if *traceStressBatchByBlock {
			blockNumber, ops, reachedBoundary, err = selection.collectBlock(reader)
		} else {
			ops, reachedBoundary, err = selection.collect(reader, *traceStressBatchSize, remaining)
			if len(ops) > 0 {
				blockNumber = ops[0].block
			}
		}
		if err != nil {
			t.Fatalf("batch %d parse: %v", batchNumber, err)
		}
		if blockNumber == 0 {
			if !selection.started {
				t.Fatalf("no trace records at or after traceStressStartBlock=%d", *traceStressStartBlock)
			}
			break
		}
		totalBatches = batchNumber
		if metadata["effective_start_block"] == nil {
			metadata["effective_start_block"] = blockNumber
			metadata["start_block"] = blockNumber
			if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "metadata.json"), metadata); err != nil {
				t.Fatal(err)
			}
		}
		if *traceStressBatchByBlock {
			if window.firstBlock == 0 {
				window.firstBlock = blockNumber
			}
			window.lastBlock = blockNumber
		} else {
			window.firstBlock = ops[0].block
			window.lastBlock = ops[len(ops)-1].block
		}
		for _, op := range ops {
			window.counts.add(op.operation)
			totalCounts.add(op.operation)
		}
		parseDur := time.Since(parseStart)
		window.parse += parseDur
		totalParse += parseDur
		batchStart := time.Now()
		var pruneDur time.Duration
		prunedThisBatch := false
		if *traceStressPruneEveryBatches > 0 && (batchNumber-1)%int64(*traceStressPruneEveryBatches) == 0 {
			dur, err := pruneOne()
			if err != nil {
				t.Fatalf("batch %d prune: %v", batchNumber, err)
			}
			pruneDur = dur
			prunedThisBatch = true
		}
		if !*traceStressDisableArchive && *traceStressPruneEveryBlocks > 0 {
			if batchNumber == 1 {
				dur, err := pruneOne()
				if err != nil {
					t.Fatalf("batch %d prune at block %d: %v", batchNumber, window.lastBlock, err)
				}
				pruneDur += dur
				prunedThisBatch = true
				nextPruneBlock = window.firstBlock + *traceStressPruneEveryBlocks
			}
			for window.lastBlock >= nextPruneBlock {
				dur, err := pruneOne()
				if err != nil {
					t.Fatalf("batch %d prune at block %d: %v", batchNumber, window.lastBlock, err)
				}
				pruneDur += dur
				prunedThisBatch = true
				nextPruneBlock += *traceStressPruneEveryBlocks
			}
		}
		// AMT pipeline (paper): prune → intermediate root → operations → root.
		// The intermediate root flushes the prune's dirt here, in the archive-
		// maintenance phase, so the final root's batch writes stay business-only.
		// Its time is reported under Archive_Maint_ms and excluded from the
		// counted operations+root time (Comparative_Wall_ms).
		snapBeforeCommit := LastCommitDiagnostics()
		var intermediateDur, intermediateWriteDur time.Duration
		if prunedThisBatch && !*traceStressDisableArchive && *traceStressMPTIntermediateRoot && *traceStressHotLayer == "mpt" {
			intermediateStart := time.Now()
			if _, err := hotBackend.CommitToBatch(intermediateBatch, *traceStressDestructiveCommit); err != nil {
				t.Fatalf("batch %d intermediate root commit: %v", batchNumber, err)
			}
			intermediateDur = time.Since(intermediateStart)
			intermediateWriteStart := time.Now()
			if err := intermediateBatch.Write(); err != nil {
				t.Fatalf("batch %d intermediate root write: %v", batchNumber, err)
			}
			intermediateWriteDur = time.Since(intermediateWriteStart)
			intermediateBatch.Reset()
		}
		snapMidCommit := LastCommitDiagnostics()
		beforeUpdateDiag := LastUpdateDiagnostics()
		blockProofStart, _, _, _ := MPTRedemptionProofStats()
		opStart := time.Now()
		batchAccessSampleDur := time.Duration(0)
		for _, op := range ops {
			sample := *traceStressAccessSampleEvery > 0 &&
				executedOperations%*traceStressAccessSampleEvery == 0
			var fromArchive bool
			var sampleErr error
			if sample {
				beforeSampleDiag := LastUpdateDiagnostics()
				sampleStart := time.Now()
				fromArchive, sampleErr = classifyAccess(hotBackend, op.key)
				sampleDur := time.Since(sampleStart)
				window.classificationFilter.add(filterStatsFromDiagnostics(LastUpdateDiagnostics()).sub(filterStatsFromDiagnostics(beforeSampleDiag)))
				window.accessSample += sampleDur
				batchAccessSampleDur += sampleDur
			}
			executionStart := time.Now()
			switch op.kind {
			case traceStressGet:
				window.gets++
				totalGets++
				_, err := getValue(op.key)
				executionDur := time.Since(executionStart)
				if sample {
					window.access.add(op.kind, fromArchive, sampleErr, executionDur)
				}
				if err != nil && !errors.Is(err, ErrNodeNotFound) {
					t.Fatalf("batch %d get: %v", batchNumber, err)
				}
			case traceStressPut:
				window.puts++
				totalPuts++
				err := putValue(op.key, op.value)
				executionDur := time.Since(executionStart)
				if sample {
					window.access.add(op.kind, fromArchive, sampleErr, executionDur)
				}
				if err != nil {
					t.Fatalf("batch %d put: %v", batchNumber, err)
				}
			case traceStressDelete:
				window.deletes++
				totalDeletes++
				err := deleteValue(op.key)
				executionDur := time.Since(executionStart)
				if sample {
					window.access.add(op.kind, fromArchive, sampleErr, executionDur)
				}
				if err != nil {
					t.Fatalf("batch %d delete: %v", batchNumber, err)
				}
			}
			executedOperations++
			// Redemption-proof attribution: diff the cumulative counters per
			// op (a redemption fires inside Get) and charge the bytes to the
			// op's block; block totals close on block change and at run end.
			if pc, pb, _, plast := MPTRedemptionProofStats(); pc != proofLastCount {
				d := pb - proofLastBytes
				window.proofRedemptions += pc - proofLastCount
				window.proofBytes += d
				if !proofBlockOpen || proofBlockNum != op.block {
					if proofBlockOpen {
						window.proofBlocks = append(window.proofBlocks, proofBlockAcc)
						window.proofCounts = append(window.proofCounts, proofCountAcc)
						proofBlockTotal++
						if proofBlockAcc > proofBlockMax {
							proofBlockMax = proofBlockAcc
						}
						proofReservoirAdd(&proofBlockReservoir, proofBlockAcc, proofBlockTotal, 1<<19, proofRng)
					}
					proofBlockNum, proofBlockAcc, proofCountAcc, proofBlockOpen = op.block, 0, 0, true
				}
				proofBlockAcc += d
				proofCountAcc += pc - proofLastCount
				for i := int64(0); i < pc-proofLastCount; i++ {
					proofRedemptionSeen++
					proofReservoirAdd(&proofRedemptionReservoir, plast, proofRedemptionSeen, 1<<18, proofRng)
				}
				proofLastCount, proofLastBytes = pc, pb
			}
		}
		if *traceStressMPTVerifyAtCommit {
			blockProofEnd, _, _, _ := MPTRedemptionProofStats()
			redemptions := blockProofEnd - blockProofStart
			counter, ok := hotBackend.(interface{ PendingProofStats() (int64, int64) })
			if !ok {
				t.Fatal("MPT block verification has no distinct-proof counter")
			}
			buckets, uniqueBytes := counter.PendingProofStats()
			if (redemptions == 0) != (buckets == 0) {
				t.Fatalf("block %d: redemption/proof mismatch: %d redemptions, %d buckets", blockNumber, redemptions, buckets)
			}
			window.blockRedemptionsAll = append(window.blockRedemptionsAll, redemptions)
			window.blockUniqueProofBytesAll = append(window.blockUniqueProofBytesAll, uniqueBytes)
			allBlockRedemptions = append(allBlockRedemptions, redemptions)
			allBlockUniqueProofBytes = append(allBlockUniqueProofBytes, uniqueBytes)
			window.blockUniqueBucketsTotal += buckets
			totalUniqueProofBuckets += buckets
			if redemptions > 0 {
				window.blockRedemptionsPositive = append(window.blockRedemptionsPositive, redemptions)
				window.blockUniqueProofBytesPositive = append(window.blockUniqueProofBytesPositive, uniqueBytes)
				positiveBlockRedemptions = append(positiveBlockRedemptions, redemptions)
				positiveBlockUniqueProofBytes = append(positiveBlockUniqueProofBytes, uniqueBytes)
			}
		}
		afterUpdateDiag := LastUpdateDiagnostics()
		coldMaintenanceDur := time.Duration(afterUpdateDiag.ArchivePromotionNanos - beforeUpdateDiag.ArchivePromotionNanos)
		if coldMaintenanceDur < 0 {
			coldMaintenanceDur = 0
		}
		// Bucket loads happen during Get's archive probe, and promotion is part
		// of serving that Get. Keep both in foreground operation time; only
		// access-classification sampling is removed here.
		opDur := time.Since(opStart) - batchAccessSampleDur
		_, verifyBeforeNanos := MPTRedemptionVerifyStats()
		commitStart := time.Now()
		lastRoot, err = hotBackend.CommitToBatch(batch, *traceStressDestructiveCommit)
		if err != nil {
			t.Fatalf("batch %d commit: %v", batchNumber, err)
		}
		commitDur := time.Since(commitStart)
		verifyBlockDur := time.Duration(0)
		if *traceStressMPTVerifyAtCommit {
			_, verifyAfterNanos := MPTRedemptionVerifyStats()
			verifyBlockDur = time.Duration(verifyAfterNanos - verifyBeforeNanos)
		}
		diag := LastCommitDiagnostics()
		// Per-phase attribution: the intermediate commit's staged bytes are
		// archive maintenance; the final commit's staged bytes and phase
		// split are the counted business root computation.
		window.stagedPruneBytes += snapMidCommit.MPTStagedHotNodeBytes - snapBeforeCommit.MPTStagedHotNodeBytes
		window.stagedOpsBytes += diag.MPTStagedHotNodeBytes - snapMidCommit.MPTStagedHotNodeBytes
		window.finalCommitPhase[0] += diag.MPTCommitHxNanos - snapMidCommit.MPTCommitHxNanos
		window.finalCommitPhase[1] += diag.MPTCommitStageNanos - snapMidCommit.MPTCommitStageNanos
		window.finalCommitPhase[2] += diag.MPTCommitTdbCommitNanos - snapMidCommit.MPTCommitTdbCommitNanos
		window.finalCommitPhase[3] += diag.MPTCommitArchiveLoopNanos - snapMidCommit.MPTCommitArchiveLoopNanos
		writeStart := time.Now()
		if err := batch.Write(); err != nil {
			t.Fatalf("batch %d write: %v", batchNumber, err)
		}
		writeDur := time.Since(writeStart)
		batch.Reset()
		window.operations += opDur
		window.prune += pruneDur
		if prunedThisBatch {
			// Per-batch Sharded-Pruning duration for the RQ4 percentile columns;
			// batches that did not prune are excluded (matching the per-prune
			// population the latency judgments are defined on).
			window.pruneWall = append(window.pruneWall, pruneDur)
		}
		window.intermediate += intermediateDur
		window.intermediateWrite += intermediateWriteDur
		window.commit += commitDur
		window.write += writeDur
		window.coldMaintenance += coldMaintenanceDur
		window.batchWall = append(window.batchWall, time.Since(batchStart))
		window.comparativeBatchWall = append(window.comparativeBatchWall, opDur+commitDur+writeDur-verifyBlockDur)
		window.rawBatchOps += diag.RawBatchOps
		window.rawBatchBytes += diag.RawBatchBytes
		totalOpsDur += opDur
		totalPrune += pruneDur
		totalCommit += commitDur
		totalWrite += writeDur

		if batchNumber%int64(*traceStressMetricsBatches) == 0 || reachedBoundary {
			flushWindow(batchNumber)
		}
		if reachedBoundary {
			break
		}
	}
	if selection.endBlock > 0 && !selection.reachedEndBlock {
		t.Fatalf("trace ended at block %d before requested end block %d", selection.lastBlock, selection.endBlock)
	}

	var finalStats *TrieStats
	var finalStatsDur time.Duration
	// Buffered pathdb writes must reach disk before anything measures what is
	// actually stored, otherwise the final root is not resolvable from the
	// database and the storage scan measures nothing. Layers without a buffered
	// write tier simply do not implement Flush.
	if *traceStressSplitArchiveDB {
		if err := hotBackend.(interface{ FlushDurable() error }).FlushDurable(); err != nil {
			t.Fatalf("final durable hot-layer flush: %v", err)
		}
	} else if flusher, ok := hotBackend.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			t.Fatalf("final hot-layer flush: %v", err)
		}
	}
	finalFullNodeDBBytes := getDirSize(stateDir)
	finalArchiveDBBytes := int64(0)
	if *traceStressSplitArchiveDB {
		finalArchiveDBBytes = getDirSize(archiveDir)
	}
	finalSystemBytes := finalFullNodeDBBytes + finalArchiveDBBytes
	if *traceStressFinalStats {
		statsStart := time.Now()
		if amt, ok := hotBackend.(*Trie); ok {
			finalStats = amt.Stats()
		}
		finalStatsDur = time.Since(statsStart)
	}
	cache := cacheDiagnostics()
	actualBlockCount := uint64(0)
	if selection.endBlock >= selection.firstBlock && selection.firstBlock > 0 {
		actualBlockCount = selection.endBlock - selection.firstBlock + 1
	}
	updateDiag := LastUpdateDiagnostics().Sub(initialUpdateDiag)
	sampledExistingReads := totalAccess.ReadHot + totalAccess.ReadArchived
	workloadFilter := filterStatsFromDiagnostics(updateDiag).sub(totalClassificationFilter)
	hotReadRate := traceStressRatio(totalAccess.ReadHot, sampledExistingReads)
	readArchiveShare := traceStressRatio(totalAccess.ReadArchived, sampledExistingReads)
	runtimeFilterFPR := traceStressRatio(workloadFilter.FalsePositives, workloadFilter.Negatives)
	average := func(total, count int64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count)
	}
	opsPerSecond := func(ops int64, elapsed time.Duration) float64 {
		if elapsed <= 0 {
			return 0
		}
		return float64(ops) / elapsed.Seconds()
	}
	accessAverageNanos := map[string]float64{
		"read_hot":        average(totalAccess.ReadHotNanos, totalAccess.ReadHot),
		"read_archived":   average(totalAccess.ReadArchivedNanos, totalAccess.ReadArchived),
		"read_missing":    average(totalAccess.ReadMissingNanos, totalAccess.ReadMissing),
		"write_hot":       average(totalAccess.WriteHotNanos, totalAccess.WriteHot),
		"write_archived":  average(totalAccess.WriteArchivedNanos, totalAccess.WriteArchived),
		"write_missing":   average(totalAccess.WriteMissingNanos, totalAccess.WriteMissing),
		"delete_hot":      average(totalAccess.DeleteHotNanos, totalAccess.DeleteHot),
		"delete_archived": average(totalAccess.DeleteArchivedNanos, totalAccess.DeleteArchived),
		"delete_missing":  average(totalAccess.DeleteMissingNanos, totalAccess.DeleteMissing),
	}
	hotMutationAverage := average(
		totalAccess.WriteHotNanos+totalAccess.DeleteHotNanos,
		totalAccess.WriteHot+totalAccess.DeleteHot,
	)
	coldMutationAverage := average(
		totalAccess.WriteArchivedNanos+totalAccess.DeleteArchivedNanos,
		totalAccess.WriteArchived+totalAccess.DeleteArchived,
	)
	coldMaintenanceEstimate := time.Duration(0)
	if coldMutationAverage > hotMutationAverage {
		coldMaintenanceEstimate = time.Duration((coldMutationAverage - hotMutationAverage) * float64(updateDiag.ArchivePromotionHits))
	}
	operationsExcludingColdEstimate := totalOpsDur - coldMaintenanceEstimate
	if operationsExcludingColdEstimate < 0 {
		operationsExcludingColdEstimate = 0
	}
	// Close the final open redemption-proof block so its total joins the
	// global ranking before the summary reads the reservoirs.
	if proofBlockOpen {
		proofBlockTotal++
		if proofBlockAcc > proofBlockMax {
			proofBlockMax = proofBlockAcc
		}
		proofReservoirAdd(&proofBlockReservoir, proofBlockAcc, proofBlockTotal, 1<<19, proofRng)
		proofBlockOpen = false
	}
	// E2 v6 note: the exact per-event histograms (single-proof size/latency,
	// filter-probe latency) and the per-block resurrection counts follow the
	// existing trailing-open-block convention — only completed windows are
	// reported in the CSV, and the final window's flush has already written
	// its row, so events recorded after the last flushWindow and the trailing
	// open block's counts are deliberately not re-reported here (they remain
	// in the lifetime maxima via the diagnostics atomics).
	proofRedemptionSorted := sortedInt64Copy(proofRedemptionReservoir)
	proofBlockGlobalSorted := sortedInt64Copy(proofBlockReservoir)
	_, _, proofRedemptionMaxExact, _ := MPTRedemptionProofStats()
	proofVerifyTotalCount, proofVerifyTotalNanos := MPTRedemptionVerifyStats()
	verificationToExclude := time.Duration(0)
	if *traceStressMPTVerifyAtCommit {
		verificationToExclude = time.Duration(proofVerifyTotalNanos)
	}
	summary := map[string]any{
		"status":                                "completed",
		"finished_at":                           time.Now().Format(time.RFC3339),
		"elapsed_ms":                            float64(time.Since(started)) / float64(time.Millisecond),
		"operations":                            totalCounts.total(),
		"batches":                               totalBatches,
		"prunes_done":                           prunesDone,
		"archive_round_domains":                 archiveRoundDomains,
		"archive_round_completed":               prunesDone / archiveRoundDomains,
		"archive_round_progress":                prunesDone % archiveRoundDomains,
		"requested_operations":                  *traceStressOps,
		"first_block":                           selection.firstBlock,
		"last_block":                            selection.lastBlock,
		"block_count":                           actualBlockCount,
		"counts":                                totalCounts,
		"executed_gets":                         totalGets,
		"executed_puts":                         totalPuts,
		"executed_deletes":                      totalDeletes,
		"parse_ms":                              float64(totalParse) / float64(time.Millisecond),
		"operations_ms":                         float64(totalOpsDur) / float64(time.Millisecond),
		"prune_launch_ms":                       float64(totalPrune) / float64(time.Millisecond),
		"commit_ms":                             float64(totalCommit) / float64(time.Millisecond),
		"db_write_ms":                           float64(totalWrite) / float64(time.Millisecond),
		"cold_maintenance_ms":                   float64(totalColdMaintenance) / float64(time.Millisecond),
		"cold_maintenance_estimate_ms":          float64(coldMaintenanceEstimate) / float64(time.Millisecond),
		"operations_excluding_cold_estimate_ms": float64(operationsExcludingColdEstimate) / float64(time.Millisecond),
		"measured_ops_excluding_cold_estimate_per_s": opsPerSecond(totalCounts.total(), operationsExcludingColdEstimate+totalCommit+totalWrite-verificationToExclude),
		"measured_ops_per_s":                         opsPerSecond(totalCounts.total(), totalOpsDur+totalCommit+totalWrite-verificationToExclude),
		"last_root":                                  common.BytesToHash(lastRoot).Hex(),
		"state_bytes":                                finalSystemBytes,
		"full_node_db_bytes":                         finalFullNodeDBBytes,
		"archive_db_bytes":                           finalArchiveDBBytes,
		"archive_bytes":                              LastCommitDiagnostics().MPTArchivePayloadBytes,
		"system_total_bytes":                         finalSystemBytes,
		"stem_cache":                                 cache,
		"access_sample":                              totalAccess,
		"access_sample_every":                        *traceStressAccessSampleEvery,
		"access_sample_ms":                           float64(totalAccessSample) / float64(time.Millisecond),
		"access_average_ns":                          accessAverageNanos,
		"hot_read_hit_rate":                          hotReadRate,
		"read_archive_share":                         readArchiveShare,
		"archive_filter_runtime": map[string]any{
			"lookups":         workloadFilter.Lookups,
			"negatives":       workloadFilter.Negatives,
			"positives":       workloadFilter.Positives,
			"false_positives": workloadFilter.FalsePositives,
			"fpr":             runtimeFilterFPR,
		},
		"archive_read_promotion": map[string]any{
			"calls":         updateDiag.ArchiveReadPromotionCalls,
			"hits":          updateDiag.ArchiveReadPromotionHits,
			"time_ms":       float64(time.Duration(updateDiag.ArchiveReadPromotionNanos)) / float64(time.Millisecond),
			"in_operations": true,
			"in_commit":     false,
		},
		// Redemption proof = the data the archive node hands the full node
		// for verification: bucket payload record + stub side record. The
		// full node's ECMH recompute audit runs on a sample
		// (MPT_REDEMPTION_VERIFY_SAMPLE, default 1000) and reports latency.
		// Run-long percentiles come from uniform reservoirs (estimates);
		// per-window percentiles in the CSV are exact; maxes are exact.
		"redemption_proof": map[string]any{
			"definition":              "bucket payload record + stub side record (ECMH audit payload)",
			"bytes_total_scope":       "per-redemption traffic; repeated buckets are counted again here",
			"redemptions":             proofLastCount,
			"bytes_total":             proofLastBytes,
			"redemption_p50_bytes":    percentileInt64(proofRedemptionSorted, .50),
			"redemption_p95_bytes":    percentileInt64(proofRedemptionSorted, .95),
			"redemption_p99_bytes":    percentileInt64(proofRedemptionSorted, .99),
			"redemption_max_bytes":    proofRedemptionMaxExact,
			"blocks_with_redemptions": proofBlockTotal,
			"block_p50_bytes":         percentileInt64(proofBlockGlobalSorted, .50),
			"block_p95_bytes":         percentileInt64(proofBlockGlobalSorted, .95),
			"block_p99_bytes":         percentileInt64(proofBlockGlobalSorted, .99),
			"block_max_bytes":         proofBlockMax,
			"verify_samples": func() any {
				if *traceStressMPTVerifyAtCommit {
					return nil
				}
				return proofVerifyTotalCount
			}(),
			"verify_distinct_buckets": func() any {
				if *traceStressMPTVerifyAtCommit {
					return proofVerifyTotalCount
				}
				return nil
			}(),
			"verify_time_scope": func() string {
				if *traceStressMPTVerifyAtCommit {
					return "sum of per-block parallel-audit wall times"
				}
				return "sum of sampled in-Get audit times"
			}(),
			"verify_ms_total": float64(proofVerifyTotalNanos) / float64(time.Millisecond),
			"verify_avg_us": func() any {
				if *traceStressMPTVerifyAtCommit {
					return nil // Parallel wall time is not an individual bucket latency.
				}
				return traceStressRatio(proofVerifyTotalNanos, proofVerifyTotalCount) / 1e3
			}(),
			"verify_wall_amortized_us_per_bucket": func() any {
				if *traceStressMPTVerifyAtCommit {
					return traceStressRatio(proofVerifyTotalNanos, proofVerifyTotalCount) / 1e3
				}
				return nil
			}(),
		},
		"stem_path_diagnostics": updateDiag,
		"stub_side_records": map[string]any{
			"staged_bytes": LastCommitDiagnostics().MPTStagedStubMetaBytes,
			"fetches":      LastCommitDiagnostics().MPTStubMetaFetches,
			"fetch_bytes":  LastCommitDiagnostics().MPTStubMetaFetchBytes,
		},
		"stub_node_stubs_max": LastCommitDiagnostics().MPTStubNodeMax,
		"stub_node_stubs_avg": traceStressRatio(LastCommitDiagnostics().MPTStubNodeSum, LastCommitDiagnostics().MPTStubNodeEvents),
		"stub_node_events":    LastCommitDiagnostics().MPTStubNodeEvents,
		// Write-path no-probe ruling tripwire: >0 means the workload
		// contained blind writes to archived keys (violates the EVM
		// read-before-write premise); must be 0 for the run to be valid.
		"mpt_duplicate_archived_keys": MPTDuplicateArchivedKeys(),
		"final_stats_ms":              float64(finalStatsDur) / float64(time.Millisecond),
		"final_stats":                 finalStats,
	}
	if *traceStressMPTVerifyAtCommit {
		if uint64(len(allBlockRedemptions)) != actualBlockCount || len(allBlockUniqueProofBytes) != len(allBlockRedemptions) {
			t.Fatalf("block proof statistics cover %d of %d blocks", len(allBlockRedemptions), actualBlockCount)
		}
		summary["block_redemption_distributions"] = map[string]any{
			"definition":              "one row per source block; proof bytes sum each distinct bucket's pre-redemption payload and stub metadata once within that block",
			"quantiles":               "exact nearest-rank over the stated block population",
			"all_blocks":              blockDistributionSummary(allBlockRedemptions, allBlockUniqueProofBytes),
			"blocks_with_redemptions": blockDistributionSummary(positiveBlockRedemptions, positiveBlockUniqueProofBytes),
			"distinct_buckets_total":  totalUniqueProofBuckets,
		}
	}
	// G2 accounting: with the path backend the LevelDB directory size misses
	// nodes still held in pathdb's write buffer and diff layers, so report the
	// written/buffered/diff components and their sum as the active-layer total.
	if ps, ok := hotBackend.(interface {
		PathStats() (written, writes, buffered, diff int64)
	}); ok {
		written, writes, buffered, diff := ps.PathStats()
		summary["path_hot_written_bytes"] = written
		summary["path_hot_writes"] = writes
		summary["path_hot_buffered_bytes"] = buffered
		summary["path_hot_diff_bytes"] = diff
		summary["active_layer_bytes_total"] = written + buffered + diff
	}
	if OpTraceProbe != nil && os.Getenv("MPT_OP_TRACE") != "" {
		summary["op_trace_total"] = OpTraceProbe()
	}
	if err := writeTraceStressJSON(filepath.Join(resultsDir, "summary.json"), summary); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceStressJSON(filepath.Join(*traceStressBaseDir, "run_status.json"), map[string]any{"status": "completed", "finished_at": time.Now().Format(time.RFC3339), "operations": totalCounts.total()}); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[TRACE_STRESS] DONE ops=%d batches=%d root=%s full_db=%d archive_db=%d elapsed=%s\n", totalCounts.total(), totalBatches, common.BytesToHash(lastRoot).Hex(), finalFullNodeDBBytes, finalArchiveDBBytes, time.Since(started))
}

// TestArchiveStemTraceInspectStem reconstructs one split stem directly from a
// preserved path-storage run. It is intentionally read-only so a failed formal
// replay can be diagnosed without changing or compacting the evidence DB.
func TestArchiveStemTraceInspectStem(t *testing.T) {
	if *traceInspectBaseDir == "" || *traceInspectStem == "" {
		t.Skip("set -traceInspectBaseDir and -traceInspectStem to inspect a preserved run")
	}
	stemKey, err := hex.DecodeString(strings.TrimPrefix(*traceInspectStem, "0x"))
	if err != nil || len(stemKey) != StemSize {
		t.Fatalf("traceInspectStem must be a %d-byte hex stem: decoded=%d err=%v", StemSize, len(stemKey), err)
	}
	stateDir := filepath.Join(*traceInspectBaseDir, "state_db")
	db, err := leveldb.New(stateDir, 512, 256, "trace-stem-inspect", true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	hasher := NewPooledKeccakHasher()
	rootRecord, err := db.Get(pathRootBranchKey(0, 0))
	if err != nil {
		t.Fatalf("read committed root record: %v", err)
	}
	if len(rootRecord) != rootBranchSize || rootRecord[0] != RootBranchHeader || rootRecord[1] != 0 {
		t.Fatalf("invalid committed root record: len=%d header=%x depth=%d", len(rootRecord), rootRecord[0], rootRecord[1])
	}
	root := hasher.Hash(rootRecord)
	config := DefaultConfig()
	config.ShardDepth = *traceStressShardDepth
	config.NodeStorageScheme = NodeStoragePath
	config.StemMode = *traceStressStemMode
	config.PhysicalDelete = false
	config.CuckooBuckets = *traceStressCuckooBuckets
	config.CuckooSlots = *traceStressCuckooSlots
	backend := NewTrie(nil, &stressDBAdapter{db}, hasher, config, true)
	if err := backend.Load(root); err != nil {
		t.Fatalf("load committed root %x: %v", root, err)
	}
	shard, err := backend.getOrCreateShard(backend.GetShardID(stemKey))
	if err != nil {
		t.Fatalf("load target shard: %v", err)
	}
	if shard.root == nil && len(shard.rootHash) > 0 {
		shard.root, err = shard.loadNode(shard.rootHash)
		if err != nil {
			t.Fatalf("load target shard root: %v", err)
		}
	}

	result := map[string]any{
		"base_dir":       *traceInspectBaseDir,
		"state_dir":      stateDir,
		"committed_root": common.BytesToHash(root).Hex(),
		"stem":           hex.EncodeToString(stemKey),
		"shard":          backend.GetShardID(stemKey),
	}
	memberships, presencePath, membershipErr := traceInspectStemMemberships(shard, stemKey)
	result["membership_error"] = errorString(membershipErr)
	result["memberships"] = memberships
	result["presence_path"] = presencePath
	valueRef, fromArchive, refErr := backend.GetValueRef(stemKey)
	result["outer_from_archive"] = fromArchive
	result["outer_error"] = errorString(refErr)
	result["outer_value_ref"] = hex.EncodeToString(valueRef)

	payload, flatErr := backend.GetFlatValue(stemKey)
	result["flat_error"] = errorString(flatErr)
	result["flat_metadata"] = hex.EncodeToString(payload)
	if flatErr == nil && len(payload) >= len(stemMetadataMagic) && bytes.Equal(payload[:len(stemMetadataMagic)], stemMetadataMagic[:]) {
		stem, decodeErr := decodeStemMetadata(payload)
		result["metadata_error"] = errorString(decodeErr)
		if decodeErr == nil {
			values := make(map[string]string)
			missing := make([]int, 0)
			for i := 0; i < StemSuffixCount; i++ {
				suffix := byte(i)
				if !stem.has(suffix) {
					continue
				}
				value, err := backend.GetFlatValue(joinStemKey(stemKey, suffix))
				if err != nil {
					missing = append(missing, i)
					continue
				}
				stem.values[suffix] = bytes.Clone(value)
				values[strconv.Itoa(i)] = hex.EncodeToString(value)
			}
			stem.commitment = buildStemCommitment(stem, hasher, nil)
			flatRoot := stem.ValuesRoot(hasher)
			result["suffix_count"] = stem.Len()
			result["suffix_values"] = values
			result["missing_suffixes"] = missing
			result["flat_values_root"] = hex.EncodeToString(flatRoot)
			result["roots_equal"] = bytes.Equal(flatRoot, valueRef)
		}
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[TRACE_STEM_INSPECT] %s\n", encoded)
	if *traceInspectOutput != "" {
		if err := os.WriteFile(*traceInspectOutput, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type traceInspectMembership struct {
	Kind              string `json:"kind"`
	Location          string `json:"location"`
	ValueRef          string `json:"value_ref"`
	NodePath          string `json:"node_path,omitempty"`
	NodePathBits      int    `json:"node_path_bits,omitempty"`
	BucketPath        string `json:"bucket_path,omitempty"`
	BucketPathBits    int    `json:"bucket_path_bits,omitempty"`
	BucketStoragePath string `json:"bucket_storage_path,omitempty"`
	BucketStorageBits int    `json:"bucket_storage_bits,omitempty"`
	BucketCount       uint64 `json:"bucket_count,omitempty"`
	BucketKeyIndex    int    `json:"bucket_key_index,omitempty"`
}

type traceInspectPresence struct {
	Location         string `json:"location"`
	NodePath         string `json:"node_path"`
	NodePathBits     int    `json:"node_path_bits"`
	Epoch            byte   `json:"epoch"`
	ArchivePresent   bool   `json:"archive_present"`
	ArchivePresentOK bool   `json:"archive_present_known"`
	SelectedBit      int    `json:"selected_bit,omitempty"`
	ChildEpoch       byte   `json:"child_epoch,omitempty"`
	ChildPresent     bool   `json:"child_archive_present,omitempty"`
	ChildPresentOK   bool   `json:"child_archive_present_known,omitempty"`
}

func traceInspectStemMemberships(shard *Shard, key []byte) ([]traceInspectMembership, []traceInspectPresence, error) {
	if shard == nil {
		return nil, nil, errors.New("nil shard")
	}
	prefix, prefixBits := shard.getShardPrefix()
	var memberships []traceInspectMembership
	var presencePath []traceInspectPresence
	var walk func(Node, []byte, int, string) error
	inspectBucket := func(bucket *ArchiveBucketNode, location string) error {
		if bucket == nil {
			return nil
		}
		keys, err := shard.bucketKeys(bucket)
		if err != nil {
			return fmt.Errorf("%s bucket keys: %w", location, err)
		}
		for index, item := range keys {
			fullKey, fullBits := shard.prependPath(item.Suffix, item.SuffixBits, bucket.Path, bucket.PathBits)
			if fullBits != len(key)*8 || !bytes.Equal(fullKey, key) {
				continue
			}
			storagePath, storageBits := bucket.StoragePath()
			memberships = append(memberships, traceInspectMembership{
				Kind:              "archive",
				Location:          location,
				ValueRef:          hex.EncodeToString(item.ValueRef),
				BucketPath:        hex.EncodeToString(bucket.Path),
				BucketPathBits:    bucket.PathBits,
				BucketStoragePath: hex.EncodeToString(storagePath),
				BucketStorageBits: storageBits,
				BucketCount:       bucket.Count,
				BucketKeyIndex:    index,
			})
		}
		return nil
	}
	walk = func(node Node, nodePrefix []byte, nodeBits int, location string) error {
		if node == nil {
			return nil
		}
		switch n := node.(type) {
		case *LeafNode:
			fullKey, fullBits := shard.prependPath(n.Path, n.PathBits, nodePrefix, nodeBits)
			if fullBits == len(key)*8 && bytes.Equal(fullKey, key) {
				storagePath, storageBits := n.StoragePath()
				memberships = append(memberships, traceInspectMembership{
					Kind:         "hot",
					Location:     location,
					ValueRef:     hex.EncodeToString(n.ValueHash),
					NodePath:     hex.EncodeToString(storagePath),
					NodePathBits: storageBits,
				})
			}
			return nil
		case *ArchiveBucketNode:
			return inspectBucket(n, location)
		case *InternalNode:
			present, presentOK := shard.subtreeArchivePresence(n)
			entry := traceInspectPresence{
				Location:         location,
				NodePath:         hex.EncodeToString(nodePrefix),
				NodePathBits:     nodeBits,
				Epoch:            n.Epoch(),
				ArchivePresent:   present,
				ArchivePresentOK: presentOK,
				SelectedBit:      -1,
			}
			for index, bucket := range n.StubList {
				if err := inspectBucket(bucket, fmt.Sprintf("%s/stub[%d]", location, index)); err != nil {
					return err
				}
			}
			pathPrefix, pathBits := nodePrefix, nodeBits
			if n.PathBits > 0 {
				matched := shard.commonPrefixLen(n.Path, n.PathBits, key, nodeBits)
				if matched != n.PathBits {
					presencePath = append(presencePath, entry)
					return nil
				}
				pathPrefix, pathBits = shard.prependPath(n.Path, n.PathBits, nodePrefix, nodeBits)
			}
			bit := shard.getBit(key, pathBits)
			entry.SelectedBit = int(bit)
			var child Node
			var childHash []byte
			if bit == 0 {
				child, childHash, entry.ChildEpoch = n.Left, n.LeftHash, n.LeftEpoch
			} else {
				child, childHash, entry.ChildEpoch = n.Right, n.RightHash, n.RightEpoch
			}
			entry.ChildPresent, entry.ChildPresentOK = shard.childArchivePresence(child, entry.ChildEpoch, len(childHash) > 0)
			presencePath = append(presencePath, entry)
			if child == nil && len(childHash) > 0 {
				loaded, err := shard.loadChildNode(n, bit, childHash)
				if err != nil {
					return fmt.Errorf("%s child %d: %w", location, bit, err)
				}
				child = loaded
			}
			childPrefix, childBits := shard.appendBit(pathPrefix, pathBits, bit)
			return walk(child, childPrefix, childBits, fmt.Sprintf("%s/%d", location, bit))
		default:
			return fmt.Errorf("%s: unknown node type %T", location, node)
		}
	}
	if err := walk(shard.root, prefix, prefixBits, "root"); err != nil {
		return memberships, presencePath, err
	}
	return memberships, presencePath, nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestArchiveStemTraceStorageStats opens a completed trace-stress state DB and
// reports reachable logical bytes separately for active and archived records.
// Logical byte counts are not LevelDB LSM physical-file sizes.
func TestArchiveStemTraceStorageStats(t *testing.T) {
	if *traceStatsBaseDir == "" || *traceStatsRoot == "" || *traceStatsOutput == "" {
		t.Skip("set -traceStatsBaseDir, -traceStatsRoot, and -traceStatsOutput to inspect a completed run")
	}
	stateDir := filepath.Join(*traceStatsBaseDir, "state_db")
	if info, err := os.Stat(stateDir); err != nil || !info.IsDir() {
		t.Fatalf("state directory does not exist: %s", stateDir)
	}
	root := common.HexToHash(*traceStatsRoot)
	if _, err := os.Stat(*traceStatsOutput); err == nil {
		t.Fatalf("output already exists: %s", *traceStatsOutput)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	db, err := leveldb.New(stateDir, 512, 256, "trace-storage-stats", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfig()
	config.ShardDepth = *traceStressShardDepth
	config.NodeStorageScheme = NodeStoragePath
	config.StemMode = *traceStressStemMode
	config.CuckooBuckets = *traceStressCuckooBuckets
	config.CuckooSlots = *traceStressCuckooSlots
	backend := NewTrie(root.Bytes(), &stressDBAdapter{db}, NewPooledKeccakHasher(), config, true)

	started := time.Now()
	stats := backend.StatsWithDiagnostics(0, 0, true)
	result := map[string]any{
		"status":        "completed",
		"base_dir":      *traceStatsBaseDir,
		"root":          root.Hex(),
		"state_bytes":   getDirSize(stateDir),
		"stats_ms":      float64(time.Since(started)) / float64(time.Millisecond),
		"stats":         stats,
		"byte_scope":    "reachable logical database record bytes; excludes LevelDB WAL, obsolete versions, and compaction overhead",
		"compare_scope": "ActiveOnlyLogicalBytes is the non-archive comparison value; ArchivedPayloadLogicalBytes is diagnostic-only",
		"command":       strings.Join(os.Args, " "),
	}
	if err := writeTraceStressJSON(*traceStatsOutput, result); err != nil {
		t.Fatal(err)
	}
	if !stats.StorageBreakdownValid {
		t.Fatalf("storage breakdown invalid: %d read failures", stats.StorageBreakdownReadFailures)
	}
}

package tree

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/shirou/gopsutil/process"
)

var (
	traceCompareInputDir          = flag.String("traceCompareInputDir", "", "Directory containing state_access_trace_*.csv.gz files")
	traceCompareEngine            = flag.String("traceCompareEngine", "mpt", "Comparison engine: mpt or verkle")
	traceCompareBaseDir           = flag.String("traceCompareBaseDir", "", "Fresh output directory for TestTrieTraceCompare")
	traceCompareOps               = flag.Int64("traceCompareOps", 100000000, "Number of ordered trace operations")
	traceCompareStartBlock        = flag.Uint64("traceCompareStartBlock", 0, "First inclusive trace block; zero starts at the first selected record")
	traceCompareEndBlock          = flag.Uint64("traceCompareEndBlock", 0, "Last inclusive trace block; zero disables an explicit end boundary")
	traceCompareBlocks            = flag.Uint64("traceCompareBlocks", 0, "Replay this many blocks from the first selected block; incompatible with traceCompareEndBlock")
	traceCompareBatchSize         = flag.Int("traceCompareBatchSize", 4000, "Trace operations per root/commit cycle")
	traceCompareMetricsBatches    = flag.Int("traceCompareMetricsBatches", 2500, "Commit batches per metrics row")
	traceCompareTimingSampleEvery = flag.Int64("traceCompareTimingSampleEvery", 1000, "Sample operation latency every N operations; zero disables")
	traceCompareStartFile         = flag.Int("traceCompareStartFile", 0, "Zero-based sorted trace shard index to start from")
	traceCompareFileLimit         = flag.Int("traceCompareFileLimit", 0, "Maximum trace shards to read; zero uses all remaining shards")
)

const traceCompareHeader = "block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len"

type traceCompareKind byte

const (
	traceCompareGet traceCompareKind = iota
	traceComparePut
	traceCompareDelete
)

type traceCompareOp struct {
	key       []byte
	value     []byte
	kind      traceCompareKind
	operation string
	block     uint64
}

type traceCompareReader struct {
	files     []string
	fileIndex int
	file      *os.File
	gz        *gzip.Reader
	csv       *csv.Reader
	current   string
}

func newTraceCompareReader(inputDir string, startFile, fileLimit int) (*traceCompareReader, error) {
	files, err := filepath.Glob(filepath.Join(inputDir, "state_access_trace_*.csv.gz"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if startFile < 0 || startFile >= len(files) {
		return nil, fmt.Errorf("traceCompareStartFile=%d outside %d trace shards", startFile, len(files))
	}
	files = files[startFile:]
	if fileLimit > 0 && fileLimit < len(files) {
		files = files[:fileLimit]
	}
	return &traceCompareReader{files: files}, nil
}

func (r *traceCompareReader) closeCurrent() error {
	var errs []error
	if r.gz != nil {
		errs = append(errs, r.gz.Close())
	}
	if r.file != nil {
		errs = append(errs, r.file.Close())
	}
	r.file, r.gz, r.csv = nil, nil, nil
	return errors.Join(errs...)
}

func (r *traceCompareReader) Close() error {
	return r.closeCurrent()
}

func (r *traceCompareReader) openNext() error {
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
	if strings.Join(header, ",") != traceCompareHeader {
		gz.Close()
		f.Close()
		return fmt.Errorf("unexpected trace header in %s: %q", path, strings.Join(header, ","))
	}
	r.file, r.gz, r.csv, r.current = f, gz, reader, filepath.Base(path)
	return nil
}

func (r *traceCompareReader) Read() (traceCompareOp, error) {
	for {
		if r.csv == nil {
			if err := r.openNext(); err != nil {
				return traceCompareOp{}, err
			}
		}
		record, err := r.csv.Read()
		if errors.Is(err, io.EOF) {
			if err := r.closeCurrent(); err != nil {
				return traceCompareOp{}, err
			}
			continue
		}
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("read %s: %w", r.current, err)
		}
		op, err := parseTraceCompareRecord(record)
		if err != nil {
			line, _ := r.csv.FieldPos(0)
			return traceCompareOp{}, fmt.Errorf("parse %s line %d: %w", r.current, line, err)
		}
		return op, nil
	}
}

var traceCompareZeroStorageHash = crypto.Keccak256Hash(make([]byte, common.HashLength)).Hex()

func parseTraceCompareRecord(record []string) (traceCompareOp, error) {
	if len(record) != 9 {
		return traceCompareOp{}, fmt.Errorf("want 9 fields, got %d", len(record))
	}
	block, err := strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return traceCompareOp{}, fmt.Errorf("block: %w", err)
	}
	if !common.IsHexAddress(record[4]) {
		return traceCompareOp{}, fmt.Errorf("invalid address %q", record[4])
	}
	address := common.HexToAddress(record[4])

	var key []byte
	switch record[3] {
	case "account":
		key = trieutils.BinaryTreeBasicDataKey(address)
	case "storage":
		slot, err := decodeTraceCompareHash(record[5])
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("storage slot: %w", err)
		}
		key = trieutils.BinaryTreeStorageSlotKey(address, slot)
	case "code":
		chunk, err := strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return traceCompareOp{}, fmt.Errorf("code chunk: %w", err)
		}
		key = trieutils.BinaryTreeCodeChunkKey(address, chunk)
	default:
		return traceCompareOp{}, fmt.Errorf("unsupported object type %q", record[3])
	}

	op := traceCompareOp{key: key, operation: record[6], block: block}
	switch record[6] {
	case "read", "touch":
		op.kind = traceCompareGet
	case "delete":
		op.kind = traceCompareDelete
	case "write", "create":
		if record[3] == "storage" && strings.EqualFold(record[7], traceCompareZeroStorageHash) {
			op.kind = traceCompareDelete
			break
		}
		op.kind = traceComparePut
		if record[7] != "" {
			op.value, err = decodeTraceCompareHash(record[7])
			if err != nil {
				return traceCompareOp{}, fmt.Errorf("value hash: %w", err)
			}
		} else {
			op.value = crypto.Keccak256(key, []byte(record[6]))
		}
	default:
		return traceCompareOp{}, fmt.Errorf("unsupported operation %q", record[6])
	}
	return op, nil
}

func decodeTraceCompareHash(input string) ([]byte, error) {
	input = strings.TrimPrefix(input, "0x")
	if len(input) != common.HashLength*2 {
		return nil, fmt.Errorf("want 32-byte hash, got %d hex chars", len(input))
	}
	return hex.DecodeString(input)
}

type traceCompareCounts struct {
	Reads   int64 `json:"reads"`
	Touches int64 `json:"touches"`
	Writes  int64 `json:"writes"`
	Creates int64 `json:"creates"`
	Deletes int64 `json:"deletes"`
}

func (c *traceCompareCounts) add(operation string) {
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

func (c traceCompareCounts) total() int64 {
	return c.Reads + c.Touches + c.Writes + c.Creates + c.Deletes
}

type traceCompareSelection struct {
	startBlock      uint64
	endBlock        uint64
	blockCount      uint64
	started         bool
	firstBlock      uint64
	lastBlock       uint64
	reachedEOF      bool
	reachedEndBlock bool
}

func (s *traceCompareSelection) collect(reader *traceCompareReader, limit int, remaining int64) ([]traceCompareOp, bool, error) {
	ops := make([]traceCompareOp, 0, limit)
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

type traceCompareWindow struct {
	counts     traceCompareCounts
	parse      time.Duration
	operations time.Duration
	root       time.Duration
	dbWrite    time.Duration
	batchWall  []time.Duration
	firstBlock uint64
	lastBlock  uint64
	timing     traceCompareTimingStats
}

type traceCompareTimingStats struct {
	Samples     int64 `json:"samples"`
	Reads       int64 `json:"reads"`
	ReadNanos   int64 `json:"read_nanos"`
	Writes      int64 `json:"writes"`
	WriteNanos  int64 `json:"write_nanos"`
	Deletes     int64 `json:"deletes"`
	DeleteNanos int64 `json:"delete_nanos"`
}

func (s *traceCompareTimingStats) add(kind traceCompareKind, elapsed time.Duration) {
	s.Samples++
	switch kind {
	case traceCompareGet:
		s.Reads++
		s.ReadNanos += elapsed.Nanoseconds()
	case traceComparePut:
		s.Writes++
		s.WriteNanos += elapsed.Nanoseconds()
	case traceCompareDelete:
		s.Deletes++
		s.DeleteNanos += elapsed.Nanoseconds()
	}
}

func (s *traceCompareTimingStats) merge(other traceCompareTimingStats) {
	s.Samples += other.Samples
	s.Reads += other.Reads
	s.ReadNanos += other.ReadNanos
	s.Writes += other.Writes
	s.WriteNanos += other.WriteNanos
	s.Deletes += other.Deletes
	s.DeleteNanos += other.DeleteNanos
}

func traceCompareMS(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64)
}

func traceComparePercentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[int(p*float64(len(ordered)-1))]
}

func writeTraceCompareJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func TestTraceCompareSelection(t *testing.T) {
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
	if err := w.Write(strings.Split(traceCompareHeader, ",")); err != nil {
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

	r, err := newTraceCompareReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	selection := traceCompareSelection{startBlock: 2, endBlock: 2}
	ops, complete, err := selection.collect(r, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || len(ops) != 2 || selection.firstBlock != 2 || selection.lastBlock != 2 || !selection.reachedEndBlock {
		t.Fatalf("unexpected block selection: complete=%v ops=%d first=%d last=%d reachedEnd=%v", complete, len(ops), selection.firstBlock, selection.lastBlock, selection.reachedEndBlock)
	}

	r2, err := newTraceCompareReader(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	selection = traceCompareSelection{blockCount: 2}
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

// TestTrieTraceCompare replays the same ordered state-access trace used by the
// ASCT benchmark against either MPT or Verkle. All engines consume the same
// 32-byte structural keys, values, operation order, and 4000-op commit cadence.
func TestTrieTraceCompare(t *testing.T) {
	engine := strings.ToLower(*traceCompareEngine)
	if engine != "mpt" && engine != "verkle" {
		t.Fatalf("traceCompareEngine must be mpt or verkle, got %q", *traceCompareEngine)
	}
	if *traceCompareInputDir == "" {
		t.Skip("set -traceCompareInputDir to run the trace comparison")
	}
	if *traceCompareBaseDir == "" {
		t.Fatal("-traceCompareBaseDir must name a fresh output directory")
	}
	blockBoundary := *traceCompareEndBlock > 0 || *traceCompareBlocks > 0
	if *traceCompareOps <= 0 && !blockBoundary {
		t.Fatal("traceCompareOps must be positive when no end block or block count is set")
	}
	if *traceCompareOps == 0 && !blockBoundary {
		t.Fatal("unlimited traceCompareOps requires traceCompareEndBlock or traceCompareBlocks")
	}
	if blockBoundary && *traceCompareOps != 0 {
		t.Fatal("use traceCompareOps=0 with traceCompareEndBlock or traceCompareBlocks so a block range is never truncated")
	}
	if *traceCompareEndBlock > 0 && *traceCompareBlocks > 0 {
		t.Fatal("traceCompareEndBlock and traceCompareBlocks are mutually exclusive")
	}
	if *traceCompareStartBlock > 0 && *traceCompareEndBlock > 0 && *traceCompareStartBlock > *traceCompareEndBlock {
		t.Fatal("traceCompareStartBlock cannot exceed traceCompareEndBlock")
	}
	if *traceCompareBatchSize <= 0 || *traceCompareMetricsBatches <= 0 {
		t.Fatal("traceCompareBatchSize and traceCompareMetricsBatches must be positive")
	}
	if *traceCompareTimingSampleEvery < 0 {
		t.Fatal("traceCompareTimingSampleEvery cannot be negative")
	}
	if _, err := os.Stat(*traceCompareBaseDir); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Fatalf("traceCompareBaseDir already exists: %s", *traceCompareBaseDir)
		}
		t.Fatalf("stat traceCompareBaseDir: %v", err)
	}

	stateDir := filepath.Join(*traceCompareBaseDir, "state_db")
	resultsDir := filepath.Join(*traceCompareBaseDir, "results")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	reader, err := newTraceCompareReader(*traceCompareInputDir, *traceCompareStartFile, *traceCompareFileLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	ldb, err := leveldb.New(stateDir, 512, 256, "trace-compare", false)
	if err != nil {
		t.Fatal(err)
	}
	defer ldb.Close()

	mdb := ethdb.WrapWithStats(ldb)
	diskDB := rawdb.NewDatabase(mdb)
	cacheConfig := core.DefaultCacheConfigWithScheme(rawdb.PathScheme)
	cacheConfig.SnapshotLimit = 0
	// PathDB.Commit caps to this layer count. Zero forces every batch through
	// diffToDisk, matching the ASCT driver's per-batch durable commit cadence.
	previousLayerCount := common.VerkleLayerCount
	common.VerkleLayerCount = 0
	defer func() { common.VerkleLayerCount = previousLayerCount }()
	trieDB := triedb.NewDatabase(diskDB, cacheConfig.TriedbConfig(engine == "verkle"))
	defer trieDB.Close()

	lastRoot := types.EmptyRootHash
	var pointCache *trieutils.PointCache
	var mpt *trie.Trie
	var verkle *trie.VerkleTrie
	if engine == "mpt" {
		mpt, err = trie.New(trie.TrieID(lastRoot), trieDB)
	} else {
		lastRoot = types.EmptyVerkleHash
		pointCache = trieutils.NewPointCache(1024)
		verkle, err = trie.NewVerkleTrie(lastRoot, trieDB, pointCache)
	}
	if err != nil {
		t.Fatal(err)
	}

	metadata := map[string]any{
		"generated_at":                time.Now().Format(time.RFC3339),
		"engine":                      engine,
		"input_dir":                   *traceCompareInputDir,
		"source_files":                reader.files,
		"operations":                  *traceCompareOps,
		"start_block":                 *traceCompareStartBlock,
		"end_block":                   *traceCompareEndBlock,
		"block_count":                 *traceCompareBlocks,
		"batch_size":                  *traceCompareBatchSize,
		"metrics_batches":             *traceCompareMetricsBatches,
		"timing_sample_every":         *traceCompareTimingSampleEvery,
		"node_storage":                rawdb.PathScheme,
		"path_db_diff_layers":         0,
		"key_rule":                    "BinaryTree/Verkle structural key for account, storage, and code",
		"value_rule":                  "32-byte value_hash; deterministic key/op hash when absent",
		"storage_zero_hash_is_delete": true,
		"command":                     strings.Join(os.Args, " "),
	}
	if err := writeTraceCompareJSON(filepath.Join(*traceCompareBaseDir, "metadata.json"), metadata); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceCompareJSON(filepath.Join(*traceCompareBaseDir, "run_status.json"), map[string]any{"status": "running", "started_at": time.Now().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}

	metricsPath := filepath.Join(resultsDir, engine+"_trace_stress.csv")
	metricsFile, err := os.Create(metricsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metricsFile.Close()
	metrics := csv.NewWriter(metricsFile)
	defer metrics.Flush()
	header := []string{
		"Window_Start_Batch", "Window_End_Batch", "Total_Batches", "Total_Operations", "First_Block", "Last_Block",
		"Window_Operations", "Reads", "Touches", "Writes", "Creates", "Deletes",
		"Executed_Gets", "Executed_Puts", "Executed_Deletes", "Parse_ms", "Operations_ms", "Root_ms", "DB_Write_ms",
		"Timing_Samples", "Sampled_Reads", "Sampled_Read_ms", "Sampled_Writes", "Sampled_Write_ms", "Sampled_Deletes", "Sampled_Delete_ms",
		"Measured_Wall_ms", "Operations_Per_Sec", "Batch_P50_ms", "Batch_P95_ms", "Batch_P99_ms", "Batch_Max_ms",
		"State_Bytes", "RSS_Bytes", "Heap_Bytes",
	}
	if err := metrics.Write(header); err != nil {
		t.Fatal(err)
	}
	metrics.Flush()

	proc, _ := process.NewProcess(int32(os.Getpid()))
	totalBatches := int64(0)
	selection := traceCompareSelection{
		startBlock: *traceCompareStartBlock,
		endBlock:   *traceCompareEndBlock,
		blockCount: *traceCompareBlocks,
	}
	var (
		totalCounts                                 traceCompareCounts
		window                                      traceCompareWindow
		totalTiming                                 traceCompareTimingStats
		totalParse                                  time.Duration
		totalOps                                    time.Duration
		totalRoot                                   time.Duration
		totalWrite                                  time.Duration
		executedGets, executedPuts, executedDeletes int64
		executedOperations                          int64
		started                                     = time.Now()
	)

	flushWindow := func(batchNumber int64) {
		measured := window.operations + window.root + window.dbWrite
		opsPerSec := float64(window.counts.total()) / measured.Seconds()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		var rss uint64
		if proc != nil {
			if info, err := proc.MemoryInfo(); err == nil {
				rss = info.RSS
			}
		}
		diskSize, _ := GetDirSize(stateDir)
		row := []string{
			strconv.FormatInt(batchNumber-int64(len(window.batchWall))+1, 10), strconv.FormatInt(batchNumber, 10), strconv.FormatInt(batchNumber, 10), strconv.FormatInt(totalCounts.total(), 10),
			strconv.FormatUint(window.firstBlock, 10), strconv.FormatUint(window.lastBlock, 10), strconv.FormatInt(window.counts.total(), 10),
			strconv.FormatInt(window.counts.Reads, 10), strconv.FormatInt(window.counts.Touches, 10), strconv.FormatInt(window.counts.Writes, 10), strconv.FormatInt(window.counts.Creates, 10), strconv.FormatInt(window.counts.Deletes, 10),
			strconv.FormatInt(executedGets, 10), strconv.FormatInt(executedPuts, 10), strconv.FormatInt(executedDeletes, 10),
			traceCompareMS(window.parse), traceCompareMS(window.operations), traceCompareMS(window.root), traceCompareMS(window.dbWrite), traceCompareMS(measured),
			strconv.FormatInt(window.timing.Samples, 10), strconv.FormatInt(window.timing.Reads, 10), traceCompareMS(time.Duration(window.timing.ReadNanos)),
			strconv.FormatInt(window.timing.Writes, 10), traceCompareMS(time.Duration(window.timing.WriteNanos)),
			strconv.FormatInt(window.timing.Deletes, 10), traceCompareMS(time.Duration(window.timing.DeleteNanos)),
			strconv.FormatFloat(opsPerSec, 'f', 2, 64), traceCompareMS(traceComparePercentile(window.batchWall, .50)), traceCompareMS(traceComparePercentile(window.batchWall, .95)), traceCompareMS(traceComparePercentile(window.batchWall, .99)), traceCompareMS(traceComparePercentile(window.batchWall, 1)),
			strconv.FormatInt(diskSize, 10), strconv.FormatUint(rss, 10), strconv.FormatUint(mem.HeapAlloc, 10),
		}
		if err := metrics.Write(row); err != nil {
			t.Fatal(err)
		}
		metrics.Flush()
		if err := metrics.Error(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("[TRACE_COMPARE:%s] batches=%d/%d ops=%d block=%d db=%d rate=%.0f ops/s\n", engine, batchNumber, totalBatches, totalCounts.total(), window.lastBlock, diskSize, opsPerSec)
		totalTiming.merge(window.timing)
		window = traceCompareWindow{}
	}

	for batchNumber := int64(1); ; batchNumber++ {
		parseStart := time.Now()
		remaining := *traceCompareOps - totalCounts.total()
		if *traceCompareOps == 0 {
			remaining = 0
		}
		ops, reachedBoundary, err := selection.collect(reader, *traceCompareBatchSize, remaining)
		if err != nil {
			t.Fatalf("batch %d parse: %v", batchNumber, err)
		}
		if len(ops) == 0 {
			if !selection.started {
				t.Fatalf("no trace records at or after traceCompareStartBlock=%d", *traceCompareStartBlock)
			}
			break
		}
		totalBatches = batchNumber
		window.firstBlock = ops[0].block
		window.lastBlock = ops[len(ops)-1].block
		for _, op := range ops {
			window.counts.add(op.operation)
			totalCounts.add(op.operation)
		}
		parseDur := time.Since(parseStart)
		window.parse += parseDur
		totalParse += parseDur

		batchStart := time.Now()
		opStart := time.Now()
		executeGet := func(op traceCompareOp) error {
			if engine == "mpt" {
				_, err := mpt.Get(op.key)
				return err
			}
			_, err := verkle.GetRaw(op.key)
			return err
		}
		executePut := func(op traceCompareOp) error {
			if engine == "mpt" {
				return mpt.Update(op.key, op.value)
			}
			return verkle.UpdateRaw(op.key, op.value)
		}
		executeDelete := func(op traceCompareOp) error {
			if engine == "mpt" {
				return mpt.Delete(op.key)
			}
			return verkle.DeleteRaw(op.key)
		}
		for _, op := range ops {
			sample := *traceCompareTimingSampleEvery > 0 &&
				executedOperations%*traceCompareTimingSampleEvery == 0
			executionStart := time.Now()
			switch op.kind {
			case traceCompareGet:
				executedGets++
				if err := executeGet(op); err != nil {
					t.Fatalf("batch %d %s get: %v", batchNumber, engine, err)
				}
			case traceComparePut:
				executedPuts++
				if err := executePut(op); err != nil {
					t.Fatalf("batch %d %s update: %v", batchNumber, engine, err)
				}
			case traceCompareDelete:
				executedDeletes++
				if err := executeDelete(op); err != nil {
					t.Fatalf("batch %d %s delete: %v", batchNumber, engine, err)
				}
			}
			if sample {
				window.timing.add(op.kind, time.Since(executionStart))
			}
			executedOperations++
		}
		window.operations += time.Since(opStart)
		totalOps += time.Since(opStart)

		rootStart := time.Now()
		previousRoot := lastRoot
		var nodes *trienode.NodeSet
		if engine == "mpt" {
			lastRoot, nodes = mpt.Commit(false)
		} else {
			lastRoot, nodes = verkle.Commit(false)
		}
		rootDur := time.Since(rootStart)
		window.root += rootDur
		totalRoot += rootDur

		writeStart := time.Now()
		if nodes != nil && lastRoot != previousRoot {
			stateSet := triedb.NewStateSet()
			if err := trieDB.Update(lastRoot, previousRoot, uint64(batchNumber), trienode.NewWithNodeSet(nodes), stateSet); err != nil {
				t.Fatalf("batch %d trie database update: %v", batchNumber, err)
			}
			if err := trieDB.Commit(lastRoot, false); err != nil {
				t.Fatalf("batch %d trie database commit: %v", batchNumber, err)
			}
		}
		if err := diskDB.Put(lastRootKey, lastRoot.Bytes()); err != nil {
			t.Fatalf("batch %d save root: %v", batchNumber, err)
		}
		writeDur := time.Since(writeStart)
		window.dbWrite += writeDur
		totalWrite += writeDur

		if engine == "mpt" {
			mpt, err = trie.New(trie.TrieID(lastRoot), trieDB)
		} else {
			verkle, err = trie.NewVerkleTrie(lastRoot, trieDB, pointCache)
		}
		if err != nil {
			t.Fatalf("batch %d reload trie: %v", batchNumber, err)
		}
		window.batchWall = append(window.batchWall, time.Since(batchStart))

		if batchNumber%int64(*traceCompareMetricsBatches) == 0 || reachedBoundary {
			flushWindow(batchNumber)
		}
		if reachedBoundary {
			break
		}
	}
	if selection.endBlock > 0 && !selection.reachedEndBlock {
		t.Fatalf("trace ended at block %d before requested end block %d", selection.lastBlock, selection.endBlock)
	}
	actualBlockCount := uint64(0)
	if selection.endBlock >= selection.firstBlock && selection.firstBlock > 0 {
		actualBlockCount = selection.endBlock - selection.firstBlock + 1
	}
	average := func(total, count int64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count)
	}
	timingAverageNanos := map[string]float64{
		"read":   average(totalTiming.ReadNanos, totalTiming.Reads),
		"write":  average(totalTiming.WriteNanos, totalTiming.Writes),
		"delete": average(totalTiming.DeleteNanos, totalTiming.Deletes),
	}

	summary := map[string]any{
		"status":              "completed",
		"finished_at":         time.Now().Format(time.RFC3339),
		"elapsed_ms":          float64(time.Since(started)) / float64(time.Millisecond),
		"operations":          totalCounts.total(),
		"batches":             totalBatches,
		"first_block":         selection.firstBlock,
		"last_block":          selection.lastBlock,
		"block_count":         actualBlockCount,
		"counts":              totalCounts,
		"executed_gets":       executedGets,
		"executed_puts":       executedPuts,
		"executed_deletes":    executedDeletes,
		"parse_ms":            float64(totalParse) / float64(time.Millisecond),
		"operations_ms":       float64(totalOps) / float64(time.Millisecond),
		"root_ms":             float64(totalRoot) / float64(time.Millisecond),
		"db_write_ms":         float64(totalWrite) / float64(time.Millisecond),
		"timing_sample":       totalTiming,
		"timing_sample_every": *traceCompareTimingSampleEvery,
		"timing_average_ns":   timingAverageNanos,
		"measured_ops_per_s":  float64(totalCounts.total()) / (totalOps + totalRoot + totalWrite).Seconds(),
		"last_root":           lastRoot.Hex(),
		"state_bytes":         func() int64 { size, _ := GetDirSize(stateDir); return size }(),
	}
	if err := writeTraceCompareJSON(filepath.Join(resultsDir, "summary.json"), summary); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceCompareJSON(filepath.Join(*traceCompareBaseDir, "run_status.json"), map[string]any{"status": "completed", "finished_at": time.Now().Format(time.RFC3339), "operations": totalCounts.total()}); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[TRACE_COMPARE:%s] DONE ops=%d batches=%d root=%s elapsed=%s\n", engine, totalCounts.total(), totalBatches, lastRoot.Hex(), time.Since(started))
}

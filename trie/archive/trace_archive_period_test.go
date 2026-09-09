package archive

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	trieutils "github.com/ethereum/go-ethereum/trie/utils"
)

var (
	traceArchivePeriodInputDir      = flag.String("traceArchivePeriodInputDir", "", "Directory containing state_access_trace_*.csv.gz files")
	traceArchivePeriodOutput        = flag.String("traceArchivePeriodOutput", "", "Output JSON path for TestTraceArchivePeriodSimulation")
	traceArchivePeriodOps           = flag.Int64("traceArchivePeriodOps", 0, "Maximum ordered operations; zero consumes all selected trace shards")
	traceArchivePeriodBatchSize     = flag.Int64("traceArchivePeriodBatchSize", 4000, "Operations per replay batch")
	traceArchivePeriodShardDepth    = flag.Int("traceArchivePeriodShardDepth", 16, "ASCT shard depth")
	traceArchivePeriodPruneBatches  = flag.String("traceArchivePeriodPruneBatches", "1,2,4,8", "Comma-separated batches per prune advance")
	traceArchivePeriodWindowOps     = flag.Int64("traceArchivePeriodWindowOps", 100000000, "Operations per read-hit-rate window")
	traceArchivePeriodCheckpointOps = flag.Int64("traceArchivePeriodCheckpointOps", 1000000000, "Operations per hot/archive stem checkpoint")
)

type traceArchivePeriodVariant struct {
	PruneEveryBatches int64 `json:"prune_every_batches"`
	OperationsPerRing int64 `json:"operations_per_ring"`
	ExistingReads     int64 `json:"existing_reads"`
	HotReads          int64 `json:"hot_reads"`
	ArchivedReads     int64 `json:"archived_reads"`
}

type traceArchivePeriodWindow struct {
	StartOperation int64                       `json:"start_operation"`
	EndOperation   int64                       `json:"end_operation"`
	ExistingReads  int64                       `json:"existing_reads"`
	MissingReads   int64                       `json:"missing_reads"`
	Variants       []traceArchivePeriodVariant `json:"variants"`
}

type traceArchivePeriodCheckpoint struct {
	Operation     int64 `json:"operation"`
	ExistingStems int64 `json:"existing_stems"`
	Variants      []struct {
		PruneEveryBatches int64 `json:"prune_every_batches"`
		HotStems          int64 `json:"hot_stems"`
		ArchivedStems     int64 `json:"archived_stems"`
	} `json:"variants"`
}

type traceArchivePeriodState struct {
	present [4]uint64
	expiry  [8]uint32
	count   uint16
}

func (s *traceArchivePeriodState) has(suffix byte) bool {
	return s.present[suffix/64]&(uint64(1)<<(suffix%64)) != 0
}

func (s *traceArchivePeriodState) put(suffix byte) bool {
	mask := uint64(1) << (suffix % 64)
	word := &s.present[suffix/64]
	if *word&mask != 0 {
		return false
	}
	*word |= mask
	s.count++
	return true
}

func (s *traceArchivePeriodState) delete(suffix byte) bool {
	mask := uint64(1) << (suffix % 64)
	word := &s.present[suffix/64]
	if *word&mask == 0 {
		return false
	}
	*word &^= mask
	s.count--
	return true
}

type traceArchivePeriodRecord struct {
	stem      [StemSize]byte
	suffix    byte
	kind      traceStressKind
	operation string
}

type traceArchivePeriodReader struct {
	files       []string
	fileIndex   int
	file        *os.File
	gz          *gzip.Reader
	csv         *csv.Reader
	current     string
	lastAddress string
	lastAddr    common.Address
	lastHeader  [StemSize]byte
	headerValid bool
}

func newTraceArchivePeriodReader(inputDir string) (*traceArchivePeriodReader, error) {
	files, err := filepath.Glob(filepath.Join(inputDir, "state_access_trace_*.csv.gz"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no state_access_trace_*.csv.gz files in %s", inputDir)
	}
	return &traceArchivePeriodReader{files: files}, nil
}

func (r *traceArchivePeriodReader) closeCurrent() error {
	var result error
	if r.gz != nil {
		result = r.gz.Close()
	}
	if r.file != nil {
		if err := r.file.Close(); result == nil {
			result = err
		}
	}
	r.file, r.gz, r.csv = nil, nil, nil
	return result
}

func (r *traceArchivePeriodReader) Close() error {
	return r.closeCurrent()
}

func (r *traceArchivePeriodReader) openNext() error {
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
		return fmt.Errorf("unexpected trace header in %s", path)
	}
	r.file, r.gz, r.csv, r.current = f, gz, reader, filepath.Base(path)
	return nil
}

func (r *traceArchivePeriodReader) address(input string) (common.Address, error) {
	if input == r.lastAddress {
		return r.lastAddr, nil
	}
	if len(input) != 42 || !strings.HasPrefix(input, "0x") {
		return common.Address{}, fmt.Errorf("invalid address %q", input)
	}
	var addr common.Address
	if _, err := hex.Decode(addr[:], []byte(input[2:])); err != nil {
		return common.Address{}, err
	}
	r.lastAddress, r.lastAddr = input, addr
	r.headerValid = false
	return addr, nil
}

func (r *traceArchivePeriodReader) headerStem(addr common.Address) [StemSize]byte {
	if r.headerValid {
		return r.lastHeader
	}
	key := trieutils.BinaryTreeBasicDataKey(addr)
	copy(r.lastHeader[:], key[:StemSize])
	r.headerValid = true
	return r.lastHeader
}

func (r *traceArchivePeriodReader) parse(record []string) (traceArchivePeriodRecord, error) {
	if len(record) != 9 {
		return traceArchivePeriodRecord{}, fmt.Errorf("want 9 fields, got %d", len(record))
	}
	addr, err := r.address(record[4])
	if err != nil {
		return traceArchivePeriodRecord{}, err
	}
	var result traceArchivePeriodRecord
	result.operation = record[6]
	switch record[6] {
	case "read", "touch":
		result.kind = traceStressGet
	case "delete":
		result.kind = traceStressDelete
	case "write", "create":
		result.kind = traceStressPut
	default:
		return traceArchivePeriodRecord{}, fmt.Errorf("unsupported operation %q", record[6])
	}

	switch record[3] {
	case "account":
		result.stem = r.headerStem(addr)
		result.suffix = trieutils.BinaryBasicDataLeafKey
	case "code":
		chunk, err := strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return traceArchivePeriodRecord{}, fmt.Errorf("code chunk: %w", err)
		}
		if chunk < 128 {
			result.stem = r.headerStem(addr)
			result.suffix = byte(128 + chunk)
			break
		}
		key := trieutils.BinaryTreeCodeChunkKey(addr, chunk)
		copy(result.stem[:], key[:StemSize])
		result.suffix = key[StemSize]
	case "storage":
		if len(record[5]) != 66 || !strings.HasPrefix(record[5], "0x") {
			return traceArchivePeriodRecord{}, fmt.Errorf("invalid storage slot %q", record[5])
		}
		var slot common.Hash
		if _, err := hex.Decode(slot[:], []byte(record[5][2:])); err != nil {
			return traceArchivePeriodRecord{}, err
		}
		key := trieutils.BinaryTreeStorageSlotKey(addr, slot[:])
		copy(result.stem[:], key[:StemSize])
		result.suffix = key[StemSize]
		if result.kind == traceStressPut && strings.EqualFold(record[7], traceStressZeroStorageHash) {
			result.kind = traceStressDelete
		}
	default:
		return traceArchivePeriodRecord{}, fmt.Errorf("unsupported object type %q", record[3])
	}
	return result, nil
}

func (r *traceArchivePeriodReader) Read() (traceArchivePeriodRecord, error) {
	for {
		if r.csv == nil {
			if err := r.openNext(); err != nil {
				return traceArchivePeriodRecord{}, err
			}
		}
		record, err := r.csv.Read()
		if errors.Is(err, io.EOF) {
			if err := r.closeCurrent(); err != nil {
				return traceArchivePeriodRecord{}, err
			}
			continue
		}
		if err != nil {
			return traceArchivePeriodRecord{}, fmt.Errorf("read %s: %w", r.current, err)
		}
		result, err := r.parse(record)
		if err != nil {
			line, _ := r.csv.FieldPos(0)
			return traceArchivePeriodRecord{}, fmt.Errorf("parse %s line %d: %w", r.current, line, err)
		}
		return result, nil
	}
}

func parseTraceArchivePeriods(input string) ([]int64, error) {
	parts := strings.Split(input, ",")
	periods := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{})
	for _, part := range parts {
		value, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid prune batch period %q", part)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		periods = append(periods, value)
	}
	if len(periods) == 0 || len(periods) > 8 {
		return nil, fmt.Errorf("want 1..8 prune batch periods")
	}
	sort.Slice(periods, func(i, j int) bool { return periods[i] < periods[j] })
	return periods, nil
}

func traceArchiveExpiryBatch(batch, shard, shardCount, pruneEvery int64, pruneCompleted bool) uint32 {
	pruneEvent := (batch - 1) / pruneEvery
	prunedShard := pruneEvent % shardCount
	ring := pruneEvent / shardCount
	cursor := prunedShard
	if pruneCompleted {
		cursor = (prunedShard + 1) % shardCount
	}
	writtenWithCurrentGlobal := shard < cursor

	nextEvent := ring*shardCount + shard
	if shard <= prunedShard {
		nextEvent += shardCount
	}
	nextRing := nextEvent / shardCount
	nextPruneUsesCurrentGlobal := (nextRing-ring)%2 == 0
	expiryEvent := nextEvent
	if writtenWithCurrentGlobal == nextPruneUsesCurrentGlobal {
		expiryEvent += shardCount
	}
	return uint32(expiryEvent*pruneEvery + 1)
}

func traceArchiveShard(stem [StemSize]byte, depth int) int64 {
	var shard uint64
	for bit := 0; bit < depth; bit++ {
		shard = (shard << 1) | uint64((stem[bit/8]>>(7-uint(bit%8)))&1)
	}
	return int64(shard)
}

func TestTraceArchiveExpiryBatch(t *testing.T) {
	const shards = int64(4)
	for _, test := range []struct {
		batch, shard, period, want int64
		completed                  bool
	}{
		{1, 1, 1, 2, false},
		{1, 0, 1, 5, true},
		{3, 1, 1, 6, false},
		{4, 2, 1, 7, false},
		{4, 0, 1, 9, true},
		{4, 2, 1, 11, true},
		{4, 3, 1, 12, true},
		{2, 1, 2, 3, false},
		{3, 1, 2, 11, true},
		{7, 2, 2, 13, false},
		{7, 2, 2, 21, true},
		{7, 3, 2, 23, true},
		{8, 2, 2, 21, true},
	} {
		if got := int64(traceArchiveExpiryBatch(test.batch, test.shard, shards, test.period, test.completed)); got != test.want {
			t.Fatalf("batch=%d shard=%d period=%d completed=%t: got %d want %d", test.batch, test.shard, test.period, test.completed, got, test.want)
		}
	}
}

func TestTraceArchivePeriodSimulation(t *testing.T) {
	if *traceArchivePeriodInputDir == "" {
		t.Skip("set -traceArchivePeriodInputDir to simulate archive periods")
	}
	if *traceArchivePeriodOutput == "" {
		t.Fatal("-traceArchivePeriodOutput is required")
	}
	if *traceArchivePeriodBatchSize <= 0 || *traceArchivePeriodWindowOps <= 0 || *traceArchivePeriodCheckpointOps <= 0 {
		t.Fatal("batch, window, and checkpoint sizes must be positive")
	}
	if *traceArchivePeriodShardDepth < 1 || *traceArchivePeriodShardDepth > 24 {
		t.Fatalf("invalid shard depth %d", *traceArchivePeriodShardDepth)
	}
	periods, err := parseTraceArchivePeriods(*traceArchivePeriodPruneBatches)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := newTraceArchivePeriodReader(*traceArchivePeriodInputDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	shardCount := int64(1) << *traceArchivePeriodShardDepth
	variants := make([]traceArchivePeriodVariant, len(periods))
	windowVariants := make([]traceArchivePeriodVariant, len(periods))
	for i, period := range periods {
		variants[i].PruneEveryBatches = period
		variants[i].OperationsPerRing = shardCount * period * *traceArchivePeriodBatchSize
		windowVariants[i] = variants[i]
	}
	states := make([]traceArchivePeriodState, 0, 1<<20)
	stateIndex := make(map[[StemSize]byte]uint32, 1<<20)
	windows := make([]traceArchivePeriodWindow, 0)
	checkpoints := make([]traceArchivePeriodCheckpoint, 0)
	var operations, existingReads, missingReads int64
	var windowExisting, windowMissing int64
	windowStartOperation := int64(1)
	prunePending := make([]bool, len(periods))
	var activeBatch int64
	started := time.Now()

	flushWindow := func() {
		window := traceArchivePeriodWindow{
			StartOperation: windowStartOperation,
			EndOperation:   operations,
			ExistingReads:  windowExisting,
			MissingReads:   windowMissing,
			Variants:       append([]traceArchivePeriodVariant(nil), windowVariants...),
		}
		windows = append(windows, window)
		windowExisting, windowMissing = 0, 0
		windowStartOperation = operations + 1
		for i := range windowVariants {
			windowVariants[i] = traceArchivePeriodVariant{
				PruneEveryBatches: periods[i],
				OperationsPerRing: shardCount * periods[i] * *traceArchivePeriodBatchSize,
			}
		}
	}
	flushCheckpoint := func() {
		checkpoint := traceArchivePeriodCheckpoint{Operation: operations}
		checkpoint.Variants = make([]struct {
			PruneEveryBatches int64 `json:"prune_every_batches"`
			HotStems          int64 `json:"hot_stems"`
			ArchivedStems     int64 `json:"archived_stems"`
		}, len(periods))
		batch := uint32((operations-1) / *traceArchivePeriodBatchSize + 1)
		for i, period := range periods {
			checkpoint.Variants[i].PruneEveryBatches = period
		}
		for i := range states {
			state := &states[i]
			if state.count == 0 {
				continue
			}
			checkpoint.ExistingStems++
			for variant := range periods {
				if batch < state.expiry[variant] {
					checkpoint.Variants[variant].HotStems++
				} else {
					checkpoint.Variants[variant].ArchivedStems++
				}
			}
		}
		checkpoints = append(checkpoints, checkpoint)
	}

	for *traceArchivePeriodOps == 0 || operations < *traceArchivePeriodOps {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		operations++
		batch := (operations-1) / *traceArchivePeriodBatchSize + 1
		if batch != activeBatch {
			activeBatch = batch
			for variant, period := range periods {
				prunePending[variant] = (batch-1)%period == 0
			}
		}
		shard := traceArchiveShard(record.stem, *traceArchivePeriodShardDepth)
		for variant, period := range periods {
			if prunePending[variant] && ((batch-1)/period)%shardCount == shard {
				prunePending[variant] = false
			}
		}
		indexPlusOne := stateIndex[record.stem]
		var state *traceArchivePeriodState
		if indexPlusOne > 0 {
			state = &states[indexPlusOne-1]
		}
		switch record.kind {
		case traceStressGet:
			if state == nil || !state.has(record.suffix) {
				missingReads++
				windowMissing++
				break
			}
			existingReads++
			windowExisting++
			for variant := range periods {
				variants[variant].ExistingReads++
				windowVariants[variant].ExistingReads++
				if uint32(batch) < state.expiry[variant] {
					variants[variant].HotReads++
					windowVariants[variant].HotReads++
				} else {
					variants[variant].ArchivedReads++
					windowVariants[variant].ArchivedReads++
				}
			}
		case traceStressPut:
			if state == nil {
				states = append(states, traceArchivePeriodState{})
				stateIndex[record.stem] = uint32(len(states))
				state = &states[len(states)-1]
			}
			state.put(record.suffix)
			for variant, period := range periods {
				state.expiry[variant] = traceArchiveExpiryBatch(batch, shard, shardCount, period, !prunePending[variant])
			}
		case traceStressDelete:
			if state == nil || !state.delete(record.suffix) || state.count == 0 {
				break
			}
			for variant, period := range periods {
				state.expiry[variant] = traceArchiveExpiryBatch(batch, shard, shardCount, period, !prunePending[variant])
			}
		}

		if operations%*traceArchivePeriodWindowOps == 0 {
			flushWindow()
		}
		if operations%*traceArchivePeriodCheckpointOps == 0 {
			flushCheckpoint()
			fmt.Printf("[TRACE_ARCHIVE_PERIOD] ops=%d stems=%d existing_reads=%d elapsed=%s\n", operations, len(states), existingReads, time.Since(started))
		}
	}
	if operations == 0 {
		t.Fatal("trace contained no operations")
	}
	if operations%*traceArchivePeriodWindowOps != 0 {
		flushWindow()
	}
	if operations%*traceArchivePeriodCheckpointOps != 0 {
		flushCheckpoint()
	}
	result := map[string]any{
		"generated_at":           time.Now().Format(time.RFC3339),
		"elapsed_ms":             float64(time.Since(started)) / float64(time.Millisecond),
		"input_dir":              *traceArchivePeriodInputDir,
		"source_files":           reader.files,
		"operations":             operations,
		"batch_size":             *traceArchivePeriodBatchSize,
		"shard_depth":            *traceArchivePeriodShardDepth,
		"existing_reads":         existingReads,
		"missing_reads":          missingReads,
		"unique_stems_ever_seen": len(states),
		"semantics":              "exact operation order; reads do not refresh; writes and effective non-final deletes reset the whole stem epoch; final deletes remove the stem; prune runs before the first operation of each eligible batch",
		"variants":               variants,
		"windows":                windows,
		"checkpoints":            checkpoints,
	}
	if err := os.MkdirAll(filepath.Dir(*traceArchivePeriodOutput), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeTraceStressJSON(*traceArchivePeriodOutput, result); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("[TRACE_ARCHIVE_PERIOD] DONE ops=%d stems=%d existing_reads=%d missing_reads=%d elapsed=%s output=%s\n",
		operations, len(states), existingReads, missingReads, time.Since(started), *traceArchivePeriodOutput)
}

func TestTraceArchivePeriodReaderMatchesStressParser(t *testing.T) {
	address := "0x0000000000000000000000000000000000001234"
	valueHash := crypto.Keccak256Hash([]byte("value")).Hex()
	reader := &traceArchivePeriodReader{}
	for _, record := range [][]string{
		{"1", "0", "0", "account", address, "", "read", valueHash, "32"},
		{"1", "0", "1", "code", address, "0", "read", valueHash, "31"},
		{"1", "0", "2", "code", address, "127", "read", valueHash, "31"},
		{"1", "0", "1", "code", address, "257", "write", valueHash, "31"},
		{"1", "0", "2", "storage", address, common.Hash{31: 3}.Hex(), "read", valueHash, "32"},
		{"1", "0", "3", "storage", address, common.Hash{0: 0xff, 31: 9}.Hex(), "write", valueHash, "32"},
		{"1", "0", "4", "storage", address, common.Hash{31: 7}.Hex(), "write", traceStressZeroStorageHash, "32"},
	} {
		got, err := reader.parse(record)
		if err != nil {
			t.Fatal(err)
		}
		want, err := parseTraceStressRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.stem[:], want.key[:StemSize]) || got.suffix != want.key[StemSize] || got.kind != want.kind || got.operation != want.operation {
			t.Fatalf("reader mismatch for %v: got=%+v want_kind=%d want_key=%x", record, got, want.kind, want.key)
		}
	}
}

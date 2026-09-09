package archive

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
)

var (
	traceColdSampleEvery = flag.Int("traceColdSampleEvery", 10000, "Sample one trace operation every N operations for TestArchiveStemTraceColdHitStats")
	traceColdOutput      = flag.String("traceColdOutput", "", "Output JSON path for TestArchiveStemTraceColdHitStats")
	traceFilterSamples   = flag.Int("traceFilterSamples", 20, "Synthetic negative samples per archive bucket for TestArchiveStemTraceFilterFPStats")
	traceFilterSeed      = flag.Int64("traceFilterSeed", 7, "Synthetic archive-filter sampling seed")
	traceFilterOutput    = flag.String("traceFilterOutput", "", "Output JSON path for TestArchiveStemTraceFilterFPStats")
)

type traceColdKindStats struct {
	Sampled  int64 `json:"sampled"`
	Hot      int64 `json:"hot"`
	Archived int64 `json:"archived"`
	Missing  int64 `json:"missing"`
	Errors   int64 `json:"errors"`
}

// TestArchiveStemTraceFilterFPStats performs read-only synthetic negative
// sampling against every reachable archive bucket in a completed trace run.
func TestArchiveStemTraceFilterFPStats(t *testing.T) {
	if *traceStatsBaseDir == "" || *traceStatsRoot == "" || *traceFilterOutput == "" {
		t.Skip("set -traceStatsBaseDir, -traceStatsRoot, and -traceFilterOutput to inspect a completed run")
	}
	if *traceFilterSamples <= 0 {
		t.Fatal("-traceFilterSamples must be positive")
	}
	stateDir := filepath.Join(*traceStatsBaseDir, "state_db")
	if info, err := os.Stat(stateDir); err != nil || !info.IsDir() {
		t.Fatalf("state directory does not exist: %s", stateDir)
	}
	if _, err := os.Stat(*traceFilterOutput); err == nil {
		t.Fatalf("output already exists: %s", *traceFilterOutput)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	root := common.HexToHash(*traceStatsRoot)
	db, err := leveldb.New(stateDir, 512, 256, "trace-filter-fp-stats", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfig()
	config.ShardDepth = *traceStressShardDepth
	config.NodeStorageScheme = NodeStoragePath
	config.StemMode = true
	config.CuckooBuckets = 16
	config.CuckooSlots = 4
	backend := NewTrie(root.Bytes(), &stressDBAdapter{db}, NewPooledKeccakHasher(), config, true)

	started := time.Now()
	fp := backend.SampleArchiveFilterFalsePositives(*traceFilterSamples, *traceFilterSeed)
	result := map[string]any{
		"status":             "completed",
		"base_dir":           *traceStatsBaseDir,
		"root":               root.Hex(),
		"samples_per_bucket": *traceFilterSamples,
		"seed":               *traceFilterSeed,
		"stats":              fp,
		"stats_ms":           float64(time.Since(started)) / float64(time.Millisecond),
		"command":            strings.Join(os.Args, " "),
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*traceFilterOutput, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if fp.SampledBuckets == 0 || fp.NegativeQueries == 0 {
		t.Fatalf("no filter samples: buckets=%d negatives=%d", fp.SampledBuckets, fp.NegativeQueries)
	}
}

func (s *traceColdKindStats) add(fromArchive bool, err error) {
	s.Sampled++
	switch {
	case err == nil && fromArchive:
		s.Archived++
	case err == nil:
		s.Hot++
	case errors.Is(err, ErrNodeNotFound):
		s.Missing++
	default:
		s.Errors++
	}
}

func (s *traceColdKindStats) archivedShare() float64 {
	if s.Sampled == 0 {
		return 0
	}
	return float64(s.Archived) / float64(s.Sampled)
}

// TestArchiveStemTraceColdHitStats samples the ordered trace against a
// completed final root and reports how many accesses target archived stems.
// It executes no trace writes and therefore does not mutate the final state.
func TestArchiveStemTraceColdHitStats(t *testing.T) {
	if *traceStatsBaseDir == "" || *traceStatsRoot == "" || *traceColdOutput == "" {
		t.Skip("set -traceStatsBaseDir, -traceStatsRoot, and -traceColdOutput to sample a completed run")
	}
	if *traceStressInputDir == "" {
		t.Fatal("-traceStressInputDir is required")
	}
	if *traceColdSampleEvery < 2 {
		t.Fatal("-traceColdSampleEvery must be at least 2")
	}
	if *traceStressOps <= 0 {
		t.Fatal("-traceStressOps must be positive for cold-hit sampling")
	}
	stateDir := filepath.Join(*traceStatsBaseDir, "state_db")
	if info, err := os.Stat(stateDir); err != nil || !info.IsDir() {
		t.Fatalf("state directory does not exist: %s", stateDir)
	}
	if _, err := os.Stat(*traceColdOutput); err == nil {
		t.Fatalf("output already exists: %s", *traceColdOutput)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	root := common.HexToHash(*traceStatsRoot)
	db, err := leveldb.New(stateDir, 512, 256, "trace-cold-hit-stats", false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	config := DefaultConfig()
	config.ShardDepth = *traceStressShardDepth
	config.NodeStorageScheme = NodeStoragePath
	config.StemMode = true
	config.CuckooBuckets = 16
	config.CuckooSlots = 4
	backend := NewTrie(root.Bytes(), &stressDBAdapter{db}, NewPooledKeccakHasher(), config, true)

	reader, err := newTraceStressReader(*traceStressInputDir, *traceStressStartFile, *traceStressFileLimit)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	started := time.Now()
	var reads, writes, deletes traceColdKindStats
	var index int64
	var firstBlock, lastBlock uint64
	for {
		if index >= int64(*traceStressOps) {
			break
		}
		op, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if index%int64(*traceColdSampleEvery) == 0 {
			if firstBlock == 0 {
				firstBlock = op.block
			}
			lastBlock = op.block
			stemKey := op.key[:StemSize]
			_, fromArchive, err := backend.GetValueRef(stemKey)
			switch op.kind {
			case traceStressGet:
				reads.add(fromArchive, err)
			case traceStressPut:
				writes.add(fromArchive, err)
			case traceStressDelete:
				deletes.add(fromArchive, err)
			}
		}
		index++
	}

	result := map[string]any{
		"status":               "completed",
		"base_dir":             *traceStatsBaseDir,
		"root":                 root.Hex(),
		"input_dir":            *traceStressInputDir,
		"source_files":         reader.files,
		"start_file":           *traceStressStartFile,
		"file_limit":           *traceStressFileLimit,
		"trace_operations":     index,
		"requested_operations": *traceStressOps,
		"sample_every":         *traceColdSampleEvery,
		"first_sampled_block":  firstBlock,
		"last_sampled_block":   lastBlock,
		"reads":                reads,
		"writes":               writes,
		"deletes":              deletes,
		"read_archived_share":  fmt.Sprintf("%.6f%%", reads.archivedShare()*100),
		"write_archived_share": fmt.Sprintf("%.6f%%", writes.archivedShare()*100),
		"stats_ms":             float64(time.Since(started)) / float64(time.Millisecond),
		"command":              strings.Join(os.Args, " "),
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*traceColdOutput, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if reads.Errors+writes.Errors+deletes.Errors != 0 {
		t.Fatalf("classification errors: reads=%d writes=%d deletes=%d", reads.Errors, writes.Errors, deletes.Errors)
	}
}

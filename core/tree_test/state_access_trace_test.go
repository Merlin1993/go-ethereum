package tree

// Ordered mainnet state-access trace exporter.
//
// TestExportStateAccessTrace replays real mainnet transactions (from the
// E:\ethdata CSV corpus) through the EVM once, while a TraceStateDB wrapper
// records every account / storage / code read and write in exact occurrence
// order. The output CSV (state_access_trace_<start>_<end>.csv) can then drive
// ASCT / StemTrie stress tests without re-running the EVM.

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

var (
	traceOutDir      = flag.String("traceOutDir2", "", "Output directory for state_access_trace CSV (default F:\\codex_asct\\mainnet_state_access_trace\\<date>)")
	traceChunkBlocks = flag.Uint64("traceChunkBlocks2", 100000, "Blocks per output trace CSV file")
	traceUseMemory   = flag.Bool("traceUseMemory2", true, "Use in-memory KV state database (fast; no disk DB)")
	traceValues      = flag.Bool("traceValues2", true, "Include value_hash and value_len columns")
	traceVerbose     = flag.Bool("traceVerbose2", false, "Verbose per-block logging")
	traceRecordFrom  = flag.Uint64("traceRecordFromBlock2", 0, "Only record events for blocks >= N; earlier blocks are warm-up (state building, no output)")
	traceGzip        = flag.Bool("traceGzip2", true, "Write gzip-compressed trace CSVs (.csv.gz)")
	traceSkipKnown   = flag.Bool("traceSkipKVKnown2", true, "Disable the KV committed-key index (state.KVSkipKnown) to cap memory on long runs")
	traceDedupCode   = flag.Bool("traceDedupCode2", true, "Record code-chunk reads once per (block, address) (matches StateDB stateObject code cache); prevents hot-contract blowup")
)

const (
	traceCodeChunkSize = 31 // ASCT stores EVM code as 31-byte chunks (suffix 128+i)
	traceHeader        = "block_number,tx_index,seq,object_type,address,slot_or_chunk,operation,value_hash,value_len"
)

// traceFileInfo describes one produced CSV shard.
type traceFileInfo struct {
	Name       string `json:"name"`
	Rows       uint64 `json:"rows"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
	FirstBlock uint64 `json:"first_block"`
	LastBlock  uint64 `json:"last_block"`
	Txs        uint64 `json:"txs"`
}

type traceManifest struct {
	GeneratedAt        string            `json:"generated_at"`
	SourceCommit       string            `json:"source_commit"`
	Command            string            `json:"command"`
	SourceFiles        []string          `json:"source_transaction_files"`
	FirstBlock         uint64            `json:"first_block"`
	LastBlock          uint64            `json:"last_block"`
	TotalBlocks        uint64            `json:"total_blocks"`
	EmptyBlocks        uint64            `json:"empty_blocks"`
	MissingBlocks       []uint64          `json:"missing_blocks"`
	TotalTxs           uint64            `json:"total_txs"`
	SuccessTxs         uint64            `json:"success_txs"`
	FailedTxs          uint64            `json:"failed_txs"`
	TotalRows          uint64            `json:"total_rows"`
	IncludeReads       bool              `json:"include_reads"`
	Compressed         bool              `json:"compressed_gzip"`
	IncludeValues      bool              `json:"include_values"`
	OpCounts           map[string]uint64 `json:"op_counts"`
	TypeCounts         map[string]uint64 `json:"object_type_counts"`
	SeqViolations      uint64            `json:"seq_violations"`
	Files              []traceFileInfo   `json:"files"`
}

// traceRecorder writes ordered events, rotating the CSV by block chunk.
type traceRecorder struct {
	outDir      string
	chunkBlocks uint64
	includeVals bool
	gzip        bool

	f        *os.File
	bufw     *bufio.Writer
	gz       *gzip.Writer
	w        *csv.Writer
	chunk    traceFileInfo // current chunk accumulator
	chunkSet bool

	block uint64
	txIdx int64
	seq   uint64
	codeSeen map[common.Address]struct{}

	// validation
	lastBlock uint64
	lastTx    int64
	lastSeq   uint64
	haveLast  bool
	seqVio    uint64

	// totals
	missingBlk []uint64
	totalRows   uint64
	totalBlocks uint64
	emptyBlocks uint64
	totalTxs    uint64
	successTxs  uint64
	failedTxs   uint64
	opCounts    map[string]uint64
	typeCounts  map[string]uint64
	files       []traceFileInfo
	startTime   time.Time
}

func newTraceRecorder(outDir string, chunkBlocks uint64, includeVals, gzipOut bool) (*traceRecorder, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	return &traceRecorder{
		outDir:      outDir,
		chunkBlocks: chunkBlocks,
		includeVals: includeVals,
		gzip:        gzipOut,
		opCounts:    make(map[string]uint64),
		typeCounts:  make(map[string]uint64),
		missingBlk:  []uint64{},
		startTime:   time.Now(),
	}, nil
}

func (r *traceRecorder) beginBlock(block uint64) {
	r.block = block
	r.txIdx = -1
	r.seq = 0
	r.totalBlocks++
	r.codeSeen = make(map[common.Address]struct{})
}

func (r *traceRecorder) beginTx(i int) {
	r.txIdx = int64(i)
	r.seq = 0
	r.totalTxs++
}

// endBlock flushes the buffered writer; called after commit.
func (r *traceRecorder) endBlock(block uint64, empty bool) {
	if empty {
		r.emptyBlocks++
	}
	if r.bufw != nil {
		r.bufw.Flush()
	}
	if r.chunkSet && block > r.chunk.LastBlock {
		r.chunk.LastBlock = block
	}
}

// markMissing records a processed block whose block metadata (timestamp)
// is absent from the blocks_*.csv corpus, i.e. a data gap in the source.
func (r *traceRecorder) markMissing(block uint64) {
	r.missingBlk = append(r.missingBlk, block)
}

func (r *traceRecorder) ensureChunk(block uint64) error {
	if r.chunkSet && block-r.chunk.FirstBlock >= r.chunkBlocks {
		if err := r.closeChunk(); err != nil {
			return err
		}
	}
	if !r.chunkSet {
		ext := ".csv"
		if r.gzip {
			ext = ".csv.gz"
		}
		name := fmt.Sprintf("state_access_trace_tmp_%05d%s", len(r.files), ext)
		f, err := os.Create(filepath.Join(r.outDir, name))
		if err != nil {
			return err
		}
		r.f = f
		if r.gzip {
			gz := gzip.NewWriter(f)
			r.bufw = bufio.NewWriterSize(gz, 1<<20)
			r.gz = gz
		} else {
			r.bufw = bufio.NewWriterSize(f, 1<<20)
		}
		r.w = csv.NewWriter(r.bufw)
		if err := r.w.Write(strings.Split(traceHeader, ",")); err != nil {
			return err
		}
		r.chunk = traceFileInfo{Name: name, FirstBlock: block, LastBlock: block}
		r.chunkSet = true
	}
	return nil
}

func (r *traceRecorder) closeChunk() error {
	if !r.chunkSet {
		return nil
	}
	if r.w != nil {
		r.w.Flush()
		if err := r.w.Error(); err != nil {
			return err
		}
	}
	if r.bufw != nil {
		if err := r.bufw.Flush(); err != nil {
			return err
		}
	}
	if r.gz != nil {
		if err := r.gz.Close(); err != nil {
			return err
		}
		r.gz = nil
	}
	if r.f != nil {
		if err := r.f.Close(); err != nil {
			return err
		}
	}
	ext := ".csv"
	if r.gzip {
		ext = ".csv.gz"
	}
	finalName := fmt.Sprintf("state_access_trace_%09d_%09d%s", r.chunk.FirstBlock, r.chunk.LastBlock, ext)
	tmpPath := filepath.Join(r.outDir, r.chunk.Name)
	finalPath := filepath.Join(r.outDir, finalName)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	st, err := os.Stat(finalPath)
	if err != nil {
		return err
	}
	r.chunk.Name = finalName
	r.chunk.SizeBytes = st.Size()
	r.chunk.SHA256 = sha256File(finalPath)
	r.files = append(r.files, r.chunk)
	r.f, r.bufw, r.w = nil, nil, nil
	r.chunkSet = false
	r.chunk = traceFileInfo{}
	return nil
}

func (r *traceRecorder) emit(objType, addr, slotChunk, op string, value []byte) {
	if err := r.ensureChunk(r.block); err != nil {
		panic(fmt.Sprintf("trace: ensure chunk: %v", err))
	}
	// validation: seq strictly increasing within (block, tx)
	if r.haveLast && r.block == r.lastBlock && r.txIdx == r.lastTx && r.seq <= r.lastSeq {
		r.seqVio++
	}
	r.lastBlock, r.lastTx, r.lastSeq = r.block, r.txIdx, r.seq
	r.haveLast = true

	row := []string{
		strconv.FormatUint(r.block, 10),
		strconv.FormatInt(r.txIdx, 10),
		strconv.FormatUint(r.seq, 10),
		objType,
		addr,
		slotChunk,
		op,
		"",
		"0",
	}
	row[8] = strconv.Itoa(len(value))
	if r.includeVals && len(value) > 0 {
		row[7] = crypto.Keccak256Hash(value).Hex()
	}
	if err := r.w.Write(row); err != nil {
		panic(fmt.Sprintf("trace: write row: %v", err))
	}
	r.totalRows++
	r.seq++
	r.opCounts[op]++
	r.typeCounts[objType]++
	if r.chunkSet {
		r.chunk.Rows++
	}
}

func (r *traceRecorder) finish() (*traceManifest, error) {
	if err := r.closeChunk(); err != nil {
		return nil, err
	}
	sort.Slice(r.missingBlk, func(i, j int) bool { return r.missingBlk[i] < r.missingBlk[j] })
	man := &traceManifest{
		GeneratedAt:   time.Now().Format(time.RFC3339),
		TotalBlocks:   r.totalBlocks,
		EmptyBlocks:   r.emptyBlocks,
		TotalTxs:      r.totalTxs,
		SuccessTxs:    r.successTxs,
		FailedTxs:     r.failedTxs,
		TotalRows:     r.totalRows,
		IncludeReads:  true,
		IncludeValues: r.includeVals,
		Compressed:    r.gzip,
		OpCounts:      r.opCounts,
		TypeCounts:    r.typeCounts,
		SeqViolations: r.seqVio,
		Files:         r.files,
		MissingBlocks: r.missingBlk,
	}
	if len(r.files) > 0 {
		man.FirstBlock = r.files[0].FirstBlock
		man.LastBlock = r.files[len(r.files)-1].LastBlock
	}
	return man, nil
}

func sha256File(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	h := sha256.New()
	if _, err := bufio.NewReaderSize(f, 1<<20).WriteTo(h); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func balanceBytes(v *uint256.Int) []byte {
	b := v.Bytes32()
	return b[:]
}

func nonceBytes(v uint64) []byte {
	var b [8]byte
	for i := 7; i >= 0; i-- {
		b[i] = byte(v)
		v >>= 8
	}
	return b[:]
}

// traceStateDB wraps state.StateDB and records ordered state accesses.
type traceStateDB struct {
	*state.StateDB
	rec *traceRecorder
}

var _ vm.StateDB = (*traceStateDB)(nil)

func (db *traceStateDB) GetBalance(addr common.Address) *uint256.Int {
	bal := db.StateDB.GetBalance(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "read", balanceBytes(bal))
	}
	return bal
}

func (db *traceStateDB) GetNonce(addr common.Address) uint64 {
	nonce := db.StateDB.GetNonce(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "read", nonceBytes(nonce))
	}
	return nonce
}

func (db *traceStateDB) GetCodeHash(addr common.Address) common.Hash {
	h := db.StateDB.GetCodeHash(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "read", h[:])
	}
	return h
}

func (db *traceStateDB) GetCodeSize(addr common.Address) int {
	size := db.StateDB.GetCodeSize(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "read", nil)
	}
	return size
}

func (db *traceStateDB) GetCode(addr common.Address) []byte {
	code := db.StateDB.GetCode(addr)
	if db.rec != nil {
		if *traceDedupCode {
			if _, seen := db.rec.codeSeen[addr]; seen {
				return code
			}
			db.rec.codeSeen[addr] = struct{}{}
		}
		db.rec.emit("account", addr.Hex(), "", "read", nil)
		if len(code) > 0 {
			emitCodeChunks(db.rec, addr, code, "read")
		}
	}
	return code
}

func (db *traceStateDB) GetState(addr common.Address, slot common.Hash) common.Hash {
	val := db.StateDB.GetState(addr, slot)
	if db.rec != nil {
		db.rec.emit("storage", addr.Hex(), slot.Hex(), "read", val[:])
	}
	return val
}

func (db *traceStateDB) GetCommittedState(addr common.Address, slot common.Hash) common.Hash {
	val := db.StateDB.GetCommittedState(addr, slot)
	if db.rec != nil {
		db.rec.emit("storage", addr.Hex(), slot.Hex(), "read", val[:])
	}
	return val
}

func (db *traceStateDB) SetState(addr common.Address, slot, value common.Hash) common.Hash {
	prev := db.StateDB.SetState(addr, slot, value)
	if db.rec != nil {
		// SSTORE always reads the slot (old value), then writes when changed.
		db.rec.emit("storage", addr.Hex(), slot.Hex(), "read", prev[:])
		if prev != value {
			db.rec.emit("storage", addr.Hex(), slot.Hex(), "write", value[:])
		}
	}
	return prev
}

func (db *traceStateDB) GetStorageRoot(addr common.Address) common.Hash {
	root := db.StateDB.GetStorageRoot(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "touch", root[:])
	}
	return root
}

func (db *traceStateDB) Exist(addr common.Address) bool {
	ok := db.StateDB.Exist(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "touch", nil)
	}
	return ok
}

func (db *traceStateDB) Empty(addr common.Address) bool {
	ok := db.StateDB.Empty(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "touch", nil)
	}
	return ok
}

func (db *traceStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	prev := db.StateDB.AddBalance(addr, amount, reason)
	if db.rec != nil && !amount.IsZero() {
		newBal := new(uint256.Int).Add(&prev, amount)
		db.rec.emit("account", addr.Hex(), "", "write", balanceBytes(newBal))
	}
	return prev
}

func (db *traceStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	prev := db.StateDB.SubBalance(addr, amount, reason)
	if db.rec != nil && !amount.IsZero() {
		newBal := new(uint256.Int).Sub(&prev, amount)
		db.rec.emit("account", addr.Hex(), "", "write", balanceBytes(newBal))
	}
	return prev
}

func (db *traceStateDB) SetBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) {
	prev := db.StateDB.GetBalance(addr)
	db.StateDB.SetBalance(addr, amount, reason)
	if db.rec != nil && !prev.Eq(amount) {
		db.rec.emit("account", addr.Hex(), "", "write", balanceBytes(amount))
	}
}

func (db *traceStateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	prev := db.StateDB.GetNonce(addr)
	db.StateDB.SetNonce(addr, nonce, reason)
	if db.rec != nil && prev != nonce {
		db.rec.emit("account", addr.Hex(), "", "write", nonceBytes(nonce))
	}
}

func (db *traceStateDB) SetCode(addr common.Address, code []byte) []byte {
	prev := db.StateDB.SetCode(addr, code)
	if db.rec != nil && (len(prev) != len(code) || string(prev) != string(code)) {
		h := crypto.Keccak256Hash(code)
		db.rec.emit("account", addr.Hex(), "", "write", h[:])
		emitCodeChunks(db.rec, addr, code, "write")
	}
	return prev
}

func (db *traceStateDB) CreateAccount(addr common.Address) {
	db.StateDB.CreateAccount(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "create", nil)
	}
}

func (db *traceStateDB) CreateContract(addr common.Address) {
	db.StateDB.CreateContract(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "touch", nil)
	}
}

func (db *traceStateDB) SelfDestruct(addr common.Address) uint256.Int {
	code := db.StateDB.GetCode(addr)
	bal := db.StateDB.SelfDestruct(addr)
	if db.rec != nil {
		db.rec.emit("account", addr.Hex(), "", "delete", balanceBytes(&bal))
		if len(code) > 0 {
			emitCodeChunks(db.rec, addr, code, "delete")
		}
	}
	return bal
}

func (db *traceStateDB) SelfDestruct6780(addr common.Address) (uint256.Int, bool) {
	code := db.StateDB.GetCode(addr)
	bal, changed := db.StateDB.SelfDestruct6780(addr)
	if db.rec != nil && changed {
		db.rec.emit("account", addr.Hex(), "", "delete", balanceBytes(&bal))
		if len(code) > 0 {
			emitCodeChunks(db.rec, addr, code, "delete")
		}
	}
	return bal, changed
}

func emitCodeChunks(rec *traceRecorder, addr common.Address, code []byte, op string) {
	n := (len(code) + traceCodeChunkSize - 1) / traceCodeChunkSize
	for i := 0; i < n; i++ {
		start := i * traceCodeChunkSize
		end := start + traceCodeChunkSize
		if end > len(code) {
			end = len(code)
		}
		rec.emit("code", addr.Hex(), strconv.Itoa(i), op, code[start:end])
	}
}

// traceRecFor returns the recorder for a block, or nil if the block is before
// the record-from threshold (warm-up, no output).
func traceRecFor(rec *traceRecorder, block uint64) *traceRecorder {
	if *traceRecordFrom != 0 && block < *traceRecordFrom {
		return nil
	}
	return rec
}

// processTraceBlock executes one block (empty or with txs) through the EVM with
// the ordered trace wrapper, then commits state and returns the new root.
func processTraceBlock(t *testing.T, host *ProcessorHost, rec *traceRecorder, block uint64, lastRoot common.Hash, msgs []*core.Message, balance, reward *uint256.Int) common.Hash {
	t.Helper()
	host.sdb.SetBlockNum(block)
	statedb, err := state.New(lastRoot, host.sdb)
	if err != nil {
		t.Fatalf("block %d: state.New: %v", block, err)
	}
	tdb := &traceStateDB{StateDB: statedb, rec: rec}
	if rec != nil {
		rec.beginBlock(block)
	}

	// System writes: miner reward.
	miner := compareBlockMiners[block]
	if miner != (common.Address{}) {
		tdb.AddBalance(miner, reward, tracing.BalanceChangeUnspecified)
	}
	// System writes: pre-allocate sender balances (matches existing replay).
	if len(msgs) > 0 {
		for _, m := range msgs {
			tdb.SetBalance(m.From, balance, tracing.BalanceChangeUnspecified)
		}
	}

	blockCtx := vm.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     func(n uint64) common.Hash { return common.Hash{} },
		Coinbase:    miner,
		BlockNumber: new(big.Int).SetUint64(block),
		Time:        compareBlockTimestamps[block],
		Difficulty:  big.NewInt(1),
		Random:      &common.Hash{},
		GasLimit:    1000000000,
		BaseFee:     big.NewInt(0),
		BlobBaseFee: big.NewInt(0),
	}
	evm := vm.NewEVM(blockCtx, tdb, params.MainnetChainConfig, vm.Config{})

	for i, m := range msgs {
		if rec != nil {
			rec.beginTx(i)
		}
		m.SkipNonceChecks = true
		_, err := core.ApplyMessage(evm, m, new(core.GasPool).AddGas(m.GasLimit))
		if err == nil {
			if rec != nil {
				rec.successTxs++
			}
		} else {
			if rec != nil {
				rec.failedTxs++
			}
			if (*traceVerbose || block%1000 == 0) && rec != nil {
				toStr := "contract-creation"
				if m.To != nil {
					toStr = m.To.Hex()
				}
				fmt.Printf("[TRACE] tx fail block=%d idx=%d from=%s to=%s err=%v\n", block, i, m.From.Hex(), toStr, err)
			}
		}
	}

	statedb.Finalise(false)
	if _, _, err := statedb.PreCommit(false); err != nil {
		t.Fatalf("block %d: pre-commit: %v", block, err)
	}
	h, err := statedb.PostCommit(block, false, false)
	if err != nil {
		t.Fatalf("block %d: post-commit: %v", block, err)
	}
	if rec != nil {
		rec.endBlock(block, len(msgs) == 0)
		if compareBlockTimestamps[block] == 0 {
			rec.markMissing(block)
		}
	}
	return h
}

func TestExportStateAccessTrace(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}
	common.DebugFlag = false
	state.KVSkipKnown = *traceSkipKnown

	outDir := *traceOutDir
	if outDir == "" {
		outDir = filepath.Join("F:\\codex_asct\\mainnet_state_access_trace", time.Now().Format("20060102"))
	}
	fmt.Printf("[TRACE] output dir: %s\n", outDir)

	cfg := &ProcessorConfig{
		DbDir:        filepath.Join(outDir, "trace_state_db"),
		DataDir:      *dataDir,
		StartFileIdx: *startIdx,
		EndFileIdx:   *endIdx,
		UseKV:        true,
		UseMemory:    *traceUseMemory,
		StartNum:     46147,
		MaxBlocks:    *maxBlocks,
	}
	fmt.Printf("[TRACE] dataDir=%s files=%d..%d useMemory=%t maxBlocks=%d\n",
		cfg.DataDir, cfg.StartFileIdx, cfg.EndFileIdx, cfg.UseMemory, cfg.MaxBlocks)

	host, err := NewProcessorHost(cfg)
	if err != nil {
		t.Fatalf("create host: %v", err)
	}
	defer host.Close()

	gspec := &core.Genesis{Config: params.TestChainConfig, Alloc: core.GenesisAlloc{}}
	genesis := gspec.MustCommit(host.db, host.trieDB)
	lastRoot := genesis.Root()

	files, err := compareFindTransactionFiles(cfg.DataDir)
	if err != nil || len(files) == 0 {
		t.Fatalf("no transaction files found: %v", err)
	}
	if cfg.StartFileIdx < 1 || cfg.EndFileIdx > len(files) || cfg.StartFileIdx > cfg.EndFileIdx {
		t.Fatalf("bad file indices: start=%d end=%d total=%d", cfg.StartFileIdx, cfg.EndFileIdx, len(files))
	}
	selectedFiles := files[cfg.StartFileIdx-1 : cfg.EndFileIdx]
	fmt.Printf("[TRACE] selected %d transaction files: %s .. %s\n", len(selectedFiles),
		filepath.Base(selectedFiles[0]), filepath.Base(selectedFiles[len(selectedFiles)-1]))

	rec, err := newTraceRecorder(outDir, *traceChunkBlocks, *traceValues, *traceGzip)
	if err != nil {
		t.Fatalf("create recorder: %v", err)
	}

	bigBalance := new(big.Int).Mul(big.NewInt(1e15), big.NewInt(1e18)) // 1M ETH per sender
	balance, _ := uint256.FromBig(bigBalance)
	reward, _ := uint256.FromBig(new(big.Int).Mul(big.NewInt(1e6), big.NewInt(1e18)))

	var (
		currentBlock uint64
		blockInit    bool
		processed    uint64
	)

processFiles:
	for _, file := range selectedFiles {
		fmt.Printf("[TRACE] processing file %s\n", filepath.Base(file))
		fileIdx := compareGetFileIndex(file)
		_ = compareLoadBlockTimestampsFromFile(cfg.DataDir, fileIdx)

		ts, err := NewTransactionStreamer(file)
		if err != nil {
			t.Errorf("open streamer %s: %v", file, err)
			continue
		}

		firstBlock, ok := ts.PeekBlockNum()
		if !ok {
			ts.Close()
			continue
		}
		if !blockInit {
			currentBlock = firstBlock
			blockInit = true
		}

		for {
			targetBlock, ok := ts.PeekBlockNum()
			if !ok {
				break
			}
			if targetBlock < currentBlock {
				ts.PopBlock(targetBlock) // duplicate/out-of-order shard overlap: skip
				continue
			}
			if currentBlock%100000 == 0 {
				fmt.Printf("[TRACE] block=%d processed=%d rows=%d elapsed=%v\n", currentBlock, processed, rec.totalRows, time.Since(rec.startTime))
			}
			// Process empty blocks between currentBlock and targetBlock.
			for currentBlock < targetBlock {
				if cfg.MaxBlocks > 0 && processed >= uint64(cfg.MaxBlocks) {
					break processFiles
				}
				lastRoot = processTraceBlock(t, host, traceRecFor(rec, currentBlock), currentBlock, lastRoot, nil, balance, reward)
				processed++
				currentBlock++
			}
			if cfg.MaxBlocks > 0 && processed >= uint64(cfg.MaxBlocks) {
				break
			}
			msgs, _ := ts.PopBlock(targetBlock)
			lastRoot = processTraceBlock(t, host, traceRecFor(rec, targetBlock), targetBlock, lastRoot, msgs, balance, reward)
			processed++
			currentBlock++
		}
		ts.Close()
	}

	man, err := rec.finish()
	if err != nil {
		t.Fatalf("finalize trace: %v", err)
	}
	man.SourceCommit = traceGitCommit()
	man.Command = strings.Join(os.Args, " ")
	man.SourceFiles = make([]string, 0, len(selectedFiles))
	for _, f := range selectedFiles {
		man.SourceFiles = append(man.SourceFiles, filepath.Base(f))
	}

	buf, _ := json.MarshalIndent(man, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, "manifest.json"), buf, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	var sb strings.Builder
	for _, fi := range man.Files {
		fmt.Fprintf(&sb, "%s  %s\n", fi.SHA256, fi.Name)
	}
	if err := os.WriteFile(filepath.Join(outDir, "SHA256SUMS"), []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write SHA256SUMS: %v", err)
	}

	fmt.Printf("\n[TRACE] DONE in %v\n", time.Since(rec.startTime))
	fmt.Printf("[TRACE] blocks=%d (empty=%d) txs=%d success=%d failed=%d rows=%d seqVio=%d missing=%d compressed=%t\n",
		man.TotalBlocks, man.EmptyBlocks, man.TotalTxs, man.SuccessTxs, man.FailedTxs, man.TotalRows, man.SeqViolations, len(man.MissingBlocks), man.Compressed)
	fmt.Printf("[TRACE] op counts: %v\n", man.OpCounts)
	fmt.Printf("[TRACE] type counts: %v\n", man.TypeCounts)
	for _, fi := range man.Files {
		fmt.Printf("[TRACE] %s rows=%d first=%d last=%d sha256=%s\n", fi.Name, fi.Rows, fi.FirstBlock, fi.LastBlock, fi.SHA256[:16])
	}
	if man.SeqViolations > 0 {
		t.Errorf("seq violations: %d", man.SeqViolations)
	}
}

func traceGitCommit() string {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

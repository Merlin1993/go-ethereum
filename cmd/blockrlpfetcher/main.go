package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net/url"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
)

type blockRecord struct {
	Number     uint64 `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parent_hash"`
	StateRoot  string `json:"state_root"`
	TxCount    int    `json:"tx_count"`
	Time       uint64 `json:"time"`
}

type manifest struct {
	RPC       string        `json:"rpc"`
	From      uint64        `json:"from"`
	To        uint64        `json:"to"`
	Count     uint64        `json:"count"`
	StartedAt string        `json:"started_at"`
	EndedAt   string        `json:"ended_at"`
	Blocks    []blockRecord `json:"blocks"`
}

func main() {
	rpcURL := flag.String("rpc", "", "Ethereum JSON-RPC endpoint")
	from := flag.Uint64("from", 0, "first block number to fetch")
	count := flag.Uint64("count", 0, "number of blocks to fetch")
	out := flag.String("out", "", "output RLP file")
	manifestPath := flag.String("manifest", "", "output manifest JSON file")
	expectedParent := flag.String("parent", "", "expected parent hash of the first fetched block")
	timeout := flag.Duration("timeout", 30*time.Second, "per-RPC-call timeout")
	retries := flag.Int("retries", 3, "RPC retries per block")
	flag.Parse()

	if *rpcURL == "" || *from == 0 || *count == 0 || *out == "" || *manifestPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *retries < 1 {
		*retries = 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := ethclient.DialContext(ctx, *rpcURL)
	cancel()
	if err != nil {
		log.Fatalf("dial rpc: %v", err)
	}
	defer client.Close()

	file, err := os.Create(*out)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer file.Close()

	m := manifest{
		RPC:       sanitizeRPC(*rpcURL),
		From:      *from,
		To:        *from + *count - 1,
		Count:     *count,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		Blocks:    make([]blockRecord, 0, *count),
	}

	var wantParent common.Hash
	if *expectedParent != "" {
		wantParent = common.HexToHash(*expectedParent)
	}
	for i := uint64(0); i < *count; i++ {
		number := *from + i
		block, err := fetchBlock(client, number, *timeout, *retries)
		if err != nil {
			log.Fatalf("fetch block %d: %v", number, err)
		}
		if block.NumberU64() != number {
			log.Fatalf("rpc returned block %d while fetching %d", block.NumberU64(), number)
		}
		if i == 0 && *expectedParent != "" && block.ParentHash() != wantParent {
			log.Fatalf("first block parent mismatch: have %s want %s", block.ParentHash(), wantParent)
		}
		if i > 0 && block.ParentHash() != common.HexToHash(m.Blocks[len(m.Blocks)-1].Hash) {
			log.Fatalf("chain discontinuity at block %d: parent %s previous %s", number, block.ParentHash(), m.Blocks[len(m.Blocks)-1].Hash)
		}
		if err := rlp.Encode(file, block); err != nil {
			log.Fatalf("encode block %d: %v", number, err)
		}
		header := block.Header()
		m.Blocks = append(m.Blocks, blockRecord{
			Number:     number,
			Hash:       block.Hash().Hex(),
			ParentHash: block.ParentHash().Hex(),
			StateRoot:  header.Root.Hex(),
			TxCount:    len(block.Transactions()),
			Time:       header.Time,
		})
		fmt.Printf("fetched block %d %s txs=%d\n", number, block.Hash(), len(block.Transactions()))
	}
	m.EndedAt = time.Now().UTC().Format(time.RFC3339)

	manifestFile, err := os.Create(*manifestPath)
	if err != nil {
		log.Fatalf("create manifest: %v", err)
	}
	defer manifestFile.Close()
	enc := json.NewEncoder(manifestFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		log.Fatalf("write manifest: %v", err)
	}
}

func fetchBlock(client *ethclient.Client, number uint64, timeout time.Duration, retries int) (*types.Block, error) {
	var last error
	for attempt := 1; attempt <= retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		block, err := client.BlockByNumber(ctx, new(big.Int).SetUint64(number))
		cancel()
		if err == nil {
			return block, nil
		}
		last = err
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if last == nil {
		last = errors.New("unknown rpc error")
	}
	return nil, last
}

func sanitizeRPC(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "redacted"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

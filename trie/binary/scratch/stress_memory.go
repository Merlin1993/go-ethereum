package main

import (
	"fmt"
	"github.com/ethereum/go-ethereum/trie/binary"
	"math/rand"
	"time"
)

func main() {
	db := binary.NewMemoryDBAdapter()
	hasher := binary.NewPooledKeccakHasher()
	config := binary.DefaultConfig()
	config.ShardDepth = 4 // 16 shards
	config.ArchiveDB = db
	trie := binary.NewTrie(nil, db, hasher, config, true)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = make([]byte, 32)
		r.Read(keys[i])
	}

	fmt.Println("Starting light stress test in memory...")
	for cycle := 0; cycle < 5; cycle++ {
		fmt.Printf("Cycle %d starting...\n", cycle)
		for i := 0; i < 1000; i++ {
			trie.Update(keys[i], []byte(fmt.Sprintf("value-%d-%d", cycle, i)))
			if i%100 == 0 {
				trie.Commit()
				trie.PruneNextShard()
			}
		}
		trie.Commit()
		trie.FlushArchives()

		stats := trie.Stats()
		fmt.Printf("Cycle %d done. ArchivedDataSize: %d, BucketCount: %d\n", cycle, stats.ArchivedDataSize, stats.BucketCount)
	}
	fmt.Println("Stress test finished successfully.")
}

package main

import (
	"fmt"
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
)

func main() {
	// Create an in-memory database
	db := rawdb.NewMemoryDatabase()

	// Create a new StateDB
	sdb, err := state.New(common.Hash{}, db)
	if err != nil {
		log.Fatal("Failed to create StateDB:", err)
	}

	fmt.Println("Starting IntermediateRoot performance statistics test...")

	// Simulate multiple calls to IntermediateRoot
	for i := 0; i < 50000; i++ {
		// Create some test accounts
		addr := common.BytesToAddress([]byte{byte(i % 256), byte((i + 1) % 256), byte((i + 2) % 256)})
		sdb.CreateAccount(addr)
		sdb.SetBalance(addr, common.U2561, nil)

		// Call IntermediateRoot
		_, _ = sdb.IntermediateRoot(true)

		// Print progress every 10000 iterations
		if (i+1)%10000 == 0 {
			fmt.Printf("Completed %d calls\n", i+1)
		}
	}

	// Get final statistics
	stats := sdb.GetIntermediateRootStats()
	fmt.Println("\n=== IntermediateRoot Performance Statistics ===")
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	// Simulate some real transaction scenarios
	fmt.Println("\nStarting real transaction scenario simulation...")

	// Create some contract accounts
	for i := 0; i < 1000; i++ {
		addr := common.BytesToAddress([]byte{byte(i % 256), byte((i + 1) % 256), byte((i + 2) % 256)})
		sdb.CreateAccount(addr)
		sdb.SetBalance(addr, common.U2561, nil)

		// Set some storage state
		for j := 0; j < 10; j++ {
			key := common.BytesToHash([]byte{byte(j), byte(j + 1), byte(j + 2)})
			value := common.BytesToHash([]byte{byte(i), byte(i + 1), byte(i + 2)})
			sdb.SetState(addr, key, value)
		}

		// Call IntermediateRoot
		_, _ = sdb.IntermediateRoot(true)
	}

	// Get statistics again
	stats = sdb.GetIntermediateRootStats()
	fmt.Println("\n=== Final Performance Statistics ===")
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}
}

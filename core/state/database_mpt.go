// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"github.com/ethereum/go-ethereum/cachetrie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// MPTDatabase is an implementation of Database interface for Merkle Patricia Tries.
// It leverages both trie and state snapshot to provide functionalities for state
// access.
type MPTDatabase struct {
	triedb *triedb.Database
	codedb *CodeDB
	snap   *snapshot.Tree
}

// Type returns Merkle, indicating this database is backed by a Merkle Patricia Trie.
func (db *MPTDatabase) Type() DatabaseType { return TypeMPT }

// NewMPTDatabase creates a state database with the Merkle Patricia Trie manner.
func NewMPTDatabase(tdb *triedb.Database, codedb *CodeDB) *MPTDatabase {
	if codedb == nil {
		codedb = NewCodeDB(tdb.Disk())
	}
	db := &MPTDatabase{
		triedb: tdb,
		codedb: codedb,
	}
	if cache := tdb.CacheTrie(); cache != nil {
		cache.SetMergeFunc(db.mergeCacheTrieInputs)
	}
	return db
}

// WithSnapshot configures the provided state snapshot. Note that this
// registration must be performed before the MPTDatabase is used.
func (db *MPTDatabase) WithSnapshot(snapshot *snapshot.Tree) Database {
	db.snap = snapshot
	return db
}

// StateReader returns a state reader associated with the specified state root.
func (db *MPTDatabase) StateReader(stateRoot common.Hash) (StateReader, error) {
	return db.stateReader(stateRoot, true)
}

func (db *MPTDatabase) stateReader(stateRoot common.Hash, useCacheTrie bool) (StateReader, error) {
	var readers []StateReader

	if cache := db.triedb.CacheTrie(); useCacheTrie && cache != nil && cache.Available(stateRoot) {
		readers = append(readers, newCacheTrieReader(cache))
	}
	// Configure the state reader using the standalone snapshot in hash mode.
	// This reader offers improved performance but is optional and only
	// partially useful if the snapshot is not fully generated.
	if db.TrieDB().Scheme() == rawdb.HashScheme && db.snap != nil {
		snap := db.snap.Snapshot(stateRoot)
		if snap != nil {
			readers = append(readers, newFlatReader(snap))
		}
	}
	// Configure the state reader using the path database in path mode.
	// This reader offers improved performance but is optional and only
	// partially useful if the snapshot data in path database is not
	// fully generated.
	if db.TrieDB().Scheme() == rawdb.PathScheme {
		reader, err := db.triedb.StateReader(stateRoot)
		if err == nil {
			readers = append(readers, newFlatReader(reader))
		}
	}
	// Configure the trie reader, which is expected to be available as the
	// gatekeeper unless the state is corrupted.
	tr, err := newMPTTrieReader(stateRoot, db.triedb)
	if err != nil {
		return nil, err
	}
	readers = append(readers, tr)

	return newMultiStateReader(readers...)
}

// Reader implements Database, returning a reader associated with the specified
// state root.
func (db *MPTDatabase) Reader(stateRoot common.Hash) (Reader, error) {
	sr, err := db.stateReader(stateRoot, true)
	if err != nil {
		return nil, err
	}
	return newReader(db.codedb.Reader(), sr), nil
}

// ReadersWithCacheStats creates a pair of state readers that share the same
// underlying state reader and internal state cache, while maintaining separate
// statistics respectively.
func (db *MPTDatabase) ReadersWithCacheStats(stateRoot common.Hash) (Reader, Reader, error) {
	r, err := db.StateReader(stateRoot)
	if err != nil {
		return nil, nil, err
	}
	sr := newStateReaderWithCache(r)
	ra := newReader(db.codedb.Reader(), newStateReaderWithStats(sr))
	rb := newReader(db.codedb.Reader(), newStateReaderWithStats(sr))
	return ra, rb, nil
}

// OpenTrie opens the main account trie at a specific root hash.
func (db *MPTDatabase) OpenTrie(root common.Hash) (Trie, error) {
	tr, err := trie.NewStateTrie(trie.StateTrieID(root), db.triedb)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// OpenStorageTrie opens the storage trie of an account.
func (db *MPTDatabase) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, self Trie) (Trie, error) {
	tr, err := trie.NewStateTrie(trie.StorageTrieID(stateRoot, crypto.Keccak256Hash(address.Bytes()), root), db.triedb)
	if err != nil {
		return nil, err
	}
	return tr, nil
}

// TrieDB retrieves any intermediate trie-node caching layer.
func (db *MPTDatabase) TrieDB() *triedb.Database {
	return db.triedb
}

func (db *MPTDatabase) setCacheTrieBlock(block uint64) {
	if cache := db.triedb.CacheTrie(); cache != nil {
		cache.SetBlockNum(block)
	}
}

func (db *MPTDatabase) beginCacheTrieBlock(block uint64, origin common.Hash) {
	if cache := db.triedb.CacheTrie(); cache != nil {
		cache.Begin(block, origin)
	}
}

func (db *MPTDatabase) stageCacheTrieAccount(address common.Address, account *types.StateAccount) {
	if cache := db.triedb.CacheTrie(); cache != nil {
		cache.StageAccount(address, account)
	}
}

func (db *MPTDatabase) stageCacheTrieStorage(address common.Address, slot common.Hash, value common.Hash) {
	if cache := db.triedb.CacheTrie(); cache != nil {
		cache.StageStorage(address, slot, value)
	}
}

// Commit flushes all pending writes and finalizes the state transition,
// committing the changes to the underlying storage. It returns an error
// if the commit fails.
func (db *MPTDatabase) Commit(update *StateUpdate) error {
	return db.commit(update, true)
}

func (db *MPTDatabase) commit(update *StateUpdate, allowCacheTrie bool) error {
	// Short circuit if nothing to commit
	if update.Empty() {
		if allowCacheTrie && update.CacheTrieAsync {
			if cache := db.triedb.CacheTrie(); cache != nil {
				cache.PublishRoots(update.BlockNumber, update.OriginRoot, update.Root, nil, nil)
			}
		}
		return nil
	}
	// Commit dirty contract code if any exists
	if len(update.Codes) > 0 {
		batch := db.codedb.NewBatchWithSize(len(update.Codes))
		for _, code := range update.Codes {
			batch.Put(code.Hash, code.Blob)
		}
		if err := batch.Commit(); err != nil {
			return err
		}
	}
	if allowCacheTrie && update.CacheTrieAsync {
		db.updateCacheTrie(update)
		return nil
	}
	// Encode the state mutations in the MPT format
	accounts, accountOrigin, storages, storageOrigin := update.EncodeMPTState()

	// If snapshotting is enabled, update the snapshot tree with this new version
	if db.snap != nil && db.snap.Snapshot(update.OriginRoot) != nil {
		if err := db.snap.Update(update.Root, update.OriginRoot, accounts, storages); err != nil {
			log.Warn("Failed to update snapshot tree", "from", update.OriginRoot, "to", update.Root, "err", err)
		}
		// Keep 128 diff layers in the memory, persistent layer is 129th.
		// - head layer is paired with HEAD state
		// - head-1 layer is paired with HEAD-1 state
		// - head-127 layer(bottom-most diff layer) is paired with HEAD-127 state
		if err := db.snap.Cap(update.Root, TriesInMemory); err != nil {
			log.Warn("Failed to cap snapshot tree", "root", update.Root, "layers", TriesInMemory, "err", err)
		}
	}
	if err := db.triedb.Update(update.Root, update.OriginRoot, update.BlockNumber, update.Nodes, &triedb.StateSet{
		Accounts:       accounts,
		AccountsOrigin: accountOrigin,
		Storages:       storages,
		StoragesOrigin: storageOrigin,
		RawStorageKey:  update.StorageKeyType == StorageKeyPlain,
	}); err != nil {
		return err
	}
	if allowCacheTrie {
		db.updateCacheTrie(update)
	}
	return nil
}

func (db *MPTDatabase) updateCacheTrie(update *StateUpdate) {
	cache := db.triedb.CacheTrie()
	if cache == nil {
		return
	}
	accounts := make(map[common.Address]*types.StateAccount, len(update.AccountsOrigin))
	for address := range update.AccountsOrigin {
		hash := crypto.Keccak256Hash(address.Bytes())
		account, ok := update.Accounts[hash]
		if !ok {
			continue
		}
		accounts[address] = account
	}
	storages := make(map[common.Address]map[common.Hash]common.Hash)
	if update.StorageKeyType == StorageKeyPlain {
		for address, origins := range update.StoragesOrigin {
			addrHash := crypto.Keccak256Hash(address.Bytes())
			writes := update.Storages[addrHash]
			if len(writes) == 0 {
				continue
			}
			for plainKey := range origins {
				keyHash := crypto.Keccak256Hash(plainKey.Bytes())
				value, ok := writes[keyHash]
				if !ok {
					continue
				}
				if storages[address] == nil {
					storages[address] = make(map[common.Hash]common.Hash)
				}
				storages[address][plainKey] = value
			}
		}
	}
	cache.PublishRoots(update.BlockNumber, update.OriginRoot, update.Root, accounts, storages)
	if !update.CacheTrieAsync {
		cache.SetRoot(update.Root)
	}
}

func (db *MPTDatabase) cacheTrieRoots() (common.Hash, common.Hash, bool) {
	cache := db.triedb.CacheTrie()
	if cache == nil {
		return common.Hash{}, common.Hash{}, false
	}
	return cache.PreviewRoots()
}

func (db *MPTDatabase) mergeCacheTrieInputs(root common.Hash, block uint64, inputs []cachetrie.MergeInput) (common.Hash, error) {
	mergedb := &mptNoCacheDatabase{db: db}
	statedb, err := New(root, mergedb)
	if err != nil {
		return common.Hash{}, err
	}
	statedb.SetBlockNum(block)
	for _, input := range inputs {
		if input.Type != cachetrie.StorageState || input.StorageKey == nil {
			continue
		}
		value := common.Hash{}
		if !input.Tombstone {
			value = input.Storage
		}
		statedb.SetState(input.Address, *input.StorageKey, value)
	}
	for _, input := range inputs {
		if input.Type != cachetrie.AccountState {
			continue
		}
		statedb.setCacheTrieMergeAccount(input.Address, input.Account, input.Tombstone)
	}
	return statedb.Commit(block, true, false)
}

type mptNoCacheDatabase struct {
	db *MPTDatabase
}

func (db *mptNoCacheDatabase) Type() DatabaseType { return db.db.Type() }

func (db *mptNoCacheDatabase) Reader(root common.Hash) (Reader, error) {
	sr, err := db.db.stateReader(root, false)
	if err != nil {
		return nil, err
	}
	return newReader(db.db.codedb.Reader(), sr), nil
}

func (db *mptNoCacheDatabase) Iteratee(root common.Hash) (Iteratee, error) {
	return db.db.Iteratee(root)
}

func (db *mptNoCacheDatabase) OpenTrie(root common.Hash) (Trie, error) {
	return db.db.OpenTrie(root)
}

func (db *mptNoCacheDatabase) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, self Trie) (Trie, error) {
	return db.db.OpenStorageTrie(stateRoot, address, root, self)
}

func (db *mptNoCacheDatabase) TrieDB() *triedb.Database {
	return db.db.TrieDB()
}

func (db *mptNoCacheDatabase) Commit(update *StateUpdate) error {
	return db.db.commit(update, false)
}

// Iteratee returns a state iteratee associated with the specified state root,
// through which the account iterator and storage iterator can be created.
func (db *MPTDatabase) Iteratee(root common.Hash) (Iteratee, error) {
	return newStateIteratee(true, root, db.triedb, db.snap)
}

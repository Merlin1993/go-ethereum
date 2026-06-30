package state

import (
	"encoding/binary"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

const (
	kvBlocksPerYear  = uint64(365 * 24 * 60 * 60 / 12)
	kvBlocksPerHalf  = kvBlocksPerYear / 2
	kvBlocksPerThree = kvBlocksPerYear / 4
)

var (
	kvAccountPrefix = []byte("kvstate:account:")
	kvStoragePrefix = []byte("kvstate:storage:")
)

// KVAccessStats records how many state accesses in the current block touch data
// last updated within common recency windows.
type KVAccessStats struct {
	Block uint64

	Reads           uint64
	Read3M          uint64
	Read6M          uint64
	Read1Y          uint64
	ReadMisses      uint64
	ReadNonExistent uint64

	Writes           uint64
	Write3M          uint64
	Write6M          uint64
	Write1Y          uint64
	WriteMisses      uint64
	WriteNonExistent uint64
}

// kvStateBackend is implemented by the direct key/value state backend.
type kvStateBackend interface {
	CommitKV(block uint64, update *stateUpdate) error
	KVBlockStats() KVAccessStats
}

// kvAccessRecorder is used by StateDB to count EVM-level state accesses.
type kvAccessRecorder interface {
	RecordAccountRead(address common.Address)
	RecordAccountWrite(address common.Address)
	RecordStorageRead(address common.Address, slot common.Hash)
	RecordStorageWrite(address common.Address, slot common.Hash)
}

type kvStoredValue struct {
	UpdateBlock uint64
	Value       []byte
}

func encodeKVStoredValue(block uint64, value []byte) []byte {
	blob := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(blob[:8], block)
	copy(blob[8:], value)
	return blob
}

func decodeKVStoredValue(blob []byte) (kvStoredValue, bool) {
	if len(blob) < 8 {
		return kvStoredValue{}, false
	}
	return kvStoredValue{
		UpdateBlock: binary.BigEndian.Uint64(blob[:8]),
		Value:       common.CopyBytes(blob[8:]),
	}, true
}

// KVDatabase is a state.Database implementation backed by direct key/value
// records. It deliberately skips trie node generation and cryptographic state
// root hashing; each committed key stores the block number of its last update.
type KVDatabase struct {
	disk ethdb.Database

	dummyTrieDB *triedb.Database
	codeCache   *lru.SizeConstrainedCache[common.Hash, []byte]
	codeSizes   *lru.Cache[common.Hash, int]
	points      *utils.PointCache

	lock         sync.RWMutex
	currentBlock uint64
	pending      map[string]*kvPending
	pendingCodes map[common.Hash][]byte
	known        map[string]uint64
	stats        KVAccessStats
}

type kvPending struct {
	key    []byte
	value  []byte
	delete bool
}

// NewKVDatabase creates a no-hash key/value state backend. The supplied database
// stores accounts, storage slots, and contract code.
func NewKVDatabase(disk ethdb.Database) *KVDatabase {
	return &KVDatabase{
		disk:         disk,
		dummyTrieDB:  triedb.NewDatabase(rawdb.NewMemoryDatabase(), nil),
		codeCache:    lru.NewSizeConstrainedCache[common.Hash, []byte](codeCacheSize),
		codeSizes:    lru.NewCache[common.Hash, int](codeSizeCacheSize),
		points:       utils.NewPointCache(pointCacheSize),
		pending:      make(map[string]*kvPending),
		pendingCodes: make(map[common.Hash][]byte),
		known:        make(map[string]uint64),
	}
}

func (db *KVDatabase) Reader(root common.Hash) (Reader, error) {
	return newReader(newCachingCodeReader(db.disk, db.codeCache, db.codeSizes), db), nil
}

func (db *KVDatabase) OpenTrie(root common.Hash) (Trie, error) {
	return &kvTrie{db: db}, nil
}

func (db *KVDatabase) OpenStorageTrie(stateRoot common.Hash, address common.Address, root common.Hash, self Trie) (Trie, error) {
	return &kvTrie{db: db, owner: address, storage: true}, nil
}

func (db *KVDatabase) PointCache() *utils.PointCache { return db.points }

func (db *KVDatabase) TrieDB() *triedb.Database { return db.dummyTrieDB }

func (db *KVDatabase) Snapshot() *snapshot.Tree { return nil }

func (db *KVDatabase) SetBlockNum(num uint64) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.currentBlock = num
	db.pending = make(map[string]*kvPending)
	db.stats = KVAccessStats{Block: num}
}

func (db *KVDatabase) Account(addr common.Address) (*types.StateAccount, error) {
	blob, ok, err := db.readValue(accountKVKey(addr))
	if err != nil || !ok {
		return nil, err
	}
	return types.FullAccount(blob)
}

func (db *KVDatabase) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	blob, ok, err := db.readValue(storageKVKey(addr, slot[:]))
	if err != nil || !ok {
		return common.Hash{}, err
	}
	var value common.Hash
	value.SetBytes(blob)
	return value, nil
}

func (db *KVDatabase) CommitKV(block uint64, update *stateUpdate) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	batch := db.disk.NewBatch()
	for _, pending := range db.pending {
		if pending.delete {
			if err := batch.Delete(pending.key); err != nil {
				return err
			}
			delete(db.known, string(pending.key))
			continue
		}
		if err := batch.Put(pending.key, encodeKVStoredValue(block, pending.value)); err != nil {
			return err
		}
		db.known[string(pending.key)] = block
	}
	for _, code := range update.codes {
		db.pendingCodes[code.hash] = common.CopyBytes(code.blob)
	}
	for hash, code := range db.pendingCodes {
		rawdb.WriteCode(batch, hash, code)
		db.codeCache.Add(hash, code)
		db.codeSizes.Add(hash, len(code))
	}
	if err := batch.Write(); err != nil {
		return err
	}
	db.pending = make(map[string]*kvPending)
	db.pendingCodes = make(map[common.Hash][]byte)
	return nil
}

func (db *KVDatabase) KVBlockStats() KVAccessStats {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.stats
}

func (db *KVDatabase) RecordAccountRead(address common.Address) {
	db.recordAccess(accountKVKey(address), false)
}

func (db *KVDatabase) RecordAccountWrite(address common.Address) {
	db.recordAccess(accountKVKey(address), true)
}

func (db *KVDatabase) RecordStorageRead(address common.Address, slot common.Hash) {
	db.recordAccess(storageKVKey(address, slot[:]), false)
}

func (db *KVDatabase) RecordStorageWrite(address common.Address, slot common.Hash) {
	db.recordAccess(storageKVKey(address, slot[:]), true)
}

func (db *KVDatabase) stagePut(key, value []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.pending[string(key)] = &kvPending{
		key:   common.CopyBytes(key),
		value: common.CopyBytes(value),
	}
}

func (db *KVDatabase) stageDelete(key []byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.pending[string(key)] = &kvPending{
		key:    common.CopyBytes(key),
		delete: true,
	}
}

func (db *KVDatabase) readValue(key []byte) ([]byte, bool, error) {
	id := string(key)

	db.lock.RLock()
	if pending, ok := db.pending[id]; ok {
		db.lock.RUnlock()
		if pending.delete {
			return nil, false, nil
		}
		return common.CopyBytes(pending.value), true, nil
	}
	if _, ok := db.known[id]; !ok {
		db.lock.RUnlock()
		return nil, false, nil
	}
	db.lock.RUnlock()

	blob, err := db.disk.Get(key)
	if err != nil {
		return nil, false, nil
	}
	stored, ok := decodeKVStoredValue(blob)
	if !ok {
		return nil, false, nil
	}
	return stored.Value, true, nil
}

func (db *KVDatabase) recordAccess(key []byte, write bool) {
	db.lock.Lock()
	defer db.lock.Unlock()

	var updateBlock uint64
	exists := false
	if pending, ok := db.pending[string(key)]; ok {
		exists = !pending.delete
		updateBlock = db.currentBlock
	} else if block, ok := db.known[string(key)]; ok {
		exists = true
		updateBlock = block
	}
	db.bumpStats(&db.stats, write, exists, updateBlock)
}

func (db *KVDatabase) bumpStats(stats *KVAccessStats, write bool, exists bool, updateBlock uint64) {
	if write {
		stats.Writes++
		if !exists {
			stats.WriteMisses++
			stats.WriteNonExistent++
			return
		}
		db.bumpWriteWindows(stats, updateBlock)
		return
	}
	stats.Reads++
	if !exists {
		stats.ReadMisses++
		stats.ReadNonExistent++
		return
	}
	db.bumpReadWindows(stats, updateBlock)
}

func (db *KVDatabase) bumpReadWindows(stats *KVAccessStats, updateBlock uint64) {
	if updateBlock > db.currentBlock {
		return
	}
	age := db.currentBlock - updateBlock
	if age <= kvBlocksPerThree {
		stats.Read3M++
	}
	if age <= kvBlocksPerHalf {
		stats.Read6M++
	}
	if age <= kvBlocksPerYear {
		stats.Read1Y++
	}
}

func (db *KVDatabase) bumpWriteWindows(stats *KVAccessStats, updateBlock uint64) {
	if updateBlock > db.currentBlock {
		return
	}
	age := db.currentBlock - updateBlock
	if age <= kvBlocksPerThree {
		stats.Write3M++
	}
	if age <= kvBlocksPerHalf {
		stats.Write6M++
	}
	if age <= kvBlocksPerYear {
		stats.Write1Y++
	}
}

type kvTrie struct {
	db      *KVDatabase
	owner   common.Address
	storage bool
}

func (t *kvTrie) Copy() Trie {
	return &kvTrie{
		db:      t.db,
		owner:   t.owner,
		storage: t.storage,
	}
}

func (t *kvTrie) GetKey(key []byte) []byte { return common.CopyBytes(key) }

func (t *kvTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	return t.db.Account(address)
}

func (t *kvTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	value, ok, err := t.db.readValue(storageKVKey(addr, key))
	if err != nil || !ok {
		return nil, err
	}
	return value, nil
}

func (t *kvTrie) UpdateAccount(address common.Address, account *types.StateAccount, codeLen int) error {
	t.db.stagePut(accountKVKey(address), types.SlimAccountRLP(*account))
	return nil
}

func (t *kvTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	t.db.stagePut(accountKVKey(address), account)
	return nil
}

func (t *kvTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	t.db.stagePut(storageKVKey(addr, key), value)
	return nil
}

func (t *kvTrie) DeleteAccount(address common.Address) error {
	t.db.stageDelete(accountKVKey(address))
	return nil
}

func (t *kvTrie) DeleteStorage(addr common.Address, key []byte) error {
	t.db.stageDelete(storageKVKey(addr, key))
	return nil
}

func (t *kvTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	t.db.lock.Lock()
	defer t.db.lock.Unlock()

	t.db.pendingCodes[codeHash] = common.CopyBytes(code)
	return nil
}

func (t *kvTrie) Hash() common.Hash { return common.Hash{} }

func (t *kvTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	return common.Hash{}, nil
}

func (t *kvTrie) Witness() map[string]struct{} { return nil }

func (t *kvTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return emptyKVNodeIterator{}, nil
}

func (t *kvTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error { return nil }

func (t *kvTrie) IsVerkle() bool { return false }

func (t *kvTrie) PruneNextShard() error { return nil }

type emptyKVNodeIterator struct{}

func (emptyKVNodeIterator) Next(bool) bool                { return false }
func (emptyKVNodeIterator) Error() error                  { return nil }
func (emptyKVNodeIterator) Hash() common.Hash             { return common.Hash{} }
func (emptyKVNodeIterator) Parent() common.Hash           { return common.Hash{} }
func (emptyKVNodeIterator) Path() []byte                  { return nil }
func (emptyKVNodeIterator) NodeBlob() []byte              { return nil }
func (emptyKVNodeIterator) Leaf() bool                    { return false }
func (emptyKVNodeIterator) LeafKey() []byte               { panic("not at leaf") }
func (emptyKVNodeIterator) LeafBlob() []byte              { panic("not at leaf") }
func (emptyKVNodeIterator) LeafProof() [][]byte           { panic("not at leaf") }
func (emptyKVNodeIterator) AddResolver(trie.NodeResolver) {}

func accountKVKey(addr common.Address) []byte {
	key := make([]byte, 0, len(kvAccountPrefix)+common.AddressLength)
	key = append(key, kvAccountPrefix...)
	key = append(key, addr[:]...)
	return key
}

func storageKVKey(addr common.Address, slot []byte) []byte {
	key := make([]byte, 0, len(kvStoragePrefix)+common.AddressLength+len(slot))
	key = append(key, kvStoragePrefix...)
	key = append(key, addr[:]...)
	key = append(key, slot...)
	return key
}

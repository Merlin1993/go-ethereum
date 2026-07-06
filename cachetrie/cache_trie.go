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

// Package cachetrie implements a root-aware sliding state cache.
//
// The cache has a committed view, used by readers for an exact state root, and a
// staged view, used by StateDB while it is building the next block root. Staged
// writes are not visible to readers until the canonical root is known and the
// block transition is published.
package cachetrie

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
)

const (
	defaultWindow    = uint64(256)
	defaultMaxItems  = 1_000_000
	windowBitCount   = 32
	accountEntryKind = byte(0)
	storageEntryKind = byte(1)
)

var (
	accountHitMeter   = metrics.GetOrRegisterMeter("cachetrie/account/hit", nil)
	accountMissMeter  = metrics.GetOrRegisterMeter("cachetrie/account/miss", nil)
	storageHitMeter   = metrics.GetOrRegisterMeter("cachetrie/storage/hit", nil)
	storageMissMeter  = metrics.GetOrRegisterMeter("cachetrie/storage/miss", nil)
	updateMeter       = metrics.GetOrRegisterMeter("cachetrie/update", nil)
	deleteMeter       = metrics.GetOrRegisterMeter("cachetrie/delete", nil)
	cleanupMeter      = metrics.GetOrRegisterMeter("cachetrie/cleanup", nil)
	cleanupItemsMeter = metrics.GetOrRegisterMeter("cachetrie/cleanup/items", nil)
	accountSizeGauge  = metrics.GetOrRegisterGauge("cachetrie/account/size", nil)
	storageSizeGauge  = metrics.GetOrRegisterGauge("cachetrie/storage/size", nil)
	totalSizeGauge    = metrics.GetOrRegisterGauge("cachetrie/size", nil)
)

type entry struct {
	account    *types.StateAccount
	storage    common.Hash
	deleted    bool
	block      uint64
	lastAccess uint64
	window     uint32
}

type storageIndexKey struct {
	address common.Address
	slot    common.Hash
}

// StateType identifies the kind of state leaf represented by a merge input.
type StateType byte

const (
	AccountState StateType = iota
	StorageState
)

// MergeInput is the retained metadata for a pending SWMT-to-global-state merge.
type MergeInput struct {
	Type       StateType
	Address    common.Address
	StorageKey *common.Hash
	SWMTKey    []byte
	Account    *types.StateAccount
	Storage    common.Hash
	Tombstone  bool
	Dependency bool
}

// MergeFunc applies a pending merge round to the backing global state tree and
// returns the resulting global state root.
type MergeFunc func(root common.Hash, block uint64, inputs []MergeInput) (common.Hash, error)

type mergeRound struct {
	bits       uint32
	inputs     []MergeInput
	done       chan struct{}
	complete   bool
	err        error
	resultRoot common.Hash
}

// Stats is a point-in-time snapshot of cachetrie activity.
type Stats struct {
	Root     common.Hash
	SWMTRoot common.Hash

	Accounts      int
	Storages      int
	LowWatermark  int
	HighWatermark int
	CurrentBit    uint8
	StartBit      uint8
	Pipeline      bool
	PendingBits   uint32
	PendingInputs int

	AccountHits   uint64
	AccountMisses uint64
	StorageHits   uint64
	StorageMisses uint64
	Updates       uint64
	Deletes       uint64

	CleanupCount       uint64
	CleanupItems       uint64
	CleanupElapsed     time.Duration
	CleanupMaxElapsed  time.Duration
	LastCleanupRemoved int

	MergeCount   uint64
	MergeInputs  uint64
	MergeElapsed time.Duration
	MergeErrors  uint64

	WriteWaitCount   uint64
	WriteWaitElapsed time.Duration

	RootCount   uint64
	RootElapsed time.Duration

	PublishCount   uint64
	PublishElapsed time.Duration
}

// CacheTrie stores recently committed account and storage values keyed by their
// canonical address/slot. It is safe for concurrent reads.
type CacheTrie struct {
	mu sync.RWMutex

	root    common.Hash
	hasRoot bool

	block  uint64
	window uint64
	max    int
	low    int
	high   int

	currentBit uint8
	startBit   uint8

	accounts map[common.Address]entry
	storages map[common.Address]map[common.Hash]entry
	// Window indexes keep watermark publish/prune work proportional to the
	// selected complete windows instead of the entire live SWMT.
	accountWindows [windowBitCount]map[common.Address]struct{}
	storageWindows [windowBitCount]map[storageIndexKey]struct{}
	// liveDigest is an order-independent aggregate of all live SWMT leaves.
	// It makes per-block SWMT root reporting proportional to the block delta
	// instead of sorting and hashing the entire live overlay.
	liveDigest  common.Hash
	storageSize int

	pendingOrigin   common.Hash
	pendingBlock    uint64
	hasPending      bool
	pendingAccounts map[common.Address]*types.StateAccount
	pendingStorages map[common.Address]map[common.Hash]common.Hash
	pipelineStarted bool
	pendingMerge    *mergeRound
	lastMergeRoot   common.Hash
	mergeFn         MergeFunc

	accountHits   atomic.Uint64
	accountMisses atomic.Uint64
	storageHits   atomic.Uint64
	storageMisses atomic.Uint64
	updates       uint64
	deletes       uint64

	cleanupCount       uint64
	cleanupItems       uint64
	cleanupElapsed     time.Duration
	cleanupMaxElapsed  time.Duration
	lastCleanupRemoved int

	mergeCount   uint64
	mergeInputs  uint64
	mergeElapsed time.Duration
	mergeErrors  uint64

	writeWaitCount   uint64
	writeWaitElapsed time.Duration

	rootCount   uint64
	rootElapsed time.Duration

	publishCount   uint64
	publishElapsed time.Duration
}

// NewCacheTrie creates a sliding state cache. The window is measured in blocks;
// maxItems limits the total number of account and storage entries retained.
func NewCacheTrie(startBlock, window uint64, maxItems int, lowWatermarks ...int) *CacheTrie {
	if window == 0 {
		window = defaultWindow
	}
	if maxItems <= 0 {
		maxItems = defaultMaxItems
	}
	low := lowWatermark(maxItems)
	if len(lowWatermarks) > 0 && lowWatermarks[0] > 0 {
		low = lowWatermarks[0]
		if low > maxItems {
			low = maxItems
		}
	}
	return &CacheTrie{
		block:      startBlock,
		window:     window,
		max:        maxItems,
		low:        low,
		high:       maxItems,
		currentBit: bitForBlock(startBlock),
		startBit:   bitForBlock(startBlock),
		accounts:   make(map[common.Address]entry),
		storages:   make(map[common.Address]map[common.Hash]entry),
	}
}

// Begin starts (or resumes) the staged write set for a block transition. Calling
// Begin repeatedly with the same block and origin is intentionally idempotent;
// StateDB may compute IntermediateRoot more than once before Commit.
func (c *CacheTrie) Begin(block uint64, origin common.Hash) {
	for {
		c.mu.Lock()
		c.block = block
		c.currentBit = bitForBlock(block)
		if wait := c.mergeWaitLocked(); wait != nil {
			c.mu.Unlock()
			start := time.Now()
			<-wait
			c.recordWriteWait(start)
			continue
		}
		if c.hasPending && c.pendingBlock == block && c.pendingOrigin == origin {
			c.mu.Unlock()
			return
		}
		c.pendingBlock = block
		c.pendingOrigin = origin
		c.hasPending = true
		c.pendingAccounts = make(map[common.Address]*types.StateAccount)
		c.pendingStorages = make(map[common.Address]map[common.Hash]common.Hash)
		c.mu.Unlock()
		return
	}
}

// SetBlockNum updates the current block number and current SWMT window bit.
func (c *CacheTrie) SetBlockNum(block uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.block = block
	c.currentBit = bitForBlock(block)
}

// SetRoot binds the cache contents to a canonical state root.
func (c *CacheTrie) SetRoot(root common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.root = root
	c.hasRoot = true
}

// Root returns the canonical state root this cache is bound to.
func (c *CacheTrie) Root() (common.Hash, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.root, c.hasRoot
}

// Available reports whether the cache can safely answer reads for root.
func (c *CacheTrie) Available(root common.Hash) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.hasRoot && c.root == root
}

// SetMergeFunc installs the backing global-state merge worker used for pending
// SWMT merge rounds. Passing nil restores the no-op merger.
func (c *CacheTrie) SetMergeFunc(fn MergeFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mergeFn = fn
}

// WaitForMerge waits for the currently running merge round, if any, and records
// the delay as replay backpressure. It is used by dual-root replay to model the
// idle time between real blocks, so background merge does not pollute foreground
// read timings.
func (c *CacheTrie) WaitForMerge() {
	for {
		c.mu.Lock()
		if c.pendingMerge == nil || c.pendingMerge.complete {
			c.mu.Unlock()
			return
		}
		wait := c.pendingMerge.done
		c.mu.Unlock()

		start := time.Now()
		<-wait
		c.recordWriteWait(start)
	}
}

// StageAccount records an account write in the in-flight block transition. The
// staged value is not served until Publish binds it to the resulting root.
func (c *CacheTrie) StageAccount(address common.Address, account *types.StateAccount) {
	for {
		c.mu.Lock()
		c.ensurePendingLocked()
		delta := 0
		if _, committed := c.accounts[address]; !committed {
			if _, staged := c.pendingAccounts[address]; !staged {
				delta = 1
			}
		}
		if wait := c.writeWaitLocked(delta); wait != nil {
			c.mu.Unlock()
			start := time.Now()
			<-wait
			c.recordWriteWait(start)
			continue
		}
		c.pendingAccounts[address] = copyAccount(account)
		c.mu.Unlock()
		return
	}
}

// StageStorage records a storage slot write in the in-flight block transition.
// A zero value represents a deleted slot, matching StateDB storage semantics.
func (c *CacheTrie) StageStorage(address common.Address, slot common.Hash, value common.Hash) {
	for {
		c.mu.Lock()
		c.ensurePendingLocked()
		delta := 0
		if c.storages[address] == nil {
			delta = 1
		} else if _, committed := c.storages[address][slot]; !committed {
			delta = 1
		}
		if c.pendingStorages[address] != nil {
			if _, staged := c.pendingStorages[address][slot]; staged {
				delta = 0
			}
		}
		if wait := c.writeWaitLocked(delta); wait != nil {
			c.mu.Unlock()
			start := time.Now()
			<-wait
			c.recordWriteWait(start)
			continue
		}
		if c.pendingStorages[address] == nil {
			c.pendingStorages[address] = make(map[common.Hash]common.Hash)
		}
		c.pendingStorages[address][slot] = value
		c.mu.Unlock()
		return
	}
}

// UpdateAccount records the committed value of an account.
func (c *CacheTrie) UpdateAccount(block uint64, address common.Address, account *types.StateAccount) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.block = block
	c.putAccountLocked(address, entry{
		account:    copyAccount(account),
		block:      block,
		lastAccess: block,
		window:     bitMask(bitForBlock(block)),
	})
	c.updates++
	updateMeter.Mark(1)
	c.maintainPipelineLocked()
	c.updateSizeMetricsLocked()
}

// DeleteAccount records an account tombstone.
func (c *CacheTrie) DeleteAccount(block uint64, address common.Address) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.block = block
	c.putAccountLocked(address, entry{
		deleted:    true,
		block:      block,
		lastAccess: block,
		window:     bitMask(bitForBlock(block)),
	})
	c.removeStorageBucketLocked(address)
	c.deletes++
	deleteMeter.Mark(1)
	c.maintainPipelineLocked()
	c.updateSizeMetricsLocked()
}

// Account retrieves an account from the cache. A hit with a nil account means
// the account is known to be absent at the cached root.
func (c *CacheTrie) Account(address common.Address) (*types.StateAccount, bool) {
	c.mu.RLock()
	item, ok := c.accounts[address]
	if !ok {
		c.mu.RUnlock()
		c.accountMisses.Add(1)
		accountMissMeter.Mark(1)
		return nil, false
	}
	var account *types.StateAccount
	if !item.deleted {
		account = copyAccount(item.account)
	}
	c.mu.RUnlock()
	c.accountHits.Add(1)
	accountHitMeter.Mark(1)
	if item.deleted {
		return nil, true
	}
	return account, true
}

// UpdateStorage records the committed value of a storage slot.
func (c *CacheTrie) UpdateStorage(block uint64, address common.Address, slot common.Hash, value common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.block = block
	account := c.storageAccountSnapshotLocked(address, nil)
	c.putStorageLocked(address, slot, entry{
		account:    account,
		storage:    value,
		block:      block,
		lastAccess: block,
		window:     bitMask(bitForBlock(block)),
	})
	c.updates++
	updateMeter.Mark(1)
	c.maintainPipelineLocked()
	c.updateSizeMetricsLocked()
}

// DeleteStorage records a storage slot tombstone.
func (c *CacheTrie) DeleteStorage(block uint64, address common.Address, slot common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.block = block
	account := c.storageAccountSnapshotLocked(address, nil)
	c.putStorageLocked(address, slot, entry{
		account:    account,
		deleted:    true,
		block:      block,
		lastAccess: block,
		window:     bitMask(bitForBlock(block)),
	})
	c.deletes++
	deleteMeter.Mark(1)
	c.maintainPipelineLocked()
	c.updateSizeMetricsLocked()
}

// Storage retrieves a storage slot from the cache. A hit with a zero hash can
// either mean an actual zero value or a cached deletion, both equivalent for
// StateDB's committed storage reads.
func (c *CacheTrie) Storage(address common.Address, slot common.Hash) (common.Hash, bool) {
	c.mu.RLock()
	slots := c.storages[address]
	if slots == nil {
		c.mu.RUnlock()
		c.storageMisses.Add(1)
		storageMissMeter.Mark(1)
		return common.Hash{}, false
	}
	item, ok := slots[slot]
	if !ok {
		c.mu.RUnlock()
		c.storageMisses.Add(1)
		storageMissMeter.Mark(1)
		return common.Hash{}, false
	}
	c.mu.RUnlock()
	c.storageHits.Add(1)
	storageHitMeter.Mark(1)
	if item.deleted {
		return common.Hash{}, true
	}
	return item.storage, true
}

// Publish records a StateDB update and binds the cache to the resulting root.
// It preserves the direct cache API's legacy semantics. SWMT async callers use
// PublishRoots instead.
func (c *CacheTrie) Publish(block uint64, origin common.Hash, root common.Hash, accounts map[common.Address]*types.StateAccount, storages map[common.Address]map[common.Hash]common.Hash) {
	c.PublishRoots(block, origin, root, accounts, storages)
	c.SetRoot(root)
}

// PublishRoots records a StateDB update into live SWMT and returns the disclosed
// global root plus the live SWMT root. The backing global root advances only
// when a completed pending merge is pruned; new writes stay in SWMT until a
// later pending round is disclosed.
func (c *CacheTrie) PublishRoots(block uint64, origin common.Hash, root common.Hash, accounts map[common.Address]*types.StateAccount, storages map[common.Address]map[common.Hash]common.Hash) (common.Hash, common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	start := time.Now()
	defer func() {
		c.publishCount++
		c.publishElapsed += time.Since(start)
	}()

	c.block = block
	c.currentBit = bitForBlock(block)
	if c.hasRoot && c.root != origin {
		c.clearLiveLocked()
		c.pipelineStarted = false
		c.pendingMerge = nil
		c.startBit = c.currentBit
	}
	if !c.hasRoot {
		c.root = origin
		c.hasRoot = true
	}
	if c.hasPending && c.pendingBlock == block && c.pendingOrigin == origin {
		if len(c.pendingAccounts) > 0 {
			if accounts == nil {
				accounts = make(map[common.Address]*types.StateAccount, len(c.pendingAccounts))
			}
			for address, account := range c.pendingAccounts {
				if _, exists := accounts[address]; !exists {
					accounts[address] = copyAccount(account)
				}
			}
		}
		if len(c.pendingStorages) > 0 {
			if storages == nil {
				storages = make(map[common.Address]map[common.Hash]common.Hash, len(c.pendingStorages))
			}
			for address, slots := range c.pendingStorages {
				if storages[address] == nil {
					storages[address] = make(map[common.Hash]common.Hash, len(slots))
				}
				for slot, value := range slots {
					if _, exists := storages[address][slot]; !exists {
						storages[address][slot] = value
					}
				}
			}
		}
		c.hasPending = false
		c.pendingAccounts = nil
		c.pendingStorages = nil
	}
	for address, account := range accounts {
		if account == nil {
			c.putAccountLocked(address, entry{deleted: true, block: block, lastAccess: block, window: bitMask(c.currentBit)})
			c.removeStorageBucketLocked(address)
			c.deletes++
			deleteMeter.Mark(1)
		} else {
			c.putAccountLocked(address, entry{account: copyAccount(account), block: block, lastAccess: block, window: bitMask(c.currentBit)})
			c.updates++
			updateMeter.Mark(1)
		}
	}
	for address, slots := range storages {
		account := c.storageAccountSnapshotLocked(address, accounts)
		for slot, value := range slots {
			if value == (common.Hash{}) {
				c.putStorageLocked(address, slot, entry{account: account, deleted: true, block: block, lastAccess: block, window: bitMask(c.currentBit)})
				c.deletes++
				deleteMeter.Mark(1)
			} else {
				c.putStorageLocked(address, slot, entry{account: account, storage: value, block: block, lastAccess: block, window: bitMask(c.currentBit)})
				c.updates++
				updateMeter.Mark(1)
			}
		}
	}
	c.maintainPipelineLocked()
	c.updateSizeMetricsLocked()
	rootStart := time.Now()
	swmtRoot := c.swmtRootLocked(c.root)
	c.rootCount++
	c.rootElapsed += time.Since(rootStart)
	return c.root, swmtRoot
}

// Apply is kept as the direct committed-update API used by tests and callers
// that do not participate in staged StateDB writes.
func (c *CacheTrie) Apply(block uint64, origin common.Hash, root common.Hash, accounts map[common.Address]*types.StateAccount, storages map[common.Address]map[common.Hash]common.Hash) {
	c.Publish(block, origin, root, accounts, storages)
}

func (c *CacheTrie) clearLiveLocked() {
	c.accounts = make(map[common.Address]entry)
	c.storages = make(map[common.Address]map[common.Hash]entry)
	c.accountWindows = [windowBitCount]map[common.Address]struct{}{}
	c.storageWindows = [windowBitCount]map[storageIndexKey]struct{}{}
	c.liveDigest = common.Hash{}
	c.storageSize = 0
}

func (c *CacheTrie) putAccountLocked(address common.Address, item entry) {
	if old, ok := c.accounts[address]; ok {
		c.removeLeafDigestLocked(accountEntryKind, address, common.Hash{}, old)
		c.removeAccountWindowLocked(address, old)
	}
	c.accounts[address] = item
	c.addAccountWindowLocked(address, item)
	c.addLeafDigestLocked(accountEntryKind, address, common.Hash{}, item)
}

func (c *CacheTrie) removeAccountLocked(address common.Address) bool {
	old, ok := c.accounts[address]
	if !ok {
		return false
	}
	c.removeLeafDigestLocked(accountEntryKind, address, common.Hash{}, old)
	c.removeAccountWindowLocked(address, old)
	delete(c.accounts, address)
	return true
}

func (c *CacheTrie) putStorageLocked(address common.Address, slot common.Hash, item entry) {
	slots := c.storages[address]
	if slots == nil {
		slots = make(map[common.Hash]entry)
		c.storages[address] = slots
	}
	if old, ok := slots[slot]; ok {
		c.removeLeafDigestLocked(storageEntryKind, address, slot, old)
		c.removeStorageWindowLocked(address, slot, old)
	} else {
		c.storageSize++
	}
	slots[slot] = item
	c.addStorageWindowLocked(address, slot, item)
	c.addLeafDigestLocked(storageEntryKind, address, slot, item)
}

func (c *CacheTrie) removeStorageLocked(address common.Address, slot common.Hash) bool {
	slots := c.storages[address]
	if slots == nil {
		return false
	}
	old, ok := slots[slot]
	if !ok {
		return false
	}
	c.removeLeafDigestLocked(storageEntryKind, address, slot, old)
	c.removeStorageWindowLocked(address, slot, old)
	delete(slots, slot)
	c.storageSize--
	if len(slots) == 0 {
		delete(c.storages, address)
	}
	return true
}

func (c *CacheTrie) removeStorageBucketLocked(address common.Address) int {
	slots := c.storages[address]
	if slots == nil {
		return 0
	}
	removed := 0
	for slot, item := range slots {
		c.removeLeafDigestLocked(storageEntryKind, address, slot, item)
		c.removeStorageWindowLocked(address, slot, item)
		removed++
	}
	delete(c.storages, address)
	c.storageSize -= removed
	return removed
}

func (c *CacheTrie) addAccountWindowLocked(address common.Address, item entry) {
	bit, ok := windowIndex(item.window)
	if !ok {
		return
	}
	if c.accountWindows[bit] == nil {
		c.accountWindows[bit] = make(map[common.Address]struct{})
	}
	c.accountWindows[bit][address] = struct{}{}
}

func (c *CacheTrie) removeAccountWindowLocked(address common.Address, item entry) {
	bit, ok := windowIndex(item.window)
	if !ok || c.accountWindows[bit] == nil {
		return
	}
	delete(c.accountWindows[bit], address)
}

func (c *CacheTrie) addStorageWindowLocked(address common.Address, slot common.Hash, item entry) {
	bit, ok := windowIndex(item.window)
	if !ok {
		return
	}
	if c.storageWindows[bit] == nil {
		c.storageWindows[bit] = make(map[storageIndexKey]struct{})
	}
	c.storageWindows[bit][storageIndexKey{address: address, slot: slot}] = struct{}{}
}

func (c *CacheTrie) removeStorageWindowLocked(address common.Address, slot common.Hash, item entry) {
	bit, ok := windowIndex(item.window)
	if !ok || c.storageWindows[bit] == nil {
		return
	}
	delete(c.storageWindows[bit], storageIndexKey{address: address, slot: slot})
}

func (c *CacheTrie) addLeafDigestLocked(kind byte, address common.Address, slot common.Hash, item entry) {
	xorHash(&c.liveDigest, leafDigest(kind, address, slot, item))
}

func (c *CacheTrie) removeLeafDigestLocked(kind byte, address common.Address, slot common.Hash, item entry) {
	xorHash(&c.liveDigest, leafDigest(kind, address, slot, item))
}

// PreviewRoots returns the roots that would be disclosed if the current staged
// write set were published now. It does not mutate SWMT or start merge work.
func (c *CacheTrie) PreviewRoots() (common.Hash, common.Hash, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.hasRoot && !c.hasPending {
		return common.Hash{}, common.Hash{}, false
	}
	root := c.root
	if !c.hasRoot {
		root = c.pendingOrigin
	}
	if !c.hasPending {
		return root, c.swmtRootLocked(root), true
	}
	accounts, storages := c.copyLiveLocked()
	c.applyPendingToCopiesLocked(accounts, storages)
	count := countCopies(accounts, storages)
	if c.pipelineStarted && (count >= c.high || nextBit(c.currentBit) == c.startBit) && c.pendingMerge != nil && c.pendingMerge.complete && c.pendingMerge.err == nil {
		root = c.pendingMerge.resultRoot
		pruneCopies(accounts, storages, c.pendingMerge.bits)
	}
	return root, swmtRootFromCopies(root, accounts, storages), true
}

// Hash returns a deterministic digest of the cache contents. It is diagnostic
// only and is not used as an Ethereum state root.
func (c *CacheTrie) Hash() common.Hash {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.swmtRootLocked(c.root)
}

func (c *CacheTrie) swmtRootLocked(root common.Hash) common.Hash {
	return swmtRootFromDigest(root, c.liveDigest, len(c.accounts), c.storageSize)
}

func swmtRootFromDigest(root common.Hash, digest common.Hash, accounts int, storages int) common.Hash {
	hasher := crypto.NewKeccakState()
	hasher.Write([]byte("cachetrie-swmt-v2"))
	hasher.Write(root[:])
	hasher.Write(digest[:])
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], uint64(accounts))
	hasher.Write(scratch[:])
	binary.BigEndian.PutUint64(scratch[:], uint64(storages))
	hasher.Write(scratch[:])
	var out common.Hash
	hasher.Read(out[:])
	return out
}

func leafDigest(kind byte, address common.Address, slot common.Hash, item entry) common.Hash {
	hasher := crypto.NewKeccakState()
	hasher.Write([]byte{kind})
	hasher.Write(address[:])
	if kind == storageEntryKind {
		hasher.Write(slot[:])
	}
	if item.deleted {
		hasher.Write([]byte{1})
	} else {
		hasher.Write([]byte{0})
	}
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], item.block)
	hasher.Write(scratch[:])
	if kind == accountEntryKind {
		if item.account != nil {
			hasher.Write(types.SlimAccountRLP(*item.account))
		}
	} else {
		hasher.Write(item.storage[:])
	}
	var out common.Hash
	hasher.Read(out[:])
	return out
}

func xorHash(dst *common.Hash, value common.Hash) {
	for i := range dst {
		dst[i] ^= value[i]
	}
}

func swmtRootFromCopies(root common.Hash, accounts map[common.Address]entry, storages map[common.Address]map[common.Hash]entry) common.Hash {
	var digest common.Hash
	for address, item := range accounts {
		xorHash(&digest, leafDigest(accountEntryKind, address, common.Hash{}, item))
	}
	storageCount := 0
	for address, slots := range storages {
		for slot, item := range slots {
			xorHash(&digest, leafDigest(storageEntryKind, address, slot, item))
			storageCount++
		}
	}
	return swmtRootFromDigest(root, digest, len(accounts), storageCount)
}

// Stats returns a snapshot of cache counters and sizes.
func (c *CacheTrie) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return Stats{
		Root:               c.root,
		SWMTRoot:           c.swmtRootLocked(c.root),
		Accounts:           len(c.accounts),
		Storages:           c.storageCountLocked(),
		LowWatermark:       c.low,
		HighWatermark:      c.high,
		CurrentBit:         c.currentBit,
		StartBit:           c.startBit,
		Pipeline:           c.pipelineStarted,
		PendingBits:        c.pendingBitsLocked(),
		PendingInputs:      c.pendingInputsLocked(),
		AccountHits:        c.accountHits.Load(),
		AccountMisses:      c.accountMisses.Load(),
		StorageHits:        c.storageHits.Load(),
		StorageMisses:      c.storageMisses.Load(),
		Updates:            c.updates,
		Deletes:            c.deletes,
		CleanupCount:       c.cleanupCount,
		CleanupItems:       c.cleanupItems,
		CleanupElapsed:     c.cleanupElapsed,
		CleanupMaxElapsed:  c.cleanupMaxElapsed,
		LastCleanupRemoved: c.lastCleanupRemoved,
		MergeCount:         c.mergeCount,
		MergeInputs:        c.mergeInputs,
		MergeElapsed:       c.mergeElapsed,
		MergeErrors:        c.mergeErrors,
		WriteWaitCount:     c.writeWaitCount,
		WriteWaitElapsed:   c.writeWaitElapsed,
		RootCount:          c.rootCount,
		RootElapsed:        c.rootElapsed,
		PublishCount:       c.publishCount,
		PublishElapsed:     c.publishElapsed,
	}
}

func (c *CacheTrie) recordWriteWait(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writeWaitCount++
	c.writeWaitElapsed += time.Since(start)
}

func (c *CacheTrie) mergeWaitLocked() chan struct{} {
	if c.pendingMerge == nil || c.pendingMerge.complete {
		return nil
	}
	if c.countLocked() < c.high && nextBit(c.currentBit) != c.startBit {
		return nil
	}
	return c.pendingMerge.done
}

func (c *CacheTrie) maintainPipelineLocked() {
	count := c.countLocked()
	if !c.pipelineStarted {
		if count >= c.low && c.pendingMerge == nil {
			if bits, inputs := c.selectPendingLocked(true); bits != 0 {
				c.pipelineStarted = true
				c.startMergeLocked(bits, inputs)
			}
		}
		return
	}
	if count < c.high && nextBit(c.currentBit) != c.startBit {
		return
	}
	if c.pendingMerge != nil && !c.pendingMerge.complete {
		return
	}
	if c.pendingMerge != nil {
		if c.pendingMerge.err != nil {
			return
		}
		c.pruneMergedLocked(c.pendingMerge.bits)
		c.lastMergeRoot = c.pendingMerge.resultRoot
		c.root = c.pendingMerge.resultRoot
		c.hasRoot = true
		c.pendingMerge = nil
	}
	if bits, inputs := c.selectPendingLocked(true); bits != 0 {
		c.startMergeLocked(bits, inputs)
	}
}

func (c *CacheTrie) ensurePendingLocked() {
	if c.hasPending {
		return
	}
	c.hasPending = true
	c.pendingBlock = c.block
	c.pendingOrigin = c.root
	c.pendingAccounts = make(map[common.Address]*types.StateAccount)
	c.pendingStorages = make(map[common.Address]map[common.Hash]common.Hash)
}

func (c *CacheTrie) writeWaitLocked(delta int) chan struct{} {
	if c.pendingMerge == nil || c.pendingMerge.complete {
		return nil
	}
	if nextBit(c.currentBit) == c.startBit {
		return c.pendingMerge.done
	}
	if delta > 0 && c.countLocked()+c.pendingNewCountLocked()+delta > c.high {
		return c.pendingMerge.done
	}
	return nil
}

func (c *CacheTrie) selectPendingLocked(forceOne bool) (uint32, []MergeInput) {
	need := c.countLocked() - c.low
	if need <= 0 && forceOne {
		need = 1
	}
	if need <= 0 {
		return 0, nil
	}
	var (
		bits      uint32
		inputs    []MergeInput
		scanStart = c.startBit
	)
	for i := 0; i < windowBitCount; i++ {
		bit := uint8((int(scanStart) + i) % windowBitCount)
		if bit == c.currentBit {
			break
		}
		bitInputs := c.mergeInputsForBitLocked(bit)
		if len(bitInputs) == 0 {
			if bits == 0 && bit == c.startBit {
				c.startBit = nextBit(bit)
			}
			continue
		}
		bits |= bitMask(bit)
		inputs = append(inputs, bitInputs...)
		need -= len(bitInputs)
		if need <= 0 {
			break
		}
	}
	return bits, inputs
}

func (c *CacheTrie) mergeInputsForBitLocked(bit uint8) []MergeInput {
	var inputs []MergeInput
	mask := bitMask(bit)
	accountInputs := make(map[common.Address]struct{})
	for address := range c.accountWindows[bit] {
		item, ok := c.accounts[address]
		if !ok {
			continue
		}
		if item.window != mask {
			continue
		}
		accountInputs[address] = struct{}{}
		inputs = append(inputs, MergeInput{
			Type:      AccountState,
			Address:   address,
			SWMTKey:   swmtAccountKey(address),
			Account:   copyAccount(item.account),
			Tombstone: item.deleted,
		})
	}
	for key := range c.storageWindows[bit] {
		slots := c.storages[key.address]
		if slots == nil {
			continue
		}
		item, ok := slots[key.slot]
		if !ok || item.window != mask {
			continue
		}
		if item.account != nil {
			if _, ok := accountInputs[key.address]; !ok {
				accountInputs[key.address] = struct{}{}
				inputs = append(inputs, MergeInput{
					Type:       AccountState,
					Address:    key.address,
					SWMTKey:    swmtAccountKey(key.address),
					Account:    copyAccount(item.account),
					Tombstone:  false,
					Dependency: true,
				})
			}
		}
		slot := key.slot
		inputs = append(inputs, MergeInput{
			Type:       StorageState,
			Address:    key.address,
			StorageKey: &slot,
			SWMTKey:    swmtStorageKey(key.address, slot),
			Storage:    item.storage,
			Tombstone:  item.deleted,
		})
	}
	sort.Slice(inputs, func(i, j int) bool {
		return bytes.Compare(inputs[i].SWMTKey, inputs[j].SWMTKey) < 0
	})
	return inputs
}

func (c *CacheTrie) startMergeLocked(bits uint32, inputs []MergeInput) {
	copied := copyMergeInputs(inputs)
	round := &mergeRound{
		bits:   bits,
		inputs: copied,
		done:   make(chan struct{}),
	}
	c.pendingMerge = round
	root := c.root
	block := c.block
	mergeFn := c.mergeFn
	go func() {
		start := time.Now()
		resultRoot := root
		var err error
		if mergeFn != nil {
			resultRoot, err = mergeFn(root, block, copyMergeInputs(copied))
		}
		c.mu.Lock()
		if c.pendingMerge == round {
			round.resultRoot = resultRoot
			round.err = err
			round.complete = true
			c.mergeCount++
			c.mergeInputs += uint64(len(copied))
			c.mergeElapsed += time.Since(start)
			if err != nil {
				c.mergeErrors++
			}
		}
		c.mu.Unlock()
		close(round.done)
	}()
}

func (c *CacheTrie) pruneMergedLocked(bits uint32) {
	start := time.Now()
	removed := 0
	for bit := uint8(0); bit < windowBitCount; bit++ {
		if bits&bitMask(bit) == 0 {
			continue
		}
		for address := range c.accountWindows[bit] {
			item, ok := c.accounts[address]
			if !ok {
				continue
			}
			if item.window != 0 && item.window&bits != 0 && item.window&^bits == 0 {
				if c.removeAccountLocked(address) {
					removed++
				}
			}
		}
		for key := range c.storageWindows[bit] {
			slots := c.storages[key.address]
			if slots == nil {
				continue
			}
			item, ok := slots[key.slot]
			if !ok {
				continue
			}
			if item.window != 0 && item.window&bits != 0 && item.window&^bits == 0 {
				if c.removeStorageLocked(key.address, key.slot) {
					removed++
				}
			}
		}
	}
	c.advanceStartBitLocked(bits)
	if removed == 0 {
		c.lastCleanupRemoved = 0
		return
	}
	elapsed := time.Since(start)
	c.cleanupCount++
	c.cleanupItems += uint64(removed)
	c.cleanupElapsed += elapsed
	if elapsed > c.cleanupMaxElapsed {
		c.cleanupMaxElapsed = elapsed
	}
	c.lastCleanupRemoved = removed
	cleanupMeter.Mark(1)
	cleanupItemsMeter.Mark(int64(removed))
}

func (c *CacheTrie) advanceStartBitLocked(bits uint32) {
	for bits&bitMask(c.startBit) != 0 {
		c.startBit = nextBit(c.startBit)
	}
}

func (c *CacheTrie) storageAccountSnapshotLocked(address common.Address, accounts map[common.Address]*types.StateAccount) *types.StateAccount {
	if accounts != nil {
		if account, ok := accounts[address]; ok {
			return copyAccount(account)
		}
	}
	if item, ok := c.accounts[address]; ok && !item.deleted {
		return copyAccount(item.account)
	}
	return nil
}

func (c *CacheTrie) copyLiveLocked() (map[common.Address]entry, map[common.Address]map[common.Hash]entry) {
	accounts := make(map[common.Address]entry, len(c.accounts))
	for address, item := range c.accounts {
		item.account = copyAccount(item.account)
		accounts[address] = item
	}
	storages := make(map[common.Address]map[common.Hash]entry, len(c.storages))
	for address, slots := range c.storages {
		copied := make(map[common.Hash]entry, len(slots))
		for slot, item := range slots {
			item.account = copyAccount(item.account)
			copied[slot] = item
		}
		storages[address] = copied
	}
	return accounts, storages
}

func (c *CacheTrie) applyPendingToCopiesLocked(accounts map[common.Address]entry, storages map[common.Address]map[common.Hash]entry) {
	if !c.hasPending {
		return
	}
	mask := bitMask(c.currentBit)
	block := c.pendingBlock
	for address, account := range c.pendingAccounts {
		if account == nil {
			accounts[address] = entry{deleted: true, block: block, lastAccess: block, window: mask}
			delete(storages, address)
		} else {
			accounts[address] = entry{account: copyAccount(account), block: block, lastAccess: block, window: mask}
		}
	}
	for address, slots := range c.pendingStorages {
		bucket := storages[address]
		if bucket == nil {
			bucket = make(map[common.Hash]entry, len(slots))
			storages[address] = bucket
		}
		var account *types.StateAccount
		if pending, ok := c.pendingAccounts[address]; ok {
			account = copyAccount(pending)
		} else if item, ok := accounts[address]; ok && !item.deleted {
			account = copyAccount(item.account)
		}
		for slot, value := range slots {
			if value == (common.Hash{}) {
				bucket[slot] = entry{account: account, deleted: true, block: block, lastAccess: block, window: mask}
			} else {
				bucket[slot] = entry{account: account, storage: value, block: block, lastAccess: block, window: mask}
			}
		}
	}
}

func pruneCopies(accounts map[common.Address]entry, storages map[common.Address]map[common.Hash]entry, bits uint32) {
	for address, item := range accounts {
		if item.window != 0 && item.window&bits != 0 && item.window&^bits == 0 {
			delete(accounts, address)
		}
	}
	for address, slots := range storages {
		for slot, item := range slots {
			if item.window != 0 && item.window&bits != 0 && item.window&^bits == 0 {
				delete(slots, slot)
			}
		}
		if len(slots) == 0 {
			delete(storages, address)
		}
	}
}

func countCopies(accounts map[common.Address]entry, storages map[common.Address]map[common.Hash]entry) int {
	count := len(accounts)
	for _, slots := range storages {
		count += len(slots)
	}
	return count
}

func (c *CacheTrie) updateSizeMetricsLocked() {
	storages := c.storageCountLocked()
	accountSizeGauge.Update(int64(len(c.accounts)))
	storageSizeGauge.Update(int64(storages))
	totalSizeGauge.Update(int64(len(c.accounts) + storages))
}

func (c *CacheTrie) countLocked() int {
	return len(c.accounts) + c.storageSize
}

func (c *CacheTrie) storageCountLocked() int {
	return c.storageSize
}

func (c *CacheTrie) pendingBitsLocked() uint32 {
	if c.pendingMerge == nil {
		return 0
	}
	return c.pendingMerge.bits
}

func (c *CacheTrie) pendingInputsLocked() int {
	if c.pendingMerge == nil {
		return 0
	}
	return len(c.pendingMerge.inputs)
}

func (c *CacheTrie) pendingNewCountLocked() int {
	if !c.hasPending {
		return 0
	}
	count := 0
	for address := range c.pendingAccounts {
		if _, exists := c.accounts[address]; !exists {
			count++
		}
	}
	for address, slots := range c.pendingStorages {
		committed := c.storages[address]
		for slot := range slots {
			if committed == nil {
				count++
				continue
			}
			if _, exists := committed[slot]; !exists {
				count++
			}
		}
	}
	return count
}

func copyAccount(account *types.StateAccount) *types.StateAccount {
	if account == nil {
		return nil
	}
	return account.Copy()
}

func copyMergeInputs(inputs []MergeInput) []MergeInput {
	copied := make([]MergeInput, len(inputs))
	for i, input := range inputs {
		copied[i] = input
		copied[i].SWMTKey = common.CopyBytes(input.SWMTKey)
		copied[i].Account = copyAccount(input.Account)
		if input.StorageKey != nil {
			key := *input.StorageKey
			copied[i].StorageKey = &key
		}
	}
	return copied
}

func swmtAccountKey(address common.Address) []byte {
	key := make([]byte, 1+common.AddressLength)
	key[0] = accountEntryKind
	copy(key[1:], address[:])
	return key
}

func swmtStorageKey(address common.Address, slot common.Hash) []byte {
	key := make([]byte, 1+common.AddressLength+common.HashLength)
	key[0] = storageEntryKind
	copy(key[1:], address[:])
	copy(key[1+common.AddressLength:], slot[:])
	return key
}

func bitForBlock(block uint64) uint8 {
	return uint8(block % windowBitCount)
}

func bitMask(bit uint8) uint32 {
	return 1 << bit
}

func windowIndex(mask uint32) (uint8, bool) {
	if mask == 0 || mask&(mask-1) != 0 {
		return 0, false
	}
	return uint8(bits.TrailingZeros32(mask)), true
}

func nextBit(bit uint8) uint8 {
	return (bit + 1) % windowBitCount
}

func lowWatermark(high int) int {
	low := high * 8 / 10
	if low < 1 {
		return 1
	}
	if low >= high && high > 1 {
		return high - 1
	}
	return low
}

// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package archive

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	// RootBranchHeader separates the binary branches above the shard boundary
	// from ordinary ASCT nodes. Every branch has exactly two child hashes.
	RootBranchHeader = byte(0xd4)
	rootBranchSize   = 2 + 2*32
)

// rootBranch is the ordinary binary path above the configured shard depth.
// At the final depth its child hashes are shard roots; at earlier depths they
// are hashes of more rootBranch values. Children are loaded only when their
// path is accessed.
type rootBranch struct {
	children      [2]*rootBranch
	childHashes   [2][]byte
	hash          []byte
	persistedHash []byte
	dirty         bool
}

func newRootBranch() *rootBranch {
	return &rootBranch{dirty: true}
}

func validRootHash(hash []byte) bool {
	if len(hash) != 32 {
		return false
	}
	for _, b := range hash {
		if b != 0 {
			return true
		}
	}
	return false
}

func normalizeRootHash(hash []byte) []byte {
	if !validRootHash(hash) {
		return nil
	}
	return bytes.Clone(hash)
}

func rootHashesEqual(a, b []byte) bool {
	return bytes.Equal(normalizeRootHash(a), normalizeRootHash(b))
}

func serializeRootBranch(branch *rootBranch, depth int) []byte {
	data := make([]byte, rootBranchSize)
	data[0] = RootBranchHeader
	data[1] = byte(depth)
	for i := 0; i < 2; i++ {
		if hash := normalizeRootHash(branch.childHashes[i]); hash != nil {
			copy(data[2+i*32:2+(i+1)*32], hash)
		}
	}
	return data
}

func (t *Trie) rootBranchStorageKey(hash []byte, depth, prefix int) []byte {
	if t.config.UsePathStorage() {
		return pathRootBranchKey(depth, prefix)
	}
	return hash
}

func (t *Trie) loadRootBranchLocked(hash []byte, depth, prefix int) (*rootBranch, error) {
	hash = normalizeRootHash(hash)
	if hash == nil {
		return nil, nil
	}
	data, err := t.db.Get(t.rootBranchStorageKey(hash, depth, prefix))
	if err != nil {
		return nil, err
	}
	if len(data) != rootBranchSize || data[0] != RootBranchHeader || int(data[1]) != depth {
		return nil, errors.New("archive: invalid binary root branch")
	}
	if actual := t.hasher.Hash(data); !bytes.Equal(actual, hash) {
		return nil, errors.New("archive: binary root branch hash mismatch")
	}
	branch := &rootBranch{
		hash:          bytes.Clone(hash),
		persistedHash: bytes.Clone(hash),
	}
	for i := 0; i < 2; i++ {
		branch.childHashes[i] = normalizeRootHash(data[2+i*32 : 2+(i+1)*32])
	}
	return branch, nil
}

// loadBinaryRoot installs a committed root. ShardDepth zero is a direct
// boundary: the global root is the only shard root and needs no extra branch.
func (t *Trie) loadBinaryRoot(rootHash []byte) error {
	t.rootMu.Lock()
	defer t.rootMu.Unlock()

	rootHash = normalizeRootHash(rootHash)
	if t.config.ShardDepth == 0 {
		t.rootBranch = nil
		t.rootHash = rootHash
		return nil
	}
	branch, err := t.loadRootBranchLocked(rootHash, 0, 0)
	if err != nil {
		return err
	}
	t.rootBranch = branch
	t.rootHash = rootHash
	return nil
}

func (t *Trie) shardRoot(id int) ([]byte, error) {
	t.rootMu.Lock()
	defer t.rootMu.Unlock()
	return t.shardRootLocked(id)
}

func (t *Trie) shardRootLocked(id int) ([]byte, error) {
	if id < 0 || id >= 1<<t.config.ShardDepth {
		return nil, fmt.Errorf("archive: shard id %d out of range", id)
	}
	if t.config.ShardDepth == 0 {
		return bytes.Clone(t.rootHash), nil
	}
	branch := t.rootBranch
	prefix := 0
	for depth := 0; depth < t.config.ShardDepth; depth++ {
		if branch == nil {
			return nil, nil
		}
		bit := (id >> (t.config.ShardDepth - 1 - depth)) & 1
		if depth == t.config.ShardDepth-1 {
			return bytes.Clone(normalizeRootHash(branch.childHashes[bit])), nil
		}
		childPrefix := (prefix << 1) | bit
		if branch.children[bit] == nil && validRootHash(branch.childHashes[bit]) {
			child, err := t.loadRootBranchLocked(branch.childHashes[bit], depth+1, childPrefix)
			if err != nil {
				return nil, err
			}
			branch.children[bit] = child
		}
		branch = branch.children[bit]
		prefix = childPrefix
	}
	return nil, nil
}

func (t *Trie) applyShardRootLocked(id int, hash []byte) error {
	if id < 0 || id >= 1<<t.config.ShardDepth {
		return fmt.Errorf("archive: shard id %d out of range", id)
	}
	hash = normalizeRootHash(hash)
	if t.config.ShardDepth == 0 {
		t.rootHash = hash
		return nil
	}
	if t.rootBranch == nil {
		if hash == nil {
			return nil
		}
		t.rootBranch = newRootBranch()
	}
	branch := t.rootBranch
	prefix := 0
	path := []*rootBranch{branch}
	for depth := 0; depth < t.config.ShardDepth; depth++ {
		bit := (id >> (t.config.ShardDepth - 1 - depth)) & 1
		if depth == t.config.ShardDepth-1 {
			if rootHashesEqual(branch.childHashes[bit], hash) {
				return nil
			}
			branch.childHashes[bit] = hash
			for _, ancestor := range path {
				ancestor.dirty = true
			}
			return nil
		}
		childPrefix := (prefix << 1) | bit
		if branch.children[bit] == nil && validRootHash(branch.childHashes[bit]) {
			child, err := t.loadRootBranchLocked(branch.childHashes[bit], depth+1, childPrefix)
			if err != nil {
				return err
			}
			branch.children[bit] = child
		}
		if branch.children[bit] == nil {
			if hash == nil {
				return nil
			}
			branch.children[bit] = newRootBranch()
		}
		branch = branch.children[bit]
		path = append(path, branch)
		prefix = childPrefix
	}
	return nil
}

func (t *Trie) hashRootBranchLocked(branch *rootBranch, depth, prefix int, batch Batcher) ([]byte, bool, error) {
	if branch == nil {
		return nil, true, nil
	}
	if !branch.dirty {
		return bytes.Clone(branch.hash), false, nil
	}
	if depth < t.config.ShardDepth-1 {
		for bit, child := range branch.children {
			if child == nil || !child.dirty {
				continue
			}
			childPrefix := (prefix << 1) | bit
			hash, empty, err := t.hashRootBranchLocked(child, depth+1, childPrefix, batch)
			if err != nil {
				return nil, false, err
			}
			branch.childHashes[bit] = hash
			if empty && batch != nil {
				branch.children[bit] = nil
			}
		}
	}
	if !validRootHash(branch.childHashes[0]) && !validRootHash(branch.childHashes[1]) {
		branch.hash = nil
		if batch != nil {
			if t.config.UsePathStorage() {
				if err := batch.Delete(pathRootBranchKey(depth, prefix)); err != nil {
					return nil, false, err
				}
			}
			branch.persistedHash = nil
			branch.dirty = false
		}
		return nil, true, nil
	}
	data := serializeRootBranch(branch, depth)
	hash := t.hasher.Hash(data)
	branch.hash = bytes.Clone(hash)
	if batch != nil {
		if err := batch.Put(t.rootBranchStorageKey(hash, depth, prefix), data); err != nil {
			return nil, false, err
		}
		branch.persistedHash = bytes.Clone(hash)
		branch.dirty = false
	}
	return bytes.Clone(hash), false, nil
}

// computeBinaryRoot applies changed shard roots and hashes only their binary
// paths back to the single global root.
func (t *Trie) computeBinaryRoot(shardRoots map[int][]byte, dirtyShards []int, batch Batcher) ([]byte, error) {
	t.rootMu.Lock()
	defer t.rootMu.Unlock()

	for _, id := range dirtyShards {
		if err := t.applyShardRootLocked(id, shardRoots[id]); err != nil {
			return nil, err
		}
	}
	if t.config.ShardDepth == 0 {
		return bytes.Clone(t.rootHash), nil
	}
	oldRoot := []byte(nil)
	if t.rootBranch != nil {
		oldRoot = bytes.Clone(t.rootBranch.persistedHash)
	}
	if t.rootBranch == nil {
		t.rootHash = nil
		return nil, nil
	}
	root, empty, err := t.hashRootBranchLocked(t.rootBranch, 0, 0, batch)
	if err != nil {
		return nil, err
	}
	t.rootHash = root
	if batch != nil {
		if empty {
			t.rootBranch = nil
		}
		if !t.config.UsePathStorage() && t.config.PhysicalDelete && validRootHash(oldRoot) && !rootHashesEqual(oldRoot, root) {
			if err := batch.Delete(oldRoot); err != nil {
				return nil, err
			}
		}
	}
	return bytes.Clone(root), nil
}

func (t *Trie) collectShardRootsLocked(branch *rootBranch, depth, prefix int, roots map[int][]byte) error {
	if branch == nil {
		return nil
	}
	for bit := 0; bit < 2; bit++ {
		childPrefix := (prefix << 1) | bit
		if depth == t.config.ShardDepth-1 {
			if hash := normalizeRootHash(branch.childHashes[bit]); hash != nil {
				roots[childPrefix] = hash
			}
			continue
		}
		if branch.children[bit] == nil && validRootHash(branch.childHashes[bit]) {
			child, err := t.loadRootBranchLocked(branch.childHashes[bit], depth+1, childPrefix)
			if err != nil {
				return err
			}
			branch.children[bit] = child
		}
		if err := t.collectShardRootsLocked(branch.children[bit], depth+1, childPrefix, roots); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) allShardRoots() (map[int][]byte, error) {
	t.rootMu.Lock()
	defer t.rootMu.Unlock()

	roots := make(map[int][]byte)
	if t.config.ShardDepth == 0 {
		if root := normalizeRootHash(t.rootHash); root != nil {
			roots[0] = root
		}
		return roots, nil
	}
	if err := t.collectShardRootsLocked(t.rootBranch, 0, 0, roots); err != nil {
		return nil, err
	}
	return roots, nil
}

// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.

// Package archivev3 把 v3 档案实现（trie/archive/mpt，与 E2 同源）包成
// core/state 的 Trie 接口，供 E1 主网回放的 AMT 臂使用。
//
// 独立成包是因为导入方向：mpt 依赖 triedb，triedb 依赖 trie，因此 trie 包
// （老 ArchiveTrie 所在包）不能反向 import mpt；本包只被 core/state 和
// harness 引用，不构成环。
package archivev3

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/archive"
	"github.com/ethereum/go-ethereum/trie/archive/mpt"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// 键域分隔标记，与老 trie.ArchiveTrie 的键域方案逐字节一致，保证同一
// KVStore 下账户、存储、代码互不冲突：
//   - 账户：addr.Bytes()（20 字节，首字节不动）
//   - 存储：20+32 复合键，首字节 xor 0x01
//   - 代码：codeHash.Bytes()，首字节 xor 0x02
const (
	storageDomain byte = 0x01
	codeDomain    byte = 0x02
)

// errUnsupported 记录 v3 档案树暂不支持的接口，报错而非静默降级。
var errUnsupported = errors.New("archivev3: operation unsupported")

// ArchiveTrie 是 v3 档案树的 state.Trie 包装，角色与老 trie.ArchiveTrie 相同：
// 全栈 EVM 回放链中 AMT 臂的底层树。账户/存储/代码共用一棵 v3 树，通过键域
// 分隔；v3 是任意字节键的扁平 16 叉树。分片、复活、裁剪轮换周期、归档桶
// 语义全部由 mpt.Trie 自带，wrapper 只做键域翻译和接口适配。
//
// 路径后端（BackendPath）不能收养既有 root，所以 wrapper 全程持有同一个
// 持久 mpt.Trie（构造时 root 为空），并借助 triedb 的 SetArchiveTrie 槽位
// 跨区块复用：state.New 每块调用 OpenTrie，槽位命中即返回同一棵树。
type ArchiveV3Trie struct {
	trie *mpt.Trie
	slot archiveTrieSlot
}

// archiveTrieSlot 抽象 triedb.Database 的两个槽位方法。老 trie.ArchiveTrie
// 用匿名接口拿同一对方法；这里显式命名避免与 trie 包的类型耦合。
type archiveTrieSlot interface {
	GetArchiveTrie() interface{}
	SetArchiveTrie(interface{})
}

// NewArchiveTrie 返回持久 v3 档案树的包装。
//
// slot 是 triedb 数据库（须实现 GetArchiveTrie/SetArchiveTrie）；kvStore 是
// v3 树的 KV 后端（持久 LevelDB 或测试用 memorydb），由 harness 打开并负责
// 生命周期；cfg 为空时用 mpt 默认配置。
func NewArchiveV3Trie(slot archiveTrieSlot, kvStore archive.KVStore, cfg *mpt.Config) (*ArchiveV3Trie, error) {
	if slot == nil {
		return nil, errors.New("archivev3: nil triedb slot")
	}
	// 优先复用槽位里的既有实例：OpenTrie 每块都被调用，只有首块真正构造
	// 树，之后全部槽位命中，保持分片状态与裁剪轮换周期进度。
	if v3t := slot.GetArchiveTrie(); v3t != nil {
		if wt, ok := v3t.(*ArchiveV3Trie); ok {
			return wt, nil
		}
		return nil, errors.New("archivev3: triedb archive slot holds a different trie type")
	}
	if kvStore == nil {
		return nil, errors.New("archivev3: nil kv store")
	}
	t, err := mpt.New(nil, kvStore, cfg)
	if err != nil {
		return nil, err
	}
	wt := &ArchiveV3Trie{trie: t, slot: slot}
	// 放回槽位，下一次 OpenTrie 复用同一实例。
	slot.SetArchiveTrie(wt)
	return wt, nil
}

// Trie 暴露底层 v3 树，供 harness 读取统计（归档桶字节数等）。
func (t *ArchiveV3Trie) Trie() *mpt.Trie { return t.trie }

func storageKey(addr common.Address, key []byte) []byte {
	k := make([]byte, common.AddressLength+len(key))
	copy(k, addr.Bytes())
	k[0] ^= storageDomain
	copy(k[common.AddressLength:], key)
	return k
}

func codeKey(codeHash common.Hash) []byte {
	// CopyBytes：直接改 codeHash.Bytes() 底层数组会污染共享 hash 值。
	k := common.CopyBytes(codeHash.Bytes())
	k[0] ^= codeDomain
	return k
}

// GetAccount 读取账户。mpt 的 ErrNotFound 统一归一为 (nil, nil)。
func (t *ArchiveV3Trie) GetAccount(address common.Address) (*types.StateAccount, error) {
	data, err := t.trie.Get(address.Bytes())
	if err != nil {
		if errors.Is(err, mpt.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if acc, err := types.FullAccount(data); err == nil {
		return acc, nil
	}
	var acc types.StateAccount
	if err := rlp.DecodeBytes(data, &acc); err != nil {
		return nil, err
	}
	return &acc, nil
}

// GetStorage 按“地址域 + storage key”复合键读取存储槽。
func (t *ArchiveV3Trie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	val, err := t.trie.Get(storageKey(addr, key))
	if err != nil && errors.Is(err, mpt.ErrNotFound) {
		return nil, nil
	}
	return val, err
}

// UpdateAccount 写入账户 RLP（Slim 编码，与老 wrapper 同口径）。
func (t *ArchiveV3Trie) UpdateAccount(address common.Address, acc *types.StateAccount, codeLen int) error {
	return t.trie.Put(address.Bytes(), types.SlimAccountRLP(*acc))
}

// UpdateAccountRLP 直接写入上层编码好的账户 RLP。
func (t *ArchiveV3Trie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	return t.trie.Put(address.Bytes(), account)
}

// UpdateStorage 写入一个存储槽。
func (t *ArchiveV3Trie) UpdateStorage(addr common.Address, key, value []byte) error {
	return t.trie.Put(storageKey(addr, key), value)
}

// DeleteAccount 删除账户（仅账户键；存储槽由调用方逐个删除）。
func (t *ArchiveV3Trie) DeleteAccount(address common.Address) error {
	return t.trie.Delete(address.Bytes())
}

// DeleteStorage 删除一个存储槽。
func (t *ArchiveV3Trie) DeleteStorage(addr common.Address, key []byte) error {
	return t.trie.Delete(storageKey(addr, key))
}

// UpdateContractCode 写入合约代码。
func (t *ArchiveV3Trie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return t.trie.Put(codeKey(codeHash), code)
}

// GetKey 在 v3 扁平键域下无意义，返回 nil（与老 wrapper 一致）。
func (t *ArchiveV3Trie) GetKey(key []byte) []byte { return nil }

// Hash 返回当前 v3 树根（不触发重算，与老 wrapper 的语义一致）。
func (t *ArchiveV3Trie) Hash() common.Hash {
	return common.BytesToHash(t.trie.Root())
}

// Commit 把脏数据落盘并返回新根。NodeSet 恒为 nil：v3 树在内部完成节点与
// 归档桶的原子提交，不走 triedb 的 NodeSet 路径（与老 wrapper 的 binary
// 提交契约相同，state 层的 db.Update 对空 set 直接跳过）。
func (t *ArchiveV3Trie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	root, err := t.trie.Commit()
	if err != nil {
		return common.Hash{}, nil
	}
	return common.BytesToHash(root), nil
}

// Witness 未实现，返回空。
func (t *ArchiveV3Trie) Witness() map[string]struct{} { return nil }

// NodeIterator / Prove：v3 档案树不参与见证与证明遍历路径。NodeIterator
// 返回 leaf-only 迭代器（服务 storage wiping），与老 wrapper 的口径一致。
func (t *ArchiveV3Trie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return newV3LeafIterator(t.trie, nil), nil
}

func (t *ArchiveV3Trie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return errUnsupported
}

func (t *ArchiveV3Trie) IsVerkle() bool { return false }

// PruneNextShard 转发 v3 树的分片裁剪（一轮裁剪轮换周期推进一个分片）。
func (t *ArchiveV3Trie) PruneNextShard() error { return t.trie.PruneNextShard() }

// Flush 把 v3 树的内部缓冲刷到 KVStore（收档前调用）。
func (t *ArchiveV3Trie) Flush() error { return t.trie.Flush() }

// ArchivePayloadBytes 返回归档桶字节数（harness 统计列用）。
func (t *ArchiveV3Trie) ArchivePayloadBytes() int64 { return t.trie.ArchivePayloadBytes() }

// String 满足 fmt.Stringer。
func (t *ArchiveV3Trie) String() string { return fmt.Sprintf("archivev3.Trie(root=%x)", t.trie.Root()) }

// StorageTrie 是存储视图：OpenStorageTrie 的返回值。所有方法转发到主
// wrapper，仅用于满足 state 层的两树结构（verkle 同款处理）。
type StorageTrie struct {
	address common.Address
	parent  *ArchiveV3Trie
}

// NewStorageTrie 返回主 wrapper 的存储视图（绑定账户，storage wiping 的
// leaf iterator 依赖账户前缀）。
func NewStorageTrie(address common.Address, parent *ArchiveV3Trie) *StorageTrie {
	return &StorageTrie{address: address, parent: parent}
}

func (s *StorageTrie) GetKey(key []byte) []byte { return nil }

func (s *StorageTrie) GetAccount(address common.Address) (*types.StateAccount, error) {
	return nil, errUnsupported
}

func (s *StorageTrie) GetStorage(addr common.Address, key []byte) ([]byte, error) {
	return s.parent.GetStorage(addr, key)
}

func (s *StorageTrie) UpdateAccount(address common.Address, acc *types.StateAccount, codeLen int) error {
	return errUnsupported
}

func (s *StorageTrie) UpdateAccountRLP(address common.Address, account []byte, codeLen int) error {
	return errUnsupported
}

func (s *StorageTrie) UpdateStorage(addr common.Address, key, value []byte) error {
	return s.parent.UpdateStorage(addr, key, value)
}

func (s *StorageTrie) DeleteAccount(address common.Address) error {
	return errUnsupported
}

func (s *StorageTrie) DeleteStorage(addr common.Address, key []byte) error {
	return s.parent.DeleteStorage(addr, key)
}

func (s *StorageTrie) UpdateContractCode(address common.Address, codeHash common.Hash, code []byte) error {
	return errUnsupported
}

func (s *StorageTrie) Hash() common.Hash { return s.parent.Hash() }

// Commit 由主 wrapper 统一提交，存储视图的 Commit 是只读空操作。
func (s *StorageTrie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet) {
	return s.parent.Hash(), nil
}

func (s *StorageTrie) Witness() map[string]struct{} { return nil }

func (s *StorageTrie) NodeIterator(startKey []byte) (trie.NodeIterator, error) {
	return newV3LeafIterator(s.parent.trie, storagePrefix(s.address)), nil
}

func (s *StorageTrie) Prove(key []byte, proofDb ethdb.KeyValueWriter) error {
	return errUnsupported
}

func (s *StorageTrie) IsVerkle() bool { return false }

func (s *StorageTrie) PruneNextShard() error { return s.parent.PruneNextShard() }

// storagePrefix 返回该账户 storage 域的键前缀（首字节 xor 0x01）。
func storagePrefix(address common.Address) []byte {
	p := make([]byte, common.AddressLength)
	copy(p, address.Bytes())
	p[0] ^= storageDomain
	return p
}

// v3LeafIterator 为 storage wiping 提供 leaf-only NodeIterator：构造时经
// mpt.Trie.ForEach 一次性收集（全局或账户前缀下）的叶子，Next 只移动
// 游标。口径与老 trie.ArchiveTrie 的 archiveStorageIterator 一致。
type v3LeafIterator struct {
	prefix []byte
	leaves []leafKV
	index  int
	err    error
}

type leafKV struct {
	key   []byte
	value []byte
}

func newV3LeafIterator(t *mpt.Trie, prefix []byte) *v3LeafIterator {
	it := &v3LeafIterator{prefix: prefix, index: -1}
	t.ForEachPrefix(prefix, len(prefix)*8, func(key, value []byte) bool {
		if len(prefix) == 0 || bytes.HasPrefix(key, prefix) {
			it.leaves = append(it.leaves, leafKV{
				key:   common.CopyBytes(key[len(prefix):]),
				value: common.CopyBytes(value),
			})
		}
		return true
	})
	return it
}

func (it *v3LeafIterator) Next(bool) bool                         { it.index++; return it.index < len(it.leaves) }
func (it *v3LeafIterator) Error() error                           { return it.err }
func (it *v3LeafIterator) Hash() common.Hash                      { return common.Hash{} }
func (it *v3LeafIterator) Parent() common.Hash                    { return common.Hash{} }
func (it *v3LeafIterator) Path() []byte                           { return nil }
func (it *v3LeafIterator) NodeBlob() []byte                       { return nil }
func (it *v3LeafIterator) Leaf() bool                             { return true }
func (it *v3LeafIterator) LeafKey() []byte                        { return it.leaves[it.index].key }
func (it *v3LeafIterator) LeafBlob() []byte                       { return it.leaves[it.index].value }
func (it *v3LeafIterator) LeafProof() [][]byte                    { return nil }
func (it *v3LeafIterator) AddResolver(resolver trie.NodeResolver) {}

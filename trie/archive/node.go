package archive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/trie/archive/cuckoo"
	"github.com/ethereum/go-ethereum/trie/archive/ecmh"
)

const (
	NodeTypeInternal      = 0x00
	NodeTypeLeaf          = 0x01
	NodeTypeArchiveBucket = 0x02 // 归档桶节点。

	epochTimeBit          = 0x01
	epochSubtreeMaskBits  = 0x06
	epochSubtreeMaskShift = 1
	epochSubtreeMaskValid = 0x08
	epochArchivePresent   = 0x10
	epochArchiveValid     = 0x20
)

func leafEpochMask(epoch byte) byte {
	return 1 << (epoch & epochTimeBit)
}

func storedSubtreeEpochMask(epoch byte) (byte, bool) {
	if epoch&epochSubtreeMaskValid == 0 {
		return 0, false
	}
	return (epoch & epochSubtreeMaskBits) >> epochSubtreeMaskShift, true
}

func setStoredSubtreeEpochMask(epoch byte, mask byte) byte {
	epoch &^= epochSubtreeMaskBits | epochSubtreeMaskValid
	return epoch | epochSubtreeMaskValid | ((mask & 0x03) << epochSubtreeMaskShift)
}

func clearStoredSubtreeEpochMask(epoch byte) byte {
	return epoch &^ (epochSubtreeMaskBits | epochSubtreeMaskValid)
}

func storedSubtreeArchivePresence(epoch byte) (bool, bool) {
	if epoch&epochArchiveValid == 0 {
		return false, false
	}
	return epoch&epochArchivePresent != 0, true
}

func setStoredSubtreeArchivePresence(epoch byte, present bool) byte {
	epoch |= epochArchiveValid
	if present {
		return epoch | epochArchivePresent
	}
	return epoch &^ epochArchivePresent
}

func clearStoredSubtreeArchivePresence(epoch byte) byte {
	return epoch &^ (epochArchivePresent | epochArchiveValid)
}

// Node 接口：统一描述二叉 Trie 节点的核心行为
// - Type：节点类型（内部/叶子）
// - Hash/SetHash：节点当前哈希（提交后生成的持久化标识）
// - IsDirty/SetDirty：脏标记（是否需要递归提交到数据库）
// - OriginalHash/SetOriginalHash：上次提交时的哈希（用于剪枝时的物理删除）
// - Epoch/SetEpoch：8 位时间状态字段（bit0 核心时间位；bit1 子状态校验位；bit2~bit7 预留）
// - Serialize：节点序列化为字节（用于持久化）
// Node 是二叉 Trie 的节点接口，统一序列化/哈希/脏标记等行为。
type Node interface {
	Type() byte
	Hash() []byte
	SetHash([]byte)
	IsDirty() bool
	SetDirty(bool)
	OriginalHash() []byte
	SetOriginalHash([]byte)
	StoragePath() ([]byte, int)
	SetStoragePath([]byte, int)
	Epoch() byte
	SetEpoch(byte)
	Serialize() ([]byte, error)
}

func serializePathBytes(path []byte, bits int, label string) ([]byte, error) {
	if bits < 0 {
		return nil, fmt.Errorf("%s bits negative: %d", label, bits)
	}
	if bits > MaxPathBits {
		return nil, fmt.Errorf("%s bits %d exceeds max %d", label, bits, MaxPathBits)
	}
	pathLen := (bits + 7) / 8
	if len(path) < pathLen {
		return nil, fmt.Errorf("%s length %d shorter than %d bits", label, len(path), bits)
	}
	if pathLen == 0 {
		return nil, nil
	}
	out := append([]byte(nil), path[:pathLen]...)
	if rem := bits % 8; rem != 0 {
		out[len(out)-1] &= byte(0xff << (8 - rem))
	}
	return out, nil
}

type InternalNode struct {
	Path     []byte
	PathBits int

	Left  Node
	Right Node

	// LeftHash / RightHash：用于序列化与懒加载，在 Commit 阶段更新
	LeftHash  []byte
	RightHash []byte

	// 子节点 epoch 摘要，用来让剪枝/提交跳过无需加载的干净子树。
	LeftEpoch  byte
	RightEpoch byte

	// StubList 保存侧挂归档桶。它用于短期聚合小冷桶，桶成熟后再下沉到 child edge。
	StubList []*ArchiveBucketNode

	hash         []byte
	dirty        bool
	originalHash []byte
	storagePath  []byte
	storageBits  int
	epoch        byte
}

func (n *InternalNode) Reset() {
	n.Path = nil
	n.PathBits = 0
	n.Left = nil
	n.Right = nil
	n.LeftHash = nil
	n.RightHash = nil
	n.LeftEpoch = 0
	n.RightEpoch = 0
	n.StubList = n.StubList[:0]
	n.hash = nil
	n.dirty = true
	n.originalHash = nil
	n.storagePath = nil
	n.storageBits = 0
	n.epoch = 0
}

func NewInternalNode(left, right Node) *InternalNode {
	return &InternalNode{
		Path:     nil,
		PathBits: 0,
		Left:     left,
		Right:    right,
		dirty:    true,
	}
}

func (n *InternalNode) Type() byte {
	return NodeTypeInternal
}

func (n *InternalNode) Epoch() byte {
	return n.epoch
}

func (n *InternalNode) SetEpoch(e byte) {
	n.epoch = e
}

func (n *InternalNode) Hash() []byte {
	return n.hash
}

func (n *InternalNode) SetHash(h []byte) {
	n.hash = h
}

func (n *InternalNode) IsDirty() bool {
	return n.dirty
}

func (n *InternalNode) SetDirty(d bool) {
	n.dirty = d
	if d {
		n.hash = nil
	}
}

func (n *InternalNode) OriginalHash() []byte {
	return n.originalHash
}

func (n *InternalNode) SetOriginalHash(h []byte) {
	n.originalHash = h
}

func (n *InternalNode) StoragePath() ([]byte, int) {
	return n.storagePath, n.storageBits
}

func (n *InternalNode) SetStoragePath(path []byte, bits int) {
	n.storagePath = append(n.storagePath[:0], path...)
	n.storageBits = bits
}

func (n *InternalNode) Serialize() ([]byte, error) {
	var scratch [binary.MaxVarintLen64]byte
	path, err := serializePathBytes(n.Path, n.PathBits, "internal path")
	if err != nil {
		return nil, err
	}
	stubData := make([][]byte, len(n.StubList))
	stubBytes := 0
	for i, bucket := range n.StubList {
		bData, err := bucket.Serialize()
		if err != nil {
			return nil, err
		}
		stubData[i] = bData
		stubBytes += uvarintLen(uint64(len(bData))) + len(bData)
	}

	size := 1 + uvarintLen(uint64(n.PathBits)) + len(path) +
		1 + len(n.LeftHash) + 1 +
		1 + len(n.RightHash) + 1 +
		uvarintLen(uint64(len(n.StubList))) + stubBytes
	buf := make([]byte, 0, size)

	// InternalNode Type=0, so just Epoch & 0x7F
	buf = append(buf, n.epoch&0x7F)

	// PathBits: Uvarint
	nBits := binary.PutUvarint(scratch[:], uint64(n.PathBits))
	buf = append(buf, scratch[:nBits]...)

	// Path: Raw bytes
	buf = append(buf, path...)

	// Children hashes & epochs
	buf = append(buf, byte(len(n.LeftHash)))
	buf = append(buf, n.LeftHash...)
	buf = append(buf, n.LeftEpoch)

	buf = append(buf, byte(len(n.RightHash)))
	buf = append(buf, n.RightHash...)
	buf = append(buf, n.RightEpoch)

	// 序列化 StubList：先写 bucket 数量，再逐个写入 bucket 内容。
	nBits = binary.PutUvarint(scratch[:], uint64(len(n.StubList)))
	buf = append(buf, scratch[:nBits]...)

	// 依次写入每个桶的数据（桶内包含了路径和 ArchivedData）
	for _, bData := range stubData {
		// 写入桶数据的长度
		nBits = binary.PutUvarint(scratch[:], uint64(len(bData)))
		buf = append(buf, scratch[:nBits]...)
		buf = append(buf, bData...)
	}

	return buf, nil
}

// ClearCaches 清除侧挂桶的缓存。
func (n *InternalNode) ClearCaches() {
	for _, bucket := range n.StubList {
		if bucket != nil {
			bucket.ClearCaches()
		}
	}
	if n.Left != nil {
		if ln, ok := n.Left.(*InternalNode); ok {
			ln.ClearCaches()
		}
	}
	if n.Right != nil {
		if rn, ok := n.Right.(*InternalNode); ok {
			rn.ClearCaches()
		}
	}
}

// LeafNode 存储路径后缀（位序列）与值哈希。
// 字段说明：
// - Path/PathBits：叶子路径的位后缀（起始于当前子树深度），PathBytes 为按高位在前的字节压缩
// - ValueHash：值的哈希（实际值被单独持久化，剪枝时可通过哈希读取）
// - hash/dirty/originalHash：与 InternalNode 同语义
// - epoch：叶子时间状态位（bit0 按分片剪枝状态与全局年度位计算）
type LeafNode struct {
	Path      []byte // 路径后缀的位序列（相对当前子树深度）
	PathBits  int    // 路径后缀的位数
	ValueHash []byte

	hash         []byte
	dirty        bool
	originalHash []byte
	storagePath  []byte
	storageBits  int
	epoch        byte
}

func NewLeafNode(path []byte, bits int, valueHash []byte) *LeafNode {
	return &LeafNode{
		Path:      path,
		PathBits:  bits,
		ValueHash: valueHash,
		dirty:     true,
	}
}

func (n *LeafNode) Reset() {
	n.Path = nil
	n.PathBits = 0
	n.ValueHash = nil
	n.hash = nil
	n.dirty = true
	n.originalHash = nil
	n.storagePath = nil
	n.storageBits = 0
	n.epoch = 0
}

func (n *LeafNode) Type() byte {
	return NodeTypeLeaf
}

func (n *LeafNode) Epoch() byte {
	return n.epoch
}

func (n *LeafNode) SetEpoch(e byte) {
	n.epoch = e
}

func (n *LeafNode) Hash() []byte {
	return n.hash
}

func (n *LeafNode) SetHash(h []byte) {
	n.hash = h
}

func (n *LeafNode) IsDirty() bool {
	return n.dirty
}

func (n *LeafNode) SetDirty(d bool) {
	n.dirty = d
	if d {
		n.hash = nil
	}
}

func (n *LeafNode) OriginalHash() []byte {
	return n.originalHash
}

func (n *LeafNode) SetOriginalHash(h []byte) {
	n.originalHash = h
}

func (n *LeafNode) StoragePath() ([]byte, int) {
	return n.storagePath, n.storageBits
}

func (n *LeafNode) SetStoragePath(path []byte, bits int) {
	n.storagePath = append(n.storagePath[:0], path...)
	n.storageBits = bits
}

// Serialize 将叶子节点编码为字节序列。
// 格式（按顺序）：
// [Header(1): Type(1)|Epoch(7)] [PathBits(Uvarint)] [Path] [ValueHashLen(1)] [ValueHash]
func (n *LeafNode) Serialize() ([]byte, error) {
	path, err := serializePathBytes(n.Path, n.PathBits, "leaf path")
	if err != nil {
		return nil, err
	}
	// 估算大小: Header(1) + PathBits(Uvarint) + Path + ValueHashLen(1) + ValueHash
	estSize := 1 + binary.MaxVarintLen64 + len(path) + 1 + len(n.ValueHash)
	buf := make([]byte, 0, estSize)

	// Header: [Type(1bit) | Epoch(7bits)]
	// 对于 LeafNode，Type=1，所以最高位为1: (1<<7) | (Epoch & 0x7F)
	buf = append(buf, 0x80|(n.epoch&0x7F))

	// PathBits: Uvarint 编码
	var scratch [binary.MaxVarintLen64]byte
	nBits := binary.PutUvarint(scratch[:], uint64(n.PathBits))
	buf = append(buf, scratch[:nBits]...)

	// Path 路径后缀
	buf = append(buf, path...)

	// ValueHash 值的哈希
	if len(n.ValueHash) > 255 {
		return nil, errors.New("value hash length exceeds 255 bytes")
	}
	buf = append(buf, byte(len(n.ValueHash)))
	buf = append(buf, n.ValueHash...)

	return buf, nil
}

// ArchiveBucketNode 归档桶节点，聚合存储历史数据。
// 它不再参与 Epoch 演进，是 Trie 的“冷”数据固化结果。
type ArchiveBucketNode struct {
	Path     []byte // 桶的绝对入口路径；桶上浮时保持不变，不向下迁移
	PathBits int    // 路径位数

	Filter     []byte // 布谷鸟过滤器序列化数据
	Commitment []byte // ECMH 承诺 (K + Hash(V))
	Keys       []ArchivedKey
	Count      uint64 // 桶内数据项数量

	hash         []byte
	dirty        bool
	originalHash []byte
	storagePath  []byte
	storageBits  int
	// 归档桶不再需要 epoch，保持静态

	// [CACHE] 缓存解码后的过滤器和承诺点，避免重复解码。
	// 这些字段不序列化到磁盘。
	cachedFilter          *cuckoo.Filter
	cachedCommitmentPoint *ecmh.Point
	cachedMeta            []byte
	cacheMu               sync.RWMutex
	metaMu                sync.RWMutex
}

func NewArchiveBucketNode(path []byte, bits int, filter []byte, commitment []byte, count uint64) *ArchiveBucketNode {
	return &ArchiveBucketNode{
		Path:       path,
		PathBits:   bits,
		Filter:     filter,
		Commitment: commitment,
		Keys:       nil,
		Count:      count,
		dirty:      true,
	}
}

func (n *ArchiveBucketNode) Type() byte {
	return NodeTypeArchiveBucket
}

func (n *ArchiveBucketNode) Epoch() byte {
	return epochArchiveValid | epochArchivePresent
}

func (n *ArchiveBucketNode) SetEpoch(e byte) {
	// 归档桶不需要设置 Epoch
}

func (n *ArchiveBucketNode) Hash() []byte {
	return n.hash
}

func (n *ArchiveBucketNode) SetHash(h []byte) {
	n.hash = h
}

func (n *ArchiveBucketNode) IsDirty() bool {
	return n.dirty
}

func (n *ArchiveBucketNode) SetDirty(d bool) {
	n.dirty = d
	if d {
		n.hash = nil
	}
}

func (n *ArchiveBucketNode) invalidateMetaCache() {
	n.metaMu.Lock()
	n.cachedMeta = nil
	n.metaMu.Unlock()
}

func (n *ArchiveBucketNode) OriginalHash() []byte {
	return n.originalHash
}

func (n *ArchiveBucketNode) SetOriginalHash(h []byte) {
	n.originalHash = h
}

func (n *ArchiveBucketNode) StoragePath() ([]byte, int) {
	return n.storagePath, n.storageBits
}

func (n *ArchiveBucketNode) SetStoragePath(path []byte, bits int) {
	n.storagePath = append(n.storagePath[:0], path...)
	n.storageBits = bits
}

func (n *ArchiveBucketNode) Serialize() ([]byte, error) {
	n.metaMu.RLock()
	if n.cachedMeta != nil {
		data := append([]byte(nil), n.cachedMeta...)
		n.metaMu.RUnlock()
		return data, nil
	}
	n.metaMu.RUnlock()

	var scratch [binary.MaxVarintLen64]byte
	path, err := serializePathBytes(n.Path, n.PathBits, "bucket path")
	if err != nil {
		return nil, err
	}
	pathBitsLen := uvarintLen(uint64(n.PathBits))
	countLen := uvarintLen(n.Count)
	filterLen := uvarintLen(uint64(len(n.Filter)))
	keysLen := uvarintLen(uint64(len(n.Keys)))
	keySuffixes := make([][]byte, len(n.Keys))
	for i, key := range n.Keys {
		suffix, err := serializePathBytes(key.Suffix, key.SuffixBits, "bucket key suffix")
		if err != nil {
			return nil, err
		}
		keySuffixes[i] = suffix
		keysLen += uvarintLen(uint64(key.SuffixBits)) + len(suffix) + uvarintLen(uint64(len(key.ValueRef))) + len(key.ValueRef)
	}
	size := 1 + pathBitsLen + len(path) + countLen + 1 + len(n.Commitment) + filterLen + len(n.Filter) + keysLen
	buf := make([]byte, 0, size)

	buf = append(buf, 0xC0)

	nBits := binary.PutUvarint(scratch[:], uint64(n.PathBits))
	buf = append(buf, scratch[:nBits]...)

	buf = append(buf, path...)

	nBits = binary.PutUvarint(scratch[:], n.Count)
	buf = append(buf, scratch[:nBits]...)

	buf = append(buf, byte(len(n.Commitment)))
	buf = append(buf, n.Commitment...)

	nBits = binary.PutUvarint(scratch[:], uint64(len(n.Filter)))
	buf = append(buf, scratch[:nBits]...)
	buf = append(buf, n.Filter...)

	nBits = binary.PutUvarint(scratch[:], uint64(len(n.Keys)))
	buf = append(buf, scratch[:nBits]...)
	for i, key := range n.Keys {
		nBits = binary.PutUvarint(scratch[:], uint64(key.SuffixBits))
		buf = append(buf, scratch[:nBits]...)
		buf = append(buf, keySuffixes[i]...)
		nBits = binary.PutUvarint(scratch[:], uint64(len(key.ValueRef)))
		buf = append(buf, scratch[:nBits]...)
		buf = append(buf, key.ValueRef...)
	}

	n.metaMu.Lock()
	n.cachedMeta = append(n.cachedMeta[:0], buf...)
	n.metaMu.Unlock()

	return buf, nil
}

// ClearCaches 清除桶内部缓存的过滤器和数据项。
func (n *ArchiveBucketNode) ClearCaches() {
	n.cacheMu.Lock()
	n.cachedFilter = nil
	n.cachedCommitmentPoint = nil
	n.cacheMu.Unlock()
	n.invalidateMetaCache()
}

// DeserializeNode 将字节序列解码为节点实例。
// 反序列化格式：
// - Internal: [Header] [PathBits(Uvarint)] [Path] [LeftHashLen] [LeftHash] [RightHashLen] [RightHash]
// - Leaf:     [Header] [PathBits(Uvarint)] [Path] [ValueHashLen] [ValueHash]
func DeserializeNode(data []byte) (Node, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}

	header := data[0]
	// 判断类型：
	// bit7=0 -> Internal
	// bit7=1, bit6=0 -> Leaf
	// bit7=1, bit6=1 -> ArchiveBucket
	isLeafOrBucket := (header & 0x80) != 0
	isBucket := (header & 0x40) != 0

	reader := bytes.NewReader(data[1:])

	if !isLeafOrBucket { // InternalNode
		epoch := header & 0x7F
		pathBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read internal path bits: %w", err)
		}
		if pathBits > uint64(MaxPathBits) {
			return nil, fmt.Errorf("internal path bits %d exceeds max %d", pathBits, MaxPathBits)
		}

		pathLen := (int(pathBits) + 7) / 8
		if pathLen > reader.Len() {
			return nil, fmt.Errorf("read internal path: length %d exceeds remaining %d", pathLen, reader.Len())
		}
		path := make([]byte, pathLen)
		if pathLen > 0 {
			if _, err := reader.Read(path); err != nil {
				return nil, fmt.Errorf("read internal path: %w", err)
			}
		}

		leftLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read left len: %w", err)
		}
		leftHash := make([]byte, int(leftLenByte))
		if leftLenByte > 0 {
			if _, err := reader.Read(leftHash); err != nil {
				return nil, fmt.Errorf("read left hash: %w", err)
			}
		}
		leftEpoch, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read left epoch: %w", err)
		}

		rightLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read right len: %w", err)
		}
		rightHash := make([]byte, int(rightLenByte))
		if rightLenByte > 0 {
			if _, err := reader.Read(rightHash); err != nil {
				return nil, fmt.Errorf("read right hash: %w", err)
			}
		}
		rightEpoch, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read right epoch: %w", err)
		}

		// 读取侧挂归档桶。
		stubCount, err := binary.ReadUvarint(reader)
		var stubs []*ArchiveBucketNode
		if err == nil && stubCount > 0 {
			if stubCount > uint64(reader.Len()) {
				return nil, fmt.Errorf("stub count %d exceeds remaining bytes %d", stubCount, reader.Len())
			}
			stubs = make([]*ArchiveBucketNode, stubCount)
			for i := uint64(0); i < stubCount; i++ {
				bLen, err := binary.ReadUvarint(reader)
				if err != nil {
					return nil, fmt.Errorf("read stub %d len: %w", i, err)
				}
				if bLen > uint64(reader.Len()) {
					return nil, fmt.Errorf("read stub %d data: length %d exceeds remaining %d", i, bLen, reader.Len())
				}
				bData := make([]byte, bLen)
				if _, err := reader.Read(bData); err != nil {
					return nil, fmt.Errorf("read stub %d data: %w", i, err)
				}
				bucketNode, err := DeserializeNode(bData)
				if err != nil {
					return nil, fmt.Errorf("deserialize stub %d: %w", i, err)
				}
				stubs[i] = bucketNode.(*ArchiveBucketNode)
			}
		}

		in := &InternalNode{
			Path:       path,
			PathBits:   int(pathBits),
			LeftHash:   leftHash,
			LeftEpoch:  leftEpoch,
			RightHash:  rightHash,
			RightEpoch: rightEpoch,
			StubList:   stubs,
			epoch:      epoch,
			dirty:      false,
		}
		return in, nil

	} else if !isBucket { // LeafNode
		epoch := header & 0x7F
		pathBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read path bits: %w", err)
		}
		if pathBits > uint64(MaxPathBits) {
			return nil, fmt.Errorf("leaf path bits %d exceeds max %d", pathBits, MaxPathBits)
		}

		pathLen := (int(pathBits) + 7) / 8
		if pathLen > reader.Len() {
			return nil, fmt.Errorf("read path: length %d exceeds remaining %d", pathLen, reader.Len())
		}
		path := make([]byte, pathLen)
		if pathLen > 0 {
			if _, err := reader.Read(path); err != nil {
				return nil, fmt.Errorf("read path: %w", err)
			}
		}

		valHashLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read val hash len: %w", err)
		}
		if int(valHashLenByte) > reader.Len() {
			return nil, fmt.Errorf("read val hash: length %d exceeds remaining %d", valHashLenByte, reader.Len())
		}
		valHash := make([]byte, int(valHashLenByte))
		if _, err := reader.Read(valHash); err != nil {
			return nil, fmt.Errorf("read val hash: %w", err)
		}

		return &LeafNode{
			Path:      path,
			PathBits:  int(pathBits),
			ValueHash: valHash,
			dirty:     false,
			epoch:     epoch,
		}, nil

	} else { // ArchiveBucketNode
		// Header 已消耗 1 字节
		pathBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read bucket path bits: %w", err)
		}
		if pathBits > uint64(MaxPathBits) {
			return nil, fmt.Errorf("bucket path bits %d exceeds max %d", pathBits, MaxPathBits)
		}

		pathLen := (int(pathBits) + 7) / 8
		if pathLen > reader.Len() {
			return nil, fmt.Errorf("read bucket path: length %d exceeds remaining %d", pathLen, reader.Len())
		}
		path := make([]byte, pathLen)
		if pathLen > 0 {
			if _, err := reader.Read(path); err != nil {
				return nil, fmt.Errorf("read bucket path: %w", err)
			}
		}

		count, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read bucket count: %w", err)
		}

		commitLen, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read bucket commitment len: %w", err)
		}
		if int(commitLen) > reader.Len() {
			return nil, fmt.Errorf("read bucket commitment: length %d exceeds remaining %d", commitLen, reader.Len())
		}
		commitment := make([]byte, int(commitLen))
		if commitLen > 0 {
			if _, err := reader.Read(commitment); err != nil {
				return nil, fmt.Errorf("read bucket commitment: %w", err)
			}
		}

		filterLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read bucket filter len: %w", err)
		}
		if filterLen > uint64(reader.Len()) {
			return nil, fmt.Errorf("read bucket filter: length %d exceeds remaining %d", filterLen, reader.Len())
		}
		filter := make([]byte, filterLen)
		if filterLen > 0 {
			if _, err := reader.Read(filter); err != nil {
				return nil, fmt.Errorf("read bucket filter: %w", err)
			}
		}

		var keys []ArchivedKey
		if reader.Len() > 0 {
			keyCount, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, fmt.Errorf("read bucket key count: %w", err)
			}
			if keyCount > 0 {
				if keyCount > uint64(reader.Len()) {
					return nil, fmt.Errorf("bucket key count %d exceeds remaining bytes %d", keyCount, reader.Len())
				}
				keys = make([]ArchivedKey, keyCount)
				for i := uint64(0); i < keyCount; i++ {
					suffixBits, err := binary.ReadUvarint(reader)
					if err != nil {
						return nil, fmt.Errorf("read bucket key %d suffix bits: %w", i, err)
					}
					if suffixBits > uint64(MaxPathBits) {
						return nil, fmt.Errorf("bucket key %d suffix bits %d exceeds max %d", i, suffixBits, MaxPathBits)
					}
					suffixLen := (int(suffixBits) + 7) / 8
					if suffixLen > reader.Len() {
						return nil, fmt.Errorf("read bucket key %d suffix: length %d exceeds remaining %d", i, suffixLen, reader.Len())
					}
					suffix := make([]byte, suffixLen)
					if suffixLen > 0 {
						if _, err := reader.Read(suffix); err != nil {
							return nil, fmt.Errorf("read bucket key %d suffix: %w", i, err)
						}
					}
					var valueRef []byte
					if reader.Len() > 0 {
						valueRefLen, err := binary.ReadUvarint(reader)
						if err != nil {
							return nil, fmt.Errorf("read bucket key %d value ref len: %w", i, err)
						}
						if valueRefLen > uint64(reader.Len()) {
							return nil, fmt.Errorf("read bucket key %d value ref: length %d exceeds remaining %d", i, valueRefLen, reader.Len())
						}
						valueRef = make([]byte, int(valueRefLen))
						if valueRefLen > 0 {
							if _, err := reader.Read(valueRef); err != nil {
								return nil, fmt.Errorf("read bucket key %d value ref: %w", i, err)
							}
						}
					}
					keys[i] = ArchivedKey{Suffix: suffix, SuffixBits: int(suffixBits), ValueRef: valueRef}
				}
			}
		}

		return &ArchiveBucketNode{
			Path:       path,
			PathBits:   int(pathBits),
			Filter:     filter,
			Commitment: commitment,
			Keys:       keys,
			Count:      count,
			dirty:      false,
		}, nil
	}
}

package binary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/trie/binary/cuckoo"
)

const (
	NodeTypeInternal      = 0x00
	NodeTypeLeaf          = 0x01
	NodeTypeArchiveBucket = 0x02 // [NEW] 归档桶节点类型
)

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
	Epoch() byte
	SetEpoch(byte)
	Serialize() ([]byte, error)
}

type InternalNode struct {
	Path     []byte
	PathBits int

	Left  Node
	Right Node

	// LeftHash / RightHash：用于序列化与懒加载，在 Commit 阶段更新
	LeftHash  []byte
	RightHash []byte

	// [NEW] 缓存孩子节点的 epoch 信息，避免 Commit 时递归加载非脏节点
	LeftEpoch  byte
	RightEpoch byte

	// StubList [NEW]：侧挂在该节点上的归档桶列表
	// 归档桶不再作为左右孩子，而是作为一个侧挂的列表存在。
	StubList []*ArchiveBucketNode

	hash         []byte
	dirty        bool
	originalHash []byte
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
}

func (n *InternalNode) OriginalHash() []byte {
	return n.originalHash
}

func (n *InternalNode) SetOriginalHash(h []byte) {
	n.originalHash = h
}

func (n *InternalNode) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Header: [Type(1bit) | Epoch(7bits)]
	// InternalNode Type=0, so just Epoch & 0x7F
	buf.WriteByte(n.epoch & 0x7F)

	// PathBits: Uvarint
	scratch := make([]byte, binary.MaxVarintLen64)
	nBits := binary.PutUvarint(scratch, uint64(n.PathBits))
	buf.Write(scratch[:nBits])

	// Path: Raw bytes
	buf.Write(n.Path)

	// Children hashes & epochs
	buf.WriteByte(byte(len(n.LeftHash)))
	buf.Write(n.LeftHash)
	buf.WriteByte(n.LeftEpoch)

	buf.WriteByte(byte(len(n.RightHash)))
	buf.Write(n.RightHash)
	buf.WriteByte(n.RightEpoch)

	// [NEW] StubList 序列化
	// 写入桶的数量
	nBits = binary.PutUvarint(scratch, uint64(len(n.StubList)))
	buf.Write(scratch[:nBits])

	// 依次写入每个桶的数据（桶内包含了路径和 ArchivedData）
	for _, bucket := range n.StubList {
		bData, err := bucket.Serialize()
		if err != nil {
			return nil, err
		}
		// 写入桶数据的长度
		nBits = binary.PutUvarint(scratch, uint64(len(bData)))
		buf.Write(scratch[:nBits])
		buf.Write(bData)
	}

	return buf.Bytes(), nil
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
}

func (n *LeafNode) OriginalHash() []byte {
	return n.originalHash
}

func (n *LeafNode) SetOriginalHash(h []byte) {
	n.originalHash = h
}

// Serialize 将叶子节点编码为字节序列。
// 格式（按顺序）：
// [Header(1): Type(1)|Epoch(7)] [PathBits(Uvarint)] [Path] [ValueHashLen(1)] [ValueHash]
func (n *LeafNode) Serialize() ([]byte, error) {
	// 估算大小: Header(1) + PathBits(Uvarint) + Path + ValueHashLen(1) + ValueHash
	estSize := 1 + binary.MaxVarintLen64 + len(n.Path) + 1 + len(n.ValueHash)
	buf := make([]byte, 0, estSize)

	// Header: [Type(1bit) | Epoch(7bits)]
	// 对于 LeafNode，Type=1，所以最高位为1: (1<<7) | (Epoch & 0x7F)
	buf = append(buf, 0x80|(n.epoch&0x7F))

	// PathBits: Uvarint 编码
	var scratch [binary.MaxVarintLen64]byte
	nBits := binary.PutUvarint(scratch[:], uint64(n.PathBits))
	buf = append(buf, scratch[:nBits]...)

	// Path 路径后缀
	if len(n.Path) > 255 {
		return nil, errors.New("path length exceeds 255 bytes")
	}
	buf = append(buf, n.Path...)

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
	Path     []byte // 桶相对于挂载节点的相对路径
	PathBits int    // 路径位数

	Filter     []byte // 布谷鸟过滤器序列化数据
	Commitment []byte // ECMH 承诺 (K + Hash(V))
	Count      uint64 // 桶内数据项数量

	hash         []byte
	dirty        bool
	originalHash []byte
	// 归档桶不再需要 epoch，保持静态

	// [CACHE] 缓存解码后的过滤器和数据项，避免重复解码/反序列化。
	// 这些字段不序列化到磁盘。
	cachedFilter *cuckoo.Filter
	cachedItems  []ArchivedKV
	cacheMu      sync.RWMutex
}

func NewArchiveBucketNode(path []byte, bits int, filter []byte, commitment []byte, count uint64) *ArchiveBucketNode {
	return &ArchiveBucketNode{
		Path:       path,
		PathBits:   bits,
		Filter:     filter,
		Commitment: commitment,
		Count:      count,
		dirty:      true,
	}
}

func (n *ArchiveBucketNode) Type() byte {
	return NodeTypeArchiveBucket
}

func (n *ArchiveBucketNode) Epoch() byte {
	return 0 // 归档数据无 Epoch 演进
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
}

func (n *ArchiveBucketNode) OriginalHash() []byte {
	return n.originalHash
}

func (n *ArchiveBucketNode) SetOriginalHash(h []byte) {
	n.originalHash = h
}

func (n *ArchiveBucketNode) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Header: bit7=1, bit6=1: ArchiveBucket
	header := byte(0xC0)
	buf.WriteByte(header)

	// PathBits
	scratch := make([]byte, binary.MaxVarintLen64)
	nBits := binary.PutUvarint(scratch, uint64(n.PathBits))
	buf.Write(scratch[:nBits])

	// Path
	buf.Write(n.Path)

	// Count
	nBits = binary.PutUvarint(scratch, n.Count)
	buf.Write(scratch[:nBits])

	// Commitment Length + Commitment
	buf.WriteByte(byte(len(n.Commitment)))
	buf.Write(n.Commitment)

	// Filter Length + Filter
	nBits = binary.PutUvarint(scratch, uint64(len(n.Filter)))
	buf.Write(scratch[:nBits])
	buf.Write(n.Filter)

	// [NEW] Hash：为了在节点重排/加载后能直接通过 metadata 找到 raw 数据
	buf.WriteByte(byte(len(n.hash)))
	buf.Write(n.hash)

	return buf.Bytes(), nil
}

// ClearCaches 清除桶内部缓存的过滤器和数据项。
func (n *ArchiveBucketNode) ClearCaches() {
	n.cacheMu.Lock()
	defer n.cacheMu.Unlock()
	n.cachedFilter = nil
	n.cachedItems = nil
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

		pathLen := (int(pathBits) + 7) / 8
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

		// [NEW] 读取 StubList
		stubCount, err := binary.ReadUvarint(reader)
		var stubs []*ArchiveBucketNode
		if err == nil && stubCount > 0 {
			stubs = make([]*ArchiveBucketNode, stubCount)
			for i := uint64(0); i < stubCount; i++ {
				bLen, err := binary.ReadUvarint(reader)
				if err != nil {
					return nil, fmt.Errorf("read stub %d len: %w", i, err)
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

		return &InternalNode{
			Path:       path,
			PathBits:   int(pathBits),
			LeftHash:   leftHash,
			RightHash:  rightHash,
			LeftEpoch:  leftEpoch,
			RightEpoch: rightEpoch,
			StubList:   stubs,
			dirty:      false,
			epoch:      epoch,
		}, nil

	} else if !isBucket { // LeafNode
		epoch := header & 0x7F
		pathBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read path bits: %w", err)
		}

		pathLen := (int(pathBits) + 7) / 8
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

		pathLen := (int(pathBits) + 7) / 8
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
		filter := make([]byte, filterLen)
		if filterLen > 0 {
			if _, err := reader.Read(filter); err != nil {
				return nil, fmt.Errorf("read bucket filter: %w", err)
			}
		}

		// [NEW] 读取持久化的 hash
		hashLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read bucket hash len: %w", err)
		}
		h := make([]byte, int(hashLenByte))
		if hashLenByte > 0 {
			if _, err := reader.Read(h); err != nil {
				return nil, fmt.Errorf("read bucket hash: %w", err)
			}
		}

		return &ArchiveBucketNode{
			Path:       path,
			PathBits:   int(pathBits),
			Filter:     filter,
			Commitment: commitment,
			Count:      count,
			hash:       h,
			dirty:      false,
		}, nil
	}
}

package binary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	NodeTypeInternal = 0x00
	NodeTypeLeaf     = 0x01
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

	hash         []byte
	dirty        bool
	originalHash []byte
	epoch        byte
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

	// Path: Raw bytes (Length derived from PathBits)
	expectedPathLen := (n.PathBits + 7) / 8
	if len(n.Path) != expectedPathLen {
		// Tolerant or strict? Let's be strict or just write what we have?
		// Logic should ensure consistency.
		if len(n.Path) > 255 { // Safety check mainly
			return nil, errors.New("path length exceeds 255 bytes")
		}
	}
	buf.Write(n.Path)

	if len(n.LeftHash) > 255 || len(n.RightHash) > 255 {
		return nil, errors.New("hash length exceeds 255 bytes")
	}

	buf.WriteByte(byte(len(n.LeftHash)))
	buf.Write(n.LeftHash)

	buf.WriteByte(byte(len(n.RightHash)))
	buf.Write(n.RightHash)

	return buf.Bytes(), nil
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
// 说明：
// - Header: bit7=1 (Leaf), bit0-6=Epoch
// - PathBits: Uvarint 编码
// - Path: 字节序列，长度由 (PathBits+7)/8 计算
// - ValueHashLen/ValueHash: 值哈希长度与内容
func (n *LeafNode) Serialize() ([]byte, error) {
	// Estimate size: Header(1) + PathBits(Uvarint) + Path + ValueHashLen(1) + ValueHash
	estSize := 1 + binary.MaxVarintLen64 + len(n.Path) + 1 + len(n.ValueHash)
	buf := make([]byte, 0, estSize)

	// Header: [Type(1bit) | Epoch(7bits)]
	// LeafNode Type=1, so (1<<7) | (Epoch & 0x7F)
	buf = append(buf, 0x80|(n.epoch&0x7F))

	// PathBits: Uvarint
	var scratch [binary.MaxVarintLen64]byte
	nBits := binary.PutUvarint(scratch[:], uint64(n.PathBits))
	buf = append(buf, scratch[:nBits]...)

	// Path
	if len(n.Path) > 255 {
		return nil, errors.New("path length exceeds 255 bytes")
	}
	buf = append(buf, n.Path...)

	if len(n.ValueHash) > 255 {
		return nil, errors.New("value hash length exceeds 255 bytes")
	}
	buf = append(buf, byte(len(n.ValueHash)))
	buf = append(buf, n.ValueHash...)

	return buf, nil
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
	nodeType := (header >> 7) & 0x01
	epoch := header & 0x7F

	reader := bytes.NewReader(data[1:])

	switch nodeType {
	case 0: // InternalNode (was NodeTypeInternal=0)
		// [Header] [PathBits(Uvarint)] [Path] [LeftHashLen] [LeftHash] [RightHashLen] [RightHash]

		pathBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read internal path bits: %w", err)
		}

		pathLen := (int(pathBits) + 7) / 8
		path := make([]byte, pathLen)
		if pathLen > 0 {
			if _, err := reader.Read(path); err != nil {
				return nil, fmt.Errorf("read internal path (len %d): %w", pathLen, err)
			}
		}

		leftLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read left len: %w", err)
		}
		leftHash := make([]byte, int(leftLenByte))
		if leftLenByte > 0 {
			if _, err := reader.Read(leftHash); err != nil {
				return nil, fmt.Errorf("read left hash (len %d): %w", leftLenByte, err)
			}
		}

		rightLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read right len: %w", err)
		}
		rightHash := make([]byte, int(rightLenByte))
		if rightLenByte > 0 {
			if _, err := reader.Read(rightHash); err != nil {
				return nil, fmt.Errorf("read right hash (len %d): %w", rightLenByte, err)
			}
		}

		return &InternalNode{
			Path:      path,
			PathBits:  int(pathBits),
			LeftHash:  leftHash,
			RightHash: rightHash,
			dirty:     false,
			epoch:     epoch,
		}, nil

	case 1: // LeafNode (was NodeTypeLeaf=1)
		// [Header] [PathBits(Uvarint)] [Path] [ValueHashLen] [ValueHash]

		pathBits, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, fmt.Errorf("read path bits: %w", err)
		}

		pathLen := (int(pathBits) + 7) / 8
		path := make([]byte, pathLen)
		if pathLen > 0 {
			if _, err := reader.Read(path); err != nil {
				return nil, fmt.Errorf("read path (len %d): %w", pathLen, err)
			}
		}

		valHashLenByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("read val hash len: %w", err)
		}
		valHash := make([]byte, int(valHashLenByte))
		if _, err := reader.Read(valHash); err != nil {
			return nil, fmt.Errorf("read val hash (len %d): %w", valHashLenByte, err)
		}

		return &LeafNode{
			Path:      path,
			PathBits:  int(pathBits),
			ValueHash: valHash,
			dirty:     false,
			epoch:     epoch,
		}, nil

	default:
		return nil, fmt.Errorf("unknown node type: %d", nodeType)
	}
}

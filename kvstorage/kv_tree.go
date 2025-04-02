package kvsrotage

import (
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"golang.org/x/crypto/sha3"
	"hash"
	"sync"
)

// TreeNode 表示树中的一个节点
type TreeNode struct {
	Hash      common.Hash // 节点的哈希值
	IsDeleted bool        // 是否已被删除
}

// MerkleTree 表示一棵完整的默克尔树
type MerkleTree struct {
	Levels [][]TreeNode // 按层级存储的树节点
	Height uint64       // 树对应的高度
}

// 编码MerkleTree为字节数组
func encodeMerkleTree(tree *MerkleTree) ([]byte, error) {
	// 创建一个紧凑的表示
	compactTree := struct {
		Levels [][][]byte // 每层的节点编码数据
		Height uint64
	}{
		Height: tree.Height,
	}

	// 编码每一层的节点
	compactTree.Levels = make([][][]byte, len(tree.Levels))
	for i, level := range tree.Levels {
		compactTree.Levels[i] = make([][]byte, len(level))
		for j, node := range level {
			// 只存储必要的信息
			if node.IsDeleted {
				// 已删除的节点只存储1字节
				compactTree.Levels[i][j] = []byte{1}
			} else if node.Hash != (common.Hash{}) {
				// 有哈希值的节点存储其哈希
				compactTree.Levels[i][j] = node.Hash.Bytes()
			} else {
				// 空节点存储0字节
				compactTree.Levels[i][j] = []byte{}
			}
		}
	}

	// 使用RLP编码整个结构
	return rlp.EncodeToBytes(compactTree)
}

// 从字节数组解码MerkleTree
func decodeMerkleTree(data []byte) (*MerkleTree, error) {
	var compactTree struct {
		Levels [][][]byte
		Height uint64
	}

	if err := rlp.DecodeBytes(data, &compactTree); err != nil {
		return nil, err
	}

	tree := &MerkleTree{
		Height: compactTree.Height,
	}

	// 解码每一层的节点
	tree.Levels = make([][]TreeNode, len(compactTree.Levels))
	for i, level := range compactTree.Levels {
		tree.Levels[i] = make([]TreeNode, len(level))
		for j, nodeData := range level {
			if len(nodeData) == 0 {
				// 空节点
				tree.Levels[i][j] = TreeNode{
					Hash:      common.Hash{},
					IsDeleted: false,
				}
			} else if len(nodeData) == 1 && nodeData[0] == 1 {
				// 已删除的节点
				tree.Levels[i][j] = TreeNode{
					Hash:      common.Hash{},
					IsDeleted: true,
				}
			} else {
				// 有哈希值的节点
				tree.Levels[i][j] = TreeNode{
					Hash:      common.BytesToHash(nodeData),
					IsDeleted: false,
				}
			}
		}
	}

	return tree, nil
}

// 计算树的键，每个高度对应一棵树
func calcTreeKey(height uint64) []byte {
	key := make([]byte, 9)
	key[0] = 't' // 't'表示tree
	binary.BigEndian.PutUint64(key[1:], height)
	return key
}

// KVTree 采用层级结构的稀疏树实现
type KVTree struct {
	db             ethdb.Database
	deleteCache    map[uint64]*cacheMerkleTree  // 高度 -> 最新的树状态
	doDeleteHeight map[uint64]map[uint64][]byte // 第一个key是当前高度，第二个key是变更的树高度，value是树的编码
}

type cacheMerkleTree struct {
	tree        *MerkleTree
	cacheHeight uint64
}

type UpdateKV struct {
	Key    common.Hash
	Height uint64
	Value  []byte // 可以为nil
	Index  uint32 // 记录节点在树中的索引位置
}

func NewKVTree(db ethdb.Database) KVTree {
	return KVTree{
		db:             db,
		deleteCache:    make(map[uint64]*cacheMerkleTree),
		doDeleteHeight: make(map[uint64]map[uint64][]byte),
	}
}

func encode(data interface{}) ([]byte, error) {
	return rlp.EncodeToBytes(data)
}

func hashData(data []byte) common.Hash {
	return crypto.Keccak256Hash(data)
}

// 计算父节点哈希，考虑左右顺序
func calcParentHash(left, right common.Hash) common.Hash {
	combined, _ := encode([]common.Hash{left, right})
	return hashData(combined)
}

// 从数据库或缓存中获取树
func (kvt *KVTree) getTree(height uint64) (*MerkleTree, error) {
	// 首先检查缓存
	if tree, exists := kvt.deleteCache[height]; exists {
		return tree.tree, nil
	}

	// 从数据库读取
	treeKey := calcTreeKey(height)
	treeData, err := kvt.db.Get(treeKey)
	if err != nil {
		// 树不存在，返回空树
		return &MerkleTree{
			Levels: [][]TreeNode{},
			Height: height,
		}, nil
	}

	// 解码树
	return decodeMerkleTree(treeData)
}

// GenerateHash 生成Merkle树并返回根哈希
func (kvt *KVTree) GenerateHash(h uint64, kvs []*UpdateKV) (common.Hash, []byte) {
	if len(kvs) == 0 {
		return common.Hash{}, nil
	}

	// 计算所有叶子节点的哈希并设置索引
	leafNodes := make([]common.Hash, 0, len(kvs))

	// 设置每个KV的索引位置
	for i, kv := range kvs {
		kv.Index = uint32(i) // 设置索引位置
		data, _ := encode(kv)
		hash := hashData(data)
		leafNodes = append(leafNodes, hash)
	}

	// 创建层级树结构 - 只保存叶子节点层
	tree := &MerkleTree{
		Levels: make([][]TreeNode, 1), // 只有一层 - 叶子节点层
		Height: h,
	}

	// 初始化叶子节点层
	tree.Levels[0] = make([]TreeNode, len(leafNodes))
	for i, hash := range leafNodes {
		tree.Levels[0][i] = TreeNode{
			Hash:      hash,
			IsDeleted: false,
		}
	}

	// 计算根哈希 - 不存储中间节点，只计算根哈希
	rootHash := calculateRootHash(tree.Levels[0])

	// 编码整棵树
	treeData, _ := encodeMerkleTree(tree)

	return rootHash, treeData
}

// 计算根哈希的辅助函数 - 不存储中间节点
func calculateRootHash(nodes []TreeNode) common.Hash {
	if len(nodes) == 0 {
		return common.Hash{}
	}

	if len(nodes) == 1 {
		return nodes[0].Hash
	}

	// 创建下一层节点
	nextLevel := make([]TreeNode, (len(nodes)+1)/2)

	// 对每对节点计算父节点
	for i := 0; i < len(nodes); i += 2 {
		parentIndex := i / 2

		// 单个节点情况
		if i+1 >= len(nodes) {
			nextLevel[parentIndex] = nodes[i]
		} else {
			// 两个子节点，计算父节点哈希
			leftChild := nodes[i]
			rightChild := nodes[i+1]

			parentHash := calcParentHash(leftChild.Hash, rightChild.Hash)

			// 只有两个子节点都被删除时，父节点才标记为已删除
			isDeleted := leftChild.IsDeleted && rightChild.IsDeleted

			nextLevel[parentIndex] = TreeNode{
				Hash:      parentHash,
				IsDeleted: isDeleted,
			}
		}
	}

	// 递归计算上一层
	return calculateRootHash(nextLevel)
}

// DeleteBind 根据新的逻辑标记节点为删除状态
func (kvt *KVTree) DeleteBind(ch uint64, kvs []*UpdateKV) {
	// 按照高度分组
	kvsByHeight := make(map[uint64][]*UpdateKV)
	for _, kv := range kvs {
		kvsByHeight[kv.Height] = append(kvsByHeight[kv.Height], kv)
	}

	// 处理每个高度的删除
	for height, heightKvs := range kvsByHeight {
		// 获取当前高度的树
		tree, err := kvt.getTree(height)
		if err != nil || len(tree.Levels) == 0 {
			continue // 树不存在，无需删除
		}

		// 确保树至少有一层（叶子节点层）
		if len(tree.Levels) < 1 {
			continue
		}

		// 处理每个需要删除的kv
		for _, kv := range heightKvs {
			// 获取节点索引（如果没有设置，则需要查找）
			nodeIndex := int(kv.Index)

			// 开始标记删除过程
			currentIndex := nodeIndex
			currentLevel := 0

			for {
				// 获取兄弟节点索引
				siblingIndex := currentIndex
				if currentIndex%2 == 0 {
					siblingIndex = currentIndex + 1
				} else {
					siblingIndex = currentIndex - 1
				}

				// 如果兄弟节点不存在，说明这是单节点，不需要处理
				if siblingIndex < len(tree.Levels[currentLevel]) {
					// 标记兄弟节点为已删除（这是关键的变化）
					tree.Levels[currentLevel][siblingIndex].IsDeleted = true
				}

				// 检查当前节点是否已经被删除
				currentNodeDeleted := tree.Levels[currentLevel][currentIndex].IsDeleted

				// 计算父节点索引
				parentLevel := currentLevel + 1
				parentIndex := currentIndex / 2

				// 确保父节点层存在
				if parentLevel >= len(tree.Levels) {
					prantLen := (len(tree.Levels[currentLevel]) + 1) / 2
					if prantLen == 1 {
						//达到根节点，直接结束
						break
					}
					// 添加父节点层
					tree.Levels = append(tree.Levels, make([]TreeNode, prantLen))
				}

				//如果自己没有被删除
				if !currentNodeDeleted {
					// 获取当前节点和兄弟节点的哈希
					currentNodeHash := tree.Levels[currentLevel][currentIndex].Hash
					if siblingIndex < len(tree.Levels[currentLevel]) {
						siblingHash := tree.Levels[currentLevel][siblingIndex].Hash

						// 计算父节点哈希
						var parentHash common.Hash
						if currentIndex%2 == 0 {
							parentHash = calcParentHash(currentNodeHash, siblingHash)
						} else {
							parentHash = calcParentHash(siblingHash, currentNodeHash)
						}

						// 更新父节点哈希
						tree.Levels[parentLevel][parentIndex].Hash = parentHash
					} else {
						//单节点则只处理自己
						tree.Levels[parentLevel][parentIndex].Hash = currentNodeHash
					}
				}

				// 如果当前节点和兄弟节点都已删除，则需要继续向上标记删除
				if currentNodeDeleted {
					// 继续向上递归，将父节点的兄弟节点标记为删除
					currentIndex = parentIndex
					currentLevel = parentLevel
				} else {
					// 当前节点未删除，停止操作
					break
				}
			}
		}

		// 编码并保存树数据
		treeData, _ := encodeMerkleTree(tree)

		// 保存到doDeleteHeight中
		if _, exists := kvt.doDeleteHeight[ch]; !exists {
			kvt.doDeleteHeight[ch] = make(map[uint64][]byte)
		}
		kvt.doDeleteHeight[ch][height] = treeData

		// 更新缓存
		kvt.deleteCache[height] = &cacheMerkleTree{
			tree:        tree,
			cacheHeight: ch,
		}
	}
}

// DeleteHeight 删除指定高度的树
func (kvt *KVTree) DeleteHeight(height uint64) {
	// 将doDeleteHeight中的树更新到数据库，并清理缓存
	batch := kvt.db.NewBatch()

	// 处理当前高度和更小的高度
	for h, trees := range kvt.doDeleteHeight {
		if h <= height {
			// 将所有树写入数据库
			for treeHeight, treeData := range trees {
				treeKey := calcTreeKey(treeHeight)
				if ct, exist := kvt.deleteCache[treeHeight]; exist && ct.cacheHeight <= height {
					delete(kvt.deleteCache, treeHeight)
				}
				batch.Put(treeKey, treeData)
			}

			// 从doDeleteHeight中删除这个高度
			delete(kvt.doDeleteHeight, h)
		}
	}

	// 提交批处理
	batch.Write()
}

// Proof 根据UpdateKV构造证明并返回根哈希
func (kvt *KVTree) Proof(kv *UpdateKV) common.Hash {
	// 获取树
	height := kv.Height
	tree, err := kvt.getTree(height)
	if err != nil || len(tree.Levels) == 0 {
		return common.Hash{} // 树不存在
	}

	// 计算目标叶子节点哈希
	data, _ := encode(kv)
	targetHash := hashData(data)

	// 叶子节点层始终是第0层
	leafLevel := 0

	// 获取节点索引
	nodeIndex := int(kv.Index)

	// 如果索引无效，尝试从数据库获取索引或通过哈希查找
	if nodeIndex <= 0 || nodeIndex >= len(tree.Levels[leafLevel]) {
		// 如果数据库可用，先尝试从数据库获取索引
		if kvt.db != nil {
			storedData, err := kvt.db.Get(kv.Key.Bytes())
			if err == nil && len(storedData) >= 12 { // 确保有足够的数据包含索引
				// 从数据中提取索引
				skv := bytes2StoreKV(storedData)
				if skv.Height == kv.Height {
					nodeIndex = int(skv.Index)
					// 检查索引是否有效
					if nodeIndex >= 0 && nodeIndex < len(tree.Levels[leafLevel]) {
						// 验证索引处的节点哈希是否匹配
						if tree.Levels[leafLevel][nodeIndex].Hash != targetHash {
							// 哈希不匹配，可能是数据已更新，需要通过哈希查找
							nodeIndex = -1
						}
					} else {
						// 索引超出范围，需要通过哈希查找
						nodeIndex = -1
					}
				}
			}
		}

		// 如果从数据库获取索引失败或索引无效，通过哈希查找
		if nodeIndex <= 0 || nodeIndex >= len(tree.Levels[leafLevel]) {
			found := false
			for i, node := range tree.Levels[leafLevel] {
				if node.Hash == targetHash {
					found = true
					nodeIndex = i
					break
				}
			}

			if !found {
				return common.Hash{} // 节点不存在
			}
		}
	} else {
		// 验证索引处的节点哈希是否匹配
		if nodeIndex < len(tree.Levels[leafLevel]) && tree.Levels[leafLevel][nodeIndex].Hash != targetHash {
			// 索引处的哈希不匹配，尝试通过哈希查找
			found := false
			for i, node := range tree.Levels[leafLevel] {
				if node.Hash == targetHash {
					found = true
					nodeIndex = i
					break
				}
			}

			if !found {
				return common.Hash{} // 节点不存在
			}
		}
	}

	// 如果节点已被标记为删除，返回空哈希
	if nodeIndex < len(tree.Levels[leafLevel]) && tree.Levels[leafLevel][nodeIndex].IsDeleted {
		return common.Hash{} // 节点已被删除
	}

	// 构建证明路径并返回根哈希
	return buildProofPath(tree, leafLevel, nodeIndex)
}

// 构建证明路径 - 实现新的证明逻辑
func buildProofPath(tree *MerkleTree, level, index int) common.Hash {
	// 如果索引超出范围，返回空哈希
	if level >= len(tree.Levels) || index >= len(tree.Levels[level]) {
		return common.Hash{}
	}

	// 获取当前节点哈希
	nodeHash := tree.Levels[level][index].Hash

	// 如果是已删除节点，返回空哈希
	if tree.Levels[level][index].IsDeleted {
		return common.Hash{}
	}

	// 如果已经到达根节点层，返回当前哈希
	if level == len(tree.Levels)-1 {
		return nodeHash
	}

	// 计算兄弟节点索引
	siblingIndex := index
	if index%2 == 0 {
		siblingIndex = index + 1
	} else {
		siblingIndex = index - 1
	}

	// 计算父节点索引
	parentLevel := level + 1
	parentIndex := index / 2

	// 获取兄弟节点哈希
	var siblingHash common.Hash

	// 检查兄弟节点是否存在且未被删除
	if siblingIndex < len(tree.Levels[level]) && !tree.Levels[level][siblingIndex].IsDeleted {
		// 兄弟节点存在且未被删除，直接使用其哈希
		siblingHash = tree.Levels[level][siblingIndex].Hash
	} else {
		// 兄弟节点不存在或已被删除，需要递归查找子节点
		siblingHash = findUnDeletedChild(tree, level, siblingIndex)
	}

	// 计算父节点哈希
	var parentHash common.Hash
	if index%2 == 0 {
		parentHash = calcParentHash(nodeHash, siblingHash)
	} else {
		parentHash = calcParentHash(siblingHash, nodeHash)
	}

	// 检查是否有预计算的父节点哈希
	if parentLevel < len(tree.Levels) && parentIndex < len(tree.Levels[parentLevel]) {
		storedParentHash := tree.Levels[parentLevel][parentIndex].Hash
		// 如果存在预计算哈希且不为空，优先使用存储的哈希
		if storedParentHash != (common.Hash{}) {
			parentHash = storedParentHash
		}
	}

	// 将计算出的父节点哈希作为当前节点哈希，继续向上构建路径
	return buildProofPathFromHash(tree, parentLevel, parentIndex, parentHash)
}

// 从特定哈希值开始构建证明路径
func buildProofPathFromHash(tree *MerkleTree, level, index int, nodeHash common.Hash) common.Hash {
	// 如果已经到达根节点层，返回当前哈希
	if level >= len(tree.Levels)-1 {
		return nodeHash
	}

	// 计算兄弟节点索引
	siblingIndex := index
	if index%2 == 0 {
		siblingIndex = index + 1
	} else {
		siblingIndex = index - 1
	}

	// 计算父节点索引
	parentLevel := level + 1
	parentIndex := index / 2

	// 获取兄弟节点哈希
	var siblingHash common.Hash

	// 检查兄弟节点是否存在且未被删除
	if siblingIndex < len(tree.Levels[level]) && !tree.Levels[level][siblingIndex].IsDeleted {
		// 兄弟节点存在且未被删除，直接使用其哈希
		siblingHash = tree.Levels[level][siblingIndex].Hash
	} else {
		// 兄弟节点不存在或已被删除，需要递归查找子节点
		siblingHash = findUnDeletedChild(tree, level, siblingIndex)
	}

	// 计算父节点哈希
	var parentHash common.Hash
	if index%2 == 0 {
		parentHash = calcParentHash(nodeHash, siblingHash)
	} else {
		parentHash = calcParentHash(siblingHash, nodeHash)
	}

	// 检查是否有预计算的父节点哈希
	if parentLevel < len(tree.Levels) && parentIndex < len(tree.Levels[parentLevel]) {
		storedParentHash := tree.Levels[parentLevel][parentIndex].Hash
		// 如果存在预计算哈希且不为空，优先使用存储的哈希
		if storedParentHash != (common.Hash{}) {
			parentHash = storedParentHash
		}
	}

	// 继续向上构建路径
	return buildProofPathFromHash(tree, parentLevel, parentIndex, parentHash)
}

// 查找未被删除的子节点
func findUnDeletedChild(tree *MerkleTree, level, index int) common.Hash {
	// 如果是叶子节点层，无法继续向下查找，返回空哈希
	if level == 0 {
		return common.Hash{}
	}

	// 计算子节点层级和索引
	childLevel := level - 1
	leftChildIndex := index * 2
	rightChildIndex := leftChildIndex + 1

	// 初始化左右子节点哈希
	var leftHash, rightHash common.Hash

	// 检查左子节点
	if leftChildIndex < len(tree.Levels[childLevel]) {
		if !tree.Levels[childLevel][leftChildIndex].IsDeleted {
			// 左子节点存在且未被删除，直接使用其哈希
			leftHash = tree.Levels[childLevel][leftChildIndex].Hash
		} else {
			// 左子节点已被删除，递归查找其子节点
			leftHash = findUnDeletedChild(tree, childLevel, leftChildIndex)
		}
	}

	// 检查右子节点
	if rightChildIndex < len(tree.Levels[childLevel]) {
		if !tree.Levels[childLevel][rightChildIndex].IsDeleted {
			// 右子节点存在且未被删除，直接使用其哈希
			rightHash = tree.Levels[childLevel][rightChildIndex].Hash
		} else {
			// 右子节点已被删除，递归查找其子节点
			rightHash = findUnDeletedChild(tree, childLevel, rightChildIndex)
		}
	}

	// 计算并返回父节点哈希
	return calcParentHash(leftHash, rightHash)
}

// ----------------------------------------------------------------------------------------------
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type hasher struct {
	sha keccakState
}

var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{
			sha: sha3.NewLegacyKeccak256().(keccakState),
		}
	},
}

func newHasher() *hasher {
	h := hasherPool.Get().(*hasher)
	return h
}

func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

func HashKey(key []byte) common.Hash {
	h := newHasher()
	h.sha.Reset()
	h.sha.Write(key)
	n := common.Hash{}
	h.sha.Sum(n[:0])
	returnHasherToPool(h)
	return n
}

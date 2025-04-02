package kvsrotage

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 创建测试用的KV列表
func createTestKVs(count int, height uint64) []*UpdateKV {
	kvs := make([]*UpdateKV, count)
	for i := 0; i < count; i++ {
		kvs[i] = &UpdateKV{
			Key:    common.Hash{byte(i)},
			Height: height,
			Value:  []byte{byte(i * 2)},
			Index:  uint32(i),
		}
	}
	return kvs
}

// TestGenerateHashAndProof 测试生成哈希和证明
func TestGenerateHashAndProof(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	kvDatabase := NewKVDatabase(db)
	ch := uint64(1)
	kvslice, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, ch)

	hash1 := kvslice.LedgerHash()
	assert.Equal(t, common.Hash{}, hash1, "空列表应返回空哈希")

	// 测试场景2：单个节点
	kvs2 := createTestKVs(1, ch)
	kvslice2, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, ch)
	for _, v := range kvs2 {
		kvslice2.TryUpdate(v.Key.Bytes(), v.Value)
	}
	hash2 := kvslice2.LedgerHash()
	kvslice2.KnotBlock(hash2)
	kvDatabase.Commit(hash2)
	hashKey := HashKey(kvs2[0].Key.Bytes())
	keyIndex := kvslice2.st.keyIndices[hashKey]
	hkv := &UpdateKV{
		Key:    hashKey,
		Height: kvs2[0].Height,
		Value:  kvs2[0].Value,
		Index:  keyIndex,
	}
	proof2 := kvDatabase.Proof(hkv)
	assert.Equal(t, hash2, proof2, "单节点的证明应该匹配")

	// 测试场景3：偶数个节点
	ch3 := ch + 1
	kvs3 := createTestKVs(4, ch3)
	kvslice3, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, ch3)
	for _, v := range kvs3 {
		kvslice3.TryUpdate(v.Key.Bytes(), v.Value)
	}
	hash3 := kvslice3.LedgerHash()
	kvslice3.KnotBlock(hash3)
	kvDatabase.Commit(hash3)
	for _, kv := range kvs3 {
		hashKey := HashKey(kv.Key.Bytes())
		keyIndex := kvslice3.st.keyIndices[hashKey]
		hkv := &UpdateKV{
			Key:    hashKey,
			Height: kv.Height,
			Value:  kv.Value,
			Index:  keyIndex,
		}
		proof := kvDatabase.Proof(hkv)
		assert.Equal(t, hash3, proof, "每个节点的证明都应该匹配根哈希")
	}

	// 测试场景4：奇数个节点
	ch4 := ch3 + 1
	kvs4 := createTestKVs(3, ch4)
	kvslice4, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, ch4)
	for _, v := range kvs4 {
		kvslice4.TryUpdate(v.Key.Bytes(), v.Value)
	}
	hash4 := kvslice4.LedgerHash()
	kvslice4.KnotBlock(hash4)
	kvDatabase.Commit(hash4)
	for _, kv := range kvs4 {
		hashKey := HashKey(kv.Key.Bytes())
		keyIndex := kvslice4.st.keyIndices[hashKey]
		hkv := &UpdateKV{
			Key:    hashKey,
			Height: kv.Height,
			Value:  kv.Value,
			Index:  keyIndex,
		}
		proof := kvDatabase.Proof(hkv)
		assert.Equal(t, hash4, proof, "奇数节点的证明也应该匹配根哈希")
	}
}

// TestDeleteBindAndHeight 测试删除操作
func TestDeleteBindAndHeight(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	kvDatabase := NewKVDatabase(db)
	ch := uint64(1)
	kvslice, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, ch)

	// 生成初始数据
	kvs := createTestKVs(4, ch)
	for _, v := range kvs {
		kvslice.TryUpdate(v.Key.Bytes(), v.Value)
	}
	h := kvslice.LedgerHash()
	kvslice.KnotBlock(h)
	kvDatabase.Commit(h)

	// 测试场景1：删除单个节点
	hkv := &UpdateKV{
		Key:    HashKey(kvs[0].Key.Bytes()),
		Height: kvs[0].Height,
		Value:  kvs[0].Value,
	}

	// 测试场景2：删除相邻节点
	hkv2 := &UpdateKV{
		Key:    HashKey(kvs[2].Key.Bytes()),
		Height: kvs[2].Height,
		Value:  kvs[2].Value,
	}
	hkv2List := []*UpdateKV{hkv, hkv2}
	kvDatabase.DeleteBind(ch+1, hkv2List)
	// 验证父节点是否也被标记删除
	//data0, _ := encode(hkv)
	//data1, _ := encode(hkv2)
	//hash0 := hashData(data0)
	//hash1 := hashData(data1)
	//combined, _ := encode([]common.Hash{hash0, hash1})
	//parentHash := hashData(combined)
	//parentKey := calcBindingKey(parentHash, ch)
	//assert.Contains(t, kvDatabase.deleteCache, parentKey)

	// 测试场景3：删除高度
	kvDatabase.DeleteHeight(ch + 1)
	assert.Empty(t, kvDatabase.deleteCache, "删除高度后缓存应该为空")
}

// 检查树的结构，返回各层节点数量
func checkTreeStructure(t *testing.T, kvt *KVTree, height uint64) []int {
	tree, err := kvt.getTree(height)
	if err != nil {
		t.Fatalf("获取树失败: %v", err)
		return nil
	}

	nodeCounts := make([]int, len(tree.Levels))
	for i, level := range tree.Levels {
		nodeCounts[i] = len(level)
		t.Logf("高度 %d 的树第 %d 层有 %d 个节点", height, i, nodeCounts[i])
	}
	return nodeCounts
}

// 打印树的结构详细信息
func printTreeDetails(t *testing.T, kvt *KVTree, height uint64) {
	tree, err := kvt.getTree(height)
	if err != nil {
		t.Fatalf("获取树失败: %v", err)
		return
	}

	t.Logf("================== 高度 %d 的树详细结构 ==================", height)
	t.Logf("树层级数: %d", len(tree.Levels))
	result, _ := encodeMerkleTree(tree)
	t.Logf("树大小: %d", len(result))

	for i, level := range tree.Levels {
		t.Logf("--- 第 %d 层 (共 %d 个节点) ---", i, len(level))
		for j, node := range level {
			deleteStatus := "未删除"
			if node.IsDeleted {
				deleteStatus = "已删除"
			}
			t.Logf("  节点[%d]: 哈希=%s, 状态=%s", j, node.Hash.Hex()[:10], deleteStatus)
		}
	}
	t.Logf("===========================================================")
}

// TestDeleteWithProof 测试删除节点后的证明正确性
func TestDeleteWithProof(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	kvDatabase := NewKVDatabase(db)

	// 高度1：创建4个初始KV
	height1 := uint64(1)
	kvslice1, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, height1)

	initialKVs := createTestKVs(4, height1)
	for _, kv := range initialKVs {
		kvslice1.TryUpdate(kv.Key.Bytes(), kv.Value)
	}
	hash1 := kvslice1.LedgerHash()
	kvslice1.KnotBlock(hash1)
	kvDatabase.Commit(hash1)

	// 打印详细树结构
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	// 验证所有初始KV都能生成正确的proof
	for i, kv := range initialKVs {
		hashKey := HashKey(kv.Key.Bytes())
		keyIndex := kvslice1.st.keyIndices[hashKey]
		hkv := &UpdateKV{
			Key:    hashKey,
			Height: kv.Height,
			Value:  kv.Value,
			Index:  keyIndex,
		}
		proof := kvDatabase.Proof(hkv)
		assert.Equal(t, hash1, proof, "初始KV %d 的证明应匹配根哈希", i)
	}

	// 高度2：修改一个KV（选择索引为0的）
	height2 := uint64(2)
	kvslice2, _ := kvDatabase.CreateDBSnapshot(hash1, height2)

	// 修改第一个KV
	modifiedKV := initialKVs[0]
	newValue := []byte{99}
	kvslice2.TryUpdate(modifiedKV.Key.Bytes(), newValue)

	hash2 := kvslice2.LedgerHash()
	kvslice2.KnotBlock(hash2)
	kvDatabase.Commit(hash2)

	// 检查树结构变化
	t.Log("修改一个KV后的树结构（高度1）:")
	//tree2NodeCounts := checkTreeStructure(t, &kvDatabase.KVTree, height1)
	// 打印详细树结构
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	// 验证未修改的三个KV仍能生成正确的proof
	for i := 1; i < len(initialKVs); i++ {
		hashKey := HashKey(initialKVs[i].Key.Bytes())
		keyIndex := kvslice1.st.keyIndices[hashKey]
		hkv := &UpdateKV{
			Key:    hashKey,
			Height: initialKVs[i].Height,
			Value:  initialKVs[i].Value,
			Index:  keyIndex,
		}
		proof := kvDatabase.Proof(hkv)
		t.Logf("KV[%d] proof = %s", i, proof.Hex()[:10])
		assert.NotEqual(t, common.Hash{}, proof, "未修改的KV %d 应该能生成有效证明", i)
	}

	// 验证被修改的KV的新值可以生成正确的proof
	modifiedHashKey := HashKey(modifiedKV.Key.Bytes())
	modifiedKeyIndex := kvslice2.st.keyIndices[modifiedHashKey]
	modifiedHKV := &UpdateKV{
		Key:    modifiedHashKey,
		Height: height2,
		Value:  newValue,
		Index:  modifiedKeyIndex,
	}
	modifiedProof := kvDatabase.Proof(modifiedHKV)
	t.Logf("修改后的KV proof = %s, hash2 = %s", modifiedProof.Hex()[:10], hash2.Hex()[:10])
	assert.Equal(t, hash2, modifiedProof, "修改后的KV应能生成匹配的证明")

	// 高度3：修改索引为1的KV（上一个被修改节点的兄弟节点）
	height3 := uint64(3)
	kvslice3, _ := kvDatabase.CreateDBSnapshot(hash2, height3)

	// 修改第二个KV（兄弟节点）
	siblingKV := initialKVs[2]
	newSiblingValue := []byte{88}
	kvslice3.TryUpdate(siblingKV.Key.Bytes(), newSiblingValue)

	hash3 := kvslice3.LedgerHash()
	kvslice3.KnotBlock(hash3)
	kvDatabase.Commit(hash3)

	// 打印详细树结构
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	// 验证剩余两个未修改的KV仍能生成正确的proof
	for i := 3; i < len(initialKVs); i++ {
		hashKey := HashKey(initialKVs[i].Key.Bytes())
		keyIndex := kvslice1.st.keyIndices[hashKey]
		hkv := &UpdateKV{
			Key:    hashKey,
			Height: initialKVs[i].Height,
			Value:  initialKVs[i].Value,
			Index:  keyIndex,
		}
		proof := kvDatabase.Proof(hkv)
		t.Logf("KV[%d] proof = %s", i, proof.Hex()[:10])
		assert.NotEqual(t, common.Hash{}, proof, "剩余未修改的KV %d 仍应能生成有效证明", i)
	}

	// 测试扩展到8个KV的情况
	t.Log("\n\n=== 开始测试8个KV的情况 ===\n")
	//TestDeleteWith8KVs(t)
}

// TestDeleteWith8KVs 测试8个KV的情况
func TestDeleteWith8KVs(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	kvDatabase := NewKVDatabase(db)

	// 高度1：创建8个初始KV
	height1 := uint64(1)
	kvslice1, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, height1)

	initialKVs := createTestKVs(8, height1)
	for _, kv := range initialKVs {
		kvslice1.TryUpdate(kv.Key.Bytes(), kv.Value)
	}
	hash1 := kvslice1.LedgerHash()
	kvslice1.KnotBlock(hash1)
	kvDatabase.Commit(hash1)

	// 记录初始的树结构
	t.Log("8个KV的初始树结构（高度1）:")
	tree1NodeCounts := checkTreeStructure(t, &kvDatabase.KVTree, height1)
	// 打印详细树结构
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	// 高度2：依次修改两个相邻的KV（例如0和1）
	height2 := uint64(2)
	kvslice2, _ := kvDatabase.CreateDBSnapshot(hash1, height2)

	// 修改第一和第二个KV
	modifiedKVs := []*UpdateKV{
		{
			Key:    initialKVs[0].Key,
			Height: height2,
			Value:  []byte{100},
		},
		{
			Key:    initialKVs[1].Key,
			Height: height2,
			Value:  []byte{101},
		},
	}

	for _, kv := range modifiedKVs {
		kvslice2.TryUpdate(kv.Key.Bytes(), kv.Value)
	}

	hash2 := kvslice2.LedgerHash()
	kvslice2.KnotBlock(hash2)
	kvDatabase.Commit(hash2)

	// 创建要删除的绑定信息
	deleteKVs := []*UpdateKV{
		{
			Key:    HashKey(initialKVs[0].Key.Bytes()),
			Height: height1,
			Value:  initialKVs[0].Value,
			Index:  kvslice1.st.keyIndices[HashKey(initialKVs[0].Key.Bytes())],
		},
		{
			Key:    HashKey(initialKVs[1].Key.Bytes()),
			Height: height1,
			Value:  initialKVs[1].Value,
			Index:  kvslice1.st.keyIndices[HashKey(initialKVs[1].Key.Bytes())],
		},
	}

	// 执行删除绑定操作
	t.Log("执行删除绑定操作 - 删除高度1中的前两个KV")
	kvDatabase.DeleteBind(height2, deleteKVs)

	// 检查树结构变化
	t.Log("修改两个相邻KV后的树结构（高度1）:")
	tree2NodeCounts := checkTreeStructure(t, &kvDatabase.KVTree, height1)
	// 打印详细树结构
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	// 验证树结构变化 - 检查父节点是否标记为已删除
	// 这对应于树中的第二层节点（索引为1）
	if len(tree2NodeCounts) > 1 {
		// 父节点应该被标记为已删除
		tree, _ := kvDatabase.getTree(height1)
		if len(tree.Levels) > 1 && len(tree.Levels[1]) > 0 {
			parentNodeDeleted := false
			for j, node := range tree.Levels[1] {
				if node.IsDeleted {
					parentNodeDeleted = true
					t.Logf("  第1层的第%d个节点被标记为已删除", j)
				}
			}
			assert.True(t, parentNodeDeleted, "应该有父节点被标记为已删除")
		}
	}

	// 高度3：再修改两个相邻的KV（例如2和3）
	height3 := uint64(3)
	kvslice3, _ := kvDatabase.CreateDBSnapshot(hash2, height3)

	// 修改第三和第四个KV
	modifiedKVs2 := []*UpdateKV{
		{
			Key:    initialKVs[2].Key,
			Height: height3,
			Value:  []byte{102},
		},
		{
			Key:    initialKVs[3].Key,
			Height: height3,
			Value:  []byte{103},
		},
	}

	for _, kv := range modifiedKVs2 {
		kvslice3.TryUpdate(kv.Key.Bytes(), kv.Value)
	}

	hash3 := kvslice3.LedgerHash()
	kvslice3.KnotBlock(hash3)
	kvDatabase.Commit(hash3)

	// 创建要删除的绑定信息
	deleteKVs2 := []*UpdateKV{
		{
			Key:    HashKey(initialKVs[2].Key.Bytes()),
			Height: height1,
			Value:  initialKVs[2].Value,
			Index:  kvslice1.st.keyIndices[HashKey(initialKVs[2].Key.Bytes())],
		},
		{
			Key:    HashKey(initialKVs[3].Key.Bytes()),
			Height: height1,
			Value:  initialKVs[3].Value,
			Index:  kvslice1.st.keyIndices[HashKey(initialKVs[3].Key.Bytes())],
		},
	}

	// 执行删除绑定操作
	t.Log("执行删除绑定操作 - 删除高度1中的第三和第四个KV")
	kvDatabase.DeleteBind(height3, deleteKVs2)

	// 检查树结构变化
	t.Log("修改四个KV后的树结构（高度1）:")
	tree3NodeCounts := checkTreeStructure(t, &kvDatabase.KVTree, height1)
	// 打印详细树结构
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	// 验证未修改的KV仍能生成正确的proof
	for i := 4; i < len(initialKVs); i++ {
		hashKey := HashKey(initialKVs[i].Key.Bytes())
		keyIndex := kvslice1.st.keyIndices[hashKey]
		hkv := &UpdateKV{
			Key:    hashKey,
			Height: initialKVs[i].Height,
			Value:  initialKVs[i].Value,
			Index:  keyIndex,
		}
		proof := kvDatabase.Proof(hkv)
		t.Logf("KV[%d] proof = %s", i, proof.Hex()[:10])
		assert.NotEqual(t, common.Hash{}, proof, "剩余未修改的KV %d 仍应能生成有效证明", i)
	}

	// 检查树的大小变化
	totalNodes1 := 0
	for _, count := range tree1NodeCounts {
		totalNodes1 += count
	}

	totalNodes3 := 0
	for _, count := range tree3NodeCounts {
		totalNodes3 += count
	}

	t.Logf("初始树节点总数: %d, 删除后树节点总数: %d", totalNodes1, totalNodes3)
	assert.Less(t, totalNodes3, totalNodes1, "删除后的树应该比初始树小")

	// 测试删除高度后的影响
	t.Log("执行删除高度2操作")
	kvDatabase.DeleteHeight(height2)

	t.Log("删除高度2后，高度1的树结构:")
	checkTreeStructure(t, &kvDatabase.KVTree, height1)
	printTreeDetails(t, &kvDatabase.KVTree, height1)

	t.Log("删除高度2后，高度3的树仍存在:")
	checkTreeStructure(t, &kvDatabase.KVTree, height3)
	printTreeDetails(t, &kvDatabase.KVTree, height3)
}

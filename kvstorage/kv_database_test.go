package kvsrotage

import (
	"bytes"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestKVDatabaseInsertAndQuery 测试KVDatabase的插入和查询功能
func TestKVDatabaseInsertAndQuery(t *testing.T) {
	// 创建内存数据库
	db := rawdb.NewMemoryDatabase()
	kvDatabase := NewKVDatabase(db)

	// 测试场景1：基本的插入和查询
	height1 := uint64(1)
	kvslice1, _ := kvDatabase.CreateDBSnapshot(common.Hash{}, height1)

	// 创建测试数据
	testKey1 := []byte("testKey1")
	testValue1 := []byte("testValue1")
	testKey2 := []byte("testKey2")
	testValue2 := []byte("testValue2")

	// 插入数据
	kvslice1.TryUpdate(testKey1, testValue1)
	kvslice1.TryUpdate(testKey2, testValue2)

	// 计算哈希并提交
	hash1 := kvslice1.LedgerHash()
	kvslice1.KnotBlock(hash1)
	err := kvDatabase.Commit(hash1)
	assert.NoError(t, err, "提交数据应该成功")

	// 测试直接从KVDatabase查询
	hashKey1 := HashKey(testKey1)
	value1, ok1 := kvDatabase.getValue(hashKey1[:])
	assert.True(t, ok1, "应该能够查询到键1")
	assert.Equal(t, testValue1, value1, "查询到的值1应该与插入的值相同")

	hashKey2 := HashKey(testKey2)
	value2, ok2 := kvDatabase.getValue(hashKey2[:])
	assert.True(t, ok2, "应该能够查询到键2")
	assert.Equal(t, testValue2, value2, "查询到的值2应该与插入的值相同")

	// 测试场景2：测试缓存功能
	// 第一次查询后，值应该已经被缓存
	// 修改数据库中的值（但不通过KVDatabase接口）
	modifiedValue1 := []byte("modifiedValue1")
	storeData := storeKV2Bytes(&StoreKV{
		Height: height1,
		Index:  kvslice1.st.keyIndices[hashKey1],
		Value:  modifiedValue1,
	})
	err = db.Put(hashKey1[:], storeData)
	assert.NoError(t, err, "直接修改数据库应该成功")

	// 再次查询，应该返回缓存中的原始值
	cachedValue1, ok1 := kvDatabase.getValue(hashKey1[:])
	assert.True(t, ok1, "应该能够查询到键1")
	assert.Equal(t, testValue1, cachedValue1, "应该返回缓存中的原始值，而不是修改后的值")

	// 清除缓存
	kvDatabase.cache.Purge()

	// 再次查询，应该返回数据库中的修改后的值
	updatedValue1, ok1 := kvDatabase.getValue(hashKey1[:])
	assert.True(t, ok1, "应该能够查询到键1")
	assert.Equal(t, modifiedValue1, updatedValue1, "应该返回数据库中的修改后的值")

	// 测试场景3：测试通过KVSlice查询
	// 创建新的KVSlice
	height2 := uint64(2)
	kvslice2, _ := kvDatabase.CreateDBSnapshot(hash1, height2)

	// 通过KVSlice查询
	valueFromSlice1, err := kvslice2.TryGet(testKey1)
	assert.NoError(t, err, "通过KVSlice查询应该成功")
	assert.Equal(t, modifiedValue1, valueFromSlice1, "通过KVSlice查询应该返回最新的值")

	valueFromSlice2, err := kvslice2.TryGet(testKey2)
	assert.NoError(t, err, "通过KVSlice查询应该成功")
	assert.Equal(t, testValue2, valueFromSlice2, "通过KVSlice查询应该返回原始值")

	// 测试场景4：测试更新值
	testValue3 := []byte("testValue3")
	kvslice2.TryUpdate(testKey1, testValue3)

	// 计算哈希并提交
	hash2 := kvslice2.LedgerHash()
	kvslice2.KnotBlock(hash2)
	err = kvDatabase.Commit(hash2)
	assert.NoError(t, err, "提交更新后的数据应该成功")

	// 查询更新后的值
	updatedValue3, ok3 := kvDatabase.getValue(hashKey1[:])
	assert.True(t, ok3, "应该能够查询到更新后的键1")
	assert.Equal(t, testValue3, updatedValue3, "应该返回更新后的值")

	// 测试场景5：测试二进制数据
	binaryKey := []byte{0x01, 0x02, 0x03, 0x04}
	binaryValue := []byte{0xFF, 0xFE, 0xFD, 0xFC}

	// 创建新的KVSlice
	height3 := uint64(3)
	kvslice3, _ := kvDatabase.CreateDBSnapshot(hash2, height3)

	// 插入二进制数据
	kvslice3.TryUpdate(binaryKey, binaryValue)

	// 计算哈希并提交
	hash3 := kvslice3.LedgerHash()
	kvslice3.KnotBlock(hash3)
	err = kvDatabase.Commit(hash3)
	assert.NoError(t, err, "提交二进制数据应该成功")

	// 查询二进制数据
	hashBinaryKey := HashKey(binaryKey)
	retrievedBinaryValue, okBinary := kvDatabase.getValue(hashBinaryKey[:])
	assert.True(t, okBinary, "应该能够查询到二进制键")
	assert.True(t, bytes.Equal(binaryValue, retrievedBinaryValue), "查询到的二进制值应该与插入的值相同")
}

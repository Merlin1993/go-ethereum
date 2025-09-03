package cachetrie

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestFixedSizeMode(t *testing.T) {
	// 测试创建固定大小模式的缓存树
	startNum := uint64(1000)
	fixedCapacity := uint64(100) // 每个逻辑位固定100个区块
	maxSize := 10000

	trie := NewFixedSizeCacheTrie(startNum, fixedCapacity, maxSize)

	// 验证模式设置
	if trie.GetWindowMode() != ModeFixedSize {
		t.Errorf("期望模式为 ModeFixedSize，实际为 %v", trie.GetWindowMode())
	}

	// 验证固定容量设置
	if trie.GetFixedCapacity() != fixedCapacity {
		t.Errorf("期望固定容量为 %v，实际为 %v", fixedCapacity, trie.GetFixedCapacity())
	}

	// 验证HRW的模式
	hrw := trie.GetHRW()
	if hrw.GetMode() != ModeFixedSize {
		t.Errorf("HRW期望模式为 ModeFixedSize，实际为 %v", hrw.GetMode())
	}

	// 测试动态切换模式
	trie.SetWindowMode(ModeCongestionControl)
	if trie.GetWindowMode() != ModeCongestionControl {
		t.Errorf("期望模式为 ModeCongestionControl，实际为 %v", trie.GetWindowMode())
	}

	// 测试设置固定容量
	newCapacity := uint64(200)
	trie.SetFixedCapacity(newCapacity)
	if trie.GetFixedCapacity() != newCapacity {
		t.Errorf("期望固定容量为 %v，实际为 %v", newCapacity, trie.GetFixedCapacity())
	}
}

func TestFixedSizeModeBehavior(t *testing.T) {
	// 测试固定大小模式的行为
	startNum := uint64(1000)
	fixedCapacity := uint64(50) // 每个逻辑位固定50个区块
	maxSize := 5000

	trie := NewFixedSizeCacheTrie(startNum, fixedCapacity, maxSize)

	// 设置区块号
	trie.SetBlockNum(startNum + 100)

	// 添加一些数据
	key := []byte("test_key")
	value := []byte("test_value")
	address := common.HexToAddress("0x1234567890123456789012345678901234567890")

	err := trie.UpdateWithAddress(address, key, value, true)
	if err != nil {
		t.Errorf("更新失败: %v", err)
	}

	// 验证数据
	node, err := trie.GetWithAddress(address, key)
	if err != nil {
		t.Errorf("获取失败: %v", err)
	}
	if node == nil {
		t.Error("期望获取到节点，实际为nil")
	}

	// 测试拥塞控制不会触发（固定大小模式下）
	// 添加大量数据
	for i := 0; i < maxSize+1000; i++ {
		testKey := []byte(fmt.Sprintf("key_%d", i))
		testValue := []byte(fmt.Sprintf("value_%d", i))
		trie.UpdateWithAddress(address, testKey, testValue, true)
	}

	// 在固定大小模式下，拥塞控制不应该触发
	hrw := trie.GetHRW()
	if hrw.CheckAndTriggerCongestionControl(trie.GetSize()) {
		t.Error("固定大小模式下不应该触发拥塞控制")
	}
}

func TestModeSwitching(t *testing.T) {
	// 测试模式切换
	startNum := uint64(1000)
	multiple := uint64(100)
	maxSize := 5000

	trie := NewCacheTrie(startNum, multiple, maxSize)

	// 初始应该是拥塞控制模式
	if trie.GetWindowMode() != ModeCongestionControl {
		t.Errorf("初始模式应该是 ModeCongestionControl，实际为 %v", trie.GetWindowMode())
	}

	// 切换到固定大小模式
	trie.SetWindowMode(ModeFixedSize)
	if trie.GetWindowMode() != ModeFixedSize {
		t.Errorf("切换后模式应该是 ModeFixedSize，实际为 %v", trie.GetWindowMode())
	}

	// 设置固定容量
	fixedCapacity := uint64(75)
	trie.SetFixedCapacity(fixedCapacity)
	if trie.GetFixedCapacity() != fixedCapacity {
		t.Errorf("期望固定容量为 %v，实际为 %v", fixedCapacity, trie.GetFixedCapacity())
	}

	// 切换回拥塞控制模式
	trie.SetWindowMode(ModeCongestionControl)
	if trie.GetWindowMode() != ModeCongestionControl {
		t.Errorf("切换回后模式应该是 ModeCongestionControl，实际为 %v", trie.GetWindowMode())
	}
}

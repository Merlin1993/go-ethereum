package cacheTrie

import (
	"testing"
)

func TestNewHeightRangeWindow(t *testing.T) {
	tests := []struct {
		name            string
		defaultSsthresh uint64
		defaultMaxSize  int
		wantSsthresh    uint64
	}{
		{
			name:            "正常初始化",
			defaultSsthresh: 10,
			defaultMaxSize:  1000,
			wantSsthresh:    10,
		},
		{
			name:            "零阈值初始化",
			defaultSsthresh: 0,
			defaultMaxSize:  1000,
			wantSsthresh:    1, // 期望被设置为1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hrw := NewHeightRangeWindow(0, tt.defaultSsthresh, tt.defaultMaxSize)
			if hrw.currentSsthresh != tt.wantSsthresh {
				t.Errorf("currentSsthresh = %v, want %v", hrw.currentSsthresh, tt.wantSsthresh)
			}
			if hrw.maxTotalAllowedSize != tt.defaultMaxSize {
				t.Errorf("maxTotalAllowedSize = %v, want %v", hrw.maxTotalAllowedSize, tt.defaultMaxSize)
			}
			if hrw.windowStartNumber != 1 {
				t.Errorf("windowStartNumber = %v, want 1", hrw.windowStartNumber)
			}
			if hrw.windowEndNumber != 0 {
				t.Errorf("windowEndNumber = %v, want 0", hrw.windowEndNumber)
			}
		})
	}
}

func TestGetBitPosition(t *testing.T) {
	tests := []struct {
		name             string
		setupFunc        func() *HeightRangeWindow
		number           uint64
		expectedPosition int
	}{
		{
			name: "查询窗口范围内的数字",
			setupFunc: func() *HeightRangeWindow {
				hrw := NewHeightRangeWindow(0, 10, 1000)
				// 假设初始设置第一个位的容量为1
				return hrw
			},
			number:           1, // 窗口起始为1，所以1应该在范围内
			expectedPosition: 0, // 应该返回第一个逻辑位
		},
		{
			name: "查询窗口范围之前的数字",
			setupFunc: func() *HeightRangeWindow {
				hrw := NewHeightRangeWindow(0, 10, 1000)
				hrw.windowStartNumber = 10 // 调整窗口起始
				return hrw
			},
			number:           5, // 小于窗口起始的10
			expectedPosition: -1,
		},
		{
			name: "查询超出窗口容量的数字",
			setupFunc: func() *HeightRangeWindow {
				hrw := NewHeightRangeWindow(0, 10, 1000)
				// 由于初始化时窗口容量有限，极大的数字应该超出范围
				return hrw
			},
			number:           1000000, // 一个非常大的数字
			expectedPosition: -2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hrw := tt.setupFunc()
			position := hrw.GetBitPosition(tt.number)
			if position != tt.expectedPosition {
				t.Errorf("GetBitPosition(%v) = %v, want %v", tt.number, position, tt.expectedPosition)
			}
		})
	}
}

func TestPruneWindow(t *testing.T) {
	t.Run("正常修剪", func(t *testing.T) {
		hrw := NewHeightRangeWindow(0, 10, 1000)

		// 初始化一些逻辑容量以便于测试
		for i := 0; i < 5; i++ {
			hrw.logicalCapacities[i] = uint64(i + 1)
		}

		originalStartNumber := hrw.windowStartNumber
		originalFirstIndex := hrw.firstSegmentIndex

		// 修剪前3个逻辑位
		hrw.PruneWindow(3)

		// 计算预期的新起始编号：原始起始编号 + 前3个逻辑位的总容量 (1+2+3=6)
		expectedStartNumber := originalStartNumber + 6
		expectedFirstIndex := (originalFirstIndex + 3) % 32

		if hrw.windowStartNumber != expectedStartNumber {
			t.Errorf("修剪后 windowStartNumber = %v, want %v",
				hrw.windowStartNumber, expectedStartNumber)
		}

		if hrw.firstSegmentIndex != expectedFirstIndex {
			t.Errorf("修剪后 firstSegmentIndex = %v, want %v",
				hrw.firstSegmentIndex, expectedFirstIndex)
		}
	})

	t.Run("无效修剪参数", func(t *testing.T) {
		hrw := NewHeightRangeWindow(0, 10, 1000)

		originalStartNumber := hrw.windowStartNumber
		originalFirstIndex := hrw.firstSegmentIndex

		// 使用无效参数：0（不修剪）
		hrw.PruneWindow(0)
		if hrw.windowStartNumber != originalStartNumber ||
			hrw.firstSegmentIndex != originalFirstIndex {
			t.Errorf("使用无效参数0修剪后，窗口状态不应该改变")
		}

		// 使用无效参数：33（超过最大值32）
		hrw.PruneWindow(33)
		if hrw.windowStartNumber != originalStartNumber ||
			hrw.firstSegmentIndex != originalFirstIndex {
			t.Errorf("使用无效参数33修剪后，窗口状态不应该改变")
		}
	})
}

func TestRecalculateLogicalCapacities(t *testing.T) {
	t.Run("初始化后的容量计算", func(t *testing.T) {
		hrw := NewHeightRangeWindow(0, 10, 1000)
		// 初始窗口结束编号比开始编号小，使用容量应为0
		if hrw.windowEndNumber != 0 || hrw.windowStartNumber != 1 {
			t.Errorf("初始窗口范围不正确: start=%v, end=%v", hrw.windowStartNumber, hrw.windowEndNumber)
		}

		// 检查第一个逻辑位容量是否合理
		if hrw.logicalCapacities[0] != 1 {
			t.Errorf("初始化后第一个逻辑位容量应为1，实际为%v", hrw.logicalCapacities[0])
		}
	})

	t.Run("使用后重新计算容量", func(t *testing.T) {
		hrw := NewHeightRangeWindow(0, 4, 1000)

		// 模拟窗口使用
		hrw.windowEndNumber = 10 // 设置窗口结束为10

		// 重新计算容量
		hrw.recalculateLogicalCapacities()

		// 验证容量是否按照预期增长
		// 在使用了10个编号的情况下，容量应该遵循倍增规则直到达到阈值
		expectedCapacities := []uint64{1, 2, 4, 3, 1, 2, 4, 8, 9, 10, 11}
		for i := 0; i < 11; i++ {
			if hrw.logicalCapacities[i] != expectedCapacities[i] {
				t.Errorf("位置%d的容量 = %v, 期望 %v",
					i, hrw.logicalCapacities[i], expectedCapacities[i])
			}
		}
	})
}

// 集成测试：测试多个方法的交互
func TestIntegration(t *testing.T) {
	t.Run("完整使用流程", func(t *testing.T) {
		hrw := NewHeightRangeWindow(0, 4, 100)

		// 第一步：获取编号1的位置
		pos1 := hrw.GetBitPosition(1)
		if pos1 != 0 {
			t.Errorf("编号1应该位于逻辑位置0，实际为%v", pos1)
		}

		// 第二步：获取一些连续的编号位置
		for i := uint64(2); i <= 10; i++ {
			pos := hrw.GetBitPosition(i)
			if pos < 0 {
				t.Errorf("编号%v应该有有效位置，实际返回%v", i, pos)
			}
		}

		// 第三步：检查窗口末尾是否已更新
		if hrw.windowEndNumber != 10 {
			t.Errorf("窗口末尾应为10，实际为%v", hrw.windowEndNumber)
		}

		// 第四步：模拟触发拥塞控制
		triggered := hrw.CheckAndTriggerCongestionControl(150)
		if !triggered {
			t.Errorf("拥塞控制应该被触发")
		}
		if hrw.currentSsthresh != 4 {
			t.Errorf("触发拥塞控制后ssthresh应为4，实际为%v", hrw.currentSsthresh)
		}

		// 第五步：修剪窗口
		oldStart := hrw.windowStartNumber
		hrw.PruneWindow(2)
		if hrw.windowStartNumber <= oldStart {
			t.Errorf("修剪后窗口起始应增加，实际从%v变为%v", oldStart, hrw.windowStartNumber)
		}

		// 第六步：尝试获取已修剪的编号位置
		oldPos := hrw.GetBitPosition(oldStart)
		if oldPos != -1 {
			t.Errorf("已修剪的编号应返回-1，实际返回%v", oldPos)
		}
	})
}

func TestValue(t *testing.T) {
	hrw := NewHeightRangeWindow(0, 4, 100)

	// 验证容量是否按照预期增长
	// 在使用了10个编号的情况下，容量应该遵循倍增规则直到达到阈值
	expectedCapacities := []uint64{1, 2, 4, 8, 9, 10, 11, 12, 13, 14}
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

	// 模拟窗口使用
	number := uint64(10) // 设置窗口结束为10
	expectBit := 3       //预期为3
	bit := hrw.GetBitPosition(number)
	if bit != expectBit {
		t.Errorf("高度%d的容量 = %v, 期望 %v",
			number, bit, expectBit)
	}

	expectedCapacities = []uint64{1, 2, 4, 8, 9, 10, 11, 12, 13, 14} //get方法不影响容量变化
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

	hrw.CheckAndTriggerCongestionControl(101) //触发拥塞控制，当前为8，减半后sshresh还是4

	expectedCapacities = []uint64{1, 2, 4, 4, 1, 2, 4, 8, 9, 10} //拥塞控制后位数变低
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

	number = uint64(44) // 设置窗口结束为44
	expectBit = 9       //预期为9
	bit = hrw.GetBitPosition(number)
	if bit != expectBit {
		t.Errorf("高度%d的容量 = %v, 期望 %v",
			number, bit, expectBit)
	}

	number = uint64(45) // 设置窗口结束为45
	expectBit = 10      //预期为10
	bit = hrw.GetBitPosition(number)
	if bit != expectBit {
		t.Errorf("高度%d的容量 = %v, 期望 %v",
			number, bit, expectBit)
	}

	hrw.PruneWindow(3) //清掉前三个

	expectedCapacities = []uint64{33, 34, 35, 4, 1, 2, 4, 8, 9, 10} //拥塞控制后位数变低
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

	hrw.PruneWindow(3) //清掉前三个

	expectedCapacities = []uint64{33, 34, 35, 36, 37, 38, 4, 8, 9, 10} //拥塞控制后位数变低
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

	number = uint64(517) // 未循环 43*11 + 43 = 430+86=516
	expectBit = 31       //预期为31
	bit = hrw.GetBitPosition(number)
	if bit != expectBit {
		t.Errorf("高度%d的容量 = %v, 期望 %v",
			number, bit, expectBit)
	}

	number = uint64(518) // 循环重回第1位
	expectBit = 0        //预期为0
	bit = hrw.GetBitPosition(number)
	if bit != expectBit {
		t.Errorf("高度%d的容量 = %v, 期望 %v",
			number, bit, expectBit)
	}

	hrw.CheckAndTriggerCongestionControl(100) //低于size无变化

	expectedCapacities = []uint64{33, 34, 35, 36, 37, 38, 4, 8, 9, 10} //拥塞控制后位数变低
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

	hrw.CheckAndTriggerCongestionControl(101)                     //裁剪，此时sshresh应为33/2 = 16
	expectedCapacities = []uint64{1, 1, 2, 4, 8, 16, 4, 8, 9, 10} //拥塞控制后位数变低
	for i := 0; i < 10; i++ {
		if hrw.logicalCapacities[i] != expectedCapacities[i] {
			t.Errorf("位置%d的容量 = %v, 期望 %v",
				i, hrw.logicalCapacities[i], expectedCapacities[i])
		}
	}

}

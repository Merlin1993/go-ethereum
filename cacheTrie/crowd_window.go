package cacheTrie

// 可能需要，用于防止溢出等

type HeightRangeWindow struct {
	initialSsthresh     uint64
	currentSsthresh     uint64
	maxTotalAllowedSize int
	logicalCapacities   [32]uint64
	windowStartNumber   uint64
	windowEndNumber     uint64
	firstSegmentIndex   int

	allSize uint64
}

// NewHeightRangeWindow 创建一个新的 HeightRangeWindow 实例。
// defaultSsthresh: 初始慢启动阈值。
// defaultMaxSize: 触发拥塞控制的最大总大小。
func NewHeightRangeWindow(startNum, defaultSsthresh uint64, defaultMaxSize int) *HeightRangeWindow {
	if defaultSsthresh == 0 {
		defaultSsthresh = 1 // 保证 sshresh 至少为1
	}
	hrw := &HeightRangeWindow{
		initialSsthresh:     defaultSsthresh,
		currentSsthresh:     defaultSsthresh,
		maxTotalAllowedSize: defaultMaxSize,
		windowStartNumber:   startNum,
		windowEndNumber:     startNum - 1,
		firstSegmentIndex:   0,
	}
	hrw.recalculateLogicalCapacities()
	return hrw
}

func (hrw *HeightRangeWindow) recalculateLogicalCapacities() {
	usedCapacity := hrw.windowEndNumber - hrw.windowStartNumber + 1
	totalCapacity := uint64(0)
	lastCapacity := hrw.logicalCapacities[hrw.firstSegmentIndex]
	for i := 0; i < 32; i++ {
		//从第一个序号开始寻找
		cIndex := (i + hrw.firstSegmentIndex) % 32
		//如果已经越过历史使用容量，那么重新计算
		if totalCapacity >= usedCapacity {
			if lastCapacity == 0 {
				//上一个为0，说明已经用完历史记录，直接置为1
				hrw.logicalCapacities[cIndex] = 1
			} else if lastCapacity <= hrw.currentSsthresh {
				//低于上线就倍增
				hrw.logicalCapacities[cIndex] = lastCapacity * 2
			} else {
				//超出上限就递增1
				hrw.logicalCapacities[cIndex] = lastCapacity + 1
			}
			lastCapacity = hrw.logicalCapacities[cIndex]
			continue
		}
		totalCapacity += hrw.logicalCapacities[cIndex]
		lastCapacity = hrw.logicalCapacities[cIndex]
		if totalCapacity >= usedCapacity {
			hrw.logicalCapacities[cIndex] = hrw.logicalCapacities[cIndex] - totalCapacity + usedCapacity
			lastCapacity = 0
		}
	}
	hrw.allSize = 0
	for _, v := range hrw.logicalCapacities {
		hrw.allSize += v
	}
}

// GetBitPosition 返回 number 在当前窗口中对应的逻辑位索引 (0-31)。
// 如果 number 在当前窗口范围之前，则返回 -1。
// 如果 number 超出当前窗口可表示的最大范围，则返回 -2。
func (hrw *HeightRangeWindow) GetBitPosition(number uint64) int {
	if number < hrw.windowStartNumber {
		return -1 // Number is behind the current window
	}

	valueInWindow := number - hrw.windowStartNumber
	cumulativeCapacityOffset := uint64(0)

	for logicalBitIndex := 0; logicalBitIndex < 32; logicalBitIndex++ {
		capacityArrayIndex := (hrw.firstSegmentIndex + logicalBitIndex) % 32
		currentBitCapacity := hrw.logicalCapacities[capacityArrayIndex]

		if valueInWindow >= cumulativeCapacityOffset && valueInWindow < cumulativeCapacityOffset+currentBitCapacity {
			hrw.windowEndNumber = number
			return (hrw.firstSegmentIndex + logicalBitIndex) % 32
		}
		cumulativeCapacityOffset += currentBitCapacity
	}
	return -2 // Number is beyond the current window's total capacity
}

// CheckAndTriggerCongestionControl 检查 currentUsedSize 是否超过 maxSize。
// 如果超过，则 ssthresh 减半，重新计算窗口各段容量，并返回 true（表示需要修剪）。
// 否则返回 false。
func (hrw *HeightRangeWindow) CheckAndTriggerCongestionControl(currentUsedSize int) bool {
	if currentUsedSize > hrw.maxTotalAllowedSize {
		newSsthresh := hrw.logicalCapacities[hrw.GetBitPosition(hrw.windowEndNumber)] / 2
		if newSsthresh < 8 {
			newSsthresh = 8 // sshresh 至少为1
		}
		hrw.currentSsthresh = newSsthresh
		hrw.recalculateLogicalCapacities()
		return true
	}
	return false
}

// PruneWindow 修剪掉当前窗口最前面的 numberOfLogicalBitsToPrune 个逻辑位。
// 它会更新 windowStartNumber (起始绝对编号) 和 firstSegmentIndex (循环使用的起始段索引)。
func (hrw *HeightRangeWindow) PruneWindow(numberOfLogicalBitsToPrune int) {
	if numberOfLogicalBitsToPrune <= 0 || numberOfLogicalBitsToPrune > 32 {
		return // 无效参数或无需修剪
	}

	prunedTotalCapacity := uint64(0)
	lastCapacity := hrw.logicalCapacities[(hrw.firstSegmentIndex+31)%32]
	for i := 0; i < numberOfLogicalBitsToPrune; i++ {
		capacityArrayIndex := (hrw.firstSegmentIndex + i) % 32
		prunedTotalCapacity += hrw.logicalCapacities[capacityArrayIndex]
		//更新这几个的容量
		if lastCapacity <= hrw.currentSsthresh {
			hrw.logicalCapacities[capacityArrayIndex] = lastCapacity * 2
		} else {
			hrw.logicalCapacities[capacityArrayIndex] = lastCapacity + 1
		}
		lastCapacity = hrw.logicalCapacities[capacityArrayIndex]
	}

	hrw.windowStartNumber += prunedTotalCapacity
	hrw.firstSegmentIndex = (hrw.firstSegmentIndex + numberOfLogicalBitsToPrune) % 32
}

func (hrw *HeightRangeWindow) getWindowPosition() int {
	start := hrw.firstSegmentIndex
	end := hrw.GetBitPosition(hrw.windowEndNumber)
	if end >= start {
		return 32 - end + start - 1
	} else {
		return start - end - 1
	}
}

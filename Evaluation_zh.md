# 具体实验
目前所有实验均打包放置到实验数据集封存中。包含数据集、脚本、图片。

## Background 2.5-1 MPT_test.go 、verkle_test.go
目前已为这三个实验分别生成了可执行文件，具体使用方法如下
- MPT - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestMethod2`
- Verkle - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestVerkleMethod2`

运行完成后，直接在可执行文件目录下result目录，生成对应的文件。
放置到data目录后，经过p3-t2_1.py文件，即可生成图片。

## Background 2.5-2 processor_eth_compare_test.go
当前实验需要运行5次执行
- MPT - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2`
- Verkle - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useVerkle`
- MPT-Pre -`.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useCache`
- Verkle-Memory -  `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useVerkle -useMemory`
- Verkle-Memory-poly - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useVerkle -useMemory -mockMode`

运行结果放置到data目录，经过p3-t0.py文件，即可生成图片。

## Evaluation 5.2、5.4 - processor_eth_compare_test.go
### 使用介绍
首先需要把以太坊数据所在位置，输入到dataDir中，然后选择范围。以实例文件为例，范围为1-2. 我们已经测试代码打包成Windows可执行文件，可以通过该可执行文件运行命令。
可执行文件位于bin目录，名字为**processor_eth_compare.test.exe**
各个测试的命令如下：
- MPT - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2`
- Verkle - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useVerkle`
- MPT+SWMT - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useCacheTrie`
- Verkle+SWMT - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestCompareProcessTransactions -dbDir "F:\db" -statsDir "F:\stat" -dataDir "E:\ethdata" -startFileIdx 1 -endFileIdx 2 -useCacheTrie -useVerkle`

### 方法：
TestCompareProcessTransactions

### 通用参数：
- startFileIdx := 1 transaction_x.csv, block_x.csv 以太坊数据输出文件起始位置x
- endFileIdx := 8  transaction_y.csv, block_y.csv 以太坊数据输出文件结束位置y
- UseMemory := false 是否使用内存数据库，会大大减少IO时间

### 其他通用参数：
- dbDir := "F:\\ethdata\\geth_compare_db_verkle" 文件存储位置
- statsDir := "F:\\ethdata\\compare_stats10_verkle" 统计信息存储位置
- dataDir := "E:\\ethdata" 以太坊数据存储位置

### 特有参数
- mpt - common.UseVerkle = false;  common.UseCacheTrie = false
- verkle - common.UseVerkle = true;  common.UseCacheTrie = false
- mpt + SWMT - common.UseVerkle = false;  common.UseCacheTrie = true
- verkle + SWMT - common.UseVerkle = true;  common.UseCacheTrie = true

### 输出文件
- MPT         - trie_stats.csv
- Verkle      - verkletrie_stats.csv
- MPT+SWMT    - cachetrie_stats.csv
- Verkle+SWMT - cachetrie_stats.csv

### 数据处理脚本
**实验一，对应5.2** p3-t1.py,需要在程序所在目录新建data目录，并将四种模式下的输出文件放置进去。会生成四张图片，分别为:
- MPT - p3_t1_mpt_verification_time.png
- Verkle - p3_t1_verkle_swmt_verification_time.png
- MPT+SWMT - p3_t1_cachetrie_verification_time.png
- Verkle+SWMT - p3_t1_verkle_verification_time.png

**实验二， 对应5.4** p3-t3.py. 需要在程序所在目录新建data目录，并将MPT+SWMT模式下的输出文件放置进去。会生成七张图片。

## Evaluation 5.3 - MPT_test.go 、verkle_test.go、 cache_trie_test.go
目前已为这三个实验分别生成了可执行文件，具体使用方法如下
- MPT - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestMethod2`
- Verkle - `.\bin\processor_eth_compare.test.exe --% -test.v -test.run TestVerkleMethod2`
- SWMT - `.\bin\SWMT_test.exe --% -test.v -test.run TestSampleCacheTriePerformance`

运行完成后，直接在可执行文件目录下result目录，生成对应的文件。
放置到data目录后，经过p3-t2.py文件，即可生成图片。

## Evaluation 5.5 - cache_trie_performance_test.go
使用命令`.\bin\SWMT_test.exe --% -test.v -test.run TestCachePerformance`即可生成对应的数据。

运行完成后，放置到data目录后，经过p3-t4.py文件，即可生成图片。
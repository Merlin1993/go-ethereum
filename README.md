# Experiment Setup and Implementation Details

## 🧱 Reuse of Geth Implementation

Our system reuses the core infrastructure of [Geth (Go Ethereum)](https://geth.ethereum.org/).

---

## 📦 Mainnet Dataset Source

We retrieved Ethereum mainnet transaction and block data by running a full node and using the [`ethereum-etl`](https://github.com/blockchain-etl/ethereum-etl) tool.

We used the following command to export blocks and transactions:

```bash
ethereumetl export_blocks_and_transactions \
  --start-block 0 \
  --end-block 1000000 \
  --blocks-output E:\ethdata\blocks_1.csv \
  --transactions-output E:\ethdata\transactions_1.csv \
  --provider-uri http://[ip]:[port]
```

The downloaded data was converted into sequentially ordered CSV files (`blocks_1.csv`, `blocks_2.csv`, ..., and `transactions_1.csv`, `transactions_2.csv`, ...). These files were then processed by our system in order, applying each block and transaction sequentially.

---

## 🧪 Experiment Test Cases

We conducted experiments in two major categories:

### 1. **Mainnet Replay Test**

- Location: `core/tree_test/processor_eth_compare_test.go`
- Description: Executes and compares system behavior with and without SWMT on real Ethereum blocks.

### 2. **Stress Tests**

We evaluated three state tree implementations under stress:

- **MPT Stress Test**: `core/tree_test/mpt_test.go`
- **Verkle Tree Stress Test**: `core/tree_test/verkle_sliding_test.go`
- **SWMT Stress Test**: `cachetrie/cache_trie_test.go`

### 3. **Simulated Load Experiments**

- Location: `cachetrie/cache_trie_performance_test.go`
- Description: Assesses performance under various synthetic load patterns.

---

## 🛠 Code Modifications

### 📁 SWMT Implementation Location

The **SWMT (Sliding Window Merkle Tree)** is implemented in the `cachetrie/` directory.

| File | Description |
|------|-------------|
| `cache_trie.go` | Implements the sliding window protocol. |
| `crowd_window.go` | Implements the congestion avoidance mechanism. |

### 🧩 Integration with Ethereum State System

- `core/state/reader.go`:  
  Implements `CacheTrieReader`, which checks SWMT cache before accessing snapshots.

- `trie/cache_proxy_trie.go`:  
  Implements write proxy logic. If cache mode is enabled, writes are directed to SWMT.

- `core/state/statedb.go`:  
  Handles SWMT mode. When cache mode is on, only SWMT commitments are generated. Pruned state data is selectively cached.

- `processor_eth_compare_test.go`:  
  Implements replay logic for Ethereum blocks and delayed writes, with conditional execution based on whether SWMT is enabled.

---

## ▶️ Execution Guide

### 🔄 Running with Mainnet Data

1. Use the `ethereum-etl` command to download dataset from a full Ethereum node.
2. Modify the execution range in `processor_eth_compare_test.go`.
3. Run the following test case:

```go
TestCompareProcessTransactions
```

### ⚙️ Configuration

The following flags are hardcoded in `common/tree_config.go`:

- `cacheTrie`: Enable/disable SWMT mode.
- `verkleTree`: Enable/disable Verkle tree mode.

# 具体实验

Background 2.5-1 MPT_test.go 、verkle_test.go
Background 2.5-2 processor_eth_compare_test.go

## Evaluation 5.2、5.4 - processor_eth_compare_test.go
### 使用介绍
首先需要把以太坊数据放置到特定位置：

### 方法：
TestCompareProcessTransactions

### 通用参数：
- startFileIdx := 1 
- endFileIdx := 8 
- UseMemory := false

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
p3-t1.py, 会生成四张图片，分别为
- MPT - p3_t1_mpt_verification_time.png
- Verkle - p3_t1_verkle_swmt_verification_time.png
- MPT+SWMT - p3_t1_cachetrie_verification_time.png
- Verkle+SWMT - p3_t1_verkle_verification_time.png

## Evaluation 5.3 - MPT_test.go 、verkle_test.go、 cache_trie_test.go
MPT

## Evaluation 5.5 - cache_trie_performance_test.go

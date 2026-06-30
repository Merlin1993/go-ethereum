# ASC Archive Plus Summary

本文档说明 ASC archive plus 的当前代码模型和主流程。逻辑细节以 `ARCHIVE_LOGIC_PLUS.md` 为准。

## 1. 核心模型

ASC plus 分成两层：

* Flat KV / snapshot：保存真实 value，是执行读取的真相库。
* ASC tree：保存 root、冷热结构和 proof metadata。

冷桶只保存证明材料，不保存真实 value。

```text
valueRef = H("BVR1" || len(K) || K || len(V) || V)
```

热叶子和冷桶都只引用 `valueRef`。真实 value 始终从 Flat KV / snapshot 读取。

## 2. 核心不变量

* 普通执行读取不依赖 archive payload。
* prune/archive 不读取原真实 value。
* write-promotion 不读取原真实 value。
* cold bucket entry 只保存 key suffix 和 `valueRef`。
* bucket item 数不能超过 `ArchiveBucketSize`。
* `StubList` 只是小桶短期合并缓冲。
* path storage 只改变物理落盘 key，不改变 ASC commitment。

## 3. 主要状态

```text
LeafNode
  key path
  valueRef

ArchiveBucketNode
  bucket path
  ECMH commitment
  Cuckoo filter
  []ArchivedKey{suffix, suffixBits, valueRef}

Flat KV
  full key -> real value
```

构桶时使用的 `ArchivedKV.Value` 实际也是 `valueRef`。

## 4. 读取流程

```text
1. 根据 key 进入对应 shard。
2. 在 ASC tree 中查找热 leaf 或候选 cold bucket。
3. 如果命中热 leaf：
   从 Flat KV 读取真实 value。
4. 如果命中 cold bucket：
   a. 校验 bucket path 前缀
   b. 用 cuckoo filter 快速排除
   c. 用 entry list 精确匹配 suffix
   d. 从 Flat KV 读取真实 value
   e. 校验 H(K,V) == entry.valueRef
5. 未命中则返回不存在。
```

读取路径的核心分工：

```text
ASC tree 决定结构和证明路径。
Flat KV 决定真实 value。
```

## 5. 写入 / Promotion 流程

```text
1. 写入新 value 到 pending Flat KV。
2. 计算 valueRef_new = H(K,V_new)。
3. 如果 key 可能已有冷状态，探测 cold bucket。
4. 如果命中 cold bucket：
   a. 读取 bucket entry 中的 valueRef_old
   b. ECMH -= H(K,valueRef_old)
   c. 删除 entry list 中的原 entry
   d. 更新 cuckoo filter / bucket metadata
5. 插入新的热 leaf：
   key path + valueRef_new
6. commit 时 ASC metadata 和 Flat KV 同 batch 落盘。
```

关键点：promotion 只需要原 `valueRef`，不需要原真实 value。

## 6. 剪枝 / Archive 流程

```text
1. 选择一个 shard。
2. 递归扫描 shard root。
3. stale hot leaf 转成 archive item：
   suffix = absolute key path
   value  = old valueRef
4. archive item 向上聚合。
5. 到边界后统一构造 archive bucket / subtree。
6. 小 bucket 可短期进入 StubList 继续聚合。
7. 成熟 bucket 或路径压力过高时，下沉到普通 child edge。
```

关键点：剪枝只移动 `valueRef`，不移动真实 value。

## 7. 构桶流程

```text
1. 如果 item 数不超过 bucket limit，直接生成 bucket。
2. 如果超过 limit，寻找公共前缀并压缩路径。
3. 按下一 bit 分成左右子树。
4. 递归构造，直到每个 bucket 都满足 hard cap。
5. bucket 内写入：
   a. local suffix
   b. valueRef
   c. cuckoo filter
   d. ECMH commitment
```

与热 child 混合重建时，可以强制展开到指定深度，保证冷热节点仍在同一 ASC tree 内。

## 8. 提交流程

```text
1. 递归序列化 dirty ASC nodes。
2. 计算节点 hash，更新 parent child hash。
3. 写 ASC node 到 batch。
4. 写 stale deletes。
5. 写 pending Flat KV。
6. destructive commit 可卸载 live shard root，只保留 root hash/path。
```

hash 仍是 commitment 标识。path mode 的物理 key 是：

```text
BPN1 || shardID || pathBits || pathBytes
```

## 9. 非主路径边界

`ArchiveDB` / `FlushArchives` / `BucketHash || 0x01` payload 不属于 plus 主正确性路径。

plus 主路径依赖：

```text
ASC node metadata + Flat KV value
```

而不是：

```text
独立 archive payload + archive value store
```

## 10. 需要重点观察的问题

* 固定 prefix shard 可能导致 storage key 分布不均。
* 小桶聚合和路径长度是天然张力。
* path pressure 能限制路径 bucket 数，但可能降低 bucket 利用率。
* 诊断数据只能用于观察，不能反馈到正常语义。

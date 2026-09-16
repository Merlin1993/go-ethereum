# AMT 压测 · 开发计划（2026-09-07）

读者：只做代码的人。本文件不含实验排期与命令，实验看 [amt_experiment_plan_20260907.md](/D:/go_workspace/go-ethereum/.agent/amt_experiment_plan_20260907.md)。

范围：本次只做 **AMT（`Config.StemMode=false`，非 stem 逐 key 归档）**。ASCT（stem 分组）出局，其 Go 代码与测试全部保留，不许破坏。

## 0. 交付定义

A1-A4 完成、`go test ./trie/archive` 全绿（含 stem 路径）、A5 备份提交已打 tag，即开发交付。A6（16 叉后备）只交付设计页，不写实现。

## A1 压测驱动支持非 stem

- `trie/archive/trace_stress_test.go` 在 699 / 851 / 1300 / 1550 处写死 `config.StemMode = true`，改为读新 flag `-traceStressStemMode`（默认 `false`）。
- 操作分发：stem 模式走 `StemTrie.Get/Put/Delete`；AMT 模式走 `backend.Get/Put/Delete`（`trie.go:330/473/594`，非 stem 可用）。
- 新增 `classifyTraceStressAccessAMT(backend, key)`：用 `backend.GetValueRef(key)` 的 `fromArchive` 做热/冷判定（`trie.go:345` -> `shard.go:616`）。Cuckoo 预判与 FP 计数在 `shard.go:790-843`，与 stem 无关，直接复用。
- CSV **列名与产物文件名一律不改**：AMT 用逐 key 分类结果填现有的 `StemGet_Hot_Hits / StemGet_Archive_Hits / StemGet_Missing / StemGet_Errors`，`StemCache_*` 六列填 0；AMT 的输出 CSV 仍叫 `results/asct_trace_stress.csv`，靠 `metadata.json` 的 `stem_mode=false` 区分。改名会打断 `analyze_trie_compare.py` 与历史结果的对齐，不值。
- `StemCacheLimit/StemCacheMB` 两个 flag 在 AMT 模式忽略，metadata 标 `not_applicable`。
- 验收：同一 key 序列下 `backend.Get` 与 `backend.ForEach` 一致；stem 相关测试零修改通过。

## A2 AMT 读激活

- 新增 `Config.ActivateArchivedKeyOnRead bool` + flag `-traceStressActivateArchivedKeyOnRead`（stem 侧已有的 `ActivateArchivedStemOnRead` 保持原样，不复用语义）。
- 语义：读命中归档层 -> 该 key 作为一次新插入回到热层，之后按正常轮询再走一遍归档。实现复用 `Shard.activateValueRef`（`shard.go:1011`：先 `removeArchivedVersionForWrite` 再常规 insert），挂在 `Shard.Get` 返回 `fromArchive==true` 之后（`shard.go:580-612`）。不设驱逐、不设命中次数阈值。
- 计时归属（已核到行，比预想省事）：`trace_stress_test.go:1116-1120` 只把 `ArchivePromotionNanos`（**写侧**晋升）从 `opDur` 减掉；读侧另有 `ArchiveReadPromotion{Calls,Hits,Nanos}`（`diagnostics.go:271-273`、`recordArchiveReadPromotion:729`，stem 挂钩在 `stem.go:559-566`），驱动不减它，`:1140` 的 `comparativeBatchWall = opDur+commit+write` 因此天然包含读激活耗时；CSV 的 `Archive_Read_Promotion_{Calls,Hits,ms}` 三列（`:981` 已在写）也已存在。**做法：AMT 激活路径直接调 `recordArchiveReadPromotion`，不新增计数器、不新增列。** 唯一红线：别走 `recordArchivePromotion`（写侧，会被减出性能分子）。
- prune 耗时走 `window.prune`（`:1135`），从不在 `comparative`（`:945`）内 —— 这就是"性能不含 prune"的代码级定义，不要改动它。
- 验收：新增非 stem 单测 `TestArchiveTrieReadActivation`：归档 -> 读命中 -> 断言回到热层、值正确、root 与"读后立即 Put 同值"一致，并断言 `ArchiveReadPromotionCalls/Hits` 各 +1 而 `Archive_Promotion_Checks/Hits` 不动。

## A3 精确轮次标注 + 无归档开关

- 驱动维护 `prunesDone`；`pruneShardIdx = (idx+1) % (1<<ShardDepth)`（`trie.go:1101`）是严格 round-robin，可精确反推轮次，新增两列：
  - `Archive_Round_Completed = prunesDone / 2^D`（轮 0 = 尚无任何一次归档；第一次跑满 2^D 次 = 轮 1）
  - `Archive_Round_Progress = prunesDone % 2^D`
- 新增 `-traceStressDisableArchive`：true 时一次 `PruneNextShard` 都不调用（实验阶段 B1 的结构探针用），显式表达，不靠"不传 prune 参数"隐式实现。
- 验收：D=8 跑 5000 批（4999 次 prune）时 `Archive_Round_Completed` 必须恰为 19。

## A4 参数全量导出

`metadata.json`（键名见 `trace_stress_test.go:810-834`）现在打 ~19 项 + 完整 command，缺：`stem_mode`、`cuckoo_buckets`、`cuckoo_slots`、`archive_bucket_size`（`ResolveArchiveBucketSize()` 的实际返回值）、leveldb `512/256` 缓存、`archive_period_ops = 2^D x batch`、`prune_every_batches`、`start_file/start_block` 生效值、`activate_archived_key_on_read`。补齐后要求：**metadata.json 能唯一确定一次运行，不需要参考脚本。**

- Cuckoo 形状 flag 化：驱动现在写死 `CuckooBuckets=16`（`:858/1302/1551`）-> `ResolveArchiveBucketSize()=60`，而论文 `4.Design.tex` 与 `DefaultConfig`（`config.go:57/68-69`）都是 m=32、b=4、桶容量 100。改成 `-traceStressCuckooBuckets/-traceStressCuckooSlots`，正式跑用 **32x4（容量 100）与论文一致**，并在 metadata 记实际值。
- 验收：任取一次正式跑，把 metadata 与命令行逐项核对，0 缺口。

## A5 备份提交（先于一切改动）

1. 建 `codex/backup-asct-stem-20260907`，把当前工作树（含 18 个已改文件与未跟踪 `.agent/`）整体提交，打 tag `pre-amt-stress-20260907`，保证可找回。
2. Go 侧 ASCT/stem 代码与测试**不删**。
3. `.agent/` 一次性脚本的删除推到实验结束后；本轮只做改名保留（`check_key_randomness_20260907.py` -> `trace_key_shard_uniformity.py`）。

## A6 最坏预期：16 叉后备（本轮只交付设计，不写代码）

背景事实：`trie/archive` 热层是硬编码二叉 —— 非测试代码里 `.Left/.Right/LeftHash/RightHash/*Epoch` 共 355 处、6 个文件（`shard.go` 150、`archive.go` 147、`node.go` 28），冷层建桶按位劈分（`archive_build.go:140`）、`rootBranch`、序列化格式全部 bit-based。把二叉改成 16 叉等于重写第二套热树，不是加一个 `Fanout` 配置。论文 `4.Design.tex:307` 还用"二叉比 16 叉的兄弟哈希更少、证明更小"作选型理由，换叉数要连理由一起改。

因此后备的正确形态不是"把 AMT 改成 16 叉"，而是：**热层直接用 go-ethereum 现成 `trie.Trie`（16 叉 hex trie，性能天然接近 MPT），冷层保留现有 bucket + Cuckoo + flat value + 分片轮转归档 + 读激活**。归档单位取"整分片封存"，连 per-key epoch 位都不需要，改动集中在封存/复活两条路径。代价：与以太坊原生单一 MPT 根不再逐字节相同（分片根之上需要一个聚合层），论文主张要从"新树"改口径为"可挂到任意状态树上的归档层"。

触发门限（预先写死，不许事后移动；判定数据来自实验文档 §4.1 的 B1 探针）：

| r | 结论 | 动作 |
|---:|---|---|
| >= 0.90 | 树形不是瓶颈 | 继续 AMT，A6 永久封存 |
| 0.70-0.90 | 结构有代价但可救 | 继续 AMT，A6 保持设计态，不写码 |
| < 0.70 | 二叉热层本身太慢 | 启动 A6 实现；AMT 二叉降为附录 |

归因规则：判 A6 之前先看 `Operations_ms / comparative` 占比。占比 >= 70% 才是树遍历瓶颈；否则先修 commit / DB 写路径，换 16 叉救不了非遍历瓶颈。

### A6 执行记录与调优计划（2026-09-16 补）

**执行状态**：A6 已实现并接线（`trie/archive/mpt` + `-traceStressHotLayer=mpt`，经 `trace_stress_hotlayer_test.go` 外注钩子避开 import 环），校准跑 B3m 至 200M ops 停。结果：**r≈0.06–0.08，比二叉更差，触发门限表的归因规则先响了**——B3m 稳定段 `Operations_ms` 只占 13.3%（2,836 ns/op，比二叉的 22,915 ns/op 便宜约 8 倍，证明 16 叉查路确实有效），而 `Commit_ms` 占 86.7%（18,488 ns/op）。按上面写死的归因规则：**这不是树遍历瓶颈，先修 commit/DB 写路径，不许因 B3m 数字给 16 叉结构判死刑**。

**根因（代码级，两条都在集成层，不在 hex trie 本身）**：

1. 聚合根每批全量重建：`mpt.go` `commitToBatchLocked` 每次提交 `buildAggregate(roots)` 从空树逐个 `Update` 全部 shard 根（D15 ⇒ 32,768 项）再 Commit 一棵新 geth trie——与批次实际脏分片数无关，每批 O(全部分片×树深) 哈希。
2. 提交不合流：`trace_stress_hotlayer_test.go` `CommitToBatch` 忽略驱动传入的 batch，调 `mpt.Trie.Commit()` 自开 LevelDB batch 自刷盘；驱动共享 batch 形同虚设（所以 B3m `DB_Write_ms=0`，同步写成本全藏进 `Commit_ms`）。

**调优内容（按序做，每步后本地测，全绿再发 B2r 同段复测 r）**：

- T1 提交合流：`mptHotLayer.CommitToBatch(batch, destructive)` 改为把节点/flat/archive 记录写进传入 batch，`Trie.Commit()` 仅保留给独立使用；预期直接砍掉每批独立刷盘。
  **完成 2026-09-16（本地）**：`mpt.Trie` 新增导出 `CommitToBatch(batch, destructive)`（内部复用 `commitToBatchLocked`，`Commit()` 保留为自管批次路径）；适配器 `trace_stress_hotlayer_test.go` 改走传入 batch。新增单测 `TestMPTCommitToBatchStagesCallerBatch`（暂存内容只进 batch、刷盘前库里不可见、刷盘后可重载）与 `TestMPTCommitToBatchMatchesCommit`（共享批与自管批 root 逐位一致，含 prune+delete 轮）。`go vet` + `go test ./trie/archive/mpt` 5 测全 PASS，`go test -c ./trie/archive` 编译门 OK。注意语义：合流后每批同步写从 2 次（mpt 自刷 + 驱动 batch.Write）变 1 次；B3m 复测时 `Commit_ms` 应显著下降、`DB_Write_ms` 应转非零，两列之和才是新旧可比口径。未发车。
- T2 聚合根增量化：聚合 trie 常驻（按 root 打开），每批只 `Update` 本批 dirty shard 的根；shard 清空才 `Delete` 对应项。若 geth trie 增量代价仍高，退一步按设计文档"aggregation record 平铺+批量写"方案，root 哈希用轻聚合（拼接哈希）替代全树。
  **完成 2026-09-16（本地）**：`Trie` 增常驻 `aggregate` 字段 + `ensureAggregateLocked()`（按当前 root 懒打开，节点走 nodeDatabase 缓存，重开零重哈希）；`commitToBatchLocked` 删除 `buildAggregate` 全量重建，改为仅对本批 dirty shard `Update/Delete(shardKey(id))`。关键坑：geth trie `Commit()` 后对象一次性作废（"trie is already committed"），故每轮提交后置空 handle、下轮从新 root 重开。`Hash()`（全量重建口径）保留为交叉校验：新增 `TestMPTIncrementalAggregateMatchesRebuild` 断言每轮 Root==Hash（覆盖 写6轮+prune4轮+重载1轮）。mpt 包 6 测全 PASS，vet/编译门/gofmt 绿。`buildAggregate` 现仅 `Hash()` 使用。
- T3 修完后复跑 B3m 同配置 200M ops 校准：看 `Commit_ms` 占比是否 <20%、Operations 是否重新成为大头；只有 Operations 占比 ≥70% 时才有资格谈下一步换树。
  **发车 2026-09-16（B3m2）**：`.agent/run_remote_b3m2_t1t2_20260916.py`（b3m launcher 克隆），StartFile=9、ops=196,608,000 与 B0b/B2r 严格配对（原 B3m 是 StartFile=0，配对基线换重载段）。判定口径提醒：T1 后比较用 `Commit_ms+DB_Write_ms` 之和。
- T4 （独立于 T1-T3，二叉/16 叉通用）每操作包装层拆账：热路径 Get/Put 里 Cuckoo 探测、shard 定位、valueRef 构造、冷热判断记账逐项计时（bench 或采样），目标把 B0b 段 25 µs/op 的差距定位到具体项。
- T5 验证口径落地（用户 9/16 裁定）：验证归交易池收集/打包前，不计入状态树生成计时；冷回插只做便宜自查（长度/短指纹），昂贵全量 valueRef 重算改后台抽样或开关关闭。注意重载段热命中 99%，此项单独做完预计只把 r 从 ≈0.25 抬到 ≈0.3，不是主刀。
- T6 复测通过（重载段 r ≥0.7）才谈 B3 全量与 B4/B5；再次不过则按实验手册 §0 走"归档层出账"的口径重定义，不再自动烧大车。

数据：`.agent/cmp_20260916/b3m_hotmpt.csv`（8 窗）、`b2r.csv`、`b0b_mpt_trace_stress.csv`；分析页 `.agent/amt_perf_handoff_20260916.md`。

## 明确不做

分段续跑 / checkpoint（预算按"跑 4 天就说明不达标"处理）；stem A/B 对比；合成 Cuckoo FP 标定；状态根一致性校验；Verkle 或 MPT 侧任何代码改动（对比驱动 `core/tree_test/trace_compare_test.go` 现成可用，`-traceCompareOps` 是 int64 支持 20B，另有 `-traceCompareStartFile` / `-traceCompareStartBlock`）。

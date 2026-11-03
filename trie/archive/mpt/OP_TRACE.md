# V11 状态操作计时诊断

此诊断基于 V11 桶聚合分支提交 `09227613c9d51070c59d34e3e7acd2560ae43444`，保持区块回放、每块裁剪、区块结束验证和原有吞吐公式。该提交已撤销 V10 的前缀分裂桶策略，恢复 V11 的同挂点补桶策略。开启 `MPT_OP_TRACE=1` 后，额外生成 `results/op_trace.csv`，每行对应主 CSV 的一个统计窗口。所有时间列单位均为纳秒。

## 运行

在本分支编译测试程序：

```bash
go test -c -o archive_optrace.test ./trie/archive
```

使用 V11 `AMT实验结果/v11_verkle_block_audit_20261030/metadata.json` 的 `command` 作为参数模板，替换可执行文件路径和**新的** `-traceStressBaseDir`，按实际机器替换输入目录；在命令前设置 `MPT_OP_TRACE=1`。保留 `-traceStressAccessSampleEvery=1000`、`-traceStressBatchByBlock=true`、`-traceStressPruneEveryBlocks=1`、`-traceStressMPTVerifyAtCommit=true` 等正式参数。V11 不使用 `-traceStressFullBucketsBeforeSplit`。短程诊断只修改结束区块，例如 `-traceStressEndBlock=65806` 跑第一个窗口。不要在已有结果目录重跑。

诊断要求 `-traceStressHotLayer=mpt` 和 `-traceStressProbeSampleOnlyTiming=false`。`metadata.json` 中的 `op_trace_enabled` 用于确认计时已开启。

## 指标与核对

- `Operations_ns`：主 CSV `Operations_ms` 的未舍入值，范围仍排除解析、裁剪、提交、桶验证和访问分类抽样。
- `Get_Call_ns`、`Put_Call_ns`、`Delete_Call_ns`：实际业务调用总耗时。`Call_ns` 是三者之和。
- `Driver_Residual_ns = Operations_ns - Call_ns`：操作循环中的计时、证明归属统计、计数等剩余时间；也包含计时器与循环边界开销。
- `Hot_ns`：初始热树 `Get` 与业务 `Put/Delete` 的树调用。复活时的热树回插另记在 `Promotion_Hot_ns`。
- `Probe_ns`：热树未命中后，查找归档桶的总时间。`Filter_ns`（布谷鸟过滤器 Lookup）与 `Load_ns`（桶 KV Get、解码）均嵌套在其中；`Probe_Other_ns = Probe_ns - Filter_ns - Load_ns`，包括 stub 路径遍历、元数据处理和候选确认。
- `Bucket_IO_ns = Bucket_Meta_IO_ns + Bucket_Payload_IO_ns`：主 CSV 的桶 KV Get 时间。桶载荷读取嵌套在 `Load_ns` 中；stub 元数据读取发生在归档探查中，未必嵌套在 `Load_ns` 中。`Load_ns - Bucket_Payload_IO_ns` 可近似看作载荷解码与装载等工作，但两个计时点有额外开销。
- `Promotion_ns`：已命中的归档键从桶移除并回插热树的总时间。`Remove_ns` 和 `Promotion_Hot_ns` 嵌套在其中；`Promotion_Other_ns = Promotion_ns - Remove_ns - Promotion_Hot_ns` 包括审计快照与其他维护。
- `Lock_ns`：获取 AMT 互斥锁的等待时间。`Call_Other_ns = Call_ns - Lock_ns - Hot_ns - Probe_ns - Promotion_ns`，包括树句柄确保、结果复制、其他包装逻辑与计时开销。

逐窗口核对 `Ops = Window_Operations`，以及上述四个等式。嵌套列不要重复相加。抽样分类读取发生时探针暂停，裁剪和提交期间也暂停；桶验证仍在原来的提交阶段。

计时探针会增加额外时钟读取和原子计数，**开启后的吞吐不能直接与未开启的正式 V11 或 MPT 吞吐相比**。诊断用于找出 V11 操作内部的主要耗时；要把 V11 与 MPT 的正式实验差额严格归因，还需在严格区块 MPT 源码上做相同范围的计时，并在同机相同轨迹下复测。

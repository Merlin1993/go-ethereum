// Package archive 实现 ASCT plus 使用的归档 trie。
//
// 整体模型可以按四层理解：
//
//  1. Trie 负责按 key 前缀切分 shard；shard 是同一棵二叉树在固定深度的子树。
//  2. Shard 是真正可变的二叉压缩 trie，保存热 leaf、冷 bucket 和侧挂 StubList。
//  3. ArchiveBucketNode 是冷数据桶，只保存 key suffix、valueRef、Cuckoo filter 和 ECMH commitment。
//  4. Flat value store 保存真实 value；热 leaf 和冷 bucket 都只引用 valueRef。
//
// 读路径先在 shard 的热树里找 leaf，没命中再查当前节点侧挂的 StubList 或归档桶。
// 冷桶命中后必须经过 suffix 精确匹配和 valueRef 校验，真实 value 始终从 flat value store
// 或外部 snapshot reader 读取。
//
// 写路径先暂存 flat value 并计算 key-bound valueRef。如果 key 之前已经在冷桶中，
// 会先删除旧的归档条目并更新桶 commitment/filter，再插入新的热 leaf。
//
// 剪枝路径按 shard 轮转。过期 leaf 被转换成 ArchivedKV，随后按公共前缀聚合成
// ArchiveBucketNode 或归档子树。小桶优先进入 StubList 做短期聚合，桶足够成熟或同一路径
// 桶数压力过高时，再下沉到普通 child edge，避免路径持续变长。
//
// 提交流程先等待异步剪枝完成，再提交 dirty shard，最后沿变更路径向上计算全局 root。
// Commit 会同时落盘 ASCT metadata 和 flat value。
package archive

# ASCT 远端拉取与关键分支说明

> 状态快照：2026-08-04  
> 仓库：`https://github.com/Merlin1993/go-ethereum.git`

## 1. 先看结论

当前要进行 ASCT Stem 重放，应使用：

```text
v1.5.5-tree-test-acst-plus
```

当前精确提交为：

```text
318e4791e8c249696106c8ddc13e75a3075c7f6f
```

这个提交已包含 Stem 存储拆分、批量更新、性能诊断，以及
`concurrent map writes` 并发崩溃修复。

**注意：截至本文档编写时，该分支只在开发机本地，还没有推送到
`origin`。** 在推送前，远端机器无法拉取这个分支和上述提交。

## 2. 关键分支

| 分支 | 当前作用 | 远端状态 | 是否用于新重放 |
|---|---|---|---|
| `v1.15.5-tree-test` | MPT、Verkle 及早期树实验的公共基础 | 远端已有，`155554935` | 只用于基线/对照 |
| `v1.5.5-tree-test-acst` | 旧 ASCT 开发线 | 远端停在 `d67a106c4`；本地停在 `2e667ec60` | 不用于新 Stem 重放 |
| `v1.5.5-tree-test-acst-plus` | 当前 ASCT Stem 开发线 | **本地已有，远端尚无** | **应使用** |
| `master` | 通用主开发线 | 远端已有 | 不用于本轮实验 |
| `release/*` | 历史发布分支 | 远端已有 | 不用于本轮实验 |

分支名需要严格区分：

- 基础分支是 `v1.15.5-tree-test`。
- ASCT 分支历史上命名为 `v1.5.5-tree-test-acst`，少了一个 `1`。
- 当前 Stem 分支是 `v1.5.5-tree-test-acst-plus`。

当前 `acst-plus` 比本地 `acst` 多 10 个提交，比远端 `acst` 多 56 个提交。
因此，远端机器不能用 `origin/v1.5.5-tree-test-acst` 代替 `acst-plus`。

## 3. 开发机：首次推送当前 Stem 分支

先确认当前代码与提交：

```powershell
git switch v1.5.5-tree-test-acst-plus
git status --short
git log -3 --oneline
git rev-parse HEAD
```

预期 `HEAD` 为：

```text
318e4791e8c249696106c8ddc13e75a3075c7f6f
```

然后创建远端同名分支：

```powershell
git push -u origin v1.5.5-tree-test-acst-plus
```

推送后验证：

```powershell
git ls-remote --heads origin v1.5.5-tree-test-acst-plus
```

返回值应包含：

```text
318e4791e8c249696106c8ddc13e75a3075c7f6f
```

`git push` 只会推送已提交的内容。未提交的报告、脚本和实验输出不会被带到
远端。

## 4. 远端机器：全新克隆

只有在上一节的分支已推送后，才执行：

```powershell
git clone https://github.com/Merlin1993/go-ethereum.git
Set-Location go-ethereum
git fetch origin --prune
git switch --track origin/v1.5.5-tree-test-acst-plus
git rev-parse HEAD
git status --short
```

Linux 命令相同，只需将 `Set-Location go-ethereum` 改为：

```bash
cd go-ethereum
```

如果 `git switch --track` 报告远端分支不存在，说明开发机还没有推送
`acst-plus`。此时应停止，不要改拉旧 `acst` 分支。

## 5. 远端机器：已有仓库时更新

先检查工作区：

```powershell
git status --short
```

如果有本地修改，先提交或者明确保存，不要直接覆盖。工作区干净后：

```powershell
git fetch origin --prune
git switch v1.5.5-tree-test-acst-plus
git pull --ff-only origin v1.5.5-tree-test-acst-plus
git rev-parse HEAD
```

如果本地还没有这个分支：

```powershell
git fetch origin --prune
git switch --track origin/v1.5.5-tree-test-acst-plus
```

使用 `--ff-only` 是为了防止拉取时自动生成实验机专属的 merge commit。
如果快进失败，应先查清本地额外提交，不要直接使用 `reset --hard`。

## 6. 按精确提交运行实验

长时间重放建议固定提交，避免远端分支后续更新导致实验不可比。

```powershell
git fetch origin v1.5.5-tree-test-acst-plus
git switch --detach 318e4791e8c249696106c8ddc13e75a3075c7f6f
git status --short
git rev-parse HEAD
```

如果需要在远端机器上保留本地分支，可以改用：

```powershell
git switch -c replay/asct-stem-318e4791e 318e4791e8c249696106c8ddc13e75a3075c7f6f
```

每次实验的 `metadata.json` 应记录完整的 `git rev-parse HEAD` 输出，不要只记分支名。

## 7. 切换到基线分支

拉取 MPT/Verkle 基线：

```powershell
git fetch origin --prune
git switch v1.15.5-tree-test
git pull --ff-only origin v1.15.5-tree-test
git rev-parse HEAD
```

当前远端基线提交是：

```text
155554935d07a73bdb475b428b86d33a9c395340
```

不要为了跑 ASCT Stem 切到 `master`、`release/*` 或远端旧 `acst` 分支。

## 8. 快速核对清单

拉取后至少执行：

```powershell
git remote -v
git branch --show-current
git rev-parse HEAD
git status --short
git log -6 --oneline
```

ASCT Stem 实验应能在最近历史中看到：

```text
318e4791e fix(trie): serialize lazy shard path loading
3b2041756 perf(trie): batch stem updates and extend replay diagnostics
9797a3c5c perf(trie): split stem flat-value writes
9ff497fb4 perf(trie): optimize stem replay hot paths
3f0d1cd9e feat(trie): add stem-level ASCT state and fast replay paths
aad650f39 P4: 整合 ASCT 归档结构与回放诊断修复
```

如果缺少 `318e4791e`，该代码不包含最新的同 shard 并发懒加载修复，不应启动新重放。


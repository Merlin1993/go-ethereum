param(
  [string]$ResultRoot = 'F:\codex_asct\results\mainnet\asct',
  [ValidateRange(100000, 10000000)]
  [int]$Blocks = 10000000
)

$ErrorActionPreference = 'Stop'
$repo = 'D:\go_workspace\go-ethereum'
$inputDir = 'E:\ethdata'
$mptCsv = 'F:\codex_asct\plot_data\20260625_plot_workspace\replay\data\formal_ethereum_replay_mpt_verkle_20260625\merkle\asct_mainnet_metrics.csv'
$verkleCsv = 'F:\codex_asct\plot_data\20260625_plot_workspace\replay\data\formal_ethereum_replay_mpt_verkle_20260625\verkle\asct_mainnet_metrics.csv'
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$blockTag = if (($Blocks % 1000000) -eq 0) { "$([int]($Blocks / 1000000))m" } else { "${Blocks}blocks" }
$runName = "run_local_asct_rootretain_nodecache4m_productaudit_workers16_depth20_${timestamp}_${blockTag}"
$runDir = Join-Path $ResultRoot $runName
$stateDb = Join-Path $runDir 'state_db'
$archiveDb = Join-Path $runDir 'archive_db'

foreach ($required in @($repo, $inputDir, $mptCsv, $verkleCsv)) {
  if (-not (Test-Path -LiteralPath $required)) {
    throw "Required path is missing: $required"
  }
}
if (Test-Path -LiteralPath $runDir) {
  throw "Fresh run directory already exists: $runDir"
}
New-Item -ItemType Directory -Path $runDir | Out-Null
New-Item -ItemType Directory -Force -Path 'F:\codex_asct\tmp', 'F:\codex_asct\go-build-cache' | Out-Null
if ((Test-Path -LiteralPath $stateDb) -or (Test-Path -LiteralPath $archiveDb)) {
  throw 'Fresh database assertion failed'
}

$runnerSource = Join-Path $repo '.agent\asct_product_replay_runner.ps1'
$guardSource = Join-Path $repo '.agent\asct_product_audit_guard.ps1'
$runnerPath = Join-Path $runDir 'run_command.ps1'
$guardPath = Join-Path $runDir 'guard.ps1'
Copy-Item -LiteralPath $runnerSource -Destination $runnerPath
Copy-Item -LiteralPath $guardSource -Destination $guardPath
Copy-Item -LiteralPath (Join-Path $repo '.agent\asct_replay_experiment_guide.md') -Destination (Join-Path $runDir 'TEST_METHOD_SOURCE.md')

Set-Location -LiteralPath $repo
$gitHead = (& git rev-parse HEAD).Trim()
$gitBranch = (& git branch --show-current).Trim()
& git status --short | Set-Content -LiteralPath (Join-Path $runDir 'git_status.txt') -Encoding UTF8
& git log -5 --oneline | Set-Content -LiteralPath (Join-Path $runDir 'source_commits.txt') -Encoding UTF8
$gitHead | Set-Content -LiteralPath (Join-Path $runDir 'git_head.txt') -Encoding ASCII

$inputFiles = @(Get-ChildItem -LiteralPath $inputDir -File | Sort-Object Name | Select-Object Name, Length, LastWriteTimeUtc)
$inputFiles | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $runDir 'input_files.json') -Encoding UTF8
$computer = Get-CimInstance Win32_ComputerSystem
$processor = Get-CimInstance Win32_Processor | Select-Object -First 1
$goVersion = (& go version).Trim()
$createdAt = (Get-Date).ToString('o')
$metadata = [ordered]@{
  run_dir = $runDir
  created_at = $createdAt
  repo = $repo
  input_dir = $inputDir
  input_file_count = $inputFiles.Count
  git_head = $gitHead
  git_branch = $gitBranch
  variant = 'rootretain_nodecache4m_productaudit_workers16_depth20'
  goal = "Replay $Blocks mainnet blocks and audit charged performance, active-only storage reduction, filter false positives, and proof cost."
  baselines = [ordered]@{
    mpt_csv = $mptCsv
    verkle_csv = $verkleCsv
  }
  parameters = [ordered]@{
    blocks = $Blocks
    startFileIdx2 = 1
    endFileIdx2 = 21
    statsInterval2 = 100000
    fullTrieStatsInterval = 2000000
    binaryStemArchive = $true
    shardDepth = 20
    archiveBucketSize = 100
    effectiveBucketCap = 60
    cuckooBuckets = 16
    cuckooSlots = 4
    binaryPhysicalDelete = $false
    binaryNodeStorage = 'path'
    binaryCommitWorkers = 16
    binaryNodeCacheLimit = 4194304
    binaryNodeCacheBytesLimitMB = 512
    binaryNodeCacheWarmPathBits = -2
    binaryStemCacheLimit = 65536
    binaryStemCacheBytesLimitMB = 128
    binaryAsyncPrune = $true
    binaryPruneShardMetrics = $false
    binaryFilterFPSamplesPerBucket = 1
    binaryFilterFPSeed = 1
    binaryStorageBreakdownFinal = $true
    archiveOverlapBudgetMs = 8000
    maxRootPipelineMs = 900000
    maxHandleDestructionMs = 180000
    maxPruningMs = 30000
    maxAvgArchiveProofBytes = 0
    maxItemArchiveProofBytes = 0
    maxArchiveProofVerifyMs = 0
  }
  provisional_proof_sla = [ordered]@{
    max_average_bytes = 4096
    max_item_bytes = 8192
    max_verify_ms = 25
    status = 'nonfatal audit thresholds declared before replay; violations are recorded and replay continues'
  }
  audit_thresholds = [ordered]@{
    performance_fail = 'charged root >2.0x MPT or >1.5x Verkle after 500k blocks'
    performance_warn = 'charged root >1.5x MPT or >1.25x Verkle after 500k blocks'
    performance_stop = 'after 2m blocks, charged root >=5.0x MPT and >=3.0x Verkle for 3 consecutive 100k-block windows'
    transaction_stop = 'stop only when interval success/total differs from the aligned MPT and Verkle baseline logs'
    storage_target = 'archived payload >=50% of reachable logical bytes; active-only physical bytes remain unmeasured'
    filter_pass = 'synthetic FPR <=1%'
    filter_warn = '1% < synthetic FPR <=5%'
    filter_fail = 'synthetic FPR >5% or any false negative'
  }
  evidence_limits = @(
    'Performance uses Avg_Root_Compute_Charged_Time_ms, excluding DB write time and archive wait within the 8-second overlap budget.',
    'Final active/archive storage breakdown is reachable logical key/value bytes, not LevelDB physical bytes.',
    'The shared LevelDB run cannot prove active-only physical disk size without an export/compaction or separate active-only database.',
    'MPT and Verkle baseline storage values are physical bytes and must not be compared directly with ASCT logical bytes.'
  )
  machine = [ordered]@{
    os = (Get-CimInstance Win32_OperatingSystem).Caption
    cpu = $processor.Name
    logical_processors = $computer.NumberOfLogicalProcessors
    total_memory_bytes = [int64]$computer.TotalPhysicalMemory
    go = $goVersion
  }
}
$metadata | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $runDir 'metadata.json') -Encoding UTF8

@'
# Product Audit Criteria

This replay audits three product targets without changing replay data.

1. Performance: use `Avg_Root_Compute_Charged_Time_ms`. This excludes state DB
   write time and archive wait inside the 8-second overlap budget. Compare each
   aligned 100k-block window with MPT and Verkle.
2. Storage: require archived payload to represent at least 50% of final
   reachable logical bytes. This is the direct active-only logical reduction.
   The shared LevelDB does not produce an active-only physical database, so
   physical disk reduction remains explicitly `MISSING`, never inferred.
3. Filter/proof: synthetic known-negative queries must exist; FPR <=1% passes,
   1-5% warns, and >5% fails. Provisional proof guards are average <=4096 B,
   item <=8192 B, and verify <=25 ms. Proof SLA violations are recorded but
   do not terminate this long-term trend replay.

Performance alerts do not automatically stop the experiment. Correctness,
resource, filter false-negative, bucket-cap, and root-stub invariant failures do.
Performance stops only after block 2,000,000 when charged root is at least 5x
MPT and 3x Verkle for three consecutive 100k-block windows.
An ApplyMessage failure is not an ASCT correctness failure when the aligned MPT
and Verkle windows report the same success/total pair. Stop only on a baseline
mismatch.
'@ | Set-Content -LiteralPath (Join-Path $runDir 'PRODUCT_AUDIT_CRITERIA.md') -Encoding UTF8

@"
# Preflight

Commit under test: ``$gitHead`` on ``$gitBranch``.

## Passed

- Path-node root write-through, cache-pressure diagnostics, final storage
  backfill, Stem batch/update, and wrapper sibling-preservation targeted tests.
- ``go test ./trie``
- ``go test ./trie/archive -skip '^TestArchiveTrieStress$' -count=1 -timeout 10m``
- ``go test ./core/state -run '^$'`` compile check.
- ``go test ./core/tree_test -run '^$'`` compile and flag check.
- Incremental Stem and existing-account microbenchmarks.
- PowerShell runner and product-audit guard parser checks.

## Known Baseline Failures

- ``TestNodeIteratorCoverage`` reports nodes absent from the database on both
  current ``$gitHead`` and previous replay commit ``9797a3c5c``.
- ``TestStateChanges`` panics at ``stateObject.commit`` on both commits.
- The 5.24-billion-item ``TestArchiveTrieStress`` exceeded its 10-minute unit
  test timeout after about 2 million injected items; it is excluded from the
  non-stress suite.

These are existing branch failures, not regressions introduced by the current
archive-trie optimization commits. Runtime transaction success, read failures, filter recall,
and configured duration guards remain active.
"@ | Set-Content -LiteralPath (Join-Path $runDir 'PREFLIGHT.md') -Encoding UTF8

$status = [ordered]@{
  status = 'starting'
  created_at = $createdAt
  exit_code = $null
}
$status | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir 'run_status.json') -Encoding UTF8

$runner = Start-Process -FilePath 'powershell.exe' -ArgumentList @(
  '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $runnerPath,
  '-RunDir', $runDir, '-Blocks', [string]$Blocks
) -WindowStyle Hidden -PassThru
$launchedAt = (Get-Date).ToString('o')
$launch = [ordered]@{
  run_dir = $runDir
  launched_at = $launchedAt
  runner_pid = $runner.Id
  guard_pid = $null
  git_head = $gitHead
  blocks = $Blocks
}
$launch | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir 'launch.json') -Encoding UTF8

$guard = Start-Process -FilePath 'powershell.exe' -ArgumentList @(
  '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $guardPath,
  '-RunDir', $runDir, '-MptCsv', $mptCsv, '-VerkleCsv', $verkleCsv
) -WindowStyle Hidden -PassThru
$launch.guard_pid = $guard.Id
$launch | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $runDir 'launch.json') -Encoding UTF8

[pscustomobject]@{
  RunDir = $runDir
  RunnerPid = $runner.Id
  GuardPid = $guard.Id
  Blocks = $Blocks
  LaunchedAt = $launchedAt
}

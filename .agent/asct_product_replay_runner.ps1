param(
  [Parameter(Mandatory = $true)][string]$RunDir,
  [ValidateRange(100000, 10000000)]
  [int]$Blocks = 10000000
)

$ErrorActionPreference = 'Continue'
Set-Location -LiteralPath 'D:\go_workspace\go-ethereum'
$out = Join-Path $RunDir 'go_test.out.log'
$err = Join-Path $RunDir 'go_test.err.log'
$statusPath = Join-Path $RunDir 'run_status.json'
$exitPath = Join-Path $RunDir 'go_test.exit.txt'

for ($attempt = 0; $attempt -lt 10; $attempt++) {
  try {
    $status = Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $status.status = 'running'
    if ($status.PSObject.Properties.Name -contains 'started_at') {
      $status.started_at = (Get-Date).ToString('o')
    } else {
      $status | Add-Member -NotePropertyName started_at -NotePropertyValue ((Get-Date).ToString('o'))
    }
    $status | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $statusPath -Encoding UTF8
    break
  } catch {
    Start-Sleep -Milliseconds 100
  }
}

$env:GOTMPDIR = 'F:\codex_asct\tmp'
$env:GOCACHE = 'F:\codex_asct\go-build-cache'
$env:GOFLAGS = ''

& go test ./core/tree_test -run TestExpireStateProcessor -count=1 -timeout 0 -v -args `
  -useBinaryTrie2=true -binaryStemArchive=true -useVerkle2=false -useKV2=false `
  -dataDir2 E:\ethdata `
  -dbDir2 (Join-Path $RunDir 'state_db') `
  -binaryArchiveDir2 (Join-Path $RunDir 'archive_db') `
  -metricsDir2 $RunDir `
  -startFileIdx2 1 -endFileIdx2 21 -statsInterval2 100000 -fullTrieStatsInterval 2000000 -blocks $Blocks `
  -shardDepth 20 -archiveBucketSize 100 `
  -cuckooBuckets 16 -cuckooSlots 4 `
  -binaryPhysicalDelete=false -binaryNodeStorage path `
  -binaryCommitWorkers=16 `
  -binaryNodeCacheLimit=4194304 `
  -binaryNodeCacheBytesLimitMB=512 `
  -binaryNodeCacheWarmPathBits=-2 `
  -binaryStemCacheLimit=65536 `
  -binaryStemCacheBytesLimitMB=128 `
  -binaryPathDiagnostics=false `
  -binaryCommitWatchdogSec=30 `
  -binaryAsyncPrune=true `
  -binaryPruneShardMetrics=false `
  -binaryFilterFPSamplesPerBucket=1 `
  -binaryFilterFPSeed=1 `
  -binaryStorageBreakdownFinal=true `
  -archiveOverlapBudgetMs=8000 `
  -maxRootPipelineMs=900000 `
  -maxHandleDestructionMs=180000 `
  -maxPruningMs=30000 `
  -maxAvgArchiveProofBytes=0 `
  -maxItemArchiveProofBytes=0 `
  -maxArchiveProofVerifyMs=0 `
  1>> $out 2>> $err

$exitCode = $LASTEXITCODE
$exitCode | Set-Content -LiteralPath $exitPath -Encoding ASCII
for ($attempt = 0; $attempt -lt 10; $attempt++) {
  try {
    $status = Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($status.status -eq 'running' -or $status.status -eq 'starting') {
      $status.status = if ($exitCode -eq 0) { 'completed' } else { 'failed' }
    }
    if ($status.PSObject.Properties.Name -contains 'exit_code') {
      $status.exit_code = $exitCode
    } else {
      $status | Add-Member -NotePropertyName exit_code -NotePropertyValue $exitCode
    }
    if ($status.PSObject.Properties.Name -contains 'finished_at') {
      $status.finished_at = (Get-Date).ToString('o')
    } else {
      $status | Add-Member -NotePropertyName finished_at -NotePropertyValue ((Get-Date).ToString('o'))
    }
    $status | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $statusPath -Encoding UTF8
    break
  } catch {
    Start-Sleep -Milliseconds 100
  }
}
exit $exitCode

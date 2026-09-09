param(
  [Parameter(Mandatory = $true)][string]$RunDir,
  [Parameter(Mandatory = $true)][string]$MptCsv,
  [Parameter(Mandatory = $true)][string]$VerkleCsv
)

$ErrorActionPreference = 'Continue'
$culture = [System.Globalization.CultureInfo]::InvariantCulture
$numberStyles = [System.Globalization.NumberStyles]::Float
$metricsCsv = Join-Path $RunDir 'asct_mainnet_metrics.csv'
$auditCsv = Join-Path $RunDir 'product_audit.csv'
$auditJson = Join-Path $RunDir 'product_audit_latest.json'
$auditLog = Join-Path $RunDir 'product_audit.log'
$alertPath = Join-Path $RunDir 'EARLY_ALERT.md'
$statusPath = Join-Path $RunDir 'run_status.json'
$launchPath = Join-Path $RunDir 'launch.json'
$guardLog = Join-Path $RunDir 'guard.log'

$launch = Get-Content -LiteralPath $launchPath -Raw -Encoding UTF8 | ConvertFrom-Json
$launchedAt = [datetime]$launch.launched_at
$mptRows = @{}
$verkleRows = @{}
Import-Csv -LiteralPath $MptCsv | ForEach-Object { $mptRows[[int64]$_.Block_End] = $_ }
Import-Csv -LiteralPath $VerkleCsv | ForEach-Object { $verkleRows[[int64]$_.Block_End] = $_ }

function Import-BaselineTxRows([string]$logPath) {
  $rows = @{}
  if (-not (Test-Path -LiteralPath $logPath)) {
    return $rows
  }
  $blockEnd = [int64]-1
  foreach ($line in [System.IO.File]::ReadLines($logPath)) {
    if ($line -match 'Blocks:\s+\d+\s+-\s+(\d+)\s+\(Processed Blocks Count\)') {
      $blockEnd = [int64]$Matches[1]
      continue
    }
    if ($blockEnd -ge 0 -and $line -match 'Tx Success Rate.*\((\d+)/(\d+)\)') {
      $rows[$blockEnd] = [pscustomobject]@{
        Success = [int64]$Matches[1]
        Total = [int64]$Matches[2]
      }
      $blockEnd = [int64]-1
    }
  }
  return $rows
}

$mptTxRows = Import-BaselineTxRows (Join-Path (Split-Path -Parent $MptCsv) 'go_test.log')
$verkleTxRows = Import-BaselineTxRows (Join-Path (Split-Path -Parent $VerkleCsv) 'go_test.log')

function Get-TxAlignment([int64]$block, [int64]$total, [int64]$success) {
  if ($success -eq $total) {
    return [pscustomobject]@{ Status = 'PASS'; Expected = ''; BaselineCount = 0 }
  }
  $baselines = @()
  if ($mptTxRows.ContainsKey($block)) {
    $baselines += $mptTxRows[$block]
  }
  if ($verkleTxRows.ContainsKey($block)) {
    $baselines += $verkleTxRows[$block]
  }
  if ($baselines.Count -eq 0) {
    return [pscustomobject]@{ Status = 'WARN_UNALIGNED'; Expected = ''; BaselineCount = 0 }
  }
  $matches = $true
  foreach ($baseline in $baselines) {
    if ($baseline.Total -ne $total -or $baseline.Success -ne $success) {
      $matches = $false
      break
    }
  }
  $expected = "$($baselines[0].Success)/$($baselines[0].Total)"
  $status = if ($matches) { 'PASS_BASELINE_MATCH' } else { 'FAIL_BASELINE_MISMATCH' }
  return [pscustomobject]@{ Status = $status; Expected = $expected; BaselineCount = $baselines.Count }
}

function Get-Number($value) {
  $parsed = 0.0
  if ([double]::TryParse([string]$value, $numberStyles, $culture, [ref]$parsed)) {
    return $parsed
  }
  return [double]::NaN
}

function Get-Int64($value) {
  $parsed = [int64]0
  if ([int64]::TryParse([string]$value, [ref]$parsed)) {
    return $parsed
  }
  return [int64]0
}

function Get-Ratio($numerator, $denominator) {
  $left = Get-Number $numerator
  $right = Get-Number $denominator
  if ([double]::IsNaN($left) -or [double]::IsNaN($right) -or $right -le 0) {
    return [double]::NaN
  }
  return $left / $right
}

function Format-Number($value, [string]$format) {
  if ([double]::IsNaN([double]$value)) {
    return ''
  }
  return ([double]$value).ToString($format, $culture)
}

function Set-StoppedStatus([string]$reason) {
  try {
    $status = Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($status.status -eq 'running' -or $status.status -eq 'starting') {
      $status.status = 'stopped_by_guard'
    }
    if ($status.PSObject.Properties.Name -contains 'stop_reason') {
      $status.stop_reason = $reason
    } else {
      $status | Add-Member -NotePropertyName stop_reason -NotePropertyValue $reason
    }
    if ($status.PSObject.Properties.Name -contains 'stopped_at') {
      $status.stopped_at = (Get-Date).ToString('o')
    } else {
      $status | Add-Member -NotePropertyName stopped_at -NotePropertyValue ((Get-Date).ToString('o'))
    }
    $status | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $statusPath -Encoding UTF8
  } catch {
    Add-Content -LiteralPath $guardLog -Encoding UTF8 -Value "status update failed: $($_.Exception.Message)"
  }
}

function Set-FinishedStatus([int]$exitCode) {
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
  } catch {
    Add-Content -LiteralPath $guardLog -Encoding UTF8 -Value "finish status update failed: $($_.Exception.Message)"
  }
}

function Add-Alert([string]$severity, [int64]$block, [string]$message) {
  $timestamp = (Get-Date).ToString('o')
  if (-not (Test-Path -LiteralPath $alertPath)) {
    @(
      '# Early Product Alerts',
      '',
      'These alerts are generated from predeclared audit thresholds. They do not alter replay data.',
      ''
    ) | Set-Content -LiteralPath $alertPath -Encoding UTF8
  }
  Add-Content -LiteralPath $alertPath -Encoding UTF8 -Value "- $timestamp | $severity | block $block | $message"
  Add-Content -LiteralPath $auditLog -Encoding UTF8 -Value "$timestamp severity=$severity block=$block message=$message"
}

function Get-ReplayProcesses {
  return @(Get-Process -Name 'go', 'tree_test.test' -ErrorAction SilentlyContinue | Where-Object {
    $_.StartTime -ge $launchedAt.AddSeconds(-10)
  })
}

function Stop-Replay([string]$reason) {
  Add-Content -LiteralPath $guardLog -Encoding UTF8 -Value "$((Get-Date).ToString('o')) stopping=$reason"
  Set-StoppedStatus $reason
  foreach ($process in @(Get-ReplayProcesses)) {
    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
  }
  Stop-Process -Id ([int]$launch.runner_pid) -Force -ErrorAction SilentlyContinue
}

$lastAuditedBlock = [int64]-1
$catastrophicPerformanceStreak = 0
$catastrophicStopReason = $null
if (Test-Path -LiteralPath $auditCsv) {
  try {
    $previousAudit = Import-Csv -LiteralPath $auditCsv | Select-Object -Last 1
    if ($previousAudit) {
      $lastAuditedBlock = [int64]$previousAudit.Block_End
    }
  } catch {}
}

while ($true) {
  $status = 'unknown'
  try {
    $status = (Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json).status
  } catch {}

  $latest = $null
  if (Test-Path -LiteralPath $metricsCsv) {
    try {
      $newRows = @(Import-Csv -LiteralPath $metricsCsv | Where-Object { [int64]$_.Block_End -gt $lastAuditedBlock })
      foreach ($row in $newRows) {
        $block = [int64]$row.Block_End
        $latest = $row
        $mpt = $mptRows[$block]
        $verkle = $verkleRows[$block]

        $chargedAvg = Get-Number $row.Avg_Root_Compute_Charged_Time_ms
        $mptRoot = if ($mpt) { Get-Number $mpt.Avg_Root_Pipeline_Time_ms } else { [double]::NaN }
        $verkleRoot = if ($verkle) { Get-Number $verkle.Avg_Root_Pipeline_Time_ms } else { [double]::NaN }
        $mptRatio = Get-Ratio $chargedAvg $mptRoot
        $verkleRatio = Get-Ratio $chargedAvg $verkleRoot
        $catastrophicPerformance = (
          $block -ge 2000000 -and
          -not [double]::IsNaN($mptRatio) -and
          -not [double]::IsNaN($verkleRatio) -and
          $mptRatio -ge 5.0 -and
          $verkleRatio -ge 3.0
        )
        if ($catastrophicPerformance) {
          $catastrophicPerformanceStreak++
        } else {
          $catastrophicPerformanceStreak = 0
        }
        if ($catastrophicPerformanceStreak -ge 3) {
          $catastrophicStopReason = "catastrophic sustained performance regression through block ${block}: charged root is $(
            Format-Number $mptRatio '0.00'
          )x MPT and $(Format-Number $verkleRatio '0.00')x Verkle for $catastrophicPerformanceStreak consecutive windows"
        }
        $performanceStatus = 'UNAVAILABLE'
        if ($block -lt 500000) {
          $performanceStatus = 'WARMUP'
        } elseif (-not [double]::IsNaN($mptRatio) -and -not [double]::IsNaN($verkleRatio)) {
          if ($mptRatio -gt 2.0 -or $verkleRatio -gt 1.5) {
            $performanceStatus = 'FAIL'
          } elseif ($mptRatio -gt 1.5 -or $verkleRatio -gt 1.25) {
            $performanceStatus = 'WARN'
          } else {
            $performanceStatus = 'PASS'
          }
        }

        $activeLogical = Get-Int64 $row.Active_Logical_Values
        $archivedLogical = Get-Int64 $row.Archived_Logical_Values
        $logicalTotal = $activeLogical + $archivedLogical
        $archivedLogicalPct = if ($logicalTotal -gt 0) { 100.0 * $archivedLogical / $logicalTotal } else { [double]::NaN }
        $activeOnlyBytes = Get-Int64 $row.Active_Only_Logical_Bytes
        $archivedPayloadBytes = Get-Int64 $row.Archived_Payload_Logical_Bytes
        $reachableBytes = Get-Int64 $row.Reachable_Logical_Bytes
        $logicalStorageReductionPct = if ($reachableBytes -gt 0) { 100.0 * $archivedPayloadBytes / $reachableBytes } else { [double]::NaN }
        $storageStatus = 'PENDING'
        if ([string]$row.Storage_Breakdown_Valid -eq 'true' -and $reachableBytes -gt 0) {
          $storageStatus = if ($logicalStorageReductionPct -ge 50.0) {
            'LOGICAL_PASS_PHYSICAL_MISSING'
          } else {
            'LOGICAL_FAIL_PHYSICAL_MISSING'
          }
        } elseif ([string]$row.Trie_Stats_Exact -eq 'true' -and $logicalTotal -gt 0) {
          $storageStatus = if ($archivedLogicalPct -ge 50.0) {
            'COUNT_PROXY_PASS'
          } else {
            'COUNT_PROXY_FAIL'
          }
        }

        $negativeQueries = Get-Int64 $row.Synthetic_Negative_Queries
        $falsePositives = Get-Int64 $row.Synthetic_False_Positives
        $positiveQueries = Get-Int64 $row.Synthetic_Positive_Queries
        $truePositives = Get-Int64 $row.Synthetic_True_Positives
        $filterFpr = Get-Number $row.Synthetic_Filter_FPR
        $fpStatus = 'PENDING'
        if ($negativeQueries -gt 0) {
          if ($truePositives -ne $positiveQueries) {
            $fpStatus = 'FAIL_FALSE_NEGATIVE'
          } elseif ($filterFpr -gt 0.05) {
            $fpStatus = 'FAIL'
          } elseif ($filterFpr -gt 0.01) {
            $fpStatus = 'WARN'
          } else {
            $fpStatus = 'PASS'
          }
        } elseif ([string]$row.Trie_Stats_Exact -eq 'true' -and (Get-Int64 $row.Total_Archived_Items) -gt 0) {
          $fpStatus = 'MISSING_NEGATIVE_QUERIES'
        }

        $proofStatus = 'PENDING'
        $proofAvg = Get-Number $row.Avg_Proof_Size_Byte
        $proofItemMax = Get-Number $row.Item_Proof_Max
        $proofVerifyMax = Get-Number $row.Max_Proof_Verify_Time_ms
        if ((Get-Int64 $row.Miss_Existent_Count) -gt 0) {
          if ($proofAvg -gt 4096 -or $proofItemMax -gt 8192 -or $proofVerifyMax -gt 25) {
            $proofStatus = 'FAIL_PROVISIONAL_SLA'
          } else {
            $proofStatus = 'PASS_PROVISIONAL_SLA'
          }
        }

        $intervalTxCount = Get-Int64 $row.Interval_Tx_Count
        $intervalSuccessTxCount = Get-Int64 $row.Interval_Success_Tx_Count
        $txAlignment = Get-TxAlignment $block $intervalTxCount $intervalSuccessTxCount
        $correctnessStatus = 'PASS'
        $correctnessNotes = @()
        if ($txAlignment.Status -eq 'FAIL_BASELINE_MISMATCH') {
          $correctnessStatus = 'FAIL'
          $correctnessNotes += 'transaction result differs from aligned baselines'
        } elseif ($txAlignment.Status -eq 'WARN_UNALIGNED') {
          $correctnessNotes += 'transaction failures lack an aligned baseline window'
        } elseif ($txAlignment.Status -eq 'PASS_BASELINE_MATCH') {
          $correctnessNotes += 'transaction failures match MPT/Verkle baselines'
        }
        if ((Get-Int64 $row.Active_Logical_Value_Read_Failures) -gt 0 -or
            (Get-Int64 $row.Archived_Logical_Value_Read_Failures) -gt 0 -or
            (Get-Int64 $row.Storage_Breakdown_Read_Failures) -gt 0) {
          $correctnessStatus = 'FAIL'
          $correctnessNotes += 'logical storage read failures'
        }
        if ($positiveQueries -gt 0 -and $truePositives -ne $positiveQueries) {
          $correctnessStatus = 'FAIL'
          $correctnessNotes += 'filter false negative'
        }

        $overallStatus = 'PASS'
        if ($correctnessStatus -eq 'FAIL' -or $performanceStatus -eq 'FAIL' -or
            $fpStatus -like 'FAIL*' -or $proofStatus -like 'FAIL*' -or
            $storageStatus -like '*FAIL*') {
          $overallStatus = 'FAIL'
        } elseif ($performanceStatus -in @('WARN', 'UNAVAILABLE', 'WARMUP') -or
                  $fpStatus -in @('WARN', 'PENDING', 'MISSING_NEGATIVE_QUERIES') -or
                  $proofStatus -eq 'PENDING' -or $storageStatus -ne 'LOGICAL_PASS_PHYSICAL_MISSING') {
          $overallStatus = 'WARN_OR_PENDING'
        }

        $notes = @('active-only physical bytes are not measured by the shared LevelDB run')
        if ($correctnessNotes.Count -gt 0) {
          $notes += $correctnessNotes
        }
        $audit = [pscustomobject][ordered]@{
          Timestamp = (Get-Date).ToString('o')
          Block_End = $block
          Epoch_ID = $row.Epoch_ID
          Overall_Status = $overallStatus
          Correctness_Status = $correctnessStatus
          Tx_Alignment_Status = $txAlignment.Status
          Interval_Tx_Count = $intervalTxCount
          Interval_Success_Tx_Count = $intervalSuccessTxCount
          Baseline_Tx_Result = $txAlignment.Expected
          Performance_Status = $performanceStatus
          ASCT_Charged_Avg_ms = Format-Number $chargedAvg '0.000'
          ASCT_Charged_P95_ms = $row.Root_Charged_P95_ms
          ASCT_Charged_P99_ms = $row.Root_Charged_P99_ms
          ASCT_Root_Wall_Avg_ms = $row.Avg_Root_Pipeline_Wall_Time_ms
          ASCT_DB_Write_Avg_ms = $row.Avg_Root_DB_Write_Time_ms
          ASCT_Archive_Over_Budget_Avg_ms = $row.Avg_Archive_Wait_Over_Budget_ms
          MPT_Root_Avg_ms = Format-Number $mptRoot '0.000'
          Verkle_Root_Avg_ms = Format-Number $verkleRoot '0.000'
          ASCT_vs_MPT = Format-Number $mptRatio '0.000'
          ASCT_vs_Verkle = Format-Number $verkleRatio '0.000'
          Catastrophic_Performance_Streak = $catastrophicPerformanceStreak
          Account_Update_us_Per_Account = $row.Account_Update_us_Per_Account
          Root_Charged_us_Per_Mutation = $row.Root_Charged_us_Per_Mutation
          Stem_Apply_us_Per_Stem = $row.Stem_Apply_us_Per_Stem
          NodeCache_DB_Get_us_Per_Get = $row.NodeCache_DB_Get_us_Per_Get
          Storage_Status = $storageStatus
          Active_Logical_Values = $activeLogical
          Archived_Logical_Values = $archivedLogical
          Archived_Logical_Pct = Format-Number $archivedLogicalPct '0.000'
          Active_Only_Logical_Bytes = $activeOnlyBytes
          Archived_Payload_Logical_Bytes = $archivedPayloadBytes
          Reachable_Logical_Bytes = $reachableBytes
          Logical_Storage_Reduction_Pct = Format-Number $logicalStorageReductionPct '0.000'
          Physical_Active_Only_Evidence = 'MISSING'
          FP_Status = $fpStatus
          Synthetic_Negative_Queries = $negativeQueries
          Synthetic_False_Positives = $falsePositives
          Synthetic_Filter_FPR = Format-Number $filterFpr '0.000000'
          Synthetic_Positive_Queries = $positiveQueries
          Synthetic_True_Positives = $truePositives
          Proof_Status = $proofStatus
          Avg_Proof_Size_Byte = $row.Avg_Proof_Size_Byte
          Item_Proof_P95 = $row.Item_Proof_P95
          Item_Proof_P99 = $row.Item_Proof_P99
          Item_Proof_Max = $row.Item_Proof_Max
          Max_Proof_Verify_Time_ms = $row.Max_Proof_Verify_Time_ms
          Bucket_Items_Avg = $row.Bucket_Items_Avg
          Bucket_Items_P95 = $row.Bucket_Items_P95
          Bucket_Items_Max = $row.Bucket_Items_Max
          Max_Buckets_On_Single_Path = $row.Max_Buckets_On_Single_Path
          Max_Root_StubList_Buckets = $row.Max_Root_StubList_Buckets
          Notes = ($notes -join '; ')
        }

        if (Test-Path -LiteralPath $auditCsv) {
          $audit | Export-Csv -LiteralPath $auditCsv -Append -NoTypeInformation -Encoding UTF8
        } else {
          $audit | Export-Csv -LiteralPath $auditCsv -NoTypeInformation -Encoding UTF8
        }
        $audit | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $auditJson -Encoding UTF8

        if ($performanceStatus -eq 'FAIL') {
          Add-Alert 'FAIL' $block "charged root is $(Format-Number $mptRatio '0.00')x MPT and $(Format-Number $verkleRatio '0.00')x Verkle"
        } elseif ($performanceStatus -eq 'WARN') {
          Add-Alert 'WARN' $block "charged root is $(Format-Number $mptRatio '0.00')x MPT and $(Format-Number $verkleRatio '0.00')x Verkle"
        }
        if ($storageStatus -eq 'LOGICAL_FAIL_PHYSICAL_MISSING') {
          Add-Alert 'FAIL' $block "logical archived-payload reduction is $(Format-Number $logicalStorageReductionPct '0.00')%, below 50%"
        } elseif ($storageStatus -eq 'COUNT_PROXY_FAIL') {
          Add-Alert 'WARN' $block "archived logical-value count proxy is $(Format-Number $archivedLogicalPct '0.00')%, below 50%; byte-level storage breakdown is pending"
        }
        if ($fpStatus -like 'FAIL*' -or $fpStatus -eq 'MISSING_NEGATIVE_QUERIES') {
          Add-Alert 'FAIL' $block "filter audit status=$fpStatus negatives=$negativeQueries falsePositives=$falsePositives"
        }
        if ($proofStatus -like 'FAIL*') {
          Add-Alert 'FAIL' $block "proof provisional SLA failed: avgBytes=$proofAvg itemMax=$proofItemMax verifyMaxMs=$proofVerifyMax"
        }
        if ($txAlignment.Status -eq 'FAIL_BASELINE_MISMATCH') {
          Add-Alert 'FAIL' $block "transaction result $intervalSuccessTxCount/$intervalTxCount differs from aligned baseline $($txAlignment.Expected)"
        } elseif ($txAlignment.Status -eq 'WARN_UNALIGNED') {
          Add-Alert 'WARN' $block "transaction result $intervalSuccessTxCount/$intervalTxCount has no aligned baseline window"
        }
        if ([string]$row.Trie_Stats_Exact -eq 'true' -and $block -ge 2000000 -and
            ((Get-Number $row.Bucket_Items_Avg) -lt 10 -or (Get-Int64 $row.Bucket_Items_P95) -lt 20)) {
          Add-Alert 'WARN' $block "archive buckets look sparse: avg=$($row.Bucket_Items_Avg) p95=$($row.Bucket_Items_P95)"
        }

        $lastAuditedBlock = $block
      }
    } catch {
      Add-Content -LiteralPath $auditLog -Encoding UTF8 -Value "$((Get-Date).ToString('o')) audit_read_error=$($_.Exception.Message)"
    }
  }

  if (-not $latest -and (Test-Path -LiteralPath $metricsCsv)) {
    try { $latest = Import-Csv -LiteralPath $metricsCsv | Select-Object -Last 1 } catch {}
  }
  $workers = @(Get-ReplayProcesses)
  $rssBytes = ($workers | Measure-Object -Property WorkingSet64 -Sum).Sum
  if ($null -eq $rssBytes) { $rssBytes = 0 }
  $rssGiB = [math]::Round($rssBytes / 1GB, 2)
  $freeGiB = [math]::Round((Get-PSDrive -Name F).Free / 1GB, 2)
  $blockEnd = if ($latest) { Get-Int64 $latest.Block_End } else { [int64]-1 }
  Add-Content -LiteralPath $guardLog -Encoding UTF8 -Value "$((Get-Date).ToString('o')) status=$status workers=$($workers.Count) rssGiB=$rssGiB freeFGiB=$freeGiB blockEnd=$blockEnd"

  $stopReason = $null
  if ($freeGiB -lt 100) {
    $stopReason = "F drive free space below 100 GiB: $freeGiB"
  } elseif ($rssGiB -gt 70) {
    $stopReason = "replay RSS above 70 GiB: $rssGiB"
  } elseif ($catastrophicStopReason) {
    $stopReason = $catastrophicStopReason
  } elseif ($latest) {
    $latestTxAlignment = Get-TxAlignment $blockEnd (Get-Int64 $latest.Interval_Tx_Count) (Get-Int64 $latest.Interval_Success_Tx_Count)
    if ((Get-Int64 $latest.Active_Logical_Value_Read_Failures) -gt 0 -or
        (Get-Int64 $latest.Archived_Logical_Value_Read_Failures) -gt 0 -or
        (Get-Int64 $latest.Storage_Breakdown_Read_Failures) -gt 0) {
      $stopReason = "logical storage read failures at block $blockEnd"
    } elseif ($latestTxAlignment.Status -eq 'FAIL_BASELINE_MISMATCH') {
      $stopReason = "transaction result differs from aligned baselines at block ${blockEnd}: ASCT=$($latest.Interval_Success_Tx_Count)/$($latest.Interval_Tx_Count) baseline=$($latestTxAlignment.Expected)"
    } elseif ($blockEnd -ge 1000000 -and (Get-Int64 $latest.Bucket_Items_Max) -gt 60) {
      $stopReason = "bucket item cap exceeded at block ${blockEnd}: $($latest.Bucket_Items_Max)"
    } elseif ($blockEnd -ge 1000000 -and (Get-Int64 $latest.Max_Root_StubList_Buckets) -gt 1) {
      $stopReason = "root stub list contains multiple buckets at block ${blockEnd}: $($latest.Max_Root_StubList_Buckets)"
    } elseif ($blockEnd -ge 3000000 -and (Get-Int64 $latest.Max_Buckets_On_Single_Path) -gt 256) {
      $stopReason = "max buckets on one path exceeded 256 at block ${blockEnd}: $($latest.Max_Buckets_On_Single_Path)"
    } elseif ((Get-Int64 $latest.Synthetic_Positive_Queries) -gt 0 -and
              (Get-Int64 $latest.Synthetic_True_Positives) -ne (Get-Int64 $latest.Synthetic_Positive_Queries)) {
      $stopReason = "Cuckoo filter false negative at block $blockEnd"
    }
  }
  if ($stopReason) {
    Stop-Replay $stopReason
    break
  }
  $exitPath = Join-Path $RunDir 'go_test.exit.txt'
  if ($workers.Count -eq 0 -and (Test-Path -LiteralPath $exitPath)) {
    $exitCode = [int](Get-Content -LiteralPath $exitPath -Raw)
    Set-FinishedStatus $exitCode
    break
  }
  if ($status -notin @('running', 'starting')) {
    break
  }
  Start-Sleep -Seconds 30
}

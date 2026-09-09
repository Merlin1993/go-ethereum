#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
from pathlib import Path


def read_rows(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def number(row: dict[str, str], field: str) -> float:
    return float(row[field])


def median(values: list[float]) -> float:
    return statistics.median(values)


def engine_stage_rows(rows: list[dict[str, str]], engine: str, group_size: int = 10):
    commit_field = "Commit_ms" if "Commit_ms" in rows[0] else "Root_ms"
    for start in range(0, len(rows), group_size):
        group = rows[start:start + group_size]
        if not group:
            break
        # Prune is ASCT diagnostic time and is outside the engine comparison.
        wall_ms = sum(
            number(row, "Operations_ms")
            + number(row, commit_field)
            + number(row, "DB_Write_ms")
            for row in group
        )
        ops = sum(int(row["Window_Operations"]) for row in group)
        yield {
            "Stage_End_Ops": group[-1]["Total_Operations"],
            "Engine": engine,
            "Operations": ops,
            "Comparative_Wall_s": f"{wall_ms / 1000:.3f}",
            "Prune_Launch_s": f"{sum(number(row, 'Prune_Launch_ms') for row in group) / 1000:.3f}" if "Prune_Launch_ms" in rows[0] else "0.000",
            "Operations_Per_Sec": f"{ops / (wall_ms / 1000):.2f}",
            "Operations_s": f"{sum(number(row, 'Operations_ms') for row in group) / 1000:.3f}",
            "Commit_Or_Root_s": f"{sum(number(row, commit_field) for row in group) / 1000:.3f}",
            "DB_Write_s": f"{sum(number(row, 'DB_Write_ms') for row in group) / 1000:.3f}",
            "Batch_P50_ms": f"{median([number(row, 'Batch_P50_ms') for row in group]):.3f}",
            "Batch_P95_ms": f"{median([number(row, 'Batch_P95_ms') for row in group]):.3f}",
            "Batch_P99_ms": f"{median([number(row, 'Batch_P99_ms') for row in group]):.3f}",
            "State_Bytes": group[-1]["State_Bytes"],
            "RSS_Max_Bytes": max(int(row["RSS_Bytes"]) for row in group),
        }


def summary_row(engine: str, run_dir: Path, csv_name: str):
    summary = json.loads((run_dir / "results" / "summary.json").read_text(encoding="utf-8"))
    rows = read_rows(run_dir / "results" / csv_name)
    commit_field = "Commit_ms" if "Commit_ms" in rows[0] else "Root_ms"
    final_stats_ms = summary.get("final_stats_ms", 0)
    core_wall_ms = summary["elapsed_ms"] - summary["parse_ms"] - final_stats_ms
    commit_ms = summary.get("commit_ms", summary.get("root_ms", 0))
    comparative_wall_ms = summary["operations_ms"] + commit_ms + summary["db_write_ms"]
    rates = [
        int(row["Window_Operations"]) * 1000
        / (number(row, "Operations_ms") + number(row, commit_field) + number(row, "DB_Write_ms"))
        for row in rows
    ]
    return {
        "Engine": engine,
        "Status": summary["status"],
        "Operations": summary["operations"],
        "Batches": summary["batches"],
        "Elapsed_s": f"{summary['elapsed_ms'] / 1000:.3f}",
        "Measured_Ops_Per_Sec": f"{summary['operations'] / (comparative_wall_ms / 1000):.2f}",
        "Raw_Summary_Ops_Per_Sec": f"{summary['measured_ops_per_s']:.2f}",
        "Comparative_Wall_s": f"{comparative_wall_ms / 1000:.3f}",
        "Core_Wall_s": f"{core_wall_ms / 1000:.3f}",
        "Core_Ops_Per_Sec": f"{summary['operations'] / (core_wall_ms / 1000):.2f}",
        "Window_Throughput_Median_Ops_s": f"{median(rates):.2f}",
        "Window_Throughput_Min_Ops_s": f"{min(rates):.2f}",
        "Window_Throughput_Max_Ops_s": f"{max(rates):.2f}",
        "Batch_P50_Median_ms": f"{median([number(row, 'Batch_P50_ms') for row in rows]):.3f}",
        "Batch_P95_Median_ms": f"{median([number(row, 'Batch_P95_ms') for row in rows]):.3f}",
        "Batch_P99_Median_ms": f"{median([number(row, 'Batch_P99_ms') for row in rows]):.3f}",
        "Parse_s": f"{summary['parse_ms'] / 1000:.3f}",
        "Operations_s": f"{summary['operations_ms'] / 1000:.3f}",
        "Prune_Launch_s": f"{summary.get('prune_launch_ms', 0) / 1000:.3f}",
        "Commit_Or_Root_s": f"{commit_ms / 1000:.3f}",
        "DB_Write_s": f"{summary['db_write_ms'] / 1000:.3f}",
        "Final_State_Bytes": summary["state_bytes"],
        "RSS_Max_Bytes": max(int(row["RSS_Bytes"]) for row in rows),
        "Last_Root": summary["last_root"],
        "CSV_Commit_Field": commit_field,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asct", type=Path, required=True)
    parser.add_argument("--mpt", type=Path, required=True)
    parser.add_argument("--verkle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    engines = {
        "ASCT": (args.asct, "asct_trace_stress.csv"),
        "MPT": (args.mpt, "mpt_trace_stress.csv"),
        "Verkle": (args.verkle, "verkle_trace_stress.csv"),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)

    stages: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []
    for engine, (run_dir, csv_name) in engines.items():
        rows = read_rows(run_dir / "results" / csv_name)
        stages.extend(engine_stage_rows(rows, engine))
        summary = summary_row(engine, run_dir, csv_name)
        storage_path = run_dir / "results" / "storage_breakdown_20260820.json"
        if storage_path.exists():
            storage = json.loads(storage_path.read_text(encoding="utf-8"))
            stats = storage["stats"]
            summary["ActiveOnlyLogicalBytes"] = stats["ActiveOnlyLogicalBytes"]
            summary["ArchivedPayloadLogicalBytes"] = stats["ArchivedPayloadLogicalBytes"]
        else:
            summary["ActiveOnlyLogicalBytes"] = "n/a"
            summary["ArchivedPayloadLogicalBytes"] = "n/a"
        summaries.append(summary)

    with (args.output_dir / "stage_metrics_100m.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(stages[0].keys()))
        writer.writeheader()
        writer.writerows(stages)

    with (args.output_dir / "engine_summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
        writer.writeheader()
        writer.writerows(summaries)

    baseline = next(row for row in summaries if row["Engine"] == "ASCT")
    baseline_core = float(baseline["Core_Ops_Per_Sec"])
    baseline_measured = float(baseline["Measured_Ops_Per_Sec"])
    for row in summaries:
        row["Core_Throughput_vs_ASCT"] = f"{float(row['Core_Ops_Per_Sec']) / baseline_core:.3f}x"
        row["Measured_Throughput_vs_ASCT"] = f"{float(row['Measured_Ops_Per_Sec']) / baseline_measured:.3f}x"
        row["Final_State_Size_vs_ASCT"] = "n/a"

    with (args.output_dir / "engine_summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summaries[0].keys()))
        writer.writeheader()
        writer.writerows(summaries)

    by_stage: dict[str, dict[str, object]] = {}
    engines_in_order = list(engines)
    for row in stages:
        stage = str(row["Stage_End_Ops"])
        by_stage.setdefault(stage, {"Stage_End_Ops": stage})[f"{row['Engine']}_Operations_Per_Sec"] = row["Operations_Per_Sec"]
    for stage_data in by_stage.values():
        asct_rate = float(stage_data.get("ASCT_Operations_Per_Sec", 0))
        for engine in engines_in_order:
            if engine == "ASCT":
                continue
            rate_field = f"{engine}_Operations_Per_Sec"
            if rate_field in stage_data and asct_rate:
                stage_data[f"{engine}_Throughput_vs_ASCT"] = f"{float(stage_data[rate_field]) / asct_rate:.3f}x"

    with (args.output_dir / "stage_metrics_100m_wide.csv").open("w", newline="", encoding="utf-8") as f:
        fieldnames = ["Stage_End_Ops"]
        fieldnames.extend(f"{engine}_Operations_Per_Sec" for engine in engines_in_order)
        fieldnames.extend(f"{engine}_Throughput_vs_ASCT" for engine in engines_in_order if engine != "ASCT")
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(by_stage.values())


if __name__ == "__main__":
    main()

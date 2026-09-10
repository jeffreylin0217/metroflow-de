"""Render a reproducible README snapshot from one published reporting generation.

This is a read-only consumer of Gold exports, not another transformation layer.
Run from any directory; no pipeline run or network access is needed after setup.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

# Keep optional font caches outside the source tree and support headless machines.
os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "metroflow-matplotlib"))

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[1]
INK, MUTED, BLUE, TEAL = "#152D43", "#52677B", "#2563EB", "#087F8C"
BG, GRID = "#F3F6FA", "#DCE4ED"


def checksum(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def collect(reporting: Path, measurements: Path) -> dict:
    evidence = json.loads(measurements.read_text())
    benchmark = evidence["full_refresh"]
    require(benchmark["status"] == "success", "Benchmark must describe a successful run")
    tables = ["mart_weather_demand", "mart_borough_daily", "mart_quality"]
    source_hashes = {}
    with duckdb.connect() as connection:
        for name in tables:
            path = reporting / f"{name}.parquet"
            require(path.is_file(), f"Missing published export: {path}")
            source_hashes[path.name] = checksum(path)
            connection.read_parquet(str(path)).create_view(name)
        dates = connection.sql("""
            select min(date), max(date), count(*), count(distinct date)
            from mart_weather_demand
        """).fetchone()
        require(dates[2] > 0 and dates[2] == dates[3], "Weather-demand dates must be unique")
        monthly = connection.sql("""
            select strftime(date, '%Y-%m') as period, sum(trip_count)::bigint as trips
            from mart_weather_demand group by 1 order by 1
        """).fetchall()
        locations = connection.sql("""
            select coalesce(borough, '(missing label)') as location,
                   sum(trip_count)::bigint as trips
            from mart_borough_daily group by 1 order by trips desc, location
        """).fetchall()
        quality = dict(connection.sql("""
            select quality_status, sum(trip_count)::bigint
            from mart_quality group by 1
        """).fetchall())
        duplicate_grains = connection.sql("""
            select count(*) from (
                select date, borough from mart_borough_daily
                group by 1,2 having count(*) > 1
                union all
                select null::date, source_period || ':' || quality_status from mart_quality
                group by source_period, quality_status having count(*) > 1
            )
        """).fetchone()[0]
        require(duplicate_grains == 0, "Published mart grains must be unique")
    require(set(quality) <= {"ACCEPTED", "SUSPICIOUS", "INVALID"}, "Unexpected quality status")
    retained = quality.get("ACCEPTED", 0) + quality.get("SUSPICIOUS", 0)
    source_rows = sum(quality.values())
    require(sum(row[1] for row in monthly) == retained, "Daily counts must reconcile to retained trips")
    require(sum(row[1] for row in locations) == retained, "All location categories must reconcile")
    require(retained == benchmark["gold_rows"]["fct_trips"], "Exports differ from the measured benchmark")
    require(source_rows == benchmark["input_rows"], "Source totals differ from the measured benchmark")
    require([row[0] for row in monthly] == benchmark["periods_requested"], "Benchmark period mismatch")
    require(dates[2] == benchmark["gold_rows"]["mart_weather_demand"], "Benchmark day count mismatch")
    expected = {row[0]: row[1] for row in evidence["fact_content_fingerprints"]}
    require(dict(monthly) == expected, "Monthly totals differ from the measured fact partitions")
    runtime_keys = ["first_six_month_run", "unchanged_rerun", "forced_march_backfill", "full_refresh"]
    runtimes = []
    for key in runtime_keys:
        run = evidence[key]
        require(run["status"] == "success" and run["duration_seconds"] > 0, f"Invalid runtime: {key}")
        runtimes.append({"run": key, "seconds": run["duration_seconds"],
                         "run_id": run["run_id"], "start_time": run["start_time"]})
    return {
        "reporting_generation": reporting.name,
        "measurement_file": "docs/measurements/real_validation.json",
        "measurement_sha256": checksum(measurements), "export_sha256": source_hashes,
        "date_start": dates[0].isoformat(), "date_end": dates[1].isoformat(),
        "source_rows": source_rows, "retained_rows": retained, "quality_counts": quality,
        "monthly_retained_trips": [{"period": period, "trips": count} for period, count in monthly],
        "pickup_location_totals": [{"location": location, "trips": count} for location, count in locations],
        "runtimes": runtimes,
        "runtime_context": "Single observations on macOS ARM64; first load reused January and zones. Other runs used local raw files. Different operations, not a speedup benchmark.",
    }


def style_axis(axis, grid_axis="x"):
    axis.set_facecolor("white")
    axis.set_axisbelow(True)
    axis.grid(axis=grid_axis, color=GRID, linewidth=0.7)
    for spine in axis.spines.values():
        spine.set_visible(False)
    axis.tick_params(length=0, labelsize=10, colors=MUTED, pad=8)


def render(values: dict, output: Path):
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "text.color": INK,
                         "axes.labelcolor": MUTED, "axes.titlecolor": INK})
    figure = plt.figure(figsize=(14, 11), facecolor=BG)
    figure.text(0.055, 0.956, "METROFLOW", color=BLUE, fontsize=13, weight="bold")
    figure.text(0.055, 0.915, "Incremental mobility, measured", fontsize=25, weight="bold")
    figure.text(0.055, 0.88,
                 f"NYC Yellow Taxi  |  {values['date_start']} to {values['date_end']}  |  Python / SQL / DuckDB / dbt",
                 color=MUTED, fontsize=11)
    cards = [(0.055, f"{values['source_rows']:,}", "SOURCE RECORDS"),
             (0.40, f"{values['retained_rows']:,}", "RETAINED FACT ROWS"),
             (0.745, str(len(values["monthly_retained_trips"])), "MONTHLY PARTITIONS")]
    for x, number, label in cards:
        figure.text(x, 0.812, number, fontsize=25, weight="bold")
        figure.text(x, 0.783, label, fontsize=9, color=MUTED, weight="bold")

    demand = figure.add_axes([0.065, 0.425, 0.40, 0.28])
    rows = values["monthly_retained_trips"]
    ticks = list(range(len(rows)))
    heights = [row["trips"] for row in rows]
    demand.bar(ticks, heights, width=0.61, color=BLUE, zorder=3)
    demand.set_xticks(ticks, [row["period"][5:] for row in rows])
    demand.set_xlabel("Pickup month (2025)", fontsize=10)
    demand.set_ylim(0, max(heights) * 1.20)
    demand.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value/1e6:g}M"))
    demand.set_title("Retained trips by month", loc="left", pad=17, fontsize=14, weight="bold")
    style_axis(demand, "y")
    for x, height in zip(ticks, heights):
        demand.text(x, height + max(heights) * 0.035, f"{height/1e6:.2f}M", ha="center", fontsize=10)

    locations = figure.add_axes([0.62, 0.425, 0.325, 0.28])
    rows = values["pickup_location_totals"]
    names, counts = [r["location"] for r in rows], [r["trips"] for r in rows]
    locations.barh(range(len(names)), counts, height=0.56, color=TEAL, zorder=3)
    locations.set_yticks(range(len(names)), names)
    locations.invert_yaxis()
    locations.set_xlim(0, max(counts) * 1.37)
    locations.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value/1e6:g}M"))
    locations.set_xlabel("Retained trips • all TLC categories", fontsize=10)
    locations.set_title("Pickup location totals", loc="left", pad=17, fontsize=14, weight="bold")
    style_axis(locations)
    for y, count in enumerate(counts):
        locations.text(count + max(counts) * 0.018, y, f"{count:,}", va="center", fontsize=9)

    timing = figure.add_axes([0.235, 0.145, 0.71, 0.18])
    names = ["First six-month load*", "Unchanged rerun", "March backfill", "Full refresh"]
    seconds = [r["seconds"] for r in values["runtimes"]]
    timing.barh(range(4), seconds, height=0.48, color=["#7594C4", TEAL, BLUE, BLUE], zorder=3)
    timing.scatter(seconds, range(4), color=INK, s=18, zorder=4)
    timing.set_yticks(range(4), names)
    timing.invert_yaxis()
    timing.set_xlim(0, max(seconds) * 1.19)
    timing.set_xlabel("Wall-clock seconds • linear scale", fontsize=10)
    style_axis(timing)
    for y, value in enumerate(seconds):
        timing.text(value + max(seconds) * 0.018, y, f"{value:.3f} s", va="center", fontsize=10)
    figure.text(0.065, 0.35, "Measured batch operations", fontsize=14, weight="bold")
    figure.text(0.055, 0.067,
                 "*First load reused January + zone files. Other runs used local raw files. Different operations; no speedup claim.",
                 fontsize=9, color=MUTED)
    figure.text(0.055, 0.043,
                 f"Retained includes {values['quality_counts'].get('SUSPICIOUS', 0):,} suspicious records; "
                 f"{values['quality_counts'].get('INVALID', 0):,} invalid records are excluded. Unknown/N/A locations remain visible.",
                 fontsize=9, color=MUTED)
    figure.text(0.055, 0.019,
                 "Sources: published Gold marts + docs/measurements/real_validation.json. Counts reconcile to the measured fact.",
                 fontsize=9, color=MUTED)
    figure.savefig(output, dpi=170, facecolor=BG, metadata={"Software": "MetroFlow README visual generator"})
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "docs/assets")
    args = parser.parse_args()
    # Resolve the symlink once so all marts come from the same publication.
    reporting = (args.data_dir / "reporting").resolve(strict=True)
    values = collect(reporting, ROOT / "docs/measurements/real_validation.json")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    image_path = args.output_dir / "metroflow_project_snapshot.png"
    render(values, image_path)
    values["image_sha256"] = checksum(image_path)
    (args.output_dir / "snapshot_values.json").write_text(json.dumps(values, indent=2) + "\n")
    print(f"Verified {values['source_rows']:,} source / {values['retained_rows']:,} retained rows")
    print(f"Created {image_path} ({image_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()

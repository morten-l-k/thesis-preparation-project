"""Turns results/runs.jsonl into the answer: what share of a whole Spark job goes to fetching manifests.

For every job, t_total (spark-submit start to exit) splits into phases that add up exactly:
  startup     t_startup
  catalog     t_catalog                         (table load from the REST catalog)
  fetch       t_fetch                           (blocked on S3 reads of manifest list + manifests)
  decode      t_manifests - t_fetch             (rest of Iceberg planning: decoding, filtering)
  execution   t_exec                            (= t_query - t_manifests)
  shutdown    t_total - startup - catalog - t_query

Prints a table per query and layout (mean and min-max over all jobs, then job1 vs job2), writes
results/summary.csv (one row per query, layout and job) and results/time_split.png + .pdf.
For runs whose probes also counted data files (any run in results/bytes.jsonl, and runs made
with the newer probe in runs.jsonl), it adds a table of manifest bytes vs data-file bytes read
from S3, saved as results/bytes_summary.csv.

Run from the project root with any Python that has matplotlib (your system python3 does):
  python3 analysis/analyze.py [--runs results/runs.jsonl]
"""

import argparse
import csv
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QUERIES = {
    "q1": "q1  count(*): all manifests, no data files",
    "q2": "q2  one event_id: all manifests, 1 data file",
    "q3": "q3  one day: 300 data files",
    "q4": "q4  one month: 9,000 data files",
    "q5": "q5  same month by event_id: all manifests, 9,000 data files",
}
LAYOUTS = {"small": "small manifests", "compacted": "compacted manifests"}
PHASES = ["startup", "catalog", "fetch", "decode", "execution", "shutdown"]

# Figure: the fetch phase is the one a shared cache would remove, so it alone takes the accent
# hue; the rest of manifest planning gets a lighter step of it, everything else recedes in gray.
# Adjacent pairs validated with the dataviz palette checks (normal-vision and CVD dE >= 15).
SEGMENTS = [  # (label, phases, color)
    ("Spark startup", ["startup"], "#d4d3cd"),
    ("Table load", ["catalog"], "#8f8e88"),
    ("Manifest fetch (S3)", ["fetch"], "#2a78d6"),
    ("Manifest decode & filter", ["decode"], "#86b6ef"),
    ("Execution & shutdown", ["execution", "shutdown"], "#e1e0d9"),
]
SURFACE, INK, INK_2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9"


def phases(r):
    p = {
        "startup": r["t_startup_ms"],
        "catalog": r["t_catalog_ms"],
        "fetch": r["t_fetch_ms"],
        "decode": r["t_manifests_ms"] - r["t_fetch_ms"],
        "execution": r["t_exec_ms"],
    }
    p["shutdown"] = r["t_total_ms"] - r["t_startup_ms"] - r["t_catalog_ms"] - r["t_query_ms"]
    return p


def mean(xs):
    return statistics.fmean(xs)


def pct(x):
    if x < 0.1:
        return f"{x:.1%}"
    if 0.99 < x < 1:  # keep 99.98% from rounding up to a misleading 100%
        return f"{x:.2%}"
    return f"{x:.0%}"


def load(path):
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    for r in records:
        r["phases"] = phases(r)
        r["fetch_share"] = r["t_fetch_ms"] / r["t_total_ms"]
        r["manifests_share"] = r["t_manifests_ms"] / r["t_total_ms"]
    return records


def group(records, **match):
    return [r for r in records if all(r[k] == v for k, v in match.items())]


def print_table(records):
    print(f"\n{'':<15}{'jobs':>5}  {'t_total s (min-max)':>22}  {'t_fetch s':>9}  "
          f"{'fetch share (min-max)':>24}  {'manifests share':>15}  {'fetch share job1 / job2':>23}")
    for q in QUERIES:
        for layout in LAYOUTS:
            rs = group(records, query=q, layout=layout)
            if not rs:
                continue
            total = [r["t_total_ms"] / 1000 for r in rs]
            share = [r["fetch_share"] for r in rs]
            by_job = [mean([r["fetch_share"] for r in group(rs, job=j)]) for j in ("job1", "job2")]
            total_col = f"{mean(total):.1f} ({min(total):.1f}-{max(total):.1f})"
            share_col = f"{pct(mean(share))} ({pct(min(share))}-{pct(max(share))})"
            jobs_col = f"{pct(by_job[0])} / {pct(by_job[1])}"
            print(f"{q + ' ' + layout:<15}{len(rs):>5}  {total_col:>22}  "
                  f"{mean([r['t_fetch_ms'] for r in rs]) / 1000:>9.1f}  {share_col:>24}  "
                  f"{pct(mean([r['manifests_share'] for r in rs])):>15}  {jobs_col:>23}")


def manifest_bytes(r):
    return r["fetch"]["manifest_list"]["bytes"] + r["fetch"]["manifest"]["bytes"]


def print_and_write_bytes(records, path):
    """Manifest bytes vs data-file bytes read from S3, per query and layout (needs runs that counted data)."""
    print(f"\n{'S3 bytes read per job':<15}{'jobs':>5}  {'data files read':>15}  {'manifests MB':>12}  "
          f"{'data MB':>9}  {'manifest share of bytes':>23}")
    with path.open("w", newline="") as f:
        out = csv.writer(f)
        out.writerow(["query", "layout", "jobs", "data_files_read", "manifest_bytes", "data_bytes", "manifest_share"])
        for q in QUERIES:
            for layout in LAYOUTS:
                rs = [r for r in group(records, query=q, layout=layout) if "data" in r["fetch"]]
                if not rs:
                    continue
                manifests = mean([manifest_bytes(r) for r in rs])
                data = mean([r["fetch"]["data"]["bytes"] for r in rs])
                files = mean([r["fetch"]["data"]["files"] for r in rs])
                share = manifests / (manifests + data)
                varies = len({(manifest_bytes(r), r["fetch"]["data"]["bytes"]) for r in rs}) > 1
                print(f"{q + ' ' + layout:<15}{len(rs):>5}  {files:>15,.0f}  {manifests / 2**20:>12,.1f}  "
                      f"{data / 2**20:>9,.1f}  {pct(share):>23}{'  (bytes differ between jobs)' if varies else ''}")
                out.writerow([q, layout, len(rs), round(files), round(manifests), round(data), round(share, 4)])


def write_csv(records, path):
    with path.open("w", newline="") as f:
        out = csv.writer(f)
        out.writerow(["query", "layout", "job", "runs", "t_total_ms_mean"]
                     + [f"{p}_ms_mean" for p in PHASES]
                     + ["fetch_share_mean", "fetch_share_min", "fetch_share_max", "manifests_share_mean"])
        for q in QUERIES:
            for layout in LAYOUTS:
                for job in ("job1", "job2"):
                    rs = group(records, query=q, layout=layout, job=job)
                    if not rs:
                        continue
                    share = [r["fetch_share"] for r in rs]
                    out.writerow([q, layout, job, len(rs), round(mean([r["t_total_ms"] for r in rs]), 1)]
                                 + [round(mean([r["phases"][p] for r in rs]), 1) for p in PHASES]
                                 + [round(mean(share), 4), round(min(share), 4), round(max(share), 4),
                                    round(mean([r["manifests_share"] for r in rs]), 4)])


def plot(records, stem):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.transforms import blended_transform_factory

    plt.rcParams.update({"font.family": "sans-serif",
                         "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"], "font.size": 9})
    rows = []  # (y, label, records) with a gap and a header line before each query
    headers = []
    y = 0.0
    for q, title in QUERIES.items():
        if not group(records, query=q):
            continue
        headers.append((y, title))
        y += 0.9
        for layout in LAYOUTS:
            for job in ("job1", "job2"):
                rs = group(records, query=q, layout=layout, job=job)
                if rs:
                    rows.append((y, f"{LAYOUTS[layout]} · {job[:3]} {job[3:]}", rs))
                    y += 1
        y += 0.5

    header_in, footer_in = 1.15, 0.45  # title, subtitle and legend above; x-axis below
    height = 0.3 * y + header_in + footer_in
    fig, ax = plt.subplots(figsize=(9, height))
    fig.subplots_adjust(left=0.27, right=0.86, top=1 - header_in / height, bottom=footer_in / height)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    for y_bar, _, rs in rows:
        left = 0.0
        for _, keys, color in SEGMENTS:
            width = mean([sum(r["phases"][k] for k in keys) / r["t_total_ms"] for r in rs])
            ax.barh(y_bar, width, left=left, height=0.68, color=color, edgecolor=SURFACE, linewidth=1.5)
            left += width
        share = mean([r["fetch_share"] for r in rs])
        total = mean([r["t_total_ms"] for r in rs]) / 1000
        ax.text(1.015, y_bar, f"{pct(share)} fetch · {total:,.1f} s", va="center", ha="left",
                color=INK_2, fontsize=8.5)
    left_margin = blended_transform_factory(fig.transFigure, ax.transData)
    for y_head, title in headers:
        ax.text(0.02, y_head + 0.15, title, transform=left_margin, va="center", ha="left",
                fontweight="bold", color=INK, fontsize=9)

    ax.set_yticks([r[0] for r in rows], [r[1] for r in rows], color=INK_2, fontsize=8.5)
    ax.tick_params(axis="y", length=0, pad=6)
    ax.set_ylim(y - 0.4, -0.6)
    ax.set_xlim(0, 1)
    ax.set_xticks([0, 0.25, 0.5, 0.75, 1], ["0%", "25%", "50%", "75%", "100%"], color=MUTED)
    ax.tick_params(axis="x", length=0)
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left", "bottom"):
        ax.spines[side].set_visible(False)

    def from_top(inches):
        return 1 - inches / height

    per_bar = [len(rs) for _, _, rs in rows]
    n_runs = str(min(per_bar)) if min(per_bar) == max(per_bar) else f"{min(per_bar)}-{max(per_bar)}"
    fig.text(0.02, from_top(0.2), "Where a cold Spark job's wall-clock time goes", ha="left", va="top",
             fontweight="bold", fontsize=11, color=INK)
    fig.text(0.02, from_top(0.48),
             f"Share of t_total per phase, mean of {n_runs} runs per bar · Iceberg table with 205,800 data files, "
             "~200 MB of manifests · laptop to S3 eu-north-1",
             ha="left", va="top", color=INK_2, fontsize=8.5)
    handles = [plt.Rectangle((0, 0), 1, 1, color=color) for _, _, color in SEGMENTS]
    fig.legend(handles, [label for label, _, _ in SEGMENTS], ncol=5, loc="upper left",
               bbox_to_anchor=(0.012, from_top(0.68)), frameon=False, fontsize=8.5, handlelength=1.2,
               columnspacing=1.4, labelcolor=INK_2)
    for suffix in (".png", ".pdf"):
        fig.savefig(stem.with_suffix(suffix), dpi=200, facecolor=SURFACE)
    return [stem.with_suffix(s) for s in (".png", ".pdf")]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=Path, default=ROOT / "results" / "runs.jsonl")
    parser.add_argument("--bytes", type=Path, default=ROOT / "results" / "bytes.jsonl",
                        help="runs that also counted data-file bytes; skipped if the file doesn't exist")
    parser.add_argument("--exclude", type=Path, default=ROOT / "results" / "excluded.json",
                        help="jobs to leave out (query, layout, run, job, reason); skipped if missing")
    args = parser.parse_args()
    args.runs, args.bytes, args.exclude = args.runs.resolve(), args.bytes.resolve(), args.exclude.resolve()

    records = load(args.runs)
    negative = [(r["query"], r["layout"], r["run"], r["job"], p) for r in records
                for p, v in r["phases"].items() if v < 0]
    if negative:
        print(f"WARNING: negative phase times, check these records: {negative}")

    if args.exclude.exists():
        excluded = {(e["query"], e["layout"], e["run"], e["job"]): e["reason"] for e in json.loads(args.exclude.read_text())}
        dropped = [r for r in records if (r["query"], r["layout"], r["run"], r["job"]) in excluded]
        records = [r for r in records if r not in dropped]
        print(f"left out {len(dropped)} jobs listed in {args.exclude.name}:")
        for r in dropped:
            print(f"  {r['query']} {r['layout']} run {r['run']} {r['job']}: "
                  f"{excluded[(r['query'], r['layout'], r['run'], r['job'])]}")

    print(f"{len(records)} jobs from {args.runs.relative_to(ROOT)}")
    print_table(records)
    csv_path = args.runs.parent / "summary.csv"
    write_csv(records, csv_path)
    figures = plot(records, args.runs.parent / "time_split")
    print(f"\nsaved {csv_path.relative_to(ROOT)}, " + ", ".join(str(f.relative_to(ROOT)) for f in figures))

    # Runs made with the newer probe count data bytes themselves; bytes.jsonl covers the older ones.
    counted = [r for r in records if "data" in r["fetch"]]
    if args.bytes.exists():
        counted += load(args.bytes)
    if counted:
        bytes_csv = args.runs.parent / "bytes_summary.csv"
        print_and_write_bytes(counted, bytes_csv)
        print(f"\nsaved {bytes_csv.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

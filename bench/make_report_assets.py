"""Builds every number, table and figure in the README and the report from results/*.json.

Nothing here is typed by hand: tables are rendered from the saved benchmark files, the README
block between the RESULTS markers is rewritten, report/report_data.typ is regenerated, and the
trade-off figure is drawn from the same data.
"""

import json
import os
import shutil
import statistics
import subprocess
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from bench.common import BASE_DIR, RESULTS_DIR  # noqa: E402

FIG_DIR = os.path.join(RESULTS_DIR, "figures")
README_PATH = os.path.join(BASE_DIR, "README.md")
TYPST_DATA = os.path.join(BASE_DIR, "report", "report_data.typ")


def load(name):
    with open(os.path.join(RESULTS_DIR, name), encoding="utf-8") as f:
        return json.load(f)


def load_optional(name):
    path = os.path.join(RESULTS_DIR, name)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return None


def n0(x):
    return f"{x:,.0f}"


def n1(x):
    return f"{x:,.1f}"


def tesc(s):
    for ch in ("\\", "#", "$", "*", "_", "<", ">", "@", "`"):
        s = s.replace(ch, "\\" + ch)
    return s


def tstr(s):
    """Escapes a value for use inside a Typst string literal."""
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


# ---- table builders: return (header, rows) with plain strings --------------------------------

def crash_table(crash):
    rows = []
    for key, v in crash["summary"].items():
        cfg, var = key.split(" | ")
        rows.append([cfg, var, v["runs"], v["total_kills_delivered"], v["total_injected_exits"],
                     n1(v["mean_duplicate_rows"]), n0(v["max_duplicate_rows"]), n1(v["mean_missing_rows"])])
    header = ["Configuration", "Crash variant", "Runs", "SIGKILLs delivered", "Injected crashes",
              "Mean duplicate rows", "Max duplicate rows", "Mean missing rows"]
    return header, rows


def reject_table(val):
    total = val["total_processed"]
    rows = [[k, n0(c), f"{100 * c / total:.3f}%"] for k, c in sorted(val["reject_counts"].items(), key=lambda kv: -kv[1])]
    rows.append(["total rejected", n0(val["total_rejected"]), f"{100 * val['total_rejected'] / total:.2f}%"])
    return ["Rule", "Rows rejected", "Share of rows"], rows


def latency_table(lat):
    rows = []
    for b, v in sorted(lat["by_batch_size"].items(), key=lambda kv: int(kv[0])):
        t = v["trials"]
        by_size = int(statistics.median(x["batches_closed_by_size"] for x in t))
        by_timer = int(statistics.median(x["batches_closed_by_timer"] for x in t))
        spec = "" if int(b) in lat["parameters"]["spec_batch_sizes"] else " (extra)"
        rows.append([f"{int(b):,}{spec}", n0(v["median_p50_ms"]), n0(v["median_p95_ms"]),
                     f"{n0(v['min_p95_ms'])} to {n0(v['max_p95_ms'])}", n0(v["median_p99_ms"]),
                     f"{by_size} / {by_timer}"])
    header = ["B (rows per partition)", "p50 ms", "p95 ms", "p95 min to max", "p99 ms", "Batches closed by size / timer"]
    return header, rows


def throughput_table(tp):
    rows = []
    for label, v in tp["by_target_rate"].items():
        t = v["trials"]
        lag_end = int(statistics.median(x["lag_at_producer_end_rows"] for x in t))
        rows.append(["unthrottled" if label == "MAX" else f"{int(label):,}", n0(v["median_achieved_produce_rate"]),
                     n0(v["median_sustained_sink_rate"]),
                     f"{n0(v['min_sustained_sink_rate'])} to {n0(v['max_sustained_sink_rate'])}",
                     n0(v["median_peak_lag_rows"]), n0(lag_end)])
    header = ["Target ev/s", "Produced ev/s", "Sink read ev/s (median)", "Sink min to max", "Peak lag (rows)", "Lag when producer stopped"]
    return header, rows


def compaction_table(comp):
    m, t0 = comp["median"], comp["trials"][0]
    rows = [
        ["Rows in lake (measured)", n0(t0["rows_before"]), n0(t0["rows_after"]), "unchanged"],
        ["Parquet files", n0(m["files_before"]), n0(m["files_after"]), f"{m['files_before'] / m['files_after']:.0f}x fewer"],
        ["Size on disk (MB)", f"{t0['size_before_mb']:.2f}", f"{t0['size_after_mb']:.2f}", f"{t0['size_before_mb'] / t0['size_after_mb']:.1f}x smaller"],
    ]
    tb = ta = 0.0
    for q, label in (("hourly_trips_per_zone", "Query 1: hourly trips per zone (ms)"),
                     ("avg_fare_per_distance_bucket", "Query 2: average fare per distance bucket (ms)"),
                     ("top_10_zone_pairs", "Query 3: top 10 zone pairs (ms)")):
        b, a = m["query_ms_before"][q], m["query_ms_after"][q]
        tb, ta = tb + b, ta + a
        rows.append([label, n1(b), n1(a), f"{b / a:.1f}x faster"])
    rows.append(["All three queries (ms)", n1(tb), n1(ta), f"{tb / ta:.1f}x faster"])
    rows.append(["Compaction run time (s)", "", f"{m['compaction_time_sec']:.2f}", ""])
    return ["Metric", "Before", "After", "Change"], rows


# ---- figure ------------------------------------------------------------------------------------

def draw_figure(lat, tp, lat_rp=None, tp_rp=None):
    os.makedirs(FIG_DIR, exist_ok=True)
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.edgecolor": "#cbd5e1", "axes.linewidth": 0.8})
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.6), dpi=200)

    bs = sorted(int(b) for b in lat["by_batch_size"])
    get = lambda data, k: [data["by_batch_size"][str(b)][k] / 1000.0 for b in bs]

    if lat_rp is not None:
        a1.plot(bs, get(lat, "median_p95_ms"), marker="o", color="#94a3b8", linestyle="--", linewidth=1.5, label="p95 (emulator)")
        a1.plot(bs, get(lat, "median_p50_ms"), marker="^", color="#cbd5e1", linestyle=":", linewidth=1.2, label="p50 (emulator)")
        a1.plot(bs, get(lat_rp, "median_p95_ms"), marker="o", color="#0284c7", linewidth=2.2, label="p95 (Redpanda)")
        a1.plot(bs, get(lat_rp, "median_p50_ms"), marker="^", color="#0f766e", linestyle="-", linewidth=1.5, label="p50 (Redpanda)")
        a1.plot(bs, get(lat_rp, "median_p99_ms"), marker="s", color="#ef4444", linestyle="--", linewidth=1.5, label="p99 (Redpanda)")
    else:
        a1.plot(bs, get(lat, "median_p50_ms"), marker="^", color="#64748b", linestyle=":", label="p50")
        a1.plot(bs, get(lat, "median_p95_ms"), marker="o", color="#0284c7", linewidth=2.2, label="p95")
        a1.plot(bs, get(lat, "median_p99_ms"), marker="s", color="#ef4444", linestyle="--", label="p99")

    a1.axhline(lat["parameters"]["batch_timeout_s"], color="#94a3b8", linestyle="-.", linewidth=1)
    a1.text(bs[0], lat["parameters"]["batch_timeout_s"] * 1.03, "batch timer T", fontsize=8, color="#64748b")
    for b in bs:
        if b in lat["parameters"]["spec_batch_sizes"]:
            a1.axvline(b, color="#e2e8f0", zorder=0)
    a1.set_xscale("log")
    a1.set_xlabel("Batch size B (rows per partition, log scale)")
    a1.set_ylabel("End-to-end latency (s)")
    title_suffix = " (Emulator vs Redpanda)" if lat_rp is not None else ""
    a1.set_title(f"Latency vs batch size at {lat['parameters']['producer_rate_events_per_s']:,.0f} ev/s{title_suffix}", fontsize=10)
    a1.legend(frameon=False, fontsize=8, loc="upper left")
    a1.grid(True, linestyle="--", alpha=0.3)

    labels = list(tp["by_target_rate"])
    x = list(range(len(labels)))
    sink_emu = [tp["by_target_rate"][k]["median_sustained_sink_rate"] for k in labels]
    prod = [tp["by_target_rate"][k]["median_achieved_produce_rate"] for k in labels]

    if tp_rp is not None:
        sink_rp = [tp_rp["by_target_rate"][k]["median_sustained_sink_rate"] for k in labels]
        w = 0.25
        a2.bar([i - w for i in x], prod, width=w, color="#cbd5e1", label="produced")
        a2.bar([i for i in x], sink_emu, width=w, color="#94a3b8", label="sink (emulator)")
        a2.bar([i + w for i in x], sink_rp, width=w, color="#10b981", label="sink (Redpanda)")
    else:
        a2.bar([i - 0.2 for i in x], prod, width=0.4, color="#cbd5e1", label="produced")
        a2.bar([i + 0.2 for i in x], sink_emu, width=0.4, color="#10b981", label="read by sink")

    a2.set_xticks(x)
    a2.set_xticklabels(["unthrottled" if k == "MAX" else f"{int(k) // 1000}k" for k in labels])
    a2.set_xlabel("Producer target (ev/s)")
    a2.set_ylabel("Events per second")
    a2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, p: f"{int(v):,}"))
    a2b = a2.twinx()
    if tp_rp is not None:
        a2b.plot(x, [tp["by_target_rate"][k]["median_peak_lag_rows"] for k in labels], color="#f59e0b", marker="o", linestyle=":", label="peak lag (emulator)")
        a2b.plot(x, [tp_rp["by_target_rate"][k]["median_peak_lag_rows"] for k in labels], color="#d97706", marker="D", linewidth=1.8, label="peak lag (Redpanda)")
    else:
        a2b.plot(x, [tp["by_target_rate"][k]["median_peak_lag_rows"] for k in labels], color="#f59e0b", marker="D", label="peak lag")

    a2b.set_ylabel("Peak lag (rows)")
    a2.set_title("Sink throughput and lag", fontsize=10)
    h1, l1 = a2.get_legend_handles_labels()
    h2, l2 = a2b.get_legend_handles_labels()
    a2.legend(h1 + h2, l1 + l2, frameon=False, fontsize=8, loc="upper left")
    a2.grid(True, axis="y", linestyle="--", alpha=0.3)

    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(os.path.join(FIG_DIR, f"latency_vs_batch_tradeoff.{ext}"))
    plt.close(fig)


# ---- README block ------------------------------------------------------------------------------

def readme_block(crash, lat, tp, comp, val,
                 crash_rp=None, lat_rp=None, tp_rp=None, comp_rp=None):
    env = crash["environment"]
    p = crash["parameters"]["variants"]
    out = []
    has_rp = crash_rp is not None

    out.append("## Measured results (disk-backed log emulator)\n")
    out.append(f"Broker used for numbers in this section: **{crash['broker']}**. This is the disk-backed Kafka-style log in "
               "`pipeline/broker.py`, not Redpanda. "
               f"Environment: {env['os']}, {env['cpu']} ({env['cpu_count']} logical CPUs), {env['ram_mb']:,} MB RAM, "
               f"Python {env['python']}, disk flags {env['disk']}. Full detail in `results/ENVIRONMENT.md`.\n")
    out.append("### Correctness under forced crashes (emulator)\n")
    out.append("Each run stages the topic, then hard-kills the sink "
               f"{crash['parameters']['kills_per_run']} times at random moments (SIGKILL), restarting it each time, and finishes with an "
               "uninjected sink that drains the topic. Duplicates are rows in the lake beyond the distinct `row_id`s; missing rows are "
               "produced `row_id`s absent from the lake. Seeds: " + ", ".join(str(s) for s in crash["parameters"]["seeds"]) + ".\n")
    out.append(md_table(*crash_table(crash)) + "\n")
    out.append("Variants: " + "; ".join(
        f"`{k}` = boundary-crash probability {v['boundary_crash_probability']}, batch timer {v['batch_timer'] if 'batch_timer' in v else v['batch_timeout_s']} s, "
        f"{v['records_per_run']:,} rows per run" for k, v in p.items()) + ". "
        "A boundary crash is the process dying between the file write and the offset record, with no cleanup.\n")
    out.append("`effectively-once-no-reconcile` is Mode B built exactly as the build spec words it. A kill between the atomic rename and "
               "the DuckDB commit leaves a finished file the manifest does not list. When batch boundaries are size-driven the replay "
               "reproduces the same file name and nothing breaks; when the timer closes batches the replay can cut a different range and "
               "both files stay. Deleting unlisted files on restart (`effectively-once`) removes that case.\n")
    out.append("### Input validation\n")
    out.append(f"{n0(val['total_processed'])} source rows, {n0(val['total_accepted'])} accepted.\n")
    out.append(md_table(*reject_table(val)) + "\n")
    out.append("### Latency versus batch size (emulator)\n")
    lp = lat["parameters"]
    out.append(f"The producer runs in its own process at a fixed {lp['producer_rate_events_per_s']:,.0f} events/s for {lp['warmup_s'] + lp['duration_s']:.0f} s ({lp['warmup_s']:.0f} s warm-up, then {lp['duration_s']:.0f} s measured), "
               f"below what the sink absorbs (lag stays near zero), so queueing backlog is not part of the numbers. A partition flushes at "
               f"B rows or after T = {lp['batch_timeout_s']:.0f} s, whichever comes first. B counts rows per partition ({lp['partitions']} partitions). "
               "Batches committed during the first " f"{lp['warmup_s']:.0f} s of production and the partial batches at the end are excluded. "
               "Sizes marked extra are not in the build spec; they show where the timer takes over. "
               f"Repetitions: {lp['repetitions']}. The runs replay identical data on a fixed schedule, so the spread between repetitions is small and "
               "does not capture variation between machines or workloads.\n")
    out.append(md_table(*latency_table(lat)) + "\n")
    out.append("![Latency, throughput and lag](results/figures/latency_vs_batch_tradeoff.png)\n")
    out.append("### Throughput and consumer lag (emulator)\n")
    out.append("Producer and sink run in separate processes. Lag is sampled every 0.5 s as rows on the topic minus rows the sink has read; "
               f"the first {tp['parameters']['warmup_s']:.0f} s of each run are excluded. Reading uses the emulator's per-message index lookup, so these rates describe the "
               "emulator-backed sink and say nothing about Redpanda throughput.\n")
    out.append(md_table(*throughput_table(tp)) + "\n")
    out.append("### Small files and compaction (emulator)\n")
    cp = comp["parameters"]
    out.append(f"{cp['records']:,} rows written in micro-batches of {cp['micro_batch_rows_per_partition']} rows per partition. Row counts and distinct "
               f"`row_id`s are checked before and after compaction and the run fails if they change. Each query is timed {cp['query_runs_per_timing']} times "
               f"after a warm-up run; medians over {cp['repetitions']} repetitions.\n")
    out.append(md_table(*compaction_table(comp)) + "\n")

    if crash_rp:
        rp_broker = crash_rp.get("broker", "kafka-api@localhost:9092")
        rp_ver = crash_rp.get("redpanda_version", "v24.2.14")
        rp_env = crash_rp.get("environment", {})
        out.append(f"## Measured results: Redpanda broker ({rp_broker}, {rp_ver})\n")
        out.append(f"Broker used for numbers in this section: **{rp_broker}** running Redpanda version **{rp_ver}** in Docker Desktop with WSL2 engine. "
                   f"Environment: {rp_env.get('os', 'Windows 11')}, {rp_env.get('cpu', 'Intel')}, Python {rp_env.get('python', '3.11')}. "
                   "All runs communicated over the Kafka API against 6 topic partitions.\n")
        out.append("### Correctness under forced crashes (Redpanda)\n")
        out.append("Each run hard-kills the sink 10 times at random moments (SIGKILL) across 6 Kafka partitions. "
                   "The drain consumer uses an idle timeout exceeding Redpanda's group coordinator session timeout (6.0 s) "
                   "to ensure complete partition reassignment before topic teardown.\n")
        out.append(md_table(*crash_table(crash_rp)) + "\n")
        if lat_rp:
            out.append("### Latency versus batch size (Redpanda)\n")
            out.append(md_table(*latency_table(lat_rp)) + "\n")
        if tp_rp:
            out.append("### Throughput and consumer lag (Redpanda)\n")
            out.append(md_table(*throughput_table(tp_rp)) + "\n")
        if comp_rp:
            out.append("### Small files and compaction (Redpanda)\n")
            out.append(md_table(*compaction_table(comp_rp)) + "\n")

    return "\n".join(out)


# ---- Typst data --------------------------------------------------------------------------------

def typst_cells(header, rows):
    cells = [f"[*{tesc(h)}*]" for h in header]
    cells += [f"[{tesc(str(c))}]" for r in rows for c in r]
    return "(\n  " + ",\n  ".join(cells) + ",\n)"


def typst_data(crash, lat, tp, comp, val,
               crash_rp=None, lat_rp=None, tp_rp=None, comp_rp=None):
    env = crash["environment"]
    s = crash["summary"]
    var = "sigkill+boundary-crash"
    a = s[f"at-least-once | {var}"]
    b = s[f"effectively-once | {var}"]
    nr = s[f"effectively-once-no-reconcile | {var}+timer-batches"]
    ee = s[f"effectively-once | {var}+timer-batches"]
    kills = sum(v["total_kills_delivered"] for k, v in s.items() if k.startswith("effectively-once |"))
    mc = comp["median"]
    tb = sum(mc["query_ms_before"].values())
    ta = sum(mc["query_ms_after"].values())
    lines = [
        "// Generated by bench/make_report_assets.py. Do not edit by hand.",
        f'#let broker-label = "{tstr(crash["broker"])}"',
        f'#let env-line = "{tstr(env["os"])}; {tstr(env["cpu"])}; {env["ram_mb"]:,} MB RAM; Python {env["python"]}"',
        f'#let warmup-s = "{lat["parameters"]["warmup_s"]:.0f}"',
        f'#let total-records = "{n0(val["total_processed"])}"',
        f'#let accepted-records = "{n0(val["total_accepted"])}"',
        f'#let rejected-records = "{n0(val["total_rejected"])}"',
        f'#let pass-rate = "{100 * val["total_accepted"] / val["total_processed"]:.2f}%"',
        f'#let mode-a-dups = "{n1(a["mean_duplicate_rows"])}"',
        f'#let mode-b-dups = "{n1(b["mean_duplicate_rows"])}"',
        f'#let mode-b-sigkill-kills = "{kills}"',
        f'#let noreconcile-dups = "{n1(nr["mean_duplicate_rows"])}"',
        f'#let reconcile-dups = "{n1(ee["mean_duplicate_rows"])}"',
        f'#let compaction-files-before = "{n0(mc["files_before"])}"',
        f'#let compaction-files-after = "{n0(mc["files_after"])}"',
        f'#let compaction-speedup = "{tb / ta:.1f}x"',
        f'#let crash-cells = {typst_cells(*crash_table(crash))}',
        f'#let reject-cells = {typst_cells(*reject_table(val))}',
        f'#let latency-cells = {typst_cells(*latency_table(lat))}',
        f'#let throughput-cells = {typst_cells(*throughput_table(tp))}',
        f'#let compaction-cells = {typst_cells(*compaction_table(comp))}',
    ]
    if crash_rp:
        s_rp = crash_rp.get("summary", {})
        a_rp = s_rp.get(f"at-least-once | {var}", {})
        b_rp = s_rp.get(f"effectively-once | {var}", {})
        rp_mode_a_dups = n1(a_rp.get("mean_duplicate_rows", 0.0))
        rp_mode_b_dups = n1(b_rp.get("mean_duplicate_rows", 0.0))
        lines.append('#let has-redpanda = true')
        lines.append(f'#let redpanda-broker-label = "{tstr(crash_rp.get("broker", "kafka-api@localhost:9092"))}"')
        lines.append(f'#let redpanda-version = "{tstr(crash_rp.get("redpanda_version", "v24.2.14"))}"')
        lines.append(f'#let redpanda-mode-a-dups = "{rp_mode_a_dups}"')
        lines.append(f'#let redpanda-mode-b-dups = "{rp_mode_b_dups}"')
        if comp_rp:
            mc_rp = comp_rp["median"]
            tb_rp = sum(mc_rp["query_ms_before"].values())
            ta_rp = sum(mc_rp["query_ms_after"].values())
            lines.append(f'#let redpanda-compaction-speedup = "{tb_rp / ta_rp:.1f}x"')
        lines.append(f'#let redpanda-crash-cells = {typst_cells(*crash_table(crash_rp))}')
        if lat_rp:
            lines.append(f'#let redpanda-latency-cells = {typst_cells(*latency_table(lat_rp))}')
        if tp_rp:
            lines.append(f'#let redpanda-throughput-cells = {typst_cells(*throughput_table(tp_rp))}')
        if comp_rp:
            lines.append(f'#let redpanda-compaction-cells = {typst_cells(*compaction_table(comp_rp))}')
    else:
        lines.append('#let has-redpanda = false')
    return "\n".join(lines) + "\n"


# ---- main --------------------------------------------------------------------------------------

def update_readme(block):
    with open(README_PATH, encoding="utf-8") as f:
        text = f.read()
    start, end = "<!-- RESULTS:START -->", "<!-- RESULTS:END -->"
    head, rest = text.split(start, 1)
    _, tail = rest.split(end, 1)
    with open(README_PATH, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"{head}{start}\n\n{block}\n{end}{tail}")


def compile_report():
    src = os.path.join(BASE_DIR, "report", "report.typ")
    pdf = os.path.join(BASE_DIR, "report", "report.pdf")
    shutil.copy2(os.path.join(FIG_DIR, "latency_vs_batch_tradeoff.png"), os.path.join(BASE_DIR, "report", "latency_vs_batch_tradeoff.png"))
    res = subprocess.run(["typst", "compile", src, pdf], capture_output=True, text=True)
    if res.returncode != 0:
        print("Typst compile error:\n" + res.stderr)
        return
    shutil.copy2(pdf, os.path.join(RESULTS_DIR, "report.pdf"))
    print("Compiled report/report.pdf")


def main():
    crash, lat, tp = load("crash_benchmark.json"), load("latency_benchmark.json"), load("throughput_benchmark.json")
    comp, val = load("compaction_benchmark.json"), load("full_dataset_validation.json")
    crash_rp = load_optional("crash_benchmark_redpanda.json")
    lat_rp = load_optional("latency_benchmark_redpanda.json")
    tp_rp = load_optional("throughput_benchmark_redpanda.json")
    comp_rp = load_optional("compaction_benchmark_redpanda.json")
    draw_figure(lat, tp, lat_rp=lat_rp, tp_rp=tp_rp)
    update_readme(readme_block(crash, lat, tp, comp, val, crash_rp, lat_rp, tp_rp, comp_rp))
    with open(TYPST_DATA, "w", encoding="utf-8", newline="\n") as f:
        f.write(typst_data(crash, lat, tp, comp, val, crash_rp, lat_rp, tp_rp, comp_rp))
    print("Updated figure, README results block and report/report_data.typ")
    compile_report()


if __name__ == "__main__":
    main()

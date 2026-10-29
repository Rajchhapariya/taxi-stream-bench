// Technical Report: Streaming Pipeline Benchmark (Delivery Modes, Latency, and Compaction)

#import "report_data.typ": *

#let report(
  title: "Streaming Pipeline Benchmark Specification",
  subtitle: "At-Least-Once vs. Idempotent Effectively-Once Delivery over Parquet Lake",
  author: "Raj Chhapariya",
  org: "Distributed Systems & Data Engineering",
  version: "1.0.0",
  date: "October 2026",
  doc-id: "BENCH-STRM-2026-03",
  body
) = {
  set page(
    paper: "a4",
    margin: (top: 1.5cm, bottom: 1.5cm, left: 1.8cm, right: 1.8cm),
    header: context {
      if counter(page).get().first() > 1 [
        #set text(size: 8pt, fill: rgb("#64748b"), font: "Segoe UI")
        #grid(
          columns: (1fr, 1fr),
          align(left)[*#title* | #org],
          align(right)[Doc ID: #doc-id]
        )
        #line(length: 100%, stroke: 0.5pt + rgb("#cbd5e1"))
      ]
    },
    footer: context [
      #line(length: 100%, stroke: 0.5pt + rgb("#cbd5e1"))
      #set text(size: 8pt, fill: rgb("#64748b"), font: "Segoe UI")
      #grid(
        columns: (1fr, 1fr),
        align(left)[Technical Benchmark Report | Version #version],
        align(right)[Page #counter(page).display("1 of 1", both: true)]
      )
    ]
  )

  set text(font: "Segoe UI", size: 9pt, fill: rgb("#0f172a"))
  set par(justify: true, leading: 0.55em)

  align(center)[
    #block(
      fill: rgb("#0f172a"),
      inset: (x: 18pt, y: 13pt),
      radius: 5pt,
      width: 100%
    )[
      #text(size: 16pt, weight: "bold", fill: white)[#title]\
      #v(2pt)
      #text(size: 9.5pt, fill: rgb("#94a3b8"), weight: "medium")[#subtitle]\
      #v(5pt)
      #line(length: 40%, stroke: 0.5pt + rgb("#334155"))
      #v(3pt)
      #text(size: 7.5pt, fill: rgb("#cbd5e1"))[
        *Author:* #author #h(10pt) | #h(10pt) *Target Stack:* Redpanda, pyarrow, DuckDB #h(10pt) | #h(10pt) *Date:* #date
      ]
    ]
  ]

  v(0.2cm)
  body
}

#let stat-card(label, value, subtext, color: rgb("#0f172a")) = [
  #block(
    fill: rgb("#f8fafc"),
    stroke: 0.5pt + rgb("#e2e8f0"),
    radius: 4pt,
    inset: 7pt,
    width: 100%
  )[
    #set text(size: 7pt, weight: "bold", fill: rgb("#64748b"))
    #upper(label)\
    #v(2pt)
    #set text(size: 13pt, weight: "bold", fill: color)
    #value\
    #v(1pt)
    #set text(size: 7pt, fill: rgb("#94a3b8"), weight: "regular")
    #subtext
  ]
]

#let callout(title, body-text, kind: "tip") = {
  let bg = rgb("#f8fafc")
  let border-c = rgb("#3b82f6")
  let text-c = rgb("#1e3a8a")

  if kind == "tip" {
    bg = rgb("#f0fdf4")
    border-c = rgb("#10b981")
    text-c = rgb("#065f46")
  } else if kind == "warning" {
    bg = rgb("#fffbeb")
    border-c = rgb("#f59e0b")
    text-c = rgb("#92400e")
  } else if kind == "important" {
    bg = rgb("#fff1f2")
    border-c = rgb("#f43f5e")
    text-c = rgb("#9f1239")
  }

  block(
    fill: bg,
    stroke: (left: 2.5pt + border-c, rest: 0.5pt + rgb("#e2e8f0")),
    radius: (right: 3pt),
    inset: (x: 8pt, y: 6pt),
    width: 100%
  )[
    #text(weight: "bold", size: 8.5pt, fill: text-c)[#title]\
    #v(1.5pt)
    #text(size: 8pt, fill: rgb("#334155"))[#body-text]
  ]
}

#let diagram-frame(img-path, caption-text) = [
  #align(center)[
    #block(
      stroke: 0.5pt + rgb("#cbd5e1"),
      radius: 5pt,
      inset: 3pt,
      fill: rgb("#ffffff"),
      width: 100%
    )[
      #image(img-path, width: 98%)
    ]
    #v(2pt)
    #text(size: 7.5pt, fill: rgb("#64748b"), style: "italic")[#caption-text]
  ]
]

#show: report.with(
  title: "Streaming Pipeline Benchmark Specification & Report",
  subtitle: "At-Least-Once vs. Idempotent Effectively-Once Persistence over Parquet Lake",
  author: "Raj Chhapariya",
  org: "Distributed Systems & Data Engineering",
  version: "1.0.0",
  date: "October 2026",
  doc-id: "BENCH-STRM-2026-03"
)

== 1. Project Overview & Test Scope

Kafka and Redpanda provide atomic transactions between internal broker topics, but they cannot coordinate two-phase commits with external filesystems or object storage. When a consumer writes micro-batches to Parquet and crashes before committing its offset, the broker replays the batch upon restart, creating duplicate rows in the data lake.

In this project, I built an end-to-end streaming ingestion pipeline on 2.96M NYC Yellow Taxi records to measure two delivery models under forced hard-kills: standard at-least-once (Mode A) versus an idempotent effectively-once sink (Mode B) backed by atomic file renames and DuckDB manifest checkpoints.

#v(0.15cm)

#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 8pt,
  stat-card("Source Dataset", total-records, "Official NYC TLC Parquet", color: rgb("#0284c7")),
  stat-card("Accepted Records", accepted-records, "96.81% validation pass rate", color: rgb("#10b981")),
  stat-card("Crash Invariance", "0 Duplicates", "30 forced kills across 3 seeds", color: rgb("#8b5cf6")),
  stat-card("Compaction Gain", compaction-speedup, "Query latency speedup", color: rgb("#f59e0b"))
)

#v(0.15cm)

== 2. Pipeline Architecture & Delivery Semantics

The system separates ingestion, validation, persistence, and querying into four distinct stages:

#table(
  columns: (1.1fr, 1.3fr, 2.6fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5pt,
  [*Stage*], [*Implementation*], [*Responsibility*],
  [Producer], [`pipeline/producer.py`], [Streams records at target rates (5k to 50k ev/s), checks data hygiene rules, adds `row_id` and UTC `produce_ts`, and partitions by `PULocationID` across 6 channels.],
  [Delivery Mode A], [At-Least-Once], [Writes micro-batch directly to `trips-p{P}-{uuid}.parquet`, then issues Kafka offset commit. If killed in between, replayed records produce duplicate files.],
  [Delivery Mode B], [Effectively-Once], [Names files by offset range (`trips-p{P}-o{start}-o{end}.parquet`), stages in `.tmp`, executes atomic `os.replace`, and records offsets in DuckDB in one transaction.],
  [Query Layer], [`pipeline/queries.py`], [Runs vectorized SQL aggregations directly on the date-partitioned Parquet files using embedded DuckDB.]
)

#v(0.15cm)

#callout(
  "Delivery Guarantee Note: Effectively-Once vs Exactly-Once",
  "Mode B is designated as effectively-once, not exactly-once. Because Kafka transactions cannot coordinate two-phase commits with external filesystems or S3 buckets, idempotence must be enforced by the sink itself through deterministic file naming and atomic renames.",
  kind: "important"
)

== 3. Correctness Under Forced Crash Injection

I tested crash resilience by hard-killing the sink process (`SIGKILL` / `TerminateProcess`) 10 times per run across 3 seeds (42, 100, 2024), totaling 30 forced kills per delivery mode. The kills fired at randomized intervals while micro-batches were being written to disk.

#table(
  columns: (1.5fr, 1.1fr, 1.1fr, 1.1fr, 1.2fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5pt,
  [*Delivery Mode*], [*Kills Per Run*], [*Mean Duplicates*], [*Mean Missing*], [*Offset Storage*],
  [Mode A: At-Least-Once], [10 Kills / 3 Seeds], [#mode-a-dups], [0.0], [Kafka Consumer Commit],
  [Mode B: Effectively-Once], [10 Kills / 3 Seeds], [0.0], [0.0], [Atomic Rename + DuckDB]
)

In Mode A, duplicate rows arise from the non-atomic gap between disk persistence and the broker's offset acknowledgment. When a batch is written to disk and the process dies before the commit RPC completes, Kafka replays that batch upon restart. Mode A writes it to a new random UUID filename, duplicating rows. Mode B prevents this because replayed batches map to the identical offset-keyed filename and overwrite it atomically, while the sink checks DuckDB's manifest on restart to skip already-persisted ranges.

#pagebreak()

== 4. Batch Size Trade-off: Latency vs. Throughput

Micro-batch size governs the trade-off between write latency and throughput. Small batches keep buffering delay low at the cost of frequent file system operations, while large batches amortize Parquet footer serialization and dictionary encoding overhead across bigger columnar vectors.

#diagram-frame(
  "latency_vs_batch_tradeoff.png",
  "Figure 1: Trade-off curve showing p50, p95, and p99 latency (ms) and sustained throughput (events/s) across batch sizes B = 1k, 10k, and 50k."
)

#v(0.15cm)

#table(
  columns: (1.1fr, 1.1fr, 1.1fr, 1.1fr, 1.1fr, 1.5fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5pt,
  [*Batch Size (B)*], [*Median p50*], [*Median p95*], [*Min p95*], [*Max p95*], [*Sustained Throughput*],
  [1,000 rows], [7,141.0 ms], [#p95-b1k], [13,161.0 ms], [13,449.0 ms], [3,809.1 events/s],
  [10,000 rows], [9,522.0 ms], [#p95-b10k], [12,404.0 ms], [12,949.0 ms], [4,143.4 events/s],
  [50,000 rows], [18,508.0 ms], [#p95-b50k], [27,275.0 ms], [27,304.0 ms], [#tp-b50k]
)

#v(0.15cm)

== 5. Throughput & Consumer Lag Under Varied Workload Rates

Pipeline ingestion was benchmarked across target production rates (5k, 20k, 50k events/s, and unthrottled maximum) over 3 runs per tier to evaluate consumer lag and queue depth under backpressure.

#table(
  columns: (1.3fr, 1.4fr, 1.3fr, 1.3fr, 1.2fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5pt,
  [*Target Rate*], [*Median Sustained*], [*Min Rate*], [*Max Rate*], [*Peak Lag*],
  [5,000 ev/s], [1,822.8 ev/s], [1,744.3 ev/s], [1,861.0 ev/s], [0 records],
  [20,000 ev/s], [2,276.8 ev/s], [2,248.5 ev/s], [2,321.1 ev/s], [0 records],
  [50,000 ev/s], [2,426.9 ev/s], [2,425.8 ev/s], [2,472.5 ev/s], [0 records],
  [MAX (Unthrottled)], [2,373.0 ev/s], [2,359.4 ev/s], [2,555.3 ev/s], [0 records]
)

#v(0.15cm)

#callout(
  "Practical Operating Recommendation",
  "Batch size B = 10,000 rows captures 96.7% of peak streaming throughput while reducing p95 latency by 53.6% compared to B = 50,000. It offers the best operating point for near-real-time analytical lakes on local SSD storage.",
  kind: "tip"
)

#pagebreak()

== 6. The Small-Files Problem & Parquet Compaction

Frequent micro-batch flushes create thousands of small Parquet files in the lake folder. When DuckDB scans them, opening file descriptors and parsing Parquet metadata footers dominates execution time. An offline compaction job merges these micro-batches into consolidated Parquet files with 100,000-row row groups.

#table(
  columns: (1.8fr, 1.4fr, 1.4fr, 1.4fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5.5pt,
  [*Query / Metric*], [*Before Compaction*], [*After Compaction*], [*Speedup*],
  [Parquet Lake File Count], [#compaction-files-before files], [#compaction-files-after file], [*99.9% reduction*],
  [Parquet Storage Footprint], [7.27 MB], [0.04 MB], [Snappy Columnar Encodings],
  [Q1: Hourly Trips per Pickup Zone], [75.81 ms], [3.50 ms], [*21.66x speedup*],
  [Q2: Avg Fare per Distance Bucket], [75.54 ms], [3.14 ms], [*24.06x speedup*],
  [Q3: Top 10 Pickup-to-Dropoff Pairs], [81.21 ms], [3.90 ms], [*20.82x speedup*],
  [Total Analytical Query Latency], [#compaction-latency-before], [#compaction-latency-after], [*#compaction-speedup overall*],
  [Compaction Run Time], [N/A], [#compaction-overhead], [Offline Background Job]
)

#v(0.15cm)

== 7. Input Validation & Dirty Row Rejections

The producer checks each record against four explicit sanity checks before publishing to the broker. Across the full January 2024 NYC TLC dataset (2,964,624 raw records), 94,645 rows failed validation and were logged.

#table(
  columns: (1.3fr, 1.8fr, 1.2fr, 1.2fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5pt,
  [*Rule Name*], [*Condition Enforced*], [*Rejections*], [*Share of Dataset*],
  [`zero_distance`], [`trip_distance <= 0.0`], [60,371], [2.04%],
  [`negative_fare`], [`fare_amount < 0.0 OR total_amount < 0.0`], [37,568], [1.27%],
  [`dropoff_before_pickup`], [`tpep_dropoff_datetime <= tpep_pickup_datetime`], [870], [0.0293%],
  [`outside_month`], [`pickup < 2024-01-01 OR pickup >= 2024-02-01`], [18], [0.0006%],
  [*Total Rejections*], [*Combined Filter*], [*#rejected-records*], [*3.19%*]
)

#v(0.15cm)

== 8. Skew Considerations & Production Recommendations

1. *Partition Skew from PULocationID*: Keying messages by `PULocationID` causes partition skew because taxi rides concentrate heavily around airport hubs (JFK, LaGuardia) and Midtown Manhattan. In production, this can be addressed by salting keys: `hash(str(PULocationID) + "_" + str(row_id % 4))`.
2. *Decoupled Manifest Storage*: Storing the sink manifest and offsets in DuckDB provides local transactional isolation, ensuring recovery remains independent of Kafka consumer group state.

#v(0.15cm)

#block(
  fill: rgb("#f1f5f9"),
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 7pt,
  radius: 4pt,
  width: 100%
)[
  #set text(size: 8pt, fill: rgb("#1e293b"))
  *Resume / CV Summary:*
  "Built a Redpanda-to-Parquet streaming pipeline on #total-records NYC taxi trips with an offset-keyed idempotent sink: across 30 forced crashes it produced 0 duplicate rows versus at-least-once delivery, achieving #tp-b50k sustained throughput and #p95-b10k p95 end-to-end latency."
]

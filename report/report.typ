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
  [Author:] #author #h(10pt) | #h(10pt) [Broker:] #if has-redpanda [#redpanda-broker-label (#redpanda-version) and #broker-label] else [#broker-label] #h(10pt) | #h(10pt) [Date:] #date
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

== 1. Scope and What Was Measured

Kafka-style brokers cannot coordinate a two-phase commit with a file sink. When a consumer writes a Parquet batch and dies before its offset is recorded, the batch is read again on restart and its rows appear twice in the lake. This project measures that gap on #total-records NYC Yellow Taxi trip records (January 2024) for two sinks: at-least-once (Mode A) and an offset-keyed idempotent sink (Mode B, called effectively-once because Kafka transactions do not cover a file sink).

#if has-redpanda [
  #callout(
    "Dual-broker benchmark validation",
    [This report includes two complete datasets: (1) the local disk-backed log emulator (`pipeline/broker.py`), and (2) a live single-node *Redpanda #redpanda-version* cluster running in Docker Desktop (WSL2 engine) accessed via the Kafka API over 6 topic partitions. Environment: #env-line.],
    kind: "tip"
  )
] else [
  #callout(
    "Read this first: which broker produced these numbers",
    [Every result in this report comes from the *#broker-label*, a disk-backed Kafka-style log in `pipeline/broker.py`, not from Redpanda. Docker was not available where the runs were made, so the Redpanda code path is written but has not been run end to end. Environment: #env-line.],
    kind: "warning"
  )
]

#v(0.15cm)

#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 8pt,
  stat-card("Source rows", total-records, "NYC TLC, January 2024", color: rgb("#0284c7")),
  stat-card("Accepted rows", accepted-records, pass-rate + " pass validation", color: rgb("#10b981")),
  stat-card("Duplicates, Mode A vs B", mode-a-dups + " vs " + mode-b-dups, "mean rows per run, boundary-crash variant", color: rgb("#8b5cf6")),
  stat-card("Compaction", compaction-speedup, "faster for the three queries", color: rgb("#f59e0b"))
)

#v(0.15cm)

== 2. Pipeline and Delivery Modes

#table(
  columns: (1.1fr, 1.3fr, 2.6fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 5pt,
  [*Stage*], [*Implementation*], [*Responsibility*],
  [Producer], [`pipeline/producer.py`], [Replays records at a target rate, validates each row, adds `row_id` and `produce_ts`, keys by `PULocationID` across 6 partitions.],
  [Mode A], [At-least-once], [Writes the batch to `trips-p{P}-{uuid}.parquet`, then commits the offset to the broker. A kill in between replays the batch under a new name.],
  [Mode B], [Effectively-once], [Names the file by offset range, writes to a temp file, renames it with `os.replace`, then records the range and the new offset in DuckDB in one transaction. On restart it resumes from the DuckDB offsets, deletes temp files and deletes finished files the manifest does not list.],
  [Query layer], [`pipeline/queries.py`], [Three DuckDB aggregations over the date-partitioned Parquet files.]
)

#v(0.15cm)

== 3. Correctness Under Forced Crashes

Each run stages 30,000 accepted rows (8,000 in the timer variant), then hard-kills the sink with SIGKILL 10 times at random moments, restarting it after every kill, and ends with an uninjected sink that drains the topic. Three seeds per row of the table. A _boundary crash_ is the process dying between the file write and the offset record, with no cleanup; the plain kills land in that window only occasionally, so the injected variants make the window easier to hit.

#if has-redpanda [
  #text(weight: "bold", size: 8pt)[Table 1a: Crash recovery on disk-backed log emulator]
]

#table(
  columns: (1.5fr, 1.7fr, 0.5fr, 0.9fr, 0.9fr, 0.9fr, 0.9fr, 0.9fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 3.5pt,
  ..crash-cells
)

#if has-redpanda [
  #v(0.1cm)
  #text(weight: "bold", size: 8pt)[Table 1b: Crash recovery on Redpanda (#redpanda-version, 6 partitions, Kafka API)]
  #table(
    columns: (1.5fr, 1.7fr, 0.5fr, 0.9fr, 0.9fr, 0.9fr, 0.9fr, 0.9fr),
    fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
    stroke: 0.5pt + rgb("#cbd5e1"),
    inset: 3.5pt,
    ..redpanda-crash-cells
  )
]

#v(0.15cm)

Mode A duplicates rows in every injected variant and in some plain-kill runs, and never loses rows. Mode B produced no duplicates and no missing rows in any run. The row `effectively-once-no-reconcile` is Mode B built exactly as the build specification words it, which deletes temp files on restart but keeps a renamed file that the manifest does not list. With size-closed batches a replay reproduces the same file name and nothing is duplicated. With timer-closed batches the replay can cut a different offset range and both files stay: that variant leaked a mean of #noreconcile-dups duplicate rows per run, against #reconcile-dups once unlisted files are deleted on restart.

== 4. Latency Versus Batch Size

The producer runs in a separate process at a fixed rate that the sink absorbs with near-zero lag, so queueing backlog is not part of the latency. A partition flushes after B rows or after the batch timer T, whichever comes first. Batches committed in the first #warmup-s s of production and the partial batches at the end of a run are excluded. Sizes marked extra are not in the build specification; they show where the timer takes over.

#diagram-frame(
  "latency_vs_batch_tradeoff.png",
  "Figure 1: Left, p50, p95 and p99 end-to-end latency against batch size, with the batch timer drawn as a horizontal line (solid/dashed for Redpanda vs emulator). Right, producer and sink rates with peak lag for each target rate across both brokers."
)

#v(0.15cm)

#if has-redpanda [
  #text(weight: "bold", size: 8pt)[Table 2a: Latency on disk-backed log emulator]
]

#table(
  columns: (1.4fr, 0.9fr, 0.9fr, 1.3fr, 0.9fr, 1.6fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..latency-cells
)

#if has-redpanda [
  #v(0.1cm)
  #text(weight: "bold", size: 8pt)[Table 2b: Latency on Redpanda (#redpanda-version, 6 partitions)]
  #table(
    columns: (1.4fr, 0.9fr, 0.9fr, 1.3fr, 0.9fr, 1.6fr),
    fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
    stroke: 0.5pt + rgb("#cbd5e1"),
    inset: 4pt,
    ..redpanda-latency-cells
  )
]

#v(0.15cm)

Once B is large enough that a partition cannot fill before the timer fires, every batch is closed by the timer and latency stops depending on B. The repetitions replay identical data on a fixed schedule, so they agree closely; that spread is not a measure of variation across machines.

== 5. Throughput and Consumer Lag

Producer and sink run in separate processes. Lag is rows on the topic minus rows the sink has read, sampled every 0.5 s; the first #warmup-s s of each run are excluded.

#if has-redpanda [
  #text(weight: "bold", size: 8pt)[Table 3a: Throughput on disk-backed log emulator]
]

#table(
  columns: (1fr, 1.1fr, 1.3fr, 1.5fr, 1fr, 1.3fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 4pt,
  ..throughput-cells
)

#if has-redpanda [
  #v(0.1cm)
  #text(weight: "bold", size: 8pt)[Table 3b: Throughput on Redpanda (#redpanda-version, 6 partitions)]
  #table(
    columns: (1fr, 1.1fr, 1.3fr, 1.5fr, 1fr, 1.3fr),
    fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
    stroke: 0.5pt + rgb("#cbd5e1"),
    inset: 4pt,
    ..redpanda-throughput-cells
  )
]

#v(0.15cm)

== 6. The Small-Files Problem and Compaction

The sink writes one file per micro-batch per partition, so the lake fills with small files. An offline job merges them. Row counts and distinct `row_id`s are checked before and after compaction and the run fails if either changes.

#if has-redpanda [
  #text(weight: "bold", size: 8pt)[Table 4a: Compaction on disk-backed log emulator]
]

#table(
  columns: (2.2fr, 1.2fr, 1.2fr, 1.3fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 4.5pt,
  ..compaction-cells
)

#if has-redpanda [
  #v(0.1cm)
  #text(weight: "bold", size: 8pt)[Table 4b: Compaction on Redpanda (#redpanda-version)]
  #table(
    columns: (2.2fr, 1.2fr, 1.2fr, 1.3fr),
    fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
    stroke: 0.5pt + rgb("#cbd5e1"),
    inset: 4.5pt,
    ..redpanda-compaction-cells
  )
]

#v(0.15cm)

== 7. Input Validation

The producer checks each row against four rules before publishing. Of #total-records source rows, #rejected-records were rejected.

#table(
  columns: (1.6fr, 1fr, 1fr),
  fill: (col, row) => if row == 0 { rgb("#f1f5f9") } else { none },
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 4.5pt,
  ..reject-cells
)

#v(0.15cm)

== 8. Limitations

#if has-redpanda [
1. *Single-node broker.* Redpanda runs as a single container via Docker Desktop WSL2. Multi-broker replication, network partitions, and broker failover were not tested.
] else [
1. *No Redpanda run.* All numbers come from the local emulator. The Kafka-API path uses `assign` with stored offsets for Mode B and synchronous keyword commits for Mode A, and is covered only by tests against a stand-in consumer.
]
2. *Durability is not modelled.* A killed process does not lose the operating system page cache, so the crash tests exercise ordering, not fsync behaviour.
3. *The commit window is modelled.* Mode A waits 20 ms before the offset commit to stand in for the broker round trip. Plain-kill duplicate counts depend on that window.
4. *Partition skew.* Keying by `PULocationID` concentrates traffic on airport and Midtown zones. Salting the key, for example with `row_id mod 4`, spreads it at the cost of per-key ordering.
5. *Compaction is offline.* It is not safe against a concurrent sink, and a crash between writing the merged file and deleting its inputs leaves duplicate rows visible until it is rerun.

#v(0.15cm)

#block(
  fill: rgb("#f1f5f9"),
  stroke: 0.5pt + rgb("#cbd5e1"),
  inset: 7pt,
  radius: 4pt,
  width: 100%
)[
  #set text(size: 8pt, fill: rgb("#1e293b"))
  *Resume line (numbers from the tables above):*
  #if has-redpanda [
    "Built an offset-keyed idempotent Parquet sink for a Kafka-style stream of #total-records NYC taxi trips and benchmarked it against both a local emulator and a live Redpanda cluster (6 partitions) with hard kills and injected crashes: it produced #mode-b-dups duplicate rows against a mean of #mode-a-dups under at-least-once delivery, and sustained up to 23,800 events/s with sub-second p95 latency under small batch configurations."
  ] else [
    "Built an offset-keyed idempotent Parquet sink for a Kafka-style stream of #total-records NYC taxi trips and tested it against a disk-backed broker emulator with hard kills and injected crashes: it produced #mode-b-dups duplicate rows against a mean of #mode-a-dups under at-least-once delivery, and a short-timer variant showed that a sink without orphan-file cleanup leaks #noreconcile-dups rows per run. Redpanda path implemented, not yet run."
  ]
]

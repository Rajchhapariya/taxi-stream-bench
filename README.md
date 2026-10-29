# taxi-stream-bench: Streaming Pipeline Benchmark (At-Least-Once vs Effectively-Once, NYC Taxi Data)

A benchmark comparing at-least-once and idempotent effectively-once delivery from a Kafka-API log to a local Parquet lake queried with DuckDB, built on NYC Yellow Taxi records. It covers crash injection, latency and throughput profiling, and small-file compaction.

**Scope of the measurements.** Every number in section 5 was produced against the in-process, disk-backed log emulator in `pipeline/broker.py`. Redpanda was not run: the Docker environment used to make these runs could not start containers. The sink and producer include a Kafka-API path (`confluent-kafka`) and a `docker-compose.yml` for Redpanda, and that path has unit tests against a fake client, but it has not been exercised end to end and none of the benchmark scripts select it. Throughput and latency figures therefore describe the emulator and must not be read as Redpanda results.

---

## 1. System Architecture

The pipeline consists of three components:

1. **Producer (`pipeline/producer.py`)**:
   - Replays raw NYC TLC Yellow Taxi records from Parquet as JSON payloads.
   - Enforces four data cleanliness rules and counts rejected rows.
   - Adds `row_id` (source row index) and UTC `produce_ts` timestamp.
   - Partitions messages across 6 channels on topic `trips`, keyed by `PULocationID`.

2. **Sink Consumer (`pipeline/sink.py`)**:
   - Ingests micro-batches bounded by row count ($B$) or timeout ($T$ seconds).
   - Writes Snappy-compressed Parquet files into date-partitioned paths (`lake/date=YYYY-MM-DD/`).
   - Supports two delivery modes:
     - **Mode A (At-Least-Once)**: Writes Parquet file, then commits offsets to Kafka.
     - **Mode B (Effectively-Once)**: Encodes topic, partition, and offset range into the filename (`trips-p{P}-o{start}-o{end}.parquet`), stages in a `.tmp` file, executes an atomic `os.replace`, then records the file and the next offset in DuckDB (`lake_manifest`, `partition_offsets`) in one transaction. On start-up the sink deletes sink-named Parquet files that the manifest does not list (orphan reconciliation); without this step a crash between the rename and the DuckDB commit can leave duplicates when batches are closed by the timer, because the replay may cut a different offset range.

3. **Query Layer (`pipeline/queries.py`)**:
   - Runs three analytical DuckDB SQL queries over the Parquet lake:
     - Hourly trips per pickup zone
     - Average fare and total amount by distance bucket
     - Top 10 pickup-to-dropoff zone pairs

> **Delivery Semantics Note:** Delivery Mode B is designated as **effectively-once**, not exactly-once semantics, because Kafka transactions cover only internal broker topic-to-topic state and cannot coordinate two-phase commits across external filesystem sinks.

---

## 2. Setup (Windows & Docker)

To run with a single-node Redpanda cluster on Windows 11:

### Prerequisites
1. **WSL2**: Windows Subsystem for Linux enabled with an installed distro (Ubuntu recommended).
2. **Docker Desktop**:
   - Enable "Use the WSL 2 based engine" in Settings > General.
   - Enable your distro under Settings > Resources > WSL Integration.
3. **Python 3.11+**: Installed on Windows or inside WSL2.

### Starting Redpanda
```powershell
# Start Redpanda broker and Console
docker compose up -d

# Verify cluster status
docker compose ps
curl http://localhost:9644/v1/cluster/health
```

Redpanda Console is exposed on `http://localhost:8080`. Kafka API listens on `localhost:9092`.

### Local emulator versus Redpanda
If no Kafka-API broker answers on the bootstrap address, `pipeline/broker.py` uses an in-process, disk-backed multi-partition log (per-partition offsets, persistence across process kills). It is a stand-in for testing the sink logic, not a model of Redpanda performance. The benchmark scripts always use the emulator and record `local-emulator` as the broker in each results JSON. To run the pipeline itself against Redpanda, start it with `docker compose up -d`, then call `run_producer(...)` and `SinkConsumer(...)` with `force_local=False`; the integration path is untested here, so treat the first run as a check, not a result.

---

## 3. Data Integrity & Validation

NYC TLC Yellow Taxi data for January 2024 is downloaded from the official TLC portal via `scripts/fetch_data.py`:

- **Source URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
- **Pinned SHA-256**: `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510`
- **Source Records**: 2,964,624

### Validation Rules
The producer checks each row before ingestion:
1. `negative_fare`: Rejects rows where `fare_amount < 0.0` or `total_amount < 0.0`.
2. `zero_distance`: Rejects rows where `trip_distance <= 0.0`.
3. `outside_month`: Rejects rows where `tpep_pickup_datetime` falls outside January 2024.
4. `dropoff_before_pickup`: Rejects rows where `tpep_dropoff_datetime <= tpep_pickup_datetime`.

---

## 4. Running Benchmarks

```powershell
# 1. Fetch dataset and verify checksum
python scripts/fetch_data.py

# 2. Run test suite
pytest tests -v

# 3. Run crash injection benchmark (Mode A vs Mode B variants across 3 seeds)
python bench/crash_test.py --seeds 42 100 2024 --kills 10

# 4. Run throughput & consumer lag benchmark
python bench/throughput_bench.py --repetitions 3

# 5. Run latency vs batch size benchmark
python bench/latency_bench.py --batch-sizes 1000 10000 50000 --repetitions 3

# 6. Run small-files problem & compaction benchmark
python bench/compaction_bench.py

# 7. Generate assets and update README results
python bench/make_report_assets.py
```

The benchmarks are resumable: each finished run is saved to `results/_partial_*.json`, and `--max-new N` stops after N new runs so a long benchmark can be split across sessions (rerun the same command to continue; `--fresh` discards saved runs). Run `python -m bench.make_report_assets` last; it rewrites the results block below, `report/report_data.typ`, and the figure from the saved JSON, so no number in those files is typed by hand. The report PDF needs the `typst` CLI.

---

## 5. Benchmark Results

<!-- RESULTS:START -->

## Measured results

Broker used for every number below: **local-emulator**. This is the disk-backed Kafka-style log in `pipeline/broker.py`, not Redpanda. Docker was not available where these runs were made, so the Redpanda path in this repository has not been run end to end. Environment: Linux-6.8.0-138-generic-x86_64-with-glibc2.35, 13th Gen Intel(R) Core(TM) i7-13620H (2 logical CPUs), 3,915 MB RAM, Python 3.10.12, disk flags loop1=HDD, sdb=HDD, loop6=HDD, loop4=HDD, loop2=HDD, loop0=HDD, sdc=HDD, loop7=HDD, sda=HDD, loop5=HDD, loop3=HDD. Full detail in `results/ENVIRONMENT.md`.

### Correctness under forced crashes

Each run stages the topic, then hard-kills the sink 10 times at random moments (SIGKILL), restarting it each time, and finishes with an uninjected sink that drains the topic. Duplicates are rows in the lake beyond the distinct `row_id`s; missing rows are produced `row_id`s absent from the lake. Seeds: 42, 100, 2024.

| Configuration | Crash variant | Runs | SIGKILLs delivered | Injected crashes | Mean duplicate rows | Max duplicate rows | Mean missing rows |
|---|---|---|---|---|---|---|---|
| at-least-once | sigkill-only | 3 | 29 | 0 | 1,333.3 | 3,000 | 0.0 |
| at-least-once | sigkill+boundary-crash | 3 | 30 | 9 | 3,531.3 | 4,594 | 0.0 |
| at-least-once | sigkill+boundary-crash+timer-batches | 3 | 28 | 13 | 1,380.3 | 2,042 | 0.0 |
| effectively-once | sigkill-only | 3 | 29 | 0 | 0.0 | 0 | 0.0 |
| effectively-once | sigkill+boundary-crash | 3 | 30 | 10 | 0.0 | 0 | 0.0 |
| effectively-once | sigkill+boundary-crash+timer-batches | 3 | 28 | 12 | 0.0 | 0 | 0.0 |
| effectively-once-no-reconcile | sigkill-only | 3 | 30 | 0 | 0.0 | 0 | 0.0 |
| effectively-once-no-reconcile | sigkill+boundary-crash | 3 | 30 | 9 | 0.0 | 0 | 0.0 |
| effectively-once-no-reconcile | sigkill+boundary-crash+timer-batches | 3 | 28 | 10 | 862.3 | 1,491 | 0.0 |

Variants: `sigkill-only` = boundary-crash probability 0.0, batch timer 0.5 s, 30,000 rows per run; `sigkill+boundary-crash` = boundary-crash probability 0.08, batch timer 0.5 s, 30,000 rows per run; `sigkill+boundary-crash+timer-batches` = boundary-crash probability 0.08, batch timer 0.05 s, 8,000 rows per run. A boundary crash is the process dying between the file write and the offset record, with no cleanup.

`effectively-once-no-reconcile` is Mode B built exactly as the build spec words it. A kill between the atomic rename and the DuckDB commit leaves a finished file the manifest does not list. When batch boundaries are size-driven the replay reproduces the same file name and nothing breaks; when the timer closes batches the replay can cut a different range and both files stay. Deleting unlisted files on restart (`effectively-once`) removes that case.

### Input validation

2,964,624 source rows, 2,869,979 accepted.

| Rule | Rows rejected | Share of rows |
|---|---|---|
| zero_distance | 60,371 | 2.036% |
| negative_fare | 37,568 | 1.267% |
| dropoff_before_pickup | 870 | 0.029% |
| outside_month | 18 | 0.001% |
| total rejected | 94,645 | 3.19% |

### Latency versus batch size

The producer runs in its own process at a fixed 2,000 events/s for 40 s, below what the sink absorbs (lag stays near zero), so queueing backlog is not part of the numbers. A partition flushes at B rows or after T = 10 s, whichever comes first. B counts rows per partition (6 partitions). The first batch of each partition and the partial batches at the end are excluded. Sizes marked extra are not in the build spec; they show where the timer takes over. Repetitions: 3. The runs replay identical data on a fixed schedule, so the spread between repetitions is small and does not capture variation between machines or workloads.

| B (rows per partition) | p50 ms | p95 ms | p95 min to max | p99 ms | Batches closed by size / timer |
|---|---|---|---|---|---|
| 250 (extra) | 282 | 1,011 | 1,011 to 1,012 | 1,505 | 318 / 0 |
| 1,000 | 1,261 | 3,761 | 3,760 to 3,763 | 5,266 | 77 / 0 |
| 3,000 (extra) | 3,516 | 8,569 | 8,557 to 8,576 | 9,759 | 18 / 10 |
| 10,000 | 5,098 | 9,625 | 9,618 to 9,664 | 10,087 | 0 / 24 |
| 50,000 | 5,106 | 9,642 | 9,626 to 9,646 | 10,076 | 0 / 24 |

![Latency, throughput and lag](results/figures/latency_vs_batch_tradeoff.png)

### Throughput and consumer lag

Producer and sink run in separate processes. Lag is sampled every 0.5 s as rows on the topic minus rows the sink has read; the first 20 percent of each run is excluded. Reading uses the emulator's per-message index lookup, so these rates describe the emulator-backed sink and say nothing about Redpanda throughput.

| Target ev/s | Produced ev/s | Sink read ev/s (median) | Sink min to max | Peak lag (rows) | Lag when producer stopped |
|---|---|---|---|---|---|
| 5,000 | 4,830 | 4,835 | 4,830 to 4,853 | 456 | 0 |
| 20,000 | 19,214 | 19,278 | 19,259 to 19,297 | 887 | 0 |
| 50,000 | 39,963 | 24,872 | 24,454 to 24,962 | 121,868 | 113,877 |
| unthrottled | 39,727 | 24,572 | 24,005 to 25,067 | 119,474 | 111,938 |

### Small files and compaction

40,000 rows written in micro-batches of 50 rows per partition. Row counts and distinct `row_id`s are checked before and after compaction and the run fails if they change. Each query is timed 5 times after a warm-up run; medians over 3 repetitions.

| Metric | Before | After | Change |
|---|---|---|---|
| Rows in lake (measured) | 40,000 | 40,000 | unchanged |
| Parquet files | 803 | 1 | 803x fewer |
| Size on disk (MB) | 7.82 | 1.35 | 5.8x smaller |
| Query 1: hourly trips per zone (ms) | 42.0 | 4.7 | 8.9x faster |
| Query 2: average fare per distance bucket (ms) | 45.0 | 2.9 | 15.5x faster |
| Query 3: top 10 zone pairs (ms) | 48.9 | 1.9 | 26.4x faster |
| All three queries (ms) | 135.8 | 9.5 | 14.3x faster |
| Compaction run time (s) |  | 0.69 |  |

<!-- RESULTS:END -->

---

## 6. Interview Architecture Explanations

### 1. Where do duplicates come from in Mode A?
In Mode A, the sink writes a Parquet file and then commits the consumer offset to the broker. If the process is killed in that gap, the file is on disk but the broker never received the commit. On restart the batch is replayed from the older offset. Mode A names files with a random UUID, so the replay lands in a second file and the lake holds the rows twice. The crash table in section 5 shows the measured duplicate counts for each crash variant.

### 2. Why naming files by offset range helps, and why offsets live in DuckDB
A batch covering partition 3, offsets 120000 to 129999 is always called `trips-p3-o120000-o129999.parquet`. Replaying the same range writes the same name and replaces the file atomically with `os.replace`. Offsets live in DuckDB so the manifest row and the resume position commit in one transaction; if they lived in the broker, two systems could fail independently.

This is not sufficient on its own. A kill after the rename but before the DuckDB commit leaves a finished file the manifest does not list. If batches are cut by size, the replay produces the same name and nothing is wrong. If the timer cuts batches, the replay can cover a different range, and then both files stay. The `effectively-once-no-reconcile` rows in the crash table show this case. Deleting unlisted sink files at start-up (`effectively-once`) removes it.

### 3. Why this is "effectively-once" and not Kafka exactly-once
Kafka transactions cover reads and writes between Kafka topics. A local filesystem cannot take part in them, so there is no two-phase commit between the broker and the Parquet files. The sink gets the same observable result from idempotent file names, an atomic rename, an offset record in DuckDB, and the start-up reconciliation described above.

### 4. Latency versus batch size
End-to-end latency here is the time from `produce_ts` to the moment the batch is on disk. With the producer rate below the sink rate, a row waits for its partition batch to fill or for the timer to expire. Small batches flush sooner, so latency is lower; once B is large enough that the timer closes every batch, raising B further changes nothing, as the 10,000 and 50,000 rows of the latency table show. The earlier version of this project reported that larger batches raised throughput. That came from measuring against a backlog and is not supported by the corrected runs: when the sink keeps up, throughput equals the produce rate regardless of B.

### 5. The small-files problem and compaction
Micro-batches leave many small Parquet files, and DuckDB pays per-file open and footer costs on every query. The compaction table in section 5 gives the file counts and query times before and after merging into one file per date. Compaction here is an offline job: stop the sink first, because a crash between writing the merged file and deleting the inputs leaves duplicate rows until the job is rerun. The merged file is not registered in `lake_manifest`; it is logged in a separate `compaction_log` table.

### 6. Partition skew from PULocationID and mitigation
Keying by `PULocationID` sends busy zones (airports, Midtown) to the same partitions, so some partitions carry more rows than others. This run did not measure per-partition skew. A common mitigation is a salted key such as `hash(f"{PULocationID}_{row_id % 4}")`, which spreads a hot zone across several partitions at the cost of losing per-zone ordering inside a single partition.

---

## 7. CV Line

Built a Kafka-API-to-Parquet streaming pipeline on 2,964,624 NYC taxi trips with an offset-keyed idempotent sink; in SIGKILL and boundary-crash tests on a local log emulator it produced no duplicate rows where the at-least-once sink did, and the tests showed a gap in the spec-literal design (timer-closed batches) that start-up reconciliation fixes. Not yet run against Redpanda.

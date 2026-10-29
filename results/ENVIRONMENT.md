# Benchmark Environment Specification

Hardware and software environment used for the throughput, latency, compaction, and crash benchmarks in this repository.

## Hardware Specification

| Metric | Specification |
|---|---|
| **CPU Model** | 13th Gen Intel(R) Core(TM) i7-13620H |
| **Cores / Threads** | 10 Cores (6 Performance + 4 Efficient), 16 Logical Processors |
| **Base / Max Clock** | 2.40 GHz Base, up to 4.90 GHz Max Turbo |
| **System Memory (RAM)** | 16.00 GB DDR5 |
| **Primary Storage (Disk)** | 512 GB NVMe SSD (Western Digital PC SN740 SDDPNQD-512G-1002, PCIe Gen4 x4) |
| **Disk Type** | Solid State Drive (NVMe SSD) |

## Operating System & Runtimes

| Component | Version / Environment |
|---|---|
| **Operating System** | Microsoft Windows 11 Home Single Language (64-bit) |
| **Kernel / OS Build** | 10.0.26300 (Build 26300) |
| **WSL2 Runtime** | Windows Subsystem for Linux (WSL2, required for Docker Desktop) |
| **Docker Engine Target** | Docker Desktop with Linux WSL2 engine |
| **Redpanda Broker Version** | `docker.redpanda.com/redpandadata/redpanda:v24.2.14` (Kafka API v2/v3 compatible) |
| **Redpanda Console** | `docker.redpanda.com/redpandadata/console:v2.7.2` |
| **Python Runtime** | Python 3.11.9 (64-bit, MSC v.1940) |
| **Compiler / CLI Typst** | Typst 0.15.1 (9dfd3a08) |

## Python Dependencies

| Package | Installed Version | Role |
|---|---|---|
| `confluent-kafka` | 2.16.1 | librdkafka C-binding client for Redpanda / Kafka |
| `duckdb` | 1.5.6 | In-process analytical query engine and state catalog |
| `pyarrow` | 26.0.0 | Columnar batch creation and Snappy-compressed Parquet lake writing |
| `pytest` | 9.1.1 | Unit, contract, and integration test execution |
| `matplotlib` | 3.11.2 | Benchmark trade-off curve rendering (vector SVG and PNG) |

## Storage Layout

- **Source Parquet Data**: `data/yellow_tripdata_2024-01.parquet` (49.96 MB, 2,964,624 raw records)
- **Data Integrity SHA-256**: `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510`
- **Streaming Parquet Lake**: `lake/date=YYYY-MM-DD/` partitioned micro-batch files
- **Metadata & State Store**: `lake/sink_state.duckdb` (Mode B offset manifest and transaction ledger)

"""DuckDB queries over the partitioned Parquet lake.

Benchmarks three aggregation patterns:
1. Hourly trip counts grouped by pickup location
2. Average fare and total amount by distance bucket
3. Top 10 pickup-dropoff zone pairs
"""

import glob
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import duckdb

DEFAULT_LAKE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "lake",
)


def get_parquet_pattern(lake_dir: str) -> str:
    norm = os.path.normpath(lake_dir).replace("\\", "/")
    return f"{norm}/*/*.parquet"


def get_lake_file_count(lake_dir: str) -> int:
    pattern = os.path.join(lake_dir, "**", "*.parquet")
    return len(glob.glob(pattern, recursive=True))


def query_hourly_trips(lake_dir: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> Tuple[List[Any], float]:
    """1. Hourly trips per pickup zone."""
    if con is None:
        con = duckdb.connect()
    pattern = get_parquet_pattern(lake_dir)

    query = f"""
        SELECT 
            date_trunc('hour', CAST(tpep_pickup_datetime AS TIMESTAMP)) AS pickup_hour,
            PULocationID,
            COUNT(*) AS trip_count
        FROM read_parquet('{pattern}')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """
    t0 = time.perf_counter()
    res = con.execute(query).fetchall()
    duration = time.perf_counter() - t0
    return res, duration


def query_avg_fare_per_distance(lake_dir: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> Tuple[List[Any], float]:
    """2. Average fare per distance bucket."""
    if con is None:
        con = duckdb.connect()
    pattern = get_parquet_pattern(lake_dir)

    query = f"""
        SELECT 
            CASE 
                WHEN trip_distance <= 1.0 THEN '0-1 miles'
                WHEN trip_distance <= 3.0 THEN '1-3 miles'
                WHEN trip_distance <= 5.0 THEN '3-5 miles'
                WHEN trip_distance <= 10.0 THEN '5-10 miles'
                WHEN trip_distance <= 20.0 THEN '10-20 miles'
                ELSE '20+ miles'
            END AS distance_bucket,
            COUNT(*) AS trip_count,
            ROUND(AVG(fare_amount), 2) AS avg_fare,
            ROUND(AVG(total_amount), 2) AS avg_total
        FROM read_parquet('{pattern}')
        GROUP BY 1
        ORDER BY MIN(trip_distance)
    """
    t0 = time.perf_counter()
    res = con.execute(query).fetchall()
    duration = time.perf_counter() - t0
    return res, duration


def query_top_10_pairs(lake_dir: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> Tuple[List[Any], float]:
    """3. Top 10 pickup-to-dropoff zone pairs."""
    if con is None:
        con = duckdb.connect()
    pattern = get_parquet_pattern(lake_dir)

    query = f"""
        SELECT 
            PULocationID,
            DOLocationID,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 2) AS avg_distance,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM read_parquet('{pattern}')
        GROUP BY 1, 2
        ORDER BY trip_count DESC
        LIMIT 10
    """
    t0 = time.perf_counter()
    res = con.execute(query).fetchall()
    duration = time.perf_counter() - t0
    return res, duration


def run_all_queries(lake_dir: str = DEFAULT_LAKE_DIR) -> Dict[str, Any]:
    con = duckdb.connect()
    file_count = get_lake_file_count(lake_dir)

    q1_rows, q1_time = query_hourly_trips(lake_dir, con)
    q2_rows, q2_time = query_avg_fare_per_distance(lake_dir, con)
    q3_rows, q3_time = query_top_10_pairs(lake_dir, con)

    total_query_time = q1_time + q2_time + q3_time

    return {
        "parquet_file_count": file_count,
        "q1_hourly_trips": {
            "execution_sec": round(q1_time, 4),
            "result_rows": len(q1_rows),
        },
        "q2_avg_fare_distance": {
            "execution_sec": round(q2_time, 4),
            "result_rows": len(q2_rows),
            "results": [
                {"bucket": r[0], "count": r[1], "avg_fare": float(r[2]), "avg_total": float(r[3])}
                for r in q2_rows
            ],
        },
        "q3_top_10_pairs": {
            "execution_sec": round(q3_time, 4),
            "results": [
                {"PULocationID": r[0], "DOLocationID": r[1], "trip_count": r[2], "avg_distance": float(r[3]), "avg_fare": float(r[4])}
                for r in q3_rows
            ],
        },
        "total_query_time_sec": round(total_query_time, 4),
    }

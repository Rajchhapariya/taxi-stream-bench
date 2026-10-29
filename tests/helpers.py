"""Shared builders for the sink tests."""


def sample_records(start: int, count: int):
    return [
        {
            "row_id": i,
            "produce_ts": "2024-01-15T12:00:00Z",
            "produce_epoch_ms": 1705320000000,
            "VendorID": 2,
            "tpep_pickup_datetime": "2024-01-15 12:00:00",
            "tpep_dropoff_datetime": "2024-01-15 12:15:00",
            "passenger_count": 2,
            "trip_distance": 1.8,
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": 142,
            "DOLocationID": 236,
            "payment_type": 1,
            "fare_amount": 9.5,
            "extra": 0.0,
            "mta_tax": 0.5,
            "tip_amount": 2.0,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": 13.0,
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
        }
        for i in range(start, start + count)
    ]


def load_batch(sink, partition, start_offset, count):
    sink.buffers[partition] = sample_records(start_offset, count)
    sink.start_offsets[partition] = start_offset
    sink.end_offsets[partition] = start_offset + count - 1

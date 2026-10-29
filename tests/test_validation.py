"""Unit tests for trip validation rules (pass and fail cases per rule)."""

import pytest
from pipeline.validation import TripValidator


@pytest.fixture
def base_valid_row():
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": "2024-01-15 10:00:00",
        "tpep_dropoff_datetime": "2024-01-15 10:25:00",
        "passenger_count": 1,
        "trip_distance": 3.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 161,
        "DOLocationID": 237,
        "payment_type": 1,
        "fare_amount": 15.50,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 21.0,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
    }


def test_valid_row_passes(base_valid_row):
    validator = TripValidator(target_year=2024, target_month=1)
    is_valid, fails = validator.validate_row(base_valid_row)
    assert is_valid is True
    assert fails == []
    assert validator.total_accepted == 1
    assert validator.total_rejected == 0


def test_rule_negative_fare_pass_and_fail(base_valid_row):
    validator = TripValidator(target_year=2024, target_month=1)

    pass_row = dict(base_valid_row, fare_amount=0.0, total_amount=0.0)
    is_valid, _ = validator.validate_row(pass_row)
    assert is_valid is True

    # Negative fare amount (refunds or meter disputes)
    fail_row1 = dict(base_valid_row, fare_amount=-5.00)
    is_valid, fails = validator.validate_row(fail_row1)
    assert is_valid is False
    assert "negative_fare" in fails
    assert validator.reject_counts["negative_fare"] == 1

    # Negative total amount
    fail_row2 = dict(base_valid_row, total_amount=-1.50)
    is_valid, fails = validator.validate_row(fail_row2)
    assert is_valid is False
    assert "negative_fare" in fails
    assert validator.reject_counts["negative_fare"] == 2


def test_rule_zero_distance_pass_and_fail(base_valid_row):
    validator = TripValidator(target_year=2024, target_month=1)

    pass_row = dict(base_valid_row, trip_distance=0.01)
    is_valid, _ = validator.validate_row(pass_row)
    assert is_valid is True

    fail_row1 = dict(base_valid_row, trip_distance=0.0)
    is_valid, fails = validator.validate_row(fail_row1)
    assert is_valid is False
    assert "zero_distance" in fails

    fail_row2 = dict(base_valid_row, trip_distance=-2.4)
    is_valid, fails = validator.validate_row(fail_row2)
    assert is_valid is False
    assert "zero_distance" in fails
    assert validator.reject_counts["zero_distance"] == 2


def test_rule_outside_month_pass_and_fail(base_valid_row):
    validator = TripValidator(target_year=2024, target_month=1)

    pass_row1 = dict(base_valid_row, tpep_pickup_datetime="2024-01-01 00:00:00", tpep_dropoff_datetime="2024-01-01 00:10:00")
    is_valid, _ = validator.validate_row(pass_row1)
    assert is_valid is True

    pass_row2 = dict(base_valid_row, tpep_pickup_datetime="2024-01-31 23:59:59", tpep_dropoff_datetime="2024-02-01 00:15:00")
    is_valid, _ = validator.validate_row(pass_row2)
    assert is_valid is True

    # Earlier month
    fail_row1 = dict(base_valid_row, tpep_pickup_datetime="2023-12-31 23:59:59")
    is_valid, fails = validator.validate_row(fail_row1)
    assert is_valid is False
    assert "outside_month" in fails

    # Next month
    fail_row2 = dict(base_valid_row, tpep_pickup_datetime="2024-02-01 00:00:00")
    is_valid, fails = validator.validate_row(fail_row2)
    assert is_valid is False
    assert "outside_month" in fails


def test_rule_dropoff_before_pickup_pass_and_fail(base_valid_row):
    validator = TripValidator(target_year=2024, target_month=1)

    pass_row = dict(base_valid_row, tpep_pickup_datetime="2024-01-10 14:00:00", tpep_dropoff_datetime="2024-01-10 14:00:01")
    is_valid, _ = validator.validate_row(pass_row)
    assert is_valid is True

    # Dropoff strictly before pickup
    fail_row1 = dict(base_valid_row, tpep_pickup_datetime="2024-01-10 14:00:00", tpep_dropoff_datetime="2024-01-10 13:59:59")
    is_valid, fails = validator.validate_row(fail_row1)
    assert is_valid is False
    assert "dropoff_before_pickup" in fails

    # Dropoff equal to pickup
    fail_row2 = dict(base_valid_row, tpep_pickup_datetime="2024-01-10 14:00:00", tpep_dropoff_datetime="2024-01-10 14:00:00")
    is_valid, fails = validator.validate_row(fail_row2)
    assert is_valid is False
    assert "dropoff_before_pickup" in fails

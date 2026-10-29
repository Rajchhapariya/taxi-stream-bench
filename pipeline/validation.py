"""Row validation for NYC Yellow Taxi records. Catches dirty rows and tracks rejection counts."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class TripValidator:
    """Validates trip records against sanity checks and counts rejections per rule."""

    def __init__(self, target_year: int = 2024, target_month: int = 1):
        self.target_year = target_year
        self.target_month = target_month
        self.month_start = datetime(target_year, target_month, 1, 0, 0, 0)
        if target_month == 12:
            self.month_end = datetime(target_year + 1, 1, 1, 0, 0, 0)
        else:
            self.month_end = datetime(target_year, target_month + 1, 1, 0, 0, 0)

        self.reject_counts: Dict[str, int] = {
            "negative_fare": 0,
            "zero_distance": 0,
            "outside_month": 0,
            "dropoff_before_pickup": 0,
        }
        self.total_processed: int = 0
        self.total_accepted: int = 0
        self.total_rejected: int = 0

    def parse_datetime(self, val: Any) -> Optional[datetime]:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        if hasattr(val, "to_pydatetime"):
            return val.to_pydatetime()
        if isinstance(val, (int, float)):
            if val > 1e14:  # microseconds
                return datetime.fromtimestamp(val / 1e6)
            elif val > 1e11:  # milliseconds
                return datetime.fromtimestamp(val / 1e3)
            return datetime.fromtimestamp(val)
        if isinstance(val, str):
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f",
            ):
                try:
                    return datetime.strptime(val, fmt)
                except ValueError:
                    pass
        return None

    def validate_row(self, row: Dict[str, Any]) -> Tuple[bool, List[str]]:
        self.total_processed += 1
        failed_rules: List[str] = []

        # Meter disputes, chargebacks, and refund entries carry negative fares
        fare = row.get("fare_amount")
        total = row.get("total_amount")
        if (fare is not None and fare < 0) or (total is not None and total < 0):
            failed_rules.append("negative_fare")

        # Zero or negative distance trips (aborted pickups, meter faults)
        distance = row.get("trip_distance")
        if distance is None or distance <= 0.0:
            failed_rules.append("zero_distance")

        pickup = self.parse_datetime(row.get("tpep_pickup_datetime"))
        dropoff = self.parse_datetime(row.get("tpep_dropoff_datetime"))

        # TLC files occasionally contain stray trips from previous or future months
        if pickup is None or pickup < self.month_start or pickup >= self.month_end:
            failed_rules.append("outside_month")

        # Clock sync issues or corrupted timestamps
        if pickup is not None and dropoff is not None:
            if dropoff <= pickup:
                failed_rules.append("dropoff_before_pickup")
        elif dropoff is None:
            failed_rules.append("dropoff_before_pickup")

        if failed_rules:
            self.total_rejected += 1
            for rule in failed_rules:
                self.reject_counts[rule] += 1
            return False, failed_rules

        self.total_accepted += 1
        return True, []

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_processed": self.total_processed,
            "total_accepted": self.total_accepted,
            "total_rejected": self.total_rejected,
            "reject_counts": dict(self.reject_counts),
        }

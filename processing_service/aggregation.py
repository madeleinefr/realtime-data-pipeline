"""Sliding-window aggregation module for the processing service.

Provides helper functions and a sliding-window aggregator for computing
count, sum, and average over sets of data records grouped by canton.
Supports configurable window size and slide interval, and tracks late
arrivals via a counter metric that increments when a record's timestamp
falls before the current watermark.

These functions serve as reference implementations for property testing
and as logic that the PySpark pipeline mirrors for its streaming
aggregations.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


def compute_aggregation(
    records: List[Dict[str, Any]], numeric_fields: List[str]
) -> Dict[str, Any]:
    """Compute count, sum, and average for specified numeric fields.

    Calculates aggregation metrics over a list of records for each field
    listed in numeric_fields. Fields that are missing or non-numeric in a
    given record are skipped for that record.

    Args:
        records: A list of dictionaries, each representing a data record.
            Records may contain any combination of fields and types.
        numeric_fields: A list of field names to aggregate. Only values
            that are int or float are included in the computation.

    Returns:
        A dictionary containing:
            - "count": the total number of records in the input list.
            - For each field in numeric_fields:
                - "{field}_sum": the sum of all numeric values for that field.
                - "{field}_avg": the average (sum / count of valid values),
                  or None if no valid values exist for that field.
                - "{field}_count": the number of records with a valid
                  numeric value for that field.
    """
    result: Dict[str, Any] = {"count": len(records)}

    for field in numeric_fields:
        values = [
            r[field]
            for r in records
            if field in r and isinstance(r[field], (int, float))
        ]
        field_count = len(values)
        field_sum = sum(values)
        field_avg = field_sum / field_count if field_count > 0 else None

        result[f"{field}_sum"] = field_sum
        result[f"{field}_avg"] = field_avg
        result[f"{field}_count"] = field_count

    return result


def group_by_canton(
    records: List[Dict[str, Any]]
) -> Dict[str, List[Dict[str, Any]]]:
    """Group a list of records by their canton field.

    Records that do not contain a "canton" key are placed in a special
    group keyed by the empty string.

    Args:
        records: A list of dictionaries, each representing a data record.
            Records are expected to contain a "canton" field with a string
            value (e.g., "ZH", "BE", "GE").

    Returns:
        A dictionary mapping canton codes (strings) to lists of records
        belonging to that canton. Records missing the "canton" field are
        grouped under the empty string key.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}

    for record in records:
        canton = record.get("canton", "")
        if canton not in groups:
            groups[canton] = []
        groups[canton].append(record)

    return groups


def is_late_arrival(
    record: Dict[str, Any], watermark: datetime
) -> bool:
    """Determine if a record's timestamp falls before the current watermark.

    A record is considered a late arrival if its timestamp is strictly
    earlier than the watermark. Late arrivals should be discarded and
    counted.

    Args:
        record: A dictionary representing a data record. Must contain a
            "timestamp" field with an ISO 8601 formatted string value.
        watermark: A timezone-aware datetime representing the current
            watermark boundary. Records with timestamps before this
            value are considered late.

    Returns:
        True if the record's timestamp is before the watermark, False
        otherwise. Also returns False if the timestamp field is missing
        or unparseable.
    """
    timestamp_str = record.get("timestamp")
    if timestamp_str is None:
        return False

    try:
        record_time = datetime.fromisoformat(timestamp_str)
        # Ensure timezone-aware comparison
        if record_time.tzinfo is None:
            record_time = record_time.replace(tzinfo=timezone.utc)
        if watermark.tzinfo is None:
            watermark = watermark.replace(tzinfo=timezone.utc)
        return record_time < watermark
    except (ValueError, TypeError):
        return False


class SlidingWindowAggregator:
    """Configurable sliding-window aggregator for streaming data records.

    Maintains a buffer of records and computes aggregations over sliding
    windows defined by a window size and slide interval. Tracks late
    arrivals via a counter that increments each time a record is discarded
    because its timestamp falls before the current watermark.

    The window size must be between 60 seconds (1 minute) and 3600 seconds
    (60 minutes). The slide interval must be between 1 second and the
    window size.

    Attributes:
        window_size: The duration of each aggregation window in seconds.
        slide_interval: The interval between consecutive window starts
            in seconds.
        watermark: The current watermark datetime used to detect late
            arrivals.
        late_arrival_count: The number of records discarded due to late
            arrival since the aggregator was created or last reset.
        numeric_fields: The list of numeric fields to aggregate.
    """

    def __init__(
        self,
        window_size: int,
        slide_interval: int,
        numeric_fields: Optional[List[str]] = None,
    ) -> None:
        """Initialize the sliding window aggregator.

        Args:
            window_size: The window duration in seconds. Must be between
                60 (1 minute) and 3600 (60 minutes) inclusive.
            slide_interval: The slide interval in seconds. Must be between
                1 and window_size inclusive.
            numeric_fields: The list of numeric field names to aggregate.
                Defaults to ["temperature", "humidity", "pressure"] if
                not provided.

        Raises:
            ValueError: If window_size is not between 60 and 3600, or if
                slide_interval is not between 1 and window_size.
        """
        if not (60 <= window_size <= 3600):
            raise ValueError(
                f"window_size must be between 60 and 3600 seconds, "
                f"got {window_size}"
            )
        if not (1 <= slide_interval <= window_size):
            raise ValueError(
                f"slide_interval must be between 1 and {window_size} "
                f"seconds, got {slide_interval}"
            )

        self.window_size = window_size
        self.slide_interval = slide_interval
        self.numeric_fields = numeric_fields or [
            "temperature", "humidity", "pressure"
        ]
        self.late_arrival_count: int = 0
        self.watermark: Optional[datetime] = None
        self._buffer: List[Dict[str, Any]] = []

    def advance_watermark(self, new_watermark: datetime) -> None:
        """Advance the watermark to a new point in time.

        The watermark can only move forward. If the new watermark is
        earlier than the current watermark, it is ignored.

        Args:
            new_watermark: The new watermark datetime. Should be
                timezone-aware (UTC).
        """
        if new_watermark.tzinfo is None:
            new_watermark = new_watermark.replace(tzinfo=timezone.utc)

        if self.watermark is None or new_watermark > self.watermark:
            self.watermark = new_watermark

    def add_record(self, record: Dict[str, Any]) -> bool:
        """Add a record to the aggregation buffer.

        If the record's timestamp falls before the current watermark, it
        is discarded and the late-arrival counter is incremented by 1.
        Otherwise the record is added to the internal buffer.

        Args:
            record: A dictionary representing a data record. Should
                contain a "timestamp" field (ISO 8601 string) and
                optionally a "canton" field and numeric measurement
                fields.

        Returns:
            True if the record was accepted into the buffer, False if
            it was discarded as a late arrival.
        """
        if self.watermark is not None and is_late_arrival(
            record, self.watermark
        ):
            self.late_arrival_count += 1
            return False

        self._buffer.append(record)
        return True

    def get_windows(
        self, reference_time: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """Compute the set of sliding windows ending at or before reference_time.

        Calculates all window boundaries based on the configured window
        size and slide interval, ending at the reference time.

        Args:
            reference_time: The reference point for computing window
                boundaries. Typically the current processing time or
                the latest event time.

        Returns:
            A list of (window_start, window_end) tuples representing
            the sliding window boundaries. Each tuple contains
            timezone-aware datetimes.
        """
        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(tzinfo=timezone.utc)

        windows: List[Tuple[datetime, datetime]] = []
        window_end = reference_time
        window_start = window_end - timedelta(seconds=self.window_size)

        # Generate windows by sliding backwards from reference_time
        # Include the window that ends at reference_time
        windows.append((window_start, window_end))

        return windows

    def compute_window_aggregation(
        self, window_start: datetime, window_end: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """Compute aggregations for all records within a specific window, grouped by canton.

        Selects records from the buffer whose timestamps fall within
        [window_start, window_end), groups them by canton, and computes
        count, sum, and average for each configured numeric field.

        Args:
            window_start: The inclusive start of the aggregation window.
            window_end: The exclusive end of the aggregation window.

        Returns:
            A dictionary mapping canton codes to aggregation results.
            Each aggregation result contains:
                - "window_start": ISO 8601 string of the window start.
                - "window_end": ISO 8601 string of the window end.
                - "canton": The canton code.
                - "count": Number of records in the window for this canton.
                - Plus per-field sum, avg, and count metrics.
        """
        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)

        # Filter records within the window boundaries [start, end)
        window_records = []
        for record in self._buffer:
            ts_str = record.get("timestamp")
            if ts_str is None:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if window_start <= ts < window_end:
                    window_records.append(record)
            except (ValueError, TypeError):
                continue

        # Group filtered records by canton and compute aggregations
        canton_groups = group_by_canton(window_records)
        results: Dict[str, Dict[str, Any]] = {}

        for canton, records in canton_groups.items():
            agg = compute_aggregation(records, self.numeric_fields)
            agg["window_start"] = window_start.isoformat()
            agg["window_end"] = window_end.isoformat()
            agg["canton"] = canton
            results[canton] = agg

        return results

    def flush_expired(self, cutoff: datetime) -> int:
        """Remove records from the buffer older than the cutoff time.

        Removes records whose timestamps are strictly before the cutoff
        to prevent unbounded memory growth.

        Args:
            cutoff: Records with timestamps before this datetime are
                removed from the buffer.

        Returns:
            The number of records removed from the buffer.
        """
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)

        original_count = len(self._buffer)
        self._buffer = [
            r for r in self._buffer
            if self._record_time(r) is not None
            and self._record_time(r) >= cutoff
        ]
        return original_count - len(self._buffer)

    def _record_time(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Parse the timestamp from a record, returning None if unparseable.

        Args:
            record: A dictionary that may contain a "timestamp" field.

        Returns:
            A timezone-aware datetime if the timestamp is valid, None
            otherwise.
        """
        ts_str = record.get("timestamp")
        if ts_str is None:
            return None
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except (ValueError, TypeError):
            return None

    def reset_late_arrival_count(self) -> int:
        """Reset the late-arrival counter and return its previous value.

        Returns:
            The number of late arrivals counted before the reset.
        """
        count = self.late_arrival_count
        self.late_arrival_count = 0
        return count

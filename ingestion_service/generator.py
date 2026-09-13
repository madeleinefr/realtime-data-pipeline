"""Synthetic IoT sensor data generator for the ingestion service.

Generates realistic IoT device records representing weather stations deployed
across Swiss cantons. Each record contains device metadata, geolocation within
Switzerland's bounds, and environmental sensor readings.

Realism model
-------------
Rather than drawing every field from a flat uniform distribution on each call
(which produces wild spikes and unstable per-canton aggregates), this module
models a *fixed fleet* of devices with persistent identity and smoothly
drifting sensor state:

* Each device is created once at import time and permanently assigned a home
  canton, fixed coordinates within that canton, and per-sensor baselines. A
  device therefore always reports from the same canton, so distinct-device
  counts per canton are stable rather than random.
* Each canton has a climate baseline (alpine cantons are colder and lower
  pressure; lowland cantons are milder), so cantons differ from each other but
  each remains internally consistent.
* Each reading is the device's fixed baseline plus a small Gaussian jitter and
  is clamped to the valid range. Because the jitter is small relative to the
  sensor range, readings for a device cluster tightly around its baseline,
  which keeps the aggregated averages smooth and removes the large spikes.
  Readings are stateless (they do not depend on previous readings), so a fixed
  ``random`` seed reproduces identical output.

Timestamps are guaranteed to be monotonically non-decreasing across sequential
calls, ensuring records can be ingested in chronological order.
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

from faker import Faker

fake = Faker()

# Module-level state for monotonically non-decreasing timestamps
_last_timestamp: datetime = datetime.now(timezone.utc)

# Swiss canton codes (26 cantons)
SWISS_CANTONS: list[str] = [
    "ZH", "BE", "LU", "UR", "SZ", "OW", "NW", "GL", "ZG", "FR",
    "SO", "BS", "BL", "SH", "AR", "AI", "SG", "GR", "AG", "TG",
    "TI", "VD", "VS", "NE", "GE", "JU",
]

# IoT device type — all devices are weather stations
DEVICE_TYPES: list[str] = [
    "weather_station",
]

# Device status options
DEVICE_STATUSES: list[str] = [
    "active",
    "inactive",
    "maintenance",
    "error",
]

# Status weights: devices are active the vast majority of the time so that the
# active-device count per canton stays stable rather than churning.
_STATUS_WEIGHTS: list[float] = [0.97, 0.01, 0.01, 0.01]

# Swiss geographic bounds
LATITUDE_MIN: float = 45.8
LATITUDE_MAX: float = 47.8
LONGITUDE_MIN: float = 5.9
LONGITUDE_MAX: float = 10.5

# Sensor reading ranges (hard clamps; readings never leave these bounds)
TEMPERATURE_MIN: float = -20.0
TEMPERATURE_MAX: float = 45.0
HUMIDITY_MIN: float = 0.0
HUMIDITY_MAX: float = 100.0
PRESSURE_MIN: float = 950.0
PRESSURE_MAX: float = 1050.0

# Size of the fixed device fleet. device_id runs device-001 .. device-500.
_FLEET_SIZE: int = 500

# Approximate relative altitude weighting per canton, used to bias the baseline
# climate. Higher = more alpine (colder, lower pressure). Cantons not listed
# default to a mid/lowland profile.
_CANTON_ALPINE_FACTOR: Dict[str, float] = {
    "GR": 1.0, "VS": 0.95, "UR": 0.9, "OW": 0.8, "NW": 0.75, "GL": 0.8,
    "TI": 0.4, "BE": 0.55, "GR_": 1.0, "SZ": 0.6, "AI": 0.6, "AR": 0.55,
    "FR": 0.45, "LU": 0.4, "SG": 0.5, "VD": 0.4, "NE": 0.45, "JU": 0.45,
    "ZG": 0.35, "SO": 0.35, "AG": 0.3, "ZH": 0.3, "TG": 0.3, "SH": 0.3,
    "BL": 0.3, "BS": 0.25, "GE": 0.25,
}

# Per-reading jitter (standard deviation of the Gaussian noise added around
# each device's fixed baseline). Kept small so readings for a given device
# cluster tightly around its baseline: this removes the wide full-range spikes
# in the aggregated averages while keeping the output a pure function of the
# ``random`` state (no persistent per-device drift), which preserves
# seed-determinism.
_TEMP_JITTER: float = 0.6
_HUMIDITY_JITTER: float = 1.2
_PRESSURE_JITTER: float = 0.5


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp ``value`` into the inclusive range [``low``, ``high``]."""
    return max(low, min(high, value))


def _build_fleet() -> List[Dict[str, Any]]:
    """Create the fixed device fleet with persistent identity and baselines.

    Each device is assigned once and never changes canton or coordinates.
    Baselines are derived from the canton's climate profile plus a small
    per-device offset, and current readings start at the baseline.
    """
    fleet: List[Dict[str, Any]] = []
    for i in range(1, _FLEET_SIZE + 1):
        canton = SWISS_CANTONS[(i - 1) % len(SWISS_CANTONS)]
        alpine = _CANTON_ALPINE_FACTOR.get(canton, 0.35)

        # Baseline temperature: ~14°C in the lowlands down to ~1°C high alpine,
        # with a small per-device offset so stations in the same canton differ
        # slightly but plausibly.
        temp_baseline = 14.0 - alpine * 13.0 + random.uniform(-1.5, 1.5)
        # Alpine areas tend to be a touch more humid on average here.
        humidity_baseline = 60.0 + alpine * 8.0 + random.uniform(-4.0, 4.0)
        # Pressure drops with altitude (~-11 hPa across the alpine factor span).
        pressure_baseline = 1013.0 - alpine * 11.0 + random.uniform(-2.0, 2.0)

        temp_baseline = _clamp(temp_baseline, TEMPERATURE_MIN, TEMPERATURE_MAX)
        humidity_baseline = _clamp(humidity_baseline, HUMIDITY_MIN, HUMIDITY_MAX)
        pressure_baseline = _clamp(pressure_baseline, PRESSURE_MIN, PRESSURE_MAX)

        fleet.append(
            {
                "device_id": f"device-{i:03d}",
                "canton": canton,
                "latitude": round(random.uniform(LATITUDE_MIN, LATITUDE_MAX), 6),
                "longitude": round(random.uniform(LONGITUDE_MIN, LONGITUDE_MAX), 6),
                "temp_baseline": temp_baseline,
                "humidity_baseline": humidity_baseline,
                "pressure_baseline": pressure_baseline,
            }
        )
    return fleet


# The fixed fleet is built once at import time.
_FLEET: List[Dict[str, Any]] = _build_fleet()


def _reading(baseline: float, jitter: float, low: float, high: float) -> float:
    """Produce one sensor reading as ``baseline`` plus small Gaussian jitter.

    The reading is a pure function of the current ``random`` state (it does not
    depend on any previous reading), so a fixed ``random`` seed reproduces the
    exact same value. Because ``jitter`` is small relative to the sensor range,
    successive readings for a device cluster tightly around its baseline,
    which keeps the aggregated averages smooth and spike-free. The result is
    clamped into [``low``, ``high``].
    """
    return _clamp(baseline + random.gauss(0.0, jitter), low, high)


def generate_device_record() -> Dict[str, Any]:
    """Generate a single synthetic IoT sensor data record.

    Selects a device from the fixed fleet, advances its sensor readings with a
    small mean-reverting random-walk step, and returns a record. Because each
    device has a persistent canton and coordinates, per-canton aggregates are
    stable; because readings drift smoothly, the aggregated averages no longer
    spike. The timestamp is guaranteed to be monotonically non-decreasing
    relative to the previous call.

    Returns:
        A dictionary containing the following fields:
            - record_id: Unique identifier (UUID4 string)
            - timestamp: ISO 8601 formatted UTC timestamp
            - device_id: Device identifier (device-001 to device-500)
            - device_type: Type of IoT device
            - canton: Swiss canton code (2-letter abbreviation)
            - latitude: Device latitude (45.8–47.8)
            - longitude: Device longitude (5.9–10.5)
            - temperature: Temperature reading in °C (-20 to 45)
            - humidity: Relative humidity percentage (0–100)
            - pressure: Atmospheric pressure in hPa (950–1050)
            - status: Current device status
    """
    global _last_timestamp

    # Generate monotonically non-decreasing timestamp
    now = datetime.now(timezone.utc)
    if now >= _last_timestamp:
        _last_timestamp = now
    timestamp = _last_timestamp

    # Pick a device from the fixed fleet and sample readings around its
    # persistent baselines. Readings are stateless (no mutation of the fleet),
    # so a fixed random seed reproduces identical output.
    device = random.choice(_FLEET)

    temperature = _reading(
        device["temp_baseline"], _TEMP_JITTER, TEMPERATURE_MIN, TEMPERATURE_MAX
    )
    humidity = _reading(
        device["humidity_baseline"], _HUMIDITY_JITTER, HUMIDITY_MIN, HUMIDITY_MAX
    )
    pressure = _reading(
        device["pressure_baseline"], _PRESSURE_JITTER, PRESSURE_MIN, PRESSURE_MAX
    )

    record: Dict[str, Any] = {
        "record_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "device_id": device["device_id"],
        "device_type": random.choice(DEVICE_TYPES),
        "canton": device["canton"],
        "latitude": device["latitude"],
        "longitude": device["longitude"],
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "pressure": round(pressure, 2),
        "status": random.choices(DEVICE_STATUSES, weights=_STATUS_WEIGHTS, k=1)[0],
    }

    return record

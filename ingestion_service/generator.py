"""Synthetic IoT sensor data generator for the ingestion service.

Generates realistic IoT device records representing sensors deployed across
Swiss cantons. Each record contains device metadata, geolocation within
Switzerland's bounds, and environmental sensor readings.

Timestamps are guaranteed to be monotonically non-decreasing across sequential
calls, ensuring records can be ingested in chronological order.
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

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

# IoT device type options
DEVICE_TYPES: list[str] = [
    "temperature_sensor",
    "humidity_sensor",
    "pressure_sensor",
    "air_quality_sensor",
    "weather_station",
]

# Device status options
DEVICE_STATUSES: list[str] = [
    "active",
    "inactive",
    "maintenance",
    "error",
]

# Swiss geographic bounds
LATITUDE_MIN: float = 45.8
LATITUDE_MAX: float = 47.8
LONGITUDE_MIN: float = 5.9
LONGITUDE_MAX: float = 10.5

# Sensor reading ranges
TEMPERATURE_MIN: float = -20.0
TEMPERATURE_MAX: float = 45.0
HUMIDITY_MIN: float = 0.0
HUMIDITY_MAX: float = 100.0
PRESSURE_MIN: float = 950.0
PRESSURE_MAX: float = 1050.0


def generate_device_record() -> Dict[str, Any]:
    """Generate a single synthetic IoT sensor data record.

    Produces a dictionary representing one IoT device reading with realistic
    sensor values for a device located in Switzerland. The timestamp is
    guaranteed to be monotonically non-decreasing relative to the previous
    call, ensuring chronological ordering of generated records.

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

    # Generate device identifier (device-001 to device-500)
    device_number = random.randint(1, 500)
    device_id = f"device-{device_number:03d}"

    record: Dict[str, Any] = {
        "record_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "device_id": device_id,
        "device_type": random.choice(DEVICE_TYPES),
        "canton": random.choice(SWISS_CANTONS),
        "latitude": round(random.uniform(LATITUDE_MIN, LATITUDE_MAX), 6),
        "longitude": round(random.uniform(LONGITUDE_MIN, LONGITUDE_MAX), 6),
        "temperature": round(random.uniform(TEMPERATURE_MIN, TEMPERATURE_MAX), 2),
        "humidity": round(random.uniform(HUMIDITY_MIN, HUMIDITY_MAX), 2),
        "pressure": round(random.uniform(PRESSURE_MIN, PRESSURE_MAX), 2),
        "status": random.choice(DEVICE_STATUSES),
    }

    return record

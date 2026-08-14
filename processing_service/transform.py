"""Data transformation module for the processing service.

Provides transformation logic for incoming data records, applying column-level
filtering (field exclusion), whitespace trimming on string fields, and numeric
unit normalization. These transformations are applied after validation to ensure
data conforms to the expected schema and format before downstream processing.

Normalization is configurable via a module-level dictionary that maps field names
to scaling factors. This allows flexible unit conversion (e.g., converting
millivolts to volts, Fahrenheit offsets, or any linear scaling).
"""

from typing import Any, Dict, List, Optional


# Default normalization configuration: maps field names to scaling factors.
# A field's numeric value is multiplied by its scaling factor during normalization.
# Example: {"temperature": 1.0, "voltage": 0.001} converts millivolts to volts.
DEFAULT_NORMALIZATION_CONFIG: Dict[str, float] = {}


def apply_transformations(
    record: Dict[str, Any],
    exclusion_list: List[str],
    normalization_config: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """Apply a sequence of transformations to a data record.

    Transforms the input record by:
    1. Removing fields listed in the exclusion list (column-level filtering).
    2. Trimming leading/trailing whitespace from all string-valued fields.
    3. Normalizing numeric fields according to the normalization config by
       multiplying their values by the configured scaling factor.

    The original record is not mutated; a new dictionary is returned.

    Args:
        record: A dictionary representing a single data record to transform.
            Keys are field names and values can be of any type.
        exclusion_list: A list of field names to remove from the record.
            Fields in this list will not appear in the output.
        normalization_config: An optional dictionary mapping field names to
            numeric scaling factors. If a field is present in both the record
            and this config, its value is multiplied by the factor. If None,
            the module-level DEFAULT_NORMALIZATION_CONFIG is used.

    Returns:
        A new dictionary with excluded fields removed, string fields trimmed,
        and numeric fields normalized according to the config.
    """
    if normalization_config is None:
        normalization_config = DEFAULT_NORMALIZATION_CONFIG

    result: Dict[str, Any] = {}

    for key, value in record.items():
        # Step 1: Skip fields in the exclusion list
        if key in exclusion_list:
            continue

        # Step 2: Trim whitespace from string fields
        if isinstance(value, str):
            value = value.strip()

        # Step 3: Normalize numeric fields using scaling factors
        elif isinstance(value, (int, float)) and key in normalization_config:
            value = value * normalization_config[key]

        result[key] = value

    return result

"""Row-level filtering module for the processing service.

Provides row-level inclusion filtering for incoming data records, allowing
only records that match all specified criteria to pass through the pipeline.
Criteria are defined as key-value pairs where each key corresponds to a record
field and each value specifies the acceptable value(s) for that field.

Filtering criteria can be loaded from the ROW_INCLUSION_CRITERIA environment
variable, which expects a JSON-encoded dictionary. This enables runtime
configuration of which records are forwarded for downstream processing.
"""

import json
import os
from typing import Any, Dict, List, Optional, Union


def apply_row_filter(
    record: Dict[str, Any], inclusion_criteria: Optional[Dict[str, Any]] = None
) -> bool:
    """Determine whether a record matches all inclusion criteria.

    Evaluates the record against each criterion using AND logic: the record
    must match every criterion to pass. A criterion value can be either a
    single value (exact match) or a list of acceptable values (match any one).

    If inclusion_criteria is None or empty, all records pass the filter.

    Args:
        record: A dictionary representing a single data record to evaluate.
            Keys are field names and values can be of any type.
        inclusion_criteria: A dictionary mapping field names to acceptable
            values. Each value can be:
            - A single value: the record's field must equal this value.
            - A list of values: the record's field must equal one of these.
            If None or empty, the filter passes all records.

    Returns:
        True if the record matches all specified criteria (or if no criteria
        are specified). False if any criterion is not met, including when the
        record is missing a required field.
    """
    if not inclusion_criteria:
        return True

    for field, criterion in inclusion_criteria.items():
        if field not in record:
            return False

        record_value = record[field]

        if isinstance(criterion, list):
            if record_value not in criterion:
                return False
        else:
            if record_value != criterion:
                return False

    return True


def load_inclusion_criteria() -> Dict[str, Any]:
    """Load row inclusion criteria from the ROW_INCLUSION_CRITERIA env var.

    Reads the ROW_INCLUSION_CRITERIA environment variable, which should
    contain a JSON-encoded dictionary of field-to-value mappings. If the
    variable is not set or is empty, returns an empty dictionary (meaning
    no filtering is applied).

    Returns:
        A dictionary of inclusion criteria parsed from the environment
        variable, or an empty dictionary if the variable is unset or empty.

    Raises:
        ValueError: If the environment variable contains invalid JSON or
            does not decode to a dictionary.
    """
    raw_value = os.environ.get("ROW_INCLUSION_CRITERIA", "")

    if not raw_value.strip():
        return {}

    try:
        criteria = json.loads(raw_value)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"ROW_INCLUSION_CRITERIA contains invalid JSON: {e}"
        ) from e

    if not isinstance(criteria, dict):
        raise ValueError(
            "ROW_INCLUSION_CRITERIA must be a JSON object (dictionary), "
            f"got {type(criteria).__name__}"
        )

    return criteria

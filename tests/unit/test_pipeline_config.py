"""Unit tests for Processing_Service PipelineConfig environment parsing.

Focuses on the column-exclusion guard (NFR-5.8 column pruning) and defensive
configuration parsing (NFR-7.2). The guard ensures that columns the transform
and aggregation stages depend on can never be dropped via COLUMN_EXCLUSION_LIST,
which would otherwise crash the stream at runtime.

Requirements: NFR-5.8, NFR-7.2
"""

import pytest

from processing_service.pipeline import PROTECTED_COLUMNS, PipelineConfig


def _clear_processing_env(monkeypatch):
    """Remove all processing-related env vars so defaults apply."""
    for var in (
        "COLUMN_EXCLUSION_LIST",
        "ROW_INCLUSION_CRITERIA",
        "WINDOW_SIZE_MINUTES",
        "SLIDE_INTERVAL_SECONDS",
    ):
        monkeypatch.delenv(var, raising=False)


class TestColumnExclusionGuard:
    """COLUMN_EXCLUSION_LIST parsing and the protected-column guard (NFR-5.8)."""

    def test_empty_exclusion_list_yields_no_exclusions(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv("COLUMN_EXCLUSION_LIST", "")
        config = PipelineConfig()
        assert config.column_exclusion_list == []

    def test_safe_columns_are_kept(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv("COLUMN_EXCLUSION_LIST", "latitude,longitude")
        config = PipelineConfig()
        assert config.column_exclusion_list == ["latitude", "longitude"]

    def test_whitespace_is_trimmed(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv("COLUMN_EXCLUSION_LIST", " latitude , longitude ")
        config = PipelineConfig()
        assert config.column_exclusion_list == ["latitude", "longitude"]

    @pytest.mark.parametrize("protected", sorted(PROTECTED_COLUMNS))
    def test_protected_column_is_stripped(self, monkeypatch, protected):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv("COLUMN_EXCLUSION_LIST", protected)
        config = PipelineConfig()
        # A depended-on column must never make it into the exclusion list.
        assert protected not in config.column_exclusion_list
        assert config.column_exclusion_list == []

    def test_mixed_safe_and_protected_keeps_only_safe(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv(
            "COLUMN_EXCLUSION_LIST", "latitude,pressure,longitude,canton"
        )
        config = PipelineConfig()
        assert config.column_exclusion_list == ["latitude", "longitude"]
        for protected in ("pressure", "canton"):
            assert protected not in config.column_exclusion_list


class TestRowInclusionCriteriaParsing:
    """ROW_INCLUSION_CRITERIA parsing with safe fallback (NFR-7.2, NFR-5.9)."""

    def test_default_is_empty_dict(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        config = PipelineConfig()
        assert config.row_inclusion_criteria == {}

    def test_valid_json_is_parsed(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", '{"status": "active"}')
        config = PipelineConfig()
        assert config.row_inclusion_criteria == {"status": "active"}

    def test_invalid_json_falls_back_to_empty(self, monkeypatch):
        _clear_processing_env(monkeypatch)
        monkeypatch.setenv("ROW_INCLUSION_CRITERIA", "{not valid json")
        config = PipelineConfig()
        # Defensive parsing: bad config must not crash startup (NFR-7.2).
        assert config.row_inclusion_criteria == {}

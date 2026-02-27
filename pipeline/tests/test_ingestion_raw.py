"""
Raw Ingestion Test Suite

This module verifies the correctness of the Raw Raw (data_engineer_assignment_dataset) to Bronze ingestion logic.
It focuses on two foundational guarantees of the ingestion layer:

1. Minimal Validation
   - Ensures rows missing required identifiers (customerId, contentId, clientId)
     are filtered out before entering Bronze.

2. Idempotent Deduplication
   - Confirms that duplicate events sharing the same natural key
     (customerId, contentId, clientId, eventDate) are removed.

These tests ensure the ingestion pipeline produces clean, deterministic,
and contract-compliant Bronze input for downstream layers.
"""

from pyspark.sql import Row
from pipeline.src.ingestion_raw import minimal_validation, dedupe


def test_minimal_validation_filters_nulls(spark):
    """Rows missing required fields must be removed."""
    df = spark.createDataFrame([
        {"customerId": "c1", "contentId": "v1", "clientId": "x", "eventDate": "2024-01-01"},
        {"customerId": None, "contentId": "v2", "clientId": "y", "eventDate": "2024-01-01"},
        {"customerId": "c3", "contentId": None, "clientId": "z", "eventDate": "2024-01-01"},
        {"customerId": "c4", "contentId": "v4", "clientId": None, "eventDate": "2024-01-01"},
    ])

    valid = minimal_validation(df)

    assert valid.count() == 1
    row = valid.collect()[0]
    assert row.customerId == "c1"
    assert row.contentId == "v1"
    assert row.clientId == "x"


def test_dedupe_removes_duplicates(spark):
    """Duplicate events (same natural key) must be removed."""
    df = spark.createDataFrame([
        {"customerId": "c1", "contentId": "v1", "clientId": "x", "eventDate": "2024-01-01"},
        {"customerId": "c1", "contentId": "v1", "clientId": "x", "eventDate": "2024-01-01"},  # duplicate
        {"customerId": "c2", "contentId": "v2", "clientId": "y", "eventDate": "2024-01-01"},
    ])

    deduped = dedupe(df)

    assert deduped.count() == 2

    keys = {(r.customerId, r.contentId, r.clientId, r.eventDate) for r in deduped.collect()}
    assert ("c1", "v1", "x", "2024-01-01") in keys
    assert ("c2", "v2", "y", "2024-01-01") in keys

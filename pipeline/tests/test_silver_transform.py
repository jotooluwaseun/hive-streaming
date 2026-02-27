"""
Silver Transformation Test Suite

This module validates the core behaviors of the Silver transformation pipeline.
It ensures that business rules, schema shaping, and idempotency guarantees
operate correctly before data is promoted to the Silver layer.

The suite covers:

1. Business Rule Enforcement
   - Negative player metrics (bufferings, bufferingTime) must be corrected to zero.
   - Quality distribution must be normalized into explicit quality_* columns.

2. Flattening Logic
   - Confirms nested Bronze-validated structures (timestampInfo, player,
     totalDistribution) are flattened into the expected Silver schema.

3. Idempotent Deduplication
   - Ensures duplicate Silver rows sharing the natural key
     (customerId, contentId, clientId, eventDate) are removed.

These tests guarantee that Silver output is clean, normalized, and ready
for downstream QoS metric computation in the Gold layer.
"""

from pipeline.src.silver_transform import apply_business_rules, flatten, dedupe


def test_apply_business_rules_fix_negative_values(spark):
    """Negative bufferings and bufferingTime should be fixed to 0."""
    
    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100, "agent": 90},
            "player": {"bufferings": -5, "bufferingTime": -200},
            "totalDistribution": {
                "sourceTraffic": {"requests": 1, "responses": 1, "requestedData": 100, "receivedData": 90},
                "p2pTraffic": {"requests": 1, "responses": 1, "requestedData": 50, "receivedData": 40},
            },
            "qualityDistribution": {"720p": 1}
        }
    ])

    df_rules = apply_business_rules(df)
    row = df_rules.collect()[0]

    assert row.player["bufferings"] == 0
    assert row.player["bufferingTime"] == 0


def test_apply_business_rules_quality_normalization(spark):
    """Missing quality tiers should appear as null columns."""
    
    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100, "agent": 90},
            "player": {"bufferings": 1, "bufferingTime": 200},
            "totalDistribution": {
                "sourceTraffic": {"requests": 1, "responses": 1, "requestedData": 100, "receivedData": 90},
                "p2pTraffic": {"requests": 1, "responses": 1, "requestedData": 50, "receivedData": 40},
            },
            "qualityDistribution": {"720p": 1}
        }
    ])

    df_rules = apply_business_rules(df)
    row = df_rules.collect()[0]

    assert row.quality_720p == 1
    assert row.quality_1080p is None
    assert row.quality_240p is None


def test_flatten_produces_expected_columns(spark):
    """Flattening should produce the correct Silver schema."""
    
    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100, "agent": 90},
            "player": {"bufferings": 2, "bufferingTime": 300},
            "totalDistribution": {
                "sourceTraffic": {"requests": 10, "responses": 1, "requestedData": 1000, "receivedData": 900},
                "p2pTraffic": {"requests": 5, "responses": 1, "requestedData": 500, "receivedData": 450},
            },
            "quality_240p": 0,
            "quality_360p": 0,
            "quality_480p": 0,
            "quality_720p": 1,
            "quality_1080p": 0,
        }
    ])

    df_flat = flatten(df)

    expected_cols = {
        "customerId", "contentId", "clientId", "eventDate",
        "serverTimestamp", "agentTimestamp",
        "bufferings", "bufferingTime",
        "src_requests", "src_responses", "src_requestedData", "src_receivedData",
        "p2p_requests", "p2p_responses", "p2p_requestedData", "p2p_receivedData",
        "quality_240p", "quality_360p", "quality_480p", "quality_720p", "quality_1080p"
    }

    assert set(df_flat.columns) == expected_cols


def test_dedupe_removes_duplicates(spark):
    """Duplicate Silver rows should be removed based on natural key."""
    
    df = spark.createDataFrame([
        {"customerId": "c1", "contentId": "v1", "clientId": "x", "eventDate": "2024-01-01"},
        {"customerId": "c1", "contentId": "v1", "clientId": "x", "eventDate": "2024-01-01"},  # duplicate
        {"customerId": "c2", "contentId": "v2", "clientId": "y", "eventDate": "2024-01-01"},
    ])

    df_deduped = dedupe(df)

    assert df_deduped.count() == 2

    keys = {(r.customerId, r.contentId, r.clientId, r.eventDate) for r in df_deduped.collect()}
    assert ("c1", "v1", "x", "2024-01-01") in keys
    assert ("c2", "v2", "y", "2024-01-01") in keys

"""
Gold Metrics Test Suite

This module validates the correctness of the Gold QoS metrics logic.
It covers three core areas:

1. QoS Field Derivations
   - Ensures total_received_data, buffering_ratio, p2p_share,
     and dominant_quality are computed correctly from Silver inputs.

2. Viewer-Level Aggregations
   - Verifies daily viewer metrics such as total buffering,
     total received data, average ratios, max quality, and event counts.

3. Content-Level Aggregations
   - Confirms content-centric QoS metrics aggregate accurately
     across multiple viewers for the same content and date.

These tests ensure deterministic, reproducible Gold-layer behavior.
"""

from pipeline.src.gold_metrics import (
    add_qos_fields,
    compute_viewer_metrics,
    compute_content_metrics,
)


def test_add_qos_fields_derivations(spark):
    """QoS fields should be correctly derived from Silver-level columns."""

    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "eventDate": "2024-01-01",
            "bufferingTime": 100,
            "bufferings": 2,
            "src_receivedData": 500,
            "p2p_receivedData": 500,
            "quality_720p": 1,
            "quality_1080p": 0,
            "quality_480p": 0,
            "quality_360p": 0,
            "quality_240p": 0,
        }
    ])

    df_qos = add_qos_fields(df)
    row = df_qos.collect()[0]

    # total_received_data
    assert row.total_received_data == 1000

    # buffering_ratio = 100 / 1000
    assert round(row.buffering_ratio, 2) == 0.10

    # p2p_share = 500 / 1000
    assert round(row.p2p_share, 2) == 0.50

    # dominant_quality
    assert row.dominant_quality == "720p"


def test_compute_viewer_metrics(spark):
    """Viewer-level metrics should aggregate correctly."""

    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "eventDate": "2024-01-01",
            "bufferingTime": 100,
            "bufferings": 2,
            "total_received_data": 1000,
            "buffering_ratio": 0.1,
            "p2p_share": 0.5,
            "dominant_quality": "720p",
        },
        {
            "customerId": "c1",
            "contentId": "v2",
            "eventDate": "2024-01-01",
            "bufferingTime": 200,
            "bufferings": 3,
            "total_received_data": 2000,
            "buffering_ratio": 0.1,
            "p2p_share": 0.25,
            "dominant_quality": "480p",
        }
    ])

    viewer_df = compute_viewer_metrics(df)
    row = viewer_df.collect()[0]

    # Aggregated totals
    assert row.total_buffering_time == 300
    assert row.total_bufferings == 5
    assert row.total_received_data == 3000

    # Derived buffering_ratio = 300 / 3000
    assert round(row.buffering_ratio, 2) == 0.10

    # Average p2p_share = (0.5 + 0.25) / 2
    assert round(row.avg_p2p_share, 2) == 0.38

    # max quality
    assert row.max_quality == "720p"

    # event count
    assert row.event_count == 2


def test_compute_content_metrics(spark):
    """Content-level metrics should aggregate correctly."""

    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "eventDate": "2024-01-01",
            "bufferingTime": 50,
            "bufferings": 1,
            "total_received_data": 500,
            "buffering_ratio": 0.1,
            "p2p_share": 0.5,
            "dominant_quality": "720p",
        },
        {
            "customerId": "c2",
            "contentId": "v1",
            "eventDate": "2024-01-01",
            "bufferingTime": 150,
            "bufferings": 2,
            "total_received_data": 1500,
            "buffering_ratio": 0.1,
            "p2p_share": 0.25,
            "dominant_quality": "480p",
        }
    ])

    content_df = compute_content_metrics(df)
    row = content_df.collect()[0]

    # Aggregated totals
    assert row.total_buffering_time == 200
    assert row.total_bufferings == 3
    assert row.total_received_data == 2000

    # Derived buffering_ratio = 200 / 2000
    assert round(row.buffering_ratio, 2) == 0.10

    # Average p2p_share = (0.5 + 0.25) / 2
    assert round(row.avg_p2p_share, 2) == 0.38

    # max quality
    assert row.max_quality == "720p"

    # event count
    assert row.event_count == 2

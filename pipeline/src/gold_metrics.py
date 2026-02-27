"""
Gold QoS Metrics Pipeline

This job builds the Gold layer for viewer-level and content-level QoS analytics.
It performs the following:

1. Reads Silver events as a structured streaming Delta source.
2. Derives QoS fields (total_received_data, buffering_ratio, p2p_share, dominant_quality).
3. Computes daily aggregated QoS metrics:
   - Viewer-level metrics grouped by (customerId, eventDate)
   - Content-level metrics grouped by (contentId, eventDate)
4. Writes both aggregations as streaming Gold Delta tables
   with checkpointing and eventDate partitioning for dashboard consumption.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as _sum,
    avg,
    count,
    expr,
    coalesce,
    lit,
    when,
    greatest,
)

from pipeline.src.spark_session import get_spark
from pipeline.src.paths import (
    silver_path,
    checkpoint_gold_viewer_path,
    checkpoint_gold_content_path,
    gold_viewer_path,
    gold_content_path
)



# ---------- READ SILVER AS STREAM ----------

def read_silver() -> DataFrame:
    spark = get_spark()
    logging.info(f"Reading Silver stream from: {silver_path}")

    return (
        spark.readStream
        .format("delta")
        .load(str(silver_path))
    )


# ---------- DERIVE QoS FIELDS FROM SILVER ----------

def add_qos_fields(df: DataFrame) -> DataFrame:
    """
    Derive:
    - total_received_data
    - buffering_ratio (per event)
    - p2p_share (per event)
    - dominant_quality
    """

    logging.info("Adding QoS derived fields from Silver")

    total_received = (
        coalesce(col("src_receivedData"), lit(0)) +
        coalesce(col("p2p_receivedData"), lit(0))
    )

    df = df.withColumn("total_received_data", total_received)

    df = df.withColumn(
        "buffering_ratio",
        when(total_received > 0, col("bufferingTime") / total_received)
        .otherwise(lit(0.0))
    )

    df = df.withColumn(
        "p2p_share",
        when(total_received > 0, col("p2p_receivedData") / total_received)
        .otherwise(lit(0.0))
    )

    df = df.withColumn(
        "dominant_quality",
        greatest(
            when(col("quality_1080p").isNotNull(), lit("1080p")),
            when(col("quality_720p").isNotNull(), lit("720p")),
            when(col("quality_480p").isNotNull(), lit("480p")),
            when(col("quality_360p").isNotNull(), lit("360p")),
            when(col("quality_240p").isNotNull(), lit("240p")),
        )
    )

    return df


# ---------- VIEWER-LEVEL METRICS ----------

def compute_viewer_metrics(df: DataFrame) -> DataFrame:
    """
    Aggregates per viewer per day.
    """

    logging.info("Computing viewer-level QoS metrics")

    agg = (
        df.groupBy("customerId", "eventDate")
        .agg(
            _sum("bufferingTime").alias("total_buffering_time"),
            _sum("bufferings").alias("total_bufferings"),
            _sum("total_received_data").alias("total_received_data"),
            avg("buffering_ratio").alias("avg_buffering_ratio"),
            avg("p2p_share").alias("avg_p2p_share"),
            count("*").alias("event_count"),
            expr("max(dominant_quality)").alias("max_quality"),
        )
    )

    agg = agg.withColumn(
        "buffering_ratio",
        expr(
            "CASE WHEN total_received_data > 0 "
            "THEN total_buffering_time / total_received_data "
            "ELSE 0.0 END"
        )
    )

    return agg


# ---------- CONTENT-LEVEL METRICS ----------

def compute_content_metrics(df: DataFrame) -> DataFrame:
    """
    Aggregates per content per day.
    """

    logging.info("Computing content-level QoS metrics")

    agg = (
        df.groupBy("contentId", "eventDate")
        .agg(
            _sum("bufferingTime").alias("total_buffering_time"),
            _sum("bufferings").alias("total_bufferings"),
            _sum("total_received_data").alias("total_received_data"),
            avg("buffering_ratio").alias("avg_buffering_ratio"),
            avg("p2p_share").alias("avg_p2p_share"),
            count("*").alias("event_count"),
            expr("max(dominant_quality)").alias("max_quality"),
        )
    )

    agg = agg.withColumn(
        "buffering_ratio",
        expr(
            "CASE WHEN total_received_data > 0 "
            "THEN total_buffering_time / total_received_data "
            "ELSE 0.0 END"
        )
    )

    return agg


# ---------- WRITE VIEWER GOLD TABLE ----------

def write_viewer_gold(df: DataFrame):
    logging.info(f"Writing Viewer QoS Gold table to: {gold_viewer_path}")

    return (
        df.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", str(checkpoint_gold_viewer_path))
        .partitionBy("eventDate")
        .start(str(gold_viewer_path))
    )


# ---------- WRITE CONTENT GOLD TABLE ----------

def write_content_gold(df: DataFrame):
    logging.info(f"Writing Content QoS Gold table to: {gold_content_path}")

    return (
        df.writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", str(checkpoint_gold_content_path))
        .partitionBy("eventDate")
        .start(str(gold_content_path))
    )


# ---------- ORCHESTRATION ----------

def run():
    logging.info("Starting Gold Metrics job")

    df_silver = read_silver()
    df_qos = add_qos_fields(df_silver)

    viewer_metrics = compute_viewer_metrics(df_qos)
    content_metrics = compute_content_metrics(df_qos)

    q1 = write_viewer_gold(viewer_metrics)
    q2 = write_content_gold(content_metrics)

    logging.info("Gold metrics streams are now running")
    q1.awaitTermination()
    q2.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run()

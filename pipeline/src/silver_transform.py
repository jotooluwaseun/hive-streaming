"""
Silver Transformation Pipeline

This job builds the Silver layer from validated Bronze events.
It performs the following transformations:

1. Reads Bronze-Validated as a streaming Delta source with schema evolution enabled.
2. Applies Silver-level business rules:
   - Corrects invalid or negative player metrics
   - Normalizes qualityDistribution into explicit quality tier columns
3. Flattens nested structures (timestampInfo, player, totalDistribution) into a clean tabular schema.
4. Ensures idempotency through deterministic deduplication on business keys.
5. Writes the curated Silver Delta table with merge-schema support,
   eventDate partitioning, and streaming checkpointing.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit
)

from pipeline.src.spark_session import get_spark
from pipeline.src.paths import (
    bronze_validated_path,
    silver_path,
    checkpoint_bronze_to_silver_path,
)

from pyspark.sql import functions as F

# ---------- READ VALIDATED BRONZE WITH SCHEMA EVOLUTION ----------

def read_validated_bronze() -> DataFrame:
    spark = get_spark()

    # Enable schema evolution for streaming
    spark.conf.set("spark.sql.streaming.schemaInference", "true")

    logging.info(f"Reading Bronze Validated stream from: {bronze_validated_path}")

    return (
        spark.readStream
        .format("delta")
        .option("mergeSchema", "true")
        .load(str(bronze_validated_path))
    )


# ---------- APPLY BUSINESS RULES (FIXED VERSION) ----------

def apply_business_rules(df: DataFrame) -> DataFrame:
    logging.info("Applying Silver-level business rules")

    # Fix negative values inside the STRUCT "player"
    df = df.withColumn(
        "player",
        F.struct(
            F.when(F.col("player.bufferings") < 0, 0)
             .otherwise(F.col("player.bufferings"))
             .alias("bufferings"),

            F.when(F.col("player.bufferingTime") < 0, 0)
             .otherwise(F.col("player.bufferingTime"))
             .alias("bufferingTime")
        )
    )

    # Normalize qualityDistribution (MAP)
    expected_qualities = ["240p", "360p", "480p", "720p", "1080p"]
    for q in expected_qualities:
        df = df.withColumn(
            f"quality_{q}",
            F.col("qualityDistribution").getItem(q)
        )

    return df


# ---------- FLATTENING LOGIC ----------

def flatten(df: DataFrame) -> DataFrame:
    logging.info("Flattening validated Bronze into Silver schema")

    return df.select(
        # Identifiers
        col("customerId"),
        col("contentId"),
        col("clientId"),
        col("eventDate"),

        # Timestamp info
        col("timestampInfo.server").alias("serverTimestamp"),
        col("timestampInfo.agent").alias("agentTimestamp"),

        # Player metrics
        col("player.bufferings").alias("bufferings"),
        col("player.bufferingTime").alias("bufferingTime"),

        # Source traffic
        col("totalDistribution.sourceTraffic.requests").alias("src_requests"),
        col("totalDistribution.sourceTraffic.responses").alias("src_responses"),
        col("totalDistribution.sourceTraffic.requestedData").alias("src_requestedData"),
        col("totalDistribution.sourceTraffic.receivedData").alias("src_receivedData"),

        # P2P traffic
        col("totalDistribution.p2pTraffic.requests").alias("p2p_requests"),
        col("totalDistribution.p2pTraffic.responses").alias("p2p_responses"),
        col("totalDistribution.p2pTraffic.requestedData").alias("p2p_requestedData"),
        col("totalDistribution.p2pTraffic.receivedData").alias("p2p_receivedData"),

        # Quality tiers (normalized)
        col("quality_240p"),
        col("quality_360p"),
        col("quality_480p"),
        col("quality_720p"),
        col("quality_1080p"),
    )


# ---------- DEDUPE FOR IDEMPOTENCY ----------

def dedupe(df: DataFrame) -> DataFrame:
    logging.info("Applying idempotent dedupe on Silver")

    return df.dropDuplicates([
        "customerId",
        "contentId",
        "clientId",
        "eventDate"
    ])


# ---------- WRITE TO SILVER ----------

def write_silver(df: DataFrame):
    logging.info(f"Writing Silver stream to: {silver_path}")

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", str(checkpoint_bronze_to_silver_path))
        .partitionBy("eventDate")
        .trigger(processingTime="30 seconds")
        .start(str(silver_path))
    )


# ---------- ORCHESTRATION ----------

def run():
    logging.info("Starting Bronze Validated → Silver transformation job")

    df_validated = read_validated_bronze()
    df_rules = apply_business_rules(df_validated)
    df_flat = flatten(df_rules)
    df_deduped = dedupe(df_flat)

    query = write_silver(df_deduped)

    logging.info("Silver transformation is now running")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run()

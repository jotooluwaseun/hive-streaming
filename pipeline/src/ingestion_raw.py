"""
Raw (data_engineer_assignment_dataset) to Bronze Ingestion Pipeline

This job implements a Densmore-compliant ingestion flow for the Raw layer.
It performs the following:

1. Reads raw telemetry events from a Delta source table as a streaming input.
2. Applies minimal structural validation to ensure required identifiers exist.
3. Enforces idempotency by deduplicating on key business identifiers.
4. Writes the validated, deduplicated raw events to the Bronze Delta table
   using streaming append mode with checkpointing and eventDate partitioning.
"""



import logging
from pyspark.sql import DataFrame

from pipeline.src.spark_session import get_spark
from pipeline.src.paths import (
    source_raw_path, 
    bronze_path, 
    checkpoint_source_to_bronze_path
)


# ---------- PIPELINE STAGES ----------

def load_source() -> DataFrame:
    """Read raw telemetry from a Delta source table."""
    spark = get_spark()
    logging.info(f"Reading raw telemetry (Delta) from: {source_raw_path}")
    df = spark.readStream.format("delta").load(str(source_raw_path))
    return df


def minimal_validation(df: DataFrame) -> DataFrame:
    """Apply only structural sanity checks."""
    logging.info("Applying minimal validation")
    
    return df.filter(
        "customerId IS NOT NULL AND contentId IS NOT NULL AND clientId IS NOT NULL"
    )


def dedupe(df: DataFrame) -> DataFrame:
    """Ensure idempotency by dropping duplicates."""
    logging.info("Dropping duplicates for idempotency")

    return df.dropDuplicates([
        "customerId",
        "contentId",
        "clientId",
        "eventDate"
    ])


def write_to_bronze(df: DataFrame):
    """Write raw data AS-IS to Bronze Delta."""
    logging.info(f"Writing Bronze stream to: {bronze_path}")

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_source_to_bronze_path))
        .partitionBy("eventDate")
        .trigger(processingTime="30 seconds")
        .start(str(bronze_path))
    )

    return query


# ---------- ORCHESTRATION ----------

def run():
    logging.info("Starting Source -> Bronze injestion job")

    df_source = load_source()
    df_valid = minimal_validation(df_source)
    df_deduped = dedupe(df_valid)

    query = write_to_bronze(df_deduped)

    logging.info("Source -> Bronze ingestion is now running")
    query.awaitTermination()


if __name__ == "__main__":
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        run()

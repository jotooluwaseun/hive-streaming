"""
Bronze Validation Pipeline

This job performs deterministic data quality validation on Bronze events.
It implements a Densmore-style contract and produces two outputs:

1. Validated Bronze rows:
   - Rows that pass all structural and semantic rules
   - Written to the Bronze-Validated (Silver staging) Delta table

2. Data Quality Results:
   - One row per rule per batch
   - Captures pass/fail counts and schema mismatches
   - Written to the dq_results Delta table for observability

The pipeline reads Bronze as a streaming Delta source, evaluates rules
(schema checks, required fields, non-negative metrics, bounded ratios),
and writes both validated data and DQ results with checkpointing.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp
)
from pyspark.sql import functions as F

from pipeline.src.spark_session import get_spark
from pipeline.src.paths import (
    bronze_path,
    bronze_validated_path,
    checkpoint_bronze_to_bronze_validated_path,
    dq_results_path
)



# ---------- EXPECTED SCHEMA (Densmore-style contract) ----------

EXPECTED_SCHEMA = {
    "customerId": "string",
    "contentId": "string",
    "clientId": "string",
    "eventDate": "date",
    "timestampInfo": "struct",
    "player": "struct",
    "totalDistribution": "struct",
    "qualityDistribution": "map"
}


# ---------- READ BRONZE ----------

def read_bronze() -> DataFrame:
    spark = get_spark()
    logging.info(f"Reading Bronze stream from: {bronze_path}")

    return (
        spark.readStream.format("delta").load(str(bronze_path))
    )


# ---------- VALIDATION RULES ----------

def apply_rules(df: DataFrame) -> (DataFrame, DataFrame): # type: ignore
    """
    Apply deterministic validation rules.
    Returns:
        valid_df: rows that passed all rules
        dq_df: rule evaluation results (one row per rule perbatch)
    """

    spark = df.sparkSession
    total_rows = df.count()
  
    # Rule 0 - Schema validation
    actual_schema = {field.name: field.dataType.simpleString() for field in df.schema.fields}

    missing_cols = [col for col in EXPECTED_SCHEMA if col not in actual_schema]
    wrong_type_cols = [
        col for col in EXPECTED_SCHEMA
        if col in actual_schema and not actual_schema[col].startswith(EXPECTED_SCHEMA[col])
    ]
    unexpected_cols = [col for col in actual_schema if col not in EXPECTED_SCHEMA]

    rule0_failed = len(missing_cols) + len(wrong_type_cols)
    rule0_passed = 1 if rule0_failed == 0 else 0

    # Rule 1 — Required fields must not be null    
    rule1 = df.filter(
        col("customerId").isNull() |
        col("contentId").isNull() |
        col("clientId").isNull() |
        col("eventDate").isNull()
    )
    rule1_failed = rule1.count()
    rule1_passed = total_rows - rule1_failed

    # Rule 2 — Bufferings must be >= 0   
    rule2 = df.filter(col("player.bufferings") < 0)
    rule2_failed = rule2.count()
    rule2_passed = total_rows - rule2_failed

    # Rule 3 — Responses must be between 0 and 1
    rule3 = df.filter(
        (col("totalDistribution.sourceTraffic.responses") < 0) |
        (col("totalDistribution.sourceTraffic.responses") > 1)
    )
    rule3_failed = rule3.count()
    rule3_passed = total_rows - rule3_failed

    # Build dq_results rows (idempotent, append-only)
    dq_rows = [
        ("schema_validation", rule0_passed, rule0_failed,
         str(missing_cols), str(wrong_type_cols), str(unexpected_cols)),
        ("required_fields_not_null", rule1_passed, rule1_failed, None, None, None),
        ("bufferings_non_negative", rule2_passed, rule2_failed, None, None, None),
        ("responses_between_0_and_1", rule3_passed, rule3_failed, None, None, None),
    ]

    dq_df = spark.createDataFrame(
        dq_rows,
        ["rule_name", "passed_count", "failed_count",
         "missing_cols", "wrong_type_cols", "unexpected_cols"]
    ).withColumn("run_timestamp", current_timestamp())

    # Valid rows = rows that passed all rules
    valid_df = df.filter(
        (col("customerId").isNotNull()) &
        (col("contentId").isNotNull()) &
        (col("clientId").isNotNull()) &
        (col("eventDate").isNotNull()) &
        (col("player.bufferings") >= 0) &
        (col("totalDistribution.sourceTraffic.responses").between(0, 1))
    )

    return valid_df, dq_df


# ---------- WRITE VALIDATED OUTPUT + DQ RESULTS ----------

def write_validated(df: DataFrame):
    logging.info(f"Writing validated Bronze → Silver staging: {bronze_validated_path}")

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_bronze_to_bronze_validated_path))
        .partitionBy("eventDate")
        .start(str(bronze_validated_path))
    )


def write_dq_results(df: DataFrame):
    logging.info(f"Writing Data Quality Results to: {dq_results_path}")

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(dq_results_path / "_checkpoint"))
        .start(str(dq_results_path))
    )


# ---------- ORCHESTRATION ----------

def run():
    logging.info("Starting Bronze Validation job")

    df_bronze = read_bronze()

    def process_batch(batch_df, batch_id):
        valid_df, dq_df = apply_rules(batch_df)

        # Write validated rows
        valid_df.write.format("delta").mode("append").save(str(bronze_validated_path))

        # Write dq results
        dq_df.write.format("delta").mode("append").save(str(dq_results_path))

    query = (
        df_bronze.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(checkpoint_bronze_to_bronze_validated_path))
        .start()
    )

    logging.info("Bronze Validation is now running")
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    run()

    
"""
Spark Session Configuration

This module initializes and configures the project's SparkSession.
It provides a single, centralized entry point for creating a Spark
environment that is fully compatible with Delta Lake.
"""

from pyspark.sql import SparkSession
import uuid


def get_spark():    
    unique_id = uuid.uuid4()

    spark = (
        SparkSession.builder
        .appName(f"HiveStreamingPipeline-{unique_id}")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", f"-Djob.id={unique_id}")
        .getOrCreate()
    )

    return spark

"""
Spark Session Test Suite

This module verifies that the project's Spark session is initialized
correctly and configured for Delta Lake operations. It ensures the
foundation of the entire Medallion pipeline is stable and compliant.

The suite covers:

1. Spark Session Initialization
   - Confirms get_spark() returns a valid, usable SparkSession.

2. Delta Lake Configuration
   - Ensures required Delta extensions and catalog settings are enabled.

3. Basic DataFrame Functionality
   - Validates that the session can create and manipulate DataFrames,
     confirming core Spark functionality is operational.

These tests guarantee that all downstream pipeline layers run on a
properly configured and reliable Spark environment.
"""


from pipeline.src.spark_session import get_spark

def test_spark_session_initializes():
    """Spark session should initialize without errors."""
    spark = get_spark()
    assert spark is not None
    assert "SparkSession" in str(type(spark))


def test_spark_has_delta_extensions():
    """Spark must be configured with Delta Lake extensions."""
    spark = get_spark()

    # Check Spark SQL extension
    assert spark.conf.get("spark.sql.extensions") == \
        "io.delta.sql.DeltaSparkSessionExtension"

    # Check Delta catalog
    assert spark.conf.get("spark.sql.catalog.spark_catalog") == \
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def test_spark_can_create_dataframe():
    """Basic DataFrame creation should work."""
    spark = get_spark()

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    assert df.count() == 2
    assert df.columns == ["id", "value"]

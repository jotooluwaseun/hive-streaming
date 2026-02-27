import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# Make src importable
ROOT = Path(__file__).resolve().parents[1] / "src"
sys.path.append(str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for all tests."""
    spark = (
        SparkSession.builder
        .appName("pytest-spark")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield spark
    spark.stop()

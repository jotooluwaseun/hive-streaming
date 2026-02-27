"""
Gold Export Utility for Power BI Dashboard

This module exports the Gold-layer QoS tables into a format suitable for Power BI. 
It initializes a Delta-enabled Spark session, loads the viewer-level and content-level 
Gold Delta tables, and writes them out as Parquet files so they can be easily consumed by 
the HiveStreaming_QoS.pbix Power BI dashboard for visualizations of key analytics from the 
output that help the understanding of the quality of service (QoS) experienced by the viewers.

"""


from pyspark.sql import SparkSession

def export_gold():
    spark = (
        SparkSession.builder.appName("ExportGoldForPowerBI")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog", )
        .getOrCreate()
    )
    
    viewer = spark.read.format("delta").load("/home/seunj/hive-streaming/pipeline/data/gold/viewer_qos")
    content = spark.read.format("delta").load("/home/seunj/hive-streaming/pipeline/data/gold/content_qos")

    viewer.write.mode("overwrite").parquet("/mnt/c/hive_streaming_export/viewer_qos")
    content.write.mode("overwrite").parquet("/mnt/c/hive_streaming_export/content_qos")

    spark.stop()

if __name__ == "__main__":
    export_gold()

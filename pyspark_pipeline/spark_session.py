"""
Spark Session Utility for Urban Mobility Lakehouse Pipeline.
Configures Spark session with Delta Lake extensions and local mode settings.
"""

import sys
import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "UrbanMobility_Lakehouse_Pipeline") -> SparkSession:
    """
    Creates and returns a SparkSession configured for local execution and Delta Lake storage.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .master("local[*]")
    )
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

if __name__ == "__main__":
    spark = get_spark_session("SparkSessionTest")
    print(f"Spark Session successfully initialized. Version: {spark.version}")
    spark.stop()

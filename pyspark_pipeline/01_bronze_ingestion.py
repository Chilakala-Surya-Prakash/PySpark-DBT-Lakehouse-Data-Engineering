"""
Bronze Layer Ingestion Script (PySpark)
----------------------------------------
Ingests raw CSV files into Delta Lake Bronze tables with schema enforcement,
ingestion timestamp metadata (_ingested_at), and source file lineage tracking.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, 
    DoubleType, TimestampType, DateType
)

def ingest_bronze_layer(spark: SparkSession, base_dir: str):
    print("=" * 60)
    print("STARTING BRONZE LAYER INGESTION (RAW CSV -> DELTA BRONZE)")
    print("=" * 60)

    raw_data_path = base_dir
    bronze_output_path = os.path.join(base_dir, "data", "bronze")
    os.makedirs(bronze_output_path, exist_ok=True)

    # 1. Customers Schema
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("city", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("last_updated_timestamp", TimestampType(), True)
    ])

    # 2. Drivers Schema
    drivers_schema = StructType([
        StructField("driver_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("vehicle_id", IntegerType(), True),
        StructField("driver_rating", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("last_updated_timestamp", TimestampType(), True)
    ])

    # 3. Locations Schema
    locations_schema = StructType([
        StructField("location_id", IntegerType(), False),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("last_updated_timestamp", TimestampType(), True)
    ])

    # 4. Payments Schema
    payments_schema = StructType([
        StructField("payment_id", IntegerType(), False),
        StructField("trip_id", IntegerType(), True),
        StructField("payment_method", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_timestamp", TimestampType(), True),
        StructField("last_updated_timestamp", TimestampType(), True)
    ])

    # 5. Trips Schema
    trips_schema = StructType([
        StructField("trip_id", IntegerType(), False),
        StructField("driver_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("vehicle_id", IntegerType(), True),
        StructField("trip_start_time", TimestampType(), True),
        StructField("trip_end_time", TimestampType(), True),
        StructField("start_location", StringType(), True),
        StructField("end_location", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("trip_status", StringType(), True),
        StructField("last_updated_timestamp", TimestampType(), True)
    ])

    # 6. Vehicles Schema
    vehicles_schema = StructType([
        StructField("vehicle_id", IntegerType(), False),
        StructField("license_plate", StringType(), True),
        StructField("model", StringType(), True),
        StructField("make", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("last_updated_timestamp", TimestampType(), True)
    ])

    datasets = {
        "customers": ("customers.csv", customers_schema),
        "drivers": ("drivers.csv", drivers_schema),
        "locations": ("locations.csv", locations_schema),
        "payments": ("payments.csv", payments_schema),
        "trips": ("trips.csv", trips_schema),
        "vehicles": ("vehicles.csv", vehicles_schema)
    }

    for name, (file_name, schema) in datasets.items():
        file_path = os.path.join(raw_data_path, file_name)
        if not os.path.exists(file_path):
            file_path = os.path.join(raw_data_path, "datasets", file_name)
        
        print(f"Ingesting raw dataset [{name}] from: {file_path}")

        df = (
            spark.read
            .option("header", "true")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(schema)
            .csv(file_path)
        )

        # Add Bronze Metadata Columns
        df_bronze = df.withColumn("_ingested_at", current_timestamp()) \
                      .withColumn("_source_file", lit(file_name))

        output_path = os.path.join(bronze_output_path, name)
        df_bronze.write.format("parquet").mode("overwrite").save(output_path)

        count = df_bronze.count()
        print(f" -> Successfully ingested [{count}] records into Bronze table: {output_path}")

    print("BRONZE LAYER INGESTION COMPLETE.\n")

if __name__ == "__main__":
    from pyspark_pipeline.spark_session import get_spark_session
    spark = get_spark_session("Bronze_Ingestion")
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    ingest_bronze_layer(spark, base_dir)
    spark.stop()

"""
Silver Layer Cleansing & Transformation Script (PySpark)
---------------------------------------------------------
Reads raw Parquet tables from Bronze layer, applies data quality cleansing,
text normalization, deduplication, and metric enrichment before saving to Silver layer.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, regexp_replace, round, 
    unix_timestamp, when, current_timestamp
)

def transform_silver_layer(spark: SparkSession, base_dir: str):
    print("=" * 60)
    print("STARTING SILVER LAYER TRANSFORMATION (BRONZE -> SILVER)")
    print("=" * 60)

    bronze_dir = os.path.join(base_dir, "data", "bronze")
    silver_dir = os.path.join(base_dir, "data", "silver")
    os.makedirs(silver_dir, exist_ok=True)

    # 1. Clean Customers
    print("Cleansing [customers] Bronze dataset...")
    df_customers = spark.read.parquet(os.path.join(bronze_dir, "customers"))
    df_customers_silver = (
        df_customers
        .dropDuplicates(["customer_id"])
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone_clean", regexp_replace(col("phone_number"), r"[^\d]", ""))
        .withColumn("city", trim(col("city")))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    df_customers_silver.write.format("parquet").mode("overwrite").save(os.path.join(silver_dir, "customers"))
    print(f" -> Saved {df_customers_silver.count()} clean customer records to Silver layer.")

    # 2. Clean Drivers
    print("Cleansing [drivers] Bronze dataset...")
    df_drivers = spark.read.parquet(os.path.join(bronze_dir, "drivers"))
    df_drivers_silver = (
        df_drivers
        .dropDuplicates(["driver_id"])
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("phone_clean", regexp_replace(col("phone_number"), r"[^\d]", ""))
        .withColumn("driver_rating", round(col("driver_rating"), 2))
        .withColumn("city", trim(col("city")))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    df_drivers_silver.write.format("parquet").mode("overwrite").save(os.path.join(silver_dir, "drivers"))
    print(f" -> Saved {df_drivers_silver.count()} clean driver records to Silver layer.")

    # 3. Clean Locations
    print("Cleansing [locations] Bronze dataset...")
    df_locations = spark.read.parquet(os.path.join(bronze_dir, "locations"))
    df_locations_silver = (
        df_locations
        .dropDuplicates(["location_id"])
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("country", trim(col("country")))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    df_locations_silver.write.format("parquet").mode("overwrite").save(os.path.join(silver_dir, "locations"))
    print(f" -> Saved {df_locations_silver.count()} clean location records to Silver layer.")

    # 4. Clean Vehicles
    print("Cleansing [vehicles] Bronze dataset...")
    df_vehicles = spark.read.parquet(os.path.join(bronze_dir, "vehicles"))
    df_vehicles_silver = (
        df_vehicles
        .dropDuplicates(["vehicle_id"])
        .withColumn("license_plate", upper(trim(col("license_plate"))))
        .withColumn("model", trim(col("model")))
        .withColumn("make", trim(col("make")))
        .withColumn("vehicle_type", trim(col("vehicle_type")))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    df_vehicles_silver.write.format("parquet").mode("overwrite").save(os.path.join(silver_dir, "vehicles"))
    print(f" -> Saved {df_vehicles_silver.count()} clean vehicle records to Silver layer.")

    # 5. Clean & Enrich Trips
    print("Cleansing & Enriching [trips] Bronze dataset...")
    df_trips = spark.read.parquet(os.path.join(bronze_dir, "trips"))
    
    # Calculate trip duration in minutes and fare per km
    df_trips_silver = (
        df_trips
        .dropDuplicates(["trip_id"])
        .withColumn("start_location", trim(col("start_location")))
        .withColumn("end_location", trim(col("end_location")))
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("trip_status", trim(col("trip_status")))
        .withColumn(
            "trip_duration_minutes",
            round((unix_timestamp(col("trip_end_time")) - unix_timestamp(col("trip_start_time"))) / 60.0, 2)
        )
        .withColumn(
            "fare_per_km",
            when(col("distance_km") > 0, round(col("fare_amount") / col("distance_km"), 2)).otherwise(0.0)
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )
    df_trips_silver.write.format("parquet").mode("overwrite").save(os.path.join(silver_dir, "trips"))
    print(f" -> Saved {df_trips_silver.count()} clean trip records to Silver layer.")

    # 6. Clean Payments
    print("Cleansing [payments] Bronze dataset...")
    df_payments = spark.read.parquet(os.path.join(bronze_dir, "payments"))
    df_payments_silver = (
        df_payments
        .dropDuplicates(["payment_id"])
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("payment_status", trim(col("payment_status")))
        .withColumn("amount", round(col("amount"), 2))
        .withColumn("_silver_processed_at", current_timestamp())
    )
    df_payments_silver.write.format("parquet").mode("overwrite").save(os.path.join(silver_dir, "payments"))
    print(f" -> Saved {df_payments_silver.count()} clean payment records to Silver layer.")

    print("SILVER LAYER TRANSFORMATION COMPLETE.\n")

if __name__ == "__main__":
    from pyspark_pipeline.spark_session import get_spark_session
    spark = get_spark_session("Silver_Transformation")
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    transform_silver_layer(spark, base_dir)
    spark.stop()

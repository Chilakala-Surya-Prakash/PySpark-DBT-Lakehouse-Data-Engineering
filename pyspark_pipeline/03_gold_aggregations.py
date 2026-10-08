"""
Gold Layer Aggregation Script (PySpark)
----------------------------------------
Reads cleaned Parquet tables from Silver layer and generates business-ready data marts,
KPI metrics, and executive summary tables saved to Gold layer.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, when, round, 
    concat_ws, current_timestamp, desc
)

def generate_gold_layer(spark: SparkSession, base_dir: str):
    print("=" * 60)
    print("STARTING GOLD LAYER AGGREGATIONS (SILVER -> GOLD MARTS)")
    print("=" * 60)

    silver_dir = os.path.join(base_dir, "data", "silver")
    gold_dir = os.path.join(base_dir, "data", "gold")
    os.makedirs(gold_dir, exist_ok=True)

    # Read Silver tables
    df_trips = spark.read.parquet(os.path.join(silver_dir, "trips"))
    df_drivers = spark.read.parquet(os.path.join(silver_dir, "drivers"))
    df_customers = spark.read.parquet(os.path.join(silver_dir, "customers"))
    df_vehicles = spark.read.parquet(os.path.join(silver_dir, "vehicles"))

    # 1. Driver Performance KPI Mart
    print("Building Gold Mart: [driver_performance_kpi]...")
    driver_kpis = (
        df_trips.groupBy("driver_id")
        .agg(
            count("trip_id").alias("total_trips_assigned"),
            _sum(when(col("trip_status") == "Completed", 1).otherwise(0)).alias("completed_trips"),
            _sum(when(col("trip_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_trips"),
            _sum(when(col("trip_status") == "Ongoing", 1).otherwise(0)).alias("ongoing_trips"),
            round(_sum(when(col("trip_status") == "Completed", col("fare_amount")).otherwise(0.0)), 2).alias("total_revenue_generated"),
            round(avg("distance_km"), 2).alias("avg_trip_distance_km"),
            round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_mins")
        )
    )

    df_gold_driver_performance = (
        df_drivers.alias("d")
        .join(driver_kpis.alias("k"), col("d.driver_id") == col("k.driver_id"), "left")
        .select(
            col("d.driver_id"),
            concat_ws(" ", col("d.first_name"), col("d.last_name")).alias("driver_name"),
            col("d.driver_rating"),
            col("d.city").alias("operating_city"),
            col("k.total_trips_assigned"),
            col("k.completed_trips"),
            col("k.cancelled_trips"),
            col("k.total_revenue_generated"),
            round(
                (col("k.cancelled_trips") / when(col("k.total_trips_assigned") > 0, col("k.total_trips_assigned")).otherwise(1)) * 100, 
                2
            ).alias("cancellation_rate_pct")
        )
        .orderBy(desc("total_revenue_generated"))
    )
    df_gold_driver_performance.write.format("parquet").mode("overwrite").save(os.path.join(gold_dir, "driver_performance_kpi"))
    print(f" -> Generated [driver_performance_kpi] with {df_gold_driver_performance.count()} rows.")

    # 2. Customer LTV & Engagement Mart
    print("Building Gold Mart: [customer_ltv_summary]...")
    customer_kpis = (
        df_trips.groupBy("customer_id")
        .agg(
            count("trip_id").alias("total_rides_booked"),
            round(_sum("fare_amount"), 2).alias("total_customer_spend"),
            round(avg("fare_amount"), 2).alias("avg_spend_per_ride"),
            round(_sum("distance_km"), 2).alias("total_distance_traveled_km")
        )
    )

    df_gold_customer_ltv = (
        df_customers.alias("c")
        .join(customer_kpis.alias("ck"), col("c.customer_id") == col("ck.customer_id"), "inner")
        .select(
            col("c.customer_id"),
            concat_ws(" ", col("c.first_name"), col("c.last_name")).alias("customer_name"),
            col("c.email"),
            col("c.city"),
            col("c.signup_date"),
            col("ck.total_rides_booked"),
            col("ck.total_customer_spend"),
            col("ck.avg_spend_per_ride"),
            col("ck.total_distance_traveled_km")
        )
        .orderBy(desc("total_customer_spend"))
    )
    df_gold_customer_ltv.write.format("parquet").mode("overwrite").save(os.path.join(gold_dir, "customer_ltv_summary"))
    print(f" -> Generated [customer_ltv_summary] with {df_gold_customer_ltv.count()} rows.")

    # 3. Vehicle Category Revenue Analytics
    print("Building Gold Mart: [revenue_by_vehicle_type]...")
    df_gold_vehicle_revenue = (
        df_trips.alias("t")
        .join(df_vehicles.alias("v"), col("t.vehicle_id") == col("v.vehicle_id"), "inner")
        .groupBy("v.vehicle_type")
        .agg(
            count("t.trip_id").alias("total_trips"),
            round(_sum("t.fare_amount"), 2).alias("total_revenue"),
            round(avg("t.fare_amount"), 2).alias("avg_fare_per_trip"),
            round(avg("t.distance_km"), 2).alias("avg_distance_km")
        )
        .orderBy(desc("total_revenue"))
    )
    df_gold_vehicle_revenue.write.format("parquet").mode("overwrite").save(os.path.join(gold_dir, "revenue_by_vehicle_type"))
    print(f" -> Generated [revenue_by_vehicle_type] with {df_gold_vehicle_revenue.count()} rows.")

    print("GOLD LAYER AGGREGATIONS COMPLETE.\n")

if __name__ == "__main__":
    from pyspark_pipeline.spark_session import get_spark_session
    spark = get_spark_session("Gold_Aggregations")
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    generate_gold_layer(spark, base_dir)
    spark.stop()

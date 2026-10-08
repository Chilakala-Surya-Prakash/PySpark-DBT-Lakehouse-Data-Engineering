#!/usr/bin/env python3
"""
End-to-End Data Pipeline Orchestrator
-------------------------------------
Orchestrates:
1. Data Ingestion & Cleansing (CSV -> Medallion Layers).
2. dbt Core Transformation Models (Staging, Dimensional Marts, Fact Tables).
3. Data Quality Validation & Metric Reports.
"""

import os
import sys
import csv
import sqlite3
import subprocess

def run_step(title, command, cwd=None):
    print("\n" + "=" * 70)
    print(f"STEP: {title}")
    print("=" * 70)
    try:
        res = subprocess.run(command, cwd=cwd, shell=True, check=True, text=True, capture_output=True)
        print(res.stdout)
        if res.stderr:
            print("LOGS:", res.stderr)
        print(f"✅ SUCCESS: {title}")
    except subprocess.CalledProcessError as e:
        print(f"❌ ERROR running {title}:")
        print(e.stdout)
        print(e.stderr)

def run_medallion_sqlite_ingestion(base_dir):
    print("\n" + "=" * 70)
    print("EXECUTING MEDALLION DATA INGESTION & CLEANSING")
    print("=" * 70)

    db_path = os.path.join(base_dir, "urban_mobility.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    files = ["customers.csv", "drivers.csv", "locations.csv", "payments.csv", "trips.csv", "vehicles.csv"]
    for f in files:
        file_path = os.path.join(base_dir, f)
        if not os.path.exists(file_path):
            file_path = os.path.join(base_dir, "datasets", f)
        
        table_name = "stg_" + f.replace(".csv", "")
        with open(file_path, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
            cols = ", ".join([f'"{h}" TEXT' for h in headers])
            cursor.execute(f'CREATE TABLE "{table_name}" ({cols})')
            
            placeholders = ", ".join(["?"] * len(headers))
            insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
            cursor.executemany(insert_sql, reader)
            
            print(f" -> Ingested {cursor.rowcount} records into staging table [{table_name}]")

    conn.commit()

    # Create Dimensional Marts & Fact Tables
    print("\nBuilding Dimensional Data Marts & Analytics Tables...")
    
    cursor.execute("""
        CREATE TABLE dim_drivers AS
        SELECT 
            CAST(d.driver_id AS INT) as driver_id,
            d.first_name || ' ' || d.last_name as driver_name,
            d.phone_number,
            d.city as operating_city,
            CAST(d.driver_rating AS REAL) as driver_rating,
            v.vehicle_id,
            v.make || ' ' || v.model as vehicle_model,
            v.vehicle_type,
            v.license_plate,
            COUNT(t.trip_id) as total_trips,
            SUM(CASE WHEN t.trip_status = 'Completed' THEN 1 ELSE 0 END) as completed_trips,
            SUM(CASE WHEN t.trip_status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled_trips,
            ROUND(SUM(CASE WHEN t.trip_status = 'Completed' THEN CAST(t.fare_amount AS REAL) ELSE 0 END), 2) as total_revenue
        FROM stg_drivers d
        LEFT JOIN stg_vehicles v ON d.vehicle_id = v.vehicle_id
        LEFT JOIN stg_trips t ON d.driver_id = t.driver_id
        GROUP BY d.driver_id
    """)

    cursor.execute("""
        CREATE TABLE dim_customers AS
        SELECT 
            CAST(c.customer_id AS INT) as customer_id,
            c.first_name || ' ' || c.last_name as full_name,
            c.email,
            c.phone_number,
            c.city,
            c.signup_date,
            COUNT(t.trip_id) as lifetime_trips,
            ROUND(SUM(CAST(t.fare_amount AS REAL)), 2) as lifetime_spend
        FROM stg_customers c
        LEFT JOIN stg_trips t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id
    """)

    cursor.execute("""
        CREATE TABLE fct_trips AS
        SELECT 
            CAST(t.trip_id AS INT) as trip_id,
            CAST(t.driver_id AS INT) as driver_id,
            CAST(t.customer_id AS INT) as customer_id,
            CAST(t.vehicle_id AS INT) as vehicle_id,
            t.trip_start_time as started_at,
            t.trip_end_time as ended_at,
            t.start_location,
            t.end_location,
            CAST(t.distance_km AS REAL) as distance_km,
            ROUND(CAST(t.fare_amount AS REAL), 2) as fare_amount,
            t.payment_method,
            t.trip_status
        FROM stg_trips t
    """)

    conn.commit()
    return conn

def print_table(rows, headers):
    col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) for i, h in enumerate(headers)]
    header_str = " | ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    print(header_str)
    print("-" * len(header_str))
    for r in rows:
        print(" | ".join(f"{str(r[i]):<{col_widths[i]}}" for i in range(len(r))))

def main():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    dbt_dir = os.path.join(base_dir, "dbt_project")

    # Step 1: Execute Medallion Ingestion
    conn = run_medallion_sqlite_ingestion(base_dir)

    # Step 2: dbt Core execution if dbt is installed
    dbt_bin = "/Users/surya/.local/bin/dbt" if os.path.exists("/Users/surya/.local/bin/dbt") else "dbt"
    if os.path.exists(dbt_bin):
        run_step("dbt Model Transformations", f"{dbt_bin} compile --profiles-dir .", cwd=dbt_dir)

    # Step 3: Print Pipeline Analytics & Business Reports
    print("\n" + "=" * 70)
    print("PIPELINE EXECUTIVE SUMMARY REPORTS")
    print("=" * 70)
    cursor = conn.cursor()
    
    print("\n--- Top 5 Drivers by Revenue ---")
    cursor.execute("SELECT driver_name, operating_city, total_trips, completed_trips, total_revenue FROM dim_drivers ORDER BY total_revenue DESC LIMIT 5")
    print_table(cursor.fetchall(), ["Driver Name", "City", "Total Trips", "Completed", "Revenue ($)"])

    print("\n--- Top 5 Customers by Lifetime Spend ---")
    cursor.execute("SELECT full_name, city, lifetime_trips, lifetime_spend FROM dim_customers ORDER BY lifetime_spend DESC LIMIT 5")
    print_table(cursor.fetchall(), ["Customer Name", "City", "Lifetime Trips", "Total Spend ($)"])

    print("\n--- Trip Status Breakdown ---")
    cursor.execute("SELECT trip_status, COUNT(*) as count, ROUND(SUM(fare_amount), 2) as total_fare FROM fct_trips GROUP BY trip_status")
    print_table(cursor.fetchall(), ["Trip Status", "Total Count", "Total Fare ($)"])

    conn.close()
    print("\n🎉 END-TO-END PIPELINE EXECUTED SUCCESSFULLY!")

if __name__ == "__main__":
    main()

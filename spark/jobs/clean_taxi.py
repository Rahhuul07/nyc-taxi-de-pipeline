import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    concat_ws,
    sha2,
    lit,
    current_timestamp,
)


def main():
    # -----------------------------
    # Windows + Hadoop helper setup
    # -----------------------------
    # These help Spark find winutils on Windows.
    # They do NOT hurt on other OS.
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ["PATH"] = os.environ.get("PATH", "") + r";C:\hadoop\bin"

    # -----------------------------
    # Input / Output paths (project root)
    # -----------------------------
    input_path = r"data\bronze\yellow_tripdata.parquet"
    output_path = r"data\silver\taxi_trips_clean_parquet"

    # -----------------------------
    # Spark session (memory increased to fix Java heap OOM)
    # -----------------------------
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Clean - Local")
        # Increase JVM heap to avoid Parquet write OOM
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        # Reduce shuffle partitions (local machine)
        .config("spark.sql.shuffle.partitions", "8")
        # Use G1GC (often helps with big Parquet writes)
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------
    # Read Bronze
    # -----------------------------
    df = spark.read.parquet(input_path)

    # -----------------------------
    # Convert timestamps (Yellow taxi default columns)
    # -----------------------------
    df = (
        df.withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
          .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    )

    # -----------------------------
    # Basic cleaning (minimum viable)
    # -----------------------------
    df = (
        df.filter(col("pickup_datetime").isNotNull())
          .filter(col("dropoff_datetime").isNotNull())
          .filter(col("trip_distance").isNotNull())
          .filter(col("trip_distance") >= 0)
    )

    # -----------------------------
    # Create stable trip_id (MERGE key)
    # -----------------------------
    df = df.withColumn(
        "trip_id",
        sha2(
            concat_ws(
                "||",
                col("VendorID").cast("string"),
                col("pickup_datetime").cast("string"),
                col("dropoff_datetime").cast("string"),
                col("PULocationID").cast("string"),
                col("DOLocationID").cast("string"),
                col("fare_amount").cast("string"),
                col("total_amount").cast("string"),
            ),
            256,
        ),
    )

    # -----------------------------
    # Select + cast to match Snowflake stage/clean schema
    # -----------------------------
    cleaned = (
        df.select(
            col("trip_id"),
            col("VendorID").cast("string").alias("vendor_id"),
            col("pickup_datetime"),
            col("dropoff_datetime"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("PULocationID").cast("int").alias("pu_location_id"),
            col("DOLocationID").cast("int").alias("do_location_id"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("total_amount").cast("double").alias("total_amount"),
            lit("yellow_tripdata.parquet").alias("source_file"),
            current_timestamp().alias("ingested_at"),
        )
        .dropDuplicates(["trip_id"])
    )

    # -----------------------------
    # Reduce partitions to avoid write OOM / too many files locally
    # -----------------------------
    cleaned = cleaned.coalesce(4)

    # -----------------------------
    # Write Silver (local parquet)
    # -----------------------------
    cleaned.write.mode("overwrite").parquet(output_path)

    # -----------------------------
    # Simple validation output
    # -----------------------------
    rows = cleaned.count()
    print(f"✅ SUCCESS: wrote cleaned parquet to: {output_path}")
    print(f"✅ Rows written: {rows}")

    spark.stop()


if __name__ == "__main__":
    main()

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, floor, sum as _sum, count as _count, avg, lit

def main():
    # Initialize Spark Session
    # In a real cluster, configuration would be handled by spark-submit arguments.
    spark = SparkSession.builder \
        .appName("Resilient_Global_Telemetry_Platform") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()

    # Define Checkpoint Directory (Required for Part 3.4)
    checkpoint_dir = "/app/data/checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    print("--- Starting Telemetry Processing Pipeline ---")

    # ---------------------------------------------------------
    # 0. Data Ingestion (Mocking historical batch dataset)
    # ---------------------------------------------------------
    print("Generating mock telemetry data...")
    data = [
        ("Model_A", "TRUCK_001", 95.5, 65.0),
        ("Model_A", "TRUCK_001", 96.0, 66.0),
        ("Model_B", "TRUCK_002", 88.0, 55.0),
        ("Model_B", "TRUCK_003", 89.5, 54.0),
    ] * 1000 # Artificially inflates the dataset for testing
    
    columns = ["vehicle_model", "truck_id", "engine_temp", "speed"]
    raw_df = spark.createDataFrame(data, columns)

    # ---------------------------------------------------------
    # 1. Transformations & Actions (Narrow vs Wide Dependencies)
    # ---------------------------------------------------------
    # NARROW DEPENDENCY: filter() and withColumn() do not require data shuffling.
    # Data is evaluated locally on the partitions.
    clean_df = raw_df.filter(col("engine_temp").isNotNull())

    # ---------------------------------------------------------
    # 2. Optimization: Mitigating Severe Data Skew with Salting
    # ---------------------------------------------------------
    SALT_BINS = 10
    
    # NARROW DEPENDENCY: Adding a random salt value (0 to 9) to distribute skewed keys
    salted_df = clean_df.withColumn("salt", floor(rand() * SALT_BINS))

    # WIDE DEPENDENCY (Shuffle Phase 1): Group by both model AND salt.
    # This forces a network shuffle but breaks up the massive logs from specific trucks.
    partial_agg_df = salted_df.groupBy("vehicle_model", "salt").agg(
        _sum("engine_temp").alias("sum_temp"),
        _count("engine_temp").alias("count_temp")
    )

    # WIDE DEPENDENCY (Shuffle Phase 2): Group by model only for the final average.
    # We use repartitioning here to enforce Hash Partitioning on the shuffle key.
    final_agg_df = partial_agg_df.repartition(10, "vehicle_model") \
        .groupBy("vehicle_model").agg(
        (_sum("sum_temp") / _sum("count_temp")).alias("avg_engine_temp")
    )

    print("Final Aggregated Data (Action triggered):")
    # ACTION: show() triggers the execution of the DAG.
    final_agg_df.show()

    # ---------------------------------------------------------
    # 4. Checkpointing: Simulating Iterative Processing
    # ---------------------------------------------------------
    print("Simulating deep iterative processing...")
    iterative_df = final_agg_df

    # Simulating a machine learning loop that updates state heavily
    for i in range(1, 21):
        # NARROW DEPENDENCY: Adding columns iteratively
        iterative_df = iterative_df.withColumn(f"simulated_weight_{i}", col("avg_engine_temp") * (1 + (i/100)))
        
        # Checkpoint every 5 iterations to truncate the DAG
        if i % 5 == 0:
            print(f"Iteration {i}: Checkpointing RDD to truncate DAG liability...")
            iterative_df = iterative_df.checkpoint()

    # Final Action to trigger the iterative graph
    print("Iterative processing complete. Final schema:")
    iterative_df.printSchema()

    # Write output (ACTION)
    output_path = "/app/data/processed/telemetry_avg_temp"
    iterative_df.write.mode("overwrite").parquet(output_path)
    print(f"Data successfully written to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
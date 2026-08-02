from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ResilientTelemetryPlatform") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# 3.4 Checkpointing Setup: Define HDFS or S3 directory for checkpointing
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")

# 3.1 Transformations and Actions (Ingestion)
# Narrow Dependency: Reading data involves no network shuffle.
df = spark.read.csv("telemetry_data.csv", header=True, inferSchema=True)

# 3.2 Optimization: Mitigating severe data skew with a Salting Strategy
# We append a random integer (0-9) to the vehicle model to distribute the heavily skewed logs across multiple partitions.
# Narrow Dependency: withColumn acts on data locally on each node.
SALT_BINS = 10
salted_df = df.withColumn("salt", F.floor(F.rand() * SALT_BINS))

# Phase 1 of Aggregation (Wide Dependency: GroupBy triggers a Shuffle)
# Hash partitioning routes data based on the hash of (vehicle_model, salt).
partial_agg = salted_df.groupBy("vehicle_model", "salt").agg(
    F.sum("engine_heat").alias("sum_heat"),
    F.count("engine_heat").alias("count_heat")
)

# Phase 2 of Aggregation (Wide Dependency: Second Shuffle)
# We group by just the vehicle model to combine the salted bins for the final average.
final_agg = partial_agg.groupBy("vehicle_model").agg(
    (F.sum("sum_heat") / F.sum("count_heat")).alias("avg_engine_heat")
)

# 3.4 Checkpointing Execution: Truncating the DAG
final_agg = final_agg.checkpoint()

# Action (Triggers Lazy Evaluation)
final_agg.show()
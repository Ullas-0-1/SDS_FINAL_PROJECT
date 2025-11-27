import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, approx_count_distinct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ---------------------------------------------------------------
# 1. SETUP SPARK (WITH THE FIX)
# ---------------------------------------------------------------
spark = SparkSession.builder \
    .appName("Query6_Mono_PySpark") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate() # ^^^ THIS LINE FIXES THE TIMESTAMP ERROR

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 2. DEFINE SCHEMA
# ---------------------------------------------------------------
# Note: I changed names to match your CSV Header "Unique ID" vs "unique_id"
# Spark's CSV reader is case-sensitive when mapping.
tower_schema = StructType([
    StructField("Unique ID", LongType(), True),
    StructField("Tower", StringType(), True),
    StructField("Start Timestamp", StringType(), True),
    StructField("End Timestamp", StringType(), True)
])

# ---------------------------------------------------------------
# 3. READ STREAM
# ---------------------------------------------------------------
# UPDATE THIS PATH
INPUT_PATH = "/Users/ullas/Desktop/SDS_FINAL_PROJECT/SpanStreamGenerator/mono_signal_towers"

print(f">>> Reading from: {INPUT_PATH}")

raw_stream = spark.readStream \
    .format("csv") \
    .schema(tower_schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load(INPUT_PATH)

# ---------------------------------------------------------------
# 4. QUERY LOGIC
# ---------------------------------------------------------------
# We rename columns back to simple names for easier coding
processed_stream = raw_stream \
    .withColumnRenamed("Unique ID", "unique_id") \
    .withColumnRenamed("Tower", "tower_id") \
    .withColumn("event_time", to_timestamp(col("End Timestamp"), "yyyyMMddHHmmssSSS")) \
    .withWatermark("event_time", " 1 seconds")

# Logic: HyperLogLog (Approx Count) to fix Memory Issue
mobility_query = processed_stream \
    .groupBy(
        window(col("event_time"), "5 seconds"),
        col("unique_id")
    ) \
    .agg(approx_count_distinct(col("tower_id")).alias("distinct_towers")) \
    .filter(col("distinct_towers") > 2) 

# ---------------------------------------------------------------
# 5. START STREAM
# ---------------------------------------------------------------
###try both append and update modes --as the way of triggering is differnt in both of them as seen here clearly
query = mobility_query.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 second") \
    .start()

print(">>> Stream Started! Waiting for data...")

# Monitor Loop
while query.isActive:
    progress = query.lastProgress
    if progress:
        batch_id = progress["batchId"]
        num_rows = progress["numInputRows"]
        processed_rate = progress["processedRowsPerSecond"]
        latency = progress["durationMs"].get("triggerExecution", 0)

        print(f"--- Batch: {batch_id} | Rows: {num_rows} | Rate: {processed_rate:.2f} rows/s | Latency: {latency} ms ---")
    
    time.sleep(2)

query.awaitTermination()






####mfor deciding window size -- keep max duration in mind to check for saninty and total itreatinos as well
###  trigger time dont know yet what will happen

### for deciding watermark -- mono take end time and keep low
##for bi tou can do anyone of them as seen here 
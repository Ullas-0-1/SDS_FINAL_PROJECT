'''
Experiment A: Watermark on START TIME ("Enter" Events)

Logic: We count the tower as soon as the user Enters (Event Type 0).

Latency: Low. The C-generator writes the "Enter" file immediately.

Watermark: Can be Low (e.g., 2 seconds) because there is no delay between the event happening and the file being written.

Save as: query6_bi_start_strategy.py
'''



import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, approx_count_distinct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

spark = SparkSession.builder \
    .appName("Query6_Bi_Start_Strategy") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema matches Bi-Signal CSV
bi_schema = StructType([
    StructField("Unique ID", LongType(), True),
    StructField("Tower", StringType(), True),
    StructField("Event Type", IntegerType(), True), 
    StructField("Timestamp", StringType(), True)
])

# !!! UPDATE PATH to bi_signal_towers !!!
INPUT_PATH = "/Users/ullas/Desktop/SDS_FINAL_PROJECT/SpanStreamGenerator/bi_signal_towers"
print(f">>> STRATEGY A (START TIME): Reading from {INPUT_PATH}")

raw_stream = spark.readStream \
    .format("csv") \
    .schema(bi_schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load(INPUT_PATH)

processed_stream = raw_stream \
    .withColumnRenamed("Unique ID", "unique_id") \
    .withColumnRenamed("Tower", "tower_id") \
    .withColumnRenamed("Event Type", "event_type") \
    .withColumn("event_time", to_timestamp(col("Timestamp"), "yyyyMMddHHmmssSSS")) \
    .withWatermark("event_time", "0.001 seconds") # Low watermark is fine here!

# LOGIC: Filter for "Enter" (0)
# This simulates "Real Time" tracking. We know where they are immediately.
mobility_query = processed_stream \
    .filter(col("event_type") == 0) \
    .groupBy(
        window(col("event_time"), "5 seconds"),
        col("unique_id")
    ) \
    .agg(approx_count_distinct(col("tower_id")).alias("distinct_towers")) \
    .filter(col("distinct_towers") > 2)

query = mobility_query.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 second") \
    .start()

while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"--- Batch: {progress['batchId']} | Rows: {progress['numInputRows']} | Latency: {progress['durationMs'].get('triggerExecution', 0)} ms ---")
    time.sleep(2)
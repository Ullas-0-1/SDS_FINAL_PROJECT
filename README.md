SDS Final Project: SpanStream Benchmark Evaluation
==================================================

Project Overview
----------------

This project implements and evaluates the **SpanStream Benchmark** for Streaming Data Systems. It focuses on processing **Spanning Events** (events with a duration) using **Apache Spark Structured Streaming** and **Apache Flink**, comparing their performance and architectural patterns against a **PostgreSQL** baseline.

We specifically target two complex query groups:

1.  **Query 6 (High Mobility):** Windowed distinct count aggregation to detect users switching cell towers frequently.
    
2.  **Query 9 (Back-to-Back Sessions):** Complex stateful pattern matching to detect consecutive calls with a gap <= 10 seconds.
    

The project evaluates two data formats:

*   **Mono-Signal:** Pre-calculated intervals (Start Time, End Time in one row).
    
*   **Bi-Signal:** Raw events (Start Event and End Event arrive separately).
    

Repository Structure
--------------------

**Root Directory (/)** Contains the Data Generator and Spark Streaming implementations.

*   stream.c: The modified C-based synthetic data generator.
    
*   cdr\_generator: The compiled executable for data generation.
    
*   query6\_mono.py: PySpark implementation for Query 6 (Mono-signal).
    
*   query6\_bi.py: PySpark implementation for Query 6 (Bi-signal).
    
*   Query9Mono.scala & query9\_mono.sh: Scala implementation and execution script for Query 9 (Mono).
    
*   Query9Bi.scala & query9\_bi.sh: Scala implementation and execution script for Query 9 (Bi).
    

**sql\_scripts/** Contains the baseline logic for verification.

*   create\_tables.sql: Schema definitions for Mono-signal (Calls/Towers) and Bi-signal tables.
    
*   benchmark\_queries.sql: The 4 verified queries used for sanity checks.
    

**flink\_streaming/** Contains the Apache Flink implementation (PyFlink Table API).

*   mono\_tower\_switching.py: Tumble window aggregation for Query 6.
    
*   bi\_back\_to\_back.py: Stream-Stream Interval Join logic for Query 9.
    

Prerequisites
-------------

*   **Apache Spark** (3.5.0 or higher)
    
*   **Apache Flink** (1.17 or higher)
    
*   **PostgreSQL** (14 or higher)
    
*   **Java 17** (Required for Spark/Scala compatibility)
    
*   **Python 3.8+** (With pandas and pyarrow installed)
    
*   **GCC Compiler** (For the data generator)
    

How to Run
----------

### Step 1: Generate Data

Compile and run the generator in the root directory. Ensure you specify a multiplier large enough to avoid ID collisions.



`   # Compile  gcc stream.c -o cdr_generator -lm  # Run (Follow prompts example: Throughput=10000, Iterations=60, Duration=5)  ./cdr_generator   `

_This will create folders: mono\_signal\_calls, mono\_signal\_towers, bi\_signal\_calls, bi\_signal\_towers._

### Step 2: PostgreSQL Verification (Sanity Check)

Load data into Postgres to establish the "Ground Truth."

1.  **Merge CSVs:** Use awk to merge the generated files into single CSVs (removing headers).
    
2.  SQL-- In psql console\\i sql\_scripts/create\_tables.sqlCOPY mono\_calls FROM '/path/to/merged\_calls.csv' DELIMITER ',' CSV;
    
3.  **Run Verification:** Execute the queries in sql\_scripts/benchmark\_queries.sql and record the counts to compare with Spark outputs.
    

### Step 3: Run Spark Streaming

**Running Query 6 (High Mobility) - Python** This query uses **PySpark** with approx\_count\_distinct (HyperLogLog) optimization.

`   # Ensure SPARK_HOME is set  export SPARK_HOME=/path/to/your/spark  # Run Mono-Signal  $SPARK_HOME/bin/spark-submit --master "local[*]" query6_mono.py  # Run Bi-Signal  $SPARK_HOME/bin/spark-submit --master "local[*]" query6_bi.py   `

**Running Query 9 (Back-to-Back Sessions) - Scala** This query uses **Scala** for type-safe state management. Use the provided shell scripts to compile and run automatically.


`   # Run Mono-Signal (Stateful MapGroupsWithState)  chmod +x query9_mono.sh  ./query9_mono.sh  # Run Bi-Signal (Complex Re-construction)  chmod +x query9_bi.sh  ./query9_bi.sh   `

_Logic: These scripts use flatMapGroupsWithState to maintain a per-user "Locker" (State) containing the last call's end time._

### Step 4: Run Flink Streaming

The Flink implementation uses the Python Table API.

Bash

`   cd flink_streaming  # Run Query 6 (Mono)  python3 mono_tower_switching.py  # Run Query 9 (Bi - Reconstructed)  python3 bi_back_to_back.py   `

Key Configuration Notes
-----------------------

*   **Watermarks:** Adjusted based on the dataset type. Mono-signal uses End Timestamp (Low latency), while Bi-signal uses Start Timestamp (High latency waiting for completion).
    
*   **Checkpoints:** The Spark scripts are configured to use local checkpoint directories. Ensure you clear these directories (rm -rf chk\_\*) if you change logic or data to avoid state conflicts.
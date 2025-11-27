import os
from pyflink.table import EnvironmentSettings, TableEnvironment
# Ensure config.py is in the same directory and contains BI_SIGNAL_FILE
from config import * # ---------------- SETUP --------------------
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

t_env.get_config().set("parallelism.default", "1") 
t_env.get_config().set("table.dynamic-table.options.enabled", "true")

# ---------------- 1. SOURCE DDL --------------------
t_env.execute_sql(f"""
CREATE TABLE bisignal (
    unique_id BIGINT,
    signal_type INT,
    caller BIGINT,
    callee BIGINT,
    event_ts STRING,
    disposition STRING,
    imei BIGINT,
    row_id BIGINT,
    event_timestamp AS TO_TIMESTAMP(event_ts, 'yyyyMMddHHmmssSSS'),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' MINUTE
)
WITH (
  'connector'='filesystem',
  'path'='{BI_SIGNAL_FILE}',
  'format'='csv',
  'csv.ignore-parse-errors'='true'
)
""")

# ---------------- 2. LOGIC: RECONSTRUCT & ANALYZE --------------------
# Strategy: 
# 1. Separate Start (0) and End (1) events.
# 2. JOIN them on unique_id to reconstruct the full call session (Start Time & End Time).
# 3. Apply the standard Mono-signal LAG logic on these reconstructed sessions.
#    This guarantees we handle overlapping calls exactly like the mono code (by sorting by Start Time).

query = """
WITH ReconstructedCalls AS (
    SELECT 
        S.caller,
        S.unique_id,
        S.event_timestamp AS start_time,
        E.event_timestamp AS end_time
    FROM bisignal S
    JOIN bisignal E 
      ON S.unique_id = E.unique_id 
     AND S.caller = E.caller
     AND E.event_timestamp BETWEEN S.event_timestamp AND S.event_timestamp + INTERVAL '4' MINUTE
    WHERE S.signal_type = 0 AND S.disposition = 'connected'
      AND E.signal_type = 1 AND E.disposition = 'connected'
)
SELECT 
    caller
FROM (
    SELECT 
        caller,
        start_time,
        end_time,
        -- Mono-Logic: Look at the End Time of the PREVIOUS call (Ordered by Start Time)
        LAG(end_time, 1) OVER (
            PARTITION BY caller 
            ORDER BY start_time
        ) AS prev_end
    FROM ReconstructedCalls
)
WHERE prev_end IS NOT NULL
  AND start_time >= prev_end -- Strict sequence (No overlap)
  AND TIMESTAMPDIFF(SECOND, prev_end, start_time) <= 10
"""

result = t_env.execute_sql(query)

# ---------------- 3. PRINT RESULTS --------------------
print("Searching for back-to-back sessions (Bi-Signal Reconstructed)...")
result.print()

# ---------------- 4. RUNTIME METRICS --------------------
try:
    job_client = result.get_job_client()
    job_result = job_client.get_job_execution_result().result()
    runtime_ms = job_result.get_net_runtime()
    print(f"\nStatus: Finished")
    print(f"Net Job Runtime: {runtime_ms} ms")
except:
    print("Job is running in streaming mode.")
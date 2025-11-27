import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from config import * # MONO_SIGNAL_FILE

# ---------------- CONFIGURATION --------------------
# Update this to your actual path
# ---------------- SETUP --------------------
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Set parallelism to 1 to ensure order is easy to see in the print output
t_env.get_config().set("parallelism.default", "1") 
t_env.get_config().set("table.dynamic-table.options.enabled", "true")

# ---------------- 1. SOURCE DDL --------------------
t_env.execute_sql(f"""
CREATE TABLE mono (
    unique_id BIGINT,
    start_ts STRING,
    end_ts STRING,
    caller BIGINT,
    callee BIGINT,
    disposition STRING,
    imei BIGINT,
    -- Computed Columns
    start_time AS TO_TIMESTAMP(start_ts, 'yyyyMMddHHmmssSSS'),
    end_time AS TO_TIMESTAMP(end_ts, 'yyyyMMddHHmmssSSS'),
    -- Watermark matches the column we sort by (start_time)
    WATERMARK FOR start_time AS start_time - INTERVAL '15' MINUTE
)
WITH (
  'connector'='filesystem',
  'path'='{MONO_SIGNAL_FILE}',
  'format'='csv',
  'csv.ignore-first-line'='true'
)
""")

# ---------------- 2. LAG QUERY --------------------
# Logic:
# 1. Partition by caller, Order by start_time.
# 2. Use LAG to get the 'end_time' of the previous call.
# 3. Filter where (Current Start - Previous End) <= 10 seconds.

query = """
SELECT 
    caller
FROM (
    SELECT 
        caller,
        start_time,
        end_time,
        -- Look at the PREVIOUS row's end_time
        LAG(end_time, 1) OVER (
            PARTITION BY caller 
            ORDER BY start_time
        ) AS prev_end
    FROM mono
)
WHERE prev_end IS NOT NULL
  AND start_time >= prev_end -- Ensure calls don't overlap (strict sequence)
  AND TIMESTAMPDIFF(SECOND, prev_end, start_time) <= 10
"""

result = t_env.execute_sql(query)

# ---------------- 3. PRINT RESULTS --------------------
print("Searching for back-to-back sessions (Mono-Signal via LAG)...")
result.print()

# ---------------- 4. RUNTIME METRICS --------------------
# Note: In streaming mode, this part runs after job completion
try:
    job_client = result.get_job_client()
    job_result = job_client.get_job_execution_result().result()
    runtime_ms = job_result.get_net_runtime()
    print(f"\nStatus: Finished")
    print(f"Net Job Runtime: {runtime_ms} ms")
except:
    print("Job is running in streaming mode.")
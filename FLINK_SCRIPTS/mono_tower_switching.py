# mono_tower_switching_processing_latency.py
from pyflink.table import EnvironmentSettings, TableEnvironment
from config import *

# ------------------ ENVIRONMENT ------------------
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().set("table.dynamic-table.options.enabled", "true")
t_env.get_config().set("parallelism.default", "1")

# ------------------ SOURCE TABLE ------------------
t_env.execute_sql(f"""
CREATE TABLE tower_signal (
    unique_id BIGINT,
    tower STRING,
    start_ts STRING,
    end_ts STRING,
    start_time AS TO_TIMESTAMP(CAST(start_ts AS STRING), 'yyyyMMddHHmmssSSS'),
    ingestion_time AS PROCTIME(), 
    WATERMARK FOR start_time AS start_time - INTERVAL '10' MINUTE
)
WITH (
  'connector' = 'filesystem',
  'path' = '{TOWER_SIGNAL_FILE}',
  'format' = 'csv',
  'csv.ignore-first-line' = 'true'
)
""")

# ------------------ QUERY ------------------
query = """
SELECT
  TUMBLE_START(start_time, INTERVAL '5' MINUTE) AS window_start,
  TUMBLE_END(start_time, INTERVAL '5' MINUTE) AS window_end,
  unique_id AS user_id,
  COUNT(DISTINCT tower) AS distinct_tower_visits
FROM tower_signal
WHERE start_time IS NOT NULL
GROUP BY
  TUMBLE(start_time, INTERVAL '5' MINUTE),
  unique_id
HAVING COUNT(DISTINCT tower) > 5;
"""

result = t_env.execute_sql(query)

# ------------------ PRINT RESULTS ------------------
result.print()

# ------------------ RUNTIME ------------------
job_client = result.get_job_client()
exec_res = job_client.get_job_execution_result().result()
print(f"Total Job Runtime: {exec_res.get_net_runtime()} ms")
# bi_tower_switching_processing_latency.py
from pyflink.table import EnvironmentSettings, TableEnvironment
from config import *   # BI_TOWER_FILE, OUTPUT_DIR

# ------------------ ENVIRONMENT ------------------
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)
t_env.get_config().set("table.dynamic-table.options.enabled", "true")
t_env.get_config().set("parallelism.default", "1")

# ------------------ SOURCE TABLE (Bi-signal tower events) ------------------
t_env.execute_sql(f"""
CREATE TABLE bisignal_tower (
    unique_id BIGINT,
    event_type INT,
    tower STRING,
    ts STRING,
    event_time AS TO_TIMESTAMP(CAST(ts AS STRING), 'yyyyMMddHHmmssSSS'),
    ingestion_time AS PROCTIME(),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
)
WITH (
  'connector' = 'filesystem',
  'path' = '{BI_TOWER_FILE}',
  'format' = 'csv',
  'csv.ignore-first-line' = 'true'
)
""")

# ------------------ QUERY ------------------
# We count only event_type = 0 (START events) because 1 (STOP events) repeat tower info
query = """
SELECT
  TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
  TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end,
  unique_id AS user_id,
  COUNT(DISTINCT tower) AS distinct_tower_visits
FROM bisignal_tower
WHERE event_time IS NOT NULL AND event_type = 0
GROUP BY
  TUMBLE(event_time, INTERVAL '5' MINUTE),
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

-- Query 6 MONO
;


-- query 6 mono spark-sql verifier
SELECT 
    (CAST(end_ts AS BIGINT) / 5000) * 5000 AS window_start,
    unique_id,
    COUNT(DISTINCT tower_id) as distinct_towers
FROM mono_towers
GROUP BY 1, 2
HAVING COUNT(DISTINCT tower_id) > 2 
ORDER BY window_start, unique_id;






-- Query 6 BI-SIGNAL
SELECT unique_id, COUNT(DISTINCT tower_id)
FROM bi_towers
WHERE event_type = 0 -- Only look at "Enter Tower" events
GROUP BY unique_id
HAVING COUNT(DISTINCT tower_id) > 5;







--Query 9 mono new

WITH SortedCalls AS (
    SELECT
        calling_party,
        start_ts::bigint,
        end_ts::bigint,
        -- Peek at the previous row's End Time
        LAG(end_ts::bigint) OVER (PARTITION BY calling_party ORDER BY start_ts::bigint) as prev_end
    FROM mono_calls
)
SELECT 
    calling_party,      -- The User ID
    prev_end,           -- Proof: When they hung up last time
    start_ts,           -- Proof: When they started this time
    (start_ts - prev_end) as gap_millis -- Proof: The Gap
FROM SortedCalls
WHERE (start_ts - prev_end) <= 10000 -- 10 seconds
  AND (start_ts - prev_end) > 0;




-- Query 9 bi new

WITH ReconstructedCalls AS (
    -- Step 1: Rebuild the Call (Join Start & End)
    SELECT
        t1.calling_party,
        t1.event_ts::bigint as start_ts,
        t2.event_ts::bigint as end_ts
    FROM bi_calls t1
    JOIN bi_calls t2 ON t1.unique_id = t2.unique_id
    WHERE t1.event_type = 0 -- Start
      AND t2.event_type = 1 -- End
),
SortedReconstructed AS (
    -- Step 2: Sort and Peek (Same as Mono)
    SELECT
        calling_party,
        start_ts,
        LAG(end_ts) OVER (PARTITION BY calling_party ORDER BY start_ts) as prev_end
    FROM ReconstructedCalls
)
SELECT 
    calling_party,
    prev_end,
    start_ts,
    (start_ts - prev_end) as gap_millis
FROM SortedReconstructed
WHERE (start_ts - prev_end) <= 10000
  AND (start_ts - prev_end) > 0;



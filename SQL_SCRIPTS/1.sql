-- 1. Mono-Signal Calls 
DROP TABLE IF EXISTS mono_calls;
CREATE TABLE mono_calls (
    unique_id BIGINT,
    calling_party BIGINT,
    called_party BIGINT,
    start_ts VARCHAR(30), -- Storing as string to be safe, or BIGINT
    end_ts VARCHAR(30),
    duration INT,
    disposition VARCHAR(20),
    imei VARCHAR(20)
);

-- 2. Bi-Signal Calls 
-- Contains both Start (Type 0) and End (Type 1) events mixed
DROP TABLE IF EXISTS bi_calls;
CREATE TABLE bi_calls (
    unique_id BIGINT,
    event_type INT,       -- 0 = START, 1 = END
    calling_party BIGINT,
    called_party BIGINT,
    event_ts VARCHAR(30),
    disposition VARCHAR(20),
    imei VARCHAR(20)
);

-- 3. Mono-Signal Towers 
DROP TABLE IF EXISTS mono_towers;
CREATE TABLE mono_towers (
    unique_id BIGINT,
    tower_id VARCHAR(10),
    start_ts VARCHAR(30),
    end_ts VARCHAR(30)
);

-- 4. Bi-Signal Towers 
DROP TABLE IF EXISTS bi_towers;
CREATE TABLE bi_towers (
    unique_id BIGINT,
    tower_id VARCHAR(10),
    event_type INT,       -- 0 = ENTER, 1 = EXIT
    event_ts VARCHAR(30)
);
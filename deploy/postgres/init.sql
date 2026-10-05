-- EGTS Telematics Platform — TimescaleDB schema

-- ── Positions ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS positions (
    time        TIMESTAMPTZ     NOT NULL,
    device_id   BIGINT          NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    speed       REAL,
    direction   SMALLINT,
    altitude    INTEGER,
    odometer    BIGINT,
    valid       BOOLEAN         DEFAULT TRUE
);

SELECT create_hypertable('positions', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_positions_device_time
    ON positions (device_id, time DESC);

-- 1-minute rollup for map/chart queries
CREATE MATERIALIZED VIEW IF NOT EXISTS positions_1m
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT
    time_bucket('1 minute', time)  AS bucket,
    device_id,
    last(lat, time)                AS lat,
    last(lon, time)                AS lon,
    round(avg(speed)::numeric, 1)  AS avg_speed_kmh,
    max(speed)                     AS max_speed_kmh,
    count(*)                       AS samples
FROM positions
GROUP BY bucket, device_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('positions_1m',
    start_offset => INTERVAL '1 hour',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

-- Retention: keep raw data for 90 days, rollup for 2 years
SELECT add_retention_policy('positions',  INTERVAL '90 days',  if_not_exists => TRUE);
SELECT add_retention_policy('positions_1m', INTERVAL '730 days', if_not_exists => TRUE);


-- ── iBeacon events ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events_ibeacon (
    time        TIMESTAMPTZ NOT NULL,
    device_id   BIGINT      NOT NULL,
    event_type  SMALLINT,           -- 1=enter 2=exit 3=periodic
    major       INTEGER,
    minor       INTEGER,
    rssi        SMALLINT,
    tx_power    SMALLINT,
    uuid        UUID
);

SELECT create_hypertable('events_ibeacon', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_ibeacon_device_time ON events_ibeacon (device_id, time DESC);
CREATE INDEX IF NOT EXISTS ix_ibeacon_minor ON events_ibeacon (minor, time DESC);
SELECT add_retention_policy('events_ibeacon', INTERVAL '365 days', if_not_exists => TRUE);


-- ── LBS cell info ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events_cell (
    time        TIMESTAMPTZ NOT NULL,
    device_id   BIGINT      NOT NULL,
    mcc         SMALLINT,
    mnc         SMALLINT,
    lac         INTEGER,
    cell_id     BIGINT,
    rssi        SMALLINT,
    rat         SMALLINT    -- 1=GSM 2=UMTS 3=LTE 4=NR
);

SELECT create_hypertable('events_cell', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_cell_device_time ON events_cell (device_id, time DESC);
SELECT add_retention_policy('events_cell', INTERVAL '30 days', if_not_exists => TRUE);


-- ── WiFi AP data ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events_wifi (
    time        TIMESTAMPTZ NOT NULL,
    device_id   BIGINT      NOT NULL,
    bssid       MACADDR,
    ssid        TEXT,
    rssi        SMALLINT,
    channel     SMALLINT
);

SELECT create_hypertable('events_wifi', 'time', if_not_exists => TRUE);
SELECT add_retention_policy('events_wifi', INTERVAL '30 days', if_not_exists => TRUE);


-- ── Device registry (non-time-series) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS devices (
    device_id   BIGINT PRIMARY KEY,
    first_seen  TIMESTAMPTZ DEFAULT now(),
    last_seen   TIMESTAMPTZ,
    last_lat    DOUBLE PRECISION,
    last_lon    DOUBLE PRECISION,
    last_speed  REAL
);

-- Upsert helper called by consumer
CREATE OR REPLACE FUNCTION update_device_last_seen(
    p_device_id BIGINT,
    p_time      TIMESTAMPTZ,
    p_lat       DOUBLE PRECISION,
    p_lon       DOUBLE PRECISION,
    p_speed     REAL
) RETURNS VOID LANGUAGE SQL AS $$
    INSERT INTO devices (device_id, first_seen, last_seen, last_lat, last_lon, last_speed)
    VALUES (p_device_id, p_time, p_time, p_lat, p_lon, p_speed)
    ON CONFLICT (device_id) DO UPDATE
        SET last_seen  = EXCLUDED.last_seen,
            last_lat   = EXCLUDED.last_lat,
            last_lon   = EXCLUDED.last_lon,
            last_speed = EXCLUDED.last_speed
        WHERE devices.last_seen < EXCLUDED.last_seen;
$$;

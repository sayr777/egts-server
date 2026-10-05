-- EGTS Platform — ClickHouse schema
CREATE DATABASE IF NOT EXISTS egts;

-- ── Positions ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS egts.positions
(
    time        DateTime64(3, 'UTC'),
    device_id   UInt64,
    lat         Float64,
    lon         Float64,
    speed       Float32,
    direction   UInt16,
    altitude    Int32,
    odometer    UInt64,
    valid       UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (device_id, time)
TTL toDate(time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ── iBeacon events ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS egts.events_ibeacon
(
    time       DateTime64(3, 'UTC'),
    device_id  UInt64,
    event_type UInt8,
    major      UInt16,
    minor      UInt16,
    rssi       Int8,
    tx_power   Int8,
    uuid       UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (device_id, time)
TTL toDate(time) + INTERVAL 365 DAY;

-- ── LBS cell info ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS egts.events_cell
(
    time      DateTime64(3, 'UTC'),
    device_id UInt64,
    mcc       UInt16,
    mnc       UInt8,
    lac       UInt32,
    cell_id   UInt64,
    rssi      Int8,
    rat       UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (device_id, time)
TTL toDate(time) + INTERVAL 30 DAY;

-- ── WiFi AP data ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS egts.events_wifi
(
    time      DateTime64(3, 'UTC'),
    device_id UInt64,
    bssid     String,
    ssid      String,
    rssi      Int8,
    channel   UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (device_id, time)
TTL toDate(time) + INTERVAL 30 DAY;

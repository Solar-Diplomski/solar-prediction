CREATE TABLE power_readings (
    timestamp TIMESTAMPTZ NOT NULL,
    plant_id INTEGER NOT NULL REFERENCES power_plant_v2(id),
    power_w DOUBLE PRECISION,
    PRIMARY KEY (timestamp, plant_id)
);
SELECT create_hypertable('power_readings', 'timestamp', chunk_time_interval => INTERVAL '1 day');
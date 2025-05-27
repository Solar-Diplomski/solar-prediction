CREATE TABLE weather_forecasts (
    forecast_time TIMESTAMPTZ NOT NULL,
    plant_id INTEGER NOT NULL REFERENCES power_plant_v2(id),
    created_at TIMESTAMPTZ NOT NULL,
    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    cloud_cover_low DOUBLE PRECISION,
    cloud_cover_mid DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    wind_direction_10m DOUBLE PRECISION,
    shortwave_radiation DOUBLE PRECISION,
    shortwave_radiation_instant DOUBLE PRECISION,
    diffuse_radiation DOUBLE PRECISION,
    diffuse_radiation_instant DOUBLE PRECISION,
    direct_normal_irradiance DOUBLE PRECISION,
    et0_fao_evapotranspiration DOUBLE PRECISION,
    vapour_pressure_deficit DOUBLE PRECISION,
    is_day INTEGER,
    sunshine_duration DOUBLE PRECISION,
    PRIMARY KEY (forecast_time, plant_id, created_at)
);
SELECT create_hypertable('weather_forecasts', 'forecast_time', chunk_time_interval => INTERVAL '1 day');
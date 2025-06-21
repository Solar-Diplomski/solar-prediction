CREATE TYPE horizon_metric_type AS ENUM ('MAE', 'RMSE', 'MBE');

CREATE TYPE cycle_metric_type AS ENUM ('MAE', 'RMSE', 'MBE');

CREATE TABLE horizon_metrics (
    model_id INTEGER NOT NULL REFERENCES model_metadata(id),
    metric_type horizon_metric_type NOT NULL,
    horizon DOUBLE PRECISION NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (model_id, metric_type, horizon)
);

CREATE TABLE cycle_metrics (
    time_of_forecast TIMESTAMPTZ NOT NULL,
    model_id INTEGER NOT NULL REFERENCES model_metadata(id),
    metric_type cycle_metric_type NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (time_of_forecast, model_id, metric_type)
);

SELECT create_hypertable('cycle_metrics', 'time_of_forecast', chunk_time_interval => INTERVAL '1 day');
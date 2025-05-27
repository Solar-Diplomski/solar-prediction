CREATE TABLE power_predictions (
    prediction_time TIMESTAMPTZ NOT NULL, -- Time the prediction is for
    model_id INTEGER NOT NULL REFERENCES model_metadata(id),
    created_at TIMESTAMPTZ NOT NULL, -- When the prediction was generated
    predicted_power_mw DOUBLE PRECISION, -- Predicted power output (MW)
    PRIMARY KEY (prediction_time, model_id, created_at)
);
SELECT create_hypertable('power_predictions', 'prediction_time', chunk_time_interval => INTERVAL '1 day');
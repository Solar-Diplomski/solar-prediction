ALTER TABLE power_predictions ADD COLUMN horizon DOUBLE PRECISION;

UPDATE power_predictions 
SET horizon = EXTRACT(EPOCH FROM (prediction_time - created_at)) / 3600.0
WHERE horizon IS NULL;

ALTER TABLE power_predictions ALTER COLUMN horizon SET NOT NULL; 
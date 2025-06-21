-- Backfill horizon column for existing power_predictions
-- Horizon is calculated as (prediction_time - created_at) in hours
UPDATE power_predictions 
SET horizon = EXTRACT(EPOCH FROM (prediction_time - created_at)) / 3600.0
WHERE horizon IS NULL; 
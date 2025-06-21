-- Make horizon column NOT NULL after backfilling existing data
ALTER TABLE power_predictions ALTER COLUMN horizon SET NOT NULL; 
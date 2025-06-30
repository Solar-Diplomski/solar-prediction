
ALTER TABLE weather_forecasts 
DROP CONSTRAINT IF EXISTS weather_forecasts_plant_id_fkey;

ALTER TABLE power_predictions 
DROP CONSTRAINT IF EXISTS power_predictions_model_id_fkey;

ALTER TABLE power_readings 
DROP CONSTRAINT IF EXISTS power_readings_plant_id_fkey;

ALTER TABLE horizon_metrics 
DROP CONSTRAINT IF EXISTS horizon_metrics_model_id_fkey;

ALTER TABLE cycle_metrics 
DROP CONSTRAINT IF EXISTS cycle_metrics_model_id_fkey;

ALTER TABLE weather_forecasts 
ADD CONSTRAINT weather_forecasts_plant_id_fkey 
FOREIGN KEY (plant_id) REFERENCES power_plant(id) ON DELETE CASCADE;

ALTER TABLE power_predictions 
ADD CONSTRAINT power_predictions_model_id_fkey 
FOREIGN KEY (model_id) REFERENCES model_metadata(id) ON DELETE CASCADE;

ALTER TABLE power_readings 
ADD CONSTRAINT power_readings_plant_id_fkey 
FOREIGN KEY (plant_id) REFERENCES power_plant(id) ON DELETE CASCADE;

ALTER TABLE horizon_metrics 
ADD CONSTRAINT horizon_metrics_model_id_fkey 
FOREIGN KEY (model_id) REFERENCES model_metadata(id) ON DELETE CASCADE;

ALTER TABLE cycle_metrics 
ADD CONSTRAINT cycle_metrics_model_id_fkey 
FOREIGN KEY (model_id) REFERENCES model_metadata(id) ON DELETE CASCADE; 
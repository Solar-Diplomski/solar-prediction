import asyncio
import logging
from typing import List
from app.prediction.weather_forecast.weather_forecast_models import WeatherForecast
from app.config.database import db_manager

logger = logging.getLogger(__name__)


class WeatherForecastRepository:
    def __init__(self):
        self._insert_query = """
            INSERT INTO weather_forecasts (
                forecast_time, plant_id, created_at, temperature_2m, relative_humidity_2m,
                cloud_cover, cloud_cover_low, cloud_cover_mid, wind_speed_10m, wind_direction_10m,
                shortwave_radiation, shortwave_radiation_instant, diffuse_radiation,
                diffuse_radiation_instant, direct_normal_irradiance, et0_fao_evapotranspiration,
                vapour_pressure_deficit, is_day, sunshine_duration
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
            ) ON CONFLICT (forecast_time, plant_id, created_at) DO NOTHING
        """

    def save_weather_forecasts_batch(self, forecasts: List[WeatherForecast]) -> None:
        try:
            loop = asyncio.get_event_loop()

            # Create the task without waiting for it
            task = loop.create_task(self._save_weather_forecasts_batch_async(forecasts))

            # Add error handling callback
            task.add_done_callback(self._handle_save_completion)

            logger.info(
                f"Started background save task for {len(forecasts)} weather forecasts"
            )

        except Exception as e:
            logger.error(f"Failed to start weather forecast save task: {e}")

    async def _save_weather_forecasts_batch_async(
        self, forecasts: List[WeatherForecast]
    ) -> int:
        if not forecasts:
            return 0

        successful_saves = 0

        for forecast in forecasts:
            success = await self._save_weather_forecast_async(forecast)
            if success:
                successful_saves += 1

        return successful_saves

    async def _save_weather_forecast_async(self, forecast: WeatherForecast) -> bool:
        try:
            forecast_records = []
            for data_point in forecast.forecast_data:
                record = (
                    data_point.time,  # forecast_time
                    forecast.power_plant_id,  # plant_id
                    forecast.fetch_time,  # created_at
                    data_point.temperature_2m,
                    data_point.relative_humidity_2m,
                    data_point.cloud_cover,
                    data_point.cloud_cover_low,
                    data_point.cloud_cover_mid,
                    data_point.wind_speed_10m,
                    data_point.wind_direction_10m,
                    data_point.shortwave_radiation,
                    data_point.shortwave_radiation_instant,
                    data_point.diffuse_radiation,
                    data_point.diffuse_radiation_instant,
                    data_point.direct_normal_irradiance,
                    data_point.et0_fao_evapotranspiration,
                    data_point.vapour_pressure_deficit,
                    data_point.is_day,
                    data_point.sunshine_duration,
                )
                forecast_records.append(record)

            # Execute batch insert
            await db_manager.execute_many(self._insert_query, forecast_records)
            return True

        except Exception as e:
            logger.error(
                f"Failed to save weather forecast for power plant {forecast.power_plant_id}: {e}"
            )
            return False

    def _handle_save_completion(self, task: asyncio.Task):
        """Callback to handle task completion and errors"""
        try:
            result = task.result()
            logger.debug(f"Weather forecast save task completed with result: {result}")
        except Exception as e:
            logger.error(f"Weather forecast save task failed: {e}")

import logging
from typing import List
from app.models.weather_forecast import WeatherForecast
from app.config.database import db_manager

logger = logging.getLogger(__name__)


class WeatherForecastRepository:
    """Repository for weather forecast database operations"""

    def __init__(self):
        self.insert_query = """
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

    async def save_weather_forecast(self, forecast: WeatherForecast) -> bool:
        """
        Save a single weather forecast to the database

        Args:
            forecast: WeatherForecast object to save

        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare data for batch insert
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
            await db_manager.execute_many(self.insert_query, forecast_records)
            return True

        except Exception as e:
            logger.error(
                f"Failed to save weather forecast for power plant {forecast.power_plant_id}: {e}"
            )
            return False

    async def save_weather_forecasts_batch(
        self, forecasts: List[WeatherForecast]
    ) -> int:
        """
        Save multiple weather forecasts to the database

        Args:
            forecasts: List of WeatherForecast objects to save

        Returns:
            Number of successfully saved forecasts
        """
        if not forecasts:
            return 0

        successful_saves = 0

        for forecast in forecasts:
            success = await self.save_weather_forecast(forecast)
            if success:
                successful_saves += 1

        return successful_saves


# Global repository instance
weather_forecast_repository = WeatherForecastRepository()

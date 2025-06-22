from datetime import datetime
import logging
from typing import List, Optional
from app.common.connectors.model_manager.model_manager_models import PowerPlant
from app.prediction.weather_forecast.open_meteo_connector import OpenMeteoConnector
from app.prediction.weather_forecast.weather_forecast_models import (
    OpenMeteoResponse,
    WeatherDataPoint,
    WeatherForecast,
)
from app.prediction.weather_forecast.weather_forecast_repository import (
    WeatherForecastRepository,
)

logger = logging.getLogger(__name__)


class WeatherForecastService:
    def __init__(
        self,
        open_meteo_connector: OpenMeteoConnector,
        weather_forecast_repository: WeatherForecastRepository,
    ):
        self._open_meteo_connector = open_meteo_connector
        self._weather_forecast_repository = weather_forecast_repository

    def get_weather_forecast(self, power_plant: PowerPlant) -> WeatherForecast:
        created_at, open_meteo_response = (
            self._open_meteo_connector.fetch_weather_forecast(power_plant)
        )
        return self._to_weather_forecast(
            power_plant.id, created_at, open_meteo_response
        )

    def get_weather_forecast_for_all_power_plants(
        self, power_plants: List[PowerPlant]
    ) -> List[WeatherForecast]:
        weather_forecasts = []
        for power_plant in power_plants:
            weather_forecast = self.get_weather_forecast(power_plant)
            weather_forecasts.append(weather_forecast)
        return weather_forecasts

    def save_weather_forecasts(self, weather_forecasts: List[WeatherForecast]):
        self._weather_forecast_repository.save_weather_forecasts_batch(
            weather_forecasts
        )

    def _to_weather_forecast(
        self,
        plant_id: int,
        fetch_time: datetime,
        open_meteo_response: OpenMeteoResponse,
    ) -> WeatherForecast:
        weather_point_list = self._get_weather_point_list(open_meteo_response)

        weather_forecast = WeatherForecast(
            power_plant_id=plant_id,
            latitude=open_meteo_response.latitude,
            longitude=open_meteo_response.longitude,
            timezone=open_meteo_response.timezone,
            elevation=open_meteo_response.elevation,
            forecast_data=weather_point_list,
            fetch_time=fetch_time,
        )

        return weather_forecast

    def _get_weather_point_list(
        self, open_meteo_response: OpenMeteoResponse
    ) -> List[WeatherDataPoint]:
        weather_point_list = []
        minutely_data = open_meteo_response.minutely_15

        times = minutely_data.get("time", [])

        for i, time_str in enumerate(times):
            try:
                time_obj = datetime.fromisoformat(time_str.replace("Z", "+00:00"))

                data_point = WeatherDataPoint(
                    time=time_obj,
                    temperature_2m=self._get_value_at_index(
                        minutely_data.get("temperature_2m"), i
                    ),
                    relative_humidity_2m=self._get_value_at_index(
                        minutely_data.get("relative_humidity_2m"), i
                    ),
                    cloud_cover=self._get_value_at_index(
                        minutely_data.get("cloud_cover"), i
                    ),
                    wind_speed_10m=self._get_value_at_index(
                        minutely_data.get("wind_speed_10m"), i
                    ),
                    wind_direction_10m=self._get_value_at_index(
                        minutely_data.get("wind_direction_10m"), i
                    ),
                    shortwave_radiation=self._get_value_at_index(
                        minutely_data.get("shortwave_radiation"), i
                    ),
                    diffuse_radiation=self._get_value_at_index(
                        minutely_data.get("diffuse_radiation"), i
                    ),
                    direct_normal_irradiance=self._get_value_at_index(
                        minutely_data.get("direct_normal_irradiance"), i
                    ),
                    cloud_cover_low=self._get_value_at_index(
                        minutely_data.get("cloud_cover_low"), i
                    ),
                    cloud_cover_mid=self._get_value_at_index(
                        minutely_data.get("cloud_cover_mid"), i
                    ),
                    et0_fao_evapotranspiration=self._get_value_at_index(
                        minutely_data.get("et0_fao_evapotranspiration"), i
                    ),
                    vapour_pressure_deficit=self._get_value_at_index(
                        minutely_data.get("vapour_pressure_deficit"), i
                    ),
                    is_day=self._get_value_at_index(minutely_data.get("is_day"), i),
                    sunshine_duration=self._get_value_at_index(
                        minutely_data.get("sunshine_duration"), i
                    ),
                    shortwave_radiation_instant=self._get_value_at_index(
                        minutely_data.get("shortwave_radiation_instant"), i
                    ),
                    diffuse_radiation_instant=self._get_value_at_index(
                        minutely_data.get("diffuse_radiation_instant"), i
                    ),
                )

                weather_point_list.append(data_point)

            except Exception as e:
                logger.warning(f"Failed to parse data point at index {i}: {e}")
                continue

        if len(weather_point_list) > 0:
            weather_point_list = weather_point_list[1:]
            logger.debug(
                "Removed first weather data point to avoid horizon=0 predictions"
            )

        return weather_point_list

    def _get_value_at_index(
        self, data_array: Optional[List], index: int
    ) -> Optional[float]:
        """Safely get value from array at given index"""
        if data_array is None or index >= len(data_array):
            return None
        return data_array[index]

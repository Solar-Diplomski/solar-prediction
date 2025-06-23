import requests
import logging
from typing import Optional, Tuple
from datetime import datetime, timedelta
from app.common.connectors.model_manager.model_manager_models import PowerPlant
from app.prediction.weather_forecast.weather_forecast_models import (
    OpenMeteoResponse,
)

logger = logging.getLogger(__name__)


class OpenMeteoConnector:
    def __init__(self, base_url: str):
        self._base_url = base_url  # "https://api.open-meteo.com/v1/forecast"
        self._timeout = 30
        self.weather_parameters = [
            "temperature_2m",
            "relative_humidity_2m",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "shortwave_radiation",
            "diffuse_radiation",
            "direct_normal_irradiance",
            "cloud_cover_low",
            "cloud_cover_mid",
            "et0_fao_evapotranspiration",
            "vapour_pressure_deficit",
            "is_day",
            "sunshine_duration",
            "shortwave_radiation_instant",
            "diffuse_radiation_instant",
        ]

    def fetch_weather_forecast(
        self, power_plant: PowerPlant
    ) -> Tuple[datetime, Optional[OpenMeteoResponse]]:

        if not power_plant.latitude or not power_plant.longitude:
            logger.warning(f"Power plant {power_plant.id} missing coordinates")
            return None

        try:
            fetch_time = self._get_normalized_time()
            start_time_str, end_time_str = self._get_72h_time_range(fetch_time)

            params = {
                "latitude": power_plant.latitude,
                "longitude": power_plant.longitude,
                "minutely_15": ",".join(self.weather_parameters),
                "start_minutely_15": start_time_str,
                "end_minutely_15": end_time_str,
                "timezone": "Europe/Zagreb",  # If needed in the future, make this configurable
            }

            response = requests.get(
                self._base_url, params=params, timeout=self._timeout
            )
            response.raise_for_status()

            data = response.json()
            return fetch_time, OpenMeteoResponse(**data)

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to fetch weather data for power plant {power_plant.id}: {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error while fetching weather data for power plant {power_plant.id}: {e}"
            )
            return None

    def _get_normalized_time(self) -> datetime:
        """Get current time normalized to 00 minutes and seconds"""
        now = datetime.now()
        return now.replace(minute=0, second=0, microsecond=0)

    def _get_72h_time_range(self, start_time: datetime) -> tuple[str, str]:
        """Get start and end times for 72-hour forecast in ISO8601 format"""
        end_time = start_time + timedelta(hours=72)

        # Format times as ISO8601 for minutely_15 parameters
        # Open Meteo expects format: 2022-06-30T12:00
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M")
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M")

        return start_time_str, end_time_str

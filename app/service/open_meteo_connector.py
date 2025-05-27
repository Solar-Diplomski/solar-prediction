import requests
import logging
from typing import List, Optional
from datetime import datetime, timedelta
from app.models.power_plant import PowerPlant
from app.models.weather_forecast import (
    WeatherForecast,
    WeatherDataPoint,
    OpenMeteoResponse,
)

logger = logging.getLogger(__name__)


class OpenMeteoConnector:
    """Connector for fetching weather forecast data from Open Meteo API"""

    def __init__(self):
        self.base_url = "https://api.open-meteo.com/v1/forecast"
        self.timeout = 30  # seconds
        self.weather_parameters = [
            "temperature_2m",
            "relative_humidity_2m",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "shortwave_radiation",
            "diffuse_radiation",
            "direct_normal_irradiance",
        ]

    def _get_normalized_time(self) -> datetime:
        """Get current time normalized to 00 minutes and seconds"""
        now = datetime.now()
        return now.replace(minute=0, second=0, microsecond=0)

    def _get_time_range(self, start_time: datetime) -> tuple[str, str]:
        """Get start and end times for 72-hour forecast in ISO8601 format"""
        end_time = start_time + timedelta(hours=72)

        # Format times as ISO8601 for minutely_15 parameters
        # Open Meteo expects format: 2022-06-30T12:00
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M")
        end_time_str = end_time.strftime("%Y-%m-%dT%H:%M")

        return start_time_str, end_time_str

    def fetch_weather_forecast(
        self, power_plant: PowerPlant
    ) -> Optional[WeatherForecast]:
        """
        Fetch weather forecast for a specific power plant

        Args:
            power_plant: PowerPlant object with latitude and longitude

        Returns:
            WeatherForecast object if successful, None if failed
        """
        if not power_plant.latitude or not power_plant.longitude:
            logger.warning(f"Power plant {power_plant.id} missing coordinates")
            return None

        try:
            fetch_time = self._get_normalized_time()
            start_time_str, end_time_str = self._get_time_range(fetch_time)

            params = {
                "latitude": power_plant.latitude,
                "longitude": power_plant.longitude,
                "minutely_15": ",".join(self.weather_parameters),
                "start_minutely_15": start_time_str,
                "end_minutely_15": end_time_str,
                "timezone": "Europe/Zagreb",  # If needed in the future, make this configurable
            }

            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            open_meteo_response = OpenMeteoResponse(**data)

            # Convert to internal format
            forecast_data = self._convert_to_forecast_data(open_meteo_response)

            weather_forecast = WeatherForecast(
                power_plant_id=power_plant.id,
                latitude=open_meteo_response.latitude,
                longitude=open_meteo_response.longitude,
                timezone=open_meteo_response.timezone,
                elevation=open_meteo_response.elevation,
                forecast_data=forecast_data,
                fetch_time=fetch_time,
            )

            return weather_forecast

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

    def _convert_to_forecast_data(
        self, response: OpenMeteoResponse
    ) -> List[WeatherDataPoint]:
        """Convert Open Meteo response to list of WeatherDataPoint objects"""
        forecast_data = []
        minutely_data = response.minutely_15

        # Get time array
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
                )

                forecast_data.append(data_point)

            except Exception as e:
                logger.warning(f"Failed to parse data point at index {i}: {e}")
                continue

        return forecast_data

    def _get_value_at_index(
        self, data_array: Optional[List], index: int
    ) -> Optional[float]:
        """Safely get value from array at given index"""
        if data_array is None or index >= len(data_array):
            return None
        return data_array[index]

    def fetch_weather_forecasts_for_all_plants(
        self, power_plants: List[PowerPlant]
    ) -> List[WeatherForecast]:
        """
        Fetch weather forecasts for all power plants

        Args:
            power_plants: List of PowerPlant objects

        Returns:
            List of WeatherForecast objects (only successful fetches)
        """
        forecasts = []

        logger.info(f"Fetching weather forecasts for {len(power_plants)} power plants")

        for plant in power_plants:
            forecast = self.fetch_weather_forecast(plant)
            if forecast:
                forecasts.append(forecast)
                logger.info(f"Successfully fetched forecast for power plant {plant.id}")
            else:
                logger.warning(f"Failed to fetch forecast for power plant {plant.id}")

        logger.info(
            f"Successfully fetched {len(forecasts)} out of {len(power_plants)} forecasts"
        )
        return forecasts

    def print_forecast_summary(self, forecast: WeatherForecast) -> None:
        """Print a summary of the weather forecast in row format"""
        print(f"\n=== Weather Forecast for Power Plant {forecast.power_plant_id} ===")
        print(f"Location: {forecast.latitude:.4f}, {forecast.longitude:.4f}")
        print(f"Timezone: {forecast.timezone}")
        print(f"Elevation: {forecast.elevation}m")
        print(f"Fetch Time: {forecast.fetch_time}")
        print(f"Data Points: {len(forecast.forecast_data)}")

        print("\nFirst 5 data points:")
        print(
            "Time                | Temp(°C) | Humidity(%) | Cloud(%) | Wind(km/h) | Wind Dir(°) | Solar(W/m²) | Diffuse(W/m²) | DNI(W/m²)"
        )
        print("-" * 120)

        for i, data_point in enumerate(forecast.forecast_data[:5]):
            print(
                f"{data_point.time.strftime('%Y-%m-%d %H:%M')} | "
                f"{data_point.temperature_2m or 'N/A':>8} | "
                f"{data_point.relative_humidity_2m or 'N/A':>11} | "
                f"{data_point.cloud_cover or 'N/A':>8} | "
                f"{data_point.wind_speed_10m or 'N/A':>10} | "
                f"{data_point.wind_direction_10m or 'N/A':>11} | "
                f"{data_point.shortwave_radiation or 'N/A':>11} | "
                f"{data_point.diffuse_radiation or 'N/A':>13} | "
                f"{data_point.direct_normal_irradiance or 'N/A':>9}"
            )

    def print_all_forecasts(self, forecasts: List[WeatherForecast]) -> None:
        """Print summaries of all weather forecasts"""
        for forecast in forecasts:
            self.print_forecast_summary(forecast)

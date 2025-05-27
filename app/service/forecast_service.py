import logging
from typing import List, Optional
from app.models.power_plant import PowerPlant, PowerPlantState
from app.models.weather_forecast import WeatherForecast
from app.service.model_manager_connector import ModelManagerConnector
from app.service.open_meteo_connector import OpenMeteoConnector

logger = logging.getLogger(__name__)


class ForecastService:
    """Service for managing forecasts and power plant state"""

    def __init__(self, power_plant_base_url: str = "http://localhost:8000"):
        self.power_plant_state = PowerPlantState()
        self.model_manager_connector = ModelManagerConnector(power_plant_base_url)
        self.open_meteo_connector = OpenMeteoConnector()
        self._is_initialized = False

    def load_power_plants(self) -> bool:
        """
        Load active power plants from external service and update state

        Returns:
            True if successful, False otherwise
        """
        try:
            power_plants = self.model_manager_connector.fetch_active_power_plants()

            if power_plants is not None:
                self.power_plant_state.update_power_plants(power_plants)
                self._is_initialized = True
                logger.info(f"Loaded {len(power_plants)} power plants")
                return True
            else:
                logger.warning("No power plants loaded from external service")
                return False

        except Exception as e:
            logger.error(f"Failed to load power plants: {e}")
            return False

    def get_power_plant(self, plant_id: int) -> Optional[PowerPlant]:
        """Get a specific power plant by ID"""
        return self.power_plant_state.get_power_plant(plant_id)

    def get_all_power_plants(self) -> List[PowerPlant]:
        """Get all power plants"""
        return self.power_plant_state.get_all_power_plants()

    def get_power_plants_count(self) -> int:
        """Get the number of power plants in state"""
        return self.power_plant_state.count()

    def is_initialized(self) -> bool:
        """Check if the service has been initialized with power plant data"""
        return self._is_initialized

    def refresh_power_plants(self) -> bool:
        """Refresh power plant data from external service"""
        return self.load_power_plants()

    def get_power_plants_summary(self) -> dict:
        """Get a summary of the current power plant state"""
        plants = self.get_all_power_plants()
        return {
            "total_count": self.get_power_plants_count(),
            "is_initialized": self._is_initialized,
            "power_plants": [
                {
                    "id": plant.id,
                    "longitude": plant.longitude,
                    "latitude": plant.latitude,
                }
                for plant in plants
            ],
        }

    def fetch_weather_forecasts(self) -> List[WeatherForecast]:
        """
        Fetch weather forecasts for all power plants in the current state

        Returns:
            List of WeatherForecast objects
        """
        if not self._is_initialized:
            logger.warning("Forecast service not initialized. Load power plants first.")
            return []

        power_plants = self.get_all_power_plants()
        if not power_plants:
            logger.warning("No power plants available for weather forecast")
            return []

        logger.info(f"Fetching weather forecasts for {len(power_plants)} power plants")
        forecasts = self.open_meteo_connector.fetch_weather_forecasts_for_all_plants(
            power_plants
        )

        return forecasts

    def fetch_and_print_weather_forecasts(self) -> None:
        """
        Fetch weather forecasts for all power plants and print them in row format
        """
        forecasts = self.fetch_weather_forecasts()

        if not forecasts:
            print("No weather forecasts available.")
            return

        print(f"\n{'='*60}")
        print(f"WEATHER FORECASTS FOR {len(forecasts)} POWER PLANTS")
        print(f"{'='*60}")

        self.open_meteo_connector.print_all_forecasts(forecasts)

    def fetch_weather_forecast_for_plant(
        self, plant_id: int
    ) -> Optional[WeatherForecast]:
        """
        Fetch weather forecast for a specific power plant

        Args:
            plant_id: ID of the power plant

        Returns:
            WeatherForecast object if successful, None if failed
        """
        power_plant = self.get_power_plant(plant_id)
        if not power_plant:
            logger.warning(f"Power plant {plant_id} not found")
            return None

        return self.open_meteo_connector.fetch_weather_forecast(power_plant)

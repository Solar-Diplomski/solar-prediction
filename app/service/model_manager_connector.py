import requests
import logging
from typing import List, Optional
from app.models.power_plant import PowerPlant

logger = logging.getLogger(__name__)


class ModelManagerConnector:
    """Connector for fetching data from model management services"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.timeout = 30  # seconds

    def fetch_active_power_plants(self) -> Optional[List[PowerPlant]]:
        """
        Fetch active power plants from the external service

        Returns:
            List of PowerPlant objects if successful, None if failed
        """
        try:
            url = f"{self.base_url}/internal/power-plant/active"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

            # Convert JSON data to PowerPlant objects
            power_plants = []
            for plant_data in data:
                try:
                    power_plant = PowerPlant(**plant_data)
                    power_plants.append(power_plant)
                except Exception as e:
                    logger.error(f"Failed to parse power plant data {plant_data}: {e}")
                    continue

            return power_plants

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch power plants: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching power plants: {e}")
            return None

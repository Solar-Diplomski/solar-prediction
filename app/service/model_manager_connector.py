import requests
import logging
from typing import List, Optional
from app.models.power_plant import PowerPlant
from app.models.ml_model import MLModel

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

    def fetch_active_models(self) -> Optional[List[MLModel]]:
        """
        Fetch active ML models from the external service

        Returns:
            List of MLModel objects if successful, None if failed
        """
        try:
            url = f"{self.base_url}/internal/models/active"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

            # Convert JSON data to MLModel objects
            models = []
            for model_data in data:
                try:
                    model = MLModel(**model_data)
                    models.append(model)
                except Exception as e:
                    logger.error(f"Failed to parse model data {model_data}: {e}")
                    continue

            logger.info(f"Successfully fetched {len(models)} active models")
            return models

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch active models: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching active models: {e}")
            return None

    def download_model_file(self, model_id: int) -> Optional[bytes]:
        """
        Download model file from the external service

        Args:
            model_id: ID of the model to download

        Returns:
            Model file content as bytes if successful, None if failed
        """
        try:
            url = f"{self.base_url}/internal/models/{model_id}/download"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            # Verify content type
            content_type = response.headers.get("content-type", "")
            if content_type != "application/octet-stream":
                logger.warning(
                    f"Unexpected content type for model {model_id}: {content_type}"
                )

            file_content = response.content
            logger.info(
                f"Successfully downloaded model {model_id}, size: {len(file_content)} bytes"
            )
            return file_content

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download model {model_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while downloading model {model_id}: {e}")
            return None

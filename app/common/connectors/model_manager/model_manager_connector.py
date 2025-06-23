import requests
import logging
from typing import List, Optional
from app.common.connectors.model_manager.model_manager_models import Model, PowerPlant
from app.prediction.state.state_models import ModelMetadata

logger = logging.getLogger(__name__)


class ModelManagerConnector:

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.timeout = 30

    def fetch_active_power_plants(self) -> Optional[List[PowerPlant]]:
        try:
            url = f"{self.base_url}/internal/power-plant/active"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

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

    def fetch_active_models_metadata(self) -> Optional[List[ModelMetadata]]:
        try:
            url = f"{self.base_url}/internal/models/active"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            json_data = response.json()

            models_metadata = []
            for json_model_metadata in json_data:
                try:
                    model_metadata = ModelMetadata(**json_model_metadata)
                    models_metadata.append(model_metadata)
                except Exception as e:
                    logger.error(
                        f"Failed to parse model metadata {json_model_metadata}: {e}"
                    )
                    continue

            logger.info(f"Successfully fetched {len(models_metadata)} active models")
            return models_metadata

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch active models: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching active models: {e}")
            return None

    def fetch_models_for_power_plant(self, plant_id: int) -> Optional[List[Model]]:
        try:
            url = f"{self.base_url}/power_plant/{plant_id}/models"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            models_data = response.json()

            models = []
            for model_data in models_data:
                try:
                    model = Model(**model_data)
                    models.append(model)
                except Exception as e:
                    logger.error(f"Failed to parse model data {model_data}: {e}")
                    continue

            logger.info(
                f"Successfully fetched {len(models)} models for plant {plant_id}"
            )
            return models

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch models for plant {plant_id}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error while fetching models for plant {plant_id}: {e}"
            )
            return None

    def fetch_model(self, model_id: int) -> Optional[Model]:
        try:
            url = f"{self.base_url}/models/{model_id}"
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return Model(**response.json())
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch model {model_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while fetching model {model_id}: {e}")
            return None

    def download_model_file(self, model_id: int) -> Optional[bytes]:
        try:
            url = f"{self.base_url}/internal/models/{model_id}/download"

            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

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

from typing import Dict, List
from app.prediction.state.state_models import MLModel, PowerPlant
from app.prediction.state.model_factory import ModelFactory
from app.prediction.state.model_manager_connector import ModelManagerConnector
import logging

logger = logging.getLogger(__name__)


class StateManager:
    def __init__(self, model_manager_connector: ModelManagerConnector):
        self._active_power_plants: Dict[int, PowerPlant] = {}
        self._active_models: Dict[int, List[MLModel]] = {}
        self._model_manager_connector: ModelManagerConnector = model_manager_connector

    def refresh_state(self):
        self._refresh_power_plant_state()
        self._refresh_model_state()

    def get_active_power_plants(self) -> List[PowerPlant]:
        return list(self._active_power_plants.values())

    def get_active_models_for_power_plant(self, power_plant_id: int) -> List[MLModel]:
        return self._active_models.get(power_plant_id, [])

    def get_active_power_plant(self, power_plant_id: int) -> PowerPlant:
        return self._active_power_plants.get(power_plant_id)

    def _refresh_power_plant_state(self):
        try:
            self._active_power_plants.clear()

            power_plants = self._model_manager_connector.fetch_active_power_plants()

            if power_plants is not None:
                self._active_power_plants.clear()
                for power_plant in power_plants:
                    self._active_power_plants[power_plant.id] = power_plant
                logger.info(f"Successfully loaded {len(power_plants)} power plants")
            else:
                logger.info("No active power plants received from model manager")

        except Exception as e:
            logger.error(f"Failed to load power plant state: {e}")
            return False

    def _refresh_model_state(self):
        try:
            self._active_models.clear()

            models_metadata = (
                self._model_manager_connector.fetch_active_models_metadata()
            )

            if models_metadata is None:
                return

            for model_metadata in models_metadata:
                model_file = self._model_manager_connector.download_model_file(
                    model_metadata.id
                )

                if model_file is None:
                    continue

                model = ModelFactory.create_model(model_metadata, model_file)

                plant_id = model_metadata.plant_id
                if plant_id not in self._active_models:
                    self._active_models[plant_id] = []

                self._active_models[plant_id].append(model)

        except Exception as e:
            logger.error(f"Failed to load model state: {e}")

import logging
import asyncio
from typing import List, Optional
from app.models.power_plant import PowerPlant, PowerPlantState
from app.models.weather_forecast import WeatherForecast
from app.models.ml_model import MLModelState, MLModelWithFile
from app.service.model_manager_connector import ModelManagerConnector
from app.service.open_meteo_connector import OpenMeteoConnector
from app.repository.weather_forecast_repository import weather_forecast_repository

logger = logging.getLogger(__name__)


class ForecastService:
    """Service for managing forecasts and power plant state"""

    def __init__(self, power_plant_base_url: str = "http://localhost:8000"):
        self.power_plant_state = PowerPlantState()
        self.ml_model_state = MLModelState()
        self.model_manager_connector = ModelManagerConnector(power_plant_base_url)
        self.open_meteo_connector = OpenMeteoConnector()
        self._is_initialized = False
        self._models_initialized = False

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
                logger.info(f"Successfully loaded {len(power_plants)} power plants")
                return True
            else:
                logger.error(
                    "Failed to load power plants: received None from connector"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to load power plants: {e}")
            return False

    def load_models(self) -> bool:
        """
        Load active ML models from external service and update state

        Returns:
            True if successful, False otherwise
        """
        try:
            # Fetch model metadata
            models_metadata = self.model_manager_connector.fetch_active_models()

            if models_metadata is None:
                logger.error("Failed to load models: received None from connector")
                return False

            if not models_metadata:
                logger.warning("No active models found")
                self._models_initialized = True
                return True

            logger.info(f"Fetched {len(models_metadata)} model metadata records")

            # Download model files
            models_with_files = []
            successful_downloads = 0

            for model_metadata in models_metadata:
                try:
                    logger.info(
                        f"Downloading model file for model {model_metadata.id} ({model_metadata.name})"
                    )
                    file_content = self.model_manager_connector.download_model_file(
                        model_metadata.id
                    )

                    if file_content is not None:
                        model_with_file = MLModelWithFile(
                            metadata=model_metadata, file_content=file_content
                        )
                        models_with_files.append(model_with_file)
                        successful_downloads += 1
                        logger.info(
                            f"Successfully loaded model {model_metadata.id} for plant {model_metadata.plant_id}"
                        )
                    else:
                        logger.error(
                            f"Failed to download file for model {model_metadata.id}"
                        )

                except Exception as e:
                    logger.error(f"Error processing model {model_metadata.id}: {e}")
                    continue

            # Update state with successfully loaded models
            if models_with_files:
                self.ml_model_state.update_models(models_with_files)
                self._models_initialized = True
                logger.info(
                    f"Successfully loaded {successful_downloads} out of {len(models_metadata)} models"
                )

                # Log summary by plant
                plants_with_models = self.ml_model_state.get_plants_with_models()
                for plant_id in plants_with_models:
                    plant_models = self.ml_model_state.get_models_for_plant(plant_id)
                    model_names = [m.metadata.name for m in plant_models]
                    logger.info(
                        f"Plant {plant_id} has {len(plant_models)} models: {model_names}"
                    )

                return True
            else:
                logger.error("No models were successfully loaded")
                return False

        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            return False

    def load_state(self) -> bool:
        """
        Load both power plants and models state

        Returns:
            True if both operations successful, False otherwise
        """
        logger.info("Loading complete state (power plants and models)")

        power_plants_success = self.load_power_plants()
        models_success = self.load_models()

        success = power_plants_success and models_success

        if success:
            logger.info("Successfully loaded complete state")
        else:
            logger.error(
                f"State loading completed with errors - Power plants: {power_plants_success}, Models: {models_success}"
            )

        return success

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

    def is_models_initialized(self) -> bool:
        """Check if the service has been initialized with ML model data"""
        return self._models_initialized

    def is_fully_initialized(self) -> bool:
        """Check if the service has been initialized with both power plant and model data"""
        return self._is_initialized and self._models_initialized

    def refresh_power_plants(self) -> bool:
        """Refresh power plant data from external service"""
        return self.load_power_plants()

    def refresh_models(self) -> bool:
        """Refresh ML model data from external service"""
        return self.load_models()

    def refresh_state(self) -> bool:
        """Refresh both power plant and model data from external service"""
        return self.load_state()

    def get_models_for_plant(self, plant_id: int) -> List[MLModelWithFile]:
        """Get all ML models for a specific plant"""
        return self.ml_model_state.get_models_for_plant(plant_id)

    def get_all_models(self) -> dict:
        """Get all ML models organized by plant_id"""
        return self.ml_model_state.get_all_models()

    def get_models_count(self) -> int:
        """Get the total number of ML models in state"""
        return self.ml_model_state.get_total_models_count()

    def get_plants_with_models(self) -> List[int]:
        """Get list of plant IDs that have ML models"""
        return self.ml_model_state.get_plants_with_models()

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
                    "capacity": plant.capacity,
                }
                for plant in plants
            ],
        }

    def get_models_summary(self) -> dict:
        """Get a summary of the current ML model state"""
        all_models = self.get_all_models()
        plants_with_models = self.get_plants_with_models()

        models_by_plant = []
        for plant_id in plants_with_models:
            plant_models = all_models[plant_id]
            models_info = [
                {
                    "id": model.metadata.id,
                    "name": model.metadata.name,
                    "type": model.metadata.type,
                    "version": model.metadata.version,
                    "features": model.metadata.features,
                    "file_type": model.metadata.file_type,
                    "file_size_bytes": len(model.file_content),
                }
                for model in plant_models
            ]
            models_by_plant.append(
                {
                    "plant_id": plant_id,
                    "models_count": len(plant_models),
                    "models": models_info,
                }
            )

        return {
            "total_models_count": self.get_models_count(),
            "plants_with_models_count": len(plants_with_models),
            "is_models_initialized": self._models_initialized,
            "models_by_plant": models_by_plant,
        }

    def get_complete_summary(self) -> dict:
        """Get a complete summary of both power plant and ML model state"""
        return {
            "power_plants": self.get_power_plants_summary(),
            "models": self.get_models_summary(),
            "initialization_status": {
                "power_plants_initialized": self._is_initialized,
                "models_initialized": self._models_initialized,
                "fully_initialized": self.is_fully_initialized(),
            },
        }

    def fetch_weather_forecasts(self) -> List[WeatherForecast]:
        """
        Fetch weather forecasts for all power plants in the current state

        Returns:
            List of WeatherForecast objects
        """
        if not self._is_initialized:
            return []

        power_plants = self.get_all_power_plants()
        if not power_plants:
            return []

        forecasts = self.open_meteo_connector.fetch_weather_forecasts_for_all_plants(
            power_plants
        )

        return forecasts

    async def _save_forecasts_async(self, forecasts: List[WeatherForecast]) -> None:
        """
        Asynchronously save weather forecasts to database

        Args:
            forecasts: List of WeatherForecast objects to save
        """
        try:
            if not forecasts:
                return

            await weather_forecast_repository.save_weather_forecasts_batch(forecasts)

        except Exception as e:
            logger.error(f"Error during async forecast save: {e}")

    def fetch_and_save_weather_forecasts(self) -> List[WeatherForecast]:
        """
        Fetch weather forecasts for all power plants and save them asynchronously to database

        Returns:
            List of WeatherForecast objects that were fetched
        """
        forecasts = self.fetch_weather_forecasts()

        if not forecasts:
            return []

        # Start async save task (fire and forget)
        asyncio.create_task(self._save_forecasts_async(forecasts))

        return forecasts

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
            return None

        return self.open_meteo_connector.fetch_weather_forecast(power_plant)

    async def fetch_and_save_weather_forecast_for_plant(
        self, plant_id: int
    ) -> Optional[WeatherForecast]:
        """
        Fetch weather forecast for a specific power plant and save it asynchronously

        Args:
            plant_id: ID of the power plant

        Returns:
            WeatherForecast object if successful, None if failed
        """
        forecast = self.fetch_weather_forecast_for_plant(plant_id)

        if forecast:
            # Start async save task (fire and forget)
            asyncio.create_task(self._save_forecasts_async([forecast]))

        return forecast

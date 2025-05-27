import logging
import asyncio
import io
import joblib
from typing import List, Optional, Dict, Any
from app.models.power_plant import PowerPlant, PowerPlantState
from app.models.weather_forecast import WeatherForecast
from app.models.ml_model import MLModelState, MLModelWithFile, MLModel
from app.service.model_manager_connector import ModelManagerConnector
from app.service.open_meteo_connector import OpenMeteoConnector
from app.repository.weather_forecast_repository import weather_forecast_repository

logger = logging.getLogger(__name__)


class LoadedModel:
    """Container for a loaded ML model with its metadata"""

    def __init__(self, model_metadata: MLModel, loaded_model: Any):
        self.metadata = model_metadata
        self.model = loaded_model
        self.features = model_metadata.features

    def predict(self, input_data: List[List[float]]) -> List[float]:
        """Make predictions using the loaded model"""
        return self.model.predict(input_data)


class ForecastService:
    """Service for managing forecasts and power plant state"""

    def __init__(self, power_plant_base_url: str = "http://localhost:8000"):
        self.power_plant_state = PowerPlantState()
        self.ml_model_state = MLModelState()
        self.model_manager_connector = ModelManagerConnector(power_plant_base_url)
        self.open_meteo_connector = OpenMeteoConnector()
        self._is_initialized = False
        self._models_initialized = False
        # Cache for loaded models: {model_id: LoadedModel}
        self._loaded_models_cache: Dict[int, LoadedModel] = {}

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

    def load_joblib_model(
        self, model_with_file: MLModelWithFile
    ) -> Optional[LoadedModel]:
        """
        Load a joblib model from binary file content

        Args:
            model_with_file: MLModelWithFile containing model metadata and binary content

        Returns:
            LoadedModel instance if successful, None if failed
        """
        try:
            # Check if model file type is joblib
            if model_with_file.metadata.file_type.lower() != "joblib":
                logger.warning(
                    f"Model {model_with_file.metadata.id} has unsupported file type: {model_with_file.metadata.file_type}"
                )
                return None

            # Load model from binary content using joblib
            model_bytes = io.BytesIO(model_with_file.file_content)
            loaded_model = joblib.load(model_bytes)

            # Create LoadedModel instance
            loaded_model_instance = LoadedModel(
                model_metadata=model_with_file.metadata, loaded_model=loaded_model
            )

            logger.info(
                f"Successfully loaded joblib model {model_with_file.metadata.id} "
                f"({model_with_file.metadata.name}) for plant {model_with_file.metadata.plant_id}"
            )

            return loaded_model_instance

        except Exception as e:
            logger.error(
                f"Failed to load joblib model {model_with_file.metadata.id}: {e}"
            )
            return None

    def load_models_for_plant(
        self, plant_id: int, use_cache: bool = True
    ) -> List[LoadedModel]:
        """
        Load all joblib models for a specific power plant

        Args:
            plant_id: ID of the power plant
            use_cache: Whether to use cached models if available

        Returns:
            List of LoadedModel instances for the plant
        """
        try:
            # Get models for the plant from state
            models_with_files = self.get_models_for_plant(plant_id)

            if not models_with_files:
                logger.info(f"No models found for plant {plant_id}")
                return []

            loaded_models = []

            for model_with_file in models_with_files:
                model_id = model_with_file.metadata.id

                # Check cache first if enabled
                if use_cache and model_id in self._loaded_models_cache:
                    loaded_models.append(self._loaded_models_cache[model_id])
                    logger.debug(f"Using cached model {model_id}")
                    continue

                # Load the model
                loaded_model = self.load_joblib_model(model_with_file)

                if loaded_model:
                    loaded_models.append(loaded_model)

                    # Cache the loaded model if caching is enabled
                    if use_cache:
                        self._loaded_models_cache[model_id] = loaded_model
                        logger.debug(f"Cached model {model_id}")

            logger.info(
                f"Successfully loaded {len(loaded_models)} out of {len(models_with_files)} "
                f"models for plant {plant_id}"
            )

            return loaded_models

        except Exception as e:
            logger.error(f"Failed to load models for plant {plant_id}: {e}")
            return []

    def load_all_models(self, use_cache: bool = True) -> Dict[int, List[LoadedModel]]:
        """
        Load all joblib models for all plants

        Args:
            use_cache: Whether to use cached models if available

        Returns:
            Dictionary mapping plant_id to list of LoadedModel instances
        """
        try:
            if not self._models_initialized:
                logger.warning("Models not initialized. Cannot load models.")
                return {}

            plants_with_models = self.get_plants_with_models()
            all_loaded_models = {}

            for plant_id in plants_with_models:
                loaded_models = self.load_models_for_plant(plant_id, use_cache)
                if loaded_models:
                    all_loaded_models[plant_id] = loaded_models

            total_loaded = sum(len(models) for models in all_loaded_models.values())
            logger.info(
                f"Successfully loaded {total_loaded} models across {len(all_loaded_models)} plants"
            )

            return all_loaded_models

        except Exception as e:
            logger.error(f"Failed to load all models: {e}")
            return {}

    def clear_model_cache(self) -> None:
        """Clear the loaded models cache"""
        self._loaded_models_cache.clear()
        logger.info("Cleared model cache")

    def get_loaded_model_cache_info(self) -> Dict[str, Any]:
        """Get information about the loaded models cache"""
        return {
            "cached_models_count": len(self._loaded_models_cache),
            "cached_model_ids": list(self._loaded_models_cache.keys()),
            "cache_summary": [
                {
                    "model_id": model_id,
                    "model_name": loaded_model.metadata.name,
                    "plant_id": loaded_model.metadata.plant_id,
                    "features_count": len(loaded_model.features),
                    "features": loaded_model.features,
                }
                for model_id, loaded_model in self._loaded_models_cache.items()
            ],
        }

    def format_weather_data_for_model(
        self, weather_forecast: WeatherForecast, model_features: List[str]
    ) -> List[List[float]]:
        """
        Format weather forecast data according to model's feature requirements

        Args:
            weather_forecast: WeatherForecast object containing weather data
            model_features: List of feature names expected by the model

        Returns:
            List of feature vectors (one per time point) formatted for model input
        """
        try:
            formatted_data = []

            # Create mapping from feature names to weather data attributes
            feature_mapping = {
                "temperature_2m": "temperature_2m",
                "relative_humidity_2m": "relative_humidity_2m",
                "cloud_cover": "cloud_cover",
                "wind_speed_10m": "wind_speed_10m",
                "wind_direction_10m": "wind_direction_10m",
                "shortwave_radiation": "shortwave_radiation",
                "diffuse_radiation": "diffuse_radiation",
                "direct_normal_irradiance": "direct_normal_irradiance",
                "cloud_cover_low": "cloud_cover_low",
                "cloud_cover_mid": "cloud_cover_mid",
                "et0_fao_evapotranspiration": "et0_fao_evapotranspiration",
                "vapour_pressure_deficit": "vapour_pressure_deficit",
                "is_day": "is_day",
                "sunshine_duration": "sunshine_duration",
                "shortwave_radiation_instant": "shortwave_radiation_instant",
                "diffuse_radiation_instant": "diffuse_radiation_instant",
            }

            # Process each weather data point
            for data_point in weather_forecast.forecast_data:
                feature_vector = []

                for feature_name in model_features:
                    # Get the corresponding attribute name
                    attr_name = feature_mapping.get(feature_name, feature_name)

                    # Get the value from the weather data point
                    value = getattr(data_point, attr_name, None)

                    # Handle missing values (replace with 0.0 or could use other strategies)
                    if value is None:
                        value = 0.0
                        logger.debug(
                            f"Missing value for feature '{feature_name}' at time {data_point.time}, using 0.0"
                        )

                    feature_vector.append(float(value))

                formatted_data.append(feature_vector)

            logger.debug(
                f"Formatted {len(formatted_data)} data points with {len(model_features)} features each"
            )

            return formatted_data

        except Exception as e:
            logger.error(f"Failed to format weather data for model: {e}")
            return []

    def prepare_model_for_predictions(
        self, plant_id: int, model_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Load and prepare models for a specific plant for making predictions

        Args:
            plant_id: ID of the power plant
            model_id: Optional specific model ID to load. If None, loads all models for the plant

        Returns:
            Dictionary containing loaded models and their information
        """
        try:
            if not self.is_fully_initialized():
                return {
                    "success": False,
                    "error": "Service not fully initialized. Both power plants and models must be loaded.",
                    "loaded_models": [],
                }

            # Check if plant exists
            power_plant = self.get_power_plant(plant_id)
            if not power_plant:
                return {
                    "success": False,
                    "error": f"Power plant {plant_id} not found",
                    "loaded_models": [],
                }

            # Load models for the plant
            if model_id is not None:
                # Load specific model
                models_with_files = self.get_models_for_plant(plant_id)
                target_model = None

                for model_with_file in models_with_files:
                    if model_with_file.metadata.id == model_id:
                        target_model = model_with_file
                        break

                if not target_model:
                    return {
                        "success": False,
                        "error": f"Model {model_id} not found for plant {plant_id}",
                        "loaded_models": [],
                    }

                loaded_model = self.load_joblib_model(target_model)
                loaded_models = [loaded_model] if loaded_model else []
            else:
                # Load all models for the plant
                loaded_models = self.load_models_for_plant(plant_id)

            if not loaded_models:
                return {
                    "success": False,
                    "error": f"No joblib models could be loaded for plant {plant_id}",
                    "loaded_models": [],
                }

            # Prepare model information
            model_info = []
            for loaded_model in loaded_models:
                model_info.append(
                    {
                        "model_id": loaded_model.metadata.id,
                        "model_name": loaded_model.metadata.name,
                        "model_type": loaded_model.metadata.type,
                        "version": loaded_model.metadata.version,
                        "features": loaded_model.features,
                        "features_count": len(loaded_model.features),
                        "file_type": loaded_model.metadata.file_type,
                        "ready_for_predictions": True,
                    }
                )

            return {
                "success": True,
                "plant_id": plant_id,
                "loaded_models_count": len(loaded_models),
                "loaded_models": loaded_models,
                "model_info": model_info,
            }

        except Exception as e:
            logger.error(f"Failed to prepare models for plant {plant_id}: {e}")
            return {
                "success": False,
                "error": f"Failed to prepare models: {str(e)}",
                "loaded_models": [],
            }

    def test_model_loading_with_weather_data(self, plant_id: int) -> Dict[str, Any]:
        """
        Test function that demonstrates the complete flow:
        1. Load models for a plant
        2. Fetch weather forecast for the plant
        3. Format weather data according to model features
        4. Prepare everything for predictions

        Args:
            plant_id: ID of the power plant

        Returns:
            Dictionary containing test results and formatted data
        """
        try:
            logger.info(
                f"Testing model loading and data preparation for plant {plant_id}"
            )

            # Step 1: Prepare models
            model_prep_result = self.prepare_model_for_predictions(plant_id)
            if not model_prep_result["success"]:
                return {
                    "success": False,
                    "step_failed": "model_preparation",
                    "error": model_prep_result["error"],
                }

            loaded_models = model_prep_result["loaded_models"]

            # Step 2: Fetch weather forecast
            weather_forecast = self.fetch_weather_forecast_for_plant(plant_id)
            if not weather_forecast:
                return {
                    "success": False,
                    "step_failed": "weather_forecast_fetch",
                    "error": f"Could not fetch weather forecast for plant {plant_id}",
                }

            # Step 3: Format weather data for each model
            formatted_data_by_model = {}
            model_readiness = []

            for loaded_model in loaded_models:
                model_id = loaded_model.metadata.id
                model_name = loaded_model.metadata.name

                try:
                    # Format weather data according to this model's features
                    formatted_data = self.format_weather_data_for_model(
                        weather_forecast, loaded_model.features
                    )

                    if formatted_data:
                        formatted_data_by_model[model_id] = {
                            "model_name": model_name,
                            "features": loaded_model.features,
                            "data_points_count": len(formatted_data),
                            "features_per_point": (
                                len(formatted_data[0]) if formatted_data else 0
                            ),
                            "sample_data_point": (
                                formatted_data[0] if formatted_data else None
                            ),
                            "ready_for_prediction": True,
                        }

                        model_readiness.append(
                            {
                                "model_id": model_id,
                                "model_name": model_name,
                                "status": "ready",
                                "data_points": len(formatted_data),
                                "features_count": len(loaded_model.features),
                            }
                        )
                    else:
                        model_readiness.append(
                            {
                                "model_id": model_id,
                                "model_name": model_name,
                                "status": "failed_data_formatting",
                                "error": "Could not format weather data for model",
                            }
                        )

                except Exception as e:
                    logger.error(f"Failed to format data for model {model_id}: {e}")
                    model_readiness.append(
                        {
                            "model_id": model_id,
                            "model_name": model_name,
                            "status": "error",
                            "error": str(e),
                        }
                    )

            # Summary
            ready_models = [m for m in model_readiness if m["status"] == "ready"]

            return {
                "success": True,
                "plant_id": plant_id,
                "weather_forecast_available": True,
                "weather_data_points": len(weather_forecast.forecast_data),
                "models_loaded": len(loaded_models),
                "models_ready_for_prediction": len(ready_models),
                "model_readiness": model_readiness,
                "formatted_data_summary": {
                    model_id: data["data_points_count"]
                    for model_id, data in formatted_data_by_model.items()
                },
                "sample_formatted_data": formatted_data_by_model,
                "next_steps": [
                    "Models are loaded and data is formatted",
                    "Ready to call model.predict(formatted_data) for each model",
                    "Predictions can be saved to database",
                ],
            }

        except Exception as e:
            logger.error(f"Failed to test model loading for plant {plant_id}: {e}")
            return {"success": False, "step_failed": "general_error", "error": str(e)}

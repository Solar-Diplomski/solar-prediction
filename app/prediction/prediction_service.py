import logging
from typing import List
from app.prediction.data_preparation_service import DataPreparationService
from app.prediction.prediction_models import PowerPrediction
from app.prediction.prediction_repository import PredictionRepository
from app.prediction.state.state_manager import StateManager
from app.prediction.state.state_models import MLModel
from app.prediction.weather_forecast.weather_forecast_models import WeatherForecast
from app.prediction.weather_forecast.weather_forecast_service import (
    WeatherForecastService,
)

logger = logging.getLogger(__name__)


class PredictionService:
    def __init__(
        self,
        state_manager: StateManager,
        weather_forecast_service: WeatherForecastService,
        data_preparation_service: DataPreparationService,
        prediction_repository: PredictionRepository,
    ):
        self._state_manager = state_manager
        self._weather_forecast_service = weather_forecast_service
        self._data_preparation_service = data_preparation_service
        self._prediction_repository = prediction_repository

    def predict(self):
        logger.info("Starting prediction")
        logger.info("Refreshing state")
        self._state_manager.refresh_state()
        power_plants = self._state_manager.get_active_power_plants()
        logger.info(f"Getting weather forecasts for {len(power_plants)} power plants")

        weather_forecasts = (
            self._weather_forecast_service.get_weather_forecast_for_all_power_plants(
                power_plants
            )
        )

        logger.info("Saving weather forecasts")
        self._weather_forecast_service.save_weather_forecasts(weather_forecasts)

        logger.info(
            f"Creating predictions for {len(weather_forecasts)} weather forecasts"
        )
        for weather_forecast in weather_forecasts:
            self._create_predictions_for_weather_forecast(weather_forecast)

    def _create_predictions_for_weather_forecast(
        self,
        weather_forecast: WeatherForecast,
    ):
        logger.info(
            f"Creating predictions for power plant {weather_forecast.power_plant_id}"
        )

        models = self._state_manager.get_active_models_for_power_plant(
            weather_forecast.power_plant_id
        )
        logger.info(
            f"Found {len(models)} models for power plant {weather_forecast.power_plant_id}"
        )

        for model in models:
            try:
                self._create_predictions_for_model(weather_forecast, model)

            except Exception as e:
                logger.error(
                    f"Error creating predictions for model {model.metadata.id}: {e}"
                )

    def _create_predictions_for_model(
        self, weather_forecast: WeatherForecast, model: MLModel
    ):
        logger.info(f"Creating predictions for model {model.metadata.id}")

        logger.info(f"Preparing data for model {model.metadata.id}")
        model_inputs = self._data_preparation_service.prepare_data(
            weather_forecast,
            model.features,
            self._state_manager.get_active_power_plant(
                weather_forecast.power_plant_id
            ).capacity,
        )

        logger.info(f"Predicting for model {model.metadata.id}")
        predictions = model.predict(model_inputs)
        power_predictions = self._map_to_power_predictions(
            predictions, weather_forecast, model
        )

        logger.info(f"Saving predictions for model {model.metadata.id}")
        self._prediction_repository.save_power_predictions_batch(power_predictions)

    def _map_to_power_predictions(
        self,
        predictions: List[float],
        weather_forecast: WeatherForecast,
        model: MLModel,
    ) -> List[PowerPrediction]:
        logger.info(
            f"Mapping predictions to power predictions for model {model.metadata.id}"
        )
        power_predictions = []
        for i, data_point in enumerate(weather_forecast.forecast_data):
            if i < len(predictions):
                prediction = PowerPrediction(
                    prediction_time=data_point.time,
                    model_id=model.metadata.id,
                    plant_id=weather_forecast.power_plant_id,
                    created_at=weather_forecast.fetch_time,
                    predicted_power_mw=float(predictions[i]),
                )
                power_predictions.append(prediction)
        return power_predictions

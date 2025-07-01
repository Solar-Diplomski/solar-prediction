from fastapi import FastAPI, HTTPException, Query, UploadFile, File
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List
import os
from app.prediction.data_preparation_service import DataPreparationService
from app.prediction.prediction_repository import PredictionRepository
from app.prediction.prediction_service import PredictionService
from app.prediction.prediction_models import ForecastResponse
from app.prediction.scheduling import PredictionScheduler
from app.common.connectors.model_manager.model_manager_connector import (
    ModelManagerConnector,
)
from app.prediction.state.state_manager import StateManager
from app.prediction.weather_forecast.open_meteo_connector import OpenMeteoConnector
from app.config.database import db_manager
from app.prediction.weather_forecast.weather_forecast_repository import (
    WeatherForecastRepository,
)
from app.prediction.weather_forecast.weather_forecast_service import (
    WeatherForecastService,
)
from app.prediction.power_readings.power_readings_repository import (
    PowerReadingsRepository,
)
from app.prediction.power_readings.power_readings_service import PowerReadingsService
from app.prediction.power_readings.power_readings_models import (
    CSVUploadResponse,
    PowerReading,
)
from app.prediction.metrics.metrics_repository import MetricsRepository
from app.prediction.metrics.metrics_service import MetricsService
from app.prediction.metrics.metrics_models import HorizonMetric, CycleMetric
from app.prediction.playground.playground_service import PlaygroundService
from app.prediction.playground.playground_models import (
    PlaygroundFeatureInfo,
    PlaygroundPredictionResponse,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
MODEL_MANAGER_BASE_URL = os.getenv("MODEL_MANAGER_BASE_URL", "http://localhost:8000")

model_manager_connector = ModelManagerConnector(base_url=MODEL_MANAGER_BASE_URL)
state_manager = StateManager(model_manager_connector=model_manager_connector)

open_meteo_connector = OpenMeteoConnector(base_url=OPEN_METEO_BASE_URL)
weather_forecast_repository = WeatherForecastRepository()
weather_forecast_service = WeatherForecastService(
    open_meteo_connector=open_meteo_connector,
    weather_forecast_repository=weather_forecast_repository,
)

data_preparation_service = DataPreparationService()
prediction_repository = PredictionRepository()
prediction_service = PredictionService(
    state_manager=state_manager,
    weather_forecast_service=weather_forecast_service,
    data_preparation_service=data_preparation_service,
    prediction_repository=prediction_repository,
)
prediction_scheduler = PredictionScheduler(prediction_service)

metrics_repository = MetricsRepository()
metrics_service = MetricsService(metrics_repository, model_manager_connector)

power_readings_repository = PowerReadingsRepository()
power_readings_service = PowerReadingsService(
    power_readings_repository, metrics_service
)

playground_service = PlaygroundService(
    model_manager_connector=model_manager_connector,
    metrics_service=metrics_service,
    power_readings_service=power_readings_service,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        db_success = await db_manager.initialize()
        if not db_success:
            logging.error("Failed to initialize database connection pool")
            raise RuntimeError("Database initialization failed")

        state_manager.refresh_state()

        await prediction_scheduler.start()

    except Exception as e:
        logging.error(f"Startup error: {e}")

    yield

    # Shutdown
    try:
        # Gracefully stop the prediction scheduler first (it needs database connection)
        await prediction_scheduler.stop()

        await db_manager.close()
    except Exception as e:
        logging.error(f"Shutdown error: {e}")


app = FastAPI(title="Solar Prediction Service", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Solar Prediction Service is running"}


@app.post("/generate")
async def generate_predictions(start_date: datetime):
    """Generate predictions for the next 72 hours from a specific start date."""
    logging.info(f"Triggering prediction process for start date: {start_date}")

    try:
        prediction_service.predict(custom_start_time=start_date)
        return {"message": "Prediction process triggered successfully"}
    except Exception as e:
        logging.error(f"Error triggering prediction: {e}", exc_info=True)
        return {"message": f"Failed to trigger prediction: {str(e)}"}


@app.get("/internal/status")
async def get_status():
    power_plants = state_manager.get_active_power_plants()
    models = []
    for power_plant in power_plants:
        models.append(state_manager.get_active_models_for_power_plant(power_plant.id))
    return {
        "service": "Solar Prediction Service",
        "power_plants": power_plants,
        "models": models,
        "prediction_scheduler": prediction_scheduler.get_status(),
    }


@app.get("/forecast/{model_id}", response_model=List[ForecastResponse])
async def get_forecast(
    model_id: int,
    start_date: datetime = Query(..., description="Start date in ISO 8601 format"),
    end_date: datetime = Query(..., description="End date in ISO 8601 format"),
):

    logging.info(
        f"Received forecast request for model {model_id}, start_date: {start_date}, end_date: {end_date}"
    )

    try:
        if start_date >= end_date:
            logging.warning(
                f"Invalid date range: start_date {start_date} >= end_date {end_date}"
            )
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        forecast_data = await prediction_repository.get_forecast_data(
            model_id, start_date, end_date
        )

        response = [
            ForecastResponse(
                id=row["id"],
                prediction_time=row["prediction_time"],
                power_output=row["power_output"],
            )
            for row in forecast_data
        ]

        return response

    except HTTPException:
        raise
    except Exception as e:
        logging.error(
            f"Error fetching forecast for model {model_id}: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Failed to fetch forecast data")


@app.get("/forecast/time_of_forecast/{model_id}", response_model=List[ForecastResponse])
async def get_forecast_by_time_of_forecast(
    model_id: int,
    tof: datetime = Query(
        ..., description="Time of forecast (created_at) in ISO 8601 format"
    ),
):

    logging.info(
        f"Received forecast request for model {model_id}, time_of_forecast: {tof}"
    )

    try:
        forecast_data = (
            await prediction_repository.get_forecast_data_by_time_of_forecast(
                model_id, tof
            )
        )

        response = [
            ForecastResponse(
                id=row["id"],
                prediction_time=row["prediction_time"],
                power_output=row["power_output"],
            )
            for row in forecast_data
        ]

        return response

    except Exception as e:
        logging.error(
            f"Error fetching forecast for model {model_id} and time_of_forecast {tof}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Failed to fetch forecast data")


@app.get("/forecast/{model_id}/timestamps", response_model=List[datetime])
async def get_forecast_timestamps(model_id: int):

    logging.info(f"Received request for forecast timestamps for model {model_id}")

    try:
        timestamps = await prediction_repository.get_unique_forecast_timestamps(
            model_id
        )
        return timestamps

    except Exception as e:
        logging.error(
            f"Error fetching forecast timestamps for model {model_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500, detail="Failed to fetch forecast timestamps"
        )


@app.get("/reading/{id}", response_model=List[PowerReading])
async def get_power_readings(
    id: int,
    start_date: datetime = Query(..., description="Start date in ISO 8601 format"),
    end_date: datetime = Query(..., description="End date in ISO 8601 format"),
):
    """
    Get power readings for a specific plant within a date range.
    """
    logging.info(
        f"Received power readings request for plant {id}, start_date: {start_date}, end_date: {end_date}"
    )

    try:
        if start_date >= end_date:
            logging.warning(
                f"Invalid date range: start_date {start_date} >= end_date {end_date}"
            )
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        readings = await power_readings_service.get_power_readings(
            id, start_date, end_date
        )

        return readings

    except HTTPException:
        raise
    except Exception as e:
        logging.error(
            f"Error fetching power readings for plant {id}: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Failed to fetch power readings")


@app.post("/reading/{plant_id}", response_model=CSVUploadResponse)
async def upload_power_readings(
    plant_id: int,
    file: UploadFile = File(
        ..., description="CSV file with timestamp and power columns"
    ),
):
    """
    Upload CSV file containing power readings for a specific power plant.

    CSV format:
    - No headers
    - Two columns: timestamp (ISO format), power (float)
    - Example: 2024-01-01T12:00:00Z,1234.56
    """
    logging.info(f"Received CSV upload request for plant {plant_id}")

    try:
        # Validate file type
        if not file.filename or not file.filename.lower().endswith(".csv"):
            return CSVUploadResponse(
                success=False,
                message="File must be a CSV file",
                validation_errors=["Invalid file type. Only CSV files are accepted."],
            )

        # Process the CSV file
        result = await power_readings_service.upload_csv_readings(file, plant_id)
        return result

    except Exception as e:
        logging.error(f"Error uploading CSV for plant {plant_id}: {e}", exc_info=True)
        return CSVUploadResponse(
            success=False,
            message="Failed to upload CSV file",
        )


@app.get("/metric/horizon/{model_id}", response_model=List[HorizonMetric])
async def get_horizon_metrics(
    model_id: int,
):
    """
    Get horizon metrics for a specific model.

    Args:
        model_id: The model ID to fetch metrics for

    Returns:
        List[HorizonMetric]: Array of horizon metrics with metric_type, horizon, and value fields
    """
    logging.info(f"Received request for horizon metrics for model {model_id}")

    try:
        metrics = await metrics_service.get_horizon_metrics(model_id)
        return metrics

    except Exception as e:
        logging.error(
            f"Error fetching horizon metrics for model {model_id}: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Failed to fetch horizon metrics")


@app.get("/metric/cycle/{model_id}", response_model=List[CycleMetric])
async def get_cycle_metrics(
    model_id: int,
    start_date: datetime = Query(..., description="Start date in ISO 8601 format"),
    end_date: datetime = Query(..., description="End date in ISO 8601 format"),
):
    """
    Get cycle metrics for a specific model within a date range.

    Args:
        model_id: The model ID to fetch metrics for
        start_date: Start date filter (required)
        end_date: End date filter (required)

    Returns:
        List[CycleMetric]: Array of cycle metrics with time_of_forecast, metric_type, and value fields
    """
    logging.info(f"Received request for cycle metrics for model {model_id}")

    try:
        if start_date >= end_date:
            logging.warning(
                f"Invalid date range: start_date {start_date} >= end_date {end_date}"
            )
            raise HTTPException(
                status_code=400, detail="Start date must be before end date"
            )

        metrics = await metrics_service.get_cycle_metrics(
            model_id, start_date, end_date
        )
        return metrics

    except HTTPException:
        raise
    except Exception as e:
        logging.error(
            f"Error fetching cycle metrics for model {model_id}: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Failed to fetch cycle metrics")


@app.post("/metric/calculate/{model_id}")
async def calculate_metrics(model_id: int):
    """
    Calculate both horizon and cycle metrics for a specific model.

    Args:
        model_id: The model ID to calculate metrics for

    Returns:
        dict: Success message indicating both horizon and cycle metrics were calculated
    """
    logging.info(f"Received request to calculate metrics for model {model_id}")

    try:
        # Calculate horizon metrics
        await metrics_service.calculate_horizon_metrics_by_model(model_id)
        logging.info(f"Successfully calculated horizon metrics for model {model_id}")

        # Calculate cycle metrics
        await metrics_service.calculate_cycle_metrics_by_model(model_id)
        logging.info(f"Successfully calculated cycle metrics for model {model_id}")

        return {
            "success": True,
            "message": f"Successfully calculated horizon and cycle metrics for model {model_id}",
            "model_id": model_id,
        }

    except Exception as e:
        logging.error(
            f"Error calculating metrics for model {model_id}: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Failed to calculate metrics")


# Playground endpoints
@app.get("/playground/model/{model_id}/features", response_model=PlaygroundFeatureInfo)
async def get_model_features(model_id: int):
    """
    Get model features and metadata for playground use.

    Args:
        model_id: The model ID to get features for

    Returns:
        PlaygroundFeatureInfo: Model metadata including required features list
    """
    logging.info(f"Received request for model features for model {model_id}")

    try:
        feature_info = playground_service.get_model_features(model_id)

        if not feature_info:
            raise HTTPException(
                status_code=404, detail=f"Model {model_id} not found or not active"
            )

        return feature_info

    except HTTPException:
        raise
    except Exception as e:
        logging.error(
            f"Error getting model features for model {model_id}: {e}", exc_info=True
        )
        raise HTTPException(status_code=500, detail="Failed to get model features")


@app.post("/playground/predict/{model_id}", response_model=PlaygroundPredictionResponse)
async def playground_predict(
    model_id: int,
    file: UploadFile = File(
        ..., description="CSV file with timestamp and feature columns"
    ),
):
    """
    Generate predictions from CSV file for playground use.

    Args:
        model_id: The model ID to use for predictions
        file: CSV file with timestamp as first column followed by feature columns

    Returns:
        PlaygroundPredictionResponse: Predictions, metrics, and validation results
    """
    logging.info(f"Received playground prediction request for model {model_id}")

    try:
        # Validate file type
        if not file.filename or not file.filename.lower().endswith(".csv"):
            return PlaygroundPredictionResponse(
                model_id=model_id,
                predictions=[],
                metrics=[],
                input_rows=0,
                success=False,
                message="File must be a CSV file",
                validation_errors=["Invalid file type. Only CSV files are accepted."],
            )

        response = await playground_service.predict_from_csv(model_id, file)
        return response

    except Exception as e:
        logging.error(
            f"Error in playground prediction for model {model_id}: {e}", exc_info=True
        )
        return PlaygroundPredictionResponse(
            model_id=model_id,
            predictions=[],
            metrics=[],
            input_rows=0,
            success=False,
            message="An error occurred while processing your request. Please try again.",
            validation_errors=[
                "Processing failed. Please check your file and try again."
            ],
        )

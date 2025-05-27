from fastapi import FastAPI, HTTPException
import logging
from contextlib import asynccontextmanager
from app.prediction.data_preparation_service import DataPreparationService
from app.prediction.prediction_repository import PredictionRepository
from app.prediction.prediction_service import PredictionService
from app.prediction.state.model_manager_connector import ModelManagerConnector
from app.prediction.state.state_manager import StateManager
from app.prediction.weather_forecast.open_meteo_connector import OpenMeteoConnector
from app.config.database import db_manager
from app.prediction.weather_forecast.weather_forecast_repository import (
    WeatherForecastRepository,
)
from app.prediction.weather_forecast.weather_forecast_service import (
    WeatherForecastService,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
MODEL_MANAGER_BASE_URL = "http://localhost:8000"

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        db_success = await db_manager.initialize()
        if not db_success:
            logging.error("Failed to initialize database connection pool")

        state_manager.refresh_state()
    except Exception as e:
        logging.error(f"Startup error: {e}")

    yield

    # Shutdown
    try:
        await db_manager.close()
    except Exception as e:
        logging.error(f"Shutdown error: {e}")


app = FastAPI(title="Solar Prediction Service", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Solar Prediction Service is running"}


@app.get("/status")
async def get_status():
    power_plants = state_manager.get_active_power_plants()
    models = []
    for power_plant in power_plants:
        models.append(state_manager.get_active_models_for_power_plant(power_plant.id))
    return {
        "service": "Solar Prediction Service",
        "power_plants": power_plants,
        "models": models,
    }


@app.post("/predictions/generate")
async def generate_predictions():
    try:
        prediction_service.predict()

    except Exception as e:
        logging.error(f"Error generating predictions: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to generate predictions.",
        )

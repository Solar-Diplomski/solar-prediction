from fastapi import FastAPI, HTTPException
import logging
from contextlib import asynccontextmanager
from typing import List
from app.service.forecast_service import ForecastService
from app.models.weather_forecast import WeatherForecast
from app.config.database import db_manager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

forecast_service = ForecastService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        # Initialize database connection pool
        db_success = await db_manager.initialize()
        if not db_success:
            logging.error("Failed to initialize database connection pool")

        # Load complete state (power plants and models)
        success = forecast_service.load_state()
        if not success:
            logging.warning("Failed to load complete state on startup")
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
    """Get service status"""
    return {
        "service": "Solar Prediction Service",
        "power_plants_loaded": forecast_service.is_initialized(),
        "models_loaded": forecast_service.is_models_initialized(),
        "fully_initialized": forecast_service.is_fully_initialized(),
        "power_plants_count": forecast_service.get_power_plants_count(),
        "models_count": forecast_service.get_models_count(),
        "complete_summary": forecast_service.get_complete_summary(),
    }


@app.post("/power-plants/refresh")
async def refresh_power_plants():
    """Manually refresh power plants from external service"""
    try:
        success = forecast_service.refresh_power_plants()
        if success:
            return {
                "message": "Power plants refreshed successfully",
                "count": forecast_service.get_power_plants_count(),
            }
        else:
            return {
                "error": "Failed to refresh power plants",
                "count": forecast_service.get_power_plants_count(),
            }
    except Exception as e:
        logging.error(f"Refresh error: {e}")
        return {
            "error": "Critical error during refresh",
            "count": forecast_service.get_power_plants_count(),
        }


@app.get("/weather-forecasts", response_model=List[WeatherForecast])
async def get_weather_forecasts():
    """
    Fetch weather forecasts for all power plants and save them to database

    Returns:
        List of weather forecasts for all active power plants
    """
    try:
        if not forecast_service.is_initialized():
            raise HTTPException(
                status_code=503,
                detail="Service not initialized. Power plants not loaded.",
            )

        forecasts = forecast_service.fetch_and_save_weather_forecasts()

        if not forecasts:
            return []

        return forecasts

    except Exception as e:
        logging.error(f"Error fetching weather forecasts: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch weather forecasts: {str(e)}"
        )


@app.get("/weather-forecasts/{plant_id}", response_model=WeatherForecast)
async def get_weather_forecast_for_plant(plant_id: int):
    """
    Fetch weather forecast for a specific power plant and save it to database

    Args:
        plant_id: ID of the power plant

    Returns:
        Weather forecast for the specified power plant
    """
    try:
        if not forecast_service.is_initialized():
            raise HTTPException(
                status_code=503,
                detail="Service not initialized. Power plants not loaded.",
            )

        forecast = await forecast_service.fetch_and_save_weather_forecast_for_plant(
            plant_id
        )

        if not forecast:
            raise HTTPException(
                status_code=404,
                detail=f"Weather forecast not available for power plant {plant_id}",
            )

        return forecast

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching weather forecast for plant {plant_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch weather forecast for plant {plant_id}: {str(e)}",
        )

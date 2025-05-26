from fastapi import FastAPI
import logging
from contextlib import asynccontextmanager
from app.service.forecast_service import ForecastService

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

forecast_service = ForecastService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        success = forecast_service.load_power_plants()
        if not success:
            logging.warning("Failed to load power plants on startup")
    except Exception as e:
        logging.error(f"Startup error: {e}")

    yield


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
        "power_plants_count": forecast_service.get_power_plants_count(),
        "power_plants": forecast_service.get_power_plants_summary(),
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

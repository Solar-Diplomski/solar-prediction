from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class WeatherDataPoint(BaseModel):
    """Single weather data point for a specific time"""

    time: datetime
    temperature_2m: Optional[float] = None
    relative_humidity_2m: Optional[float] = None
    cloud_cover: Optional[float] = None
    wind_speed_10m: Optional[float] = None
    wind_direction_10m: Optional[float] = None
    shortwave_radiation: Optional[float] = None
    diffuse_radiation: Optional[float] = None
    direct_normal_irradiance: Optional[float] = None


class WeatherForecast(BaseModel):
    """Weather forecast for a specific power plant"""

    power_plant_id: int
    latitude: float
    longitude: float
    timezone: str
    elevation: float
    forecast_data: List[WeatherDataPoint]
    fetch_time: datetime


class OpenMeteoResponse(BaseModel):
    """Response model for Open Meteo API"""

    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    minutely_15_units: dict
    minutely_15: dict

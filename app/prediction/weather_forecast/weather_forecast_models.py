from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel


class WeatherDataPoint(BaseModel):
    time: datetime
    temperature_2m: Optional[float] = None
    relative_humidity_2m: Optional[float] = None
    cloud_cover: Optional[float] = None
    wind_speed_10m: Optional[float] = None
    wind_direction_10m: Optional[float] = None
    shortwave_radiation: Optional[float] = None
    diffuse_radiation: Optional[float] = None
    direct_normal_irradiance: Optional[float] = None
    cloud_cover_low: Optional[float] = None
    cloud_cover_mid: Optional[float] = None
    et0_fao_evapotranspiration: Optional[float] = None
    vapour_pressure_deficit: Optional[float] = None
    is_day: Optional[int] = None
    sunshine_duration: Optional[float] = None
    shortwave_radiation_instant: Optional[float] = None
    diffuse_radiation_instant: Optional[float] = None
    direct_radiation_instant: Optional[float] = None


class WeatherForecast(BaseModel):
    power_plant_id: int
    latitude: float
    longitude: float
    timezone: str
    elevation: float
    forecast_data: List[WeatherDataPoint]
    fetch_time: datetime


class OpenMeteoResponse(BaseModel):
    latitude: float
    longitude: float
    generationtime_ms: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str
    elevation: float
    minutely_15_units: dict
    minutely_15: dict

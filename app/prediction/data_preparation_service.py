import logging
import math
from typing import Any, Callable, Dict, List
from app.prediction.weather_forecast.weather_forecast_models import (
    WeatherDataPoint,
    WeatherForecast,
)

logger = logging.getLogger(__name__)


class FeatureCalculationError(Exception):
    pass


class UnsupportedFeatureError(Exception):
    pass


class DataPreparationService:
    def __init__(self):
        self._feature_calculators: Dict[str, Callable] = {}
        self._register_default_calculators()

    def prepare_data(
        self,
        weather_forecast: WeatherForecast,
        model_features: List[str],
        power_plant_capacity: int,
    ) -> List[List[float]]:
        try:
            self._validate_features(model_features)

            context = self._prepare_context(weather_forecast, power_plant_capacity)

            formatted_data = []
            for data_point in weather_forecast.forecast_data:
                feature_vector = self._calculate_features_for_data_point(
                    data_point, model_features, context
                )
                formatted_data.append(feature_vector)

            logger.debug(
                f"Prepared {len(formatted_data)} data points with {len(model_features)} features each"
            )

            return formatted_data

        except Exception as e:
            logger.error(f"Failed to prepare data: {e}")
            raise

    def _validate_features(self, model_features: List[str]) -> None:
        supported_features = set(self._feature_calculators.keys())
        unsupported_features = [
            f for f in model_features if f not in supported_features
        ]

        if unsupported_features:
            raise UnsupportedFeatureError(
                f"Unsupported features: {unsupported_features}."
            )

    def _prepare_context(
        self, weather_forecast: WeatherForecast, power_plant_capacity: int
    ) -> Dict[str, Any]:
        return {
            "power_plant_capacity": power_plant_capacity,
            "latitude": weather_forecast.latitude,
            "longitude": weather_forecast.longitude,
            "elevation": weather_forecast.elevation,
            "power_plant_id": weather_forecast.power_plant_id,
        }

    def _calculate_features_for_data_point(
        self,
        data_point: WeatherDataPoint,
        model_features: List[str],
        context: Dict[str, Any],
    ) -> List[float]:
        feature_vector = []

        for feature_name in model_features:
            try:
                calculator = self._feature_calculators[feature_name]
                value = calculator(data_point, context)

                # Handle None values by replacing with 0.0
                if value is None:
                    value = 0.0
                    logger.debug(
                        f"Missing value for feature '{feature_name}' at time {data_point.time}, using 0.0"
                    )

                feature_vector.append(float(value))

            except Exception as e:
                # Failed feature calculations are replaced with 0.0
                logger.warning(
                    f"Feature calculation failed for '{feature_name}' at time {data_point.time}: {e}, using 0.0"
                )
                feature_vector.append(0.0)

        return feature_vector

    def _register_default_calculators(self) -> None:
        # Direct weather data features
        self._register_weather_features()

        # Time-based features
        self._register_time_features()

        # Power plant features
        self._register_plant_features()

        # Derived/calculated features
        self._register_derived_features()

    def _register_weather_features(self) -> None:
        weather_attributes = [
            "temperature_2m",
            "relative_humidity_2m",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "wind_speed_10m",
            "wind_direction_10m",
            "shortwave_radiation",
            "shortwave_radiation_instant",
            "diffuse_radiation",
            "diffuse_radiation_instant",
            "direct_normal_irradiance",
            "et0_fao_evapotranspiration",
            "vapour_pressure_deficit",
            "is_day",
            "sunshine_duration",
            "direct_radiation_instant",
        ]

        for attr in weather_attributes:
            self._feature_calculators[attr] = lambda dp, ctx, attribute=attr: getattr(
                dp, attribute, None
            )

    def _register_time_features(self) -> None:
        """Register time-based feature calculators"""
        self._feature_calculators["datetime"] = lambda dp, ctx: dp.time
        self._feature_calculators["hour"] = lambda dp, ctx: dp.time.hour
        self._feature_calculators["month"] = lambda dp, ctx: dp.time.month
        self._feature_calculators["day"] = lambda dp, ctx: dp.time.day
        self._feature_calculators["day_of_year"] = (
            lambda dp, ctx: dp.time.timetuple().tm_yday
        )
        self._feature_calculators["week_of_year"] = (
            lambda dp, ctx: dp.time.isocalendar()[1]
        )
        self._feature_calculators["day_of_week"] = lambda dp, ctx: dp.time.weekday()

        self._feature_calculators["hour_sin"] = lambda dp, ctx: math.sin(dp.time.hour)
        self._feature_calculators["hour_cos"] = lambda dp, ctx: math.cos(dp.time.hour)
        self._feature_calculators["month_sin"] = lambda dp, ctx: math.sin(dp.time.month)
        self._feature_calculators["month_cos"] = lambda dp, ctx: math.cos(dp.time.month)

    def _register_plant_features(self) -> None:
        """Register power plant feature calculators"""

        self._feature_calculators["capacity"] = lambda dp, ctx: ctx[
            "power_plant_capacity"
        ]

        self._feature_calculators["latitude"] = lambda dp, ctx: ctx["latitude"]
        self._feature_calculators["longitude"] = lambda dp, ctx: ctx["longitude"]
        self._feature_calculators["elevation"] = lambda dp, ctx: ctx["elevation"]

    def _register_derived_features(self) -> None:
        """Register derived feature calculators"""

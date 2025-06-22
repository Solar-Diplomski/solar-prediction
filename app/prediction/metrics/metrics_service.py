import logging
from typing import List, Dict
from datetime import datetime
from decimal import Decimal
import numpy as np
from app.prediction.metrics.metrics_repository import MetricsRepository
from app.prediction.metrics.metrics_models import (
    HorizonMetric,
    CycleMetric,
)
from app.common.connectors.model_manager.model_manager_connector import (
    ModelManagerConnector,
)

logger = logging.getLogger(__name__)


class MetricsService:
    def __init__(
        self,
        metrics_repository: MetricsRepository,
        model_manager_connector: ModelManagerConnector,
    ):
        self._metrics_repository = metrics_repository
        self._model_manager_connector = model_manager_connector
        self._horizon_values = [0.25, 1, 6, 24, 48, 72]

    async def get_horizon_metric_types(self) -> List[str]:
        try:
            metric_types = await self._metrics_repository.get_horizon_metric_types()
            return metric_types
        except Exception as e:
            logger.error(f"Error fetching horizon metric types: {e}")
            raise

    async def get_cycle_metric_types(self) -> List[str]:
        try:
            metric_types = await self._metrics_repository.get_cycle_metric_types()
            return metric_types
        except Exception as e:
            logger.error(f"Error fetching cycle metric types: {e}")
            raise

    async def get_horizon_metrics(self, model_id: int) -> List[HorizonMetric]:
        try:
            rows = await self._metrics_repository.get_horizon_metrics(model_id)
            metrics = [
                HorizonMetric(
                    metric_type=row["metric_type"],
                    horizon=Decimal(str(row["horizon"])),
                    value=Decimal(str(row["value"])),
                )
                for row in rows
            ]

            return metrics
        except Exception as e:
            logger.error(f"Error fetching horizon metrics for model {model_id}: {e}")
            raise

    async def get_cycle_metrics(
        self, model_id: int, start_date: datetime, end_date: datetime
    ) -> List[CycleMetric]:
        try:
            rows = await self._metrics_repository.get_cycle_metrics(
                model_id, start_date, end_date
            )
            metrics = [
                CycleMetric(
                    time_of_forecast=row["time_of_forecast"],
                    metric_type=row["metric_type"],
                    value=Decimal(str(row["value"])),
                )
                for row in rows
            ]

            return metrics
        except Exception as e:
            logger.error(f"Error fetching cycle metrics for model {model_id}: {e}")
            raise

    async def calculate_horizon_metrics_by_model(self, model_id: int) -> None:
        try:
            metric_types = await self._metrics_repository.get_horizon_metric_types()
            model = self._model_manager_connector.fetch_model(model_id)

            data = (
                await self._metrics_repository.get_predictions_and_readings_for_model(
                    model_id, model.plant_id
                )
            )

            if not data:
                logger.warning(
                    f"No data found for model {model_id} and plant {model.plant_id}"
                )
                return

            horizon_data = self._group_data_by_horizon(data)

            metrics_to_save = []
            for horizon in self._horizon_values:
                if horizon not in horizon_data:
                    logger.warning(
                        f"No data for horizon {horizon} for model {model_id}"
                    )
                    continue

                horizon_predictions = horizon_data[horizon]
                predicted_values = [
                    row["predicted_power"] for row in horizon_predictions
                ]
                actual_values = [row["actual_power"] for row in horizon_predictions]

                for metric_type in metric_types:
                    metric_value = self._calculate_metric(
                        metric_type, predicted_values, actual_values
                    )
                    metrics_to_save.append(
                        (model_id, metric_type, horizon, metric_value)
                    )

            if metrics_to_save:
                await self._metrics_repository.save_horizon_metrics(metrics_to_save)
                logger.info(
                    f"Calculated and saved {len(metrics_to_save)} horizon metrics for model {model_id}"
                )
            else:
                logger.warning(f"No metrics calculated for model {model_id}")

        except Exception as e:
            logger.error(f"Error calculating horizon metrics for model {model_id}: {e}")
            raise

    async def calculate_horizon_metrics_by_plant(self, plant_id: int) -> None:
        try:
            models = self._model_manager_connector.fetch_models_for_power_plant(
                plant_id
            )

            if not models:
                logger.warning(f"No models found for plant {plant_id}")
                return

            for model in models:
                model_id = model["id"]
                logger.info(
                    f"Calculating metrics for model {model_id} in plant {plant_id}"
                )
                await self.calculate_horizon_metrics_by_model(model_id, plant_id)

            logger.info(
                f"Completed calculating horizon metrics for {len(models)} models in plant {plant_id}"
            )

        except Exception as e:
            logger.error(f"Error calculating horizon metrics for plant {plant_id}: {e}")
            raise

    def _group_data_by_horizon(self, data: List[dict]) -> Dict[float, List[dict]]:
        horizon_data = {}
        for row in data:
            horizon = float(row["horizon"])
            if horizon not in horizon_data:
                horizon_data[horizon] = []
            horizon_data[horizon].append(row)

        return horizon_data

    def _calculate_metric(
        self, metric_type: str, predicted: List[float], actual: List[float]
    ) -> float:
        if not predicted or not actual or len(predicted) != len(actual):
            raise ValueError(
                "Predicted and actual values must have the same non-zero length"
            )

        predicted_array = np.array(predicted)
        actual_array = np.array(actual)
        errors = predicted_array - actual_array

        if metric_type == "MAE":
            return float(np.mean(np.abs(errors)))
        elif metric_type == "RMSE":
            return float(np.sqrt(np.mean(errors**2)))
        elif metric_type == "MBE":
            return float(np.mean(errors))
        else:
            raise ValueError(f"Unsupported metric type: {metric_type}")

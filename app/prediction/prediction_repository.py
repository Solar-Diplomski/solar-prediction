import asyncio
import logging
from typing import List
from datetime import datetime
from app.prediction.prediction_models import PowerPrediction
from app.config.database import db_manager

logger = logging.getLogger(__name__)


class PredictionRepository:
    def __init__(self):
        self.insert_query = """
            INSERT INTO power_predictions (
                prediction_time, model_id, created_at, predicted_power, horizon
            ) VALUES (
                $1, $2, $3, $4, $5
            ) ON CONFLICT (prediction_time, model_id, created_at) DO NOTHING
        """

    async def get_forecast_data(
        self, model_id: int, start_date: datetime, end_date: datetime
    ) -> List[dict]:
        """
        Fetch forecast data for a specific model within a date range.
        Returns only the most recent prediction for each prediction time.
        """
        query = """
            SELECT DISTINCT ON (prediction_time) 
                model_id as id,
                prediction_time,
                predicted_power as power_output
            FROM power_predictions
            WHERE model_id = $1 
            AND prediction_time >= $2 
            AND prediction_time <= $3
            ORDER BY prediction_time, created_at DESC
        """

        try:
            rows = await db_manager.execute(query, model_id, start_date, end_date)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch forecast data for model {model_id}: {e}")
            raise

    async def get_forecast_data_by_time_of_forecast(
        self, model_id: int, created_at: datetime
    ) -> List[dict]:
        """
        Fetch forecast data for a specific model and created_at timestamp.
        Returns all predictions from that specific forecast run.
        """
        query = """
            SELECT 
                model_id as id,
                prediction_time,
                predicted_power as power_output
            FROM power_predictions
            WHERE model_id = $1 
            AND created_at = $2
            ORDER BY prediction_time
        """

        try:
            rows = await db_manager.execute(query, model_id, created_at)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(
                f"Failed to fetch forecast data for model {model_id} and created_at {created_at}: {e}"
            )
            raise

    async def get_unique_forecast_timestamps(self, model_id: int) -> List[datetime]:
        """
        Fetch unique created_at timestamps for a specific model.
        Returns timestamps ordered by most recent first.
        """
        query = """
            SELECT DISTINCT created_at
            FROM power_predictions
            WHERE model_id = $1
            ORDER BY created_at DESC
        """

        try:
            rows = await db_manager.execute(query, model_id)
            return [row["created_at"] for row in rows]
        except Exception as e:
            logger.error(
                f"Failed to fetch forecast timestamps for model {model_id}: {e}"
            )
            raise

    def save_power_predictions_batch(self, predictions: List[PowerPrediction]) -> None:
        try:
            loop = asyncio.get_event_loop()

            # Create the task without waiting for it
            task = loop.create_task(
                self._save_power_predictions_batch_async(predictions)
            )

            # Add error handling callback
            task.add_done_callback(self._handle_save_completion)

            logger.info(
                f"Started background save task for {len(predictions)} power predictions"
            )

        except Exception as e:
            logger.error(f"Failed to start power prediction save task: {e}")

    async def _save_power_predictions_batch_async(
        self, predictions: List[PowerPrediction]
    ) -> int:
        if not predictions:
            return 0

        try:
            prediction_records = []
            for prediction in predictions:
                record = (
                    prediction.prediction_time,
                    prediction.model_id,
                    prediction.created_at,
                    prediction.predicted_power,
                    prediction.horizon,
                )
                prediction_records.append(record)

            await db_manager.execute_many(self.insert_query, prediction_records)
            return len(predictions)

        except Exception as e:
            logger.error(f"Failed to save power predictions batch: {e}")
            return 0

    def _handle_save_completion(self, task: asyncio.Task):
        """Callback to handle task completion and errors"""
        try:
            result = task.result()
            logger.debug(f"Power prediction save task completed with result: {result}")
        except Exception as e:
            logger.error(f"Power prediction save task failed: {e}")

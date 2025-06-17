import logging
from typing import List
from datetime import datetime
from app.prediction.power_readings.power_readings_models import PowerReading
from app.config.database import db_manager

logger = logging.getLogger(__name__)


class PowerReadingsRepository:
    def __init__(self):
        self.insert_query = """
            INSERT INTO power_readings (
                timestamp, plant_id, power_w
            ) VALUES (
                $1, $2, $3
            ) ON CONFLICT (timestamp, plant_id) DO NOTHING
        """

    async def get_power_readings(
        self, plant_id: int, start_date: datetime, end_date: datetime
    ) -> List[PowerReading]:
        """
        Fetch power readings for a specific plant within a date range.
        """
        query = """
            SELECT timestamp, power_w
            FROM power_readings
            WHERE plant_id = $1 
            AND timestamp >= $2 
            AND timestamp <= $3
            ORDER BY timestamp
        """

        try:
            rows = await db_manager.execute(query, plant_id, start_date, end_date)
            return [
                PowerReading(timestamp=row["timestamp"], power_w=row["power_w"])
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Failed to fetch power readings for plant {plant_id}: {e}")
            raise

    async def save_power_readings_batch(
        self, readings: List[PowerReading], plant_id: int
    ) -> int:
        """
        Save a batch of power readings for a specific plant.
        Returns the number of successfully inserted rows.
        """
        if not readings:
            return 0

        try:
            reading_records = []
            for reading in readings:
                record = (
                    reading.timestamp,
                    plant_id,
                    reading.power_w,
                )
                reading_records.append(record)

            await db_manager.execute_many(self.insert_query, reading_records)
            logger.info(
                f"Successfully saved {len(readings)} power readings for plant {plant_id}"
            )
            return len(readings)

        except Exception as e:
            logger.error(
                f"Failed to save power readings batch for plant {plant_id}: {e}"
            )
            raise

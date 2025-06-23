import csv
import io
import logging
from typing import Set, List
from datetime import datetime
from fastapi import UploadFile
from app.prediction.power_readings.power_readings_models import (
    PowerReading,
    CSVValidationResult,
    CSVUploadResponse,
)
from app.prediction.power_readings.power_readings_repository import (
    PowerReadingsRepository,
)
from app.prediction.metrics.metrics_service import MetricsService

logger = logging.getLogger(__name__)


class PowerReadingsService:

    def __init__(
        self,
        power_readings_repository: PowerReadingsRepository,
        metrics_service: MetricsService,
    ):
        self._repository = power_readings_repository
        self._metrics_service = metrics_service

    async def get_power_readings(
        self, plant_id: int, start_date: datetime, end_date: datetime
    ) -> List[PowerReading]:
        """
        Get power readings for a specific plant within a date range.
        """
        return await self._repository.get_power_readings(plant_id, start_date, end_date)

    async def upload_csv_readings(
        self, file: UploadFile, plant_id: int
    ) -> CSVUploadResponse:
        """
        Upload and process CSV file containing power readings.
        """
        try:
            validation_result = await self._validate_and_parse_csv(file)

            if not validation_result.is_valid:
                return CSVUploadResponse(
                    success=False,
                    message="CSV validation failed",
                    validation_errors=validation_result.errors,
                )

            await self._repository.save_power_readings_batch(
                validation_result.readings, plant_id
            )

            try:
                await self._trigger_metrics_calculation(plant_id)
                logger.info(
                    f"Successfully completed metric calculation for plant {plant_id}"
                )
            except Exception as e:
                logger.error(f"Failed to calculate metrics for plant {plant_id}: {e}")

            return CSVUploadResponse(
                success=True,
                message=f"Successfully uploaded {len(validation_result.readings)} power readings",
            )

        except Exception as e:
            logger.error(f"Failed to save CSV for plant {plant_id}: {e}")
            return CSVUploadResponse(
                success=False,
                message="Failed to save CSV",
            )

    async def _trigger_metrics_calculation(self, plant_id: int) -> None:
        """
        Trigger metric calculations for all models in the power plant.
        """
        try:
            # Calculate horizon metrics for all models in the plant
            await self._metrics_service.calculate_horizon_metrics_by_plant(plant_id)
            logger.info(f"Completed horizon metrics calculation for plant {plant_id}")

            # Calculate cycle metrics for all models in the plant
            await self._metrics_service.calculate_cycle_metrics_by_plant(plant_id)
            logger.info(f"Completed cycle metrics calculation for plant {plant_id}")

        except Exception as e:
            logger.error(f"Error during metrics calculation for plant {plant_id}: {e}")
            raise

    async def _validate_and_parse_csv(self, file: UploadFile) -> CSVValidationResult:
        """
        Validate and parse CSV file content.
        """
        errors = []
        readings = []
        seen_timestamps: Set[datetime] = set()

        try:
            content = await file.read()
            content_str = content.decode("utf-8")

            csv_reader = csv.reader(io.StringIO(content_str))

            for row_number, row in enumerate(csv_reader, start=1):
                if len(row) != 2:
                    errors.append(
                        f"Row {row_number}: Expected 2 columns, got {len(row)}"
                    )
                    continue

                timestamp_str, power_str = row

                try:
                    timestamp = datetime.fromisoformat(
                        timestamp_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    errors.append(
                        f"Row {row_number}: Invalid timestamp format '{timestamp_str}'"
                    )
                    continue

                if timestamp in seen_timestamps:
                    errors.append(
                        f"Row {row_number}: Duplicate timestamp '{timestamp_str}'"
                    )
                    continue
                seen_timestamps.add(timestamp)

                try:
                    power_w = float(power_str)
                except ValueError:
                    errors.append(
                        f"Row {row_number}: Invalid power value '{power_str}'"
                    )
                    continue

                readings.append(PowerReading(timestamp=timestamp, power_w=power_w))

        except UnicodeDecodeError:
            errors.append(
                "File contains invalid characters. Please ensure the file is saved as UTF-8."
            )
        except MemoryError:
            errors.append("File is too large to process. Please use a smaller file.")
        except Exception:
            errors.append(
                "Unable to read the CSV file. Please check that the file is not corrupted and try again."
            )

        return CSVValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            readings=readings,
        )

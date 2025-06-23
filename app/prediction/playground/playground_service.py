import logging
import csv
import io
from typing import List, Optional
from datetime import datetime
from fastapi import UploadFile
from app.prediction.playground.playground_models import (
    PlaygroundFeatureInfo,
    PlaygroundPredictionResponse,
    PlaygroundPredictionRow,
    PlaygroundMetric,
    CSVValidationResult,
)
from app.common.connectors.model_manager.model_manager_connector import (
    ModelManagerConnector,
)

from app.common.models.model_factory import ModelFactory
from app.prediction.metrics.metrics_service import MetricsService
from app.prediction.power_readings.power_readings_service import PowerReadingsService

logger = logging.getLogger(__name__)


class PlaygroundService:

    def __init__(
        self,
        model_manager_connector: ModelManagerConnector,
        metrics_service: MetricsService,
        power_readings_service: PowerReadingsService,
    ):
        self._model_manager_connector = model_manager_connector
        self._metrics_service = metrics_service
        self._power_readings_service = power_readings_service

    def get_model_features(self, model_id: int) -> Optional[PlaygroundFeatureInfo]:
        """Get model features and metadata for playground use"""
        try:
            model_metadata = self._model_manager_connector.fetch_model(model_id)

            if not model_metadata:
                return None

            return PlaygroundFeatureInfo(
                model_id=model_metadata.id,
                model_name=model_metadata.name,
                features=model_metadata.features,
                plant_id=model_metadata.plant_id,
                plant_name=model_metadata.plant_name,
            )

        except Exception as e:
            logger.error(f"Error getting model features for model {model_id}: {e}")
            return None

    async def predict_from_csv(
        self, model_id: int, file: UploadFile
    ) -> PlaygroundPredictionResponse:
        """Process CSV file and generate predictions with metrics"""
        try:
            model_metadata = self._model_manager_connector.fetch_model(model_id)
            if not model_metadata:
                return PlaygroundPredictionResponse(
                    model_id=model_id,
                    predictions=[],
                    metrics=[],
                    input_rows=0,
                    success=False,
                    message=f"Model {model_id} not found",
                    validation_errors=["Model not found"],
                )

            # Download and create ML model
            model_file = self._model_manager_connector.download_model_file(model_id)
            if not model_file:
                return PlaygroundPredictionResponse(
                    model_id=model_id,
                    predictions=[],
                    metrics=[],
                    input_rows=0,
                    success=False,
                    message=f"Failed to download model {model_id}",
                    validation_errors=["Model file not available"],
                )

            ml_model = ModelFactory.create_model(model_metadata, model_file)

            # Check file size (max 100MB)
            file_size_mb = 100
            max_file_size = file_size_mb * 1024 * 1024  # 100MB in bytes

            # Get file size
            file.file.seek(0, 2)  # Seek to end of file
            file_size = file.file.tell()
            file.file.seek(0)  # Reset to beginning

            if file_size > max_file_size:
                return PlaygroundPredictionResponse(
                    model_id=model_id,
                    predictions=[],
                    metrics=[],
                    input_rows=0,
                    success=False,
                    message=f"File size exceeds the maximum limit of {file_size_mb}MB",
                    validation_errors=[
                        f"File size is {file_size / (1024*1024):.1f}MB. Maximum allowed size is {file_size_mb}MB."
                    ],
                )

            # Validate CSV
            validation_result = await self._validate_csv(file, ml_model.features)
            if not validation_result.is_valid:
                return PlaygroundPredictionResponse(
                    model_id=model_id,
                    predictions=[],
                    metrics=[],
                    input_rows=0,
                    success=False,
                    message="CSV validation failed",
                    validation_errors=validation_result.errors,
                )

            raw_predictions = ml_model.predict(validation_result.feature_data)

            prediction_rows = []
            for i, timestamp in enumerate(validation_result.timestamps):
                if i < len(raw_predictions):
                    prediction_rows.append(
                        PlaygroundPredictionRow(
                            timestamp=timestamp, prediction=float(raw_predictions[i])
                        )
                    )

            # Calculate metrics if we have power readings
            metrics = await self._calculate_metrics_for_predictions(
                model_metadata.plant_id, validation_result.timestamps, raw_predictions
            )

            return PlaygroundPredictionResponse(
                model_id=model_id,
                predictions=prediction_rows,
                metrics=metrics,
                input_rows=validation_result.row_count,
                success=True,
                message=f"Successfully generated {len(prediction_rows)} predictions",
            )

        except Exception as e:
            logger.error(f"Error predicting from CSV for model {model_id}: {e}")
            return PlaygroundPredictionResponse(
                model_id=model_id,
                predictions=[],
                metrics=[],
                input_rows=0,
                success=False,
                message="An error occurred while processing your request. Please check your input and try again.",
                validation_errors=[
                    "Processing failed. Please verify your CSV format and model selection."
                ],
            )

    async def _validate_csv(
        self, file: UploadFile, required_features: List[str]
    ) -> CSVValidationResult:
        """Validate CSV data with timestamp as first column and exact feature matching"""
        try:
            content = await file.read()
            content_str = content.decode("utf-8")

            # Parse CSV
            csv_reader = csv.DictReader(io.StringIO(content_str))

            # Check if headers exist
            if not csv_reader.fieldnames:
                return CSVValidationResult(
                    is_valid=False,
                    errors=["CSV file is empty or has no headers"],
                    timestamps=[],
                    feature_data=[],
                    row_count=0,
                )

            # Check for exact match of columns (timestamp + features)
            expected_columns = ["timestamp"] + required_features
            csv_columns = list(csv_reader.fieldnames)

            if csv_columns != expected_columns:
                missing_columns = set(expected_columns) - set(csv_columns)
                extra_columns = set(csv_columns) - set(expected_columns)
                wrong_order = csv_columns != expected_columns

                errors = []
                if missing_columns:
                    errors.append(
                        f"Missing required columns: {sorted(missing_columns)}"
                    )
                if extra_columns:
                    errors.append(f"Unexpected columns: {sorted(extra_columns)}")
                if wrong_order and not missing_columns and not extra_columns:
                    errors.append(
                        f"Columns are in wrong order. Expected order: {expected_columns}"
                    )

                return CSVValidationResult(
                    is_valid=False,
                    errors=errors,
                    timestamps=[],
                    feature_data=[],
                    row_count=0,
                )

            # Convert rows to feature vectors and extract timestamps
            feature_data = []
            timestamps = []
            row_count = 0
            errors = []

            for row_num, row in enumerate(csv_reader, start=1):
                try:
                    # Parse timestamp
                    timestamp_str = row["timestamp"]
                    if not timestamp_str:
                        errors.append(f"Row {row_num}: Missing timestamp")
                        continue

                    try:
                        timestamp = datetime.fromisoformat(
                            timestamp_str.replace("Z", "+00:00")
                        )
                        timestamps.append(timestamp)
                    except ValueError:
                        errors.append(
                            f"Row {row_num}: Invalid timestamp format '{timestamp_str}'"
                        )
                        continue

                    # Extract features in the exact order required by the model
                    feature_vector = []
                    for feature in required_features:
                        value = row[feature]
                        if value is None or value == "":
                            errors.append(
                                f"Row {row_num}: Missing value for feature '{feature}'"
                            )
                            break
                        try:
                            feature_vector.append(float(value))
                        except ValueError:
                            errors.append(
                                f"Row {row_num}: Invalid numeric value '{value}' for feature '{feature}'"
                            )
                            break

                    if len(feature_vector) == len(required_features):
                        feature_data.append(feature_vector)
                        row_count += 1

                except Exception as e:
                    errors.append(f"Row {row_num}: Error processing row: {str(e)}")

            if errors:
                return CSVValidationResult(
                    is_valid=False,
                    errors=errors,
                    timestamps=[],
                    feature_data=[],
                    row_count=0,
                )

            if row_count == 0:
                return CSVValidationResult(
                    is_valid=False,
                    errors=["No valid data rows found"],
                    timestamps=[],
                    feature_data=[],
                    row_count=0,
                )

            return CSVValidationResult(
                is_valid=True,
                errors=[],
                timestamps=timestamps,
                feature_data=feature_data,
                row_count=row_count,
            )

        except UnicodeDecodeError:
            return CSVValidationResult(
                is_valid=False,
                errors=[
                    "File contains invalid characters. Please ensure the file is saved as UTF-8."
                ],
                timestamps=[],
                feature_data=[],
                row_count=0,
            )
        except Exception as e:
            return CSVValidationResult(
                is_valid=False,
                errors=[f"CSV parsing error: {str(e)}"],
                timestamps=[],
                feature_data=[],
                row_count=0,
            )

    async def _calculate_metrics_for_predictions(
        self, plant_id: int, timestamps: List[datetime], predictions: List[float]
    ) -> List[PlaygroundMetric]:
        """Calculate metrics by fetching actual power readings for the given timestamps"""
        try:
            if not timestamps or not predictions:
                return []

            # Get date range for fetching power readings
            start_date = min(timestamps)
            end_date = max(timestamps)

            # Fetch power readings for the plant
            power_readings = await self._power_readings_service.get_power_readings(
                plant_id, start_date, end_date
            )

            if not power_readings:
                logger.warning(
                    f"No power readings found for plant {plant_id} in date range {start_date} to {end_date}"
                )
                return []

            # Create timestamp to reading mapping
            readings_map = {
                reading.timestamp: reading.power_w for reading in power_readings
            }

            # Match predictions with actual readings
            matched_predictions = []
            matched_actuals = []

            for i, timestamp in enumerate(timestamps):
                if timestamp in readings_map and i < len(predictions):
                    matched_predictions.append(predictions[i])
                    matched_actuals.append(readings_map[timestamp])

            if not matched_predictions:
                logger.warning("No matching power readings found for timestamps")
                return []

            # Calculate metrics using the metrics service
            metrics = []
            metric_types = ["RMSE", "MAE", "MBE"]

            for metric_type in metric_types:
                try:
                    metric_value = self._metrics_service.calculate_metric(
                        metric_type, matched_predictions, matched_actuals
                    )
                    metrics.append(
                        PlaygroundMetric(metric_type=metric_type, value=metric_value)
                    )
                except Exception as e:
                    logger.warning(f"Failed to calculate {metric_type}: {e}")

            return metrics

        except Exception as e:
            logger.error(f"Error calculating metrics for plant {plant_id}: {e}")
            return []

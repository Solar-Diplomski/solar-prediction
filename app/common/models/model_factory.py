from app.common.models.ml_models import MLModel, JoblibModel
from app.common.connectors.model_manager.model_manager_models import ModelMetadata


class ModelFactory:

    @staticmethod
    def create_model(metadata: ModelMetadata, file_content: bytes) -> MLModel:
        if metadata.file_type == "joblib":
            return JoblibModel(metadata, file_content)
        raise ValueError(f"Unsupported file type: {metadata.file_type}")

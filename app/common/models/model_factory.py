from app.common.models.ml_models import MLModel, JoblibModel, PickleModel, ZipModel
from app.common.connectors.model_manager.model_manager_models import ModelMetadata


class ModelFactory:

    @staticmethod
    def create_model(metadata: ModelMetadata, file_content: bytes) -> MLModel:
        return ModelFactory.create_model_by_type(
            metadata.file_type, file_content, metadata
        )

    @staticmethod
    def create_model_by_type(
        file_type: str, file_content: bytes, metadata: ModelMetadata = None
    ) -> MLModel:
        if file_type == "joblib":
            return JoblibModel(metadata, file_content)
        elif file_type in ("pkl", "pickle"):
            return PickleModel(metadata, file_content)
        elif file_type == "zip":
            return ZipModel(metadata, file_content)
        raise ValueError(f"Unsupported file type: {file_type}")

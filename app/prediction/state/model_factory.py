from app.prediction.state.state_models import ModelMetadata, MLModel, JoblibModel


class ModelFactory:

    @staticmethod
    def create_model(metadata: ModelMetadata, file_content: bytes) -> MLModel:
        if metadata.file_type == "joblib":
            return JoblibModel(metadata, file_content)
        raise ValueError(f"Unsupported file type: {metadata.file_type}")

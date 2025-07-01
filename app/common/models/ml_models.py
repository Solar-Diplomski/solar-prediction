from abc import ABC, abstractmethod
from typing import Any, List
import joblib
import pickle
from typing import Optional
import io
import zipfile
import tempfile
import importlib.util
import sys
from app.common.connectors.model_manager.model_manager_models import ModelMetadata


class MLModel(ABC):

    def __init__(self, metadata: ModelMetadata, file_content: bytes):
        self.metadata: ModelMetadata = metadata
        self.features: List[str] = metadata.features
        self.plant_id: int = metadata.plant_id
        self._model: Optional[Any] = None
        self._load(file_content)

    @abstractmethod
    def _load(self, file_content: bytes):
        pass

    @abstractmethod
    def predict(self, features: List[List[float]]) -> List[float]:
        pass


class JoblibModel(MLModel):

    def _load(self, file_content: bytes):
        file_like_object = io.BytesIO(file_content)
        self._model = joblib.load(file_like_object)

    def predict(self, features: List[List[float]]) -> List[float]:
        return self._model.predict(features)


class PickleModel(MLModel):

    def _load(self, file_content: bytes):
        file_like_object = io.BytesIO(file_content)
        self._model = pickle.load(file_like_object)

    def predict(self, features: List[List[float]]) -> List[float]:
        return self._model.predict(features)


class ZipModel(MLModel):

    def __init__(self, metadata: ModelMetadata, file_content: bytes):
        self._loaded_module = None
        self._delegate_model = None
        super().__init__(metadata, file_content)

    def _load(self, file_content: bytes):
        with io.BytesIO(file_content) as file_like:
            with zipfile.ZipFile(file_like, "r") as zip_ref:
                # Get list of files in the zip
                file_list = zip_ref.namelist()

                # Find the model and .py files
                model_files = [
                    f for f in file_list if f.endswith((".joblib", ".pkl", ".pickle"))
                ]
                py_files = [f for f in file_list if f.endswith(".py")]

                if not model_files:
                    raise ValueError(
                        "No model file (.joblib, .pkl, .pickle) found in the zip archive"
                    )
                if not py_files:
                    raise ValueError("No .py file found in the zip archive")

                # Use the first model and .py files found
                model_filename = model_files[0]
                py_filename = py_files[0]

                # Extract files to a temporary directory
                with tempfile.TemporaryDirectory() as temp_dir:
                    model_file_path = zip_ref.extract(model_filename, temp_dir)
                    py_file_path = zip_ref.extract(py_filename, temp_dir)

                    # Dynamically import the class definition and keep it in memory
                    module_name = f"dynamic_model_{id(self)}"  # Unique module name
                    spec = importlib.util.spec_from_file_location(
                        module_name, py_file_path
                    )
                    self._loaded_module = importlib.util.module_from_spec(spec)

                    # Add to sys.modules so the serialization library can find it
                    sys.modules[module_name] = self._loaded_module
                    spec.loader.exec_module(self._loaded_module)

                    # Make classes available in __main__ module for joblib/pickle to find
                    import __main__

                    main_backup = {}

                    # Backup any existing attributes in __main__ that we might overwrite
                    for attr_name in dir(self._loaded_module):
                        if not attr_name.startswith("_"):
                            if hasattr(__main__, attr_name):
                                main_backup[attr_name] = getattr(__main__, attr_name)
                            setattr(
                                __main__,
                                attr_name,
                                getattr(self._loaded_module, attr_name),
                            )

                    try:
                        # Read the model file content and delegate to appropriate model type
                        with open(model_file_path, "rb") as f:
                            model_content = f.read()

                        # Extract file extension and let ModelFactory determine the type
                        file_extension = model_filename.split(".")[-1].lower()

                        # Import here to avoid circular import
                        from app.common.models.model_factory import ModelFactory

                        # Create the delegate model with the loaded classes available
                        self._delegate_model = ModelFactory.create_model_by_type(
                            file_extension, model_content, self.metadata
                        )
                    finally:
                        # Restore __main__ module state
                        for attr_name in dir(self._loaded_module):
                            if not attr_name.startswith("_"):
                                if attr_name in main_backup:
                                    setattr(__main__, attr_name, main_backup[attr_name])
                                elif hasattr(__main__, attr_name):
                                    delattr(__main__, attr_name)

    def predict(self, features: List[List[float]]) -> List[float]:
        return self._delegate_model.predict(features)

    def __del__(self):
        # Clean up the dynamically loaded module when the object is destroyed
        if self._loaded_module and hasattr(self._loaded_module, "__name__"):
            module_name = self._loaded_module.__name__
            if module_name in sys.modules:
                del sys.modules[module_name]

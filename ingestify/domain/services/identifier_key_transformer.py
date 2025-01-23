from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Optional, Union

from ingestify.exceptions import IngestifyError


class TransformationType(Enum):
    IDENTITY = "IDENTITY"
    BUCKET = "BUCKET"
    RANGE = "RANGE"
    CUSTOM = "CUSTOM"


class Transformation(ABC):
    @property
    @abstractmethod
    def transformation_type(self) -> TransformationType:
        pass

    def is_identity(self) -> bool:
        return self.transformation_type == TransformationType.IDENTITY

    @abstractmethod
    def __call__(self, id_key_value: Union[str, int]) -> str:
        pass

    @classmethod
    def from_dict(cls, config: dict) -> "Transformation":
        type_ = config.pop("type")
        if type_ == "bucket":
            return BucketTransformation(**config)
        else:
            raise IngestifyError(f"Cannot build Transformation from {config}")


class IdentityTransformation(Transformation):
    transformation_type = TransformationType.IDENTITY

    def __call__(self, id_key_value: Union[str, int]) -> str:
        # Return the original value as a string
        return str(id_key_value)


class BucketTransformation(Transformation):
    transformation_type = TransformationType.BUCKET

    def __init__(self, bucket_size: int = None, bucket_count: int = None):
        self.bucket_size = bucket_size
        self.bucket_count = bucket_count

    def __call__(self, id_key_value: Union[str, int]) -> str:
        if self.bucket_count:
            return str(int(id_key_value) % self.bucket_count)
        elif self.bucket_size:
            bucket_start = int(id_key_value) // self.bucket_size * self.bucket_size
            bucket_end = bucket_start + self.bucket_size - 1
            return f"{bucket_start}-{bucket_end}"
        else:
            raise IngestifyError("Invalid BucketTransformation")


class IdentifierTransformer:
    def __init__(self):
        # Mapping of (provider, dataset_type, id_key) to the transformation
        self.key_transformations: dict[tuple[str, str, str], Transformation] = {}

    def register_transformation(
        self,
        provider: str,
        dataset_type: str,
        id_key: str,
        transformation: Union[Transformation, dict],
    ):
        """
        Registers a transformation for a specific (provider, dataset_type, id_key).
        """
        if isinstance(transformation, dict):
            transformation = Transformation.from_dict(transformation)

        self.key_transformations[(provider, dataset_type, id_key)] = transformation

    def get_transformation(
        self, provider: str, dataset_type: str, id_key: str
    ) -> Transformation:
        """
        Retrieves the transformation for the given column or defaults to identity.
        """
        transformation = self.key_transformations.get((provider, dataset_type, id_key))
        return transformation if transformation else IdentityTransformation()

    def to_path(self, provider: str, dataset_type: str, identifier: dict) -> str:
        """
        Transforms the identifier into a path string using registered transformations.
        For non-identity transformations, includes both transformed and original values,
        with the transformed value appearing first and including the suffix.
        """
        path_parts = []
        for key, value in identifier.items():
            transformation = self.get_transformation(provider, dataset_type, key)
            if not transformation.is_identity():
                # Non-identity transformation: include both transformed and original
                transformed_value = transformation(value)
                suffix = transformation.transformation_type.value.lower()
                path_parts.append(f"{key}_{suffix}={transformed_value}")

            # Append the original value (either standalone for identity or alongside transformed)
            path_parts.append(f"{key}={value}")

        # Join the parts with `/` to form the full path
        return "/".join(path_parts)

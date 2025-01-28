from pydantic import BaseModel as PydanticBaseModel, ConfigDict


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, from_attributes=True)

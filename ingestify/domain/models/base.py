from functools import partial
from typing import ClassVar, Any, Optional

import pydantic
from pydantic import BaseModel as PydanticBaseModel, ConfigDict


# class BaseModel(PydanticBaseModel):
#     model_config = ConfigDict(arbitrary_types_allowed=True)
#
#     _sa_instance_state: Optional[dict] = None
from sqlalchemy.orm import MappedAsDataclass


class BaseModel(
    MappedAsDataclass,
    # DeclarativeBase,
    dataclass_callable=partial(
        pydantic.dataclasses.dataclass, config=ConfigDict(arbitrary_types_allowed=True)
    ),
):
    pass

import abc
import inspect
import time
from datetime import datetime, timezone
from typing import Generic, Type, TypeVar


class ComponentRegistry:
    def __init__(self):
        self.__registered_components = {}

        class _Registered(abc.ABCMeta):
            def __new__(mcs, cls_name, bases, class_dict):
                class_dict["name"] = cls_name
                component_cls = super(_Registered, mcs).__new__(
                    mcs, cls_name, bases, class_dict
                )
                if not inspect.isabstract(component_cls):
                    self.register_component(cls_name, component_cls)
                return component_cls

        self.__metaclass = _Registered

    @property
    def metaclass(self):
        return self.__metaclass

    def register_component(self, cls_name, component_cls):
        self.__registered_components[cls_name] = component_cls

    def get_component(self, cls_name: str):
        return self.__registered_components[cls_name]


T = TypeVar("T")
R = TypeVar("R")


class ComponentFactory(Generic[T]):
    def __init__(self, registry: ComponentRegistry):
        self.registry = registry

    @classmethod
    def build_factory(
        cls, component_cls: Type[R], registry: ComponentRegistry
    ) -> "ComponentFactory[R]":
        return cls[component_cls](registry)

    def build(self, cls_name, **kwargs) -> T:
        component_cls = self.registry.get_component(cls_name)
        try:
            return component_cls.from_dict(**kwargs)
        except AttributeError:
            pass
        try:
            return component_cls(**kwargs)
        except TypeError as e:
            raise e
            # raise TypeError(f"Could not initialize {cls_name}")


def key_from_dict(d: dict) -> str:
    return "/".join([f"{k}={v}" for k, v in sorted(d.items()) if not k.startswith("_")])


def utcnow() -> datetime:
    return datetime.fromtimestamp(time.time(), timezone.utc)

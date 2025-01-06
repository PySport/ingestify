import abc
import asyncio
import inspect
import logging
import os
import time
import re
from multiprocessing import get_context, cpu_count, get_all_start_methods

from datetime import datetime, timezone
from string import Template
from typing import Dict, Generic, Type, TypeVar, Tuple, Optional, Any, Callable, Awaitable, List, Iterable

import cloudpickle
from typing_extensions import Self


from itertools import islice


logger = logging.getLogger(__name__)


def chunker(it, size):
    iterator = iter(it)
    while chunk := list(islice(iterator, size)):
        yield chunk


def sanitize_exception_message(exception_message):
    """
    Sanitizes an exception message by removing any sensitive information such as passwords.
    """
    # Regular expression to identify potential sensitive information like URLs with passwords
    sensitive_info_pattern = r":(\w+)@"

    # Replace sensitive information with a placeholder
    sanitized_message = re.sub(sensitive_info_pattern, ":******@", exception_message)

    return sanitized_message


def key_from_dict(d: dict) -> str:
    return "/".join([f"{k}={v}" for k, v in sorted(d.items()) if not k.startswith("_")])


def utcnow() -> datetime:
    return datetime.fromtimestamp(time.time(), timezone.utc)


NOT_SET = object()


class AttributeBag:
    def __init__(self, attributes=NOT_SET, **kwargs):
        if attributes is not NOT_SET:
            self.attributes = attributes
        else:
            self.attributes = kwargs
        self.key = key_from_dict(self.attributes)

    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        if "attributes" in self.__dict__ and item in self.attributes:
            return self.attributes[item]
        raise AttributeError(f"{item} not found")

    def items(self):
        return self.attributes.items()

    def format_string(self, string: str):
        return Template(string).substitute(**self.attributes)

    def matches(self, attributes: Dict) -> bool:
        for k, v in self.attributes.items():
            if attributes.get(k) != v:
                return False
        return True

    @property
    def filtered_attributes(self):
        return {k: v for k, v in self.attributes.items() if not k.startswith("_")}

    def __eq__(self, other):
        if isinstance(other, AttributeBag):
            return self.key == other.key

    def __hash__(self):
        return hash(self.key)

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join([f'{k}={v}' for k, v in self.filtered_attributes.items()])})"

    def __str__(self):
        return "/".join([f"{k}={v}" for k, v in self.filtered_attributes.items()])

    @classmethod
    def create_from(cls, other: "AttributeBag", **kwargs):
        _args = dict(**other.attributes)
        _args.update(kwargs)

        return cls(**_args)

    def split(self, attribute_name: str) -> Tuple[Self, Optional[Any]]:
        return self.attributes.get(attribute_name), self.__class__(
            **{k: v for k, v in self.attributes.items() if k != attribute_name}
        )

def cloud_unpack_and_call(args):
    f_pickled, org_args = args

    f = cloudpickle.loads(f_pickled)
    return f(org_args)


def map_in_pool(func, iterable, processes=0):
    # TODO: move to cmdline
    if os.environ.get("INGESTIFY_RUN_EAGER") == "true":
        return list(map(func, iterable))

    if not processes:
        processes = int(os.environ.get("INGESTIFY_CONCURRENCY", "0"))

    if "fork" in get_all_start_methods():
        ctx = get_context("fork")
    else:
        ctx = get_context("spawn")

    wrapped_fn = cloudpickle.dumps(func)

    with ctx.Pool(processes or cpu_count()) as pool:
        return pool.map(
            cloud_unpack_and_call, ((wrapped_fn, item) for item in iterable)
        )


class SyncPool:
    def map(self, func, iterable):
        return [func(item) for item in iterable]

    def join(self):
        return True

    def close(self):
        return True


class DummyPool:
    def map(self, func, iterable):
        logger.info(f"DummyPool: not running {len(list(iterable))} tasks")
        return None

    def join(self):
        return True

    def close(self):
        return True


class TaskExecutor:
    def __init__(self, processes=0, dry_run: bool = False):
        if dry_run:
            pool = DummyPool()
        elif os.environ.get("INGESTIFY_RUN_EAGER") == "true":
            pool = SyncPool()
        else:
            if not processes:
                processes = int(os.environ.get("INGESTIFY_CONCURRENCY", "0"))

            if "fork" in get_all_start_methods():
                ctx = get_context("fork")
            else:
                ctx = get_context("spawn")

            pool = ctx.Pool(processes or cpu_count())
        self.pool = pool

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.join()

    def run(self, func, iterable):
        wrapped_fn = cloudpickle.dumps(func)
        start_time = time.time()
        res = self.pool.map(
            cloud_unpack_and_call, ((wrapped_fn, item) for item in iterable)
        )
        if res:
            took = time.time() - start_time
            logger.info(
                f"Finished {len(res)} tasks in {took:.1f} seconds. {(len(res)/took):.1f} tasks/sec"
            )
        return res

    def join(self):
        self.pool.close()
        self.pool.join()


def try_number(s: str):
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s

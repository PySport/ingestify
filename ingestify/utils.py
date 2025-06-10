import logging
import os
import time
import re
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from datetime import datetime, timezone
from string import Template
from typing import Dict, Tuple, Optional, Any, List

from pydantic import Field
from typing_extensions import Self


from itertools import islice

from ingestify.domain.models.timing import Timing

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


class SyncExecutor:
    def map(self, func, iterable):
        return [func(item) for item in iterable]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DummyExecutor:
    def map(self, func, iterable):
        logger.info(f"DummyPool: not running {len(list(iterable))} tasks")
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class TaskExecutor:
    def __init__(self, processes=0, dry_run: bool = False):
        if dry_run:
            executor = DummyExecutor()
        elif os.environ.get("INGESTIFY_RUN_EAGER") == "true":
            executor = SyncExecutor()
        else:
            if not processes:
                processes = get_concurrency()

            # if "fork" in get_all_start_methods():
            #     ctx = get_context("fork")
            # else:
            #     ctx = get_context("spawn")

            # pool = ctx.Pool(processes or cpu_count())

            executor = ThreadPoolExecutor(max_workers=processes)

        self.executor = executor

    def __enter__(self):
        self.executor.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.__exit__(exc_type, exc_val, exc_tb)

    def run(self, func, iterable):
        # If multiprocessing
        # wrapped_fn = cloudpickle.dumps(func)
        # res = self.executor.map(
        #     cloud_unpack_and_call, ((wrapped_fn, item) for item in iterable)
        # )
        start_time = time.time()
        res = list(self.executor.map(func, iterable))
        if res:
            took = time.time() - start_time
            logger.info(
                f"Finished {len(res)} tasks in {took:.1f} seconds. {(len(res)/took):.1f} tasks/sec"
            )
        return res


def try_number(s: str):
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s


class HasTiming:
    """Mixin to give Pydantic models ability to time actions."""

    timings: List[Timing] = Field(default_factory=list)

    @contextmanager
    def record_timing(
        self, description: str, metadata: Optional[dict] = None
    ) -> Timing:
        if not metadata:
            metadata = {}

        start = utcnow()
        try:
            result = None
            yield
        except Exception as e:
            result = {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            }
            raise e
        finally:
            metadata = dict(result=result, **metadata)
            self.timings.append(
                Timing(
                    name=description,
                    started_at=start,
                    ended_at=utcnow(),
                    metadata=metadata,
                )
            )

    def start_timing(self, name):
        start = utcnow()

        def finish():
            self.timings.append(Timing(name=name, started_at=start, ended_at=utcnow()))

        return finish


def get_concurrency():
    concurrency = int(os.environ.get("INGESTIFY_CONCURRENCY", "0"))
    if not concurrency:
        concurrency = min(32, (os.cpu_count() or 1) + 4)
    return concurrency

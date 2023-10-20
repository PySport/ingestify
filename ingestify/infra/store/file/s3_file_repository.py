import os
import shutil
from pathlib import Path
from typing import IO, AnyStr, BinaryIO

import boto3 as boto3

from ingestify.domain import Dataset
from ingestify.domain.models import FileRepository


class S3FileRepository(FileRepository):
    def __init__(self, url):
        super().__init__(url)

        self._s3 = None

    @property
    def s3(self):
        if not self._s3:
            self._s3 = boto3.resource("s3")
        return self._s3

    def __getstate__(self):
        return {"base_dir": self.base_dir, "_s3": None}

    def save_content(
        self,
        bucket: str,
        dataset: Dataset,
        version_id: int,
        filename: str,
        stream: BinaryIO,
    ):
        key = self.get_path(bucket, dataset, version_id, filename)
        s3_bucket = Path(key.parts[0])

        stream = self.prepare_write_stream(stream)

        self.s3.Object(str(s3_bucket), str(key.relative_to(s3_bucket))).put(Body=stream)
        return f"s3://{key}"

    def load_content(
        self, bucket: str, dataset: Dataset, version_id: int, filename: str
    ) -> BinaryIO:
        key = self.get_path(bucket, dataset, version_id, filename)
        s3_bucket = Path(key.parts[0])
        stream = self.s3.Object(str(s3_bucket), str(key.relative_to(s3_bucket))).get()[
            "Body"
        ]
        return self.prepare_read_stream(stream)

    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("s3://")

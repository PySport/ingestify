import json
from datetime import datetime
from email.utils import format_datetime, parsedate
from hashlib import sha1
from io import BytesIO
from typing import Optional, Callable, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from ingestify.domain.models import DraftFile, File
from ingestify.utils import utcnow

_session = None


def get_session():
    """Initialize the session when it's needed. This will make sure it's initialized
    within the correct context, and we don't get issues when the session is created
    in process #1 and used in process #2
    """
    global _session

    if not _session:
        retry_strategy = Retry(
            total=4,  # Maximum number of retries
            backoff_factor=2,  # Exponential backoff factor (e.g., 2 means 1, 2, 4, 8 seconds, ...)
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        # Create a new session object
        _session = requests.Session()
        _session.mount("http://", adapter)
        _session.mount("https://", adapter)

    return _session


def retrieve_http(
    url,
    current_file: Optional[File] = None,
    headers: Optional[dict] = None,
    pager: Optional[Tuple[str, Callable[[str, dict], Optional[str]]]] = None,
    last_modified: Optional[datetime] = None,
    **kwargs,
) -> Optional[DraftFile]:
    headers = headers or {}
    if current_file:
        if last_modified and current_file.modified_at >= last_modified:
            # Not changed
            return None
        # else:
        #     print(f"{current_file.modified_at=} {last_modified=}")
        # headers["if-modified-since"] = (
        #     format_datetime(current_file.modified_at, usegmt=True),
        # )
        headers["if-none-match"] = current_file.tag

    http_kwargs = {}
    file_attributes = {}
    for key, item in kwargs.items():
        if key.startswith("http_"):
            http_kwargs[key[5:]] = item
        elif key.startswith("file_"):
            file_attributes[key[5:]] = item
        else:
            raise Exception(f"Don't know how to use {key}")

    response = get_session().get(url, headers=headers, **http_kwargs)
    response.raise_for_status()
    if response.status_code == 304:
        # Not modified
        return None

    if last_modified:
        # From metadata received from api in discover_datasets
        modified_at = last_modified
    elif "last-modified" in response.headers:
        # Received from the webserver
        modified_at = parsedate(response.headers["last-modified"])
    else:
        modified_at = utcnow()

    tag = response.headers.get("etag")
    # content_length = int(response.headers.get("content-length", 0))

    if pager:
        """
        A pager helps with responses that return the data in pages.
        """
        data_path, pager_fn = pager
        data = []
        while True:
            current_page_data = response.json()
            data.extend(current_page_data[data_path])
            next_url = pager_fn(url, current_page_data)
            if not next_url:
                break
            else:
                response = requests.get(next_url, headers=headers, **http_kwargs)

        content = json.dumps({data_path: data}).encode("utf-8")
    else:
        content = response.content

    if not tag:
        tag = sha1(content).hexdigest()

    # if not content_length: - Don't use http header as it might be wrong
    # for example in case of compressed data
    content_length = len(content)

    if current_file and current_file.tag == tag:
        # Not changed. Don't keep it
        return None

    return DraftFile(
        created_at=utcnow(),
        modified_at=modified_at,
        tag=tag,
        size=content_length,
        content_type=response.headers.get("content-type"),
        stream=BytesIO(content),
        **file_attributes,
    )

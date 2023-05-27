import json
from email.utils import format_datetime, parsedate
from hashlib import sha1
from io import BytesIO
from typing import Optional, Callable, Tuple

import requests

from ingestify.domain.models import DraftFile, File
from ingestify.utils import utcnow


def retrieve_http(
    url,
    current_file: Optional[File] = None,
    headers: Optional[dict] = None,
    pager: Optional[Tuple[str, Callable[[str, dict], Optional[str]]]] = None,
    **kwargs
) -> Optional[DraftFile]:
    headers = headers or {}
    if current_file:
        headers["if-modified-since"] = (
            format_datetime(current_file.modified_at, usegmt=True),
        )
        headers["if-none-match"] = (current_file.tag,)

    response = requests.get(url, headers=headers, **kwargs)
    if response.status_code == 304:
        # Not modified
        return None

    if "last-modified" in response.headers:
        modified_at = parsedate(response.headers["last-modified"])
    else:
        modified_at = utcnow()

    tag = response.headers.get("etag")
    content_length = int(response.headers.get("content-length", 0))

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
                response = requests.get(next_url, headers=headers, **kwargs)

        content = json.dumps({data_path: data}).encode("utf-8")
    else:
        content = response.content

    if not tag:
        tag = sha1(content).hexdigest()
    if not content_length:
        content_length = len(content)

    return DraftFile(
        modified_at=modified_at,
        tag=tag,
        size=content_length,
        content_type=response.headers.get("content-type"),
        stream=BytesIO(content),
    )

from email.utils import format_datetime, parsedate
from io import BytesIO
from typing import Optional

import requests
from domain.models import DraftFile, File
from utils import utcnow


def retrieve_http(url, current_file: Optional[File] = None) -> Optional[DraftFile]:
    headers = {}
    if current_file:
        headers = {
            "if-modified-since": format_datetime(current_file.modified_at, usegmt=True),
            "if-none-match": current_file.tag,
        }
    response = requests.get(url, headers=headers)
    if response.status_code == 304:
        # Not modified
        return None

    if "last-modified" in response.headers:
        modified_at = parsedate(response.headers["last-modified"])
    else:
        modified_at = utcnow()

    tag = response.headers.get("etag")
    content_length = response.headers.get("content-length", 0)

    return DraftFile(
        modified_at=modified_at,
        tag=tag,
        size=int(content_length) if content_length else None,
        content_type=response.headers.get("content-type"),
        stream=BytesIO(response.content),
    )

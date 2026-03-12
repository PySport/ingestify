import gzip
from unittest.mock import MagicMock, patch

import pytest

from ingestify.infra.fetch.http import retrieve_http
from ingestify.utils import BufferedStream


def make_mock_response(content, status_code=200, headers=None):
    headers = headers or {}
    mock = MagicMock()
    mock.status_code = status_code
    mock.headers = MagicMock()
    mock.headers.get = lambda key, default=None: headers.get(key, default)
    mock.headers.__contains__ = lambda self, key: key in headers
    mock.raise_for_status = MagicMock()
    mock.iter_content = lambda chunk_size=1: [content]
    return mock


FILE_KWARGS = dict(
    file_data_feed_key="test",
    file_data_spec_version="v1",
    file_data_serialization_format="json",
)

PLAIN_JSON = b'{"key": "value"}' * 100


def test_plain_content_size_and_stream():
    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(PLAIN_JSON)
        result = retrieve_http("https://example.com/data.json", **FILE_KWARGS)

    assert isinstance(result.stream, BufferedStream)
    assert result.size == len(PLAIN_JSON)
    assert result.stream.read() == PLAIN_JSON


def test_gzip_content_stored_as_is_with_uncompressed_size():
    compressed = gzip.compress(PLAIN_JSON)

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(compressed)
        result = retrieve_http("https://example.com/data.json.gz", **FILE_KWARGS)

    assert isinstance(result.stream, BufferedStream)
    assert result.size == len(PLAIN_JSON)   # uncompressed size from gzip trailer
    assert result.stream.read() == compressed  # stored as-is

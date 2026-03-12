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


def test_decompress_false_stores_gzip_as_is():
    compressed = gzip.compress(PLAIN_JSON)

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(compressed)
        result = retrieve_http("https://example.com/data.json.gz", **FILE_KWARGS)

    assert isinstance(result.stream, BufferedStream)
    assert result.size == len(compressed)
    assert result.stream.read() == compressed


def test_decompress_true_decompresses_and_sets_correct_size():
    compressed = gzip.compress(PLAIN_JSON)

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(compressed)
        result = retrieve_http(
            "https://s3.amazonaws.com/bucket/data.json.gz?X-Amz-Signature=abc123",
            http_decompress=True,
            **FILE_KWARGS,
        )

    assert isinstance(result.stream, BufferedStream)
    assert result.size == len(PLAIN_JSON)
    assert result.stream.read() == PLAIN_JSON


def test_decompress_true_corrupt_gzip_raises():
    corrupt = b"\x1f\x8b" + b"\x00" * 20

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(corrupt)
        with pytest.raises(Exception):
            retrieve_http(
                "https://example.com/data.json.gz",
                http_decompress=True,
                **FILE_KWARGS,
            )

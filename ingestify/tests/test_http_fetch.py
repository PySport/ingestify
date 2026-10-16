import gzip
from datetime import datetime, timezone
from hashlib import sha1
from unittest.mock import MagicMock, patch

import pytest

from ingestify.domain.models.dataset.file import NotModifiedFile
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
    assert result.content_compression_method == "gzip"
    assert result.size == len(PLAIN_JSON)  # uncompressed size from gzip trailer
    assert result.stream.read() == compressed  # stored as-is


def test_plain_content_has_no_compression_method():
    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(PLAIN_JSON)
        result = retrieve_http("https://example.com/data.json", **FILE_KWARGS)

    assert result.content_compression_method is None


def make_pager_response(page_data):
    mock = MagicMock()
    mock.status_code = 200
    mock.headers = MagicMock()
    mock.headers.get = lambda key, default=None: default
    mock.headers.__contains__ = lambda self, key: False
    mock.raise_for_status = MagicMock()
    mock.json = lambda: page_data
    return mock


def test_pager_returns_draft_file_without_compression_method():
    """Regression test: pager branch used to leave content_compression_method
    unbound, raising UnboundLocalError when building the DraftFile. Pager output
    is freshly JSON-encoded (never compressed), so None is the correct value."""
    page = {"items": [{"id": 1}, {"id": 2}]}

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_pager_response(page)

        result = retrieve_http(
            "https://example.com/data.json",
            pager=("items", lambda url, data: None),
            **FILE_KWARGS,
        )

    assert result.content_compression_method is None
    assert result.stream.read() == b'{"items": [{"id": 1}, {"id": 2}]}'
    assert result.size == len(b'{"items": [{"id": 1}, {"id": 2}]}')


def make_current_file(content, modified_at, tag=None):
    """A stored File as retrieve_http sees it: only modified_at and tag are read."""
    current = MagicMock()
    current.modified_at = modified_at
    current.tag = tag if tag is not None else sha1(content).hexdigest()
    return current


STORED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_unchanged_last_modified_short_circuits_without_a_request():
    """Default: a file whose reported last_modified did not move is not even
    requested. Cheap, and wrong only when the provider's timestamp lies."""
    current_file = make_current_file(PLAIN_JSON, STORED_AT)

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        result = retrieve_http(
            "https://example.com/data.json",
            current_file=current_file,
            last_modified=STORED_AT,
            **FILE_KWARGS,
        )

    assert isinstance(result, NotModifiedFile)
    mock_session.assert_not_called()


def test_recheck_detects_content_that_changed_behind_a_stale_timestamp():
    """With trust_last_modified=False the file is fetched despite an unmoved
    timestamp, and a body that really differs comes back as a DraftFile."""
    current_file = make_current_file(PLAIN_JSON, STORED_AT)
    changed = PLAIN_JSON + b"extra"

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(changed)
        result = retrieve_http(
            "https://example.com/data.json",
            current_file=current_file,
            last_modified=STORED_AT,
            trust_last_modified=False,
            **FILE_KWARGS,
        )

    assert not isinstance(result, NotModifiedFile)
    assert result.stream.read() == changed


def test_recheck_keeps_a_genuinely_unchanged_file_out_of_the_revision():
    """The point of comparing hashes rather than overwriting: a file that is
    byte-identical costs a request but no new revision."""
    current_file = make_current_file(PLAIN_JSON, STORED_AT)

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(PLAIN_JSON)
        result = retrieve_http(
            "https://example.com/data.json",
            current_file=current_file,
            last_modified=STORED_AT,
            trust_last_modified=False,
            **FILE_KWARGS,
        )

    assert isinstance(result, NotModifiedFile)
    assert result.reason == "tag matched current_file"


def test_recheck_drops_if_modified_since_but_keeps_if_none_match():
    """Only the time-based validator is dropped. if-none-match is a content
    validator, so a server with a real ETag can still save us the download."""
    current_file = make_current_file(PLAIN_JSON, STORED_AT, tag="server-etag")

    with patch("ingestify.infra.fetch.http.get_session") as mock_session:
        mock_session.return_value.get.return_value = make_mock_response(PLAIN_JSON)
        retrieve_http(
            "https://example.com/data.json",
            current_file=current_file,
            last_modified=STORED_AT,
            trust_last_modified=False,
            **FILE_KWARGS,
        )
        headers = mock_session.return_value.get.call_args.kwargs["headers"]

    assert "if-modified-since" not in headers
    assert headers["if-none-match"] == "server-etag"

"""Tests for IdentifierTransformer.to_path."""
from ingestify.domain.services.identifier_key_transformer import IdentifierTransformer


def test_to_path_short_value_unchanged():
    t = IdentifierTransformer()
    path = t.to_path("p", "d", {"key": "short"})
    assert path == "key=short"


def test_to_path_special_chars_url_encoded():
    t = IdentifierTransformer()
    path = t.to_path("p", "d", {"key": "$99 mattress"})
    assert path == "key=%2499%20mattress"


def test_to_path_long_value_truncated_with_hash():
    t = IdentifierTransformer()
    long_value = "a" * 50
    path = t.to_path("p", "d", {"key": long_value})
    # Truncated at 40 chars + _ + 8-char hash
    assert path.startswith("key=" + "a" * 40 + "_")
    assert len(path.split("=")[1]) == 40 + 1 + 8  # value_hash


def test_to_path_long_value_hash_is_stable():
    t = IdentifierTransformer()
    long_value = "keyword " * 10
    path1 = t.to_path("p", "d", {"key": long_value})
    path2 = t.to_path("p", "d", {"key": long_value})
    assert path1 == path2


def test_to_path_integer_value_unchanged():
    t = IdentifierTransformer()
    path = t.to_path("p", "d", {"id": 12345})
    assert path == "id=12345"

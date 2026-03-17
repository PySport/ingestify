import gzip
from io import BytesIO

from ingestify.utils import BufferedStream, detect_compression, gzip_uncompressed_size

PLAIN = b'{"key": "value"}' * 100


def to_stream(data: bytes) -> BufferedStream:
    return BufferedStream.from_stream(BytesIO(data))


def test_detect_compression_gzip():
    assert detect_compression(to_stream(gzip.compress(PLAIN))) == "gzip"


def test_detect_compression_plain():
    assert detect_compression(to_stream(PLAIN)) is None


def test_detect_compression_resets_position():
    stream = to_stream(gzip.compress(PLAIN))
    detect_compression(stream)
    assert stream.tell() == 0


def test_gzip_uncompressed_size():
    compressed = gzip.compress(PLAIN)
    assert gzip_uncompressed_size(to_stream(compressed)) == len(PLAIN)

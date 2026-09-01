import struct
from spectre.framing import encode_frame


def test_encode_frame_uses_little_endian_u32():
    payload = b"abc"
    frame = encode_frame(payload)
    assert frame[:4] == struct.pack("<I", 3)
    assert frame[4:] == payload

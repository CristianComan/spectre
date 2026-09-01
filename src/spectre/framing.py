from __future__ import annotations

import asyncio
import struct

_PREFIX = struct.Struct("<I")


def encode_frame(payload: bytes) -> bytes:
    return _PREFIX.pack(len(payload)) + payload


async def read_frame(reader: asyncio.StreamReader) -> bytes:
    prefix = await reader.readexactly(_PREFIX.size)
    (length,) = _PREFIX.unpack(prefix)
    if length == 0:
        raise ValueError("Zero-length SAPIENT frame is not valid for this v0.1 client")
    return await reader.readexactly(length)

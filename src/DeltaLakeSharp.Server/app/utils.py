"""Shared utility functions for Delta Table Service backends."""

from __future__ import annotations

import json
from typing import Any


def parse_json(data: bytes) -> dict[str, Any]:
    """Decode bytes to a JSON dict.

    Args:
        data: UTF-8 encoded JSON bytes.

    Returns:
        Parsed JSON as a Python dictionary.
    """
    return json.loads(data.decode("utf-8"))


def to_bytes(obj: Any) -> bytes:
    """Encode a Python object as JSON bytes.

    Args:
        obj: A JSON-serializable Python object.

    Returns:
        UTF-8 encoded JSON bytes.
    """
    return json.dumps(obj).encode("utf-8")

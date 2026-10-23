"""Lossless JSON numbers and atomic files on a shared, persistent filesystem."""

import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

from datanomik_client import decode_json


def json_text(value):
    """Encode Decimal as JSON numbers without a binary floating-point conversion."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite decimal")
        return str(value)
    if isinstance(value, float):
        raise ValueError("Float input is forbidden; decode JSON using Decimal")
    if isinstance(value, dict):
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + json_text(v)
                              for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(json_text(v) for v in value) + "]"
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


@contextmanager
def atomic_path(destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".tmp-", dir=destination.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        yield temporary
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path, value):
    with atomic_path(path) as temporary:
        temporary.write_text(json_text(value) + "\n", encoding="utf-8")


def read_json(path):
    return decode_json(Path(path).read_text(encoding="utf-8"))


def write_jsonl(path, records):
    count = 0
    digest = hashlib.sha256()
    with atomic_path(path) as temporary:
        with temporary.open("wb") as output:
            for record in records:
                line = (json_text(record) + "\n").encode("utf-8")
                output.write(line)
                digest.update(line)
                count += 1
    return {"count": count, "sha256": digest.hexdigest(), "path": str(path)}


def read_jsonl(path):
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            yield decode_json(line)


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

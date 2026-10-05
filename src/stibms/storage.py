"""One code path for a local directory and ``gs://bucket/prefix`` (fsspec / gcsfs).

Paths given to :class:`Store` methods are relative to its root, with ``/`` separators.
On GCS an object write is atomic; locally a write goes through a temporary file and a rename, so
a reader never sees a truncated file. ``_SUCCESS`` markers are always written last.
"""
from __future__ import annotations

import io
import json
import os
import threading
import uuid

import fsspec
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


class Store:
    def __init__(self, root: str):
        self.root = root.rstrip("/")
        self.fs, self._base = fsspec.core.url_to_fs(self.root)
        self.local = "file" in (self.fs.protocol if isinstance(self.fs.protocol, tuple) else (self.fs.protocol,))
        if self.local:
            os.makedirs(self._base, exist_ok=True)
        self._lock = threading.Lock()
        self.bytes_written = 0

    def __repr__(self) -> str:
        return f"Store({self.root!r})"

    def path(self, rel: str) -> str:
        return f"{self._base}/{rel.lstrip('/')}"

    def url(self, rel: str) -> str:
        return f"{self.root}/{rel.lstrip('/')}"

    def exists(self, rel: str) -> bool:
        return self.fs.exists(self.path(rel))

    def read_bytes(self, rel: str) -> bytes:
        return self.fs.cat_file(self.path(rel))

    def write_bytes(self, rel: str, data: bytes) -> None:
        p = self.path(rel)
        if self.local:
            os.makedirs(os.path.dirname(p), exist_ok=True)
            tmp = f"{p}.{uuid.uuid4().hex}.part"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, p)
        else:
            self.fs.pipe_file(p, data)
        with self._lock:
            self.bytes_written += len(data)

    def read_json(self, rel: str):
        return json.loads(self.read_bytes(rel))

    def write_json(self, rel: str, obj) -> None:
        self.write_bytes(rel, json.dumps(obj, indent=1, sort_keys=True, default=str).encode())

    def write_table(self, rel: str, table: pa.Table, row_group_size: int | None = None,
                    row_group_bounds: list[int] | None = None) -> int:
        """Write an Arrow table as zstd Parquet. ``row_group_bounds``: explicit row offsets where a
        new row group starts (e.g. at line boundaries). Returns the file size."""
        buf = io.BytesIO()
        with pq.ParquetWriter(buf, table.schema, compression="zstd", compression_level=6,
                              write_statistics=True) as w:
            if row_group_bounds:
                cuts = [*row_group_bounds, table.num_rows]
                for a, b in zip(cuts[:-1], cuts[1:]):
                    if b > a:
                        w.write_table(table.slice(a, b - a))
            elif table.num_rows == 0:
                w.write_table(table)
            else:
                w.write_table(table, row_group_size=row_group_size or 1 << 20)
        data = buf.getvalue()
        self.write_bytes(rel, data)
        return len(data)

    def write_parquet(self, rel: str, df: pl.DataFrame, **kw) -> int:
        return self.write_table(rel, df.to_arrow(), **kw)

    def read_parquet(self, rel: str, **kw) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(self.read_bytes(rel)), **kw)

    def glob(self, pattern: str) -> list[str]:
        """Relative paths matching ``pattern`` (relative to the root)."""
        base = self._base.rstrip("/") + "/"
        out = []
        for p in self.fs.glob(self.path(pattern)):
            p = str(p)
            out.append(p[len(base):] if p.startswith(base) else p.split(base.lstrip("/"), 1)[-1])
        return sorted(out)

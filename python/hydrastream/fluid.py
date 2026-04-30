from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from . import _pulse


@dataclass(slots=True)
class _Plan:
    filters: list[tuple[str, str, Any]] = field(default_factory=list)
    select_fields: list[str] | None = None
    window_seconds: int | None = None
    window_by: str | None = None
    time_field: str = "timestamp"
    mean_field: str | None = None
    mean_alias: str | None = None
    watermark_ms: int | None = None
    allowed_lateness_ms: int | None = None
    event_id_field: str | None = None
    drop_late: bool = True

    def to_native(self) -> dict[str, Any]:
        return {
            "filters": [
                {"field": field, "op": op, "value": value}
                for field, op, value in self.filters
            ],
            "select": self.select_fields,
            "window": None
            if self.window_seconds is None
            else {
                "seconds": self.window_seconds,
                "by": self.window_by,
                "time_field": self.time_field,
            },
            "mean": None
            if self.mean_field is None
            else {
                "field": self.mean_field,
                "alias": self.mean_alias or f"{self.mean_field}_mean",
            },
            "watermark_ms": self.watermark_ms,
            "allowed_lateness_ms": self.allowed_lateness_ms,
            "event_id_field": self.event_id_field,
            "drop_late": self.drop_late,
        }


class StreamBuilder:
    def __init__(self, native: _pulse.NativeStream) -> None:
        self._native = native
        self._plan = _Plan()

    @property
    def address(self) -> tuple[str, int]:
        return self._native.address()

    def filter(self, field: str, op: str, value: Any) -> "StreamBuilder":
        self._plan.filters.append((field, op, value))
        return self

    def select(self, *fields: str) -> "StreamBuilder":
        self._plan.select_fields = list(fields)
        return self

    def window(self, seconds: int, by: str | None = None, time_field: str = "timestamp") -> "StreamBuilder":
        if seconds <= 0:
            raise ValueError("seconds must be positive")
        self._plan.window_seconds = seconds
        self._plan.window_by = by
        self._plan.time_field = time_field
        return self

    def mean(self, field: str, alias: str | None = None) -> "StreamBuilder":
        self._plan.mean_field = field
        self._plan.mean_alias = alias
        return self

    def exactly_once(self, event_id_field: str = "event_id") -> "StreamBuilder":
        self._plan.event_id_field = event_id_field
        return self

    def watermark(self, *, ms: int, allowed_lateness_ms: int = 0, drop_late: bool = True) -> "StreamBuilder":
        if ms < 0 or allowed_lateness_ms < 0:
            raise ValueError("watermark and allowed lateness must be non-negative")
        self._plan.watermark_ms = ms
        self._plan.allowed_lateness_ms = allowed_lateness_ms
        self._plan.drop_late = drop_late
        return self

    def emit(self, event: dict[str, Any]) -> None:
        self._native.emit_json(event)

    def emit_batch(self, columns: dict[str, list[Any]]) -> None:
        self._native.emit_columns(columns)

    def stats(self) -> dict[str, int]:
        return self._native.stats()

    def save_checkpoint(self, path: str) -> None:
        self._native.save_checkpoint(path)

    def load_checkpoint(self, path: str) -> None:
        self._native.load_checkpoint(path)

    def next_batch(self, timeout_ms: int = 1000):
        columns = self._native.next_batch(self._plan.to_native(), timeout_ms)
        if columns is None:
            return None
        return pa.record_batch(columns)

    def collect_columns(self, columns: dict[str, list[Any]]) -> dict[str, list[Any]]:
        return self._native.aggregate_columns(columns, self._plan.to_native())

    def collect_trade_numeric(self, timestamps: Any, is_trade: Any, symbol_id: Any, values: Any, *, window_ms: int = 1000):
        return self._native.aggregate_trade_numeric(timestamps, is_trade, symbol_id, values, window_ms)

    async def next_batch_async(self, timeout_ms: int = 1000):
        return await asyncio.to_thread(self.next_batch, timeout_ms)

    def __iter__(self):
        while True:
            batch = self.next_batch(timeout_ms=50)
            if batch is None:
                break
            yield batch

    def __aiter__(self) -> "StreamBuilder":
        return self

    async def __anext__(self):
        while True:
            batch = await self.next_batch_async(timeout_ms=250)
            if batch is not None:
                return batch
            if self._native.is_closed():
                raise StopAsyncIteration

    def close(self) -> None:
        self._native.close()


def stream_from_udp(
    host: str = "127.0.0.1",
    port: int = 0,
    *,
    batch_size: int = 1024,
    linger_ms: int = 20,
    capacity: int = 65536,
) -> StreamBuilder:
    native = _pulse.NativeStream.udp(host, port, batch_size, linger_ms, capacity)
    return StreamBuilder(native)


def stream_manual(*, batch_size: int = 1024, linger_ms: int = 20, capacity: int = 65536) -> StreamBuilder:
    native = _pulse.NativeStream.manual(batch_size, linger_ms, capacity)
    return StreamBuilder(native)

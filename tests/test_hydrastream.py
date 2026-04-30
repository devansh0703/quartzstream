import json
import socket
import time
from pathlib import Path

import pyarrow as pa
import pytest

from hydrastream import stream_from_udp, stream_manual


def drain_until(stream, *, attempts=10, timeout_ms=200):
    for _ in range(attempts):
        batch = stream.next_batch(timeout_ms=timeout_ms)
        if batch is not None:
            return batch
    return None


def test_manual_pipeline_schema_evolution_and_filter():
    stream = stream_manual(batch_size=8, linger_ms=5, capacity=64)
    stream.emit({"timestamp": 1000, "symbol": "AAPL", "value": 10.0, "flag": True})
    stream.emit({"timestamp": 1001, "symbol": "AAPL", "value": 11.5, "note": "x"})
    stream.emit({"timestamp": 1002, "symbol": "MSFT", "value": 8.0})

    batch = stream.filter("symbol", "==", "AAPL").next_batch(timeout_ms=500)
    assert isinstance(batch, pa.RecordBatch)
    assert batch.num_rows == 2
    assert batch.column(batch.schema.get_field_index("symbol")).to_pylist() == ["AAPL", "AAPL"]
    assert "flag" in batch.schema.names
    assert "note" in batch.schema.names
    stream.close()


def test_exactly_once_deduplicates_events_and_tracks_stats():
    stream = stream_manual(batch_size=16, linger_ms=5, capacity=64).exactly_once("event_id")
    stream.emit({"timestamp": 1000, "event_id": "a", "value": 1})
    stream.emit({"timestamp": 1001, "event_id": "a", "value": 9})
    stream.emit({"timestamp": 1002, "event_id": "b", "value": 2})

    batch = stream.select("event_id", "value").next_batch(timeout_ms=500)
    assert batch.to_pylist() == [{"event_id": "a", "value": 1}, {"event_id": "b", "value": 2}]
    stats = stream.stats()
    assert stats["duplicate_events"] == 1
    assert stats["seen_event_ids"] == 2
    stream.close()


def test_watermark_finalizes_windows_and_drops_late_events():
    stream = (
        stream_manual(batch_size=16, linger_ms=5, capacity=64)
        .exactly_once("event_id")
        .window(seconds=1, by="symbol")
        .mean("value", alias="avg_value")
        .watermark(ms=100, allowed_lateness_ms=0, drop_late=True)
        .select("window_start", "window_end", "symbol", "avg_value", "count")
    )

    stream.emit({"timestamp": 1000, "event_id": "1", "symbol": "AAPL", "value": 10.0})
    stream.emit({"timestamp": 2500, "event_id": "2", "symbol": "AAPL", "value": 30.0})
    batch = drain_until(stream)
    assert batch is not None
    rows = batch.to_pylist()
    assert rows == [
        {"window_start": 1000, "window_end": 2000, "symbol": "AAPL", "avg_value": 10.0, "count": 1}
    ]

    stream.emit({"timestamp": 1500, "event_id": "3", "symbol": "AAPL", "value": 20.0})
    assert stream.next_batch(timeout_ms=100) is None
    stats = stream.stats()
    assert stats["late_events"] == 1
    stream.close()


def test_close_flushes_open_windows():
    stream = (
        stream_manual(batch_size=16, linger_ms=5, capacity=64)
        .window(seconds=1, by="symbol")
        .mean("value", alias="avg_value")
        .select("window_start", "window_end", "symbol", "avg_value", "count")
    )
    stream.emit({"timestamp": 1000, "symbol": "AAPL", "value": 10.0})
    assert stream.next_batch(timeout_ms=100) is None
    stream.close()
    batch = drain_until(stream, attempts=2, timeout_ms=50)
    assert batch is not None
    assert batch.to_pylist() == [
        {"window_start": 1000, "window_end": 2000, "symbol": "AAPL", "avg_value": 10.0, "count": 1}
    ]


def test_checkpoint_restore_recovers_window_state(tmp_path: Path):
    checkpoint = tmp_path / "hydrastream-checkpoint.json"
    stream = (
        stream_manual(batch_size=16, linger_ms=5, capacity=64)
        .window(seconds=1, by="symbol")
        .mean("value", alias="avg_value")
        .watermark(ms=100)
        .select("window_start", "window_end", "symbol", "avg_value", "count")
    )
    stream.emit({"timestamp": 1000, "symbol": "AAPL", "value": 12.0})
    assert stream.next_batch(timeout_ms=100) is None
    stream.save_checkpoint(str(checkpoint))
    assert checkpoint.exists()
    stream.close()

    restored = (
        stream_manual(batch_size=16, linger_ms=5, capacity=64)
        .window(seconds=1, by="symbol")
        .mean("value", alias="avg_value")
        .watermark(ms=100)
        .select("window_start", "window_end", "symbol", "avg_value", "count")
    )
    restored.load_checkpoint(str(checkpoint))
    restored.emit({"timestamp": 2500, "symbol": "AAPL", "value": 20.0})
    batch = drain_until(restored)
    assert batch is not None
    assert batch.to_pylist() == [
        {"window_start": 1000, "window_end": 2000, "symbol": "AAPL", "avg_value": 12.0, "count": 1}
    ]
    stats = restored.stats()
    assert stats["checkpoint_loads"] == 1
    restored.close()


def test_projection_of_missing_fields_returns_nulls():
    stream = stream_manual(batch_size=8, linger_ms=5, capacity=32).select("symbol", "missing")
    stream.emit({"timestamp": 1000, "symbol": "AAPL"})
    batch = stream.next_batch(timeout_ms=500)
    assert batch.to_pylist() == [{"symbol": "AAPL", "missing": None}]
    stream.close()


def test_backpressure_stats_increment_on_small_capacity():
    stream = stream_manual(batch_size=512, linger_ms=100, capacity=1)
    for idx in range(2000):
        stream.emit({"timestamp": idx, "event_id": str(idx), "value": idx})
    time.sleep(0.05)
    stats = stream.stats()
    assert stats["ingested"] == 2000
    assert stats["dropped"] > 0
    stream.close()


def test_invalid_watermark_arguments_raise():
    stream = stream_manual()
    with pytest.raises(ValueError):
        stream.watermark(ms=-1)
    with pytest.raises(ValueError):
        stream.watermark(ms=1, allowed_lateness_ms=-2)
    stream.close()


def test_emit_batch_with_mismatched_column_lengths_raises():
    stream = stream_manual()
    with pytest.raises(ValueError):
        stream.emit_batch({"timestamp": [1, 2], "value": [1.0]})
    stream.close()


def test_numeric_fast_path_validates_shapes_and_window():
    stream = stream_manual()
    with pytest.raises(ValueError):
        stream.collect_trade_numeric([1, 2], [1], [0, 1], [1.0, 2.0], window_ms=1000)
    with pytest.raises(ValueError):
        stream.collect_trade_numeric([1, 2], [1, 1], [0, 1], [1.0, 2.0], window_ms=0)
    stream.close()


def test_watermark_with_drop_late_false_keeps_late_rows():
    stream = (
        stream_manual(batch_size=16, linger_ms=5, capacity=64)
        .window(seconds=1, by="symbol")
        .mean("value", alias="avg_value")
        .watermark(ms=100, allowed_lateness_ms=0, drop_late=False)
        .select("window_start", "window_end", "symbol", "avg_value", "count")
    )
    stream.emit({"timestamp": 1000, "symbol": "AAPL", "value": 10.0})
    stream.emit({"timestamp": 2500, "symbol": "AAPL", "value": 30.0})
    first = drain_until(stream)
    assert first is not None
    stream.emit({"timestamp": 1500, "symbol": "AAPL", "value": 20.0})
    stream.close()
    remaining = []
    while True:
        batch = stream.next_batch(timeout_ms=50)
        if batch is None:
            break
        remaining.extend(batch.to_pylist())
    assert any(row["window_start"] == 1000 and row["count"] == 1 for row in remaining)
    assert stream.stats()["late_events"] == 1


@pytest.mark.asyncio
async def test_udp_window_mean_async():
    stream = (
        stream_from_udp(batch_size=16, linger_ms=10, capacity=128)
        .filter("metric", "==", "price")
        .window(seconds=1, by="symbol")
        .mean("value", alias="avg_price")
        .watermark(ms=100)
        .select("window_start", "window_end", "symbol", "avg_price", "count")
    )
    host, port = stream.address
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        base = int(time.time() * 1000)
        events = [
            {"timestamp": base, "symbol": "AAPL", "metric": "price", "value": 100.0},
            {"timestamp": base + 100, "symbol": "AAPL", "metric": "price", "value": 104.0},
            {"timestamp": base + 1500, "symbol": "MSFT", "metric": "price", "value": 50.0},
            {"timestamp": base + 1600, "symbol": "AAPL", "metric": "ignored", "value": 999.0},
        ]
        for event in events:
            sock.sendto(json.dumps(event).encode("utf-8"), (host, port))

        batch = None
        for _ in range(10):
            batch = await stream.next_batch_async(timeout_ms=500)
            if batch is not None:
                break
        assert isinstance(batch, pa.RecordBatch)
        rows = batch.to_pylist()
        values = {row["symbol"]: row["avg_price"] for row in rows}
        counts = {row["symbol"]: row["count"] for row in rows}
        assert values["AAPL"] == 102.0
        assert counts["AAPL"] == 2
    finally:
        sock.close()
        stream.close()

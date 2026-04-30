from __future__ import annotations

import argparse
import time

from quartzstream import stream_manual


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic QuartzStream manual-ingest benchmark.")
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--batch-size", type=int, default=8192)
    args = parser.parse_args()

    stream = (
        stream_manual(batch_size=args.batch_size, linger_ms=1, capacity=args.events + 1024)
        .filter("kind", "==", "trade")
        .select("timestamp", "symbol", "value")
    )

    start_emit = time.perf_counter()
    for index in range(args.events):
        stream.emit(
            {
                "timestamp": index,
                "kind": "trade",
                "symbol": "AAPL" if index % 2 == 0 else "MSFT",
                "value": float(index),
            }
        )
    emit_done = time.perf_counter()

    rows = 0
    batches = 0
    while rows < args.events:
        batch = stream.next_batch(timeout_ms=1000)
        if batch is None:
            continue
        rows += batch.num_rows
        batches += 1

    end = time.perf_counter()
    stats = stream.stats()
    stream.close()

    emit_seconds = emit_done - start_emit
    end_to_end_seconds = end - start_emit
    print(f"events={args.events}")
    print(f"rows_out={rows}")
    print(f"batches={batches}")
    print(f"dropped={stats['dropped']}")
    print(f"emit_seconds={emit_seconds:.6f}")
    print(f"end_to_end_seconds={end_to_end_seconds:.6f}")
    print(f"emit_events_per_sec={args.events / emit_seconds:.0f}")
    print(f"end_to_end_events_per_sec={args.events / end_to_end_seconds:.0f}")


if __name__ == "__main__":
    main()

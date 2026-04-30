# HydraStream Implementation Explanation

## What this engine is

HydraStream is a **stream-processing engine**, not a clone of pandas internals.  
It offers pandas-like ergonomics in Python while running supported operations in native C++ for low-latency workloads.

## Why this architecture

The design separates concerns:

- Python API focuses on developer ergonomics and plan building.
- C++ handles ingestion, state, and operator execution.
- pybind11 handles interop with minimal orchestration logic.

This avoids Python-object-heavy hot paths and keeps the CPU-intensive work in native code.

## Execution semantics

### Filtering and projection

- Filters are evaluated per row with typed scalar comparison support for numeric, string, and boolean cells.
- Projection preserves selected field order and emits `None` for missing values.

### Windowing and mean aggregation

- Tumbling windows use event-time bucketing (`window(seconds=...)`).
- Group-by key is optional (`by`); when missing, aggregation is global per window.
- Mean is computed as `sum / count` over numeric rows only.
- Finalization occurs based on watermark and lateness policy.

### Watermark and late data

- Active watermark is `max_event_time - watermark_ms`.
- Rows older than watermark minus allowed lateness increment `late_events`.
- If `drop_late=True`, those rows are discarded.

### Exactly-once dedupe

- If configured with `event_id_field`, event IDs are inserted into a native set.
- Duplicate IDs are dropped and counted in stats.

### Checkpointing

- Checkpoint persists processor state to disk:
  - max event time
  - window origin
  - dedupe set
  - open window aggregates
- Restore reloads state into the processor, enabling continuation after restart.

## Native ingestion model

### Manual source

- Python calls `emit`/`emit_batch` directly into native queue.

### UDP source

- Native non-blocking UDP socket receives JSON events.
- Flat JSON parser converts packets into internal rows.

### Queueing and batching

- Ingress queue is bounded (`capacity`); overflow increments `dropped`.
- Worker thread periodically drains queue into microbatches (`linger_ms`, `batch_size`).
- Consumer retrieves pending chunks with timeout.

## Performance path split

HydraStream supports two execution paths:

1. **General row path** for flexible row dictionaries and plan semantics.
2. **Numeric fast path** (`aggregate_trade_numeric`) for dense numeric arrays.

The fast path:

- validates 1-D equal-length arrays
- releases GIL during the aggregation loop
- aggregates by `(window, symbol)` key
- returns sorted output columns for deterministic comparison

## Modularity updates in this pass

The C++ engine is now modular:

- Shared contracts in `core.hpp`
- Processing logic in `core_processing.cpp`
- Runtime engine in `native_stream.cpp`
- Bindings only in `pulse_module.cpp`

This improves maintainability, compile-time isolation, and extension safety.

## Benchmarks against strong baselines

Benchmark script now compares equivalent workloads against:

- pandas
- polars
- duckdb

For each engine:

- computes grouped windowed mean/count
- materializes a keyed result map
- checks logical equivalence with HydraStream output
- prints build/process/end-to-end throughput metrics and speedups

## About “full pandas parity”

Pandas covers an extremely broad surface area (index model, IO ecosystem, nullable dtypes, joins/reshape/time series/statistics/plotting/extensions).  
That cannot be credibly delivered in one pass. The current implementation provides a robust, tested streaming core and a concrete architecture for incremental feature expansion while preserving latency goals.

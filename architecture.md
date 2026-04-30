# QuartzStream Architecture

## Scope of the current engine

This repository implements a low-latency streaming dataframe core with:

- UDP and in-process/manual event ingestion
- Bounded ingress queue with backpressure/drop accounting
- Typed row processing with filter and projection support
- Event-time tumbling windows + mean aggregation
- Watermarking and allowed lateness controls
- Exactly-once style dedupe (event-id set)
- Checkpoint save/load of processor state
- Arrow-compatible output through `pyarrow.RecordBatch`
- Python sync + async iteration APIs
- Numeric fast-path aggregation for dense array inputs

It is **not** full pandas feature parity. The implementation is a specialized streaming engine with a pandas-like user API.

## Layered design

### 1. Python API layer (`python/quartzstream/fluid.py`)

Responsibilities:

- User-facing fluent API (`filter`, `select`, `window`, `mean`, `watermark`, `exactly_once`)
- Plan construction and serialization to native format
- Convenience methods for emit/collect/iteration

Key contract:

- API emits a plan dictionary consumed by C++ without Python-side execution of stream operators.

### 2. Native engine core (`src/*.cpp`)

Responsibilities:

- Event ingestion and buffering
- Plan parsing and row processing
- Stateful window aggregation and dedupe
- Numeric fast-path kernel for array-oriented workloads
- Native threading outside the Python GIL for ingest/worker execution

Current modular C++ layout:

- `src/core.hpp`  
  Shared types, plan definitions, function/class declarations.
- `src/core_processing.cpp`  
  Plan parsing, scalar coercion, filtering/projection, row/column transforms, JSON ingestion parser, window processing logic.
- `src/native_stream.cpp`  
  Stream lifecycle, worker loop, UDP socket handling, queue draining, checkpoint I/O, stats, and numeric aggregation kernel.
- `src/pulse_module.cpp`  
  pybind11 module bindings only.

### 3. Bridge layer (pybind11)

Responsibilities:

- Expose `NativeStream` constructors and methods to Python
- Marshall NumPy arrays and dictionaries in/out
- Maintain predictable exception behavior through Python exceptions

## Runtime data flow

1. Python builds a plan.
2. Events are emitted through manual or UDP source.
3. Ingress queue accumulates events under bounded capacity.
4. Worker thread microbatches rows into pending chunks.
5. `next_batch(plan)` parses plan and processes pending chunks with state.
6. Final rows are projected and converted to column arrays.
7. Python wraps output as `pyarrow.RecordBatch`.

## State model

- `seen_event_ids`: dedupe memory for exactly-once mode
- `window_state`: keyed window/group accumulators (`sum`, `count`, bounds, group)
- `max_event_time`: watermark progression anchor
- `window_origin`: normalizes large epoch timestamps for stable bucketing

Checkpoint persistence stores/restores the above state.

## Concurrency model

- One native worker thread per stream instance
- Shared mutex/condition variable for queue and pending chunks
- Dedicated processor mutex for stateful plan execution and stats reads
- Numeric aggregation kernel releases GIL for CPU-intensive loops

## Benchmark architecture

`benchmarks/benchmark_pandas_vs_quartzstream.py` compares equivalent workloads across:

- pandas
- polars
- duckdb
- quartzstream

All comparison paths assert logical output equivalence before speedup reporting.

## Extension strategy

To grow toward broader dataframe capabilities while preserving latency goals:

1. Add new plan operators as typed specs first.
2. Implement row-path semantics in `core_processing.cpp`.
3. Add vectorized kernel path when operation dominates runtime.
4. Add cross-engine benchmark + correctness assertion for every operator family.
5. Keep pybind module thin; avoid binding business logic directly.

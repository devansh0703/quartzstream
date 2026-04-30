# HydraStream

HydraStream is a C++-powered Python streaming package that ingests live events into Arrow-native batches and exposes a fluent Python API for low-latency stream processing.

Current implementation scope:

- UDP and manual in-process event sources
- Bounded ingress queue with backpressure accounting
- C++-side batching, filtering, projection, and tumbling-window mean aggregation
- Native numeric column fast path for filter/window/mean workloads
- Arrow `RecordBatch` output through `pyarrow`
- Native Python async iteration over live batches
- Modular C++ engine split across processing/runtime/bindings translation units
- Cross-engine benchmark script for pandas, polars, and duckdb comparisons

The broader product brief in `context.md` includes additional future capabilities that are not yet implemented in this repository.

See `architecture.md` and `explanation.md` for detailed implementation design and execution semantics.

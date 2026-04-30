from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd
import polars as pl

from quartzstream import stream_manual


@dataclass(frozen=True)
class Result:
    rows_in: int
    rows_out: int
    seconds: float

    @property
    def rows_per_second(self) -> float:
        return self.rows_in / self.seconds


def make_columns(count: int) -> dict[str, object]:
    timestamps = np.arange(count, dtype=np.int64)
    return {
        "timestamp": timestamps,
        "is_trade": (timestamps % 5 != 0).astype(np.uint8),
        "symbol_id": (timestamps % 2).astype(np.int64),
        "value": (timestamps % 1000).astype(np.float64),
    }


def run_pandas(columns: dict[str, list[object]]) -> tuple[Result, Result, dict[tuple[int, str], tuple[float, int]]]:
    build_start = time.perf_counter()
    frame = pd.DataFrame(columns)
    build_done = time.perf_counter()

    process_start = time.perf_counter()
    filtered = frame.loc[frame["is_trade"] == 1, ["timestamp", "symbol_id", "value"]].copy()
    filtered["window_start"] = (filtered["timestamp"] // 1000) * 1000
    grouped = (
        filtered.groupby(["window_start", "symbol_id"], sort=True)["value"]
        .agg(avg_value="mean", count="count")
        .reset_index()
    )
    process_done = time.perf_counter()

    lookup = {
        (int(row.window_start), int(row.symbol_id)): (float(row.avg_value), int(row.count))
        for row in grouped.itertuples(index=False)
    }
    row_count = len(columns["timestamp"])
    processing = Result(row_count, len(grouped), process_done - process_start)
    end_to_end = Result(row_count, len(grouped), process_done - build_start)
    print(f"pandas_build_seconds={build_done - build_start:.6f}")
    return processing, end_to_end, lookup


def run_polars(columns: dict[str, list[object]]) -> tuple[Result, Result, dict[tuple[int, str], tuple[float, int]]]:
    build_start = time.perf_counter()
    frame = pl.DataFrame(columns)
    build_done = time.perf_counter()

    process_start = time.perf_counter()
    grouped = (
        frame.filter(pl.col("is_trade") == 1)
        .with_columns(((pl.col("timestamp") // 1000) * 1000).alias("window_start"))
        .group_by(["window_start", "symbol_id"])
        .agg([pl.col("value").mean().alias("avg_value"), pl.col("value").count().alias("count")])
        .sort(["window_start", "symbol_id"])
    )
    process_done = time.perf_counter()

    lookup = {
        (int(row[0]), int(row[1])): (float(row[2]), int(row[3]))
        for row in grouped.iter_rows()
    }
    row_count = len(columns["timestamp"])
    processing = Result(row_count, len(grouped), process_done - process_start)
    end_to_end = Result(row_count, len(grouped), process_done - build_start)
    print(f"polars_build_seconds={build_done - build_start:.6f}")
    return processing, end_to_end, lookup


def run_duckdb(columns: dict[str, list[object]]) -> tuple[Result, Result, dict[tuple[int, str], tuple[float, int]]]:
    build_start = time.perf_counter()
    frame = pd.DataFrame(columns)
    con = duckdb.connect(database=":memory:")
    con.register("events", frame)
    build_done = time.perf_counter()

    process_start = time.perf_counter()
    rows = con.execute(
        """
        SELECT
            FLOOR(timestamp / 1000.0)::BIGINT * 1000 AS window_start,
            symbol_id,
            AVG(value) AS avg_value,
            COUNT(*) AS count
        FROM events
        WHERE is_trade = 1
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall()
    process_done = time.perf_counter()
    con.close()

    lookup = {
        (int(window_start), int(symbol_id)): (float(avg_value), int(count))
        for window_start, symbol_id, avg_value, count in rows
    }
    row_count = len(columns["timestamp"])
    processing = Result(row_count, len(rows), process_done - process_start)
    end_to_end = Result(row_count, len(rows), process_done - build_start)
    print(f"duckdb_build_seconds={build_done - build_start:.6f}")
    return processing, end_to_end, lookup


def run_quartzstream(columns: dict[str, list[object]]) -> tuple[Result, dict[tuple[int, str], tuple[float, int]]]:
    stream = (
        stream_manual(batch_size=len(columns["timestamp"]) + 1, linger_ms=1, capacity=len(columns["timestamp"]) + 1024)
        .filter("is_trade", "==", 1)
        .window(seconds=1, by="symbol_id")
        .mean("value", alias="avg_value")
        .select("window_start", "window_end", "symbol_id", "avg_value", "count")
    )

    start = time.perf_counter()
    out = stream.collect_trade_numeric(
        columns["timestamp"],
        columns["is_trade"],
        columns["symbol_id"],
        columns["value"],
        window_ms=1000,
    )
    end = time.perf_counter()
    stream.close()

    lookup = {
        (int(out["window_start"][index]), int(out["symbol_id"][index])): (
            float(out["avg_value"][index]),
            int(out["count"][index]),
        )
        for index in range(len(out.get("window_start", [])))
    }
    return Result(len(columns["timestamp"]), len(lookup), end - start), lookup


def assert_same_results(
    pandas_lookup: dict[tuple[int, str], tuple[float, int]],
    quartzstream_lookup: dict[tuple[int, str], tuple[float, int]],
) -> None:
    if pandas_lookup.keys() != quartzstream_lookup.keys():
        missing = pandas_lookup.keys() ^ quartzstream_lookup.keys()
        raise AssertionError(f"result key mismatch: {sorted(missing)[:5]}")
    for key, (pandas_mean, pandas_count) in pandas_lookup.items():
        hydra_mean, hydra_count = quartzstream_lookup[key]
        if pandas_count != hydra_count or abs(pandas_mean - hydra_mean) > 1e-9:
            raise AssertionError(
                f"result mismatch for {key}: pandas={(pandas_mean, pandas_count)} "
                f"quartzstream={(hydra_mean, hydra_count)}"
            )


def print_result(name: str, result: Result) -> None:
    print(f"{name}_rows_in={result.rows_in}")
    print(f"{name}_rows_out={result.rows_out}")
    print(f"{name}_seconds={result.seconds:.6f}")
    print(f"{name}_rows_per_second={result.rows_per_second:.0f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare pandas and QuartzStream on the same synthetic rows.")
    parser.add_argument("--events", type=int, default=200_000)
    args = parser.parse_args()

    columns = make_columns(args.events)
    pandas_processing, pandas_end_to_end, pandas_lookup = run_pandas(columns)
    polars_processing, polars_end_to_end, polars_lookup = run_polars(columns)
    duckdb_processing, duckdb_end_to_end, duckdb_lookup = run_duckdb(columns)
    quartzstream_end_to_end, quartzstream_lookup = run_quartzstream(columns)

    assert_same_results(pandas_lookup, quartzstream_lookup)
    assert_same_results(polars_lookup, quartzstream_lookup)
    assert_same_results(duckdb_lookup, quartzstream_lookup)

    print_result("pandas_processing", pandas_processing)
    print_result("pandas_end_to_end", pandas_end_to_end)
    print_result("polars_processing", polars_processing)
    print_result("polars_end_to_end", polars_end_to_end)
    print_result("duckdb_processing", duckdb_processing)
    print_result("duckdb_end_to_end", duckdb_end_to_end)
    print_result("quartzstream_end_to_end", quartzstream_end_to_end)
    print(f"quartzstream_vs_pandas_processing_speedup={quartzstream_end_to_end.rows_per_second / pandas_processing.rows_per_second:.3f}")
    print(f"quartzstream_vs_pandas_end_to_end_speedup={quartzstream_end_to_end.rows_per_second / pandas_end_to_end.rows_per_second:.3f}")
    print(f"quartzstream_vs_polars_processing_speedup={quartzstream_end_to_end.rows_per_second / polars_processing.rows_per_second:.3f}")
    print(f"quartzstream_vs_polars_end_to_end_speedup={quartzstream_end_to_end.rows_per_second / polars_end_to_end.rows_per_second:.3f}")
    print(f"quartzstream_vs_duckdb_processing_speedup={quartzstream_end_to_end.rows_per_second / duckdb_processing.rows_per_second:.3f}")
    print(f"quartzstream_vs_duckdb_end_to_end_speedup={quartzstream_end_to_end.rows_per_second / duckdb_end_to_end.rows_per_second:.3f}")


if __name__ == "__main__":
    main()

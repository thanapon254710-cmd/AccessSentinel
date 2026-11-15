"""Parquet read helper shared by the HDFS adapters.

Reads are single-threaded on purpose: in the Linux service image, processes that used Arrow's threaded
read path aborted at interpreter exit ("terminate called without an active exception", exit 134) in
30-50 % of runs across pyarrow 21, 22 and 26; with use_threads=False it happened in 0 of 40 runs
(measured 2026-11-11, see docs/PROGRESS.md Phase 4). History files are small, so the cost is negligible.
"""
import io

import pyarrow.parquet as pq


def read_parquet_bytes(data: bytes):
    return pq.read_table(io.BytesIO(data), use_threads=False)

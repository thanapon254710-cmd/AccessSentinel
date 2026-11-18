
from pathlib import Path
import pyarrow.parquet as pq

root = Path("accesssentinel-export")

total_rows = 0
total_bytes = 0

print("SYNTHETIC EVENT DATA")
print("-" * 65)

for events_dir in sorted(root.glob("*/events")):
    parquet_files = list(events_dir.rglob("*.parquet"))

    rows = 0
    size = 0

    for file in parquet_files:
        metadata = pq.ParquetFile(file).metadata
        rows += metadata.num_rows
        size += file.stat().st_size

    total_rows += rows
    total_bytes += size

    print(f"\nDirectory: {events_dir.parent.name}")
    print(f"Parquet files: {len(parquet_files):,}")
    print(f"Events: {rows:,}")
    print(f"Storage: {size:,} bytes ({size / 1024:.2f} KB)")

print("\n" + "=" * 65)
print(f"TOTAL EVENTS: {total_rows:,}")
print(f"TOTAL PARQUET SIZE: {total_bytes:,} bytes")
print(f"TOTAL PARQUET SIZE: {total_bytes / (1024**2):.4f} MB")

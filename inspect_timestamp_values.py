
from pathlib import Path
import pyarrow.parquet as pq
import math

ROOT = Path("accesssentinel-export")

files = list(ROOT.rglob("*.parquet"))

print("=" * 65)
print("ACCESS SENTINEL - RAW TIMESTAMP INSPECTION")
print("=" * 65)

all_values = []
inspected_files = 0

for file in files:
    try:
        pf = pq.ParquetFile(file)

        if "t" not in pf.schema_arrow.names:
            continue

        table = pq.read_table(file, columns=["t"])
        values = table["t"].to_pylist()

        values = [
            v for v in values
            if isinstance(v, (int, float))
            and math.isfinite(v)
        ]

        if not values:
            continue

        all_values.extend(values)
        inspected_files += 1

        # Display only the first 8 files
        if inspected_files <= 8:
            print(f"\nFile: {file}")
            print(f"Rows: {len(values):,}")
            print(f"Minimum t: {min(values)}")
            print(f"Maximum t: {max(values)}")
            print(f"Sample values: {values[:10]}")

    except Exception as e:
        print(f"Skipped {file}: {e}")

print("\n" + "=" * 65)
print("OVERALL TIMESTAMP STATISTICS")
print("=" * 65)

if all_values:
    print(f"Files inspected: {inspected_files:,}")
    print(f"Total timestamp values: {len(all_values):,}")
    print(f"Minimum t: {min(all_values)}")
    print(f"Maximum t: {max(all_values)}")
    print(f"First 20 values: {all_values[:20]}")

    sorted_values = sorted(all_values)
    middle = len(sorted_values) // 2
    print(f"Median t: {sorted_values[middle]}")

else:
    print("No numeric timestamp values found.")

print("\nInspection completed.")

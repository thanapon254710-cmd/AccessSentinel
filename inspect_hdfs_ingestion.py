
from pathlib import Path
from datetime import datetime, timezone

import pyarrow.parquet as pq

ROOT = Path("accesssentinel-export")

total_rows = 0
total_bytes = 0
ingestion_times = []
event_times = []
files_with_ingestion = 0
files_without_ingestion = 0

for file in ROOT.rglob("*.parquet"):
    try:
        parquet = pq.ParquetFile(file)
        columns = parquet.schema_arrow.names

        required = [
            c for c in ["t", "_ingested_at", "ti"]
            if c in columns
        ]

        if not required:
            continue

        rows = parquet.read(columns=required).to_pylist()
        total_rows += len(rows)
        total_bytes += file.stat().st_size

        if "_ingested_at" in required:
            files_with_ingestion += 1
        else:
            files_without_ingestion += 1

        for row in rows:
            for field, target in [
                ("t", event_times),
                ("_ingested_at", ingestion_times),
            ]:
                value = row.get(field)

                if isinstance(value, (int, float)) and value > 0:
                    target.append(float(value))

    except Exception as exc:
        print(f"Skipped {file}: {exc}")


def show_range(title, values):
    print(f"\n{title}")
    print("-" * 55)

    if not values:
        print("No timestamps found")
        return

    earliest = min(values)
    latest = max(values)

    print("Count:", len(values))
    print("Earliest:", datetime.fromtimestamp(
        earliest, timezone.utc).isoformat())
    print("Latest:  ", datetime.fromtimestamp(
        latest, timezone.utc).isoformat())
    print("Span:    ", round((latest - earliest) / 86400, 2), "days")


print("ACCESS SENTINEL HDFS INGESTION REPORT")
print("=" * 55)
print("Parquet rows inspected:", total_rows)
print("Parquet storage:", round(total_bytes / (1024 ** 2), 4), "MB")
print("Files with _ingested_at:", files_with_ingestion)
print("Files without _ingested_at:", files_without_ingestion)

show_range("EVENT TIME (t)", event_times)
show_range("INGESTION TIME (_ingested_at)", ingestion_times)

print("\n" + "=" * 55)


from pathlib import Path
from datetime import datetime, timezone
import pyarrow.parquet as pq
import subprocess
import re

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

ROOT = Path("accesssentinel-export")
OUTPUT = "hdfs_storage_report.txt"

# Fallback if live HDFS capacity cannot be retrieved
FALLBACK_FREE_GB = 943.58

# Plausible Unix timestamp range: 2000 to 2100
MIN_TIMESTAMP = 946684800
MAX_TIMESTAMP = 4133980799

# --------------------------------------------------
# SCAN EXPORTED PARQUET FILES
# --------------------------------------------------

files = list(ROOT.rglob("*.parquet"))

total_events = 0
total_bytes = 0
valid_files = 0
all_timestamps = []

for file in files:
    try:
        pf = pq.ParquetFile(file)
        schema = pf.schema_arrow

        # Only count event files containing timestamp column "t"
        if "t" not in schema.names:
            continue

        rows = pf.metadata.num_rows
        size = file.stat().st_size

        total_events += rows
        total_bytes += size
        valid_files += 1

        values = pq.read_table(
            file,
            columns=["t"]
        )["t"].to_pylist()

        # Keep non-null numeric timestamps
        values = [
            v for v in values
            if v is not None and isinstance(v, (int, float))
        ]

        all_timestamps.extend(values)

    except Exception as e:
        print(f"Skipped: {file} ({e})")

# --------------------------------------------------
# VALIDATE TIMESTAMPS
# --------------------------------------------------

normalized = [
    v / 1000 if v > 1e12 else v
    for v in all_timestamps
]

valid = [
    v for v in normalized
    if MIN_TIMESTAMP <= v <= MAX_TIMESTAMP
]

invalid_count = len(normalized) - len(valid)

earliest = min(valid) if valid else None
latest = max(valid) if valid else None

retention_days = (
    (latest - earliest) / 86400
    if earliest is not None and latest is not None
    else 0
)

def convert_time(value):
    if value is None:
        return "N/A"

    return datetime.fromtimestamp(
        value,
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

# --------------------------------------------------
# GET CURRENT HDFS FREE CAPACITY
# --------------------------------------------------

free_gb = FALLBACK_FREE_GB
capacity_source = "Fallback value"

command = [
    "docker",
    "compose",
    "--env-file", ".\\.env",
    "-f", ".\\deploy\\docker-compose.yml",
    "exec", "-T", "namenode",
    "hdfs", "dfsadmin", "-report"
]

try:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=30
    )

    match = re.search(
        r"DFS Remaining:\s*[\d,]+\s*\(([\d.]+)\s*GB\)",
        result.stdout,
        re.IGNORECASE
    )

    if result.returncode == 0 and match:
        free_gb = float(match.group(1))
        capacity_source = "Live HDFS report"
    else:
        print("Could not read live HDFS capacity.")
        if result.stderr:
            print(result.stderr)

except Exception as e:
    print(f"Could not retrieve HDFS capacity: {e}")

# --------------------------------------------------
# ESTIMATE EVENT CAPACITY
# --------------------------------------------------

average_bytes = (
    total_bytes / total_events
    if total_events > 0
    else 0
)

free_bytes = free_gb * (1024 ** 3)

max_additional_events = (
    int(free_bytes / average_bytes)
    if average_bytes > 0
    else 0
)

total_capacity_events = (
    total_events + max_additional_events
)

# --------------------------------------------------
# GENERATE REPORT
# --------------------------------------------------

lines = [
    "ACCESS SENTINEL - HDFS STORAGE REPORT",
    "=" * 55,
    "",
    "1. CURRENT DATA",
    "-" * 55,
    f"Parquet files: {valid_files:,}",
    f"Total events: {total_events:,}",
    f"Parquet storage: {total_bytes / (1024 ** 2):,.2f} MB",
    f"Average storage per event: {average_bytes:,.2f} bytes",
    "",
    "2. EVENT TIMESTAMP RANGE",
    "-" * 55,
    f"Total timestamps: {len(normalized):,}",
    f"Valid timestamps: {len(valid):,}",
    f"Invalid timestamps: {invalid_count:,}",
    f"Earliest event: {convert_time(earliest)}",
    f"Latest event: {convert_time(latest)}",
    f"Observed timestamp span: {retention_days:.2f} days",
    "",
    "3. HDFS STORAGE CAPACITY",
    "-" * 55,
    f"Available capacity: {free_gb:,.2f} GB",
    f"Capacity source: {capacity_source}",
    f"Estimated additional events: {max_additional_events:,}",
    f"Estimated total event capacity: {total_capacity_events:,}",
    "",
    "4. DATA RETENTION POLICY",
    "-" * 55,
    "Automatic TTL / expiration: NOT CONFIGURED",
    "Current retention: No automatic deletion policy found",
    "Note: Timestamp span is not a configured retention limit.",
    "",
    "5. IMPORTANT LIMITATIONS",
    "-" * 55,
    "- Timestamp validation accepts Unix dates from 2000 to 2100.",
    "- Invalid timestamps are excluded from the observed span.",
    "- Capacity assumes future events have similar compression.",
    "- Does not reserve space for feedback, indexes, or other files.",
    "- HDFS replication and operational overhead may reduce capacity.",
    "- Event counts include scanned tenant and root event files.",
    "- Counts are not deduplicated across tenants.",
    "- Capacity estimate assumes all available space is usable.",
    "",
    "=" * 55,
    "END OF REPORT"
]

report = "\n".join(lines)

# --------------------------------------------------
# SAVE REPORT
# --------------------------------------------------

Path(OUTPUT).write_text(
    report,
    encoding="utf-8"
)

print(report)
print(f"\nReport saved to: {OUTPUT}")

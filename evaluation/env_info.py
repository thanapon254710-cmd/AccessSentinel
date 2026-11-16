"""Record the hardware / software environment of an experiment run (Deliverable 4 reproducibility).

    python -m evaluation.env_info                     # prints JSON
    python -m evaluation.env_info --out evaluation/results/raw/environment.json

Every experiment calls `collect()` and stores the result next to its raw measurements, so each number can be
traced to the machine, library versions and git commit that produced it. Nothing here is estimated: values
that cannot be read are recorded as null.
"""
import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone


def _run(cmd, timeout=15):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return out.stdout.strip() if out.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _total_ram_bytes():
    try:
        if sys.platform == "win32":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
            m = MEMORYSTATUSEX(); m.dwLength = ctypes.sizeof(m)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
            return {"total": m.ullTotalPhys, "available": m.ullAvailPhys}
        pages, size = os.sysconf("SC_PHYS_PAGES"), os.sysconf("SC_PAGE_SIZE")
        avail = os.sysconf("SC_AVPHYS_PAGES")
        return {"total": pages * size, "available": avail * size}
    except Exception:  # noqa: BLE001
        return {"total": None, "available": None}


def _cpu_name():
    if sys.platform == "win32":
        out = _run(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor | Select-Object -First 1).Name"])
        return out or platform.processor()
    try:
        for line in open("/proc/cpuinfo", encoding="utf-8"):
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or None


def collect() -> dict:
    env = {"recorded_at": datetime.now(timezone.utc).isoformat(), "unix_time": time.time(),
           "host_os": platform.platform(), "python": sys.version.split()[0],
           "cpu": {"name": _cpu_name(), "logical_cpus": os.cpu_count()}, "ram_bytes": _total_ram_bytes()}
    gpu = _run(["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader"])
    env["gpu"] = [g.strip() for g in gpu.splitlines()] if gpu else []
    libs = {}
    for mod in ("torch", "numpy", "pandas", "sklearn", "pyarrow", "confluent_kafka", "matplotlib"):
        try:
            m = __import__(mod)
            libs[mod] = getattr(m, "__version__", "unknown")
        except ImportError:
            libs[mod] = None
    env["libraries"] = libs
    try:
        import torch
        env["torch_cuda_available"] = bool(torch.cuda.is_available())
        env["torch_threads"] = torch.get_num_threads()
    except ImportError:
        env["torch_cuda_available"] = None
    env["git_commit"] = _run(["git", "rev-parse", "HEAD"])
    env["git_dirty"] = bool(_run(["git", "status", "--porcelain"]))
    dock = _run(["docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}"])
    if dock and dock.count("|") == 2:
        v, n, m = dock.split("|")
        env["docker"] = {"server_version": v, "vm_cpus": int(n), "vm_memory_bytes": int(m)}
    else:
        env["docker"] = None
    return env


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    env = collect()
    text = json.dumps(env, indent=2)
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

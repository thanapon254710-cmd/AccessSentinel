"""Process plumbing for stream services: logging, SIGTERM/SIGINT -> graceful stop, heartbeat file for the
container health check (a stuck consumer stops touching it and the container turns unhealthy)."""
import logging
import os
import signal
import time

HEARTBEAT_FILE = os.environ.get("AS_HEARTBEAT_FILE", "/tmp/as-heartbeat")


class Heartbeat:
    def __init__(self, path: str = HEARTBEAT_FILE, every: float = 5.0):
        self.path, self.every, self._last = path, every, 0.0

    def __call__(self) -> None:
        now = time.monotonic()
        if now - self._last >= self.every:
            try:
                with open(self.path, "w") as f:
                    f.write(str(time.time()))
            except OSError:
                pass
            self._last = now


def is_healthy(path: str = HEARTBEAT_FILE, max_age: float = 60.0) -> bool:
    try:
        return time.time() - os.path.getmtime(path) < max_age
    except OSError:
        return False


def setup(level: str) -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def install_stop(stop_fn) -> None:
    def _handler(signum, frame):
        logging.getLogger("accesssentinel.streaming").info("signal %s: stopping after the current batch", signum)
        stop_fn()
    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


if __name__ == "__main__":                       # used by the compose health check
    raise SystemExit(0 if is_healthy() else 1)

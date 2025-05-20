"""
Background WAL-flush thread – central to *eventual durability*.

• Runs forever (daemon) every `interval` seconds.
• Sequence:
      1. fsync() the WAL  -> hardens all bytes written so far
      2. append a FLUSH record  (marks durability barrier)
      3. fsync() again          (persists the FLUSH marker itself)

Any COMMIT that appears *before* the most-recent FLUSH record is
guaranteed durable at crash time; fast-txns that commit later may be lost.
"""
from __future__ import annotations
import threading, time
from .wal import fsync, _write

def _loop(interval: float = 0.5) -> None:
    while True:
        time.sleep(interval)
        try:
            # 1. flush existing WAL bytes
            fsync()
            # 2. append FLUSH marker
            _write({"type": "FLUSH"})
            # 3. ensure marker itself is on disk
            fsync()
        except FileNotFoundError:
            # WAL not created yet – ignore
            pass

def start(interval: float = 0.5) -> None:
    th = threading.Thread(target=_loop, kwargs={"interval": interval},
                          daemon=True, name="wal-flush")
    th.start()

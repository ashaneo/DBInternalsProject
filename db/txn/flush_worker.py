# <!-- filename: db/txn/flush_worker.py -->
import threading, time
from .wal import fsync
def _loop():
    while True:
        time.sleep(0.5)
        try: fsync()
        except FileNotFoundError: pass
def start(): threading.Thread(target=_loop,daemon=True).start()

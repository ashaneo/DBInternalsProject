# <!-- filename: db/txn/wal.py -->
import json, pathlib, time, os
BASE = pathlib.Path(__file__).resolve().parent.parent.parent
LOG  = BASE / "logs"; LOG.mkdir(exist_ok=True)
WAL  = LOG / "wal.log"

def _write(rec:dict):
    with open(WAL,"a") as f:
        f.write(json.dumps(rec)+"\n")

def log_begin(txn):           _write({"ts":time.time(),"type":"BEGIN","id":txn})
def log_action(txn,act,data): _write({"ts":time.time(),"type":act,"id":txn,"data":data})
def log_commit(txn):          _write({"ts":time.time(),"type":"COMMIT","id":txn})
def fsync():
    fd=os.open(WAL, os.O_RDWR); os.fsync(fd); os.close(fd)

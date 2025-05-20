# <!-- filename: tests/test_select.py -->
import sys, os

BASE_DIR = os.getcwd()
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from db.executor import execute
from db.parser import parse

def run(sql): execute(parse(sql))

def test_basic_select(tmp_path, monkeypatch):
    # isolate data dir
    import db.storage as sto
    monkeypatch.setattr(sto, "DATA_DIR", tmp_path/"data"); sto.DATA_DIR.mkdir()
    # create + insert
    run("CREATE TABLE tt (id, name) PRIMARY KEY id;")
    run("INSERT INTO t VALUES (1,'a');")
    rows = execute(parse("SELECT * FROM t;"))
    assert rows[0]["id"] == 1

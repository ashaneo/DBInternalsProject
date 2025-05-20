# <!-- filename: db/storage.py -->
import json, pathlib
from typing import Any, Dict, List
from .catalog import load, save, BASE

DATA_DIR = BASE / "data"; DATA_DIR.mkdir(exist_ok=True)

def _path(name: str) -> pathlib.Path: return DATA_DIR / f"{name}.dat"

#───────────────── DDL
def create_table(name: str, columns: list[str], pk: str):
    sch = load()
    if name in sch["tables"]: raise ValueError("table exists")
    sch["tables"][name] = {"columns": columns, "primary_key": pk}
    save(sch); _path(name).touch()

def drop_table(name: str):
    sch = load()
    sch["tables"].pop(name, None); save(sch)
    _path(name).unlink(missing_ok=True)

#───────────────── INSERT/DELETE low-level
def append_row(name: str, row: Dict[str, Any]):
    with _path(name).open("a") as f:
        f.write(json.dumps(row) + "\n")

def scan(name: str) -> List[Dict[str, Any]]:
    with _path(name).open() as f:
        return [json.loads(l) for l in f]

def delete_pk(name: str, pk_field: str, value):
    src, tmp = _path(name), _path(name).with_suffix(".tmp")
    with src.open() as s, tmp.open("w") as t:
        for line in s:
            row = json.loads(line)
            if row.get(pk_field) != value: t.write(line)
    tmp.replace(src)

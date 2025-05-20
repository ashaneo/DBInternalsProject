# <!-- filename: db/catalog.py -->
import json, pathlib

BASE = pathlib.Path(__file__).resolve().parent.parent
META = BASE / "metadata"; META.mkdir(exist_ok=True)
SCHEMA_FILE = META / "schema.json"

def _init():
    SCHEMA_FILE.write_text(json.dumps({"tables": {}}))

def load() -> dict:
    if not SCHEMA_FILE.exists(): _init()
    try:
        return json.loads(SCHEMA_FILE.read_text())
    except json.JSONDecodeError:
        _init(); return load()

def save(sch: dict):
    SCHEMA_FILE.write_text(json.dumps(sch, indent=4))

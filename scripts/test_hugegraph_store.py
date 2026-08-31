"""
Smoke test for HugeGraphStore.

Run with the active hg-llm venv (or any env with ``requests``):

    /Users/mac/.workbuddy/binaries/python/envs/hg-llm/bin/python3.10 \\
        /Users/mac/Desktop/apache-code/hugegraph-dev/semantica-hg-backend/scripts/test_hugegraph_store.py

Logs to ``/Users/mac/Desktop/apache-code/hugegraph-dev/_out/semantica-hg-backend/logs/test_hg_store.log``.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

# Allow running from anywhere
HG_DEV = "/Users/mac/Desktop/apache-code/hugegraph-dev"
SEMANTICA = os.path.join(HG_DEV, "semantica-hg-backend")
LOG_FILE = os.path.join(HG_DEV, "_out", "semantica-hg-backend", "logs", "test_hg_store.log")
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, SEMANTICA)

# Mirror stdout/stderr to the log file (no truncation; append mode)
_log_fh = open(LOG_FILE, "a", buffering=1)
_log_fh.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} smoke test start ===\n")
_orig_print = print
def print(*args, **kw):
    line = " ".join(str(a) for a in args)
    _orig_print(line, **kw)
    _log_fh.write(line + "\n")
sys.stdout = _log_fh
sys.stderr = _log_fh

# Restore stdout/stderr when this file is imported as a module
def _restore_streams():  # pragma: no cover
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    _log_fh.close()

import requests
from semantica.graph_store.hugegraph_store import HugeGraphStore

BASE = "http://localhost:8080"
GRAPH = "hugegraph"


def test_connectivity() -> None:
    print("[1/6] connectivity")
    r = requests.get(f"{BASE}/", timeout=5)
    r.raise_for_status()
    info = r.json()
    print(f"  hugegraph version = {info.get('version')}")


def test_gremlin_job() -> None:
    print("[2/6] gremlin job submission")
    r = requests.post(
        f"{BASE}/graphs/{GRAPH}/jobs/gremlin",
        json={"gremlin": "g.V().limit(1)", "language": "gremlin"},
        timeout=5,
    )
    r.raise_for_status()
    task_id = r.json().get("task_id")
    print(f"  task_id = {task_id}")
    for _ in range(30):
        rr = requests.get(f"{BASE}/jobs/{task_id}", timeout=5).json()
        if rr.get("status") in ("finished", "success", "completed"):
            print(f"  status = {rr.get('status')}, data = {json.dumps(rr.get('data', {}))[:200]}")
            return
        time.sleep(0.3)
    raise RuntimeError("Gremlin job timeout")


def test_store_create_node(store: HugeGraphStore) -> str:
    print("[3/6] store.create_node")
    res = store.create_node(
        labels=["SemanticaTestPerson"],
        properties={"name": "Alice", "age": 30},
    )
    print(f"  result = {res}")
    assert res["id"], "vertex id missing"
    return res["id"]


def test_store_get_node(store: HugeGraphStore, vid: str) -> None:
    print("[4/6] store.get_node")
    res = store.get_node(vid)
    print(f"  fetched = {res}")
    assert res is not None, "vertex not found"
    assert res["properties"].get("name") == "Alice"


def test_store_create_relationship(store: HugeGraphStore) -> None:
    print("[5/6] create relationship")
    a = store.create_node(labels=["SemanticaTestPerson"], properties={"name": "Bob"})
    b = store.create_node(labels=["SemanticaTestPerson"], properties={"name": "Carol"})
    rel = store.create_relationship(
        start_node_id=a["id"],
        end_node_id=b["id"],
        rel_type="knows",
        properties={"since": 2020},
    )
    print(f"  edge = {rel}")
    assert rel["id"]


def test_store_execute_query(store: HugeGraphStore) -> None:
    print("[6/6] store.execute_query (Gremlin)")
    res = store.execute_query("g.V().hasLabel('SemanticaTestPerson').limit(5).valueMap()")
    print(f"  records = {len(res['records'])}, sample = {json.dumps(res['records'][:2], default=str)[:300]}")


def main() -> int:
    try:
        test_connectivity()
        test_gremlin_job()
        store = HugeGraphStore(host="localhost", port=8080, graph=GRAPH)
        store.connect()
        test_store_create_node(store)
        test_store_create_relationship(store)
        test_store_execute_query(store)
        print("ALL TESTS PASSED")
        _restore_streams()
        _log_fh.close()
        return 0
    except Exception as e:
        print(f"FAILED: {e}")
        _restore_streams()
        _log_fh.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端验证 Semantica 上层 Pipeline -> HugeGraph 落地闭环。

链路
----
GraphBuilder.build(sources, graph_store=GraphStore(backend="hugegraph"))
  -> graph_store.add_nodes(resolved_entities)     # 逻辑 id
  -> graph_store.add_edges(formatted_edges)       # source_id/target_id = 逻辑 id
       -> HugeGraphStore.create_node / create_relationship
            -> HugeGraph 1.7.0 REST

本脚本喂**预抽取**的实体/关系（不走 LLM，保证可复现、无外部依赖），
断言节点与边真实落库。

日志：/Users/mac/Desktop/apache-code/hugegraph-dev/_out/semantica-hg-backend/logs/test_pipeline_e2e.log
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

HG_DEV = "/Users/mac/Desktop/apache-code/hugegraph-dev"
SEMANTICA = os.path.join(HG_DEV, "semantica-hg-backend")
LOG_FILE = os.path.join(HG_DEV, "_out", "semantica-hg-backend", "logs",
                        "test_pipeline_e2e.log")
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, SEMANTICA)

# tee stdout/stderr to the log file
_log_fh = open(LOG_FILE, "a", buffering=1)
_log_fh.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} pipeline e2e start ===\n")
_orig_print = print


def print(*args, **kw):  # noqa: A001
    line = " ".join(str(a) for a in args)
    _log_fh.write(line + "\n")
    _orig_print(line, **kw)


sys.stderr = _log_fh

import requests  # noqa: E402

BASE = "http://localhost:8080"
GRAPH = "hugegraph"

from semantica.graph_store.graph_store import GraphStore  # noqa: E402
from semantica.kg.graph_builder import GraphBuilder  # noqa: E402

# ---------------------------------------------------------------------------
# 预抽取的 KG 数据（模拟 LLM 抽取结果，可复现）
# GraphBuilder._process_item 接受 {"entities": [...], "relationships": [...]}，
# 实体需带 id/name/type，关系需带 source/target/type。
# ---------------------------------------------------------------------------
SOURCES = [
    {
        "entities": [
            {"id": "person_alice", "name": "Alice", "type": "Person",
             "confidence": 0.95, "metadata": {}},
            {"id": "person_bob", "name": "Bob", "type": "Person",
             "confidence": 0.92, "metadata": {}},
            {"id": "org_acme", "name": "Acme Corp", "type": "Organization",
             "confidence": 0.98, "metadata": {}},
            {"id": "loc_shanghai", "name": "Shanghai", "type": "Location",
             "confidence": 0.90, "metadata": {}},
        ],
        "relationships": [
            {"source": "person_alice", "target": "org_acme",
             "type": "WORKS_AT", "confidence": 0.9, "metadata": {"since": "2020"}},
            {"source": "person_bob", "target": "org_acme",
             "type": "WORKS_AT", "confidence": 0.88, "metadata": {"since": "2021"}},
            {"source": "person_alice", "target": "person_bob",
             "type": "KNOWS", "confidence": 0.85, "metadata": {}},
            {"source": "org_acme", "target": "loc_shanghai",
             "type": "LOCATED_IN", "confidence": 0.93, "metadata": {}},
        ],
    }
]

EXPECTED_NODES = 4
EXPECTED_EDGES = 4


def gremlin(script: str):
    r = requests.post(
        f"{BASE}/gremlin",
        data=json.dumps({"gremlin": script, "aliases": {"g": "__g_DEFAULT-hugegraph"}}),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return (r.json().get("result") or {}).get("data") or []


def check(label: str, cond: bool, detail: str = "") -> bool:
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {label}" + (f" — {detail}" if detail else ""))
    return cond


def main() -> int:
    ok_all = True
    print("=" * 70)
    print("Semantica Pipeline -> HugeGraph end-to-end")
    print("=" * 70)

    # ---- 0. readiness ------------------------------------------------------
    print("\n[0] readiness")
    r = requests.get(f"{BASE}/graphs", timeout=10)
    r.raise_for_status()
    print(f"  server reachable: {r.json()}")
    for _ in range(10):
        gr = requests.post(
            f"{BASE}/gremlin",
            data=json.dumps({"gremlin": "g.V().limit(1)",
                             "aliases": {"g": "__g_DEFAULT-hugegraph"}}),
            headers={"Accept": "application/json"}, timeout=20,
        )
        if gr.ok and '"code":200' in gr.text:
            print("  gremlin ready")
            break
        time.sleep(2)
    else:
        print("  gremlin NOT ready — abort")
        return 1

    # ---- 1. build graph via GraphBuilder + GraphStore(hugegraph) -----------
    print("\n[1] GraphBuilder.build(...) with GraphStore(backend='hugegraph')")
    gs = GraphStore(backend="hugegraph",
                    config={"host": "localhost", "port": 8080,
                            "graph": GRAPH, "timeout": 30})
    gs.connect()
    print(f"  GraphStore connected, backend={gs.backend}")

    builder = GraphBuilder(graph_store=gs)
    graph = builder.build(SOURCES)
    n_ent = len(graph["entities"])
    n_rel = len(graph["relationships"])
    print(f"  build() returned {n_ent} entities / {n_rel} relationships")
    ok_all &= check("entity count", n_ent >= EXPECTED_NODES, f"{n_ent}>={EXPECTED_NODES}")
    ok_all &= check("relationship count", n_rel >= EXPECTED_EDGES, f"{n_rel}>={EXPECTED_EDGES}")

    # ---- 2. assert nodes landed -------------------------------------------
    print("\n[2] assert nodes landed in HugeGraph")
    rows = gremlin("g.V().limit(50).project('id','label').by(id).by(label)")
    print(f"  vertices in store: {json.dumps(rows, default=str)}")
    ids = {r.get("id") for r in rows}
    for eid in ("person_alice", "person_bob", "org_acme", "loc_shanghai"):
        ok_all &= check(f"vertex {eid} exists", eid in ids)

    # label check: CUSTOMIZE_STRING ids must preserve their type label
    lbl = {r.get("id"): r.get("label") for r in rows}
    ok_all &= check("person_alice label=Person", lbl.get("person_alice") == "Person",
                    f"got {lbl.get('person_alice')}")
    ok_all &= check("org_acme label=Organization", lbl.get("org_acme") == "Organization",
                    f"got {lbl.get('org_acme')}")

    # ---- 3. assert edges landed -------------------------------------------
    print("\n[3] assert edges landed in HugeGraph")
    rows = gremlin(
        "g.E().limit(50).project('label','outV','inV')"
        ".by(label).by(outV().id()).by(inV().id())"
    )
    print(f"  edges in store: {json.dumps(rows, default=str)}")
    etriples = {(r.get("outV"), r.get("label"), r.get("inV")) for r in rows}
    for s, t, ty in (
        ("person_alice", "org_acme", "WORKS_AT"),
        ("person_bob", "org_acme", "WORKS_AT"),
        ("person_alice", "person_bob", "KNOWS"),
        ("org_acme", "loc_shanghai", "LOCATED_IN"),
    ):
        ok_all &= check(f"edge {s}-[{ty}]->{t}", (s, ty, t) in etriples)

    # ---- 4. logical-id get_node round-trip --------------------------------
    print("\n[4] get_node by logical id round-trip")
    store = gs._store_backend
    for eid in ("person_alice", "org_acme"):
        node = store.get_node(eid)
        got = (node or {}).get("properties", {}).get("name")
        ok_all &= check(f"get_node('{eid}')", node is not None, f"name={got}")

    # ---- 5. stats + neighbor traversal ------------------------------------
    print("\n[5] get_stats / get_neighbors")
    stats = gs.get_stats()
    print(f"  stats: {stats}")
    ok_all &= check("vertex_count >= 4", stats.get("vertex_count", 0) >= 4, str(stats))
    ok_all &= check("edge_count >= 4", stats.get("edge_count", 0) >= 4, str(stats))

    nb = gs.get_neighbors(node_id="person_alice", direction="out")
    print(f"  neighbors of person_alice: {json.dumps(nb, default=str)[:300]}")
    ok_all &= check("alice has >=2 out-neighbors", len(nb) >= 2, f"got {len(nb)}")

    # ---- 6. verify_backends-style contract (create/get/update/delete) ------
    print("\n[6] adapter contract: create -> get -> update -> delete")
    n = gs.create_node(labels=["ContractTest"],
                       properties={"id": "contract_1", "name": "C1"})
    nid = n.get("id")
    ok_all &= check("create_node returns logical id", nid == "contract_1", f"got {nid}")

    got = gs.get_node(nid)
    ok_all &= check("get_node", got is not None and
                    got.get("properties", {}).get("name") == "C1")

    gs.update_node(nid, properties={"updated": "yes"})
    got2 = gs.get_node(nid)
    ok_all &= check("update_node", (got2 or {}).get("properties", {}).get("updated") == "yes",
                    str((got2 or {}).get("properties")))

    n2 = gs.create_node(labels=["ContractTest"],
                        properties={"id": "contract_2", "name": "C2"})
    rel = gs.create_relationship("contract_1", n2["id"], "LINKS_TO", {"w": 1})
    ok_all &= check("create_relationship", bool(rel.get("id")), str(rel.get("id")))

    gs.delete_node(nid)
    gs.delete_node(n2["id"])
    ok_all &= check("delete_node", gs.get_node(nid) is None)

    gs.close()

    # ---- summary -----------------------------------------------------------
    print("\n" + "=" * 70)
    print("END-TO-END RESULT:", "ALL CHECKS PASSED" if ok_all else "SOME CHECKS FAILED")
    print("=" * 70)
    return 0 if ok_all else 1


if __name__ == "__main__":
    try:
        rc = main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"EXCEPTION: {e}")
        rc = 1
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    _log_fh.close()
    sys.exit(rc)

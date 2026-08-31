#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
探测 HugeGraph CUSTOMIZE_STRING id 策略下「逻辑 id」在 REST / Gremlin 双路径的可用性。

背景
----
Semantica GraphBuilder.build() 走两条写入路径：
  add_nodes(resolved_entities)  -> GraphStore.add_nodes  -> create_node(labels, props)
  add_edges(formatted_edges)    -> GraphStore.add_edges  -> create_relationship(src, dst, type)
其中 src/dst 是**逻辑实体 id**（如 "person_alice"），不是 HG 自增数字 id。

当前适配器用 DEFAULT 自增策略，create_node 返回复合 id "11580...:Person"，
与 Pipeline 后续建边时传入的 "person_alice" 对不上 —— 边必然建不出来。

本脚本验证候选方案：CUSTOMIZE_STRING 策略，让顶点 id == 逻辑 id。
需回答 4 个问题：
  Q1 REST POST /graph/vertices 能否用 "person_alice" 作 id 建点？
  Q2 REST GET  /graph/vertices/{id} 用什么编码能取回？
  Q3 REST POST /graph/edges 能否用 "person_alice" 直接作 outV/inV？
  Q4 Gremlin  V('person_alice') 能否查到？

日志：/Users/mac/Desktop/apache-code/hugegraph-dev/_out/semantica-hg-backend/logs/probe_customize_string.log
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import os  # noqa: E402

HG_DEV = "/Users/mac/Desktop/apache-code/hugegraph-dev"
LOG_FILE = os.path.join(HG_DEV, "_out", "semantica-hg-backend", "logs",
                        "probe_customize_string.log")
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

BASE = "http://localhost:8080"
GRAPH = "hugegraph"
SCHEMA = f"{BASE}/graphs/{GRAPH}/schema"

import requests  # noqa: E402

S = requests.Session()
S.headers.update({"Content-Type": "application/json"})

results: dict[str, str] = {}


def log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)


def wait_task(task_id, attempts=60) -> dict:
    url = f"{BASE}/graphs/{GRAPH}/tasks/{task_id}"
    for _ in range(attempts):
        try:
            r = S.get(url, timeout=10)
            if r.ok:
                d = r.json()
                if (d.get("task_status") or "").lower() in ("success", "failed", "cancelled"):
                    return d
        except Exception:
            pass
        time.sleep(0.2)
    return {}


def ensure_pk(name: str, data_type: str = "TEXT") -> None:
    r = S.get(f"{SCHEMA}/propertykeys", timeout=10)
    existing = {p["name"] for p in r.json().get("propertykeys", [])}
    if name in existing:
        return
    r = S.post(f"{SCHEMA}/propertykeys",
               data=json.dumps({"name": name, "data_type": data_type,
                                "cardinality": "SINGLE"}), timeout=10)
    log(f"  pk {name}: HTTP {r.status_code} {r.text[:120]}")


def ensure_vlabel(label: str, id_strategy: str, props: list) -> None:
    r = S.get(f"{SCHEMA}/vertexlabels", timeout=10)
    existing = {v["name"] for v in r.json().get("vertexlabels", [])}
    if label in existing:
        log(f"  vlabel {label}: already exists")
        return
    body = {"name": label, "id_strategy": id_strategy, "primary_keys": [],
            "properties": props, "nullable_keys": [],
            "enable_label_index": True}
    r = S.post(f"{SCHEMA}/vertexlabels", data=json.dumps(body), timeout=10)
    log(f"  vlabel {label}: HTTP {r.status_code} {r.text[:200]}")


def ensure_elabel(label: str, src: str, dst: str, props: list) -> None:
    r = S.get(f"{SCHEMA}/edgelabels", timeout=10)
    existing = {e["name"] for e in r.json().get("edgelabels", [])}
    if label in existing:
        log(f"  elabel {label}: already exists")
        return
    body = {"name": label, "source_label": src, "target_label": dst,
            "properties": props, "frequency": "SINGLE"}
    r = S.post(f"{SCHEMA}/edgelabels", data=json.dumps(body), timeout=10)
    log(f"  elabel {label}: HTTP {r.status_code} {r.text[:200]}")


def main() -> int:
    log("=== CUSTOMIZE_STRING id strategy probe ===")

    # ---- schema -----------------------------------------------------------
    log("[schema] property keys id/name")
    ensure_pk("id")
    ensure_pk("name")

    log("[schema] vertex label ProbePerson (CUSTOMIZE_STRING)")
    ensure_vlabel("ProbePerson", "CUSTOMIZE_STRING", ["id", "name"])
    ensure_vlabel("ProbeOrg", "CUSTOMIZE_STRING", ["id", "name"])

    # ---- Q1: POST vertices with logical id --------------------------------
    log("[Q1] POST /graph/vertices id='person_alice'")
    body = {"label": "ProbePerson", "id": "person_alice",
            "properties": {"id": "person_alice", "name": "Alice"}}
    r = S.post(f"{BASE}/graphs/{GRAPH}/graph/vertices", data=json.dumps(body), timeout=10)
    log(f"  Q1 HTTP {r.status_code}: {r.text[:300]}")
    results["Q1_post_logical_id"] = "PASS" if r.ok else f"FAIL({r.status_code})"

    body2 = {"label": "ProbeOrg", "id": "org_acme",
             "properties": {"id": "org_acme", "name": "Acme"}}
    r2 = S.post(f"{BASE}/graphs/{GRAPH}/graph/vertices", data=json.dumps(body2), timeout=10)
    log(f"  Q1b HTTP {r2.status_code}: {r2.text[:200]}")
    results["Q1b_post_second"] = "PASS" if r2.ok else f"FAIL({r2.status_code})"

    # ---- Q2: GET vertex by logical id (3 encodings) -----------------------
    from urllib.parse import quote
    for tag, enc in (("raw", "person_alice"),
                     ("quoted", quote("person_alice", safe="")),
                     ("jsonquoted", '%22person_alice%22')):
        rr = S.get(f"{BASE}/graphs/{GRAPH}/graph/vertices/{enc}", timeout=10)
        log(f"  Q2 GET {tag:12s} -> HTTP {rr.status_code}: {rr.text[:160]}")
        results[f"Q2_get_{tag}"] = "PASS" if rr.ok else f"FAIL({rr.status_code})"

    # ---- Q3: POST edge using logical string ids as endpoints --------------
    log("[Q3] POST /graph/edges outV='person_alice' inV='org_acme'")
    ensure_elabel("WORKS_AT", "ProbePerson", "ProbeOrg", ["since"])
    for tag, outv, inv in (
        ("str", "person_alice", "org_acme"),
        ("jsonstr", '"person_alice"', '"org_acme"'),
    ):
        # note: for jsonstr we must embed real quotes in JSON body
        if tag == "jsonstr":
            ebody = {"label": "WORKS_AT", "outV": "person_alice", "inV": "org_acme",
                     "outVLabel": "ProbePerson", "inVLabel": "ProbeOrg",
                     "properties": {"since": "2020"}}
        else:
            ebody = {"label": "WORKS_AT", "outV": "person_alice", "inV": "org_acme",
                     "outVLabel": "ProbePerson", "inVLabel": "ProbeOrg",
                     "properties": {"since": "2020"}}
        rr = S.post(f"{BASE}/graphs/{GRAPH}/graph/edges", data=json.dumps(ebody), timeout=10)
        log(f"  Q3 {tag} HTTP {rr.status_code}: {rr.text[:250]}")
        results[f"Q3_edge_{tag}"] = "PASS" if rr.ok else f"FAIL({rr.status_code})"

    # ---- Q4: Gremlin lookup ------------------------------------------------
    log("[Q4] Gremlin V('person_alice')")
    for script in ("g.V('person_alice').valueMap()",
                   "g.V().hasLabel('ProbePerson').limit(5).valueMap()",
                   "g.V('person_alice').outE().inV().valueMap()"):
        rr = S.post(f"{BASE}/gremlin",
                    data=json.dumps({"gremlin": script,
                                     "aliases": {"g": "__g_DEFAULT-hugegraph"}}),
                    headers={"Accept": "application/json"}, timeout=20)
        ok = rr.ok and '"code":200' in rr.text
        log(f"  Q4 {script[:45]:47s} -> HTTP {rr.status_code}: {rr.text[:200]}")
        results[f"Q4_{script[:28]}"] = "PASS" if ok else f"FAIL({rr.status_code})"

    # ---- summary -----------------------------------------------------------
    log("=== SUMMARY ===")
    for k, v in results.items():
        log(f"  {k:38s} {v}")
    npass = sum(1 for v in results.values() if v == "PASS")
    log(f"TOTAL {npass}/{len(results)} PASS")
    return 0 if npass == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())

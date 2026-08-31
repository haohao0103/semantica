#!/usr/bin/env python3
"""
run_kg_enrich_single.py — 单图 KG 增强生产流程（hstore 单节点多图路由缺陷规避版）

背景（2026-08-31 实测）：本实例 hstore 单节点多图隔离不可靠（写任何非 kg_rag 图会
复制进 kg_rag，clear 也会跨图），因此 KG 增强流程收敛为 **单图使用**：
  - 唯一工作图 = kg_rag（实例中唯一隔离的图，也是 NL2SQL KG 所在图）
  - 读写/写回/对账/防护全部走 REST 图作用域端点，绕开 Gremlin 与 create_relationship
    对 PRIMARY_KEY 复合 id（如 "2:orders"）的误解析
  - GraphGuard 强制开启：目标图身份 + 端点归属 + 写后跨图泄漏检测 + 自动回滚

推理规则（贴合 NL2SQL 场景）：
  - lineage（Table->Table 表血缘）传递闭包 -> lineage_plus
  - synonym（Metric<->Metric 指标同义）对称 -> 反向 synonym 边

用法：
  .../hg-e2e/bin/python3 .../scripts/run_kg_enrich_single.py [--graph kg_rag]
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HG_DEV = "/Users/mac/Desktop/apache-code/hugegraph-dev"
SEMANTICA = os.path.join(HG_DEV, "semantica-hg-backend")
LOG_FILE = os.path.join(HG_DEV, "_out", "semantica-hg-backend", "logs",
                        "run_kg_enrich_single.log")
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, SEMANTICA)

class _Tee:
    def __init__(self, path):
        self.fh = open(path, "a", buffering=1, encoding="utf-8")
        self.fh.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} start ===\n")
    def write(self, s):
        sys.__stdout__.write(s); self.fh.write(s); return len(s)
    def flush(self):
        self.fh.flush()

sys.stdout = _Tee(LOG_FILE)  # type: ignore
sys.stderr = sys.stdout       # type: ignore

from semantica.graph_store.hugegraph_store import HugeGraphStore  # noqa: E402
from semantica.reasoning import DatalogReasoner                  # noqa: E402
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kg_enrich_guard import GraphGuard, GraphGuardError, GraphLeakError  # noqa: E402

META_PROPS = ["inferred", "rule_id", "derived_at", "base_predicate", "source_facts"]


def sanitize_pred(label: str) -> str:
    s = re.sub(r"[^a-z0-9_]", "_", str(label).lower())
    return s or "rel"


def make_const(orig: str, cmap: dict) -> str:
    s = re.sub(r"[^a-z0-9_]", "_", str(orig).lower())
    if not s or s[0].isdigit():
        s = "id_" + s
    if s not in cmap:
        cmap[s] = orig
    return s


def http_json(session, url):
    r = session.get(url, timeout=30)
    raw = r.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    r.raise_for_status()
    return json.loads(raw)


def rest_list_edges(store, label=None, limit=100000):
    url = f"{store.base_url}/graphs/{store.graph}/graph/edges?limit={limit}"
    if label:
        url += f"&label={label}"
    return http_json(store._session, url).get("edges") or []


def read_base_edges(store, labels):
    """REST 读基础边，保留端点 label（outVLabel/inVLabel），并构建 {vid: label} 映射。"""
    edges, vlabels = [], {}
    for lab in labels:
        for e in rest_list_edges(store, label=lab):
            ov, iv = str(e["outV"]), str(e["inV"])
            vlabels[ov] = e.get("outVLabel")
            vlabels[iv] = e.get("inVLabel")
            edges.append({"predicate": e["label"], "src": ov, "dst": iv})
    return edges, vlabels


def ensure_edge_label(url, graph, name, source, target, properties):
    """幂等创建边 label（带元数据属性）。"""
    schema_base = f"{url.rstrip('/')}/graphs/{graph}/schema"
    labels = urlopen_session(schema_base, "edgelabels")
    labels = labels if isinstance(labels, list) else labels.get("edgelabels", [])
    if any(l.get("name") == name for l in labels):
        return False
    payload = {"name": name, "source_label": source, "target_label": target,
               "frequency": "SINGLE", "sort_keys": [], "nullable_keys": [],
               "properties": properties}
    post_json(f"{schema_base}/edgelabels", payload)
    print(f"  [schema] 创建边 label {name} ({source}->{target})")
    return True


def urlopen_session(base, sub):
    import urllib.request as ur
    req = ur.Request(f"{base}/{sub}")
    with ur.urlopen(req, timeout=30) as r:
        raw = r.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)


def post_json(url, payload):
    import urllib.request as ur
    body = json.dumps(payload).encode()
    req = ur.Request(url, data=body, method="POST",
                     headers={"Content-Type": "application/json"})
    with ur.urlopen(req, timeout=30) as r:
        raw = r.read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw) if raw else {}


def derive(base_edges, hier_labels, sym_labels):
    """Datalog 推理：传递闭包 + 对称。返回 (new_facts, derived_keys, payloads, cmap)。"""
    cmap = {}
    r = DatalogReasoner()
    for e in base_edges:
        Ps = sanitize_pred(e["predicate"])
        r.add_fact(f"{Ps}({make_const(e['src'], cmap)},{make_const(e['dst'], cmap)})")
    rules = []
    for L in hier_labels:
        Ls = sanitize_pred(L)
        rules.append(f"{Ls}_plus(X,Y) :- {Ls}(X,Y).")
        rules.append(f"{Ls}_plus(X,Y) :- {Ls}(X,Z), {Ls}_plus(Z,Y).")
    for S in sym_labels:
        Ss = sanitize_pred(S)
        rules.append(f"{Ss}(Y,X) :- {Ss}(X,Y).")
    for rule in rules:
        r.add_rule(rule)

    base_tuples = {(sanitize_pred(e["predicate"]),
                    make_const(e["src"], cmap), make_const(e["dst"], cmap))
                   for e in base_edges}
    pat = re.compile(r"^([a-z0-9_]+)\(\s*([a-z0-9_]+)\s*,\s*([a-z0-9_]+)\s*\)$")
    new_facts = []
    for f in map(str, r.derive_all()):
        m = pat.match(f)
        if not m:
            continue
        t = (m.group(1), m.group(2), m.group(3))
        if t in base_tuples:
            continue
        new_facts.append(t)

    derived_at = datetime.now(timezone.utc).isoformat()
    derived_keys = set(new_facts)
    payloads = []
    for pred, a, b in new_facts:
        if pred.endswith("_plus"):
            base = pred[:-5]
            rule_id = f"transitive_closure:{base}"
        else:
            base, rule_id = pred, f"symmetric:{pred}"
        meta = {"inferred": True, "rule_id": rule_id,
                "derived_at": derived_at, "base_predicate": base,
                "source_facts": json.dumps([f"{pred}({a},{b})"], ensure_ascii=False)}
        payloads.append((cmap.get(a, a), cmap.get(b, b), pred, meta))
    return new_facts, derived_keys, payloads


def write_inferred(store, payloads, vlabels):
    """REST edges/batch 写回（带 outVLabel/inVLabel，PRIMARY_KEY id 安全）。
    lineage_plus 边带元数据属性（edgelabel 已声明）；synonym 反向边不带属性
    （synonym edgelabel 未声明元数据键，对账靠 base_keys 集合判断）。"""
    edges = []
    for s, d, pred, meta in payloads:
        if s not in vlabels or d not in vlabels:
            print(f"  [warn] 端点 label 缺失，跳过 {s}-[{pred}]->{d}")
            continue
        props = meta if pred.endswith("_plus") else {}
        edges.append({"label": pred, "outV": s, "outVLabel": vlabels[s],
                      "inV": d, "inVLabel": vlabels[d], "properties": props})
    if not edges:
        return []
    url = f"{store.base_url}/graphs/{store.graph}/graph/edges/batch"
    post_json(url, edges)
    for e in edges:
        print(f"  + wrote {e['outV']} -[{e['label']}]-> {e['inV']}  "
              f"({e['properties'].get('rule_id', 'symmetric') if e['properties'] else 'symmetric'})")
    return edges


def reconcile(store, derived_keys, sym_labels, base_edges):
    """幂等对账：删除 label=lineage_plus 且不在本轮推导集合的边；
    synonym 反向边（不在基础对且不在本轮推导）同样回收。"""
    derived = set(derived_keys)
    base_keys = {(sanitize_pred(e["predicate"]), _san(e["src"]), _san(e["dst"]))
                 for e in base_edges}
    removed = 0
    for e in rest_list_edges(store):
        lab, ov, iv = e["label"], str(e["outV"]), str(e["inV"])
        key = (sanitize_pred(lab), _san(ov), _san(iv))
        if lab.endswith("_plus") and key not in derived:
            try:
                store.delete_relationship(e["id"])
                removed += 1
                print(f"  - GC stale {lab}({ov},{iv})")
            except Exception as ex:
                print(f"  [warn] del {key}: {ex}")
        elif lab in sym_labels and key not in base_keys and key not in derived:
            try:
                store.delete_relationship(e["id"])
                removed += 1
                print(f"  - GC stale {lab}({ov},{iv})")
            except Exception as ex:
                print(f"  [warn] del {key}: {ex}")
    return removed


def _san(s: str) -> str:
    """与 make_const 完全一致的归一化（含数字开头 id_ 前缀），供对账 key 匹配。"""
    s = re.sub(r"[^a-z0-9_]", "_", str(s).lower())
    if not s or s[0].isdigit():
        s = "id_" + s
    return s


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8081)
    ap.add_argument("--graph", default="kg_rag",
                    help="单图模式下唯一工作图（默认 kg_rag）")
    ap.add_argument("--hier", default="lineage",
                    help="传递闭包关系（逗号分隔），默认 lineage（表血缘）")
    ap.add_argument("--sym", default="synonym",
                    help="对称关系（逗号分隔），默认 synonym（指标同义）")
    ap.add_argument("--no-guard", action="store_true",
                    help="关闭防护（不推荐，单图模式建议强制开启）")
    args = ap.parse_args()

    hier = [x.strip() for x in args.hier.split(",") if x.strip()]
    sym = [x.strip() for x in args.sym.split(",") if x.strip()]
    print(f"[cfg] 单图工作流: graph={args.graph} hier={hier} sym={sym} "
          f"guard={'OFF' if args.no_guard else 'ON'}")

    store = HugeGraphStore(host=args.host, port=args.port, graph=args.graph)
    store.connect()
    graphs = store._session.get(f"{store.base_url}/graphs", timeout=10).json().get("graphs", [])
    print(f"[env] 实例图: {graphs}")

    # ---- 1. 防护层：目标图身份校验 ----
    guard = None
    if not args.no_guard:
        others = [g for g in graphs if g != args.graph]
        guard = GraphGuard(args.host, args.port, args.graph,
                           other_graphs=others, allowlist=[args.graph])
        try:
            print("[guard] 写入前校验目标图...")
            guard.precheck(store)
        except GraphGuardError as e:
            print(f"[FATAL] 写入前校验未通过，中止: {e}")
            return 1

    # ---- 2. 读基础边（REST，带端点 label）----
    base, vlabels = read_base_edges(store, hier + sym)
    print(f"[read] 基础边 {len(base)} 条: " +
          ", ".join(f"{e['src']}-{e['predicate']}->{e['dst']}" for e in base[:12]))
    if not base:
        print("[FATAL] kg_rag 无基础边（先灌 NL2SQL 数据），中止")
        return 1

    # ---- 3. 建 lineage_plus 边 label（幂等）----
    for L in hier:
        ensure_edge_label(f"http://{args.host}:{args.port}", args.graph,
                          f"{sanitize_pred(L)}_plus", "Table", "Table", META_PROPS)

    # ---- 4. Datalog 推理 ----
    new_facts, keys, payloads = derive(base, hier, sym)
    print(f"[derive] 新推断边 {len(new_facts)} 条")

    # ---- 5. 防护：端点归属 + 快照 + 写回 + 写后泄漏检测 ----
    if guard:
        eps = sorted({p[0] for p in payloads} | {p[1] for p in payloads})
        try:
            guard.verify_endpoints(store, eps)
        except GraphGuardError as e:
            print(f"[FATAL] 端点归属校验失败，中止: {e}")
            return 1
        snap = guard.snapshot()
    else:
        snap = None

    written = write_inferred(store, payloads, vlabels)
    print(f"[write] 写回推断边 {len(written)} 条")

    if guard:
        try:
            guard.post_verify(snap, [{"kind": "edge", "id": None} for _ in written])
        except GraphLeakError as e:
            print(f"[FATAL] 写回检测到跨图泄漏，已回滚: {e}")
            return 1

    # ---- 6. 幂等对账 ----
    removed = reconcile(store, keys, sym, base)
    print(f"[reconcile] 撤回过期推断边 {removed} 条")

    print(f"[done] 单图增强完成: 新推断边 {len(new_facts)}，对账回收 {removed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

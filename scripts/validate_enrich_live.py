#!/usr/bin/env python3
"""
validate_enrich_live.py — 在真实 HugeGraph 上验证 Semantica Datalog 推理增强 KG 的端到端链路

验证目标（对应讨论：用 Semantica 推理隐含关系存入图谱）：
  1. 连真实 HugeGraph（端口 8081，隔离测试图 kg_enrich_poc，不污染生产图 kg_rag）
  2. REST 读取真实基础边（EDB）
  3. 真实 Semantica DatalogReasoner 推理隐含边（IDB：传递闭包 + 对称）
  4. 真实写回 HugeGraph（带 provenance 元数据：inferred/rule_id/derived_at/base_predicate/source_facts）
  5. 真实对账回收（列出推断边，删除本轮未再出现的过期推断边）
  6. 幂等演示：删除一条推断边后重跑，证明可恢复；修改基础图后重跑，证明过期推断边被 GC

关键约束（已踩坑）：
  - ⚠️ 单图模式（2026-08-31 起）：本实例 hstore 多图隔离不可靠，KG 增强一律走
    `run_kg_enrich_single.py`（唯一工作图 kg_rag）。本脚本会播种 e_a..e_y 测试顶点，
    **不要再指向 kg_rag 等含生产数据的图**（避免污染 NL2SQL KG）。
  - 本实例 /gremlin 端点 traversal-source 绑定不稳定（跨图污染/refCnt 错误），
    故读基边与对账一律走图作用域 REST `/graphs/{g}/graph/edges`，绕开 Gremlin。
  - create_relationship 内部用 Gremlin 解析顶点 label；把端点 id 传成 `id:label`
    复合形式可让 _split_compound 直接解析 label，跳过 Gremlin 查询。
  - sanitize_identifier 只接受小写 [a-z0-9_]，故测试图标签用 dependson/synonym/entity。
  - REST 边接口返回 gzip，需手动解压。

运行：
  /Users/mac/.workbuddy/binaries/python/envs/hg-e2e/bin/python3 \\
      /Users/mac/Desktop/apache-code/hugegraph-dev/semantica-hg-backend/scripts/validate_enrich_live.py
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HG_DEV = "/Users/mac/Desktop/apache-code/hugegraph-dev"
SEMANTICA = f"{HG_DEV}/semantica-hg-backend"
LOG_FILE = f"{HG_DEV}/_out/semantica-hg-backend/logs/validate_enrich_live.log"
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, SEMANTICA)

# ---- 日志双写（控制台 + 项目固定日志，遵循"所有跑数留日志"约定）----------
class _Tee:
    def __init__(self, path):
        self.fh = open(path, "a", buffering=1, encoding="utf-8")
        self.fh.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} start ===\n")
    def write(self, s):
        sys.__stdout__.write(s); self.fh.write(s); return len(s)
    def flush(self): self.fh.flush()
sys.stdout = _Tee(LOG_FILE)  # type: ignore
sys.stderr = sys.stdout       # type: ignore

from semantica.graph_store.hugegraph_store import HugeGraphStore  # noqa: E402
from semantica.reasoning import DatalogReasoner                  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kg_enrich_guard import GraphGuard, GraphGuardError, GraphLeakError  # noqa: E402


# ---------------- Semantica Datalog 推理核心（真实引擎）--------------------
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


def build_rules(hierarchy_labels, symmetric_labels):
    rules = []
    for L in hierarchy_labels:
        Ls = sanitize_pred(L)
        rules.append(f"{Ls}_plus(X,Y) :- {Ls}(X,Y).")
        rules.append(f"{Ls}_plus(X,Y) :- {Ls}(X,Z), {Ls}_plus(Z,Y).")
    for S in symmetric_labels:
        Ss = sanitize_pred(S)
        rules.append(f"{Ss}(Y,X) :- {Ss}(X,Y).")
    return rules


def enrich(base_edges, hierarchy_labels, symmetric_labels):
    cmap: dict = {}
    r = DatalogReasoner()
    for e in base_edges:
        Ps = sanitize_pred(e["predicate"])
        Cs = make_const(e["src"], cmap)
        Cd = make_const(e["dst"], cmap)
        r.add_fact(f"{Ps}({Cs},{Cd})")
    for rule in build_rules(hierarchy_labels, symmetric_labels):
        r.add_rule(rule)

    all_facts = [str(x) for x in r.derive_all()]
    base_tuples = {(sanitize_pred(e["predicate"]),
                    make_const(e["src"], cmap),
                    make_const(e["dst"], cmap)) for e in base_edges}

    new_facts = []
    pat = re.compile(r"^([a-z0-9_]+)\(\s*([a-z0-9_]+)\s*,\s*([a-z0-9_]+)\s*\)$")
    for f in all_facts:
        m = pat.match(f)
        if not m:
            continue
        pred, a, b = m.group(1), m.group(2), m.group(3)
        if (pred, a, b) in base_tuples:
            continue
        new_facts.append((pred, a, b))

    derived_at = datetime.now(timezone.utc).isoformat()
    derived_keys = {(p, a, b) for p, a, b in new_facts}
    return new_facts, derived_keys, derived_at, cmap


# ---------------- 真实 HugeGraph REST I/O（绕开 Gremlin）------------------
def http_get_json(session, url):
    r = session.get(url, timeout=30)
    raw = r.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    r.raise_for_status()
    return json.loads(raw)


def rest_list_edges(store, label=None, limit=10000):
    url = f"{store.base_url}/graphs/{store.graph}/graph/edges?limit={limit}"
    if label:
        url += f"&label={label}"
    data = http_get_json(store._session, url)
    return data.get("edges") or []


def clear_graph(store):
    # 先删边再删点，保证幂等重跑
    for e in rest_list_edges(store, limit=10000):
        try:
            store.delete_relationship(e["id"])
        except Exception as ex:
            print(f"  [warn] skip del edge {e.get('id')}: {ex}")
    # 列点删除（CUSTOMIZE_STRING 顶点 id 即逻辑 id）
    url = f"{store.base_url}/graphs/{store.graph}/graph/vertices?limit=10000"
    try:
        data = http_get_json(store._session, url)
        for v in (data.get("vertices") or []):
            vid = v.get("id")
            if vid is None:
                continue
            try:
                store._session.delete(
                    f"{store.base_url}/graphs/{store.graph}/graph/vertices/{vid}",
                    timeout=store.timeout,
                )
            except Exception as ex:
                print(f"  [warn] skip del vertex {vid}: {ex}")
    except Exception as ex:
        print(f"  [warn] list vertices failed: {ex}")


def seed(store, vertices, base_edges):
    """播种顶点+基边；返回可回滚的写入清单（顶点泄漏是实测风险，必须能回滚）。

    幂等：已存在的顶点不再创建、也不记入 created，避免回滚时误删已有数据。
    """
    created: list = []
    for vid in vertices:
        try:
            if store.get_node(vid) is not None:
                print(f"  [seed] 顶点 {vid} 已存在，跳过（不计入回滚清单）")
                continue
        except Exception:  # noqa: BLE001
            pass  # 查询失败则按"不存在"处理，进入创建分支
        try:
            n = store.create_node(labels=["entity"], properties={"id": vid, "name": vid})
            created.append({"kind": "vertex", "id": n.get("id", vid)})
        except Exception as e:  # noqa: BLE001
            print(f"  [warn] skip vertex {vid}: {e}")
    for (s, p, d) in base_edges:
        try:
            e = store.create_relationship(f"{s}:entity", f"{d}:entity", p,
                                          properties={"seed": True})
            created.append({"kind": "edge", "id": e.get("id")})
        except Exception as e:  # noqa: BLE001
            print(f"  [warn] skip edge {s}-[{p}]->{d}: {e}")
    return created


def write_inferred(store, new_facts, derived_at):
    written: list = []
    for pred, a, b in new_facts:
        if pred.endswith("_plus"):
            base = pred[:-5]
            rule_id = f"transitive_closure:{base}"
        else:
            rule_id = f"symmetric:{pred}"
        meta = {
            "inferred": True,
            "rule_id": rule_id,
            "derived_at": derived_at,
            "base_predicate": base if pred.endswith("_plus") else pred,
            "source_facts": json.dumps([f"{pred}({a},{b})"], ensure_ascii=False),
        }
        try:
            rel = store.create_relationship(f"{a}:entity", f"{b}:entity", pred, properties=meta)
            written.append({"kind": "edge", "id": rel.get("id")})
            print(f"  + wrote {a} -[{pred}]-> {b}  ({rule_id})")
        except Exception as ex:
            print(f"  [warn] skip write {a}-[{pred}]->{b}: {ex}")
    return written


def reconcile(store, derived_keys):
    removed = 0
    for e in rest_list_edges(store, limit=10000):
        if not e.get("properties", {}).get("inferred"):
            continue
        key = (e["label"], str(e["outV"]), str(e["inV"]))
        if key not in derived_keys:
            try:
                store.delete_relationship(e["id"])
                removed += 1
                print(f"  - GC stale inferred {key}")
            except Exception as ex:
                print(f"  [warn] skip del {key}: {ex}")
    return removed


def read_base(store, labels):
    edges = []
    for lab in labels:
        for e in rest_list_edges(store, label=lab, limit=10000):
            edges.append({
                "predicate": e["label"],
                "src": str(e["outV"]),
                "dst": str(e["inV"]),
            })
    return edges


# ---------------- 主流程 ---------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8081)
    ap.add_argument("--graph", default="kg_enrich_poc")
    ap.add_argument("--hier", default="dependson")
    ap.add_argument("--sym", default="synonym")
    ap.add_argument("--allowlist", default="",
                    help="允许写入的图白名单（逗号分隔）；为空则不限制。"
                         "强烈建议显式指定，防止误写生产图")
    ap.add_argument("--no-guard", action="store_true", help="关闭写入防护层")
    args = ap.parse_args()

    print(f"[setup] 真实 HugeGraph {args.host}:{args.port}/{args.graph}")
    store = HugeGraphStore(host=args.host, port=args.port, graph=args.graph)
    store.connect()
    print(f"[setup] 连接成功，现存图: {store._session.get(f'{store.base_url}/graphs').json()}")

    HIER = [x.strip() for x in args.hier.split(",") if x.strip()]
    SYM = [x.strip() for x in args.sym.split(",") if x.strip()]
    print(f"[config] 传递闭包: {HIER}  对称: {SYM}\n")

    # ---- 写入防护层：目标图身份校验 + 端点归属 + 写后跨图泄漏检测 ----
    guard = None
    if not args.no_guard:
        allow = [x.strip() for x in args.allowlist.split(",") if x.strip()] or None
        all_graphs = store._session.get(f"{store.base_url}/graphs", timeout=10).json().get("graphs", [])
        others = [g for g in all_graphs if g != args.graph]
        guard = GraphGuard(args.host, args.port, args.graph,
                           other_graphs=others, allowlist=allow)
        try:
            print("[guard] 写入前校验目标图...")
            guard.precheck(store)
        except GraphGuardError as e:
            print(f"[FATAL] 写入前校验未通过，已中止（未写入任何数据）: {e}")
            return 1

    # ---- Phase A: 播种 + 推理 + 写回 + 对账 ----
    print("="*60)
    print("Phase A — 播种基础图 → 真实推理 → 写回 → 对账")
    print("="*60)
    clear_graph(store)
    vertices = ["e_a", "e_b", "e_c", "e_d", "e_x", "e_y"]
    base = [
        ("e_a", "dependson", "e_b"),
        ("e_b", "dependson", "e_c"),
        ("e_c", "dependson", "e_d"),
        ("e_x", "synonym", "e_y"),
    ]
    # 播种也必须纳入防护：实测写入目标图的顶点会泄漏到别的图
    snap_seed = guard.snapshot() if guard else None
    seeded = seed(store, vertices, base)
    print(f"[A] 已播种顶点 {len(vertices)} 个，基础边 {len(base)} 条")
    if guard:
        try:
            guard.post_verify(snap_seed, seeded)
        except GraphLeakError as e:
            print(f"[FATAL] 播种阶段检测到跨图泄漏，已回滚并中止: {e}")
            return 1

    base_edges = read_base(store, HIER + SYM)
    print(f"[A] REST 读取基础边 {len(base_edges)} 条: "
          + ", ".join(f"{e['src']}-{e['predicate']}->{e['dst']}" for e in base_edges))

    new_facts, keys, derived_at, _ = enrich(base_edges, HIER, SYM)
    print(f"[A] Semantica Datalog 推导出推断边 {len(new_facts)} 条")
    if guard:
        # 端点归属校验：待建边的两端顶点必须真实存在于目标图
        eps = sorted({a for _, a, _ in new_facts} | {b for _, _, b in new_facts})
        try:
            guard.verify_endpoints(store, eps)
        except GraphGuardError as e:
            print(f"[FATAL] 端点归属校验失败，已中止（未写入推断边）: {e}")
            return 1
    snap_w = guard.snapshot() if guard else None
    written = write_inferred(store, new_facts, derived_at)
    print(f"[A] 真实写回推断边 {len(written)} 条")
    if guard:
        try:
            guard.post_verify(snap_w, written)
        except GraphLeakError as e:
            print(f"[FATAL] 写回阶段检测到跨图泄漏，已回滚并中止: {e}")
            return 1
    d = reconcile(store, keys)
    print(f"[A] 对账回收过期推断边 {d} 条 (首轮应为 0)")

    print(f"\n[A] 当前图全部边 ({len(rest_list_edges(store))} 条):")
    for e in sorted(rest_list_edges(store), key=lambda x: (str(x['label']), str(x['outV']), str(x['inV']))):
        inf = e.get('properties', {}).get('inferred')
        tag = " [推断]" if inf else " [基边]"
        print(f"    {e['outV']} -[{e['label']}]-> {e['inV']}{tag}")

    # ---- Phase B: 幂等 + 过期 GC 演示 ----
    print("\n" + "="*60)
    print("Phase B — 删除一条推断边后重跑(验证恢复) + 改基础图重跑(验证GC)")
    print("="*60)
    # B1: 删一条推断边 dependson_plus(e_a,e_c)，重跑应重建
    target = None
    for e in rest_list_edges(store, label="dependson_plus"):
        if e["outV"] == "e_a" and e["inV"] == "e_c":
            target = e["id"]; break
    if target:
        store.delete_relationship(target)
        print(f"[B1] 手动删除推断边 dependson_plus(e_a,e_c)，重跑前该边缺失")
    be = read_base(store, HIER + SYM)
    nf, k2, dt2, _ = enrich(be, HIER, SYM)
    snap_b1 = guard.snapshot() if guard else None
    w2 = write_inferred(store, nf, dt2)
    if guard:
        try:
            guard.post_verify(snap_b1, w2)
        except GraphLeakError as e:
            print(f"[FATAL] B1 写回检测到跨图泄漏，已回滚并中止: {e}")
            return 1
    d2 = reconcile(store, k2)
    print(f"[B1] 重跑写回 {len(w2)} 条（含被删边的重建），对账回收 {d2} 条")

    # B2: 删除基础边 e_b->e_c，原推断链 a->c/a->d/b->d 失效，应被 GC
    base_bc = None
    for e in rest_list_edges(store, label="dependson"):
        if e["outV"] == "e_b" and e["inV"] == "e_c":
            base_bc = e["id"]; break
    if base_bc:
        store.delete_relationship(base_bc)
        print(f"[B2] 删除基础边 dependson(e_b,e_c)，断裂 a-b-c-d 链")
    be2 = read_base(store, HIER + SYM)
    nf2, k3, dt3, _ = enrich(be2, HIER, SYM)
    snap_b2 = guard.snapshot() if guard else None
    w3 = write_inferred(store, nf2, dt3)
    if guard:
        try:
            guard.post_verify(snap_b2, w3)
        except GraphLeakError as e:
            print(f"[FATAL] B2 写回检测到跨图泄漏，已回滚并中止: {e}")
            return 1
    d3 = reconcile(store, k3)   # 上一轮 a->c/a->d/b->d 已失效 → 被 GC
    print(f"[B2] 重跑写回 {len(w3)} 条，对账回收过期推断边 {d3} 条 (应为 5)")

    print(f"\n[B] 最终图全部边 ({len(rest_list_edges(store))} 条):")
    for e in sorted(rest_list_edges(store), key=lambda x: (str(x['label']), str(x['outV']), str(x['inV']))):
        inf = e.get('properties', {}).get('inferred')
        tag = " [推断]" if inf else " [基边]"
        print(f"    {e['outV']} -[{e['label']}]-> {e['inV']}{tag}")

    print(f"\n[done] 验证完成。隔离测试图保留于 {args.graph} 供检查；"
          f"清理命令: curl -X DELETE 'http://{args.host}:{args.port}/graphs/{args.graph}"
          "?confirm_message=I'm sure to drop all data'")
    return 0


if __name__ == "__main__":
    sys.exit(main())

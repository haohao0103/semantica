#!/usr/bin/env python3
"""
infer_enrich_kg.py — 用 Semantica Datalog 推理增强 HugeGraph 知识图谱 (PoC)

设计目标（对应与用户的讨论）：
  - 把 HugeGraph 子图当成"基础事实(EDB)"，加载进 DatalogReasoner
  - 用策展规则（传递闭包 / 对称）推导出隐含关系(IDB)
  - 把推断边写回 HugeGraph，并打标 + 带 provenance 元数据
  - T+1 批量模式为主；支持撤回对账（删除上一轮产出、本轮未再出现的推断边）

两种运行模式：
  --mode live   连真实 HugeGraph，读有界子图 -> 推理 -> 写回 + 对账（需服务在线）
  --mode demo   用内存样本跑同一套推理+写回结构化逻辑（验证引擎，不连服务）

注意：本 PoC 面向 AI / GraphRAG / NL2SQL 场景的 KG。请勿指向风控 60 亿点边关系图谱。

⚠️ 单图模式（2026-08-31 起）：本实例 hstore 多图隔离不可靠，生产 KG 增强请用
`run_kg_enrich_single.py`（唯一工作图 kg_rag，REST 读写 + guard，兼容 PRIMARY_KEY
复合 id）。本脚本 --mode live 的写回走 create_relationship（_split_compound），
对 PRIMARY_KEY 复合 id（如 "2:orders"）会误解析，仅适用于 CUSTOMIZE_STRING 逻辑 id。

运行（demo，验证推理核心）：
  /Users/mac/.workbuddy/binaries/python/envs/hg-e2e/bin/python3 \\
      /Users/mac/Desktop/apache-code/hugegraph-dev/semantica-hg-backend/scripts/infer_enrich_kg.py \\
      --mode demo

运行（live，需 HugeGraph 在线）：
  .../hg-e2e/bin/python3 .../scripts/infer_enrich_kg.py --mode live \\
      --host 127.0.0.1 --port 8081 --graph kg_rag \\
      --base-labels dependsOn,lineage --symmetric-labels synonym,schema_knows \\
      --limit 2000

前置条件（本环境验证所得，务必遵守）：
  * HugeGraph 必须用 **JDK 11** 启动（默认 JDK 21 会让 Groovy 2.5.14 读不了 v65 class，
    Gremlin 全部报 "gremlin-groovy is not an available GremlinScriptEngine"）。
  * 勿用官方 start-hugegraph.sh（依赖 /bin/ps，本机被拒会误杀已启动的 JVM）；
    改用 `_out/hg_restart/start_server_detached.py`（设 JAVA_HOME=JDK11 并脱离会话）。
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

# ---- 路径 / 日志（遵循项目约定：日志落 _out/ 固定目录，不写 /tmp）----------
HG_DEV = "/Users/mac/Desktop/apache-code/hugegraph-dev"
SEMANTICA = os.path.join(HG_DEV, "semantica-hg-backend")
LOG_FILE = os.path.join(HG_DEV, "_out", "semantica-hg-backend", "logs", "infer_enrich_kg.log")
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, SEMANTICA)


class _Tee:
    """把 stdout/stderr 同时打到控制台和日志文件。"""

    def __init__(self, path: str):
        self.fh = open(path, "a", buffering=1, encoding="utf-8")
        self.fh.write(f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} start ===\n")

    def write(self, s: str) -> int:
        sys.__stdout__.write(s)
        self.fh.write(s)
        return len(s)

    def flush(self) -> None:
        self.fh.flush()


sys.stdout = _Tee(LOG_FILE)  # type: ignore
sys.stderr = sys.stdout  # type: ignore

# ---- 真实依赖（非模拟）----------------------------------------------------
from semantica.graph_store.hugegraph_store import HugeGraphStore  # noqa: E402
from semantica.reasoning import DatalogReasoner  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kg_enrich_guard import GraphGuard, GraphGuardError, GraphLeakError  # noqa: E402


# ---- 工具：把图标签 / 顶点 id 规范化为合法 Datalog 常量 --------------------
def sanitize_pred(label: str) -> str:
    s = re.sub(r"[^a-z0-9_]", "_", str(label).lower())
    return s or "rel"


def make_const(orig: str, cmap: dict) -> str:
    """返回一个合法 Datalog 常量，并记录 安全名->原始id 映射（写回时用原始 id）。"""
    s = re.sub(r"[^a-z0-9_]", "_", str(orig).lower())
    if not s or s[0].isdigit():
        s = "id_" + s
    if s not in cmap:
        cmap[s] = orig
    return s


def _san(s: str) -> str:
    """与 Datalog 常量归一化同款（用于对账时把 REST 原始 id 对齐到 derived_keys）。"""
    return re.sub(r"[^a-z0-9_]", "_", str(s).lower())


def build_rules(hierarchy_labels, symmetric_labels):
    """传递闭包（每类层级关系 -> *_plus）+ 对称关系。"""
    rules = []
    for L in hierarchy_labels:
        Ls = sanitize_pred(L)
        rules.append(f"{Ls}_plus(X,Y) :- {Ls}(X,Y).")
        rules.append(f"{Ls}_plus(X,Y) :- {Ls}(X,Z), {Ls}_plus(Z,Y).")
    for S in symmetric_labels:
        Ss = sanitize_pred(S)
        rules.append(f"{Ss}(Y,X) :- {Ss}(X,Y).")
    return rules


def enrich(base_edges, hierarchy_labels, symmetric_labels, writeback=None):
    """核心：EDB -> 规则 -> 推导 -> 写回。

    writeback(src,dst,pred,meta) 可选。不传时只做推理并返回 payloads，
    便于调用方在**写回前**插入防护校验（见 run_live 里的 GraphGuard）。
    返回 (new_facts, derived_keys, payloads)。
    """
    cmap: dict = {}
    r = DatalogReasoner()
    for e in base_edges:
        Ps = sanitize_pred(e["predicate"])
        Cs = make_const(e["src"], cmap)
        Cd = make_const(e["dst"], cmap)
        fact = f"{Ps}({Cs},{Cd})"
        r.add_fact(fact)

    for rule in build_rules(hierarchy_labels, symmetric_labels):
        r.add_rule(rule)

    all_facts = [str(x) for x in r.derive_all()]
    # derive_all 返回形如 "pred(a, b)"（逗号后有空格）；统一解析成元组比较，
    # 用元组集合排除基础事实，避免空格/格式差异导致的误判。
    base_tuples = set()
    for e in base_edges:
        base_tuples.add(
            (sanitize_pred(e["predicate"]), make_const(e["src"], cmap), make_const(e["dst"], cmap))
        )

    new_facts = []
    pat = re.compile(r"^([a-z0-9_]+)\(\s*([a-z0-9_]+)\s*,\s*([a-z0-9_]+)\s*\)$")
    for f in all_facts:
        m = pat.match(f)
        if not m:
            continue
        pred, a, b = m.group(1), m.group(2), m.group(3)
        if (pred, a, b) in base_tuples:  # 只保留"新推导"的边，排除基础事实本身
            continue
        new_facts.append((pred, a, b))

    derived_at = datetime.now(timezone.utc).isoformat()
    derived_keys = {(p, a, b) for p, a, b in new_facts}
    payloads: list = []

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
            "base_predicate": pred[:-5] if pred.endswith("_plus") else pred,
            "source_facts": json.dumps([f"{pred}({a},{b})"], ensure_ascii=False),
        }
        payloads.append((cmap.get(a, a), cmap.get(b, b), pred, meta))
        if writeback:
            writeback(*payloads[-1])

    return new_facts, derived_keys, payloads


# ---- live 模式：读 / 写 / 对账 ----------------------------------------------
def _rest_list_edges(store, labels=None, limit=10000):
    """REST 列举边（图作用域，gzip 需手动解压）；绕开 Gremlin（hstore 下不稳定）。"""
    url = f"{store.base_url}/graphs/{store.graph}/graph/edges?limit={int(limit)}"
    if labels:
        url += "&label=" + ",".join(labels)
    r = store._session.get(url, timeout=30)
    raw = r.content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw).get("edges") or []


def read_base_edges_live(store, base_labels, limit):
    """REST 读取基础边（与 validate 同款，hstore 下比 Gremlin 更稳定）。

    HugeGraph 的 edges 接口 label 过滤只接受单个标签，故逐标签查询再合并。
    传入的标签列表可能含重复（hierarchy 与 symmetric 重叠），用 dict 去重保序。
    """
    edges = []
    for lab in dict.fromkeys(base_labels):
        for e in _rest_list_edges(store, labels=[lab], limit=limit):
            edges.append(
                {
                    "predicate": e["label"],
                    "src": str(e["outV"]),
                    "dst": str(e["inV"]),
                    "_id": e.get("id"),
                }
            )
    return edges


def _compound_id(store, vid):
    """把顶点 id 拼成 `id:label` 复合形式，使 create_relationship 跳过 Gremlin 查 label。

    关键：本实例 hstore 的 Gremlin traversal-source 绑定不稳定（跨图污染/refCnt 错误），
    对**纯 id** 调 create_relationship 会触发 _lookup_vertex_label 走 Gremlin，
    进而把边写到错误的图（实测曾泄漏到 kg_enrich_live）。改用复合 id 后完全绕开 Gremlin，
    与 validate 的稳妥写法一致。label 通过 REST GET（图作用域，可靠）解析，不碰 Gremlin。
    """
    vid = str(vid)
    if ":" in vid:
        return vid
    try:
        node = store.get_node(vid)
        label = node.get("label") if isinstance(node, dict) else None
    except Exception:  # noqa: BLE001
        label = None
    if not label:
        return vid
    return f"{vid}:{label}"


def live_writeback(store, orig_src, orig_dst, pred, meta):
    try:
        s = _compound_id(store, orig_src)
        d = _compound_id(store, orig_dst)
        rel = store.create_relationship(s, d, pred, properties=meta)
        print(f"  + wrote {orig_src} -[{pred}]-> {orig_dst}  ({meta['rule_id']})")
        return rel
    except Exception as e:  # 跳过悬空引用等，计入跳过
        print(f"  [warn] skip write {orig_src}-[{pred}]->{orig_dst}: {e}")
        return None


def reconcile_live(store, derived_keys):
    """撤回：删除上一轮产出、本轮未再出现的推断边（Datalog 单调，需主动回收）。

    用 REST 列举推断边（绕开 Gremlin）；key 用 _san 归一化以对齐 enrich 产出的
    derived_keys（REST 返回原始 id 含 ':'，Datalog 常量中已被归一化为 '_'）。
    """
    removed = 0
    for e in _rest_list_edges(store, limit=10000):
        if not e.get("properties", {}).get("inferred"):
            continue
        key = (_san(e["label"]), _san(e["outV"]), _san(e["inV"]))
        if key not in derived_keys:
            try:
                store.delete_relationship(e["id"])
                removed += 1
            except Exception as ex:  # noqa: BLE001
                print(f"  [warn] skip delete {key}: {ex}")
    return removed


# ---- demo 模式：内存样本，验证真实推理引擎 ---------------------------------
def demo_base_edges():
    # 组织层级 + 同义关系 的样本
    return [
        {"predicate": "belongs_to", "src": "alice", "dst": "team_a"},
        {"predicate": "belongs_to", "src": "bob", "dst": "team_a"},
        {"predicate": "belongs_to", "src": "team_a", "dst": "dept_x"},
        {"predicate": "belongs_to", "src": "dept_x", "dst": "org_y"},
        {"predicate": "related_to", "src": "alice", "dst": "bob"},
    ]


def run_demo(hierarchy_labels, symmetric_labels):
    print("[demo] 用内存样本跑真实 DatalogReasoner（不连 HugeGraph）")
    collected = []

    def wb(src, dst, pred, meta):
        collected.append((src, dst, pred, meta))
        print(f"  + inferred {src} -[{pred}]-> {dst}  ({meta['rule_id']})")

    new_facts, _, _ = enrich(
        demo_base_edges(), hierarchy_labels, symmetric_labels, wb
    )
    print(f"\n[demo] 基础边 {len(demo_base_edges())} 条 -> 新推断边 {len(new_facts)} 条")
    print("[demo] 写回载荷样例:", json.dumps(collected[0], ensure_ascii=False) if collected else "无")
    return len(new_facts)


def run_live(args, hierarchy_labels, symmetric_labels):
    print(f"[live] 连接 HugeGraph {args.host}:{args.port}/{args.graph}")
    store = HugeGraphStore(host=args.host, port=args.port, graph=args.graph)
    store.connect()  # 不可达会抛 ProcessingError
    # 注意：传入标签列表（hierarchy + symmetric），勿传逗号字符串（会按字符迭代）
    base = read_base_edges_live(store, hierarchy_labels + symmetric_labels, args.limit)
    print(f"[live] 读取基础边 {len(base)} 条")
    if not base:
        print("[live] 未读到任何基础边（检查 --base-labels 是否存在于该图）")
        return 0

    # ---- 写入防护层：目标图身份校验 + 端点归属 + 写后跨图泄漏检测 ----
    guard = None
    if not args.no_guard:
        allow = [x.strip() for x in (args.allowlist or "").split(",") if x.strip()] or None
        all_graphs = (
            store._session.get(f"{store.base_url}/graphs", timeout=10)
            .json()
            .get("graphs", [])
        )
        others = [g for g in all_graphs if g != args.graph]
        guard = GraphGuard(
            args.host, args.port, args.graph,
            other_graphs=others, allowlist=allow,
        )
        try:
            print("[guard] 写入前校验目标图...")
            guard.precheck(store)
        except GraphGuardError as e:
            print(f"[FATAL] 写入前校验未通过，已中止（未写入任何数据）: {e}")
            return 1

    # 推理（不写回，先收集 payloads 以供防护层在写回前校验）
    new_facts, keys, payloads = enrich(base, hierarchy_labels, symmetric_labels)
    print(f"[live] 推导出推断边 {len(new_facts)} 条")

    if guard:
        # 端点归属校验：待建边的两端顶点必须真实存在于目标图，防止跨图解析
        eps = sorted({p[0] for p in payloads} | {p[1] for p in payloads})
        try:
            guard.verify_endpoints(store, eps)
        except GraphGuardError as e:
            print(f"[FATAL] 端点归属校验失败，已中止（未写入推断边）: {e}")
            return 1

    # 写回（每条都记录可回滚 id），写后做跨图泄漏检测
    snap = guard.snapshot() if guard else None
    written = []
    for orig_src, orig_dst, pred, meta in payloads:
        rel = live_writeback(store, orig_src, orig_dst, pred, meta)
        if rel is not None:
            rid = rel.get("id") if isinstance(rel, dict) else rel
            written.append({"kind": "edge", "id": rid})
    print(f"[live] 真实写回推断边 {len(written)} 条")

    if guard:
        try:
            guard.post_verify(snap, written)
        except GraphLeakError as e:
            print(f"[FATAL] 写回阶段检测到跨图泄漏，已回滚并中止: {e}")
            return 1

    removed = reconcile_live(store, keys)
    print(f"[live] 新推断边 {len(new_facts)} 条；撤回过期推断边 {removed} 条")
    return len(new_facts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["live", "demo"], default="demo")
    ap.add_argument("--host", default="127.0.0.1")
    # 本环境 HugeGraph REST 实际监听 8081（8080 无服务/返回 502）。
    ap.add_argument("--port", type=int, default=8081)
    ap.add_argument("--graph", default="kg_rag")
    ap.add_argument("--base-labels", default="belongs_to,part_of,parent_of")
    ap.add_argument("--symmetric-labels", default="related_to,synonym")
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--allowlist", default="",
                    help="允许写入的图白名单（逗号分隔）；为空则不限制。"
                         "强烈建议显式指定，防止误写生产图")
    ap.add_argument("--no-guard", action="store_true", help="关闭写入防护层")
    args = ap.parse_args()

    hierarchy_labels = [x.strip() for x in args.base_labels.split(",") if x.strip()]
    symmetric_labels = [x.strip() for x in args.symmetric_labels.split(",") if x.strip()]

    print(f"hierarchy(传递闭包): {hierarchy_labels}")
    print(f"symmetric(对称): {symmetric_labels}\n")

    try:
        if args.mode == "live":
            return 0 if run_live(args, hierarchy_labels, symmetric_labels) >= 0 else 1
        else:
            run_demo(hierarchy_labels, symmetric_labels)
            return 0
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

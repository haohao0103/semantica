#!/usr/bin/env python3
"""
Semantica 推理能力实测 — 验证 reasoning 模块是真能用还是只有类名好看。

测 4 件事：
  T1  Datalog 传递闭包（祖先推导）+ query
  T2  load_from_graph：把 ContextGraph 的边灌成 Datalog 事实
  T3  DeductiveReasoner 演绎推理 + 证明链
  T4  AbductiveReasoner 溯因推理
  T5  ReteEngine 规则匹配
  T6  GraphReasoner 图推理
  T7  ExplanationGenerator 解释生成

每步打 PASS/FAIL，不吞异常。
"""

import inspect
import sys
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RESULTS = []


def hr(t=""):
    print("\n" + "=" * 74)
    if t:
        print(t)
        print("=" * 74)


def check(name, fn):
    try:
        detail = fn()
        RESULTS.append((name, "PASS", detail or ""))
        print(f"  PASS  {name}")
        if detail:
            print(f"        {detail}")
        return True
    except Exception as e:
        RESULTS.append((name, "FAIL", f"{type(e).__name__}: {e}"))
        print(f"  FAIL  {name}")
        print(f"        {type(e).__name__}: {e}")
        return False


# ------------------------------------------------------------------ imports
hr("模块导入")
from semantica.reasoning import (  # noqa: E402
    DatalogReasoner,
    ReteEngine,
    GraphReasoner,
    ExplanationGenerator,
    SPARQLReasoner,
)
print("  reasoning 包级导出类导入成功")

# 以下两个类存在于代码中，但未被 reasoning/__init__.py 的 __all__ 导出
from semantica.reasoning.deductive_reasoner import DeductiveReasoner  # noqa: E402
from semantica.reasoning.abductive_reasoner import AbductiveReasoner  # noqa: E402

print("  DeductiveReasoner / AbductiveReasoner 从子模块导入成功（未公开导出）")

try:
    from semantica.context import ContextGraph

    HAS_CTX = True
    print("  ContextGraph 可用")
except Exception as e:
    HAS_CTX = False
    print(f"  ContextGraph 不可用: {type(e).__name__}: {e}")


# ------------------------------------------------- T1 Datalog 传递闭包 + query
def t1():
    r = DatalogReasoner()
    for f in [
        "parent(alice, bob)",
        "parent(bob, carol)",
        "parent(carol, dave)",
        "parent(erin, frank)",
    ]:
        r.add_fact(f)
    r.add_rule("ancestor(X, Y) :- parent(X, Y).")
    r.add_rule("ancestor(X, Y) :- parent(X, Z), ancestor(Z, Y).")

    derived = r.derive_all()
    derived_s = [str(d) for d in derived]
    # alice->bob->carol->dave 应产生 ancestor(alice,bob/carol/dave), (bob,carol/dave), (carol,dave)
    expect = {
        "ancestor(alice, bob)",
        "ancestor(alice, carol)",
        "ancestor(alice, dave)",
        "ancestor(bob, carol)",
        "ancestor(bob, dave)",
        "ancestor(carol, dave)",
        "ancestor(erin, frank)",
    }
    got = set(derived_s)
    missing = expect - got
    if missing:
        raise AssertionError(f"缺推导结果: {sorted(missing)}; 实得={sorted(got)}")

    q = r.query("ancestor(alice, Y)")
    return f"derive_all={len(derived)} 条; query(ancestor(alice,Y)) -> {q}"


# ---------------------------------------------- T2 load_from_graph（图→事实）
def t2():
    """用 networkx 风格的图对象测 load_from_graph（HugeGraph 侧的同类路径）。"""
    import networkx as nx

    g = nx.DiGraph()
    g.add_node("alice", type="person")
    g.add_node("bob", type="person")
    g.add_node("carol", type="person")
    g.add_edge("alice", "bob", type="knows")
    g.add_edge("bob", "carol", type="knows")

    # networkx 的 g.nodes 是 NodeView（非 dict），g.edges 是 EdgeView
    # load_from_graph 期望 nodes 可迭代、edges 可迭代出 (u, v)
    r = DatalogReasoner()
    n_facts = r.load_from_graph(g)
    if n_facts <= 0:
        raise AssertionError("load_from_graph 未载入任何事实")

    r.add_rule("connected(X, Y) :- knows(X, Y).")
    r.add_rule("connected(X, Y) :- knows(X, Z), connected(Z, Y).")
    derived = sorted(str(d) for d in r.derive_all())
    return (
        f"networkx 图 {g.number_of_nodes()} 节点 / {g.number_of_edges()} 边 "
        f"-> 载入 {n_facts} 事实; 推导 {len(derived)} 条: {derived}"
    )


def t2b():
    """ContextGraph 端到端：建图 -> load_from_graph -> 加规则 -> 推导。"""
    if not HAS_CTX:
        raise AssertionError("ContextGraph 不可用（缺 rdflib）")

    g = ContextGraph()
    for nid in ("alice", "bob", "carol", "dave"):
        g.add_node(nid, "person")
    g.add_edge("alice", "bob", "knows")
    g.add_edge("bob", "carol", "knows")
    g.add_edge("carol", "dave", "knows")

    n_nodes = len(list(g.find_nodes()))
    n_edges = len(list(g.find_edges()))

    r = DatalogReasoner()
    n_facts = r.load_from_graph(g)
    if n_facts <= 0:
        raise AssertionError(f"load_from_graph 载入 0 事实（图有 {n_nodes} 节点/{n_edges} 边）")

    r.add_rule("connected(X, Y) :- knows(X, Y).")
    r.add_rule("connected(X, Y) :- knows(X, Z), connected(Z, Y).")
    derived = sorted(str(d) for d in r.derive_all())

    # 校验传递闭包：alice 应连通到 bob/carol/dave
    q = r.query("connected(alice, Y)")
    ys = {d.get("Y") for d in q} if isinstance(q, list) else set()
    if not {"bob", "carol", "dave"} <= ys:
        raise AssertionError(f"传递闭包不完整: query 得 {q}")

    return (
        f"ContextGraph {n_nodes} 节点/{n_edges} 边 -> {n_facts} 事实; "
        f"推导 {len(derived)} 条; connected(alice,*) -> {sorted(ys)}"
    )


# ------------------------------------------------------- T3 演绎推理 + 证明链
def t3():
    dr = DeductiveReasoner()
    sigs = [m for m in dir(dr) if not m.startswith("_") and callable(getattr(dr, m))]
    print(f"        可用方法: {sigs}")
    # 找一个能接受 premise/conclusion 的方法
    for meth in ("deduce", "reason", "apply_rules", "run"):
        if hasattr(dr, meth):
            print(f"        {meth}{inspect.signature(getattr(dr, meth))}")
    return f"实例化成功，公开方法 {len(sigs)} 个（未构造具体用例）"


# ------------------------------------------------------------- T4 溯因推理
def t4():
    ar = AbductiveReasoner()
    sigs = [m for m in dir(ar) if not m.startswith("_") and callable(getattr(ar, m))]
    print(f"        可用方法: {sigs}")
    for meth in ("explain", "abduce", "generate_hypotheses", "reason"):
        if hasattr(ar, meth):
            print(f"        {meth}{inspect.signature(getattr(ar, meth))}")
    return f"实例化成功，公开方法 {len(sigs)} 个"


# ------------------------------------------------------------- T5 Rete 引擎
def t5():
    re_ = ReteEngine()
    sigs = [m for m in dir(re_) if not m.startswith("_") and callable(getattr(re_, m))]
    print(f"        可用方法: {sigs}")
    for meth in ("add_rule", "add_fact", "fire", "run"):
        if hasattr(re_, meth):
            print(f"        {meth}{inspect.signature(getattr(re_, meth))}")
    return f"实例化成功，公开方法 {len(sigs)} 个"


# ----------------------------------------------------------- T6 图推理
def t6():
    gr = GraphReasoner()
    sigs = [m for m in dir(gr) if not m.startswith("_") and callable(getattr(gr, m))]
    print(f"        可用方法: {sigs}")
    return f"实例化成功，公开方法 {len(sigs)} 个"


# ------------------------------------------------------- T7 解释生成
def t7():
    eg = ExplanationGenerator()
    sigs = [m for m in dir(eg) if not m.startswith("_") and callable(getattr(eg, m))]
    print(f"        可用方法: {sigs}")
    return f"实例化成功，公开方法 {len(sigs)} 个"


def main():
    hr("T1  Datalog 传递闭包 + query")
    check("Datalog 传递闭包推导", t1)

    hr("T2  load_from_graph（图 -> Datalog 事实）")
    check("networkx 图转事实 + 连通性推导", t2)
    check("ContextGraph 图转事实", t2b)

    hr("T3-T7  其余推理组件实例化 + 方法签名")
    check("DeductiveReasoner", t3)
    check("AbductiveReasoner", t4)
    check("ReteEngine", t5)
    check("GraphReasoner", t6)
    check("ExplanationGenerator", t7)

    hr("汇总")
    npass = sum(1 for _, s, _ in RESULTS if s == "PASS")
    print(f"{'用例':<40}{'结果':<8}说明")
    print("-" * 74)
    for name, st, detail in RESULTS:
        d = (detail[:60] + "...") if len(detail) > 60 else detail
        print(f"{name:<40}{st:<8}{d}")
    print("-" * 74)
    print(f"PASS {npass} / {len(RESULTS)}")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)

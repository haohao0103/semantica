#!/usr/bin/env python3
"""
PoC：货拉拉司机团伙欺诈识别
=================================

链路：造数据 -> 写 HugeGraph -> 【从 HugeGraph 读回】-> 桥接成 Datalog 事实
      -> Semantica 确定性推理 -> 风险名单

关键约束：喂给推理器的每一条事实都必须来自 HugeGraph 的读取结果，
          绝不使用内存里的原始数据 —— 否则 PoC 不成立。

场景设计
--------
黑产团伙批量注册司机号，共享设备 / 手机号 / 银行卡刷单套补贴。
30 个司机：d01-d09 是一个被刻意拆散的 9 人团伙（单看共享设备只能看到碎片），
d10-d30 是正常司机。d09 是已确认的黑名单账号。

    d01,d02,d03  共享 dev_a / ph_a / card_a
    d04,d05      共享 dev_b / card_b
    d06,d07,d08,d09 共享 dev_c
    d03 -ph_x-  d04      （桥接 1：把 A、B 连起来）
    d05 -card_x- d06     （桥接 2：把 B、C 连起来）
    => d01..d09 全连通，形成 9 人团伙

预期
----
    1 跳共享（传统规则引擎）: 只看到 3 个碎片 {01-03} {04,05} {06-09}
    Datalog 传递闭包        : 一次性捞出 9 人完整团伙
    风险传播（d09 黑名单）   : d01-d08 全部标记高危
    误报                    : d10-d30 一个都不该被标记
"""

import sys
import traceback
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from semantica.graph_store import HugeGraphStore  # noqa: E402
from semantica.reasoning import DatalogReasoner  # noqa: E402

GRAPH = "hugegraph"
HOST, PORT = "localhost", 8080

# 顶点 / 边 label 一律带 fraud_ 前缀，避免污染库里其它数据
L_DRIVER, L_DEVICE = "fraud_driver", "fraud_device"
L_PHONE, L_CARD = "fraud_phone", "fraud_card"
E_DEVICE, E_PHONE, E_CARD = (
    "fraud_used_device",
    "fraud_used_phone",
    "fraud_owns_card",
)

CHECKS = []


def hr(t=""):
    print("\n" + "=" * 76)
    if t:
        print(t)
        print("=" * 76)


def expect(cond, name, detail=""):
    CHECKS.append((name, bool(cond), detail))
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"\n        {detail}" if detail else ""))
    return bool(cond)


# --------------------------------------------------------------- 1. 造数据
def build_dataset():
    """返回 (drivers, devices, phones, cards, edges)。edges = [(src, tgt, label)]"""
    drivers = [f"d{i:02d}" for i in range(1, 31)]

    devices, phones, cards = set(), set(), set()
    edges = []

    def link(d, dev=None, ph=None, card=None):
        if dev:
            devices.add(dev)
            edges.append((d, dev, E_DEVICE))
        if ph:
            phones.add(ph)
            edges.append((d, ph, E_PHONE))
        if card:
            cards.add(card)
            edges.append((d, card, E_CARD))

    # 团伙 A：d01-d03 共享 dev_a / ph_a / card_a
    for d in ("d01", "d02", "d03"):
        link(d, dev="dev_a", ph="ph_a", card="card_a")
    # 桥接 1：d03 与 d04 共享手机号 ph_x
    link("d03", ph="ph_x")

    # 团伙 B：d04-d05 共享 dev_b / card_b；d04 复用 ph_x
    link("d04", dev="dev_b", ph="ph_x", card="card_b")
    # 桥接 2：d05 与 d06 共享银行卡 card_x
    link("d05", dev="dev_b", ph="ph_n05", card="card_b")
    link("d05", card="card_x")

    # 团伙 C：d06-d09 共享 dev_c；d06 复用 card_x
    link("d06", dev="dev_c", ph="ph_n06", card="card_x")
    for d in ("d07", "d08", "d09"):
        link(d, dev="dev_c", ph=f"ph_n{d[1:]}", card=f"card_n{d[1:]}"  )

    # 正常司机 d10-d30：设备/手机/卡全独立
    for i in range(10, 31):
        d = f"d{i:02d}"
        sfx = d[1:]
        link(d, dev=f"dev_n{sfx}", ph=f"ph_n{sfx}", card=f"card_n{sfx}")

    return drivers, sorted(devices), sorted(phones), sorted(cards), edges


# -------------------------------------------------------- 2. 写入 HugeGraph
def write_to_hugegraph(store, drivers, devices, phones, cards, edges):
    hr("S2  写入 HugeGraph")
    # 先清掉上一轮的 fraud_ 数据，保证可重复跑
    try:
        store.execute_query(
            f"{ALIAS}.V().hasLabel('{L_DRIVER}','{L_DEVICE}','{L_PHONE}','{L_CARD}').drop()"
        )
        print("  已清理上一轮 fraud_ 数据")
    except Exception as e:
        print(f"  清理失败（可忽略）: {type(e).__name__}: {e}")

    ok_n = 0
    for label, ids in (
        (L_DRIVER, drivers),
        (L_DEVICE, devices),
        (L_PHONE, phones),
        (L_CARD, cards),
    ):
        for i in ids:
            try:
                store.create_node([label], {"id": f"fraud_{i}", "name": i})
                ok_n += 1
            except Exception as e:
                print(f"    写顶点失败 {i}: {type(e).__name__}: {e}")
    print(f"  顶点写入成功 {ok_n} / {len(drivers) + len(devices) + len(phones) + len(cards)}")

    ok_e = 0
    for s, t, lab in edges:
        try:
            store.create_relationship(f"fraud_{s}", f"fraud_{t}", lab, {})
            ok_e += 1
        except Exception as e:
            print(f"    写边失败 {s}->{t} [{lab}]: {type(e).__name__}: {e}")
    print(f"  边写入成功 {ok_e} / {len(edges)}")
    return ok_n, ok_e


# ------------------------------------- 3. 从 HugeGraph 读回（多策略 Gremlin）
def read_edges_from_hugegraph(store):
    """
    只信 HugeGraph 的返回。按优先级尝试几种 Gremlin 写法，取第一个成功的。
    """
    hr("S3  从 HugeGraph 读回全图（Gremlin）")
    strategies = [
        (
            "project",
            f"{ALIAS}.E().project('s','t','l').by(outV().id()).by(inV().id()).by(label())",
        ),
        (
            "elementMap",
            f"{ALIAS}.E().elementMap()",
        ),
        (
            "valueMap+path",
            f"{ALIAS}.V().outE().as('e').outV().id().as('s')"
            f".select('e').inV().id().as('t').select('e').label().as('l')"
            f".select('s','t','l')",
        ),
    ]
    last_err = None
    for name, script in strategies:
        try:
            res = store.execute_query(script)
            recs = res.get("records", res if isinstance(res, list) else [])
            if not recs:
                print(f"  策略[{name}] 返回 0 条，换下一个")
                continue
            parsed = []
            for r in recs:
                s, t, l = None, None, None
                if name == "project":
                    s, t, l = r.get("s"), r.get("t"), r.get("l")
                elif name == "elementMap":
                    s, t = r.get("outV"), r.get("inV")
                    l = r.get("label") or r.get("~label")
                    if isinstance(s, dict):
                        s = s.get("id") or s.get("@value")
                    if isinstance(t, dict):
                        t = t.get("id") or t.get("@value")
                else:
                    s, t, l = r.get("s"), r.get("t"), r.get("l")
                if s and t and l:
                    parsed.append((str(s), str(t), str(l)))
            if parsed:
                print(f"  策略[{name}] 成功，读回 {len(parsed)} 条边")
                return parsed
            print(f"  策略[{name}] 解析出 0 条，换下一个")
        except Exception as e:
            last_err = e
            print(f"  策略[{name}] 异常: {type(e).__name__}: {str(e)[:120]}")

    raise RuntimeError(f"所有 Gremlin 策略均失败: {last_err}")


# ------------------------------------------------- 4. 桥接：HG 边 -> Datalog
EDGE_TO_PRED = {
    E_DEVICE: "used_device",
    E_PHONE: "used_phone",
    E_CARD: "owns_card",
}


def bridge_to_datalog(hg_edges, blacklist=("d09",)):
    """
    HugeGraph 顶点 id 形如 fraud_d01；边 label 形如 fraud_used_device。
    归一成 Datalog 事实（Datalog 要求：常量小写开头，大写视为变量）。
    """
    hr("S4  桥接：HugeGraph 边 -> Datalog 事实")

    def strip(vid):
        s = str(vid)
        return s[len("fraud_"):] if s.startswith("fraud_") else s

    facts = []
    n_skipped = 0
    for s, t, lab in hg_edges:
        pred = EDGE_TO_PRED.get(lab)
        if not pred:
            n_skipped += 1
            continue
        facts.append(f"{pred}({strip(s)}, {strip(t)})")

    for b in blacklist:
        facts.append(f"blacklisted({strip(b)})")

    print(f"  生成事实 {len(facts)} 条（跳过未知边类型 {n_skipped} 条）")
    kinds = defaultdict(int)
    for f in facts:
        kinds[f.split("(")[0]] += 1
    print(f"  谓词分布: {dict(kinds)}")

    r = DatalogReasoner()
    for f in facts:
        r.add_fact(f)
    return r, facts


# ------------------------------------------------------------- 5. 规则推理
RULES = [
    "shared_device(X, Y) :- used_device(X, D), used_device(Y, D).",
    "shared_phone(X, Y) :- used_phone(X, P), used_phone(Y, P).",
    "shared_card(X, Y) :- owns_card(X, C), owns_card(Y, C).",
    "linked(X, Y) :- shared_device(X, Y).",
    "linked(X, Y) :- shared_phone(X, Y).",
    "linked(X, Y) :- shared_card(X, Y).",
    "same_gang(X, Y) :- linked(X, Y).",
    "same_gang(X, Y) :- linked(X, Z), same_gang(Z, Y).",
    "high_risk(X) :- same_gang(X, Y), blacklisted(Y).",
]


def run_reasoning(store):
    hr("S5  Semantica 确定性推理（Datalog）")

    # ---------- 基线 1：只用「共享设备」的 1 跳规则（传统规则引擎水平）
    hg_edges = read_edges_from_hugegraph(store)
    r1, facts = bridge_to_datalog(hg_edges)
    r1.add_rule("shared_device(X, Y) :- used_device(X, D), used_device(Y, D).")
    r1.add_rule("one_hop(X, Y) :- shared_device(X, Y).")
    base = [str(d) for d in r1.derive_all() if str(d).startswith("one_hop(")]
    clusters_1hop = build_clusters(
        [(t.split("(")[1].rstrip(")").split(", ")[0],
          t.split("(")[1].rstrip(")").split(", ")[1]) for t in base]
    )
    big_1hop = [c for c in clusters_1hop if len(c) > 1]
    print(f"\n  [基线] 仅共享设备 1 跳 -> {len(big_1hop)} 个连通簇: "
          f"{[sorted(c) for c in big_1hop]}")

    # ---------- 主方案：多规则 + 传递闭包
    r2 = DatalogReasoner()
    for f in facts:
        r2.add_fact(f)
    for rule in RULES:
        r2.add_rule(rule)
    derived = [str(d) for d in r2.derive_all()]

    gang_pairs = [
        t[t.index("(") + 1:t.rindex(")")].split(", ")
        for t in derived if t.startswith("same_gang(")
    ]
    gang_pairs = [(a, b) for a, b in gang_pairs if a != b]
    clusters = build_clusters(gang_pairs)
    big = [c for c in clusters if len(c) > 1]

    high_risk = sorted({
        t[t.index("(") + 1:t.rindex(")")]
        for t in derived if t.startswith("high_risk(")
    })

    print(f"  推导总数 {len(derived)} 条")
    print(f"  same_gang 非自反对 {len(gang_pairs)} 组")
    print(f"  团伙簇: {[sorted(c) for c in big]}")
    print(f"  高危名单({len(high_risk)}): {high_risk}")

    return facts, derived, big, high_risk, big_1hop


def build_clusters(pairs):
    """并查集求连通分量"""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for a, b in pairs:
        union(a, b)

    groups = defaultdict(set)
    for x in list(parent):
        groups[find(x)].add(x)
    return [g for g in groups.values() if len(g) > 1]


# --------------------------------------------------- 6. 对照：Gremlin 多跳
def gremlin_baseline(store):
    hr("S7  对照组：直接用 Gremlin 查多跳邻居")
    out = {}
    for depth in (2, 3):
        try:
            recs = store.execute_query(
                f"{ALIAS}.V('fraud_d09')"
                f".repeat(__.both().simplePath()).times({depth})"
                f".dedup().id()"
            )
            ids = [str(x) for x in recs.get("records", [])]
            out[depth] = sorted(ids)
            print(f"  d09 的 {depth} 跳内节点({len(ids)}): {sorted(ids)[:20]}")
        except Exception as e:
            out[depth] = None
            print(f"  {depth} 跳查询失败: {type(e).__name__}: {str(e)[:100]}")
    return out


# -------------------------------------------------------------------- main
ALIAS = "g"


def main():
    print("PoC：货拉拉司机团伙欺诈识别 — HugeGraph + Semantica")
    print(f"仓库: {REPO}")

    hr("S1  造数据")
    drivers, devices, phones, cards, edges = build_dataset()
    print(f"  司机 {len(drivers)} / 设备 {len(devices)} / "
          f"手机 {len(phones)} / 银行卡 {len(cards)} / 关系边 {len(edges)}")

    store = HugeGraphStore(host=HOST, port=PORT, graph=GRAPH)
    hr("S0  连接 HugeGraph")
    if not store.connect():
        raise RuntimeError("连接 HugeGraph 失败，先确认 localhost:8080 可用")
    print(f"  已连接 {HOST}:{PORT}/{GRAPH}")

    try:
        write_to_hugegraph(store, drivers, devices, phones, cards, edges)

        stats = store.get_stats()
        print(f"  HugeGraph 当前统计: {stats}")

        facts, derived, big, high_risk, big_1hop = run_reasoning(store)
        gm = gremlin_baseline(store)
    finally:
        store.close()

    # ---------------------------------------------------------- 断言
    hr("S6  结果校验")
    biggest = max(big, key=len) if big else set()
    expect(len(biggest) == 9, "传递闭包识别出完整的 9 人团伙",
           f"最大团伙 = {sorted(biggest)}（{len(biggest)} 人）")
    expect(sorted(biggest) == [f"d{i:02d}" for i in range(1, 10)],
           "团伙成员精确等于 d01-d09", f"实得 {sorted(biggest)}")

    expect(len(big_1hop) == 3, "基线（仅共享设备）只看到 3 个碎片",
           f"实得 {len(big_1hop)} 个: {[sorted(c) for c in big_1hop]}")
    expect(len(biggest) > max((len(c) for c in big_1hop), default=0),
           "传递闭包发现的团伙显著大于 1 跳基线",
           f"闭包 {len(biggest)} 人 vs 基线最大 "
           f"{max((len(c) for c in big_1hop), default=0)} 人")

    expect("d09" in high_risk, "黑名单本人 d09 被标记高危")
    expect(all(f"d{i:02d}" in high_risk for i in range(1, 9)),
           "同团 d01-d08 全部被风险传播标记",
           f"实得高危 {high_risk}")

    normals = [f"d{i:02d}" for i in range(10, 31)]
    false_pos = [n for n in normals if n in high_risk]
    expect(not false_pos, "零误报：21 个正常司机无一被标记",
           f"误报 = {false_pos}" if false_pos else "误报 = 0")

    # ---------------------------------------------------------- 汇总
    hr("汇总")
    npass = sum(1 for _, ok, _ in CHECKS if ok)
    print(f"{'检查项':<44}{'结果':<8}")
    print("-" * 76)
    for name, ok, _ in CHECKS:
        print(f"{name:<44}{'PASS' if ok else 'FAIL':<8}")
    print("-" * 76)
    print(f"PASS {npass} / {len(CHECKS)}")

    print("\n【场景结论】")
    print(f"  写入 HugeGraph : {len(drivers)+len(devices)+len(phones)+len(cards)} 顶点 / {len(edges)} 边")
    print(f"  从 HG 读回事实 : {len(facts)} 条")
    print(f"  Datalog 推导   : {len(derived)} 条")
    print(f"  1 跳基线       : 3 个碎片（看不到完整团伙）")
    print(f"  传递闭包       : 1 个 {len(biggest)} 人团伙")
    print(f"  高危名单       : {len(high_risk)} 人，误报 {len(false_pos)} 人")
    if gm.get(3):
        print(f"  Gremlin 3 跳   : d09 可达 {len(gm[3])} 个节点（无规则语义、无解释链）")

    return 0 if npass == len(CHECKS) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)

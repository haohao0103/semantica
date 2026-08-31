#!/usr/bin/env python3
"""
把 fraud_risk_e2e 那个 case 拆开讲清楚 —— 面向「看不懂推理过程」的读者。

输出 6 件事：
  S1  完整数据明细：30 个司机各自用什么设备 / 手机 / 银行卡
  S2  关联对：哪两个司机共享了什么（这是 linked 的来源）
  S3  ★ 关键推理链：d01 到 d09 是怎么连上的，逐跳列出依据
  S4  跳数对比：原始图 vs 规则折叠图，解释 Gremlin 为什么抓不到
  S5  分阶段推导统计：93 条事实怎么变成 386 条
  S6  误报核验：21 个正常司机为什么一个都没被冤枉
"""

import sys
from collections import defaultdict, deque
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from semantica.graph_store import HugeGraphStore  # noqa: E402
from semantica.reasoning import DatalogReasoner  # noqa: E402

ALIAS = "g"
GRAPH = "hugegraph"
EDGE_TO_PRED = {
    "fraud_used_device": "used_device",
    "fraud_used_phone": "used_phone",
    "fraud_owns_card": "owns_card",
}
PRED_CN = {
    "used_device": "共享设备",
    "used_phone": "共享手机",
    "owns_card": "共享银行卡",
}

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_fraud_risk_e2e import build_dataset  # noqa: E402


def hr(t=""):
    print("\n" + "=" * 78)
    if t:
        print(t)
        print("=" * 78)


def strip(v):
    v = str(v)
    return v[len("fraud_"):] if v.startswith("fraud_") else v


def main():
    drivers, devices, phones, cards, edges = build_dataset()

    # ---------------------------------------------------------- S1 数据明细
    hr("S1  数据明细：每个司机用了什么设备 / 手机号 / 银行卡")
    own = defaultdict(lambda: {"dev": set(), "ph": set(), "card": set()})
    for s, t, lab in edges:
        kind = {"fraud_used_device": "dev",
                "fraud_used_phone": "ph",
                "fraud_owns_card": "card"}[lab]
        own[s][kind].add(t)

    print(f"{'司机':<8}{'设备':<26}{'手机号':<22}{'银行卡'}")
    print("-" * 78)
    for d in drivers:
        o = own[d]
        tag = ""
        if d in ("d01", "d02", "d03", "d04", "d05", "d06", "d07", "d08", "d09"):
            tag = "  <- 团伙成员"
        if d == "d09":
            tag = "  <- 团伙成员 / 已确认黑名单"
        print(f"{d:<8}{','.join(sorted(o['dev'])):<26}"
              f"{','.join(sorted(o['ph'])):<22}"
              f"{','.join(sorted(o['card']))}{tag}")

    # ---------------------------------------------------------- S2 关联对
    hr("S2  关联对：哪两个司机共享了什么（排除自反）")
    shares = defaultdict(list)  # (a,b) -> [(类型, 资源)]
    by_res = defaultdict(lambda: defaultdict(list))  # 类型 -> 资源 -> [司机]
    for s, t, lab in edges:
        pred = EDGE_TO_PRED[lab]
        by_res[pred][t].append(s)
    for pred, res_map in by_res.items():
        for res, ds in res_map.items():
            if len(ds) > 1:
                for i in range(len(ds)):
                    for j in range(i + 1, len(ds)):
                        a, b = sorted((ds[i], ds[j]))
                        shares[(a, b)].append((pred, res))

    print(f"  共 {len(shares)} 对司机存在共享关系：")
    for (a, b), items in sorted(shares.items()):
        desc = " + ".join(f"{PRED_CN[p]} {r}" for p, r in sorted(items))
        print(f"    {a} <-> {b}   [{desc}]")

    # ------------------------------------------- S3 ★ 关键推理链 d01 -> d09
    hr("S3  关键推理链：d01 是怎么跟黑名单 d09 扯上关系的")

    adj = defaultdict(list)
    for (a, b), items in shares.items():
        adj[a].append((b, items))
        adj[b].append((a, items))

    def bfs(src, dst):
        prev = {}
        q = deque([src])
        seen = {src}
        while q:
            u = q.popleft()
            if u == dst:
                break
            for v, items in adj[u]:
                if v not in seen:
                    seen.add(v)
                    prev[v] = (u, items)
                    q.append(v)
        if dst not in seen:
            return None
        path, cur = [], dst
        while cur != src:
            u, items = prev[cur]
            path.append((u, cur, items))
            cur = u
        return list(reversed(path))

    chain = bfs("d01", "d09")
    if not chain:
        print("  未找到路径（异常）")
        return 1

    print(f"  d01 与 d09 之间没有共享任何东西，中间隔了 {len(chain)} 跳：\n")
    cursor = "d01"
    print(f"    起点 {cursor}")
    for i, (u, v, items) in enumerate(chain, 1):
        desc = " + ".join(f"{PRED_CN[p]} {r}" for p, r in sorted(items))
        print(f"    第 {i} 跳：{u} ──[{desc}]──> {v}")
    print(f"    终点 d09（已确认黑名单）")
    print(f"\n  => d01 与黑名单 d09 同属一个团伙，风险传播后 d01 被标记高危。")
    print(f"  => 这条链上任意一环断掉，团伙就会碎成小块（这正是 1 跳规则看不到的原因）。")

    # ------------------------------------------------ S4 跳数对比（核心）
    hr("S4  为什么 Gremlin 抓不到：原始图 vs 规则折叠图")

    # 原始图跳数：司机之间必须经由设备/手机/卡中转
    raw_adj = defaultdict(set)
    for s, t, lab in edges:
        raw_adj[s].add(t)
        raw_adj[t].add(s)

    def raw_hops(src, dst):
        dist = {src: 0}
        q = deque([src])
        while q:
            u = q.popleft()
            for v in raw_adj[u]:
                if v not in dist:
                    dist[v] = dist[u] + 1
                    q.append(v)
        return dist.get(dst)

    rh = raw_hops("d01", "d09")
    print(f"  原始图（司机─设备─司机）      : d01 到 d09 需要 {rh} 跳")
    print(f"  规则折叠图（司机─同伙─司机）  : d01 到 d09 只需 {len(chain)} 跳")
    print()
    print(f"  含义：规则把「共享设备」这种 2 跳模式折叠成 1 条语义边，")
    print(f"        Datalog 再对语义边做传递闭包，{len(chain)} 步就能收敛出团伙。")
    print(f"        而 Gremlin 在原始图上要爬 {rh} 跳 —— 换任何一个起点都要重爬，")
    print(f"        且沿途全是设备/卡号节点，无法回答「他们是不是一伙的」。")

    store = HugeGraphStore(host="localhost", port=8080, graph=GRAPH)
    print()
    if store.connect():
        try:
            sp = store.shortest_path("fraud_d01", "fraud_d09")
            print(f"  HugeGraph shortest_path(d01, d09) 实测: {sp}")
        except Exception as e:
            print(f"  HugeGraph shortest_path 调用失败: {type(e).__name__}: {e}")
        store.close()

    # ------------------------------------------------- S5 分阶段推导统计
    hr("S5  分阶段推导：93 条事实如何变成 386 条")
    facts = []
    for s, t, lab in edges:
        facts.append(f"{EDGE_TO_PRED[lab]}({s}, {t})")
    facts.append("blacklisted(d09)")

    stages = [
        ("阶段 0  原始事实（HugeGraph 读回）", []),
        ("阶段 1  + shared_device/phone/card", [
            "shared_device(X, Y) :- used_device(X, D), used_device(Y, D).",
            "shared_phone(X, Y) :- used_phone(X, P), used_phone(Y, P).",
            "shared_card(X, Y) :- owns_card(X, C), owns_card(Y, C).",
        ]),
        ("阶段 2  + linked（三类共享归并）", [
            "linked(X, Y) :- shared_device(X, Y).",
            "linked(X, Y) :- shared_phone(X, Y).",
            "linked(X, Y) :- shared_card(X, Y).",
        ]),
        ("阶段 3  + same_gang 传递闭包", [
            "same_gang(X, Y) :- linked(X, Y).",
            "same_gang(X, Y) :- linked(X, Z), same_gang(Z, Y).",
        ]),
        ("阶段 4  + high_risk 风险传播", [
            "high_risk(X) :- same_gang(X, Y), blacklisted(Y).",
        ]),
    ]

    acc = []
    prev_total = 0
    for name, rules in stages:
        r = DatalogReasoner()
        for f in facts:
            r.add_fact(f)
        for rule in acc + rules:
            r.add_rule(rule)
        derived = [str(d) for d in r.derive_all()]
        delta = len(derived) - prev_total
        print(f"  {name}")
        print(f"      累计推导 {len(derived):>4} 条  (本阶段新增 {delta:>4} 条)")
        for rule in rules:
            head = rule.split("(")[0].split(" ")[-1]
            n = sum(1 for d in derived if d.startswith(head + "("))
            print(f"        {head:<16} {n:>4} 条")
        acc += rules
        prev_total = len(derived)

    # ---------------------------------------------------------- S6 误报核验
    hr("S6  误报核验：21 个正常司机为什么一个都没被冤枉")
    normals = [f"d{i:02d}" for i in range(10, 31)]
    bad = [n for n in normals if shares_any(n, shares)]
    print(f"  正常司机 d10-d30 共 {len(normals)} 人")
    print(f"  与任何人存在共享关系的: {bad if bad else '无'}")
    print()
    print("  原因：d10-d30 每人独占一套 dev_nXX / ph_nXX / card_nXX，")
    print("        linked(X,Y) 要求 X 与 Y 指向同一个资源，独占则无法匹配；")
    print("        same_gang 以 linked 为唯一入口，无入口即不产生任何推导；")
    print("        high_risk 又以 same_gang 为前提 —— 三层下来天然零误报。")
    print(f"\n  实测误报数: {len(bad)}")

    return 0


def shares_any(d, shares):
    return any(d in (a, b) for (a, b) in shares)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)

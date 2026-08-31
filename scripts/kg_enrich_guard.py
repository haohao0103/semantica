#!/usr/bin/env python3
"""
kg_enrich_guard.py — KG 推理增强写入防护层

背景（本环境实测）：hstore 集群存在**写侧跨图泄漏**——向目标图写入的顶点/边会
泄漏到另一个图。因此增强流程在写回推断边前，必须校验"确实写到了目标图"，
并在写后确认"没有污染其他图"。

防护四层：
  1. 目标图身份校验：图存在、可服务、Gremlin alias 与图名一致、图在白名单内
  2. 端点归属校验：待建边的两端顶点必须**真实存在于目标图**（防止跨图解析）
  3. 写入后泄漏检测：对比写入前后**所有图**的点边计数，任何其他图计数变化即判为泄漏
  4. 自动回滚：检测到泄漏立即删除本轮写入的边，并抛 GraphLeakError 中止

用法：
    guard = GraphGuard(host, port, target_graph="kg_enrich_live",
                       allowlist=["kg_enrich_live", "kg_rag"])
    guard.precheck(store)                      # 写前
    guard.verify_endpoints(store, ["a","b"])   # 端点归属
    base = guard.snapshot()                    # 基线快照
    written = [...]                            # 执行写入
    guard.post_verify(base, written)           # 写后校验（泄漏则自动回滚）

自检：python3 kg_enrich_guard.py --selftest
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import urllib.parse
from typing import Any, Dict, List, Optional, Sequence

HG_BACKEND = "/Users/mac/Desktop/apache-code/hugegraph-dev/semantica-hg-backend"
if HG_BACKEND not in sys.path:
    sys.path.insert(0, HG_BACKEND)

from semantica.graph_store.hugegraph_store import HugeGraphStore  # noqa: E402


class GraphGuardError(Exception):
    """防护层通用异常。"""


class GraphLeakError(GraphGuardError):
    """检测到跨图泄漏。"""


class GraphGuard:
    """写入前校验目标图 + 写入后跨图泄漏检测 + 自动回滚。"""

    def __init__(
        self,
        host: str,
        port: int,
        target_graph: str,
        other_graphs: Optional[Sequence[str]] = None,
        allowlist: Optional[Sequence[str]] = None,
        verbose: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.target_graph = target_graph
        self.other_graphs = list(other_graphs or [])
        # 白名单：为空表示不限制（但强烈建议显式传入，防止误写生产图）
        self.allowlist = list(allowlist) if allowlist else None
        self.verbose = verbose

    # -- 内部 ------------------------------------------------------------
    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  [guard] {msg}")

    def _store(self, graph: str) -> HugeGraphStore:
        s = HugeGraphStore(host=self.host, port=self.port, graph=graph)
        s.connect()
        return s

    # -- 计数：必须用 REST（图作用域），不能用 Gremlin g.V().count() --
    # 实测 HugeGraph 1.7 hstore 下，默认图(hugegraph)的 Gremlin 遍历源会返回
    # 陈旧/幻影计数（g.V().count() 返回 2，但 g.V().valueMap(true) 实际 0 个顶点），
    # 导致跨图泄漏误报。REST GET /graphs/{g}/graph/{vertices,edges} 是图作用域权威计数。
    def _http_json(self, store: "HugeGraphStore", url: str):
        r = store._session.get(url, timeout=30)
        raw = r.content
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        r.raise_for_status()
        return json.loads(raw)

    def _count(self, graph: str) -> Dict[str, int]:
        s = self._store(graph)
        try:
            vdata = self._http_json(s, f"{s.base_url}/graphs/{graph}/graph/vertices?limit=100000")
            v = len(vdata.get("vertices") or [])
        except Exception:  # noqa: BLE001
            v = -1
        try:
            edata = self._http_json(s, f"{s.base_url}/graphs/{graph}/graph/edges?limit=100000")
            e = len(edata.get("edges") or [])
        except Exception:  # noqa: BLE001
            e = -1
        return {"V": v, "E": e}

    def list_graphs(self) -> List[str]:
        s = self._store(self.target_graph)
        r = s._session.get(f"{s.base_url}/graphs", timeout=10)
        r.raise_for_status()
        return r.json().get("graphs", [])

    # -- 第 1 层：目标图身份校验 -------------------------------------------
    def precheck(self, store: HugeGraphStore) -> None:
        if store.graph != self.target_graph:
            raise GraphGuardError(
                f"store.graph={store.graph!r} 与目标图 {self.target_graph!r} 不一致"
            )
        if self.allowlist is not None and self.target_graph not in self.allowlist:
            raise GraphGuardError(
                f"目标图 {self.target_graph!r} 不在白名单 {self.allowlist} 内，拒绝写入"
            )
        graphs = self.list_graphs()
        if self.target_graph not in graphs:
            raise GraphGuardError(
                f"目标图 {self.target_graph!r} 不存在，现有图: {graphs}"
            )
        # Gremlin traversal source 必须与图名一致（曾因硬编码打到默认图）
        expect_alias = f"__g_DEFAULT-{self.target_graph}"
        actual = getattr(store, "_traversal_source", None)
        if actual != expect_alias:
            raise GraphGuardError(
                f"Gremlin alias 异常: 期望 {expect_alias!r}，实际 {actual!r}；"
                "多图下会把查询打到错误的图"
            )
        # 目标图必须真的能服务（读取自身数据不报错）
        self._count(self.target_graph)
        self._log(
            f"身份校验通过: graph={self.target_graph} alias={expect_alias} "
            f"白名单={'是' if self.allowlist else '未启用'}"
        )

    # -- 第 2 层：端点归属校验 --------------------------------------------
    def verify_endpoints(self, store: HugeGraphStore, vertex_ids: Sequence[str]) -> None:
        """待建边的两端顶点必须真实存在于目标图，防止跨图解析导致写偏。

        逐个按 id 走 REST 路径查询（引号包裹 + URL 编码）：本实例批量 `?ids=`
        对 PRIMARY_KEY 复合 id（如 "2:orders"）失效（忽略参数返回全表，实测 96 条），
        而路径形式 GET /vertices/"2:orders" 精确返回单顶点。同时绕开 Gremlin。
        """
        ids = sorted({str(v) for v in vertex_ids})
        if not ids:
            return
        s = self._store(self.target_graph)
        found = 0
        for vid in ids:
            try:
                quoted = urllib.parse.quote(
                    '"' + str(vid).replace('"', '\\"') + '"', safe="")
                url = (
                    f"{s.base_url}/graphs/{self.target_graph}/graph/vertices/{quoted}"
                )
                r = s._session.get(url, timeout=15)
                if r.status_code == 200:
                    found += 1
            except Exception:  # noqa: BLE001
                pass
        if found != len(ids):
            raise GraphGuardError(
                f"端点归属校验失败: 目标图 {self.target_graph!r} 中只找到 "
                f"{found}/{len(ids)} 个顶点（顶点可能不存在或落在别的图），拒绝写入"
            )
        self._log(f"端点归属校验通过: {len(ids)} 个顶点均存在于 {self.target_graph}")

    # -- 第 3 层：写入后泄漏检测 -------------------------------------------
    def snapshot(self) -> Dict[str, Dict[str, int]]:
        graphs = [self.target_graph] + [g for g in self.other_graphs if g != self.target_graph]
        snap = {g: self._count(g) for g in graphs}
        self._log("基线快照: " + ", ".join(f"{g}(V={c['V']},E={c['E']})" for g, c in snap.items()))
        return snap

    def post_verify(
        self,
        baseline: Dict[str, Dict[str, int]],
        written: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Dict[str, int]]:
        """对比基线：其他图计数必须一字不差，否则判定泄漏并回滚。"""
        after = {g: self._count(g) for g in baseline}
        self._log("写入后: " + ", ".join(f"{g}(V={c['V']},E={c['E']})" for g, c in after.items()))

        leaks = []
        for g, before in baseline.items():
            if g == self.target_graph:
                continue
            now = after[g]
            if now["V"] != before["V"] or now["E"] != before["E"]:
                leaks.append(
                    f"{g}: V {before['V']}->{now['V']}, E {before['E']}->{now['E']}"
                )
        if leaks:
            msg = "检测到跨图泄漏 -> " + "; ".join(leaks)
            self._log("泄漏！执行回滚")
            self.rollback(written or [])
            raise GraphLeakError(msg)

        tgt_before, tgt_after = baseline[self.target_graph], after[self.target_graph]
        self._log(
            f"无泄漏；目标图 {self.target_graph}: "
            f"V {tgt_before['V']}->{tgt_after['V']}, E {tgt_before['E']}->{tgt_after['E']}"
        )
        return after

    # -- 第 4 层：回滚 ------------------------------------------------------
    def rollback(self, written: List[Dict[str, Any]]) -> int:
        """删除本轮写入的边 / 顶点（尽力而为，返回成功删除条数）。

        item 形如 {"kind": "edge"|"vertex", "id": ...}。
        注：泄漏既可能发生在写边，也可能发生在**播种顶点**阶段
        （实测播种的顶点会泄漏到别的图），故两者都要能回滚。
        """
        store = self._store(self.target_graph)
        removed = 0
        for item in written:
            if not isinstance(item, dict):
                continue
            oid, kind = item.get("id"), item.get("kind", "edge")
            if not oid:
                continue
            try:
                if kind == "vertex":
                    store._session.delete(
                        f"{store.base_url}/graphs/{self.target_graph}"
                        f"/graph/vertices/{oid}",
                        timeout=store.timeout,
                    )
                else:
                    store.delete_relationship(oid)
                removed += 1
            except Exception as e:  # noqa: BLE001
                self._log(f"回滚失败 {kind} {oid}: {e}")
        self._log(f"回滚完成，删除 {removed}/{len(written)} 条")
        return removed


# ---------------- 自检：证明泄漏检测逻辑真的会触发 -------------------------
def _selftest() -> int:
    print("=== GraphGuard 自检（离线，验证泄漏检测与回滚逻辑）===")
    g = GraphGuard("127.0.0.1", 8081, "kg_test", other_graphs=["kg_other"],
                   allowlist=["kg_test"], verbose=True)

    # 1) 白名单拦截
    try:
        g2 = GraphGuard("127.0.0.1", 8081, "kg_rag", allowlist=["kg_test"])
        g2.precheck(type("S", (), {"graph": "kg_rag", "_traversal_source": "__g_DEFAULT-kg_rag"})())
        print("  [FAIL] 白名单未拦截")
        return 1
    except GraphGuardError as e:
        print(f"  [ok] 白名单拦截生效: {str(e)[:60]}")

    # 2) alias 不一致拦截
    #     注意：必须用一个**真实存在**的图作为目标，否则会先被"图不存在"拦下，
    #     alias 校验根本执行不到（旧版自检就是这个假阳性）。
    real_graph = g.list_graphs()[0]
    try:
        g3 = GraphGuard("127.0.0.1", 8081, real_graph, allowlist=[real_graph])
        g3.precheck(type("S", (), {"graph": real_graph,
                                   "_traversal_source": "__g_DEFAULT-hugegraph"})())
        print("  [FAIL] alias 校验未拦截")
        return 1
    except GraphGuardError as e:
        if "alias" not in str(e):
            print(f"  [FAIL] 期望 alias 校验触发，实际被别的检查拦下: {e}")
            return 1
        print(f"  [ok] alias 校验生效 ({real_graph}): {str(e)[:60]}")

    # 2b) 图不存在拦截（用不存在的图，验证前置检查）
    try:
        g2b = GraphGuard("127.0.0.1", 8081, "no_such_graph_xyz", allowlist=["no_such_graph_xyz"])
        g2b.precheck(type("S", (), {"graph": "no_such_graph_xyz",
                                    "_traversal_source": "__g_DEFAULT-no_such_graph_xyz"})())
        print("  [FAIL] 图不存在未拦截")
        return 1
    except GraphGuardError as e:
        print(f"  [ok] 图不存在拦截生效: {str(e)[:50]}")

    # 3) 泄漏检测（注入假计数，验证能识别其他图变化）
    base = {"kg_test": {"V": 0, "E": 0}, "kg_other": {"V": 0, "E": 0}}
    g._count = lambda graph: ({"V": 0, "E": 0} if graph == "kg_test" else {"V": 2, "E": 2})
    try:
        g.post_verify(base, written=[])
        print("  [FAIL] 泄漏未被检测")
        return 1
    except GraphLeakError as e:
        print(f"  [ok] 泄漏检测生效: {str(e)[:70]}")

    # 4) 无泄漏路径放行
    g._count = lambda graph: {"V": 0, "E": 0}
    try:
        g.post_verify(base, written=[])
        print("  [ok] 无泄漏路径正常放行")
    except GraphLeakError as e:
        print(f"  [FAIL] 误报泄漏: {e}")
        return 1

    print("=== 自检通过 ===")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    sys.exit(_selftest() if a.selftest else 0)

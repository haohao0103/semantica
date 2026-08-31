#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
真实 LLM 抽取 → GraphBuilder → HugeGraph 落地，端到端验证。

与上一步 test_pipeline_e2e.py 的区别：
  上一步喂「预抽取」的实体/关系（不调 LLM，可复现）；
  本脚本喂**纯文本**，由 Semantica 的 LLM extractor 现场抽取实体与关系，
  再落库 HugeGraph —— 完整闭环：
      文本 → LLM 抽取 → 实体解析 → GraphBuilder.build() → HugeGraph REST

LLM provider：OpenAI 兼容端点（base_url），模型/密钥从
``/Users/mac/.workbuddy/models.json`` 读取（id=glm-5.3，实测可用 2026-08-28）。

日志：/Users/mac/Desktop/apache-code/hugegraph-dev/_out/semantica-hg-backend/logs/test_llm_pipeline_e2e.log
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
                        "test_llm_pipeline_e2e.log")
Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, SEMANTICA)

_log_fh = open(LOG_FILE, "a", buffering=1)
_log_fh.write(
    f"\n=== {time.strftime('%Y-%m-%d %H:%M:%S')} LLM pipeline e2e start ===\n"
)
_orig_print = print


def print(*args, **kw):  # noqa: A001
    line = " ".join(str(a) for a in args)
    _log_fh.write(line + "\n")
    _orig_print(line, **kw)


sys.stderr = _log_fh

import requests  # noqa: E402

BASE = "http://localhost:8080"
GRAPH = "hugegraph"

# ---------------------------------------------------------------------------
# 待抽取的原始文本（纯文本，无任何预标注）
# ---------------------------------------------------------------------------
CORPUS = [
    "Marie Curie was a physicist born in Warsaw, Poland. "
    "She discovered radium and polonium while working at the University of Paris. "
    "She won the Nobel Prize in Physics in 1903.",

    "Pierre Curie was a French physicist who worked at the University of Paris. "
    "He was married to Marie Curie and they collaborated on research.",
]


def load_llm_config() -> dict:
    """Read a working OpenAI-compatible endpoint from workbuddy models.json."""
    cfg_path = "/Users/mac/.workbuddy/models.json"
    models = json.load(open(cfg_path))
    by_id = {m.get("id"): m for m in models}
    # glm-5.3 verified working 2026-08-28 (other keys are quota-exhausted).
    mid = os.getenv("HG_E2E_LLM_MODEL", "glm-5.3")
    if mid not in by_id:
        raise RuntimeError(f"model {mid!r} not in {cfg_path}")
    m = by_id[mid]
    base = m["url"].replace("/chat/completions", "")
    return {
        "model_id": mid,
        "api_key": m["apiKey"],
        "base_url": base.rstrip("/"),
    }


def gremlin(script: str):
    r = requests.post(
        f"{BASE}/gremlin",
        data=json.dumps({"gremlin": script,
                         "aliases": {"g": "__g_DEFAULT-hugegraph"}}),
        headers={"Accept": "application/json"},
        timeout=60,
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
    print("Real-LLM Pipeline: text -> LLM extract -> HugeGraph")
    print("=" * 70)

    # ---- 0. readiness ------------------------------------------------------
    print("\n[0] readiness")
    llm = load_llm_config()
    print(f"  LLM: model={llm['model_id']} base_url={llm['base_url']} "
          f"key={llm['api_key'][:8]}...")
    r = requests.get(f"{BASE}/graphs", timeout=10)
    r.raise_for_status()
    print(f"  HugeGraph reachable: {r.json()}")
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

    # ---- 1. LLM extraction (no graph_store yet) ----------------------------
    print("\n[1] LLM extraction sanity check (NER + triplets, no store)")
    from semantica.kg.graph_builder import GraphBuilder

    # Common kwargs that route every extractor to the OpenAI-compatible LLM.
    llm_opts = dict(
        ner_method="llm",
        triplet_method="llm",
        relation_method="llm",
        provider="openai",
        llm_model=llm["model_id"],
        api_key=llm["api_key"],
        base_url=llm["base_url"],
        extract_relations=True,
        extract_triplets=True,
    )

    probe = GraphBuilder()  # no graph_store -> in-memory only
    t0 = time.time()
    g0 = probe.build(CORPUS, **llm_opts)
    dt = time.time() - t0
    n_ent, n_rel = len(g0["entities"]), len(g0["relationships"])
    print(f"  LLM extracted {n_ent} entities / {n_rel} relationships in {dt:.1f}s")
    ok_all &= check("LLM produced entities", n_ent > 0, f"{n_ent}")
    ok_all &= check("LLM produced relationships", n_rel > 0, f"{n_rel}")
    for e in g0["entities"][:8]:
        print(f"    entity: {e.get('id')} | {e.get('name')} | {e.get('type')}")
    for rel in g0["relationships"][:8]:
        print(f"    rel: {rel.get('source')} -[{rel.get('type')}]-> "
              f"{rel.get('target')}")

    if n_ent == 0 or n_rel == 0:
        print("  aborting: LLM extraction produced nothing usable")
        return 1

    # ---- 2. same build, but persisted to HugeGraph -------------------------
    print("\n[2] build() with GraphStore(backend='hugegraph') -> persist")
    from semantica.graph_store.graph_store import GraphStore

    gs = GraphStore(backend="hugegraph",
                    config={"host": "localhost", "port": 8080,
                            "graph": GRAPH, "timeout": 60})
    gs.connect()
    print(f"  connected, backend={gs.backend}")

    builder = GraphBuilder(graph_store=gs)
    t0 = time.time()
    graph = builder.build(CORPUS, **llm_opts)
    dt = time.time() - t0
    print(f"  build+persist done in {dt:.1f}s: "
          f"{len(graph['entities'])} entities / "
          f"{len(graph['relationships'])} relationships")

    # ---- 3. assert what the LLM found actually landed ----------------------
    print("\n[3] assert LLM-extracted graph landed in HugeGraph")
    rows = gremlin("g.V().limit(200).project('id','label').by(id).by(label)")
    print(f"  {len(rows)} vertices in store")
    ids = {r.get("id") for r in rows}
    exp_ent = {e.get("id") for e in graph["entities"]}
    landed = exp_ent & ids
    print(f"  expected {len(exp_ent)} entity ids, {len(landed)} found in store")
    ok_all &= check("all entities landed", len(landed) == len(exp_ent),
                    f"{len(landed)}/{len(exp_ent)}")
    if landed != exp_ent:
        print(f"    missing: {sorted(exp_ent - ids)[:10]}")

    # ---- 4. assert edges landed -------------------------------------------
    print("\n[4] assert edges landed")
    erows = gremlin(
        "g.E().limit(200).project('label','outV','inV')"
        ".by(label).by(outV().id()).by(inV().id())"
    )
    print(f"  {len(erows)} edges in store")
    print(f"  sample: {json.dumps(erows[:5], default=str)}")
    ok_all &= check("edges landed", len(erows) > 0, f"{len(erows)}")

    # Reconcile: how many extracted relationships actually landed?
    #
    # Measure against *unique, resolvable* tuples, not the raw count. Three
    # legitimate reductions happen before the write:
    #   1. dangling endpoints — the LLM emitted a relation to a string that
    #      entity resolution never materialised as a vertex (e.g. "Warsaw,
    #      Poland" vs entity "Warsaw"). These cannot be stored.
    #   2. duplicate (source, type, target) tuples — NER + relation + triplet
    #      extractors overlap, so the same triple is produced several times;
    #      HG collapses identical edges.
    #   3. self-loops — skipped.
    ent_ids = {e.get("id") for e in graph["entities"]}
    raw = graph["relationships"]
    dangling = [r for r in raw
                if r.get("source") not in ent_ids or r.get("target") not in ent_ids]
    valid = [r for r in raw if r not in dangling]
    exp_rel = {(r.get("source"), r.get("type"), r.get("target")) for r in valid}
    got_rel = {(r.get("outV"), r.get("label"), r.get("inV")) for r in erows}
    missing = exp_rel - got_rel
    print(f"  raw {len(raw)} -> dangling {len(dangling)} -> valid {len(valid)} "
          f"-> unique {len(exp_rel)}")
    if dangling:
        print(f"    dangling sample: "
              f"{[(r.get('source'), r.get('type'), r.get('target')) for r in dangling[:3]]}")
    print(f"  landed {len(got_rel)}, missing {len(missing)}")
    if missing:
        print(f"    missing sample: {sorted(missing)[:6]}")
    ratio = len(got_rel) / max(len(exp_rel), 1)
    print(f"  landing ratio (vs unique resolvable): {ratio:.0%}")
    ok_all &= check("edge landing ratio >= 90%", ratio >= 0.90, f"{ratio:.0%}")

    # ---- 5. semantic spot-check on real content ---------------------------
    print("\n[5] semantic spot-check (name/type preserved)")
    vrows = gremlin("g.V().limit(200).valueMap()")
    names = set()
    for vr in vrows:
        for v in (vr.get("name") or []):
            names.add(str(v).lower())
    print(f"  vertex names: {sorted(names)[:12]}")
    # Curie corpus should yield at least these
    hits = [t for t in ("marie curie", "pierre curie", "warsaw", "paris")
            if any(t in n for n in names)]
    print(f"  expected-name hits: {hits}")
    ok_all &= check("LLM content present in graph", len(hits) >= 2,
                    f"{len(hits)} hits")

    # ---- 6. traverse from an LLM-discovered entity ------------------------
    print("\n[6] traversal from an LLM-discovered entity")
    if landed:
        some_id = sorted(landed)[0]
        nb = gs.get_neighbors(node_id=some_id, direction="both")
        print(f"  neighbors of {some_id}: {json.dumps(nb, default=str)[:300]}")
        ok_all &= check("get_neighbors works on LLM node", isinstance(nb, list),
                        f"{len(nb)} neighbors")

    stats = gs.get_stats()
    print(f"  final stats: {stats}")
    ok_all &= check("stats consistent",
                    stats.get("vertex_count", 0) >= len(landed), str(stats))
    gs.close()

    print("\n" + "=" * 70)
    print("LLM E2E RESULT:", "ALL CHECKS PASSED" if ok_all else "SOME CHECKS FAILED")
    print("=" * 70)
    return 0 if ok_all else 1


if __name__ == "__main__":
    try:
        rc = main()
    except Exception:
        import traceback
        traceback.print_exc()
        rc = 1
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    _log_fh.close()
    sys.exit(rc)

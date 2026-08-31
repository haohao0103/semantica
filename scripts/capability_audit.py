#!/usr/bin/env python3
"""
Semantica 能力审计脚本 — 实测，不读文档。

做法：
  1. import 每个子模块，抓取 __all__（公开契约）
  2. 静态解析全部 .py，提取顶层 class / def 数量
  3. 专项关键词扫描：text2sql、数据库元数据、向量/图后端、导出格式、解析格式
  4. 对每个模块做真实 import 冒烟，标记 IMPORT_FAIL

输出：结构化表格 + 专项结论
日志：见运行时 tee 路径
"""

import ast
import importlib
import os
import re
import sys
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PKG = REPO / "semantica"

sys.path.insert(0, str(REPO))


def hr(title=""):
    print("\n" + "=" * 78)
    if title:
        print(title)
        print("=" * 78)


# ---------------------------------------------------------------- 1. 模块清单
def module_inventory():
    rows = []
    for d in sorted(p for p in PKG.iterdir() if p.is_dir()):
        name = d.name
        if name.startswith("_"):
            continue
        pyfiles = [f for f in d.rglob("*.py") if "test" not in str(f).lower()]
        n_files = len(pyfiles)
        n_loc = 0
        n_class = 0
        n_def = 0
        for f in pyfiles:
            try:
                src = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            n_loc += src.count("\n")
            try:
                tree = ast.parse(src)
            except SyntaxError:
                continue
            for node in tree.body:
                if isinstance(node, ast.ClassDef):
                    n_class += 1
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    n_def += 1
        rows.append((name, n_files, n_loc, n_class, n_def))
    return rows


# ------------------------------------------------------- 2. 公开契约 __all__
def public_api():
    out = {}
    for d in sorted(p for p in PKG.iterdir() if p.is_dir()):
        name = d.name
        init = d / "__init__.py"
        if not init.exists():
            out[name] = ("NO_INIT", [])
            continue
        try:
            src = init.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            out[name] = ("READ_FAIL", [])
            continue
        m = re.search(r"__all__\s*=\s*\[(.*?)\]", src, re.S)
        if not m:
            out[name] = ("NO_ALL", [])
            continue
        names = re.findall(r"[\"']([A-Za-z_][A-Za-z0-9_]*)[\"']", m.group(1))
        out[name] = ("OK", names)
    return out


# ------------------------------------------------------------- 3. import 冒烟
def import_smoke(mods):
    res = {}
    for name in mods:
        try:
            m = importlib.import_module(f"semantica.{name}")
            exported = [n for n in dir(m) if not n.startswith("_")]
            res[name] = ("OK", len(exported))
        except Exception as e:
            res[name] = (f"FAIL: {type(e).__name__}: {e}", 0)
    return res


# --------------------------------------------------------- 4. 关键词专项扫描
SCANS = {
    "text2sql / NL2SQL": [
        r"text2sql", r"text_to_sql", r"nl2sql", r"natural_language_to_sql",
        r"generate_sql", r"sql_generation", r"\bSQLGenerator\b",
    ],
    "text2GQL / NL→Cypher或Gremlin": [
        r"text2gql", r"text2cypher", r"text2gremlin", r"nl2gql",
        r"generate_gremlin", r"generate_cypher",
    ],
    "数据库元数据抽取 (FK/DDL/schema)": [
        r"get_foreign_keys", r"information_schema", r"get_pk_constraint",
        r"get_columns\s*\(", r"get_table_names", r"reflect_metadata",
        r"get_unique_constraints", r"get_indexes\s*\(",
    ],
    "GraphRAG / 检索增强": [
        r"graphrag", r"graph_rag", r"retrieval_augmented", r"\bRAG\b",
    ],
    "本体 / ontology": [
        r"ontology", r"\bOWL\b", r"\bRDFS?\b", r"\bSHACL\b",
    ],
    "推理 / reasoning": [
        r"reasoner", r"inference", r"\bdatalog\b", r"rule_engine",
        r"deductive", r"inductive",
    ],
    "冲突检测 / 去重": [
        r"conflict", r"deduplicat", r"entity_resolution", r"entity_matching",
    ],
    "血缘 / provenance": [
        r"provenance", r"lineage", r"\bRDF\*\b", r"reification",
    ],
    "时序知识图谱": [
        r"temporal", r"time_?aware", r"valid_time", r"bitemporal",
    ],
    "MCP / Agent 集成": [
        r"mcp_server", r"model_context_protocol", r"\bMCPServer\b",
    ],
    "评测 / evals": [
        r"\bevals?\b", r"benchmark", r"evaluat",
    ],
}


def keyword_scan():
    results = {}
    pyfiles = [f for f in PKG.rglob("*.py") if "test" not in str(f).lower()]
    cache = {}
    for f in pyfiles:
        try:
            cache[f] = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            pass

    for topic, pats in SCANS.items():
        compiled = [re.compile(p, re.I) for p in pats]
        hit_files = set()
        hit_count = 0
        for f, src in cache.items():
            for c in compiled:
                ms = c.findall(src)
                if ms:
                    hit_files.add(str(f.relative_to(REPO)))
                    hit_count += len(ms)
        results[topic] = (len(hit_files), hit_count, sorted(hit_files)[:6])
    return results


# ------------------------------------------- 5. 后端 / 格式 / provider 探测
def probe_backends():
    probes = {
        "graph_store 后端": (
            "semantica.graph_store",
            r"backend\s*==\s*[\"']([a-z0-9_]+)[\"']|[\"'](neo4j|falkordb|hugegraph|"
            r"networkx|memgraph|neptune|arangodb|janusgraph|tigergraph|nebula)[\"']",
        ),
        "vector_store 后端": (
            "semantica.vector_store",
            r"[\"'](faiss|qdrant|chroma|pinecone|weaviate|milvus|pgvector|"
            r"elasticsearch|lancedb|duckdb|numpy|memory)[\"']",
        ),
        "triplet_store 后端": (
            "semantica.triplet_store",
            r"[\"'](sqlite|duckdb|postgres|blazegraph|oxigraph|rdfox|jena|"
            r"virtuoso|graphdb|neptune|memory)[\"']",
        ),
        "LLM provider": (
            "semantica.llms",
            r"[\"'](openai|anthropic|gemini|google|ollama|bedrock|azure|"
            r"deepseek|groq|mistral|cohere|openrouter|vllm|huggingface)[\"']",
        ),
        "embedding provider": (
            "semantica.embeddings",
            r"[\"'](openai|sentence_transformers|sentence-transformers|huggingface|"
            r"cohere|ollama|fastembed|voyage|bedrock|tfidf|word2vec|glove|bm25)[\"']",
        ),
        "parse 解析格式": (
            "semantica.parse",
            r"[\"']\.(pdf|docx?|pptx?|xlsx?|csv|json|jsonl|md|markdown|txt|html?|xml|"
            r"yaml|yml|epub|rtf|ipynb|parquet|avro|zip|tex|log)[\"']",
        ),
        "ingest 数据源": (
            "semantica.ingest",
            r"[\"'](postgres|postgresql|mysql|sqlite|mariadb|oracle|mssql|"
            r"snowflake|bigquery|redshift|clickhouse|hive|spark|mongodb|"
            r"elasticsearch|kafka|neo4j|athena|databricks|trino|presto|"
            r"starrocks|doris|maxcompute|odps|sqlserver|db2)[\"']",
        ),
        "export 导出格式": (
            "semantica.export",
            r"[\"'](json|jsonl|csv|tsv|xml|rdf|turtle|ttl|n3|ntriples|nt|"
            r"graphml|gexf|gexf|cypher|gremlin|cypher_shell|parquet|arrow|"
            r"excel|xlsx|yaml|gexf|dot|graphviz|html|md|markdown|sql|"
            r"neo4j|pandas|dataframe|pickle|msgpack|avro|orc)[\"']",
        ),
    }
    out = {}
    for label, (mod, pattern) in probes.items():
        try:
            m = importlib.import_module(mod)
            mfile = getattr(m, "__file__", None)
            root = Path(mfile).parent if mfile else None
            found = set()
            if root:
                for f in root.rglob("*.py"):
                    if "test" in str(f).lower():
                        continue
                    try:
                        src = f.read_text(encoding="utf-8", errors="ignore")
                    except Exception:
                        continue
                    for hit in re.findall(pattern, src, re.I):
                        if isinstance(hit, tuple):
                            hit = next((h for h in hit if h), None)
                        if hit:
                            found.add(hit.lower())
            out[label] = sorted(found)
        except Exception as e:
            out[label] = [f"IMPORT_FAIL: {type(e).__name__}: {e}"]
    return out


# ------------------------------------------------------------- 6. 关键类实测
def probe_key_classes():
    checks = [
        ("semantica.kg", "GraphBuilder", "KG 构建"),
        ("semantica.kg", "GraphAnalyzer", "图分析"),
        ("semantica.kg", "EntityResolver", "实体消歧"),
        ("semantica.ontology", "OntologyGenerator", "本体自动生成"),
        ("semantica.ontology", "OntologyBuilder", "本体构建"),
        ("semantica.reasoning", "Reasoner", "推理机"),
        ("semantica.conflicts", "ConflictDetector", "冲突检测"),
        ("semantica.conflicts", "ConflictResolver", "冲突消解"),
        ("semantica.deduplication", "DeduplicationEngine", "去重引擎"),
        ("semantica.provenance", "ProvenanceTracker", "血缘追踪"),
        ("semantica.context", "ContextBuilder", "上下文构建(Agent)"),
        ("semantica.context", "AgentContext", "Agent 上下文"),
        ("semantica.explorer", "GraphExplorer", "图探索"),
        ("semantica.change_management", "ChangeManager", "变更管理"),
        ("semantica.pipeline", "PipelineBuilder", "流水线 DSL"),
        ("semantica.graph_store", "HugeGraphStore", "★ HugeGraph 后端(我们加的)"),
        ("semantica.ingest", "SchemaExporter", "★ DB schema 导出"),
        ("semantica.ingest", "DBIngestor", "★ DB 接入"),
    ]
    rows = []
    for mod, cls, desc in checks:
        try:
            m = importlib.import_module(mod)
            if hasattr(m, cls):
                rows.append(("OK", mod, cls, desc))
            else:
                rows.append(("MISSING", mod, cls, desc))
        except Exception as e:
            rows.append((f"FAIL:{type(e).__name__}", mod, cls, desc))
    return rows


# ------------------------------------------------------------------- main
def main():
    print("Semantica 能力审计")
    print(f"仓库: {REPO}")
    print(f"Python: {sys.executable}")
    print(f"版本: {importlib.import_module('semantica').__version__}")

    hr("1. 模块规模（排除 test）")
    inv = module_inventory()
    print(f"{'模块':<22}{'文件':>6}{'行数':>9}{'顶层类':>8}{'顶层函数':>10}")
    print("-" * 78)
    tot = [0, 0, 0, 0]
    for name, nf, loc, nc, nd in inv:
        print(f"{name:<22}{nf:>6}{loc:>9}{nc:>8}{nd:>10}")
        tot[0] += nf
        tot[1] += loc
        tot[2] += nc
        tot[3] += nd
    print("-" * 78)
    print(f"{'合计':<22}{tot[0]:>6}{tot[1]:>9}{tot[2]:>8}{tot[3]:>10}")

    hr("2. 公开契约 __all__")
    api = public_api()
    print(f"{'模块':<22}{'状态':<12}{'公开符号数':>10}   前若干")
    print("-" * 78)
    for name, (st, names) in api.items():
        preview = ", ".join(names[:4]) + ("..." if len(names) > 4 else "")
        print(f"{name:<22}{st:<12}{len(names):>10}   {preview}")

    hr("3. import 冒烟（真实加载）")
    mods = [r[0] for r in inv]
    smoke = import_smoke(mods)
    ok = [k for k, (s, _) in smoke.items() if s == "OK"]
    bad = [(k, s) for k, (s, _) in smoke.items() if s != "OK"]
    print(f"成功 {len(ok)} / {len(mods)}")
    for k, (s, n) in smoke.items():
        if s == "OK":
            print(f"  OK    {k:<24} 导出符号 {n}")
    for k, s in bad:
        print(f"  FAIL  {k:<24} {s}")

    hr("4. 关键词专项扫描（命中文件数 / 命中次数）")
    scans = keyword_scan()
    print(f"{'主题':<34}{'文件数':>7}{'次数':>7}")
    print("-" * 78)
    for topic, (nf, nc, sample) in scans.items():
        flag = "" if nc else "   <== 零命中"
        print(f"{topic:<34}{nf:>7}{nc:>7}{flag}")
    print("\n--- 关键主题命中样例 ---")
    for topic in ("text2sql / NL2SQL", "数据库元数据抽取 (FK/DDL/schema)",
                  "text2GQL / NL→Cypher或Gremlin"):
        if topic in scans:
            nf, nc, sample = scans[topic]
            print(f"\n[{topic}] 文件={nf} 次数={nc}")
            for s in sample:
                print(f"    {s}")

    hr("5. 后端 / 格式 / provider 实测")
    backends = probe_backends()
    for label, items in backends.items():
        print(f"\n[{label}]")
        if items and items[0].startswith("IMPORT_FAIL"):
            print(f"    {items[0]}")
        else:
            print(f"    {len(items)} 项: {', '.join(items)}")

    hr("6. 关键类实测")
    rows = probe_key_classes()
    print(f"{'状态':<16}{'模块':<32}{'类':<24}{'说明'}")
    print("-" * 78)
    for st, mod, cls, desc in rows:
        print(f"{st:<16}{mod:<32}{cls:<24}{desc}")

    hr("审计结束")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)

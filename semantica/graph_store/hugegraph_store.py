"""
HugeGraph Store for property graph storage and Gremlin querying (HG 1.7+).

This module provides a property-graph store implementation backed by a single-node
HugeGraph 1.7+ server, using:

* **REST CRUD** for schema / vertex / edge management under
  ``/graphs/{graph}/...`` (property keys, vertex/edge labels, vertices, edges).
* **Gremlin traversal** via the embedded TinkerPop Gremlin Server REST endpoint
  ``POST /gremlin``.

.. important:: HG 1.7.0 Gremlin channel facts (verified 2026-08-28)

    1. The traversal source is **not** bound to the bare variable ``g``. HG binds
       it as ``__g_DEFAULT-hugegraph`` (``G_PREFIX="__g_"`` + graph name
       ``DEFAULT-hugegraph``). The ``-`` is a hyphen, so it cannot be used as a
       bare identifier in a Groovy script.
    2. Therefore every Gremlin script is submitted with an **alias** that maps
       ``g`` → ``__g_DEFAULT-hugegraph``::

           {"gremlin": "g.V().limit(1)", "aliases": {"g": "__g_DEFAULT-hugegraph"}}

    3. The ``/gremlin`` response is **gzip-compressed** (content-type is still
       ``application/json``). The client must decompress it.
    4. HG 1.7.0 requires **JDK 11** (Groovy 2.5.14 only supports up to JDK 11
       bytecode). JDK 17/21 yields
       ``gremlin-groovy is not an available GremlinScriptEngine``.

Vertex and edge IDs follow HugeGraph's compound format: vertices are returned as
``"<numeric>:<label>"`` (e.g. ``"1:person"``) and edges as ``"<src>><label>><dst>"``.

Author: HugeGraph × Semantica Integration
License: MIT
"""

from __future__ import annotations

import gzip
import io
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover
    REQUESTS_AVAILABLE = False

from ..utils.exceptions import ProcessingError
from ..utils.logging import get_logger
from ..utils.progress_tracker import get_progress_tracker
from .query_sanitize import sanitize_identifier


# Traversal source bound by HugeGraph's injectTraversalSource():
#   G_PREFIX("__g_") + graph name ("DEFAULT-hugegraph") -> "__g_DEFAULT-hugegraph"
TRAVERSAL_SOURCE = "__g_DEFAULT-hugegraph"
# Alias name used inside submitted Gremlin scripts.
ALIAS = "g"


def _is_gzip(data: bytes) -> bool:
    return data[:2] == b"\x1f\x8b"


def _coerce_props_to_schema(
    properties: Dict[str, Any], declared_types: Dict[str, str]
) -> Dict[str, Any]:
    """Coerce property values to the data type HG has declared for each key.

    A property key's type is fixed at creation. When a caller later supplies a
    differently-typed value (e.g. ``{"updated": True}`` after the key was
    created as TEXT), the write fails with ``Invalid property value ...``.
    Coercing here keeps such updates working instead of erroring out.
    """
    out: Dict[str, Any] = {}
    for key, value in properties.items():
        declared = (declared_types.get(key) or "").upper()
        try:
            if declared == "TEXT":
                out[key] = value if isinstance(value, str) else str(value)
            elif declared == "DOUBLE":
                out[key] = float(value) if not isinstance(value, bool) else float(int(value))
            elif declared in ("INT", "LONG"):
                out[key] = int(value)
            elif declared == "BOOLEAN":
                out[key] = bool(value)
            else:
                out[key] = value
        except (TypeError, ValueError):
            out[key] = value
    return out


def _hg_data_type(value: Any) -> str:
    """Map a Python value to a HugeGraph property-key data type.

    Getting this wrong makes writes fail with e.g.
    ``Invalid property value '0.95' for key 'confidence', expect a value of
    type String, actual type Double`` — so floats/booleans are handled
    explicitly rather than falling through to TEXT.
    """
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INT"
    if isinstance(value, float):
        return "DOUBLE"
    return "TEXT"


def _split_compound(vertex_id: Any) -> Tuple[Any, str]:
    """Split HugeGraph's compound vertex id ``"<id>:<label>"``.

    Returns ``(raw_id, label)``.

    Two id regimes are supported:

    * **CUSTOMIZE_STRING / PRIMARY_KEY** (the default here, and what Semantica's
      pipeline uses): the vertex id is the caller's logical id, e.g.
      ``"person_alice"``. The REST edge endpoint takes it as a bare JSON
      *string* — wrapping it in extra quotes yields
      ``Invalid vertex id '"person_alice"'``.
    * **DEFAULT (automatic)**: HG assigns a numeric id and the REST layer
      reports it as ``"<numeric>:<label>"``. The REST edge endpoint requires the
      numeric id as a JSON *number* (a quoted string resolves to a different Id
      and fails with ``Invalid vertex id '...'``), so it is converted to ``int``.

    Note the asymmetry with :func:`_rest_id`: REST *edge endpoints* want the
    bare id, REST *vertex GET* wants it JSON-quoted.
    """
    s = str(vertex_id)
    if ":" in s:
        raw, _, label = s.rpartition(":")
    else:
        raw, label = s, ""
    # Only numeric ids (DEFAULT strategy) are coerced to int; logical string
    # ids must stay strings.
    if raw.isdigit():
        try:
            raw = int(raw)
        except (ValueError, TypeError):
            pass
    return raw, label


def _is_logical_id(vertex_id: Any) -> bool:
    """True when ``vertex_id`` is a logical (non-compound, non-numeric) id.

    Logical ids are what Semantica's pipeline uses (``person_alice``); they must
    not be treated as HG numeric ids.
    """
    raw, _ = _split_compound(vertex_id)
    return not isinstance(raw, int)


def _rest_id(vertex_id: Any) -> str:
    """Encode a vertex id for the REST *vertex/edge GET-ish* path.

    ``GET /graph/vertices/{id}`` parses the id via HugeGraph's
    ``checkAndParseVertexId``, which only accepts Number / String / UUID —
    and for a String it expects the **JSON-quoted** literal. So
    ``person_alice`` must be sent as ``"person_alice"`` (the quote characters
    are part of the value and get URL-encoded to ``%22``).

    Verified 2026-08-28: raw ``person_alice`` and url-encoded-but-unquoted
    ``person_alice`` both return 400
    ``The vertex id must be formatted as Number/String/UUID``; the JSON-quoted
    form returns 200.
    """
    raw, _ = _split_compound(vertex_id)
    if isinstance(raw, int):
        return str(raw)
    return quote(json.dumps(str(raw)), safe="")


def _v_arg(vertex_id: Any) -> str:
    """Format a vertex id for use inside a Gremlin ``g.V(...)`` step.

    * Numeric (DEFAULT-strategy) ids are emitted unquoted: ``g.V(123)``.
    * Logical / string ids are single-quoted: ``g.V('person_alice')``.

    Both are supported by HG's Gremlin traversal source; note this is *not* the
    same encoding as :func:`_rest_id`.
    """
    raw, _ = _split_compound(vertex_id)
    if isinstance(raw, int):
        return str(raw)
    return "'" + str(raw).replace("'", "\\'") + "'"


def _decode_response(resp: "requests.Response") -> Any:
    """Decode a possibly-gzip-compressed JSON response body."""
    raw = resp.content
    if _is_gzip(raw):
        raw = gzip.decompress(raw)
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


class HugeGraphStore:
    """
    HugeGraph 1.7 property-graph store (REST CRUD + Gremlin traversal).

    Parameters
    ----------
    host : str
        Hostname, e.g. ``"localhost"``.
    port : int
        HTTP port, default 8080 (HG REST + embedded Gremlin Server share it).
    graph : str
        Graph (store) name, default ``"hugegraph"``.
    timeout : float
        Request timeout in seconds.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        graph: str = "hugegraph",
        timeout: float = 30.0,
        **config,
    ) -> None:
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests is required: pip install requests")
        self.logger = get_logger("hugegraph_store")
        self.progress_tracker = get_progress_tracker()
        if not self.progress_tracker.enabled:
            self.progress_tracker.enabled = True

        self.host = host
        self.port = port
        self.graph = graph
        # Per-graph Gremlin traversal source. HugeGraph binds each graph's
        # traversal source as ``__g_DEFAULT-<graph>`` (hyphen, NOT underscore).
        # The module-level TRAVERSAL_SOURCE constant was hardcoded to
        # ``__g_DEFAULT-hugegraph``, which silently routed *every* Gremlin read
        # (get_relationships / execute_query / _lookup_vertex_label) to the
        # default graph regardless of ``self.graph``. Deriving it per instance
        # fixes multi-graph reads.
        self._traversal_source = f"__g_DEFAULT-{graph}"
        self.timeout = timeout
        self.base_url = f"http://{host}:{port}"
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        # Cached schema (label sets) to speed up ensure_* calls.
        self._schema_cache: Optional[Dict[str, Any]] = None

    # -- connection ---------------------------------------------------------

    def connect(self) -> bool:
        """Verify the HugeGraph service is reachable."""
        try:
            r = self._session.get(
                f"{self.base_url}/graphs", timeout=self.timeout
            )
            r.raise_for_status()
            info = r.json()
            raw = info.get("graphs", [])
            # HG returns either ["hugegraph"] (strings) or [{...}] (dicts).
            graphs = [g if isinstance(g, str) else g.get("name") for g in raw]
            self.logger.info(
                f"Connected to HugeGraph at {self.base_url}; graphs={graphs}"
            )
            return True
        except Exception as e:  # pragma: no cover
            raise ProcessingError(f"Failed to connect to HugeGraph: {e}")

    def close(self) -> None:
        self._session.close()

    # -- gremlin execution --------------------------------------------------

    def _gremlin_raw(self, script: str, aliases: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Submit a Gremlin script to ``POST /gremlin`` and return parsed JSON.

        Uses an ``aliases`` mapping so the script can refer to the traversal
        source as ``g`` while HG actually bound it as ``__g_DEFAULT-hugegraph``.
        Handles gzip-compressed responses.
        """
        url = f"{self.base_url}/gremlin"
        body = {
            "gremlin": script,
            "aliases": aliases or {ALIAS: self._traversal_source},
        }
        r = self._session.post(
            url,
            data=json.dumps(body),
            headers={"Accept": "application/json"},
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = _decode_response(r)
        status = (data or {}).get("status", {})
        if status.get("code", 200) != 200:
            raise ProcessingError(
                f"Gremlin error: {status.get('message')} (code={status.get('code')})"
            )
        return data or {}

    def _gremlin(self, script: str) -> Dict[str, Any]:
        """Submit a Gremlin script and normalise the result.

        Mirrors Neo4j's ``execute_query`` output shape:
        ``{"success": True, "records": [...], "keys": [...], "metadata": {...}}``.
        """
        self.logger.debug(f"gremlin: {script[:200]}")
        data = self._gremlin_raw(script)
        payload = data.get("result", {})
        rows = payload.get("data", []) or []
        if not isinstance(rows, list):
            rows = [rows]

        keys: List[str] = []
        for row in rows:
            if isinstance(row, dict):
                for k in row.keys():
                    if k not in keys:
                        keys.append(k)
        return {
            "success": True,
            "records": rows,
            "keys": keys,
            "metadata": {"gremlin": script},
        }

    # -- schema -------------------------------------------------------------

    def _get_schema(self) -> Dict[str, Any]:
        if self._schema_cache is not None:
            return self._schema_cache
        r = self._session.get(
            f"{self.base_url}/graphs/{self.graph}/schema", timeout=self.timeout
        )
        r.raise_for_status()
        self._schema_cache = r.json()
        return self._schema_cache

    def property_key_type(self, name: str) -> Optional[str]:
        """Return the declared HG data type of a property key, or ``None``."""
        schema = self._get_schema()
        for pk in schema.get("propertykeys", []):
            if pk.get("name") == name:
                return pk.get("data_type")
        return None

    def ensure_property_key(
        self, name: str, data_type: str = "TEXT", cardinality: str = "SINGLE"
    ) -> None:
        """Create a property key if missing.

        If the key already exists with a *different* data type, HG rejects the
        re-creation and writes of the new type then fail with e.g.
        ``Invalid property value ... expect a value of type String, actual type
        Double``. Since a property key's type cannot be altered in place, the
        mismatch is logged and left as-is.
        """
        schema = self._get_schema()
        existing = {pk["name"]: pk for pk in schema.get("propertykeys", [])}
        if name in existing:
            declared = (existing[name].get("data_type") or "").upper()
            if declared and declared != data_type.upper():
                self.logger.debug(
                    f"property key '{name}' already declared as {declared}, "
                    f"requested {data_type}; keeping {declared}"
                )
            return
        body = {
            "name": name,
            "data_type": data_type,
            "cardinality": cardinality,
        }
        r = self._session.post(
            f"{self.base_url}/graphs/{self.graph}/schema/propertykeys",
            data=json.dumps(body),
            timeout=self.timeout,
        )
        # A key created concurrently (or by another writer) comes back as
        # "has existed"; that is not an error for an ensure_* call.
        if not r.ok and "ExistedException" not in r.text:
            r.raise_for_status()
        self._schema_cache = None

    def ensure_vertex_label(
        self,
        label: str,
        primary_keys: Optional[List[str]] = None,
        properties: Optional[List[str]] = None,
        id_strategy: str = "CUSTOMIZE_STRING",
    ) -> None:
        """Create a vertex label if missing.

        ``properties`` is the full set of property keys the label may carry
        (must include the primary keys). If omitted, it defaults to the primary
        keys. HG rejects any property not declared on the label at write time.

        ``id_strategy`` selects how vertex ids are assigned:

        * ``"CUSTOMIZE_STRING"`` (default) — the caller supplies the id, so the
          vertex id equals Semantica's logical entity id (``person_alice``).
          This is what makes :meth:`create_relationship` able to resolve the
          logical ids that :class:`~semantica.kg.graph_builder.GraphBuilder`
          passes as edge endpoints.
        * ``"PRIMARY_KEY"`` — id is derived from ``primary_keys``.
        * ``"DEFAULT"`` — HG assigns an opaque numeric id.

        .. note:: Changing ``id_strategy`` for an *existing* label is a no-op
            (HG does not allow altering it); delete the label first.
        """
        schema = self._get_schema()
        existing = {vl["name"]: vl for vl in schema.get("vertexlabels", [])}
        if label in existing:
            # HG can extend an existing label's property set via
            # PUT /schema/vertexlabels/{label}?action=append. Do that when the
            # caller needs keys the label does not declare yet — otherwise
            # writes fail with "Undefined property key".
            if properties:
                declared = set(existing[label].get("properties") or [])
                missing = [p for p in properties if p not in declared]
                if missing:
                    for p in missing:
                        self.ensure_property_key(p, "TEXT", "SINGLE")
                    r = self._session.put(
                        f"{self.base_url}/graphs/{self.graph}/schema/vertexlabels/"
                        f"{label}?action=append",
                        # HG validates the label name on append too; omitting it
                        # yields "The name of vertex label can't be null".
                        # Appended properties must be declared nullable, else
                        # it fails with "The new added properties: [...] must
                        # be nullable" (existing vertices lack the key).
                        data=json.dumps({
                            "name": label,
                            "properties": missing,
                            "nullable_keys": missing,
                        }),
                        timeout=self.timeout,
                    )
                    if r.ok:
                        self._schema_cache = None
                    else:
                        self.logger.warning(
                            f"could not extend label {label} with {missing}: "
                            f"{r.status_code} {r.text[:200]}"
                        )
            return
        primary_keys = primary_keys or []
        if properties is None:
            properties = list(primary_keys)
        else:
            # Always include primary keys in the declared property set.
            properties = list(dict.fromkeys(list(primary_keys) + list(properties)))
        if id_strategy == "PRIMARY_KEY" and not primary_keys:
            id_strategy = "DEFAULT"
        # Declare non-primary-key properties nullable: entities of the same
        # label routinely carry different property subsets, and non-nullable
        # keys make writes that omit them fail.
        nullable = [p for p in properties if p not in primary_keys]
        body = {
            "name": label,
            "id_strategy": id_strategy,
            "primary_keys": primary_keys,
            "properties": properties,
            "nullable_keys": nullable,
            "enable_label_index": True,
        }
        r = self._session.post(
            f"{self.base_url}/graphs/{self.graph}/schema/vertexlabels",
            data=json.dumps(body),
            timeout=self.timeout,
        )
        if not r.ok and "ExistedException" not in r.text:
            r.raise_for_status()
        self._schema_cache = None

    def ensure_edge_label(
        self,
        label: str,
        source_labels: List[str],
        target_labels: List[str],
        properties: Optional[List[str]] = None,
    ) -> None:
        """Create an edge label if missing.

        If the label already exists but is missing property keys the caller now
        needs, it is dropped and recreated (safe only when no edges of that
        label exist yet — which is the case during ``create_relationship``
        before the edge is written). HG rejects any property not declared on the
        edge label at write time, just like vertex labels.
        """
        schema = self._get_schema()
        existing = {el["name"]: el for el in schema.get("edgelabels", [])}
        if label in existing:
            el = existing[label]
            declared = set(el.get("properties") or [])
            want_props = set(properties or [])
            # HG edge labels constrain source/target *labels*. The first
            # create_relationship using this label fixes the pair, so a later
            # edge between different labels is rejected. Widen the label to
            # the union and recreate when a new pair shows up.
            cur_src = {s for s in (el.get("source_label") or "").split(",") if s}
            cur_tgt = {t for t in (el.get("target_label") or "").split(",") if t}
            new_src = {s for s in source_labels if s}
            new_tgt = {t for t in target_labels if t}
            need_widen = bool(
                (new_src and not new_src.issubset(cur_src))
                or (new_tgt and not new_tgt.issubset(cur_tgt))
            )
            if need_widen:
                source_labels = sorted(cur_src | new_src)
                target_labels = sorted(cur_tgt | new_tgt)
                properties = sorted(declared | want_props)
                self._recreate_edge_label(
                    label, source_labels, target_labels, list(properties)
                )
                return
            if want_props and not want_props.issubset(declared):
                self._delete_edge_label(label)
                # fall through to recreate below with the new property set
            else:
                return
        body = {
            "name": label,
            "source_label": ",".join(source_labels),
            "target_label": ",".join(target_labels),
            "properties": list(properties or []),
            "frequency": "SINGLE",
        }
        r = self._session.post(
            f"{self.base_url}/graphs/{self.graph}/schema/edgelabels",
            data=json.dumps(body),
            timeout=self.timeout,
        )
        if not r.ok and "ExistedException" not in r.text:
            r.raise_for_status()
        self._schema_cache = None

    def _wait_task(self, task_id: Any, attempts: int = 60) -> Dict[str, Any]:
        """Block until an async HG task (e.g. schema delete) finishes.

        HG schema deletes are asynchronous and return a ``task_id``; recreating
        the same label before the delete completes yields "already existed".
        Poll ``/tasks/{id}`` until ``task_status`` becomes a terminal state.
        """
        import time

        url = f"{self.base_url}/graphs/{self.graph}/tasks/{task_id}"
        last: Dict[str, Any] = {}
        for _ in range(attempts):
            try:
                r = self._session.get(url, timeout=self.timeout)
                if r.ok:
                    last = r.json()
                    status = (last.get("task_status") or "").lower()
                    if status in ("success", "failed", "cancelled"):
                        return last
            except Exception:
                pass
            time.sleep(0.2)
        return last

    def _recreate_edge_label(
        self,
        label: str,
        source_labels: List[str],
        target_labels: List[str],
        properties: List[str],
    ) -> None:
        """Widen an existing edge label's source/target label pair.

        Deleting an edge label also deletes its edges, so existing edges are
        read out first and re-created afterwards. Used when a second
        ``create_relationship`` call uses the same label between different
        vertex labels than the first one did.
        """
        # Snapshot existing edges of this label before dropping it.
        saved: List[Dict[str, Any]] = []
        try:
            script = (
                f"{ALIAS}.E().hasLabel('{label}').limit(10000)"
                ".project('outV','inV','outVL','inVL','props')"
                ".by(outV().id()).by(inV().id())"
                ".by(outV().label()).by(inV().label()).by(valueMap())"
            )
            saved = self._gremlin(script).get("records") or []
        except Exception as e:  # pragma: no cover - best effort
            self.logger.warning(f"could not snapshot edges of {label}: {e}")

        self._delete_edge_label(label)
        body = {
            "name": label,
            "source_label": ",".join(source_labels),
            "target_label": ",".join(target_labels),
            "properties": properties,
            "frequency": "SINGLE",
        }
        try:
            r = self._session.post(
                f"{self.base_url}/graphs/{self.graph}/schema/edgelabels",
                data=json.dumps(body),
                timeout=self.timeout,
            )
            if not r.ok and "ExistedException" not in r.text:
                r.raise_for_status()
        except Exception as e:  # pragma: no cover
            raise ProcessingError(f"Failed to recreate edge label {label}: {e}")
        self._schema_cache = None

        # Restore the edges we had to drop with the label.
        restored = 0
        for e in saved:
            try:
                rb = {
                    "label": label,
                    "outV": e.get("outV"),
                    "inV": e.get("inV"),
                    "outVLabel": e.get("outVL"),
                    "inVLabel": e.get("inVL"),
                    "properties": e.get("props") or {},
                }
                rr = self._session.post(
                    f"{self.base_url}/graphs/{self.graph}/graph/edges",
                    data=json.dumps(rb),
                    timeout=self.timeout,
                )
                if rr.ok:
                    restored += 1
            except Exception:
                pass
        if saved:
            self.logger.info(
                f"widened edge label {label}: restored {restored}/{len(saved)} edges"
            )

    def _delete_edge_label(self, label: str) -> None:
        """Drop an edge label (only valid when no edges use it).

        HG deletes asynchronously, so we wait for the returned task to finish
        before callers (e.g. ``ensure_edge_label`` recreating it) proceed.
        """
        try:
            r = self._session.delete(
                f"{self.base_url}/graphs/{self.graph}/schema/edgelabels/{label}",
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            task_id = data.get("task_id")
            if task_id is not None:
                self._wait_task(task_id)
        except Exception as e:  # pragma: no cover - best effort
            self.logger.warning(f"_delete_edge_label {label} failed (ignored): {e}")
        self._schema_cache = None

    # -- CRUD: vertices -----------------------------------------------------

    def create_node(
        self,
        labels: List[str],
        properties: Dict[str, Any],
        **options,
    ) -> Dict[str, Any]:
        """Create a vertex via REST ``POST /graphs/{graph}/graph/vertices``."""
        tracking_id = self.progress_tracker.start_tracking(
            module="graph_store",
            submodule="HugeGraphStore",
            message=f"Creating vertex with labels {labels}",
        )
        try:
            label = sanitize_identifier(labels[0], "label") if labels else "DEFAULT"
            # Semantica's pipeline carries a logical entity id in properties
            # (e.g. "person_alice") and later uses *that same value* as the
            # edge endpoint. Under the default CUSTOMIZE_STRING strategy we
            # make the HG vertex id equal that logical id, so the round-trip
            # create_node(...) -> create_relationship(src_logical, ...) works.
            # Without it, HG would mint an opaque numeric id and every later
            # edge built from the logical id would fail to resolve.
            logical_id = properties.get("id")
            if logical_id is None:
                logical_id = (options or {}).get("id") or (options or {}).get("node_id")

            props = dict(properties)
            if logical_id is not None:
                logical_id = str(logical_id)
                props.setdefault("id", logical_id)

            # Ensure every property key exists, then create the label with the
            # full property set so writes of any of them succeed.
            for pk_name, pk_val in props.items():
                data_type = _hg_data_type(pk_val)
                self.ensure_property_key(pk_name, data_type, "SINGLE")
            self.ensure_vertex_label(
                label,
                primary_keys=None,
                properties=list(props.keys()),
                id_strategy="CUSTOMIZE_STRING" if logical_id is not None else "DEFAULT",
            )
            props = self._coerce(props)
            body = {"label": label, "properties": props}
            if logical_id is not None:
                body["id"] = logical_id
            r = self._session.post(
                f"{self.base_url}/graphs/{self.graph}/graph/vertices",
                data=json.dumps(body),
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            vid = data.get("id", "")
            vlabel = data.get("label", label)
            # With CUSTOMIZE_STRING the vertex id *is* the logical id, so it is
            # returned verbatim (no "<id>:<label>" compound) — that keeps it
            # directly usable as an edge endpoint. Under DEFAULT strategy HG
            # hands back an opaque numeric id, which we keep as-is.
            if logical_id is None:
                # DEFAULT strategy: HG returns a numeric id; keep it usable.
                returned_id = vid if not vlabel else f"{vid}:{vlabel}"
            else:
                returned_id = str(vid)
            self.progress_tracker.stop_tracking(
                tracking_id, status="completed", message=f"Created vertex {returned_id}"
            )
            return {
                "id": returned_id,
                "label": vlabel,
                "properties": data.get("properties", props),
            }
        except Exception as e:
            self.progress_tracker.stop_tracking(
                tracking_id, status="failed", message=str(e)
            )
            raise ProcessingError(f"Failed to create vertex: {e}")

    def create_nodes(
        self, nodes: List[Dict[str, Any]], **options
    ) -> List[Dict[str, Any]]:
        """Batch-create vertices (sequential REST POSTs)."""
        tracking_id = self.progress_tracker.start_tracking(
            module="graph_store",
            submodule="HugeGraphStore",
            message=f"Creating {len(nodes)} vertices in batch",
        )
        try:
            created: List[Dict[str, Any]] = []
            for node in nodes:
                labels = node.get("labels", [])
                properties = node.get("properties", {})
                created.append(self.create_node(labels, properties, **options))
            self.progress_tracker.stop_tracking(
                tracking_id, status="completed", message=f"Created {len(created)} vertices"
            )
            return created
        except Exception as e:
            self.progress_tracker.stop_tracking(
                tracking_id, status="failed", message=str(e)
            )
            raise ProcessingError(f"Failed to create vertices: {e}")

    def _coerce(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Coerce property values to the data types HG has declared.

        Property-key types are fixed at creation, so a later write with a
        differently-typed value (e.g. ``{"since": 2024}`` against a key first
        created as TEXT) fails with ``Invalid property value``. Coercing here
        keeps such writes working.
        """
        if not properties:
            return properties
        declared = {
            k: t
            for k in properties
            if (t := self.property_key_type(k)) is not None
        }
        return _coerce_props_to_schema(properties, declared)

    def _lookup_vertex_label(self, node_id: Any) -> str:
        """Resolve a vertex's label via Gremlin, for ids that carry no label.

        Logical (``CUSTOMIZE_STRING``) ids like ``person_alice`` have no
        ``:label`` suffix, but the REST edge endpoint still requires
        ``outVLabel``/``inVLabel``. Returns ``""`` when the vertex cannot be
        found (the caller then falls back to whatever HG infers).
        """
        try:
            script = f"{ALIAS}.V({_v_arg(node_id)}).label()"
            data = self._gremlin_raw(script)
            rows = (data.get("result") or {}).get("data") or []
            if rows:
                return str(rows[0])
        except Exception as e:  # pragma: no cover - best effort
            self.logger.debug(f"_lookup_vertex_label({node_id}) failed: {e}")
        return ""

    def get_node(self, node_id: str, **options) -> Optional[Dict[str, Any]]:
        """Get a vertex by id.

        Accepts either a logical id (``person_alice``) or a DEFAULT-strategy
        numeric/compound id. Logical string ids are JSON-quoted before being
        URL-encoded, which is what HG's ``checkAndParseVertexId`` expects.
        """
        try:
            encoded = _rest_id(node_id)
            r = self._session.get(
                f"{self.base_url}/graphs/{self.graph}/graph/vertices/{encoded}",
                timeout=self.timeout,
            )
            if r.status_code == 404:
                return None
            r.raise_for_status()
            data = r.json()
            return {
                "id": data.get("id", node_id),
                "label": data.get("label"),
                "properties": data.get("properties", {}),
            }
        except Exception as e:  # pragma: no cover
            raise ProcessingError(f"Failed to get vertex {node_id}: {e}")

    def get_nodes(
        self,
        labels: Optional[List[str]] = None,
        properties: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        **options,
    ) -> List[Dict[str, Any]]:
        """List vertices via Gremlin (optionally filtered)."""
        parts = [f"{ALIAS}.V()"]
        if labels:
            lbl = sanitize_identifier(labels[0], "label")
            parts.append(f".hasLabel('{lbl}')")
        if properties:
            for k, v in properties.items():
                parts.append(f".has('{k}', {json.dumps(v)})")
        parts.append(f".limit({int(limit)}).valueMap()")
        script = "".join(parts)
        return self._gremlin(script).get("records", [])

    def update_node(
        self, node_id: str, properties: Dict[str, Any], merge: bool = True, **options
    ) -> Dict[str, Any]:
        """Update a vertex's properties via REST PUT ``?action=append``.

        HugeGraph requires an updated property key to be declared **both**
        globally and on the vertex's own label; otherwise it fails with
        ``Can't update property for vertex 'x' because there is no property key
        'p' in its vertex label``. So the key is declared globally and then
        appended to the label before the write.
        """
        # Declare any brand-new property keys first, then make sure the
        # vertex's own label carries them.
        for pk_name, pk_val in properties.items():
            data_type = _hg_data_type(pk_val)
            self.ensure_property_key(pk_name, data_type, "SINGLE")
        label = self._lookup_vertex_label(node_id)
        if label:
            self.ensure_vertex_label(
                label, properties=list(properties.keys()),
                id_strategy="CUSTOMIZE_STRING",
            )
        # The key may pre-exist with a different type; coerce so the write
        # does not fail on a type mismatch.
        properties = self._coerce(properties)
        encoded = _rest_id(node_id)
        r = self._session.put(
            f"{self.base_url}/graphs/{self.graph}/graph/vertices/{encoded}?action=append",
            data=json.dumps({"properties": properties}),
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "id": data.get("id", node_id),
            "label": data.get("label"),
            "properties": data.get("properties", properties),
        }

    def delete_node(self, node_id: str, detach: bool = True, **options) -> bool:
        """Delete a vertex."""
        encoded = _rest_id(node_id)
        r = self._session.delete(
            f"{self.base_url}/graphs/{self.graph}/graph/vertices/{encoded}",
            timeout=self.timeout,
        )
        r.raise_for_status()
        return True

    # -- CRUD: relationships ------------------------------------------------

    def create_relationship(
        self,
        start_node_id: str,
        end_node_id: str,
        rel_type: str,
        properties: Optional[Dict[str, Any]] = None,
        **options,
    ) -> Dict[str, Any]:
        """Create an edge via REST ``POST /graphs/{graph}/graph/edges``.

        ``start_node_id`` / ``end_node_id`` are the ids returned by
        :meth:`create_node` — normally Semantica's **logical entity ids**
        (``person_alice``) under the default ``CUSTOMIZE_STRING`` strategy, and
        ``<numeric>:<label>`` compound ids under the ``DEFAULT`` strategy.
        ``rel_type`` is the edge label.

        Endpoint encoding is subtle: the REST edge endpoint wants the **bare**
        id (a JSON string for logical ids, a JSON number for numeric ids). It
        rejects the JSON-quoted form used by the vertex GET path with
        ``Invalid vertex id '"person_alice"'``.
        """
        tracking_id = self.progress_tracker.start_tracking(
            module="graph_store",
            submodule="HugeGraphStore",
            message=f"Creating edge [{rel_type}]",
        )
        try:
            properties = properties or {}
            safe_label = sanitize_identifier(rel_type, "edge label")
            # The REST edge endpoint wants the bare vertex id (string for
            # logical ids, number for numeric ones) plus separate label fields.
            src_numeric, src_label = _split_compound(start_node_id)
            dst_numeric, dst_label = _split_compound(end_node_id)
            # Compound ids carry their own label; logical ids do not, and HG
            # needs outVLabel/inVLabel to resolve them. Recover the labels by
            # looking the endpoints up.
            if not src_label:
                src_label = self._lookup_vertex_label(start_node_id)
            if not dst_label:
                dst_label = self._lookup_vertex_label(end_node_id)
            # A missing endpoint means the vertex does not exist (LLM-extracted
            # relations often reference entities that entity resolution dropped
            # or never materialised). HG would reject this with an opaque 400
            # on the schema endpoint, so raise a precise error instead — the
            # caller (GraphStore.add_edges) skips it and counts the skip.
            if not src_label or not dst_label:
                missing = [
                    n for n, l in ((start_node_id, src_label),
                                   (end_node_id, dst_label)) if not l
                ]
                raise ProcessingError(
                    f"cannot create edge [{safe_label}]: endpoint vertex not "
                    f"found: {missing}. The vertex was never created (dangling "
                    f"reference from entity resolution / LLM extraction)."
                )
            # Declare every property key the edge carries, then ensure the edge
            # label exists with those keys (drop+recreate if it was declared
            # without them).
            for pk_name, pk_val in properties.items():
                data_type = _hg_data_type(pk_val)
                self.ensure_property_key(pk_name, data_type, "SINGLE")
            self.ensure_edge_label(
                safe_label, [src_label], [dst_label], properties=list(properties.keys())
            )
            properties = self._coerce(properties)
            body = {
                "label": safe_label,
                "outV": src_numeric,
                "inV": dst_numeric,
                "outVLabel": src_label,
                "inVLabel": dst_label,
                "properties": properties,
            }
            r = self._session.post(
                f"{self.base_url}/graphs/{self.graph}/graph/edges",
                data=json.dumps(body),
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            self.progress_tracker.stop_tracking(
                tracking_id, status="completed", message=f"Created edge {data.get('id')}"
            )
            return {
                "id": data.get("id", ""),
                "type": safe_label,
                "start_node_id": str(start_node_id),
                "end_node_id": str(end_node_id),
                "properties": data.get("properties", properties),
            }
        except Exception as e:
            self.progress_tracker.stop_tracking(
                tracking_id, status="failed", message=str(e)
            )
            raise ProcessingError(f"Failed to create edge: {e}")

    def get_relationships(
        self,
        node_id: Optional[str] = None,
        rel_type: Optional[str] = None,
        direction: str = "both",
        limit: int = 100,
        **options,
    ) -> List[Dict[str, Any]]:
        """Get edges (and their endpoints) via Gremlin."""
        v = _v_arg(node_id)
        if direction == "in":
            edge_step = f"{ALIAS}.V({v}).inE()"
        elif direction == "out":
            edge_step = f"{ALIAS}.V({v}).outE()"
        else:
            edge_step = f"{ALIAS}.V({v}).bothE()"
        parts = [
            edge_step,
            f".hasLabel('{rel_type}')" if rel_type else "",
            f".limit({int(limit)})",
            ".project('id','label','outV','inV','properties')",
            ".by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())",
        ]
        script = "".join(p for p in parts if p)
        return self._gremlin(script).get("records", [])

    def delete_relationship(self, rel_id: str, **options) -> bool:
        """Delete an edge by ID."""
        encoded = quote(str(rel_id), safe="")
        r = self._session.delete(
            f"{self.base_url}/graphs/{self.graph}/graph/edges/{encoded}",
            timeout=self.timeout,
        )
        r.raise_for_status()
        return True

    # -- query / traversal --------------------------------------------------

    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        **options,
    ) -> Dict[str, Any]:
        """Execute a Gremlin script.

        HugeGraph 1.7's Gremlin Server speaks Gremlin, not Cypher. ``query`` is
        therefore treated as a Gremlin traversal/script. Cypher→Gremlin
        translation is out of scope for this adapter and should be done by the
        caller (e.g. Semantica's Cypher layer) if needed.
        """
        return self._gremlin(query)

    def get_neighbors(
        self,
        node_id: str,
        rel_type: Optional[str] = None,
        direction: str = "both",
        depth: int = 1,
        **options,
    ) -> List[Dict[str, Any]]:
        """Get neighbouring vertices via Gremlin."""
        rel = f"'{rel_type}'" if rel_type else ""
        v = _v_arg(node_id)
        if direction == "in":
            script = f"{ALIAS}.V({v}).inE({rel}).outV().limit(100).valueMap()"
        elif direction == "out":
            script = f"{ALIAS}.V({v}).outE({rel}).inV().limit(100).valueMap()"
        else:
            script = (
                f"{ALIAS}.V({v}).bothE({rel}).otherV().limit(100).valueMap()"
            )
        return self._gremlin(script).get("records", [])

    def shortest_path(
        self,
        start_node_id: str,
        end_node_id: str,
        rel_type: Optional[str] = None,
        max_depth: int = 10,
        **options,
    ) -> Optional[Dict[str, Any]]:
        """Find shortest path using HugeGraph's SHORTEST_PATH algorithm."""
        rel = rel_type or "*"
        vs = _v_arg(start_node_id)
        ve = _v_arg(end_node_id)
        script = (
            f"{ALIAS}.V({vs})"
            f".shortestPath({ALIAS}.V({ve}), '{rel}', "
            f"'OUT','{int(max_depth)}','1')"
        )
        result = self._gremlin(script)
        records = result.get("records", [])
        if not records:
            return None
        return {"path": records}

    # -- management ---------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Return basic graph statistics via Gremlin."""
        result = self._gremlin(f"{ALIAS}.V().count()")
        v_count = (result.get("records") or [0])[0] if result.get("records") else 0
        e_result = self._gremlin(f"{ALIAS}.E().count()")
        e_count = (e_result.get("records") or [0])[0] if e_result.get("records") else 0
        return {"vertex_count": v_count, "edge_count": e_count}

    def create_index(
        self,
        label: str,
        property_name: str,
        index_type: str = "secondary",
        base_type: str = "VERTEX_LABEL",
        **options,
    ) -> bool:
        """Create an index on a property (best-effort).

        ``base_type`` is ``VERTEX_LABEL`` for vertex indexes or ``EDGE_LABEL``
        for edge indexes. HG rejects property-filter traversals (``.has('p', v)``)
        on properties that are not indexed, so callers should index queryable
        properties up front.
        """
        url = f"{self.base_url}/graphs/{self.graph}/schema/indexlabels"
        body = {
            "name": f"{label}_{property_name}_idx",
            "base_type": base_type,
            "base_value": label,
            "index_type": index_type.upper(),
            "fields": [property_name],
            "check_exist": True,
        }
        try:
            r = self._session.post(
                url, data=json.dumps(body), timeout=self.timeout
            )
            r.raise_for_status()
            data = r.json()
            task_id = data.get("task_id")
            if task_id is not None:
                self._wait_task(task_id)
            self._schema_cache = None
            return True
        except Exception as e:
            self.logger.warning(f"create_index failed (ignored): {e}")
            return False

    # -- context manager ----------------------------------------------------

    def __enter__(self) -> "HugeGraphStore":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

import os
import json
from typing import List, Optional, Dict, Any
from pathlib import Path
import re

import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, Response, StreamingResponse
import html as html_escape


OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
INDEX_NAME = os.environ.get("INDEX_NAME", "ndf-docs")
DOC_ROOT = os.environ.get("DOC_ROOT", str(Path(__file__).resolve().parent.parent / "data" / "Note de frais"))
ALLOW_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx"}

app = FastAPI(title="NDF Search API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _terms_filter(field: str, values: Optional[List[str]]) -> Optional[Dict[str, Any]]:
    if not values:
        return None
    expanded: List[str] = []
    for v in values:
        if not v:
            continue
        expanded.extend([s.strip() for s in v.split(",") if s.strip()])
    if not expanded:
        return None
    return {"terms": {field: expanded}}


def _build_search_body(
    q: str,
    level1: Optional[List[str]],
    level2: Optional[List[str]],
    from_: int,
    size: int,
    sort: Optional[str],
) -> Dict[str, Any]:
    must: List[Dict[str, Any]] = []
    should: List[Dict[str, Any]] = []
    if q:
        must.append({
            "multi_match": {
                "query": q,
                "fields": ["title^3", "content"],
            }
        })
        # Astuce: pour la tolérance de casse/typo, on reste sur des champs analysés
        # et on ajoute un peu de fuzziness
        must[-1]["multi_match"]["fuzziness"] = "AUTO"
    filters: List[Dict[str, Any]] = []
    t1 = _terms_filter("level1", level1)
    t2 = _terms_filter("level2", level2)
    if t1:
        filters.append(t1)
    if t2:
        filters.append(t2)

    body: Dict[str, Any] = {
        "from": from_,
        "size": size,
        "_source": ["title", "path", "file_name", "level1", "level2", "modified_at", "ext"],
        "query": {"bool": {"must": (must if must else [{"match_all": {}}]), "should": should, "minimum_should_match": 0, "filter": filters}},
        "aggs": {
            "by_level1": {"terms": {"field": "level1", "size": 200}},
            "by_level2": {"terms": {"field": "level2", "size": 200}},
        },
        "highlight": {
            "fields": {
                "title": {},
                "content": {"fragment_size": 160, "number_of_fragments": 1}
            },
            "pre_tags": ["<mark>"],
            "post_tags": ["</mark>"],
        },
    }
    if sort == "recency":
        body["sort"] = [{"modified_at": {"order": "desc"}}]
    return body


def _ensure_highlight_settings():
    try:
        # Augmente la limite d'analyse pour le highlight des champs longs
        requests.put(
            f"{OPENSEARCH_URL}/{INDEX_NAME}/_settings",
            json={"index.highlight.max_analyzed_offset": 5000000},
            timeout=5,
        )
    except Exception:
        pass


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/search")
def search(
    q: str = "",
    level1: Optional[List[str]] = Query(default=None),
    level2: Optional[List[str]] = Query(default=None),
    from_: int = Query(default=0, alias="from"),
    size: int = 20,
    sort: Optional[str] = None,
):
    _ensure_highlight_settings()
    body = _build_search_body(q, level1, level2, from_, size, sort)
    resp = requests.post(f"{OPENSEARCH_URL}/{INDEX_NAME}/_search", json=body, timeout=30)
    if not resp.ok:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@app.get("/api/filters")
def filters():
    body = {
        "size": 0,
        "aggs": {
            "by_level1": {"terms": {"field": "level1", "size": 200}},
            "by_level2": {"terms": {"field": "level2", "size": 200}},
        },
    }
    resp = requests.post(f"{OPENSEARCH_URL}/{INDEX_NAME}/_search", json=body, timeout=30)
    if not resp.ok:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json().get("aggregations", {})


@app.get("/api/suggest")
def suggest(q: str):
    # search_as_you_type via bool_prefix
    body = {
        "size": 5,
        "_source": ["title", "level1", "level2"],
        "query": {
            "multi_match": {
                "query": q,
                "type": "bool_prefix",
                "fields": [
                    "suggest",
                    "suggest._2gram",
                    "suggest._3gram"
                ]
            }
        }
    }
    resp = requests.post(f"{OPENSEARCH_URL}/{INDEX_NAME}/_search", json=body, timeout=15)
    if not resp.ok:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    hits = resp.json().get("hits", {}).get("hits", [])
    return [{"id": h.get("_id"), **h.get("_source", {})} for h in hits]


@app.get("/api/document/{doc_id}")
def get_document(doc_id: str):
    resp = requests.get(f"{OPENSEARCH_URL}/{INDEX_NAME}/_doc/{doc_id}", timeout=15)
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Not found")
    if not resp.ok:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    data = resp.json()
    return data.get("_source", data)


@app.get("/api/file")
def get_file(path: str):
    # Sécuriser: le fichier doit être sous DOC_ROOT
    try:
        abs_path = Path(path).resolve()
        root = Path(DOC_ROOT).resolve()
        abs_path.relative_to(root)
    except Exception:
        raise HTTPException(status_code=403, detail="Chemin non autorisé")
    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(status_code=404, detail="Fichier introuvable")
    ext = abs_path.suffix.lower()
    mime_map = {
        ".pdf": "application/pdf",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    media_type = mime_map.get(ext, "application/octet-stream")
    # Fournir le bon nom de fichier pour le téléchargement (attachment)
    return FileResponse(str(abs_path), media_type=media_type, filename=abs_path.name)


@app.get("/api/file/inline")
def get_file_inline(path: str):
    # Sert le fichier en mode inline (pour l'aperçu) sans forcer le téléchargement
    try:
        abs_path = Path(path).resolve()
        root = Path(DOC_ROOT).resolve()
        abs_path.relative_to(root)
    except Exception:
        raise HTTPException(status_code=403, detail="Chemin non autorisé")
    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(status_code=404, detail="Fichier introuvable")
    ext = abs_path.suffix.lower()
    mime_map = {
        ".pdf": "application/pdf",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    media_type = mime_map.get(ext, "application/octet-stream")
    def file_iterator(chunk_size: int = 8192):
        with open(abs_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    headers = {"Content-Disposition": f"inline; filename=\"{abs_path.name}\""}
    return StreamingResponse(file_iterator(), media_type=media_type, headers=headers)


@app.get("/api/file/html")
def get_file_as_html(path: str, q: Optional[str] = None):
    # Rend .xlsx/.xls en HTML via openpyxl/xlrd
    try:
        abs_path = Path(path).resolve()
        root = Path(DOC_ROOT).resolve()
        abs_path.relative_to(root)
    except Exception:
        raise HTTPException(status_code=403, detail="Chemin non autorisé")
    if not abs_path.exists() or not abs_path.is_file():
        raise HTTPException(status_code=404, detail="Fichier introuvable")
    ext = abs_path.suffix.lower()
    if ext not in [".xlsx", ".xls"]:
        raise HTTPException(status_code=400, detail="Format non supporté pour rendu HTML")
    try:
        if ext == ".xlsx":
            import openpyxl
            wb = openpyxl.load_workbook(str(abs_path), data_only=True, read_only=True)
            ws = wb.active
            rows = []
            for row in ws.iter_rows(values_only=True):
                rows.append(["" if v is None else str(v) for v in row])
        else:
            import xlrd
            wb = xlrd.open_workbook(str(abs_path))
            sh = wb.sheet_by_index(0)
            rows = []
            for rx in range(sh.nrows):
                rows.append(["" if sh.cell_value(rx, cx) is None else str(sh.cell_value(rx, cx)) for cx in range(sh.ncols)])
        # Convertir en HTML simple avec échappement
        html_rows = []
        def highlight_cell(text: str, query: Optional[str]) -> str:
            escaped = html_escape.escape(text)
            if not query:
                return escaped
            try:
                pattern = re.compile(re.escape(query), re.IGNORECASE)
                return pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", escaped)
            except Exception:
                return escaped

        for r in rows:
            tds = "".join(f"<td>{highlight_cell(cell, q)}</td>" for cell in r)
            html_rows.append(f"<tr>{tds}</tr>")
        html = (
            "<!doctype html><html><head><meta charset='utf-8'><style>"
            "table{width:100%;border-collapse:collapse}"
            "td,th{border:1px solid #e5e7eb;padding:4px}"
            "</style></head><body><table>"
            + "\n".join(html_rows)
            + "</table></body></html>"
        )
        return Response(content=html, media_type="text/html")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur rendu XLS: {e}")


@app.get("/api/viewer")
def get_viewer(file: str, q: Optional[str] = None):
    # Redirige vers un viewer PDF.js hébergé sur CDN fiable
    cdn = "https://cdn.jsdelivr.net/npm/pdfjs-dist@4.6.82/web/viewer.html?file="
    hash_part = f"#search={q}" if q else ""
    return RedirectResponse(url=f"{cdn}{file}{hash_part}")


def _is_hidden(path: Path) -> bool:
    return any(part.startswith(".") for part in path.parts)


def _scan_disk_stats(root_path: Path) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "root": str(root_path),
        "total": 0,
        "by_level1": {},
        "by_level2": {},
        "by_level1_level2": {},
    }
    if not root_path.exists():
        return stats
    for level1_dir in root_path.iterdir():
        if not level1_dir.is_dir() or _is_hidden(level1_dir):
            continue
        level1 = level1_dir.name
        lvl1_total = 0
        for level2_dir in level1_dir.iterdir():
            if not level2_dir.is_dir() or _is_hidden(level2_dir):
                continue
            level2 = level2_dir.name
            lvl2_count = 0
            for file_path in level2_dir.rglob("*"):
                if not file_path.is_file() or _is_hidden(file_path) or file_path.name.startswith("~$"):
                    continue
                if file_path.suffix.lower() not in ALLOW_EXTS:
                    continue
                stats["total"] += 1
                lvl1_total += 1
                lvl2_count += 1
                stats["by_level2"][level2] = stats["by_level2"].get(level2, 0) + 1
            if lvl2_count:
                if level1 not in stats["by_level1_level2"]:
                    stats["by_level1_level2"][level1] = {}
                stats["by_level1_level2"][level1][level2] = lvl2_count
        if lvl1_total:
            stats["by_level1"][level1] = stats["by_level1"].get(level1, 0) + lvl1_total
    return stats


def _fetch_index_stats() -> Dict[str, Any]:
    body = {
        "size": 0,
        "aggs": {
            "by_level1": {"terms": {"field": "level1", "size": 500}},
            "by_level2": {"terms": {"field": "level2", "size": 1000}},
        },
    }
    r = requests.post(f"{OPENSEARCH_URL}/{INDEX_NAME}/_search", json=body, timeout=30)
    r.raise_for_status()
    data = r.json()
    total = data.get("hits", {}).get("total", {})
    total_value = total.get("value", total if isinstance(total, int) else 0)
    agg1 = {b["key"]: b["doc_count"] for b in data.get("aggregations", {}).get("by_level1", {}).get("buckets", [])}
    agg2 = {b["key"]: b["doc_count"] for b in data.get("aggregations", {}).get("by_level2", {}).get("buckets", [])}
    return {"total": total_value, "by_level1": agg1, "by_level2": agg2}


@app.get("/api/stats")
def stats():
    disk = _scan_disk_stats(Path(DOC_ROOT))
    try:
        index = _fetch_index_stats()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenSearch error: {e}")

    def diff_maps(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
        keys = set(a.keys()) | set(b.keys())
        return {k: int(a.get(k, 0)) - int(b.get(k, 0)) for k in sorted(keys)}

    diff = {
        "total_missing": int(disk.get("total", 0)) - int(index.get("total", 0)),
        "by_level1_missing": diff_maps(disk.get("by_level1", {}), index.get("by_level1", {})),
        "by_level2_missing": diff_maps(disk.get("by_level2", {}), index.get("by_level2", {})),
    }
    return {"doc_root": disk.get("root"), "disk": disk, "index": index, "diff": diff}


# Frontend statique
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
else:
    @app.get("/", response_class=HTMLResponse)
    def root_html():
        return """<html><body><h1>NDF Search</h1><p>Frontend non construit.</p></body></html>"""



#!/usr/bin/env python3
import hashlib
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Iterator, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential
import click
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


ROOT = Path(os.environ.get("DOC_ROOT", "data/Note de frais")).resolve()
TIKA_URL = os.environ.get("TIKA_URL", "http://localhost:9998")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
INDEX_NAME = os.environ.get("INDEX_NAME", "ndf-docs")

ALLOW_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx"}
FILTER_LEVEL1 = {s.strip() for s in os.environ.get("FILTER_LEVEL1", "").split(",") if s.strip()}
FILTER_LEVEL2 = {s.strip() for s in os.environ.get("FILTER_LEVEL2", "").split(",") if s.strip()}
MAX_DOCS = int(os.environ.get("MAX_DOCS", "0") or 0)
MAX_OCR_MB = int(os.environ.get("MAX_OCR_MB", "30") or 30)


def is_hidden(path: Path) -> bool:
    return any(part.startswith(".") for part in path.parts)


def compute_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def to_iso(dt: float) -> str:
    return datetime.fromtimestamp(dt, tz=timezone.utc).isoformat()


def iter_documents() -> Iterator[Dict[str, str]]:
    if not ROOT.exists():
        return
    yielded = 0
    for level1_dir in ROOT.iterdir():
        if not level1_dir.is_dir() or is_hidden(level1_dir):
            continue
        level1 = level1_dir.name
        if FILTER_LEVEL1 and level1 not in FILTER_LEVEL1:
            continue

        for level2_dir in level1_dir.iterdir():
            if not level2_dir.is_dir() or is_hidden(level2_dir):
                continue
            level2 = level2_dir.name
            if FILTER_LEVEL2 and level2 not in FILTER_LEVEL2:
                continue

            for file_path in level2_dir.rglob("*"):
                if not file_path.is_file():
                    continue
                if is_hidden(file_path) or file_path.name.startswith("~$"):
                    continue
                ext = file_path.suffix.lower()
                if ext not in ALLOW_EXTS:
                    continue

                rel_sub = str(file_path.parent.relative_to(level2_dir)) if file_path.parent != level2_dir else ""

                yielded += 1
                yield {
                    "path": str(file_path),
                    "file_name": file_path.name,
                    "level1": level1,
                    "level2": level2,
                    "ext": ext.lstrip("."),
                    "relative_subpath": rel_sub,
                }
                if MAX_DOCS and yielded >= MAX_DOCS:
                    return


@retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(5))
def tika_extract_text(file_path: Path) -> Dict[str, Optional[str]]:
    meta_resp = requests.put(f"{TIKA_URL}/meta", data=file_path.read_bytes(), headers={"Accept": "application/json"}, timeout=120)
    meta_resp.raise_for_status()
    try:
        metadata = meta_resp.json()
    except Exception:
        metadata = {}

    text_resp = requests.put(f"{TIKA_URL}/tika", data=file_path.read_bytes(), headers={"Accept": "text/plain"}, timeout=300)
    text_resp.raise_for_status()
    text = text_resp.text

    return {
        "title": (metadata.get("title") or metadata.get("dc:title") or "").strip() or file_path.stem,
        "content": text or "",
        "media_type": metadata.get("Content-Type") or metadata.get("Content-Type-Parsed") or "",
    }


def ensure_index():
    # create index if not exists
    resp = requests.get(f"{OPENSEARCH_URL}/{INDEX_NAME}")
    if resp.status_code == 200:
        return
    if resp.status_code == 404:
        try:
            mapping_file = Path(__file__).with_name("opensearch-index.json")
            body = json.loads(mapping_file.read_text(encoding="utf-8"))
        except Exception:
            body = {"mappings": {"properties": {"content": {"type": "text"}}}}
        create = requests.put(f"{OPENSEARCH_URL}/{INDEX_NAME}", json=body)
        create.raise_for_status()
        return
    resp.raise_for_status()


@retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(5))
def index_document(doc: Dict):
    # Utiliser un ID sans caractères spéciaux problématiques
    # pour éviter les soucis de routing/URL. Ici on prend le sha256 seul.
    doc_id = doc.get("sha256") or doc.get("id")
    resp = requests.put(f"{OPENSEARCH_URL}/{INDEX_NAME}/_doc/{doc_id}", json=doc, timeout=60)
    resp.raise_for_status()


def build_doc(payload: Dict[str, str]) -> Dict:
    p = Path(payload["path"]) 
    stat = p.stat()
    sha256 = compute_sha256(p)
    meta = {"title": p.stem, "content": "", "media_type": ""}
    # Skipper l'OCR si le fichier est trop volumineux
    try:
        size_mb = stat.st_size / (1024 * 1024)
        if size_mb <= MAX_OCR_MB:
            meta = tika_extract_text(p)
    except Exception:
        # Tolérant : on indexe quand même sans contenu
        pass

    suggest_terms = []
    if payload.get("level1"):
        suggest_terms.append(payload["level1"])
    if payload.get("level2"):
        suggest_terms.append(payload["level2"])
    if meta.get("title"):
        suggest_terms.append(meta["title"])

    doc = {
        "id": sha256,
        "path": payload["path"],
        "file_name": payload["file_name"],
        "level1": payload["level1"],
        "level2": payload["level2"],
        "title": meta.get("title") or Path(payload["file_name"]).stem,
        "content": meta.get("content") or "",
        "media_type": meta.get("media_type") or "",
        "ext": payload["ext"],
        "modified_at": to_iso(stat.st_mtime),
        "size_bytes": stat.st_size,
        "sha256": sha256,
        "suggest": list({t for t in suggest_terms if t}),
    }
    return doc

def derive_levels(file_path: Path) -> Optional[Dict[str, str]]:
    try:
        rel = file_path.resolve().relative_to(ROOT)
    except Exception:
        return None
    parts = rel.parts
    if len(parts) < 3:  # need at least level1/level2/filename
        return None
    return {"level1": parts[0], "level2": parts[1]}


def index_path(file_path: Path):
    if not file_path.exists() or not file_path.is_file():
        return
    ext = file_path.suffix.lower()
    if ext not in ALLOW_EXTS:
        return
    levels = derive_levels(file_path)
    if not levels:
        return
    payload = {
        "path": str(file_path),
        "file_name": file_path.name,
        "level1": levels["level1"],
        "level2": levels["level2"],
        "ext": ext.lstrip("."),
        "relative_subpath": "",
    }
    try:
        doc = build_doc(payload)
        index_document(doc)
        print(f"Indexé: {file_path}")
    except Exception as e:
        print(f"Erreur indexation {file_path}: {e}")


class IngestEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        index_path(Path(event.src_path))

    def on_modified(self, event):
        if event.is_directory:
            return
        index_path(Path(event.src_path))


@click.command()
@click.option("--watch", is_flag=True, default=False, help="Surveiller DOC_ROOT et indexer automatiquement")
@click.option("--initial", is_flag=True, default=True, help="Effectuer une indexation initiale")
def main(watch: bool, initial: bool):
    ensure_index()
    if initial:
        total = 0
        for payload in iter_documents():
            try:
                doc = build_doc(payload)
                index_document(doc)
                total += 1
                if total <= 10 or total % 50 == 0:
                    print(f"Indexés: {total} (dernier: {payload['file_name']})")
            except Exception as e:
                print(f"Erreur sur {payload['path']}: {e}")
        print(f"Indexation initiale: {total} documents")

    if watch:
        print(f"Watch mode ON sur: {ROOT}")
        handler = IngestEventHandler()
        observer = Observer()
        observer.schedule(handler, str(ROOT), recursive=True)
        observer.start()
        try:
            while True:
                observer.join(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.stop()
        observer.join()
        return
    if not initial and not watch:
        print("Rien à faire (ni --initial ni --watch)")


if __name__ == "__main__":
    main()



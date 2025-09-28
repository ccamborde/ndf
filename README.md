## Moteur de recherche – Notes de frais

### Prérequis
- Docker / Docker Compose
- Python 3.10+

### Démarrer les services
```
docker compose up -d
```
- OpenSearch: `http://localhost:9200`
- Dashboards: `http://localhost:5601`
- Tika Server: `http://localhost:9998`

### Créer l’index
```
curl -X PUT "http://localhost:9200/ndf-docs" -H 'Content-Type: application/json' --data-binary @opensearch-index.json
```

### Installer dépendances de l’ingester
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Lancer l’ingestion
Par défaut, l’ingester scanne `data/Note de frais/<N1>/<N2>/**` (récursif) et indexe PDF/DOC/DOCX/XLS/XLSX.

```
export TIKA_URL=http://localhost:9998
export OPENSEARCH_URL=http://localhost:9200
export INDEX_NAME=ndf-docs
python ingest.py
```

### Lancer l’API et le frontend
```
# Build et démarrage des services
docker compose up -d --build api opensearch tika

# API: http://localhost:8080/api/health
# Frontend: http://localhost:8080/
```

### Rechercher via l’API
```
curl -s "http://localhost:8080/api/search?q=repas&level1=Communaute%CC%81%20de%20communes&level2=Bordeaux" | jq '.hits.hits[0]._source'
```

### Filtrage dans l’API de recherche
- Tous: sans filtre.
- Niveau 1: filtrer `level1="Communauté de communes"` ou `"Conseils départementaux"`.
- Niveau 2: filtres combinés `level1=...` et `level2=...`.

### Notes
- Les sous-dossiers sous le Niveau 2 sont parcourus, mais n’ajoutent pas de sémantique (stockés en `relative_subpath`).
- Les fichiers cachés et temporaires Office (`~$`) sont ignorés.
- OCR via Tika/Tesseract pour PDF scannés (langue FR supportée par l’image Tika).



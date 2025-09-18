# Development

Contributions are welcome! The contribution guidelines will be formalized in a future release. In the meantime:

- Use the **dev environment** (`environment-dev.yml`) with editable install (`-e .`).
- Code style: `ruff` + `black` (run locally).
- Tests: add small **smoke tests** (graph/Dijkstra, impacts aggregation incl. `TCO`, minimal Folium export).
- Docstrings: **NumPy style**.

## Build the docs locally

```bash
# One-time
pip install -r docs/requirements-docs.txt

# Live preview (auto-reload)
mkdocs serve
# open http://127.0.0.1:8000
# - Rebuilds on changes in docs/, mkdocs.yml, and src/ (watch enabled)
```

## License & Changelog

- License: **MIT** (see `LICENSE`)
- Changelog: use GitHub Releases (e.g., `v1.1.2`, `v1.1.3`)

  - Files containing the number version/releases (to be modified accordingly):
    - `transnetmap-impacts/environment.yml`
    - `transnetmap-impacts/pyproject.toml`
    - `transnetmap-impacts/src/transnetmap/__init__.py`
    - `transnetmap-impacts/DESING.md`
    - `transnetmap-impacts/docs/installation.md`
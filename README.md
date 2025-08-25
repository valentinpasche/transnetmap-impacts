## Notes

> version 1.0.1

> This package requires access to a PostgreSQL database with the PostGIS extension enabled.

---

## Installation

#### Stable

```bash
conda env create -f environment.yml
conda activate transnetmap
python -c "import transnetmap as tnm; print('OK:', tnm.__name__)"
```

#### Development

```bash
conda env create -f environment-dev.yml
conda activate transnetmap-dev
python -c "import transnetmap as tnm; print('OK:', tnm.__name__)"
```

---


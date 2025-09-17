# Installation

Installing this package requires a Conda virtual environment to manage dependencies in isolation.  
It is not distributed on public channels such as **PyPI** (pip) or **Conda-Forge**.  

The stable environment installs the package from a **GitHub tag ZIP**,  
and all its dependencies from conda-forge.

The installation of dependencies is forced on `conda-forge` only (with `nodefaults`).  
This approach ensures a reproducible and simple installation for the user.

## Stable

```bash
# Creates the environment from the configuration file
conda env create -f environment.yml

# Activates the newly created environment
conda activate transnetmap

# Installation verification
python -c "import transnetmap as tnm; print('OK:', tnm.__name__)"
```

**Requirements**

- Python 3.12
- PostgreSQL with **PostGIS** enabled (not included)
- See `environment.yml` for the full dependency list (e.g., `networkx`, `geopandas`, `polars`, `folium`, `pyarrow`, ADBC drivers for Polars).

## Development (optional)

```bash
conda env create -f environment-dev.yml
conda activate transnetmap-dev
python -c "import transnetmap as tnm; print('OK:', tnm.__name__)"
```

> The dev environment installs the package in **editable** mode (`-e .`) and includes tooling (pytest, ruff, black, mkdocs).
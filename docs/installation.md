# Installation

Installing this package requires a **Conda** environment to manage dependencies in isolation.  
It is **not** distributed on public channels such as **PyPI** (pip) or **conda** main channels.

- The **stable** environment installs the package from a **GitHub tag ZIP** (via the `pip:` section in `environment.yml`).
- All binary dependencies come from **conda-forge** with `nodefaults` to ensure a reproducible setup.

## Stable

You can create the environment either from a local file or directly from a URL.

### Option A — Using the local file
```bash
# 1) Create the environment
conda env create -f environment.yml

# 2) Activate
conda activate transnetmap

# 3) Quick check
python -c "import transnetmap as tnm; print('OK:', tnm.__name__, 'version:', getattr(tnm, '__version__', '?'))"
```

### Option B — Using the file URL (no download needed)
```bash
# Replace v1.1.1 with the tag you want to install
conda env create --file https://raw.githubusercontent.com/valentinpasche/transnetmap-impacts/v1.1.1/environment.yml
conda activate transnetmap

python -c "import transnetmap as tnm; print('OK:', tnm.__name__, 'version:', getattr(tnm, '__version__', '?'))"
```

**Requirements**

- Python 3.12
- A **PostgreSQL** server with **PostGIS enabled** (not included)
- See environment.yml for the full dependency list (e.g. `networkx`, `geopandas`, `polars`, `folium`, `pyarrow`, ADBC drivers for Polars)

**Notes**

- `environment.yml` pins `channels: [conda-forge, nodefaults]` → no need to pass `-c` on the CLI.
- The package itself is installed from the **GitHub tag** referenced in `environment.yml`.

## Development (optional)

Use this if you plan to modify the code or build the docs locally.

```bash
# Clone (recommended)
git clone https://github.com/valentinpasche/transnetmap-impacts.git
cd transnetmap-impacts

# Or unzip the repository ZIP and cd into the project root

# Create the dev environment (does a pip editable install: -e .)
conda env create -f environment-dev.yml
conda activate transnetmap-dev

python -c "import transnetmap as tnm; print('OK:', tnm.__name__, 'version:', getattr(tnm, '__version__', '?'))"
```

### Docs locally (optional)

```bash
pip install -r docs/requirements-docs.txt
mkdocs serve          # open http://127.0.0.1:8000
# Before releasing, you can run a strict build:
# mkdocs build --strict
```

**Troubleshooting**

- **404 during pip step:** ensure the repository is public and the referenced tag exists (e.g. v1.1.1).
- **Conda too old:** conda update -n base -c conda-forge conda.
- **Imports fail during docs build:** ensure pip install -e . (or the dev env) is active before mkdocs serve.
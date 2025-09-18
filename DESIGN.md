# DESIGN — transnetmap-impacts

## 📁 Package organization - version 1.1.1

```text
transnetmap-impacts/
├── .gitignore
├── .readthedocs.yaml
├── DESIGN.md
├── environment.yml
├── environment-dev.yml
├── LICENSE
├── mkdocs.yml
├── pyproject.toml
├── README.md
├── docs/
│   ├── api/
│   ├── assets/
│   ├── examples/
│   ├── ... # Markdown files
│   └── requirements-docs.txt
├── src/
│   └── transnetmap/
│       ├── __init__.py
│       ├── analysis
│       │   ├── __init__.py
│       │   ├── edgelist.py
│       │   ├── graph.py
│       │   └── time_functions.py
│       ├── post/
│       │   ├── __init__.py
│       │   ├── results.py
│       │   └── heatmap.py
│       ├── pre/
│       │   ├── __init__.py
│       │   ├── network.py
│       │   ├── network_child.py
│       │   ├── nptm.py
│       │   └── pvs.py
│       └── utils/
│           ├── __init__.py
│           ├── config.py
│           ├── constant.py
│           ├── map.py
│           ├── scale.py
│           ├── sql.py
│           ├── time.py
│           └── utils.py
└── tests/ -> in progress
    ├── get_started/
    │   ├── inputs/
    │   ├── outputs/
    │   └── get_started.py
    ├── setup_raw_data_before_NPTM.py
    ├── add_time_function.py # TODO
    ├── test_import_time_function.py # TODO
    └── ...
```

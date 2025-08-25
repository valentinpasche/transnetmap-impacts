# DESIGN — transnetmap-impacts

## 📁 Organisation du package - version 1.0.1

```text
transnetmap-impacts/
├── LICENSE
├── README.md
├── environment-dev.yml
├── environment.yml
├── pyproject.toml
├── DESIGN.md
├── src/
│   └── transnetmap/
│       ├── __init__.py
│       ├── pre/
│       │   ├── __init__.py
│       │   ├── network.py
│       │   ├── network_child.py
│       │   ├── nptm.py
│       │   └── pvs.py
│       ├── analysis
│       │   ├── __init__.py
│       │   ├── edgelist.py
│       │   ├── graph.py
│       │   └── time_functions.py
│       ├── post/
│       │   ├── __init__.py
│       │   ├── results.py
│       │   └── heatmap.py
│       └── utils/
│           ├── __init__.py
│           ├── config.py
│           ├── dct.py
│           ├── map_utils.py
│           ├── scale_utils.py
│           ├── sql.py
│           ├── time_utils.py
│           └── utils.py
└── tests/ -> in progress (but maybe do that for V2)
    ├── datasets/
    ├── add_time_function.py
    ├── test_import_time_function.py
    ├── test_pre.py
    ├── test_analysis.py
    ├── test_results.py
    └── test_nptm_import.py #TODO -> NotImplementedError
```

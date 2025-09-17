# Getting started — small example with an extract from the Swiss traffic model (NPTM)

This quick guide walks you through the **full workflow** with a small NPTM extract: import raw data, configure the run, prepare inputs (pre), build & optimise the edge list (analysis), and produce an interactive heatmap (post).

!!! warning "Prerequisites"
    - PostgreSQL + PostGIS available
    - Valid DB URI (write access)
    - Inputs located under `tests/get_started/inputs/...`

!!! note "Before you start"
    - The raw-data bootstrap example lives in `tests/setup_raw_data_before_NPTM.py`.  
      It shows how to pre-format the inputs used here.
    - This page loads **pre-baked artifact files** (images & HTML maps) so you can explore the results even without a running database. (located `docs/examples/get_started/...`)

---

## 1 Load demo inputs

```python
import pathlib

import geopandas as gpd
import polars as pl

from transnetmap.utils import ParamConfig
import transnetmap.pre as pre
from transnetmap.analysis import Graph
from transnetmap.post import HeatMap

# Base script location : `tests/get_started/get_started.py`
data_folder = pathlib.Path("inputs")

# Raw NPTM data (zones + OD matrices)
gdf_zones  = gpd.read_parquet(data_folder / "simplify_zones.parquet")
imt_time   = pl.read_ipc(data_folder / "imt_time.arrow", memory_map=False)
imt_length = pl.read_ipc(data_folder / "imt_length.arrow", memory_map=False)
pt_time    = pl.read_ipc(data_folder / "pt_time.arrow", memory_map=False)
pt_length  = pl.read_ipc(data_folder / "pt_length.arrow", memory_map=False)
```

---

## 2 Configure the run

*See [ParamConfig ↗](api/utils/ParamConfig.md)*

```python
config = ParamConfig(**{
    "network_number": 1,
    "physical_values_set_number": 2,
    "network_extension_type": "IMT",
    "main_print": False,
    "sql_echo": False,
    "db_nptm_schema": "nptm",
    "db_zones_table": "zones",
    "db_imt_table": "imt",
    "db_pt_table": "pt",
    "uri": "postgresql://username:password@host:port/database", # set your own
})
```

---

## 3 Pre — prepare inputs & structure data

*See [transnetmap.pre ↗](api/pre/index.md)*

### 3.1 NPTM (format & push)

*See [NPTM ↗](api/pre/nptm/NPTM.md)*

```python
nptm = pre.NPTM(config)
nptm.setup_data(
    zones_gdf      = gdf_zones, 
    imt_mtx_time   = imt_time, 
    imt_mtx_length = imt_length, 
    pt_mtx_time    = pt_time, 
    pt_mtx_length  = pt_length,
)

nptm_data_description = """Sources: Swiss Confederation
Data from the National Passenger Traffic Model (ARE), base year 2017.
Displacement matrices dated 20-Apr-2022.
CRS: WGS84 (EPSG:4326). Units: time in minutes; distances (length) in km.
"""

nptm.to_sql({
    "IMT":   "OD matrix, individual motorised transport",
    "PT":    "OD matrix, public transport",
    "zones": "Structure and definition of mobility zones",
    "schema": nptm_data_description,
})
```

### 3.2 Stations (new network)

*See [Stations ↗](api/pre/network_child/Stations.md)*

```python
stations = pre.Stations(config)
stations.read_csv(data_folder / "stations_1.csv")
stations.show()
```

<figure markdown>
  ![Network map](examples/get_started/images/stations_map.png){ width="780" }
  <figcaption>Stations of the new network — static preview.</figcaption>
  [Open the interactive map ↗](examples/get_started/maps/stations_map.html){ target="_blank" rel="noopener" }
</figure>

### 3.3 Links between stations

*See [Links ↗](api/pre/network_child/Links.md)*

```python
links = pre.Links(config)
links.read_csv(data_folder / "links_1.csv")
```

### 3.4 Build the network

*See [Network ↗](api/pre/network/Network.md)*

```python
network = pre.Network(config)
network.create_network(stations, links)
network.show()
network.to_sql()  # also consider: stations.to_sql(), links.to_sql()
```

<figure markdown>
  ![Network map](examples/get_started/images/network_map.png){ width="780" }
  <figcaption>New network — static preview.</figcaption>
  [Open the interactive map ↗](examples/get_started/maps/network_map.html){ target="_blank" rel="noopener" }
</figure>

### 3.5 Setup physical values (PVS)

*See [PVS_TravelTime ↗](api/pre/pvs/PVS_TravelTime.md)*  
*See [PVS_Impacts ↗](api/pre/pvs/PVS_Impacts.md)*

!!! tip "Special case"
    If pvs{`number`} already exists in the DB, you can skip this section.

```python
# Time parameters
pvs_tt = pre.PVS_TravelTime(config)
pvs_tt.read_csv(data_folder / "physical_values_travel_time_2.csv")
pvs_tt.to_sql()

# Impact parameters (example: EP)
pvs_ep = pre.PVS_Impacts(config, "EP")
pvs_ep.read_csv(data_folder / "physical_values_impacts_EP_2.csv")
pvs_ep.to_sql()
```

---

## 4 Analysis — edge list & path optimisation

*See [Graph ↗](api/analysis/graph/Graph.md)*

```python
graph = Graph(config)
graph.create_edgelist()

com_schema_results = f"""The schema stores optimisation results for network '{config.network_number}'
with physical parameters 'pvs{config.physical_values_set_number}'.
Base data: Swiss NPTM ({config.db_nptm_schema}). Extension type: {config.network_extension_type}.
Optimisation is time-dependent.

Tables:
- edgelist     : sections filtered for the optimisation algorithm
- optimisation : optimisation results filtered with NPTM
- results_<id> : partial results (impacts), from/to zone <id> ({config.db_zones_table})
"""
graph.to_sql_edgelist(comment_schema=com_schema_results)

# Runs optimisation and persists the results
graph.process_dijkstra()
```

---

## 5 Post — interactive heatmap

*See [HeatMap ↗](api/post/heatmap/HeatMap.md)*

First, **view all zones** to pick a valid `id_zone` and label:

```python
pre.Network(config).show_all()
```

<figure markdown>
  ![All zones](examples/get_started/images/network_all_map.png){ width="780" }
  <figcaption>All zones — use this to pick <code>id_zone</code> and <code>zone_label</code></figcaption>
  [Open the interactive map ↗](examples/get_started/maps/network_all_map.html){target="_blank" rel="noopener"}
</figure>

<figure markdown>
  ![Select zone id & label](examples/get_started/images/select_zone_id_label.png){ width="585" }
  <figcaption>Zoom view with popup: read the zone <code>id</code> and a human-readable label</figcaption>
</figure>

Then create the heatmap and select layers:

```python
heatmap = HeatMap(config, id_zone=838, zone_label="GVA (Geneva airport)", to_zone=True)

# List available choropleths — example selection:
heatmap.show_layers()  # → Selected layers: 'EP_NTS' and 'time_diff_PT_NTS'

heatmap.generate_map(choropleths=["EP_NTS", "time_diff_PT_NTS"])
```

<figure markdown>
  ![EP impacts](examples/get_started/images/heatmap_EP.png){ width="780" }
  <figcaption>"EP_NTS" — Primary energy impacts for the new network</figcaption>
</figure>

<figure markdown>
  ![PT vs NTS time diff](examples/get_started/images/heatmap_time_diff.png){ width="780" }
  <figcaption>"time_diff_PT_NTS" — Travel-time difference (PT vs new network)</figcaption>
  [Open the interactive map ↗](examples/get_started/maps/heatmap.html){target="_blank" rel="noopener"}
</figure>

!!! tip "Reproducibility"
    HeatMap does **not** include a `.show()` method (unlike `Network`/`Stations`).  
    Use `generate_map()` to build a reproducible HTML map with the chosen choropleths.

---

### Sources
- Swiss Confederation — ARE, National Passenger Traffic Model (base 2017; displacement matrices dated 20 Apr 2022).  
- CRS: WGS84 (EPSG:4326). Time in minutes, distances in km.

---

**That’s it!** You now have a working pipeline from raw NPTM inputs to an interactive heatmap.

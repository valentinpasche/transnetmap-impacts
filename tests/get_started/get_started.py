# Getting started: small example with an extract from the Swiss traffic model (NPTM).
# For preliminary work on importing and configuring master data, see "setup_raw_data_before_NPTM.py"

import pathlib

import geopandas as gpd
import polars as pl

from transnetmap.utils import ParamConfig
import transnetmap.pre as pre
from transnetmap.analysis import Graph
from transnetmap.post import HeatMap

data_folder = pathlib.Path("inputs")

# ==========================================================================================
# === Import raw NPTM data ===
# ==========================================================================================

gdf_zones = gpd.read_parquet(data_folder / "simplify_zones.parquet")

imt_time = pl.read_ipc(data_folder / "imt_time.arrow", memory_map=False)
imt_length = pl.read_ipc(data_folder / "imt_length.arrow", memory_map=False)
pt_time = pl.read_ipc(data_folder / "pt_time.arrow", memory_map=False)
pt_length = pl.read_ipc(data_folder / "pt_length.arrow", memory_map=False)


# ==========================================================================================
# === Complete configuration with creation and calculation parameters ===
# ==========================================================================================

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
    "uri": "postgresql://valentin:valentin@pgsql01.gm.heia-fr.ch:5432/gripit_DB",
})


# ==========================================================================================
# === Pre-processing subpackage: input preparation and data structuring ===
# ==========================================================================================

# === National Passenger Traffic Model ===

nptm_data_description = '''Sources : Swiss Confederation
Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
according to 2017 status.
Data relating to travel times and distances are dated 20 April 2022 in the displacements matrixs.
- EPSG: WGS84 (4326)
- Units: time in minutes and distances (lenght) in kilometers.
'''

nptm = pre.NPTM(config)
nptm.setup_data(
    zones_gdf      = gdf_zones, 
    imt_mtx_time   = imt_time, 
    imt_mtx_length = imt_length, 
    pt_mtx_time    = pt_time, 
    pt_mtx_length  = pt_length
)
nptm.to_sql(
    {
    'IMT':   "OD matrix, individual motorised transport", 
    'PT':    "OD matrix, public transport", 
    'zones': "Structure and definition of mobility zones,", 
    'schema': nptm_data_description, 
    }
)


# -----------------------------------
# === Stations of the new network ===

stations = pre.Stations(config)
stations.read_csv(data_folder / "stations_1.csv")


# ------------------------------
# === Links between stations ===

links = pre.Links(config)
links.read_csv(data_folder / "links_1.csv")


# -------------------
# === New network ===

network = pre.Network(config)
network.create_network(stations, links)

network.show()

# Also execute 'stations.to_sql()' and 'links.to_sql()'.
network.to_sql()


# -------------------------------------
# === Sets of physical values, time ===

pvs_tt = pre.PVS_TravelTime(config)
pvs_tt.read_csv(data_folder / "physical_values_travel_time_2.csv")

pvs_tt.to_sql()


# ----------------------------------------
# === Sets of physical values, impacts ===

pvs_ep = pre.PVS_Impacts(config, "EP")
pvs_ep.read_csv(data_folder / "physical_values_impacts_EP_2.csv")

pvs_ep.to_sql()


# ==========================================================================================
# === Analysis core subpackage — edge list building and path optimisation ===
# ==========================================================================================

graph = Graph(config)
graph.create_edgelist()

com_schema_results = f'''The tables in the schema are the results of optimising the new network "{config.network_number}" with the physical parameters "pvs{config.physical_values_set_number}".
The basic data comes from NPTM in Switzerland (schema {config.db_nptm_schema}),
the type of transport, from NPTM, used to extend the new network to all the areas studied is {config.network_extension_type}.               
Optimisation is time-dependent.

The tables are organised as follows:
    - edgelist: list of sections filtered for the optimisation algorithm
    - optimisation: optimisation results filtered with NPTM data
    - results_"id": partial results, calculated with impacts, from or to a zone (id ref. {config.db_zones_table})


Sources : Swiss Confederation
    Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
    according to 2017 status.
'''
# Save the newly created 'edgelist' table, required before 'graph.process_dijkstra()'
graph.to_sql_edgelist(comment_schema=com_schema_results)

# # Automatically execute  'graph.to_sql_optimisation()'
graph.process_dijkstra()


# ==========================================================================================
# === Post-processing subpackage — interactive map visualization and impact presentation ===
# ==========================================================================================

# Select the from/to zone (id and label) for the heat map.
pre.Network(config).show_all()

heatmap = HeatMap(config, id_zone=838, zone_label="GVA (Geneva airport)", to_zone=True)

# Displays the list of available choropleth layers
heatmap.show_layers() # -> Selected layers : 'EP_NTS' and 'time_diff_PT_NTS'

heatmap.generate_map(choropleths=["EP_NTS", "time_diff_PT_NTS"])



# setup_raw_data_before_NPTM

import pathlib

import geopandas as gpd
import numpy as np
import polars as pl

raw_file_path = pathlib.Path(r"...")

# Temporary backup folder
temp_folder_path = pathlib.Path("datasets/get_started")

# Constants for OD matrix
no_relations_value = 999999
comment_line_top=8
comment_line_rear=7980

# ===============================
# === Fonctions ===
# ===============================

def show_gdf(gdf):
    """" Utility for displaying geographic data in the browser. """
    import folium
    import webbrowser
    import tempfile
    
    # Parameters specific to the study area
    m = folium.Map(location=[46.457, 6.542], zoom_start=10)
        
    folium.GeoJson(gdf).add_to(m)
    
    temp_html = tempfile.NamedTemporaryFile(suffix=".html").name
    m.save(temp_html)
    webbrowser.open("file://" + temp_html)


def read_mtx(file, comment_line_top=None, comment_line_rear=None):
    """ Format-specific reading of raw OD matrices. """
    with open(file, 'r', encoding='cp1252') as file:
        lines = file.readlines()
        
        if not comment_line_rear and comment_line_top:
            data_lines = lines[:comment_line_top]
        elif not comment_line_top and comment_line_rear:
            data_lines = lines[-comment_line_rear:]
        elif comment_line_top and comment_line_rear:
            data_lines = lines[comment_line_top:-comment_line_rear]
        else: pass
        
        # Initialize an empty dictionary for the final data
        data_dict = {"from": [], "to": [], "value": []}
        for line in data_lines:
            # Split each line into columns
            columns = line.split()
            
            # Append each column to the appropriate key in the dictionary
            data_dict["from"].append(columns[0])
            data_dict["to"].append(columns[1])
            data_dict["value"].append(columns[2])
            
    return data_dict

# ===============================
# === Import zones ===
# ===============================

file = raw_file_path / "Verkehrszonen_Schweiz_NPVM_2017_shp"
name_base_id =  'ID' # The name of the 'ID' column depends on the base data

zones = gpd.read_file(file).sort_values(by=name_base_id).reset_index(drop=True)

extract = zones[
    zones["N_AMR"].isin([
        "Lausanne", "Renens\x96Ecublens", "Montreux\x96Vevey", "Prilly\x96Le Mont-sur-Lausanne", "Nyon",
        "Vernier\x96Lancy", "Thônex\x96Chêne-Bougeries", "Genève", "Le Grand-Saconnex", "Rolle\x96Saint-Prex"
        ])
    ]
simplify = extract.copy()

# The name of the column containing the geometries must be "geom".
simplify.rename_geometry('geom', inplace=True)

# Optional, simplification of geometries, reduction in the size of generated HTML files (maps, choropleths, etc.)
simplify.geometry = extract.geometry.simplify_coverage(500)

# ===============================
# === Control study area ===
# ===============================

show_gdf(simplify)

# ===============================
# === Prcess zones ===
# ===============================

simplify.insert(0, 'id', np.arange(1, len(simplify) + 1))
simplify.rename(columns={name_base_id:'nptmid'}, inplace=True)

# Optional, Filter on the study area
simplify = simplify[['id', 'geom', 'nptmid', 'ID_alt', 'ID_Gem', 'N_Gem', 'stg_type', 'N_stg_type',
       'ID_KT', 'N_KT', 'ID_SL3', 'N_SL3', 'ID_Agglo', 'N_Agglo', 'ID_AMR', 'N_AMR']]

ids = np.sort(simplify["nptmid"].unique())
expr_mask = pl.col("from").is_in(ids) & (pl.col("to").is_in(ids))

# ===============================
# === Step 1 : IMT travel time ===
# ===============================

file = raw_file_path / "DWV_2017_Strasse_Reisezeit_Distanz_CH\DWV_2017_Strasse_Reisezeit_CH.mtx"

imt_time = pl.DataFrame(
    read_mtx(file, 
             comment_line_top=comment_line_top, 
             comment_line_rear=comment_line_rear
             )
)
imt_time = imt_time.with_columns(
        pl.col("from").cast(pl.Int64),
        pl.col("to").cast(pl.Int64),
        pl.col("value").cast(pl.Float32)
)
imt_time = imt_time.filter(expr_mask)

# ===============================
# === Step 2 : IMT travel distance ===
# ===============================

file = raw_file_path / "DWV_2017_Strasse_Reisezeit_Distanz_CH\DWV_2017_Strasse_Distanz_CH.mtx"

imt_length = pl.DataFrame(
    read_mtx(file, 
             comment_line_top=comment_line_top, 
             comment_line_rear=comment_line_rear
            )
)
imt_length = imt_length.with_columns(
        pl.col("from").cast(pl.Int64),
        pl.col("to").cast(pl.Int64),
        pl.col("value").cast(pl.Float32)
)
imt_length = imt_length.filter(expr_mask)

# ===============================
# === Step 3 : PT travel time ===
# ===============================

file = raw_file_path / "DWV_2017_OeV_Reisezeit_Distanz_CH\DWV_2017_ÖV_Reisezeit_CH.mtx"

pt_time = pl.DataFrame(
    read_mtx(file, 
             comment_line_top=comment_line_top, 
             comment_line_rear=comment_line_rear
             )
    )
pt_time = pt_time.with_columns(
                    pl.col("from").cast(pl.Int64),
                    pl.col("to").cast(pl.Int64),
                    pl.col("value").cast(pl.Float32)
                    )
pt_time = pt_time.with_columns(
    pl.when(pl.col("value") == no_relations_value).then(None).otherwise(pl.col("value")).alias("value")
    )
pt_time = pt_time.filter(expr_mask)

# ===============================
# === Step 4 : PT travel distance ===
# ===============================

file = raw_file_path / "DWV_2017_OeV_Reisezeit_Distanz_CH\DWV_2017_ÖV_Distanz_CH.mtx"

pt_length = pl.DataFrame(
    read_mtx(file, 
             comment_line_top=comment_line_top, 
             comment_line_rear=comment_line_rear
             )
    )
pt_length = pt_length.with_columns(
                    pl.col("from").cast(pl.Int64),
                    pl.col("to").cast(pl.Int64),
                    pl.col("value").cast(pl.Float32)
                    )
pt_length = pt_length.with_columns(
    pl.when(pl.col("value") == no_relations_value).then(None).otherwise(pl.col("value")).alias("value")
    )
pt_length = pt_length.filter(expr_mask)

# ===============================
# === Save the prepared data ===
# ===============================

simplify.to_parquet(temp_folder_path / "simplify_zones.parquet")

imt_time.write_ipc(temp_folder_path / "imt_time.arrow", compression='lz4')
imt_length.write_ipc(temp_folder_path / "imt_length.arrow", compression='lz4')
pt_time.write_ipc(temp_folder_path / "pt_time.arrow", compression='lz4')
pt_length.write_ipc(temp_folder_path / "pt_length.arrow", compression='lz4')

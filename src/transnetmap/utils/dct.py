# -*- coding: utf-8 -*-

"""
The list of impacts can change and be extended.
"""
impacts_list = [
    'CO2', 
    'EP',
]


"""
It is essential that the dictionary "dct_type" keys remain unchanged.
The ‘NTS’ prefix / suffix, => New Transportation System
"""
# In database column "type"
dct_type = {
    'IMT':1, 
    'withoutIMT':-1, 
    'PT':2, 
    'withoutPT':-2, 
    'NTS-lower':3, 
    'NTS-main':4, 
    'NTS-higher':5, 
    'with-NTS':6,
    'extend-NTS':7,
}


"""
It is essential that the dictionary "dct_level" keys remain unchanged.
"""
# In database column "level"
dct_level = {
    'lower':1, 
    'main':2, 
    'higher':3,
}


# Radius, in meters, of stations for displaying "station circles". Used in Network.show() 
stations_areas_radius = {
    1: 5e3, 
    2: 10e3, 
    3: 30e3,
}


# List of valid Folium tile providers
DCT_VALID_TILES = {
    "OpenStreetMap": "OpenStreetMap",
    "OpenStreetMap Mapnik": "OpenStreetMap Mapnik",
    "OpenStreetMap CH": "OpenStreetMap CH",
    "OpenStreetMap DE": "OpenStreetMap DE",
    "OpenStreetMap France": "OpenStreetMap France",
    "OpenRailwayMap": "OpenRailwayMap",
    "CartoDB Positron": "CartoDB Positron",
    "CartoDB PositronNoLabels": "CartoDB PositronNoLabels",
    "CartoDB PositronOnlyLabels": "CartoDB PositronOnlyLabels",
    "CartoDB Voyager": "CartoDB Voyager",
    "CartoDB VoyagerNoLabels": "CartoDB VoyagerNoLabels",
    "CartoDB VoyagerOnlyLabels": "CartoDB VoyagerOnlyLabels",
    "CartoDB VoyagerLabelsUnder": "CartoDB VoyagerLabelsUnder",
    "CartoDB DarkMatter": "CartoDB DarkMatter",
    "WorldStreetMap": "Esri WorldStreetMap",
    "WorldTerrain": "Esri WorldTerrain",
    "WorldImagery": "Esri WorldImagery",
    "WorldGrayCanvas": "Esri WorldGrayCanvas",
    "GeoportailFrance": "GeoportailFrance plan",
    "SwissFederalGeoportal NationalMapColor": "SwissFederalGeoportal NationalMapColor",
    "SwissFederalGeoportal NationalMapGrey": "SwissFederalGeoportal NationalMapGrey",
    "SwissFederalGeoportal SWISSIMAGE": "SwissFederalGeoportal SWISSIMAGE",
}


# HeatMap, Dictionary of default values for each type of analysis
defaults_thresholds_scale_color = {
    "time": {"fill_color": "Paired", "reverse_color": False},
    "length": {"fill_color": "Paired", "reverse_color": False},
    "changes": {"fill_color": "RdYlGn", "reverse_color": True}, # Discrete values
    "transport_type": {"fill_color": "GnBu", "reverse_color": False}, # Discrete values
    "difference": {"fill_color": "plasma", "reverse_color": False},
    "impacts": {"fill_color": "Spectral", "reverse_color": True},
}


def generate_level_to_type_mapping(dct_level, dct_type, prefix="NTS") -> dict:
    """
    Dynamically map levels to types based on dictionary keys and values.
    
    Parameters
    ----------
    dct_level : dict
        Dictionary mapping level names to integers (e.g., {'lower':1, 'main':2, 'higher':3}).
    dct_type : dict
        Dictionary mapping type names to integers (e.g., {'IMT':1, 'PT':2, 'NTS-lower':3}).
    prefix : str, optional
        Prefix for NTS-related types in `dct_type` (default is 'NTS').
        
    Returns
    -------
    dict
        A dictionary mapping level values (from `dct_level`) to type values (from `dct_type`).
    """
    mapping = {}
    for level_key, level_value in dct_level.items():
        type_key = f"{prefix}-{level_key}"  # Generate the corresponding type key
        if type_key in dct_type:
            mapping[level_value] = dct_type[type_key]
    return mapping


# ===========================
if __name__ == "__main__":
    
    dct = generate_level_to_type_mapping(dct_level, dct_type)
    
    
# -*- coding: utf-8 -*-
"""
Core constants and simple lookups for transnetmap.

This module centralizes:

- the list of supported impacts (``IMPACTS``).
- transport and level enumerations (``DCT_TYPE``, ``DCT_LEVEL``).
- default UI/tiles lookups.
- default color/scale choices for heatmaps.
- and a helper to derive level → type mapping.

Notes
-----
* Keys of ``DCT_TYPE`` and ``DCT_LEVEL`` are part of the public contract.
* ``IMPACTS`` may be extended in a future version via a public setter.
"""

from __future__ import annotations

from typing import Dict, List

__all__ = [
    "IMPACTS",
    "DCT_TYPE",
    "DCT_LEVEL",
    "STATIONS_AREAS_RADIUS",
    "DCT_VALID_TILES",
    "DEFAULT_THRESHOLDS_SCALE_COLOR",
]


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

# The list of impacts can change and be extended.
IMPACTS: List[str] = [
    "CO2",
    "EP",
    "TCO",
]
""" The list of native impacts.  
Can be modified and supplemented from the source code. """

# ------------------------------------------------------------------
# It is essential that the dictionary "DCT_TYPE" keys remain unchanged.
# The ‘NTS’ prefix / suffix => New Transportation System
# Values are used in DB column "type"
DCT_TYPE: Dict[str, int] = {
    "IMT": 1,
    "withoutIMT": -1,
    "PT": 2,
    "withoutPT": -2,
    "NTS-lower": 3,
    "NTS-main": 4,
    "NTS-higher": 5,
    "with-NTS": 6,
    "extend-NTS": 7,
}
""" Integer code for transport types/modes.
Values are used in DB column 'type'.  
The ‘NTS’ prefix / suffix => New Transportation System """

# ------------------------------------------------------------------
# It is essential that the dictionary "DCT_LEVEL" keys remain unchanged.
# Values are used in DB column "level"
DCT_LEVEL: Dict[str, int] = {
    "lower": 1,
    "main": 2,
    "higher": 3,
}
""" Integer code for the levels of the new transport system.  
Values are used in DB column 'level'. """

# Radius, in meters, of stations for displaying "station circles". Used in Network.show()
STATIONS_AREAS_RADIUS = {
    1: 5e3,
    2: 10e3,
    3: 30e3,
}
""" Radius, in meters, of stations for displaying "station circles".  
Used in Network.show()"""


# Valid Folium tile providers (keys are the public names exposed to users)
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
""" Valid Folium tile providers (keys are the public names exposed to users). """


# HeatMap defaults by analysis type
DEFAULT_THRESHOLDS_SCALE_COLOR = {
    "time": {"fill_color": "Paired", "reverse_color": False},
    "length": {"fill_color": "Paired", "reverse_color": False},
    "changes": {"fill_color": "RdYlGn", "reverse_color": True},         # Discrete
    "transport_type": {"fill_color": "GnBu", "reverse_color": False},   # Discrete
    "difference": {"fill_color": "plasma", "reverse_color": False},
    "impacts": {"fill_color": "Spectral", "reverse_color": True},
}
""" Default values for choropleth color palettes, by analysis type. """


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
def generate_level_to_type_mapping(
    dct_level: Dict[str, int],
    dct_type: Dict[str, int],
    prefix: str = "NTS",
) -> dict:
    """
    Dynamically map level values to type values, based on dictionary keys.

    Parameters
    ----------
    dct_level : dict
        Level name → int (e.g., {'lower':1, 'main':2, 'higher':3}).
    dct_type : dict
        Type name → int (e.g., {'IMT':1, 'PT':2, 'NTS-lower':3}).
    prefix : str, optional
        Prefix for NTS-related types in ``dct_type`` (default 'NTS').

    Returns
    -------
    dict
        Mapping from level values (``dct_level``) to type values (``dct_type``).
    """
    mapping = {}
    for level_key, level_value in dct_level.items():
        type_key = f"{prefix}-{level_key}"
        if type_key in dct_type:
            mapping[level_value] = dct_type[type_key]
    return mapping


# -----------------------------------------------------------------------------
# Internal compatibility v1.0.x (intentionally undocumented)
# -----------------------------------------------------------------------------
# These aliases ensure that nothing breaks if a module still references
# the old name in lowercase letters.
impacts_list = IMPACTS
dct_type = DCT_TYPE
dct_level = DCT_LEVEL
stations_areas_radius = STATIONS_AREAS_RADIUS
defaults_thresholds_scale_color = DEFAULT_THRESHOLDS_SCALE_COLOR


# -----------------------------------------------------------------------------
# Quick sanity check
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    _map = generate_level_to_type_mapping(DCT_LEVEL, DCT_TYPE)
    print(_map)
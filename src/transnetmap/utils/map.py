# -*- coding: utf-8 -*-
"""
Map helpers for Folium rendering in transnetmap.

This module provides small utilities to:
    
- save/open a Folium map on disk and optionally in a browser (`show_map`).
- create a map auto-fitted to a GeoDataFrame extent (`auto_fit_map`).
- configure multiple tile layers with validity checks (`setup_tiles`).

Notes
-----
* Tiles validation uses ``transnetmap.utils.constant.DCT_VALID_TILES``.
"""

from __future__ import annotations

import os
import tempfile
import webbrowser
from typing import TYPE_CHECKING, List, Optional, Tuple

import folium
import numpy as np

from transnetmap.utils.constant import DCT_VALID_TILES

if TYPE_CHECKING:  # noqa: F401
    import geopandas as gpd
    from branca.element import Figure

__all__ = ["show_map", "auto_fit_map", "setup_tiles"]


# -----------------------------------------------------------------------------
# File save & open
# -----------------------------------------------------------------------------
def show_map(
    map_object: folium.Map | Figure,
    *,
    file_name: str = "map",
    save_to_desktop: bool = False,
    custom_path: Optional[str] = None,
    open_browser: bool = True,
    return_path: bool = False,
) -> Optional[str]:
    """
    Display a Folium map in the browser with options to save in a specific folder.

    Parameters
    ----------
    map_object : folium.Map or branca.element.Figure
        The map object to display.
    file_name : str, optional
        File name to save without extension. Default is ``"map"``.
    save_to_desktop : bool, optional
        If ``True``, saves the map to the Desktop. Default is ``False``.
    custom_path : str, optional
        If provided, saves the map to this directory (overrides ``save_to_desktop``).
    open_browser : bool, optional
        If ``True``, opens the saved file in the default browser. Default is ``True``.
    return_path : bool, optional
        If ``True``, returns the saved file path instead of opening it. Default is ``False``.

    Returns
    -------
    str or None
        The saved file path if ``return_path`` is ``True``, otherwise ``None``.

    Raises
    ------
    ValueError
        If ``custom_path`` does not exist or is not writable, or if ``file_name`` contains a dot.
    RuntimeError
        If saving the map fails due to an I/O error.

    Notes
    -----
    A ``RuntimeError`` is raised when an unexpected issue occurs during the save process.
    """
    if "." in os.path.basename(file_name):
        raise ValueError("The 'file_name' parameter should not include an extension.")

    file_name = f"{file_name}.html"
    is_temporary = False

    # Determine save path
    if custom_path:
        if not os.path.exists(custom_path) or not os.access(custom_path, os.W_OK):
            raise ValueError(f"The path '{custom_path}' does not exist or is not writable.")
        save_path = os.path.join(custom_path, file_name)
    elif save_to_desktop:
        save_path = os.path.join(os.path.expanduser("~"), "Desktop", file_name)
    else:
        is_temporary = True
        save_path = tempfile.NamedTemporaryFile(suffix=".html", delete=False).name

    # Save the map with error handling
    try:
        map_object.save(save_path)
    except Exception as e:
        raise RuntimeError(f"Failed to save map to '{save_path}': {e}")

    # Print status
    if not is_temporary:
        print(f"Map saved to: {save_path}")
    else:
        print("Map saved as a temporary file.")

    # Return file path if required
    if return_path:
        return save_path

    # Open the map if required (Temporary files always open)
    if is_temporary or open_browser:
        webbrowser.open(f"file://{save_path}")

    return None


# -----------------------------------------------------------------------------
# Map creation with auto-fit
# -----------------------------------------------------------------------------
def auto_fit_map(
    gdf: gpd.GeoDataFrame,
    *,
    location: Optional[Tuple[float, float]] = None,
    zoom_start: Optional[int] = None,
    tiles: bool = True,
) -> folium.Map:
    """
    Create a Folium map and automatically fit bounds to the GeoDataFrame.

    If a custom ``location`` is provided, ``zoom_start`` must also be specified.

    Parameters
    ----------
    gdf : GeoDataFrame
        Spatial data used to determine the map extent.
    location : tuple of float, optional
        Custom ``(lat, lon)`` center. If provided, ``zoom_start`` must also be provided.
    zoom_start : int, optional
        Custom zoom level. If ``None``, the map will auto-fit the bounds.
    tiles : bool, optional
        If ``True`` (default), uses ``"OpenStreetMap"`` as the base tile. If ``False``, no base tiles.

    Returns
    -------
    folium.Map
        A map with appropriate centering, zoom level, and optional tile layer.

    Raises
    ------
    ValueError
        If ``location`` is provided without ``zoom_start``.
        If the GeoDataFrame is ``None`` or empty.
        If bounds computed from the GeoDataFrame are invalid.

    Notes
    -----
    - If ``location`` is not specified, the function computes a center point from
      the GeoDataFrame bounding box (``total_bounds``).
    - If ``zoom_start`` is ``None``, ``fit_bounds()`` is applied to adjust zoom dynamically.
    - Setting ``tiles=False`` removes the background tile layer (useful when overlaying
      only custom layers).
    """
    if location is not None and zoom_start is None:
        raise ValueError("If `location` is specified, `zoom_start` must also be provided.")

    if gdf is None or getattr(gdf, "empty", True):
        raise ValueError("The provided GeoDataFrame is empty or None. Cannot generate a map.")

    # Get bounds (minx, miny, maxx, maxy)
    bounds = getattr(gdf, "total_bounds", None)
    if bounds is None or np.isnan(bounds).all():
        raise ValueError("Invalid bounding box computed from the GeoDataFrame.")

    minx, miny, maxx, maxy = bounds

    # Auto-center if `location` is not provided
    if location is None and zoom_start is not None:
        location = ((miny + maxy) / 2, (minx + maxx) / 2)  # Mean center point

    # Create the map with specified location and zoom
    base_tiles = "OpenStreetMap" if tiles else None
    m = folium.Map(location=location, zoom_start=zoom_start, tiles=base_tiles)

    # Auto-fit bounds if no custom zoom is specified
    if zoom_start is None:
        m.fit_bounds([[miny, minx], [maxy, maxx]])

    return m


# -----------------------------------------------------------------------------
# Tile setup
# -----------------------------------------------------------------------------
def setup_tiles(mymap: folium.Map, *, map_tiles: Optional[List[str]] = None) -> folium.Map:
    """
    Add tile layers to a Folium map and set the default visible layer.

    If ``"OpenRailwayMap"`` is in the list, it is treated as an overlay; a base layer
    is ensured if necessary.

    Parameters
    ----------
    mymap : folium.Map
        Map object to which tile layers are added.
    map_tiles : list of str, optional
        Names of tile layers to add. The first tile is the default visible layer.
        If not provided or empty, defaults to ``["CartoDB Voyager"]``.

    Returns
    -------
    folium.Map
        The updated map with tile layers added.

    Notes
    -----
    - The ``OpenRailwayMap`` tile provider is temporarily disabled in v1 due to loading failures.
      It may be reintroduced in a future release.
    """
    if not map_tiles:
        map_tiles = ["CartoDB Voyager"]  # Default tile layer

    # Ensure only valid tiles are used
    for i, tile in enumerate(map_tiles):
        if tile not in DCT_VALID_TILES:
            raise ValueError(f"Invalid tile layer: '{tile}'. Choose from {list(DCT_VALID_TILES.keys())}.")

    # TODO: V2, Remove if "OpenRailwayMap" is working properly
    if "OpenRailwayMap" in map_tiles:
        map_tiles.remove("OpenRailwayMap")
        print("The 'OpenRailwayMap' tile has been disabled due to loading errors and will be re-evaluated in v2.")

    # Ensure OpenRailwayMap is the first layer if present
    if "OpenRailwayMap" in map_tiles:
        map_tiles.remove("OpenRailwayMap")
        map_tiles.insert(0, "OpenRailwayMap")

        # Ensure at least one base map exists
        if len(map_tiles) == 1:
            map_tiles.append("CartoDB VoyagerLabelsUnder")  # Default base map if OpenRailwayMap is alone

        nb_show = [0, 1]  # Show first two layers
    else:
        nb_show = [0]  # Show only the first layer

    # Add tile layers to the map
    for i, tile in enumerate(map_tiles):
        folium.TileLayer(
            DCT_VALID_TILES[tile],
            name=tile,
            overlay=(tile == "OpenRailwayMap"),  # ORW stays as overlay
            control=True,
            show=(i in nb_show),
        ).add_to(mymap)

    return mymap


# -----------------------------------------------------------------------------
# Manual test (no side effects at import)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    m = folium.Map(location=[47.03743, 8.35966], zoom_start=8, tiles=None)
    setup_tiles(m, ["CartoDB Positron", "OpenStreetMap Mapnik", "OpenStreetMap"])
    folium.LayerControl().add_to(m)
    show_map(m, save_to_desktop=True)

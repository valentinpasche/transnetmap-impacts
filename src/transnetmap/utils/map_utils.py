# -*- coding: utf-8 -*-

import os
import webbrowser
import tempfile

import folium
import numpy as np
from transnetmap.utils.dct import DCT_VALID_TILES

def show_map(map_object, 
             file_name="map", 
             save_to_desktop=False, 
             custom_path=None, 
             open_browser=True, 
             return_path=False,
):
    """
    Displays a Folium map in a web browser with options to save the map to a known directory.

    Parameters
    ----------
    map_object : folium.Map or branca.element.Figure
        The map object to display.
    file_name : str, optional
        Name of the file to save (without extension). Default is "map".
    save_to_desktop : bool, optional
        If True, saves the map to the desktop. Default is False.
    custom_path : str, optional
        If provided, saves the map to this specific directory (overrides save_to_desktop).
    open_browser : bool, optional
        If True, opens the saved map in a web browser. Default is True.
    return_path : bool, optional
        If True, returns the saved file path instead of opening it. Default is False.

    Raises
    ------
    ValueError
        If the specified `custom_path` does not exist or is not writable.
    RuntimeError
        If the map fails to save to disk (e.g. due to I/O issues).

    Returns
    -------
    str or None
        Returns the path to the saved map file if `return_path=True`, otherwise None.

    Notes
    -----
    A RuntimeError will be raised if an unexpected issue occurs during the save process.
    """
    if "." in os.path.basename(file_name):
        raise ValueError("❌ The 'file_name' parameter should not include an extension.")

    file_name = f"{file_name}.html"
    is_temporary = False

    # Determine save path
    if custom_path:
        if not os.path.exists(custom_path) or not os.access(custom_path, os.W_OK):
            raise ValueError(f"❌ The path '{custom_path}' does not exist or is not writable.")
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
        raise RuntimeError(f"❌ Failed to save map to '{save_path}': {e}")

    # Print status
    print(f"✅ Map saved to: {save_path}" if not is_temporary else "ℹ️ Map saved as a temporary file.")

    # Return file path if required
    if return_path:
        return save_path

    # Open the map if required (Temporary files always open)
    if is_temporary or open_browser:
        webbrowser.open(f"file://{save_path}")

    return None


def auto_fit_map(gdf, location=None, zoom_start=None, tiles=True) -> folium.Map:
    """
    Creates a Folium map and automatically fits the bounds to the given GeoDataFrame (gdf).
    If a custom location is provided, `zoom_start` must also be specified.
    
    This function ensures that the map is centered appropriately on the data, either by 
    automatically fitting the bounds of the provided `gdf` or by using a manually defined 
    `location` and `zoom_start`.

    Parameters
    ----------
    gdf : GeoDataFrame
        A GeoDataFrame containing the spatial data to determine the map extent.
    location : tuple(float, float), optional
        Custom (latitude, longitude) coordinates to center the map.
        ⚠️ If provided, `zoom_start` must also be specified.
    zoom_start : int, optional
        Custom zoom level. If None, the map will automatically fit the bounds.
    tiles : bool, optional
        If True (default), the map will use `"OpenStreetMap"` as the tile background.
        If False, no tile background is used, allowing for a clean visualization of layers only.
    
    Raises
    ------
    ValueError
        If `location` is provided but `zoom_start` is missing.
        If the provided GeoDataFrame is empty or None.

    Returns
    -------
    folium.Map
        A Folium map object with appropriate centering, zoom level, and optional tile layer.

    Notes
    -----
    - If `location` is not specified, the function automatically computes a center point 
      from the GeoDataFrame’s bounding box (`total_bounds`).
    - If `zoom_start` is not provided, `fit_bounds()` is applied to adjust the zoom dynamically.
    - Setting `tiles=False` removes the background tile layer, which is useful when overlaying 
      layers on a transparent background (e.g., for analysis-focused visualizations).
    """
    # 🔹 If location is provided, ensure zoom_start is also specified
    if location is not None and zoom_start is None:
        raise ValueError("⚠️ If `location` is specified, `zoom_start` must also be provided.")
    
    if gdf is None or gdf.empty:
        raise ValueError("⚠️ The provided GeoDataFrame is empty or None. Cannot generate a map.")

    # 🔹 Get bounds (minx, miny, maxx, maxy)
    bounds = gdf.total_bounds

    if np.isnan(bounds).all():
        raise ValueError("⚠️ Invalid bounding box computed from the GeoDataFrame.")
    minx, miny, maxx, maxy = bounds
    
    # 🔹 Auto-center if `location` is not provided
    if location is None and zoom_start is not None:
        location = ((miny + maxy) / 2, (minx + maxx) / 2)  # Mean center point

    # 🔹 Create the map with specified location and zoom
    # If `tiles=True`, uses OpenStreetMap as the default tile layer; otherwise, no background tile is used.
    if tiles:
        tiles = "OpenStreetMap"
    else:
        tiles = None    
    
    m = folium.Map(location=location, zoom_start=zoom_start, tiles=tiles)

    # 🔹 Auto-fit bounds if no custom zoom is specified
    if zoom_start is None:
        m.fit_bounds([[miny, minx], [maxy, maxx]])

    return m


# TODO: V2, Possibly a visualization method for the available folium/laeflet "tiles"
def setup_tiles(mymap, map_tiles) -> folium.Map:
    """
    Adds tile layers to the Folium map and sets the default visible layer.
    If "OpenRailwayMap" is in the list, it is set as the first overlay layer 
    and another base layer is automatically added if necessary.

    Parameters
    ----------
    mymap : folium.Map
        The Folium map object to which tile layers will be added.
    map_tiles : list of str
        List of tile layer names to add. The first tile in the list will be the default one.

    Returns
    -------
    folium.Map
        The updated map object with tile layers added.

    Notes
    -----
    - The `OpenRailwayMap` tile provider has been temporarily disabled in version 1 due to recent loading failures.
      It may be reintroduced in a future release when reliability is confirmed.
    """
    if not map_tiles:  # Safety check: avoid empty list
        map_tiles = ["CartoDB Voyager"]  # Default tile layer

    # Ensure only valid tiles are used
    for i, tile in enumerate(map_tiles):
        if tile not in DCT_VALID_TILES:
            raise ValueError(f"❌ Invalid tile layer: '{tile}'. Choose from {list(DCT_VALID_TILES.keys())}.")

    # TODO: V2, Remove if "OpenRailwayMap" is working properly
    if "OpenRailwayMap" in map_tiles:
        map_tiles.remove('OpenRailwayMap')
        print("⚠️ The 'OpenRailwayMap' tile has been disabled due to loading errors and will be re-evaluated in v2.")

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
            show=(i in nb_show),  # Show first two layers if OpenRailwayMap is present
        ).add_to(mymap)

    return mymap


# ===========================
if __name__ == "__main__":
    
    m = folium.Map(location=[47.03743, 8.35966], zoom_start=8, tiles=None)
    
    setup_tiles(m, ["CartoDB Positron", "OpenStreetMap Mapnik", "OpenStreetMap"])
    
    folium.LayerControl().add_to(m)
    
    show_map(m, save_to_desktop=True)
    
    
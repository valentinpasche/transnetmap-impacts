# -*- coding: utf-8 -*-
"""
Interactive heatmap generation over a transport network.

This module exposes the class `HeatMap`, the visualization core of the package.
It loads a per-zone partial network produced by the class `transnetmap.post.results.Results`,
prepares **choropleth layers** (time, length, changes, transport type, impacts),
builds **popups**, computes **thresholds** (continuous or discrete), and renders an
interactive **Folium** map. In practice, the most frequently used method is
method `HeatMap.generate_map()`.

Why this module matters

Even though most calculations (optimization, enrichment, impacts) happen upstream,
the end goal is to **inspect results on a map**. `HeatMap` brings together the data,
styles, legends, and popups, offering a simple API to produce maps ready to share.

Features

- **Layer catalog** (time, length, changes, IMT/PT type, environmental/energy/financial impacts).
- User-selected layers and popup fields with **dependency validation**.
- **Scales** (continuous/discrete) using Jenks or user-defined classes, validated by
  `transnetmap.utils.scale`.
- **Legends** (Branca) consistent with the chosen thresholds.
- **Tiles** setup via `transnetmap.utils.map` (OSM, toner, satellite, etc.).
- Export to **HTML** (optional desktop path) and/or return the layers for integration.

Inputs & dependencies

- Per-zone result tables: ``results_{id_zone}`` produced by the class `Results`.
- Scale helpers: `transnetmap.utils.scale` (Jenks, validations).
- Constants/mappings: `transnetmap.utils.constant` (e.g., ``DCT_TYPE``).
- Mapping helpers: `transnetmap.utils.map`.
- Configuration: `transnetmap.utils.config.HeatMapConfig`.

Layer catalog & popups

- **Layers**: travel time, length, number of changes, IMT/PT type, and impacts (e.g., ``CO2``, ``EP``, ``TCO``).
  The module verifies the presence of required columns before enabling a layer.
- **Popups**: configurable fields; text wrapping avoids mid-word breaks; units/labels are normalized.

Scales & thresholds

- **Continuous**: quantiles/Jenks (e.g., natural breaks on the distribution).
- **Discrete**: user classes validated (sorted bounds, inclusive ranges).
- **Colors**: default palettes aligned with the theme; user overrides allowed.

Workflow (high level)

1. Load/validate a per-zone partial network (``results_{id_zone}``) prepared by the class `Results`.
2. Select layers and popup fields (column and dependency validation).
3. Compute thresholds (Jenks or user-defined) and prepare choropleths.
4. Render the Folium map; optionally save to HTML and/or return layers.

API overview

- class `HeatMap`
  - `HeatMap.generate_map()` → **main method** to build the map.
  - `HeatMap.show_layers()`, `HeatMap.show_popup_fields()` for quick identification of options.

Examples:

    Quick map with default layers:
    
    >>> from transnetmap.post.heatmap import HeatMap
    >>> hm = HeatMap(
    ...     param,                      # ParamConfig already initialized (URI, schema, tables)
    ...     id_zone=1701,
    ...     zone_label="Zurich",
    ...     to_zone=True,
    ... )
    >>> hm.generate_map(
    ...     choropleths=["time", "changes"],
    ...     popup=["zone_id", "time", "changes"],
    ...     file_name="heatmap_1701",
    ...     save_to_desktop=True,            # or custom_path="out/"
    ... )

Notes
-----
- Expected columns for each layer must exist; otherwise the layer is ignored
  (with a validation message).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union, Optional, List, Dict, Tuple, Any
from itertools import permutations

import numpy as np
import pandas as pd
import polars as pl
import folium
from folium.plugins import Fullscreen
import matplotlib.colors
from branca.colormap import StepColormap

from transnetmap.utils.config import ParamConfig, HeatMapConfig

from transnetmap.post.results import Results

from transnetmap.pre.network import Network
from transnetmap.pre.network_child import Stations
from transnetmap.pre.nptm import NPTM


from transnetmap.utils.map import show_map, auto_fit_map, setup_tiles
from transnetmap.utils.constant import DCT_TYPE
from transnetmap.utils.utils import cap_first, remove_duplicates_preserve_order
from transnetmap.utils.scale import (
    compute_jenks_dynamic_scale,
    compute_discrete_scale_changes,
    compute_boolean_scale_type,
    validate_user_defined_scale,
    adjust_bins_for_folium,
)

if TYPE_CHECKING:  # noqa: F401
    import geopandas as gpd

__all__ = ["HeatMap"]


# -----------------------------------------------------------------------------
# Class Definition: HeatMap
# -----------------------------------------------------------------------------
class HeatMap(Results):
    """
    **Main interface for generating interactive heatmaps based on a transport network.**

    The `HeatMap` class extends the `Results` engine to allow **visual analysis of transport indicators**
    over a selected geographical zone. It provides an all-in-one workflow to compute results,
    apply choropleth styles, attach popups, and generate a final interactive map using `folium`.

    The map is built with **customizable layers, color scales, popup fields, and tile backgrounds**.
    Once configured, only one public method (`generate_map()`) is needed to produce a complete HTML map.

    The class does **not** include a `.show()` method like `Network()` or `Stations()`.
    Use `generate_map()` to trigger the full visualization process.
    
    ---
    
    <h3>Selecting the zone (use <code>Network.show_all()</code>)</h3>

    To pick a valid <code>id_zone</code> and an optional <code>zone_label</code>, first **open the network map**:
    
    ```python
    from transnetmap.pre import Network
    
    # Opens an interactive map of all zones (hover/click to inspect IDs)
    Network(param).show_all()
    ```
    
    Identify the zone visually (its integer <code>id</code> starts at 1) and then initialize <code>HeatMap</code>:
        
    ```python
    from transnetmap.post import HeatMap

    hm = HeatMap(param, id_zone=1701, zone_label="Lausanne")
    hm.generate_map(file_name="heatmap_1701")
    ```
    Why this step matters

    - It prevents passing an invalid <code>id_zone</code> (the base class <code>Results</code> will 
    raise a <code>ValueError</code> and explicitly suggest using <code>pre.Network().show_all()</code>).  
    - It makes the geographic scope of your analysis explicit and reproducible.  
    - It avoids guesswork when matching your external datasets to the model’s zone numbering.
    
    ---
    
    **Key Features**
    
    - Generates **folium heatmaps** from transport data.
    - Automatically applies impact values and network configuration from `Results`.
    - Customizes **choropleth layers** (values, types, styling).
    - Configures **popup fields**, legends, color bins and colormaps.
    - Adds network layers, station markers, and static information overlays.
    - Supports export to desktop or temporary HTML with browser display.

    **How It Works**
    
    1. Initializes with network config, zone selection, and direction (from/to).
    2. Automatically validates the zone and loads (or computes) its partial network.
    3. Calls `generate_map()` to select data layers, build the map, and save the output.

    **Parameters (via __init__)**
    
    *network_config* : `dict` or `ParamConfig`  
    
    * Transport network configuration. Can be either a raw dictionary or a validated `ParamConfig` instance.  
    * This configuration is automatically validated upon instantiation.  
    * Required fields: `["network_number", "physical_values_set_number", "network_extension_type",
    "db_nptm_schema", "db_zones_table", "db_imt_table", "db_pt_table", "uri"]`

    *map_config* : `HeatMapConfig`, optional  
    
    * Map rendering and output configuration (e.g., styles, tiles, popup behavior).  
    * If not provided, a default configuration is used.
        
    *id_zone* : `int`  
    
    * ID of the traffic analysis zone to be visualized.
        
    *zone_label* : `str`  
    
    * Human-readable label for the zone (e.g., city or region name). Used in legends and info boxes.
        
    *from_zone* : `bool`  
    
    * Whether to compute the network analysis **from** the selected zone.
        
    *to_zone* : `bool`  
    
    * Whether to compute the network analysis **to** the selected zone.  
    * Only one of `from_zone` or `to_zone` must be True. If both or neither are True, a `ValueError` is raised.

    **Public Method:**
        
    - `generate_map()`: Generates and saves the interactive map based on selected layers.

    **Helper Methods**

    - `info()`: Displays general settings for the current HeatMap instance
    - `show_layers()`: Displays the list of valid choropleth layers (with categories)
    - `show_popup_fields()`: Displays valid popup fields based on database columns.

    Attributes
    ----------
    id_zone : int
        The validated zone ID used for network and map generation.

    direction : dict
        Dictionary specifying the computation direction, with keys:  
        - "primary": either "from" or "to" (depending on user selection)  
        - "secondary": the inverse direction.  

    impacts : list of str
        List of environmental impact types available for display (e.g., "EP", "CO2", "TCO").

    network_config : ParamConfig
        Validated transport network configuration.

    heatmap_config : HeatMapConfig
        Map configuration (styling, output, popups, tiles, etc.).

    columns_metadata : dict
        Metadata about available database columns, including display alias and unit.

    heatmap_layers : dict
        All layers that can be visualized as choropleths, including their types and field dependencies.

    table : pd.DataFrame or None
        Partial network result for the selected zone, loaded from the database.  
        Set to `None` at initialization, populated during `generate_map()`.

    geo_data : gpd.GeoDataFrame or None
        Geographic data (polygons) used to build the choropleths.  
        Set to `None` at initialization, populated during `generate_map()`.

    **Example**
    -----------
    ```python
    from transnetmap.post.heatmap import HeatMap
    from transnetmap.utils.config import ParamConfig

    config = ParamConfig(network_number=4, physical_values_set_number=2, ...)
    heatmap = HeatMap(config, id_zone=123, zone_label="Fribourg", from_zone=True)

    # Basic usage with 2 layers and no popup
    heatmap.generate_map(
        choropleths=['time_NTS', 'time_IMT'],
        popup=False
    )
    ```

    See `.show_layers()` or `.show_popup_fields()` for exploring valid inputs.
    """

    def __init__(
        self,
        network_config: Union[dict, ParamConfig],
        *,
        id_zone: int,
        zone_label: str,
        from_zone: bool = False,
        to_zone: bool = False,
        map_config: Optional[HeatMapConfig] = None
    ) -> None:
        """
        Initializes a HeatMap instance for a specific zone and direction.
    
        Parameters
        ----------
        network_config : dict or ParamConfig
            Network configuration parameters (from Results class).  
            Default required fields from `results.Results`:  
                `["network_number", "physical_values_set_number", "network_extension_type",  
                 "db_nptm_schema", "db_zones_table", "db_imt_table", "db_pt_table", "uri"]`
        id_zone : int
            The zone ID to filter the data.
        zone_label : str
            Human-readable label of the selected zone (e.g. "*Lausanne*", "*New York - JFK*").  
            This should be identified manually when using `Network.show_all()`.
        from_zone : bool, optional
            Whether to compute results **from** the zone (XOR with `to_zone`).  
            If True, filters data where 'from' = id_zone. Default is False.
        to_zone : bool, optional
            Whether to compute results **to** the zone (XOR with `from_zone`).  
            If True, filters data where 'to' = id_zone. Default is False.
        map_config : HeatMapConfig, optional
            Custom configuration for heat map generation. If None, uses default configuration.
    
        Raises
        ------
        ValueError
            If both `from_zone` and `to_zone` are True or False (must be XOR).
        """        
        # Step 1: Initialize parent class (Results)
        super().__init__(network_config)  # Pass `network_config` explicitly to Results()
        
        # Step 2: Validate the XOR condition (only one of `from_zone` or `to_zone` must be True)
        if not (from_zone ^ to_zone):
            raise ValueError("Exactly one of `from_zone` or `to_zone` must be True.")
    
        # Step 3: Store necessary attributes
        self.zone_label = "`unknown name`" if not zone_label else zone_label
        self.heatmap_config = map_config or HeatMapConfig()  # Store HeatMap configuration
        self.direction = {"primary": "from", "secondary": "to"
                          } if from_zone else {"primary": "to", "secondary": "from"}
        
        # Step 4: Validate and prepare the partial network
        self.validate_id_zone(id_zone)  # Check that the zone ID is valid
        self.prepare_partial_network(id_zone)  # Prepare data for map, according to ID
        
        # Step 5: Extract available impacts
        self.impacts = self.impacts_statut["current"]
        
        # Define a unified dictionary for all units (impacts + generic attributes)
        units = {
            impact: self.dct_impacts_instances[impact].table['impact_unit'][0]
            for impact in self.impacts
        }
        units.update({"time": "min", "length": "km"})
        
        # Create a structured dictionary mapping columns to aliases & units
        self.columns_metadata = {}
        
        for prefix in ["NTS", "IMT", "PT"]:
            for base_col, unit in units.items():
                column_name = f"{prefix}_{base_col}"  # Ex: "NTS_time", "IMT_length"
                alias = f"{prefix} {base_col.replace('_', ' ')}"  # Ex: "NTS time"
                self.columns_metadata[column_name] = {"alias": alias, "unit": unit}
        
        # Add raw column mappings (for cases without prefixes)
        self.columns_metadata["id"] = {"alias": "Zone id", "unit": "-"}
        self.columns_metadata["NTS_type"] = {"alias": "NTS type", "unit": "-"}
        self.columns_metadata["NTS_changes"] = {"alias": "NTS Changes", "unit": "-"}
                
        # Step 6: Clean up unused attributes inherited from `Results`
        self._cleanup_results_attributes()
        
        # Step 7: Initialize heatmap layers dynamically
        self._initialize_heatmap_layers()
        
        # Step 8: Initialize placeholders for processing
        self.geo_data = None # geopandas.GeoDataFrame with columns "id" and "geom"
        self.table = None # pandas.DataFrame with columns "id" and all the necessary numeric columns

    def _cleanup_results_attributes(self) -> None:
        """
        Removes unnecessary attributes inherited from Results to lighten the instance.
        """
        del self.analysis_data
        del self.dct_impacts_instances
        del self.optimisation_updated_table
        del self.impacts_statut
        del self.table

    def _log(self, message: str) -> None:
        """Handles conditional logging based on the `main_print` flag."""
        if self.main_print:
            print(message)

    def replace_all_impacts_in_db(self) -> None:
        raise NotImplementedError(
            "This method cannot be called from `HeatMap`. It is reserved for `Results`."
        )

    def show(self) -> None:
        """
        This method is not available in the `HeatMap` class.
    
        Use the `generate_map()` method instead to create and display the map.
        """
        raise NotImplementedError(
            "The `show()` method is not available in `HeatMap`. "
            "Use `generate_map()` to generate and display the map."
        )


    def _initialize_heatmap_layers(self) -> None:
        """
        Initializes the dictionary of available heatmap layers dynamically.
    
        - Standard layers: `time`, `length`, `changes`, `type`.
        - Impact layers (CO2, EP, etc.) retrieved from `self.impacts`.
        - Difference layers are generated for all possible valid comparisons.
    
        Returns
        -------
        None
            The dynamically generated dictionary is stored in `self.heatmap_layers`.
            
        Examples
        --------
        >>> heatmap = HeatMap(config, id_zone=123, zone_label="label", from_zome=True)
        >>> print(heatmap.impacts)
        ['EP']
        >>> heatmap_layers = heatmap.heatmap_layers
        >>> heatmap.show_layers()
        ...
        >>> heatmap.show_popup_fields()
        ...
        """
        self._log("Initializing heatmap layers dynamically...")
        
        transports = ["NTS", "IMT", "PT"]
        
        impacts = {}
        for impact in self.impacts:
            impacts[impact] = self.columns_metadata[f"NTS_{impact}"]["unit"]  # Retrieve unit from metadata
        
        # Base layers (fixed types)
        base_layers = {
            "time": ["min", "travel time"],
            "length": ["km", "route distance"],
        }
        
        # Add impacts layers dynamically (CO2, EP, ...)
        for impact, unit in impacts.items():
            base_layers.update({f"{impact}": [f"{unit}", f"{impact} Emissions"]})

        
        heatmap_layers = {}
    
        # Generate simple transport layers (NTS, IMT, PT)
        for transport in transports:
            for analysis_type, infos in base_layers.items():
                layer_key = f"{analysis_type}_{transport}"
                heatmap_layers[layer_key] = {
                    "type": analysis_type,
                    "unit": infos[0],
                    "depends_on": [f"{transport}_{analysis_type}"],
                    "label": f"{transport} {infos[1]}",
                    "legend_name": f"{cap_first(infos[1])} ({infos[0]})",
                }
    
        # Generate difference layers (e.g., `time_diff_NTS_IMT`)        
        heatmap_diff_layers = {}
        
        for analysis_type, infos in base_layers.items():
            for transport1, transport2 in permutations(transports, 2):
                layer_key = f"{analysis_type}_diff_{transport1}_{transport2}"
                heatmap_diff_layers[layer_key] = {
                    "type": f"{analysis_type}_difference",
                    "unit": infos[0],
                    "depends_on": [f"{transport1}_{analysis_type}", f"{transport2}_{analysis_type}"],
                    "label": f"{cap_first(analysis_type)} Difference ({transport1} - {transport2})",
                    "legend_name": f"{cap_first(analysis_type)} Difference ({infos[0]})",
                }
        
        heatmap_layers.update(heatmap_diff_layers)
    
        # Special NTS layers (changes & type)
        heatmap_layers["changes_NTS"] = {
            "type": "changes",
            "unit": "-",
            "depends_on": ["NTS_changes"],
            "label": "Route changes (NTS)",
            "legend_name": "Route changes (-)",
        }

        heatmap_layers["type_NTS"] = {
            "type": "transport_type",
            "unit": "-",
            "depends_on": ["NTS_type"],
            "label": "Route transport type (NTS)",
            "legend_name": f"Route transport type ({DCT_TYPE['with-NTS']} = NTS, "
            f"{DCT_TYPE['extend-NTS']} = {self.network_extension_type})",
        }
    
        self.heatmap_layers = heatmap_layers  # Store in the instance
    
        self._log(f"Heatmap layers initialized. Total layers: {len(heatmap_layers)}")


    # -----------------------------------------------------------------------------
    # Public Methods
    # -----------------------------------------------------------------------------
    def info(self) -> None:
        """
        Displays the current heatmap settings: selected zone, network configuration, impacts, etc.
    
        Examples
        --------
        >>> heatmap.info()
        """
        print("\nHeatMap parameters:")
        print(f" - Zone label: {self.zone_label}")
        print(f" - ID zone: {self.id_zone}")
        print(f" - Direction: {self.direction['primary']} the zone")
        print(f" - Network extension: {self.network_extension_type}")
        print(f" - Physical value set: {self.name_pvs}")
        print(f" - Network number: {self.network_number}")
        print(f" - Available impacts: {self.impacts}")

    def show_layers(self) -> None:
        """
        Displays the list of available choropleth layers (to use in `generate_map()`).
    
        These correspond to the keys in `self.heatmap_layers`.
    
        Examples
        --------
        >>> heatmap.show_layers()
        """
        from collections import defaultdict
    
        print("\nAvailable choropleth layers:")
        grouped = defaultdict(list)
        for layer in self.heatmap_layers:
            if "_diff_" in layer:
                if layer.startswith("time_"):
                    group = "Difference layers - Time"
                elif layer.startswith("length_"):
                    group = "Difference layers - Length"
                elif layer.startswith("CO2_"):
                    group = "Difference layers - CO2"
                elif layer.startswith("EP_"):
                    group = "Difference layers - EP"
                elif layer.startswith("TCO_"):
                    group = "Difference layers - TCO"
                else:
                    group = "Difference layers - Other"
            
            elif layer.startswith("time_"):
                group = "Time layers"
            elif layer.startswith("length_"):
                group = "Length layers"
            elif layer.startswith("CO2_"):
                group = "CO2 layers"
            elif layer.startswith("EP_"):
                group = "EP layers"
            elif layer.startswith("TCO_"):
                group = "TCO layers"
            elif layer.startswith("changes_"):
                group = "Changes layers"
            elif layer.startswith("type_"):
                group = "Type layers"
            
            else:
                group = "Other"
    
            grouped[group].append(layer)
    
        for group, layers in grouped.items():
            print(f"\n- {group}:")
            for l in layers:
                print(f"  - {l}")
    
        print("\nUse `self.heatmap_layers` to access metadata.\n")

    def show_popup_fields(self) -> None:
        """
        Displays the available popup fields (database-native columns).
    
        These can be used in the `popup` argument of `generate_map()`.
    
        Examples
        --------
        >>> heatmap.show_popup_fields()
        """
        print("\nAvailable popup fields:\n")
        print("  Field".ljust(20) + "→  Alias (unit)")
        print("  " + "-" * 50)
        for field, meta in self.columns_metadata.items():
            alias_unit = f"{meta['alias']} ({meta['unit']})"
            print(f"  {field.ljust(20)}→  {alias_unit}")
        print("\nUse `self.columns_metadata` for details.\n")


    def generate_map(
        self,
        *,
        choropleths: Optional[List[str]] = None,
        popup: Optional[Union[bool, None, List[str]]] = None,
        file_name: Optional[str] = None,
        save_to_desktop: Optional[bool] = None,
        custom_path: Optional[str] = None,
        open_browser: Optional[bool] = None,
        map_tiles: Optional[List[str]] = None,
        include_stations: Optional[bool] = None,
        include_network_layers: Optional[bool] = None,
    ) -> Optional[folium.Map]:
        # TODO: V2, If necessary, add the `HeatMapConfig()` as parameter here if relevant.
        """
        Generates and saves an interactive Folium heatmap based on the current network settings.
        
        This is the main method of the `HeatMap` class. It computes and renders choropleth layers, 
        adds optional popups, overlays station or network data, configures map tiles, and saves the result 
        as a standalone HTML file. 
        
        All parameters default to values defined in the `HeatMapConfig` instance if not provided explicitly.
        
        Parameters
        ----------
        choropleths : list of str, optional
            A list of choropleth layers to display. Each entry must match a valid key in `self.heatmap_layers`.  
            If None, a map is generated without any heat layers. 
            
            To preview available options, use `self.show_layers()`.
        
        popup : bool or list of str or None, optional
            Defines how popups behave:  
                
            - `False` → disables popups entirely.  
            - `True` → auto-selects fields based on the `depends_on` attributes of selected choropleths.  
            - `list` → custom fields from the table columns (`self.table.columns`).  
            - `None` → falls back to the `HeatMapConfig.popup_fields` setting.  
            
            To preview available options, use `self.show_popup_fields()`.  
        
        file_name : str, optional
            The base filename (without extension) for saving the HTML map.  
            Defaults to `"heatmap"` via config.
        
        save_to_desktop : bool, optional
            If True, saves the HTML file directly to the user's desktop.  
            Mutually exclusive with `custom_path`.
        
        custom_path : str, optional
            If provided, saves the map to this exact directory. Must be a valid path with write permissions.
        
        open_browser : bool, optional
            If True, the generated map will open automatically in the browser after saving.
        
        map_tiles : list of str, optional
            List of background tile layers to load (e.g., `"CartoDB Voyager"`, `"OpenStreetMap"`).   
            Defaults to config settings. The first tile is used as the default background.
        
        include_stations : bool, optional
            If True, includes a transport station marker layer on the map.
        
        include_network_layers : bool, optional
            If True, overlays the new network geometries (NTS `Stations` and NTS `Links`).
        
        Notes
        -----
        - All arguments must be passed as keyword arguments. For example:  
            `generate_map(choropleths=['time_PT'], popup=True)`  
            `generate_map(['time_PT'], True)` (ambiguous)
    
        - Choropleth styles (color, opacity, etc.) are controlled by `HeatMapConfig.choropleth_style`.
    
        - Choropleth thresholds (bins, ranges, colors) are auto-computed unless specified
          in `HeatMapConfig.thresholds_scale['scale']`.
    
        - A warning is logged if the estimated HTML file size becomes too large (> 300–500 MB),
          which may impact loading performance in some browsers.
    
        Raises
        ------
        ValueError
            If any `choropleths` or `popup` fields are invalid or unknown.  
            If custom save paths do not exist or are not writable.  
            If tile names are unknown.  
    
        Returns
        -------
        None
            The method saves the generated HTML map and opens it in the browser (if enabled).  
            *The object is returned only for internal use.*
    
        Examples
        --------
        >>> config = ParamConfig(network_number=1, physical_values_set_number=2, network_extension_type="IMT")
        >>> heatmap = HeatMap(config, id_zone=123, zone_label="Fribourg", from_zone=True)
        >>> heatmap.generate_map(
        ...     choropleths=['time_NTS', 'time_IMT'],
        ...     popup=False,
        ...     save_to_desktop=True
        ... )
        """
        self._log("Generating heat map...")
        
        # Step 1: Handle parameter priorities (user input > config default)
        file_name = file_name if file_name is not None else self.heatmap_config.file_name
        save_to_desktop = save_to_desktop if save_to_desktop is not None else self.heatmap_config.save_to_desktop
        custom_path = custom_path if custom_path is not None else self.heatmap_config.custom_path
        open_browser = open_browser if open_browser is not None else self.heatmap_config.open_browser
        map_tiles = map_tiles if map_tiles is not None else self.heatmap_config.map_tiles
        include_stations = include_stations if include_stations is not None else self.heatmap_config.include_stations
        include_network_layers = include_network_layers if include_network_layers is not None else self.heatmap_config.include_network_layers
        
        # Step 2: Load and process heatmap data        
        self._load_heatmap_data()
        
        # Step 3: Validate & preprocess requested choropleths and popup
        choropleths, popup = self._validate_choropleths_popup_name(choropleths, popup)
        n_layers = len(choropleths) + (1 if popup else 0)
        warning_log = self._warn_if_map_too_heavy(n_layers)
        
        # Step 4: Create the base map
        self._log("Creating base map...")
        m = auto_fit_map(
            self.geo_data,
            location=self.heatmap_config.location,
            zoom_start=self.heatmap_config.zoom_start,
            tiles=False,
        )
        setup_tiles(m, map_tiles=map_tiles)
        
        # Step 5: Add stations & network layers if requested
        if include_stations:
            self._log("Adding station layer...")
            stations_layer = Stations(self.config).read_sql().show(return_layers=True) 
            m.add_child(stations_layer, name="markers_stations")
    
        if include_network_layers:
            self._log("Adding network layers...")
            network_layers = Network(self.config).read_sql().show(return_layers=True)
            for name_fg, layer in network_layers.items():
                m.add_child(layer, name=f"polylines_{name_fg}")
                    
        
        # Step 6: Add choropleth layers if requested, with legends
        if choropleths:
            self._log("Preparing choropleth layers...")            
            
            # Load and process heatmap data        
            self._load_heatmap_data()
            choropleth_layers = self._prepare_layers(choropleths)
            
            # Determine color scales by type of analysis
            analysis_types = {layer_types["type"] for layer_types in choropleth_layers.values()}
            data_for_scales = {atype: [] for atype in analysis_types}
            
            for layer in choropleth_layers.values():
                data_for_scales[layer["type"]].append(layer["data"]["values"])
            
            # Calculate scales only once per type, same types are concatenated
            for analysis_type, values_list in data_for_scales.items():
                all_values = pd.concat(values_list, axis=0, ignore_index=True)
                self._compute_threshold_scale(analysis_type, all_values.to_frame())
            
            # Create choropleths
            unique_captions = set()           
            for layer_name, layer_data in choropleth_layers.items():
                analysis_type = layer_data["type"]
                choropleth = self._create_choropleth_layer(layer_data["data"], layer_data["meta"])
                self._log(f"Adding choropleth layer `{layer_name}`...")
                m.add_child(choropleth, name=f"choropleth_{layer_name}")
                
                # Create branca legends, one per type of analysis
                if analysis_type in unique_captions:
                    self._log(f"Custom legend for analysis type `{analysis_type}` already exist.")
                    pass
                else:
                    legend = self._create_custom_branca_legend(analysis_type, layer_data["meta"])
                    unique_captions.add(analysis_type)
                    self._log(f"Adding legend for analysis type `{analysis_type}`...")
                    m.add_child(legend, name=f"legend_{analysis_type}")
            
        # Step 7: Add popup layer if requested
        if popup:
            popup_layer = self._create_popup_layer(popup)
            self._log("Adding popup layer...")
            m.add_child(popup_layer, name="full_popup")
        
        # Step 8: Add map infos
        self._log("Adding map infos...")
        # TODO: V2, Keep info box visible in fullscreen mode (requires JS tweak)
        self._add_static_map_info(m)
        
        # Step 9: Final cleanup & map controls
        self._log("Finalizing map...")
        # TODO: V2, Prevent overlapping of choropleth layers
        folium.LayerControl().add_to(m)
        Fullscreen(
            position="topleft",
            title="Full screen",
            title_cancel="Exit",
            force_separate_button=True,
        ).add_to(m)
        
        # Step 10: Save and display the map
        if warning_log:
            print(warning_log)
        
        self._log("Saving map...")
        show_map(
            m,
            file_name=file_name,
            save_to_desktop=save_to_desktop,
            custom_path=custom_path,
            open_browser=open_browser,
        )
    
        print("Heat map generation complete!")
        
        return m


    # -----------------------------------------------------------------------------
    # Validation Methods
    # -----------------------------------------------------------------------------
    def _validate_choropleths_popup_name(
        self,
        choropleths: Optional[Union[List[str], None]],
        popup: Optional[Union[List[str], bool, None]]
    ) -> Tuple[List[str], List[str]]:
        """
        Validates and resolves choropleth layer and popup field names for map generation.
    
        This method interprets the user-provided arguments (`choropleths`, `popup`) in combination
        with defaults defined in `HeatMapConfig`, and returns two lists of valid layer and field names.
    
        Behavior:
        - If `choropleths` is None or an empty list, no choropleth layers are generated.
        - If `popup` is:
            - False → no popups.
            - True  → popup fields auto-resolved from all `depends_on` values in the selected choropleth layers.
            - None  → fallback to `config.popup_fields`, and if still None, behaves like True.
            - List  → custom popup field names (must exist in `self.table.columns`).
    
        Validations:
        - Choropleth layers are validated against `self.heatmap_layers` (map layer types).
        - Popup fields must match native database columns in `self.table.columns`.
        - Duplicate entries are automatically removed (order preserved).
    
        Parameters
        ----------
        choropleths : list of str or None
            Names of the choropleth layers to render. Must be keys in `self.heatmap_layers`.
    
        popup : list of str, bool or None
            Controls which popup fields are displayed:
            - False → disables popups.
            - True  → auto-resolves fields from choropleth `depends_on`.
            - list  → manually selected field names.
            - None  → fallback to config (`self.heatmap_config.popup_fields`), and then True.
    
        Returns
        -------
        Tuple[List[str], List[str]]
            - choropleths_layers: Validated and de-duplicated choropleth layer names.
            - popup_fields: Validated and de-duplicated popup field names.
    
        Raises
        ------
        ValueError
            If invalid field names or types are provided.
        """
        if choropleths == [] or choropleths is None:
            choropleths_layers = []
            self._log("No choropleth layers requested. Generating a base map only.")
        elif isinstance(choropleths, list):
            if not all(isinstance(layer, str) for layer in choropleths):
                raise ValueError(
                    "`choropleths` list must contain only strings (keys of `self.heatmap_layers`)."
                )
            choropleths_layers = remove_duplicates_preserve_order(choropleths)
            invalid_layers = [layer for layer in choropleths_layers if layer not in self.heatmap_layers]
            if invalid_layers:
                raise ValueError(
                    f"Invalid choropleths requested: {invalid_layers}. Must match `self.heatmap_layers` keys."
                )
        else:
            raise ValueError(
                f"Invalid type for `choropleths`: {type(choropleths)}. Must be list or None."
            )
        
        if popup is None:
            popup = self.heatmap_config.popup_fields
            if popup is None:
                popup = True

        if popup is False:
            popup_fields = []
        elif popup is True:
            popup_fields = []
            for layer in choropleths_layers:
                popup_fields += self.heatmap_layers[layer]["depends_on"]
            self._log("No popup fields defined. Using same fields as choropleths (from `depends_on`).")
        elif isinstance(popup, list):
            if not all(isinstance(field, str) for field in popup):
                raise ValueError(
                    "`popup` list must contain only strings (columns names of `self.table`)."
                )
            popup_fields = popup
            invalid_fields = [field for field in popup_fields if field not in self.table.columns]
            if invalid_fields:
                raise ValueError(
                    f"Invalid popup requested: {invalid_fields}. Must match `self.table.columns`."
                )
            elif popup_fields == []:
                self._log("Popups list is empty (no fields selected).")
            else:
                self._log(f"Using user-defined popup fields: {popup_fields}")
        else:
            raise ValueError(f"Invalid type for `popup`: {type(popup)}. Must be bool, list, or None.")

        popup_fields = remove_duplicates_preserve_order(popup_fields)

        return choropleths_layers, popup_fields


    def _warn_if_map_too_heavy(self, n_layers: int) -> Optional[str]:
        """
        Warns the user if the estimated size of the generated HTML map may cause performance or loading issues.
    
        This function estimates the file size based on the number of geometries (points) and the number of layers.
        It logs a warning if the map is likely to be too large for reliable use in web browsers (especially Chrome).
        A second copy of the warning message can optionally be printed before saving.
    
        Estimation is based on:
        - ~40 bytes per point in the final HTML
        - Total = n_layers * number of points
    
        Thresholds:
        - ≥ 500 MB or ≥ 8 layers → Strong warning (map may crash)
        - ≥ 300 MB or ≥ 5 layers → Soft warning (slow load likely)
    
        Parameters
        ----------
        n_layers : int
            Total number of layers that include geometries (e.g., choropleths + popup).
    
        Returns
        -------
        str or None
            The full warning message if triggered, otherwise None.
        """
        n_points = int(self.geo_data.geometry.apply(
            lambda geom: len(geom.exterior.coords)
            if geom.geom_type == 'Polygon'
            else sum(len(p.exterior.coords) for p in geom.geoms)
        ).sum())
        
        # Rough JSON export estimate, ~40 bytes per point after JSON wrapping
        estimated_size_mb = round((n_layers * n_points * 40) / 1e6)
        
        warning_log = ""
        warning_header = (
            "\n" + "=" * 50 + "\n"
            "============ WARNING: HTML FILE SIZE =============\n"
            + "=" * 50 + "\n"
        )
        
        if n_layers >= 8 or estimated_size_mb > 500:
            warning_log = ("\n"
                f"Estimated HTML size: {estimated_size_mb} MB "
                f"({n_layers} layers, {n_points:,} points).\n"
                "This map may fail to load in some browsers (e.g. Chrome).\n"
                "Recommendation: Split into multiple maps.\n"
            )
        elif n_layers >= 5 or estimated_size_mb > 300:
            warning_log = ("\n"
                f"Large map detected (~{estimated_size_mb} MB, {n_layers} layers).\n"
                " May load slowly or lag. Consider reducing the number of layers.\n"
            )
        
        if warning_log:
            warning_log = warning_header + warning_log + warning_header
            self._log(warning_log)
            return warning_log
        
        return None


    # -----------------------------------------------------------------------------
    # Procces Methods
    # -----------------------------------------------------------------------------
    def _load_heatmap_data(self) -> gpd.GeoDataFrame:
        """
        Loads and processes the data required for generating the heat map.
        
        This method retrieves the transport data from the partial network (`results_{id_zone}`)
        and merges it with the corresponding zones data. The resulting dataset is then split 
        into different transport modes (NTS, IMT, PT) and stored internally for further use.

        The processed data is stored in the following attributes:
        - `self.geo_data` : GeoDataFrame containing only geometric information (zone geometries).
        - `self.table` : DataFrame containing all transport-related numerical values for analysis.

        Returns
        -------
        heatmap_data : geopandas.GeoDataFrame
            The processed GeoDataFrame with transport data merged to zones.
        
        Raises
        ------
        RuntimeError
            If required data is missing.
        """
        self._log("Loading transport data...")
    
        # Step 1: Load transport data (partial network & zones)
        results_columns = ['from', 'to', 'type', 'time', 'length', *self.impacts, 'nb_edges']
        self.read_sql_partial_network(columns=results_columns)
        zones_table = NPTM(self.config).read_sql(self.db_zones_table, columns=["id", "geom"])
    
        # Step 2: Prepare results table
        results_table = (
            self.table
            .filter(pl.col(self.direction["primary"]) == self.id_zone)
            .to_pandas()
            .drop(columns=[self.direction["primary"]])
            .rename(columns={self.direction["secondary"]: "id"})
            .set_index("id")
        )
    
        # Step 3: Split tables by transport type
        NTS_table = results_table[
            results_table["type"].isin([DCT_TYPE["with-NTS"], DCT_TYPE["extend-NTS"]])
        ].add_prefix("NTS_")
        
        IMT_table = results_table[
            results_table["type"] == DCT_TYPE["IMT"]
        ].drop(columns=["type", "nb_edges"]).add_prefix("IMT_")
        
        PT_table = results_table[
            results_table["type"] == DCT_TYPE["PT"]
        ].drop(columns=["type", "nb_edges"]).add_prefix("PT_")
    
        # Step 4: Merge everything with `zones_table`
        heatmap_data = (
            zones_table
            .merge(NTS_table, left_on="id", right_index=True, how="inner")
            .merge(IMT_table, left_on="id", right_index=True, how="inner")
            .merge(PT_table, left_on="id", right_index=True, how="inner")
        )
        
        # Step 5: Define categorical transport type
        heatmap_data["NTS_type"] = np.where(
            heatmap_data["NTS_type"] == DCT_TYPE["with-NTS"], "NTS", self.network_extension_type
        )
        
        heatmap_data["NTS_changes"] = heatmap_data["NTS_nb_edges"] - 1  # Number of segments - 1
        
        # Drop unused columns
        heatmap_data = heatmap_data.drop(columns=["NTS_nb_edges"])
        
        # Step 6: Save attributes for later usage
        self.geo_data = heatmap_data[["id", "geom"]].copy()
        self.table = heatmap_data.drop(columns="geom").copy()
        
        return heatmap_data


    def _prepare_difference_layer(self, layer_name: str) -> pd.DataFrame:
        """
        Computes a difference layer by subtracting two transport mode values.
    
        Parameters
        ----------
        layer_name : str
            The name of the requested difference layer (e.g., "time_diff_NTS_IMT").
    
        Returns
        -------
        pd.DataFrame
            A DataFrame with "id" and "values" (computed difference).
    
        Raises
        ------
        ValueError
            If the requested layer is invalid or the necessary columns are missing.
        """
        self._log(f"Preparing difference layer `{layer_name}`...")
    
        # Step 1: Validate the layer name
        if layer_name not in self.heatmap_layers:
            raise ValueError(
                f"The requested layer `{layer_name}` is not in `self.heatmap_layers` keys."
            )
    
        # Step 2: Ensure it has exactly 2 dependencies
        layer_info = self.heatmap_layers[layer_name]
        depends_on = layer_info.get("depends_on", [])
    
        if len(depends_on) != 2:
            raise ValueError(
                f"`{layer_name}` must have exactly 2 dependencies in `depends_on`. Found: {depends_on}"
            )
    
        transport1_col, transport2_col = depends_on
    
        # Step 3: Verify the required columns exist
        missing_columns = [col for col in depends_on if col not in self.table.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns for `{layer_name}`: {missing_columns}")
    
        # Step 4: Compute the difference
        values = self.table[transport1_col] - self.table[transport2_col]
    
        # Step 5: Ensure all values are numeric
        if not np.issubdtype(values.dtype, np.number):
            raise ValueError(f"Computed values for `{layer_name}` contain non-numeric data!")
    
        # Step 6: Return the processed DataFrame
        result = pd.DataFrame({"id": self.table["id"], "values": values})
    
        self._log(f"Difference layer `{layer_name}` computed successfully.")
    
        return result


    def _prepare_transport_type_layer(self) -> pd.DataFrame:
        """
        Prepares a transport type layer by converting categorical values in "NTS_type" to numeric.
    
        Returns
        -------
        pd.DataFrame
            A DataFrame with "id" and "values" where values represent encoded transport types.
    
        Raises
        ------
        ValueError
            If the "NTS_type" column is missing or contains unexpected values.
        """
        self._log("Preparing transport type layer...")
    
        # Step 1: Ensure "NTS_type" exists in the dataset
        if "NTS_type" not in self.table.columns:
            raise ValueError("Column `NTS_type` is missing from the dataset.")
    
        # Step 2: Define mapping of transport types to numeric values
        transport_mapping = {
            "NTS": DCT_TYPE["with-NTS"],
            self.network_extension_type: DCT_TYPE["extend-NTS"]  # Dynamically include IMT or PT
        }
    
        # Step 3: Verify all values are in the mapping
        unique_types = set(self.table["NTS_type"].unique())
        unexpected_types = unique_types - set(transport_mapping.keys())
    
        if unexpected_types:
            raise ValueError(f"Unexpected transport type values found: {unexpected_types}")
    
        # Step 4: Convert transport types to numeric values
        values = self.table["NTS_type"].map(transport_mapping).astype('int8')
    
        # Step 5: Return the processed DataFrame
        result = pd.DataFrame({"id": self.table["id"], "values": values})
    
        self._log("Transport type layer prepared successfully.")
    
        return result


    def _prepare_layers(self, choropleths: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Prepares the data required for generating each requested choropleth layer.

        This method processes the requested heatmap layers, retrieving the relevant
        transport data and computing derived values where necessary. It organizes 
        the data into a structured dictionary for easy integration into map generation.

        Parameters
        ----------
        choropleths : List[str]
            List of requested choropleth layer names, as defined in `self.heatmap_layers`.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            A dictionary where:
            - Keys are layer names.
            - Values are dictionaries containing:
                - "type" : str → The category of analysis (e.g., "length", "changes", "time_difference").
                - "data" : pd.DataFrame → Processed DataFrame with columns ["id", "values"].
                - "meta" : dict → Layer metadata (type, label, unit, ...).

        Raises
        ------
        ValueError
            If any requested layer is not recognized in `self.heatmap_layers`.

        Notes
        -----
        - **Standard layers (e.g., "NTS_time")** → Extracted directly from `self.table`.
        - **Difference layers (e.g., "time_diff_NTS_IMT")** → Computed using `_prepare_difference_layer()`.
        - **Transport type layer ("NTS_type")** → Processed via `_prepare_transport_type_layer()`.
        - If a requested layer has no `depends_on` fields, it is skipped.
        """
        self._log("Setup choropleth layers data...")
    
        prepared_layers = {}
    
        # Step 1: Validate requested layers
        invalid_layers = [layer for layer in choropleths if layer not in self.heatmap_layers]
        if invalid_layers:
            raise ValueError(
                f"Invalid choropleths requested: {invalid_layers}. "
                "Check `self.heatmap_layers` keys for valid options."
            )
    
        # Step 2: Prepare standard layers (time, length, impacts, changes)
        for layer in choropleths:
            layer_info = self.heatmap_layers[layer]
            dct_layer = {"type": layer_info["type"]}
    
            if "depends_on" not in layer_info:
                self._log(f"Layer `{layer}` could not be computed.\n"
                          "The link to the data (dependencies) has not been identified.")
                self._log(f"Layer `{layer}` is ignored.")
                continue  # Skip layers that don't have dependencies (e.g., placeholders)
            
            if len(layer_info["depends_on"]) == 1:
                
                # Transport type layer if requested
                if layer == "type_NTS":
                    dct_layer["data"] = self._prepare_transport_type_layer()
                    
                # Standard single-column layers
                else:
                    column_name = layer_info["depends_on"][0]
                    dct_layer["data"] = self.table[["id", column_name]].rename(
                        columns={column_name: "values"}
                    )
                    self._log(f"Standard layer `{layer}` computed successfully.")
    
            # Difference layers (e.g., time_diff_NTS_IMT)
            elif len(layer_info["depends_on"]) == 2:
                dct_layer["data"] = self._prepare_difference_layer(layer)
            
            dct_layer["meta"] = layer_info # Adding layer metadata
            
            prepared_layers[layer] = dct_layer
    
        self._log(f"{len(prepared_layers)} choropleth layers were prepared.")
    
        return prepared_layers


    # -----------------------------------------------------------------------------
    # Choropleth Management Methods
    # -----------------------------------------------------------------------------
    def _get_scale_config(
        self,
        analysis_type: str,
    ) -> Dict[str, Union[List[float], float, float, str, bool]]:
        """
        Retrieves the scale configuration for a given analysis type.
        Ensures that `scale`, `fill_color`, and `reverse_color` are properly set.
        Keys `vmin` and `vmax` are defined or set here because they are only useful locally,
            within a single configuration (HeatMapConfig).
        
        Parameters
        ----------
        analysis_type : str
            The type of analysis for which to retrieve the scale settings.
    
        Returns
        -------
        Dict[str, Union[List[float], float, float, str, bool]]
            A dictionary containing `scale`, `vmin`, `vmax`, `fill_color` and `reverse_color`.
    
        Raises
        ------
        KeyError
            If a required value is missing from `thresholds_scale`.
        """
        from transnetmap.utils.constant import DEFAULT_THRESHOLDS_SCALE_COLOR
        
        # Check if the analysis is already in `thresholds_scale`
        if analysis_type not in self.heatmap_config.thresholds_scale:
            self.heatmap_config.thresholds_scale[analysis_type] = {}
            
        # Check whether `analysis_type` is an impact or a difference
        is_difference = "difference" in analysis_type
        is_impact = analysis_type in self.impacts
    
        # Set default values for `fill_color` and `reverse_color` according to `analysis_type`
        default_config = None
        if is_difference:
            default_config = DEFAULT_THRESHOLDS_SCALE_COLOR["difference"]
        elif is_impact:
            default_config = DEFAULT_THRESHOLDS_SCALE_COLOR["impacts"]
        elif analysis_type in DEFAULT_THRESHOLDS_SCALE_COLOR:
            default_config = DEFAULT_THRESHOLDS_SCALE_COLOR[analysis_type]
        else:
            raise KeyError(f"Unknown analysis type `{analysis_type}`. No default values found.")
    
        # Ensure that the `scale` key exists
        self.heatmap_config.thresholds_scale[analysis_type].setdefault("scale", None)
        
        # Ensure that the limits keys exists
        self.heatmap_config.thresholds_scale[analysis_type].setdefault("vmin", None)
        self.heatmap_config.thresholds_scale[analysis_type].setdefault("vmax", None)
        
        # Check and update only if `None`
        for key in ["fill_color", "reverse_color"]:
            if self.heatmap_config.thresholds_scale[analysis_type].get(key) is None:
                self.heatmap_config.thresholds_scale[analysis_type][key] = default_config[key]
    
        # Check if `fill_color` and `reverse_color` are always `None`
        for key in ["fill_color", "reverse_color"]:
            if self.heatmap_config.thresholds_scale[analysis_type][key] is None:
                raise KeyError(f"`{key}` is missing for `{analysis_type}` in `thresholds_scale`.")
    
        return self.heatmap_config.thresholds_scale[analysis_type]


    def _compute_threshold_scale(
        self,
        analysis_type: str,
        data: pd.DataFrame,
        bins: int = 8,
    ) -> Tuple[str, List[float]]:
        """
        Computes the threshold scale for color mapping based on the analysis type and dataset.

        - For continuous datasets, a dynamic scale is generated using Jenks Natural Breaks.
        - If a user-defined scale is provided in the configuration, it is used instead (after validation).
        - For discrete types (`transport_type` and `changes`), fixed discrete scales are always computed automatically,
          and user-defined scales are ignored.

        Behavior
        --------
        - If a user-defined scale is provided but data values exceed its range, no modification is applied to the config.
          Instead, a warning is logged and values are clamped visually in Folium and Branca.
        - For dynamic (Jenks) scales, `vmin` and `vmax` are always set to the rounded limits of the computed scale.
        - The number of bins must be ≥ 4 to ensure compatibility with Folium choropleths.
        - Adaptive rounding to the order of magnitude of the data, for dynamic scales:
            - `< 1`: Rounded to nearest 0.01
            - `< 10`: Rounded to nearest 0.1
            - `< 100`: Rounded to nearest 1
            - `>= 100`: Rounded to nearest 5

        Parameters
        ----------
        analysis_type : str
            Type of analysis (e.g., 'time', 'changes', 'EP', etc.)
        data : pd.DataFrame
            Input dataset containing the values used for classification.
        bins : int, optional
            Desired number of bins for Jenks classification (default is 8, minimum is 4).

        Returns
        -------
        Tuple[str, List[float]]
            - The analysis type (as-is)
            - The computed or validated threshold scale for color classification.

        Raises
        ------
        ValueError
            - If the input data is invalid or contains NaNs.
            - If the number of bins is less than 4.
            - If user-defined scale is invalid.
        """
        if bins < 4:
            raise ValueError("Number of threshold bins must be at least 4.")

        self._log(f"Computing threshold scale for analysis type `{analysis_type}`...")

        # Step 1: Get scale configuration & extract values
        scale_config = self._get_scale_config(analysis_type)

        for col in data.columns:
            if data[col].isna().any():
                raise ValueError(f"Column `{col}` contains NaN values.")

        all_values = data.to_numpy().flatten()

        if all_values.size == 0:
            raise ValueError("No valid data found. Empty input.")

        # Step 2: Handle discrete types
        if analysis_type == "transport_type":
            scale, vmin, vmax = compute_boolean_scale_type(all_values)
            self._log("Forced discrete scale for analysis type `transport_type`.")

        elif analysis_type == "changes":
            scale, vmin, vmax = compute_discrete_scale_changes(all_values)
            self._log("Forced discrete scale for analysis type `changes`.")

        # Step 3: User-defined scale
        elif scale_config.get("scale") is not None:
            scale, vmin, vmax = validate_user_defined_scale(scale_config, analysis_type, all_values)

            if scale is None:
                # Should not happen but fallback safety
                scale, vmin, vmax = compute_jenks_dynamic_scale(all_values, bins)
                self._log(f"Fallback to dynamic Jenks scale for analysis type `{analysis_type}`.")

        # Step 4: Jenks dynamic scale
        else:
            scale, vmin, vmax = compute_jenks_dynamic_scale(all_values, bins)
            self._log(f"Computed Jenks scale for analysis type `{analysis_type}`: {scale}")

        # Step 5: Store in config
        self.heatmap_config.thresholds_scale[analysis_type] = {
            "scale": scale,
            "vmin": vmin,
            "vmax": vmax,
            "fill_color": scale_config["fill_color"],
            "reverse_color": scale_config["reverse_color"],
        }

        return analysis_type, scale


    def _create_custom_branca_legend(self, analysis_type: str, layer_meta: dict) -> StepColormap:
        """
        Creates a custom Branca StepColormap legend for a given analysis layer.
    
        - Uses the threshold scale and color mapping from `heatmap_config`.
        - Adjusts tick labels and layout for discrete types like `transport_type` and `changes`.
    
        Parameters
        ----------
        analysis_type : str
            Type of analysis (e.g. 'time', 'EP', 'transport_type', etc.).
        layer_meta : dict
            Metadata for the layer, including at least the `legend_name`.
    
        Returns
        -------
        StepColormap
            A Branca colormap object ready to be added to a folium map.
    
        Raises
        ------
        ValueError
            If the scale or color configuration is missing or inconsistent.
        """
        self._log(f"Creating custom legend for analysis type `{analysis_type}`...")
        
        config = self.heatmap_config.thresholds_scale.get(analysis_type, {})
        scale = config.get("scale")
        color_scale = config.get("color_scale")
        vmin = config.get("vmin")
        vmax = config.get("vmax")
    
        if not scale or not color_scale:
            raise ValueError(
                f"Missing scale or color scale for analysistype `{analysis_type}`. "
                "Cannot create legend."
            )
        
        if len(scale) != len(color_scale) + 1:
            raise ValueError(
                f"Mismatch in scale and color count for analysis type `{analysis_type}`.\n"
                f"Expected {len(scale) - 1} colors for {len(scale)} thresholds, "
                "got {len(color_scale)} colors."
            )
    
        caption = layer_meta.get("legend_name", f"{analysis_type} (unknown unit)")
        tick_labels = None
        width = 400
    
        # Special formatting for transport_type (boolean categorical values)
        if analysis_type == "transport_type":
            if scale[0] == (vmin - 1.5):  # e.g. values 6 & 7 → [4.5, 5.5, 6.5, 7.5]
                color_scale = color_scale[1:]
            else:  # e.g. values 6 & 8 → [5.5, 6.5, 7.5, 8.5]
                color_scale = [color_scale[0], color_scale[-1]]
    
            inter = vmax - vmin
            scale = [vmin - inter / 2, vmin + inter / 2, vmax + inter / 2]
            tick_labels = [vmin, vmax]
            width = 250
    
        # Special formatting for discrete integer "changes"
        elif analysis_type == "changes":
            scale = np.arange(vmin - 0.5, vmax + 1.5, 1).tolist()
            tick_labels = list(range(int(vmin), int(vmax + 1)))
            color_scale = color_scale[:len(tick_labels)]
            width = min(max(len(color_scale) * 50, 150), 300)
    
        colormap = StepColormap(
            colors=color_scale,
            index=scale,
            vmin=scale[0],
            vmax=scale[-1],
            caption=caption,
            tick_labels=tick_labels
        )
        colormap.width = width
    
        return colormap


    def _remove_choropleth_legend(self, choropleth: folium.Choropleth) -> None:
        """
        Removes the auto-generated color map (legend) from a single folium.Choropleth layer.
        
        Parameters
        ----------
        choropleth : folium.Choropleth
            The Choropleth object from which to remove the internal color map legend.
        
        Notes
        -----
        - Folium stores the legend in a `_children` element starting with "color_map".
        - This method should be used after creating a Choropleth when you want to display
          a custom legend instead.
        """
        keys_to_remove = [key for key in choropleth._children if key.startswith("color_map")]
        
        for key in keys_to_remove:
            del choropleth._children[key]
        
        if keys_to_remove:
            self._log(f"Removed legend from choropleth layer `{choropleth.layer_name}`...")
        else:
            self._log(f"No legend found to remove in layer `{choropleth.layer_name}`.")


    def _create_choropleth_layer(self, data: pd.DataFrame, layer_meta: dict) -> folium.Choropleth:
        """
        Creates a choropleth layer for the heatmap visualization.
        The threshold scale must be defined upstream using `_compute_threshold_scale()`.
        
        Parameters
        ----------
        data : DataFrame
            The processed dataset containing the column to be mapped.
            Must have columns: ["id", "values"].
        
        layer_meta : dict
            (The name of the layer as it will appear in LayerControls, type of layer, unit, etc.)
        
        Returns
        -------
        folium.Choropleth
            The generated folium Choropleth layer.
            
        Notes V1
        --------
        Certain style parameters from `HeatMapConfig` are force-overridden for display consistency:
        - `overlay=True`
        - `control=True`
        - `show=False`
        
        This ensures that:
        - Layers always appear in the LayerControl,
        - Background map tiles remain visible beneath the overlay.
        - When the map is open, the choropleth layers do not overlap.
        
        These values cannot currently be customized.
        """
        self._log(f"Creating choropleth layer `{layer_meta['label']}`...")

        # Step 1: Validate input data
        missing_columns = set(["id", "values"]) - set(data.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")

        # Step 2: Retrieve settings
        layer_type = layer_meta['type']
        thresholds_scale = self.heatmap_config.thresholds_scale[layer_type]
        style = self.heatmap_config.choropleth_style
        
        # TODO: V2, Currently forced properties HeatMapConfig for display purposes
        self._log("Choropleth layer settings (overlay, control, show) "
                  "have been forced for consistent display.")
        style["overlay"] = True # Determines if the layer is an overlay
        style["control"] = True # Determines if the layer appears in LayerControls
        style["show"] = False # Default visibility of the layer
        
        # Define bin edges dynamically.
        if data["values"].min() < thresholds_scale["vmin"] or data["values"].max() > thresholds_scale["vmax"]:
            bins_for_folium = adjust_bins_for_folium(thresholds_scale["scale"])
        else:
            bins_for_folium = thresholds_scale["scale"]
        
        # Step 3: Apply reverse color scale if needed
        fill_color = thresholds_scale["fill_color"]
        if thresholds_scale["reverse_color"]:
            fill_color += "_r"
        
        # Step 4: Create the Choropleth layer
        choropleth = folium.Choropleth(
            geo_data=self.geo_data, # GeoDataFrame with GeoJSON geometries.
            data=data[["id", "values"]], # Data to bind to the GeoJSON. DataFrame with columns ["id", "values"]
            columns=["id", "values"], # Must pass column 1 as the key, and column 2 the values.
            key_on="feature.properties.id", # Variable in the geo_data GeoJSON file to bind the data to.
            bins=bins_for_folium,
            name=layer_meta["label"], # Name of the layer, in `LayerControl`
            fill_color=fill_color,
            fill_opacity=style["fill_opacity"],
            line_color=style["line_color"],
            line_weight=style["line_weight"],
            line_opacity=style["line_opacity"],
            smooth_factor=style["smooth_factor"],
            overlay=style["overlay"],
            control=style["control"],
            show=style["show"],
        )
        
        # Step 5: Store the actual color scale used by folium
        colormap = choropleth.color_scale        
        colors_hex = [matplotlib.colors.to_hex(c) for c in colormap.colors]# Convert RGBA → HEX
        self.heatmap_config.thresholds_scale[layer_type]["color_scale"] = colors_hex
        
        # Step 6: Remove the `branca.colormap.StepColormap` automatically created by folium
        self._remove_choropleth_legend(choropleth)
        
        return choropleth


    # -----------------------------------------------------------------------------
    # Popup Management Methods
    # -----------------------------------------------------------------------------
    def _create_popup_layer(self, popup_fields: List[str]) -> folium.GeoJson:
        """
        Creates a folium popup layer using selected fields from the result table.
    
        The popup layer is styled according to the heatmap config and includes
        human-readable field names and units as aliases.
    
        Parameters
        ----------
        popup_fields : List[str]
            List of column names to include in the popup. Must exist in both
            `self.table` and `self.columns_metadata`.
    
        Returns
        -------
        folium.GeoJson
            A GeoJson layer with formatted popups and interactive highlighting.
    
        Notes
        -----
        - The popup is styled using `popup_style` and `popup_geodata_style` from the config.
        - Aliases for popup fields are constructed from `columns_metadata`.
        - The base `geo_data` is merged with `self.table` to include required fields.
        
        Notes V1
        --------
        Certain style parameters from `HeatMapConfig` are force-overridden for display consistency:
        - `overlay=True`
        - `control=True`
        
        This ensures that:
        - Layers always appear in the LayerControl,
        - Background map tiles remain visible beneath the overlay.
        
        These values cannot currently be customized.
        """
        self._log("Creating data popups...")
    
        # Step 1: Create the `aliases` list for GeoJson
        missing = [col for col in popup_fields if col not in self.columns_metadata]
        if missing:
            raise ValueError(f"Metadata missing for popup fields: {missing}")
        
        fields = popup_fields
        aliases = [
            f"{self.columns_metadata[col]['alias']} ({self.columns_metadata[col]['unit']})"
            for col in popup_fields
        ]
                
        # Step 2: Add data to GeoDataFrame
        # Ensure 'id' is included for the merge, even if not shown in the popup
        merge_fields = ["id"] + [f for f in fields if f != "id"]
        data = self.geo_data.merge(self.table[[*merge_fields]], on="id", how="inner")
        
        # Step 3: Define style functions
        popup_geodata_style = self.heatmap_config.popup_geodata_style
        popup_geodata_highlight = self.heatmap_config.popup_geodata_highlight
        popup_style = self.heatmap_config.popup_style
        
        # TODO: V2, Currently forced properties HeatMapConfig for display purposes
        self._log("Popup layer settings (overlay, control) have been forced for consistent display.")
        popup_style["overlay"] = True # Determines if the layer is an overlay
        popup_style["control"] = True # Determines if the layer appears in LayerControls
    
        # Step 4: Create popup layer
        popup_layer = folium.GeoJson(
            data,
            style_function=lambda x: popup_geodata_style,
            highlight_function=lambda x: popup_geodata_highlight,
            name=popup_style["name"],
            overlay=popup_style["overlay"],
            control=popup_style["control"],
            show=popup_style["show"],
            popup=folium.features.GeoJsonPopup(
                fields=fields,
                aliases=aliases,
                style=popup_style["html_fields"],
            ),
        )
    
        return popup_layer


    def _add_static_map_info(self, m: folium.Map) -> None:
        """
        Adds a static information box to the map with key network parameters.

        The info box is positioned in the bottom-left corner and contains:
        - The network number, physical value set, and extension type.
        - The selected zone and its direction (from/to).
        - An optional description of data sources (wrapped to fit width).

        The box is styled to match folium's built-in UI elements.

        Parameters
        ----------
        m : folium.Map
            The map object to which the information box is added.
        """
        from folium.elements import Element
        
        hr_line = "<hr style='border: none; border-top: 1px solid #ccc; margin: 4px 0 4px 0;'>"
        title = "<div><b style='font-size: 14px;'>Network settings</b></div>"
                
        lines_info = [
            f"<div>Network: <b>#{self.network_number}</b>",
            f"Physical set: <b>PVS #{self.config.physical_values_set_number}</b>",
            f"Extension type: <b>{self.network_extension_type}</b>",
            f"Zone: <b>{self.direction['primary']} {self.zone_label}</b> (ID = {self.id_zone})</div>"
        ]
        div_info = "<br>".join(lines_info)
        
        raw_sources = self.heatmap_config.data_source_note
        if not raw_sources:
            self._log("No data source note specified in `HeatMapConfig`.\n"
                      "You can set one with `config.data_source_note = '...'`.")
            text_sources = "<i>not specified</i>"
        else:
            from transnetmap.utils.utils import wrap_text_at_space
            max_line_len = max(len(i) for i in lines_info) # HTML tags for ´zones´ are 13 characters long.
            text_sources = "<br>" + wrap_text_at_space(raw_sources, max_line_len)
            
        div_sources = f"<div>Sources: {text_sources}</div>"
        
        html_content = title + hr_line + div_info + hr_line + div_sources
        
        html_box = f"""
        <div id="map-info-box" style="
            position: fixed;
            bottom: 10px;
            left: 10px;
            z-index: 9999;
            background-color: #fff;
            padding: 8px 12px;
            border: 1px solid #bbb;
            border-radius: 6px;
            font-size: 13px;
            font-family: Arial, sans-serif;
            color: #333;
            line-height: 1.4;
        ">
            {html_content}
        </div>
        """
        m.get_root().html.add_child(Element(html_box), name="map_info_box") # Name is unused with "get_root()"


# -----------------------------------------------------------------------------
# Main Execution (Testing)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    
    # Network creation and calculation parameters
    network_config = ParamConfig(**{
        "network_number": 4,
        "physical_values_set_number": 2,
        "network_extension_type": "IMT",
        "main_print": True,
        "sql_echo": False,
        "db_nptm_schema": "nptm17",
        "db_zones_table": "zones17",
        "db_imt_table": "imt22",
        "db_pt_table": "pt22",
        "uri": "postgresql://username:password@host:port/database",
    })
    
    # ===========================
    # === init ===
        
    heatmap = HeatMap(network_config, id_zone=6882, zone_label="Vevey" , from_zone=True)
    
    heatmap.info()
    heatmap.show_layers()
    heatmap.show_popup_fields()
    
    # === generate map ===
    
    choro = ['time_NTS', 'time_IMT']
    popup = []
    
    heatmap.generate_map(
        choropleths=choro, 
        popup=False, 
    )
    
    
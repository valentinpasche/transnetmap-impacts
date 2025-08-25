# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Union, Any
from urllib.parse import urlparse

@dataclass
class ParamConfig:
    """
    Base configuration class for handling parameters across all transnetmap classes,
    with extended validation for complex parameters.
    
    Use `ParamConfig.describe()` to display a clean summary of current settings.

    Attributes
    ----------
    network_number : Optional[int]
        Identification number for the network being created or manipulated. This is
        typically required for all transnetmap classes.
    physical_values_set_number : Optional[int]
        Identification number for the physical value set (PVS) used in conjunction
        with specific networks or scenarios. Required for analysis and post-processing.
    network_extension_type : Optional[str]
        Specifies the type of transportation to use for extending the network in analysis.
        Accepted values are:
            - 'IMT': Refers to the Individual Motorized Transport table.
            - 'PT': Refers to the Public Transport table.
        This parameter determines which dataset is used to supplement the network
        with additional information during computations such as Dijkstra's algorithm.
    db_nptm_schema : Optional[str]
        Name of the schema in the PostgreSQL database that contains data from the
        National Passenger Traffic Model (NPTM). Commonly used for zone and travel
        data management.
        This schema serves as a namespace for the model and acts as an identifier
        used in constructing dependent table names.
    db_zones_table : Optional[str]
        Name of the table containing data relating to NPTM zones. This table is
        essential for spatial operations and visual outputs such as heat maps.
    db_imt_table : Optional[str]
        Name of the table containing Individual Motorized Transport (IMT) data, such
        as travel times and distances. Used in NPTM-related computations and post-processing.
    db_pt_table : Optional[str]
        Name of the table containing Public Transport (PT) data, such as travel
        times and distances. Used in NPTM-related computations and post-processing.
    uri : Optional[str]
        Connection string for the PostgreSQL database, formatted as 
        "postgresql://user:password@host:port/database". This is required for any
        database interaction.
    main_print : bool
        Controls whether general execution information should be printed to the
        console. Useful for monitoring progress in scripts or debugging.
    sql_echo : bool
        Controls whether SQL statements executed via SQLAlchemy should be logged
        to the console. Primarily useful for debugging database interactions.
    required_fields : List[str]
        List of field names that are required for validation. This is set 
        dynamically in the context of each class that uses ParamConfig.
    """
    # Global parameters (common to all classes)
    network_number: Optional[int] = None  # Unique identifier for a network instance.
    physical_values_set_number: Optional[int] = None  # Identifier for a physical value set (e.g., pvs1, pvs2).
    network_extension_type: Optional[str] = None  # Identifier for the type ('IMT' or 'PT') for network extension.
    db_nptm_schema: Optional[str] = None  # Name of the schema for NPTM data in the database.
    db_zones_table: Optional[str] = None  # Table name for traffic zones data.
    db_imt_table: Optional[str] = None  # Table name for Individual Motorized Transport (IMT) travel data.
    db_pt_table: Optional[str] = None  # Table name for Public Transport (PT) travel data.
    uri: Optional[str] = ""  # PostgreSQL connection string, required for database access.
    main_print: bool = False  # Toggles general execution information in the console.
    sql_echo: bool = False  # Toggles SQL logging for debugging database interactions.

    # Custom field validation (e.g., required fields)
    required_fields: List[str] = field(default_factory=list)  # Dynamically set in each class.
    
    def validate(self) -> "ParamConfig":
        """
        Validate that all required fields are provided and check complex formats.
        """
        # Validate required fields
        for field_name in self.required_fields:
            if getattr(self, field_name) is None:
                raise ValueError(f"Required parameter '{field_name}' is missing.")
        
        # Check parameter types
        self._validate_types()

        # Validate specific fields
        self._validate_uri() # Validate the uri string for database
        self._validate_network_extension_type() # Validate the network extension type

    def _validate_types(self):
            """
            Explicitly validate types for each field.
            """
            type_map = {
                "network_number": (int, type(None)),
                "physical_values_set_number": (int, type(None)),
                "network_extension_type": (str, type(None)),
                "db_nptm_schema": (str, type(None)),
                "db_zones_table": (str, type(None)),
                "db_imt_table": (str, type(None)),
                "db_pt_table": (str, type(None)),
                "uri": (str,),
                "main_print": (bool,),
                "sql_echo": (bool,),
            }
    
            # Validate types only for fields in required_fields
            for field_name in self.required_fields:
                value = getattr(self, field_name)
                expected_types = type_map.get(field_name, None)
    
                if expected_types is None:
                    raise KeyError(f"Field '{field_name}' is not recognized in type_map.")
                if not isinstance(value, expected_types):
                    raise TypeError(
                        f"Parameter '{field_name}' must be of type {expected_types}, got {type(value).__name__}."
                    )
    
    def _validate_uri(self):
        """
        Validate the format of the URI field.
        """
        if not self.uri:
            return  # Skip validation if URI is not required

        # Parse the URI using urlparse
        parsed_uri = urlparse(self.uri)

        # Validate the scheme, host, and port
        if parsed_uri.scheme != "postgresql":
            raise ValueError("The 'uri' must start with 'postgresql://'.")
        if not parsed_uri.hostname:
            raise ValueError("The 'uri' must include a valid hostname.")
        if not parsed_uri.port:
            raise ValueError("The 'uri' must include a valid port.")
        if not parsed_uri.path or parsed_uri.path == "/":
            raise ValueError("The 'uri' must include a database name in the path.")
            
    def _validate_network_extension_type(self):
        """
        Validate the 'network_extension_type' parameter.
        """
        if not self.network_extension_type:
            return  # Skip validation if network_extension_type is not required
        
        if self.network_extension_type not in {'IMT', 'PT', None}:
            raise ValueError(
                f"Invalid 'network_extension_type': {self.network_extension_type}\n"
                "It must be one of: 'IMT', 'PT', or None."
            )
        
    def validate_for_class(self, required_fields: list):
        """
        Validates that the specified required fields are present in the ParamConfig object.

        Parameters
        ----------
        required_fields : list of str
            List of field names that must be validated.

        Raises
        ------
        ValueError
            If any required field is missing.
        """
        missing_fields = [field for field in required_fields if getattr(self, field, None) is None]
        if missing_fields:
            raise ValueError(f"Missing required parameters: {', '.join(missing_fields)}")

    def describe(self):
        """
        Displays a summary of the current network configuration.
    
        Includes parameters such as network number, schema/table names, and connection URI.
        """
        print("\n🧩 ParamConfig (network settings):")
        print(f" - Network number           : {self.network_number}")
        print(f" - Physical value set       : {self.physical_values_set_number}")
        print(f" - Extension type           : {self.network_extension_type}")
        print(f" - Schema (NPTM)            : {self.db_nptm_schema}")
        print(f" - Table (zones)            : {self.db_zones_table}")
        print(f" - Table (IMT)              : {self.db_imt_table}")
        print(f" - Table (PT)               : {self.db_pt_table}")
        print(f" - PostgreSQL URI           : {self.uri}")
        print(f" - SQL Echo                 : {self.sql_echo}")
        print(f" - Print summary            : {self.main_print}")


@dataclass
class HeatMapConfig:
    """
    Configuration container for heatmap generation.
    
    This dataclass centralizes all map-related display and export options
    used by the `HeatMap` class and its associated visualizations. It allows
    the user to control map appearance, layer visibility, file saving behavior,
    and popup content selection.
    
    Configuration class for controlling map generation and visualization in the `HeatMap` workflow.
    
    This class defines all behavior related to how maps are displayed, saved, and annotated. It is
    designed to be passed as a configuration object to the `HeatMap` class and allows users to customize:
    - General export behavior (file saving and browser opening)
    - Map tile and display settings
    - Layer inclusion (networks, stations)
    - Choropleth behavior and styles
    - Popup field selection and styles
    - Optional metadata (data source notes)
    
    Use `HeatMapConfig.describe()` to display a clean summary of current settings.
    
    Sections
    --------
    🔹 General map and export parameters
    Controls basic HTML export behavior (file name, output path, browser open).
    - file_name : str
        Default name for the exported HTML map file.
    - save_to_desktop : bool
        If True, saves the map directly to the user's desktop.
    - custom_path : str or None
        Optional custom directory where the map should be saved.
    - open_browser : bool
        Whether to open the map in the default web browser after saving.
    
    🔹 Map display and tile layers
    Controls tile background(s), zoom level, and map centering behavior.
    - map_tiles : List[str]
        List of tile providers to include on the map.
    - zoom_start : int or None
        Initial zoom level of the map. If None, uses auto-fit.
    - location : List[float] or None
        Fallback map center (lat, lon) if zoom/fit fails.
    
    🔹 Layer toggles
    Allows including transport networks and stations on the map.
    - include_network_layers : bool
        Whether to display transport network layers.
    - include_stations : bool
        Whether to display transport station markers.
    
    🔹 Choropleth configuration
    Controls the color scaling behavior and general styles of the heatmap layers.
    - thresholds_scale : Dict[str, Dict[str, Any]]
        Parameters controlling StepColormap appearance for each analysis type.
    - choropleth_style : Dict[str, Any]
        Visual styling of filled heatmap layers (opacity, border, etc.)
    
    🔹 Popups
    Controls which database fields appear in interactive map popups and their formatting.
    - popup_fields : List[str] or bool or None
        Controls which database fields are shown in popup (see `HeatMap.generate_map()` for rules).
    - popup_style : Dict[str, Any]
        Style options for HTML popups (font, overlay, visibility, etc.)
    - popup_geodata_style : Dict[str, Any]
        Base style for the popup’s GeoJson outline.
    - popup_geodata_highlight : Dict[str, Any]
        Hover style applied when the user points to a feature.
    
    🔹 Metadata
    Includes optional free-text notes (e.g., data sources), shown in the info box on the map.
    - data_source_note : str or None
        Optional note that describes the source of the input transport data.
        Displayed in the bottom-left info box on the map.
    
    Notes
    -----
    - Threshold scales (`thresholds_scale["scale"]`) are computed automatically by `generate_map()` 
      if left as `None`. You may override them here to apply fixed thresholds.
    
    - Default colors (`fill_color`) for each analysis type are stored in:
        `transnetmap.utils.dct.defaults_thresholds_scale_color`
    
    - The following keys in the `choropleth_style` and `popup_style` dictionaries are passed directly 
      to Folium layers and must follow Folium API:
        https://python-visualization.github.io/folium/
    
    - Use `HeatMapConfig.describe()` to display current settings in a user-friendly summary.
    
    This class does not contain internal logic—it is used only for configuration injection.
    """  
    # 🔹 General settings
    data_source_note: Optional[str] = None  # Appears in top-left map box
    map_tiles: List[str] = field(default_factory=lambda: ["CartoDB Voyager", "OpenStreetMap"])
    zoom_start: int = None
    location: Optional[Tuple[float, float]] = None  # Auto-fit if None
    file_name: Optional[str] = "heatmap"
    save_to_desktop: bool = False
    custom_path: Optional[str] = None
    open_browser: bool = True
    include_network_layers: bool = True
    include_stations: bool = True

    # 🔹 Choropleth settings
    thresholds_scale: Dict[str, Dict[str, Union[List[float], str, bool]]] = field(default_factory=lambda: {
        "time": {"scale": None, "fill_color": None, "reverse_color": None},
        "length": {"scale": None, "fill_color": None, "reverse_color": None},
        "changes": {"scale": None, "fill_color": None, "reverse_color": None},
        "transport_type": {"scale": None, "fill_color": None, "reverse_color": None},
        "impacts": {"fill_color": None, "reverse_color": None},  # Placeholder for all impacts
        "difference": {"fill_color": None, "reverse_color": None},  # Placeholder for all differences
    })
    
    choropleth_style: Dict[str, Union[str, float, int, bool]] = field(default_factory=lambda: {
        "fill_opacity": 0.8,  # Transparency of filled areas
        "line_color": "black",  # Border color
        "line_weight": 1,  # Border thickness
        "line_opacity": 0.2,  # Transparency of borders
        "smooth_factor": 1,  # Controls smoothing of geometries
        "overlay": True,  # Determines if the layer is an overlay
        "control": True,  # Determines if the layer appears in LayerControls
        "show": False,  # Default visibility of the layer
    })

    # 🔹 Popup settings
    popup_fields: Union[List[str], bool] = None  # Optional: Defines which columns to include in popups (with aliases).

    popup_style: Dict[str, Any] = field(default_factory=lambda: {
        "name": "Data popup", # Name of the layer
        "overlay": True,  # Determines if the layer is an overlay
        "control": True,  # Determines if the layer appears in LayerControls
        "show": False,  # Default visibility of the layer
        "html_fields": "color: #333333; font-family: arial; font-size: 11px; padding: 10px;"
    })
    
    popup_geodata_style: Dict[str, Any] = field(default_factory=lambda: {
        "fillColor": "#ffffff",
        "color": "#000000",
        "fillOpacity": 0.1,
        "weight": 0.1,
    })
    
    popup_geodata_highlight: Dict[str, Any] = field(default_factory=lambda: {
        "fillColor": "#000000",
        "color": "#000000",
        "fillOpacity": 0.3,
        "weight": 0.1,
    })

    def describe(self):
        """
        Displays a summary of the heatmap configuration (excluding visual styles).
    
        Includes map behavior options, export settings, and data field controls.
        """
        print("\n🗺️ HeatMapConfig:")
        print(f" - file_name             : {self.file_name}")
        print(f" - save_to_desktop       : {self.save_to_desktop}")
        print(f" - custom_path           : {self.custom_path or 'None'}")
        print(f" - open_browser          : {self.open_browser}")
        print(f" - include_stations      : {self.include_stations}")
        print(f" - include_network_layers: {self.include_network_layers}")
        print(f" - map_tiles             : {self.map_tiles}")
        print(f" - zoom_start            : {self.zoom_start}")
        print(f" - fallback location     : {self.location or 'Auto-fit'}")
        print(f" - popup_fields          : {self.popup_fields}")
        print(f" - data_source_note      : {self.data_source_note or 'None'}")
        print(f" - thresholds_scale keys : {list(self.thresholds_scale.keys())}")
        
        print("\nℹ️  Thresholds:")
        print("   - Threshold scales (`scale`) are computed automatically by `generate_map()` if left as `None`.")
        print("   - You can override them here to use fixed thresholds per analysis type.")
        
        print("\nℹ️  Default colors:")
        print("   - Default `fill_color` values for each analysis type are stored in:")
        print("     `transnetmap.utils.dct.defaults_thresholds_scale_color`")

        print("\nStyling keys follow Folium API. See: https://python-visualization.github.io/folium/")


# ===========================
if __name__ == "__main__":

    # Complete dictionary of creation and calculation parameters
    dct_param = {
        "network_number": 4,
        "physical_values_set_number": None,
        "network_extension_type": "IMT",
        "main_print": True,
        "sql_echo": False,
        "db_nptm_schema": "nptm17",
        "db_zones_table": "zones17",
        "db_imt_table": "imt22",
        "db_pt_table": "pt22",
        "uri": "postgresql://username:password@host:port/database",
    }

    # Define required fields for Links
    required_fields = ["network_number", "main_print", "network_extension_type"]
    
    # Create a ParamConfig instance  
    config_net = ParamConfig(**dct_param, required_fields=required_fields)
    
    # # Validate parameters
    config_net.validate()
    
    config_net.describe()
    
    # ===========================
    
    config_hm = HeatMapConfig()
    config_hm.describe()
    
    
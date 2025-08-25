# -*- coding: utf-8 -*-

from typing import Union
from transnetmap.utils.config import ParamConfig
from transnetmap.pre.network_child import Stations, Links

class Network:
    """
    Represents a transportation network consisting of interconnected stations and links.
    This class provides methods to create, manipulate, and visualize network data, as well as
    interact with the database.

    Attributes
    ----------
    type : str
        The type of object, always set to 'network'.
    network_number : int
        The identification number of the network.
    db_nptm_schema : str
        Name of the schema containing the National Passenger Traffic Model (NPTM) data.
        This schema serves as a namespace for the model and acts as an identifier
        used in constructing dependent table names.
    table_name : str
        The name of the database table for the network, constructed as
        'network_<network_number>_<db_nptm_schema>'.
    uri : str
        PostgreSQL database connection string for reading and writing data.
    main_print : bool
        Indicates whether execution information should be printed to the console.
    sql_echo : bool
        Indicates whether SQL query logs should be displayed.
    stations : Stations, optional
        The Stations object associated with this network. This attribute is initialized
        only when required, such as for visualization or specific network operations.
    links : Links, optional
        The Links object associated with this network. This attribute must be provided
        when creating or manipulating the network.

    Notes
    -----
    - The Network class combines data from stations and links to create geometries
      (polylines) representing the network. Additional attributes such as distances
      are calculated for further analysis.
    - Visualization options allow the network to be displayed with varying levels of detail.
    """

    _type = 'network'  # Object type

    def __init__(self, param: Union[dict, ParamConfig], required_fields=None):
        """
        Initializes the Network instance with specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
            - "network_number" : int
                Identification number for the network instance.
            - "db_nptm_schema" : str
                Name of the schema containing the National Passenger Traffic Model (NPTM) data.
            - "uri" : str
                PostgreSQL database connection string.

            Optional keys:
            - "main_print" : bool
                Enables console output for execution status. Default is False.
            - "sql_echo" : bool
                Enables SQL query logging. Default is False.

        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided,
            defaults to ["network_number", "db_nptm_schema", "uri"].

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `Network`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        # Define required fields for Network (default)
        default_required_fields = ["network_number", "db_nptm_schema", "uri"]

        # Use custom required fields if provided, otherwise use the default ones
        required_fields = required_fields or default_required_fields

        # Case 1: param is a dictionary
        if isinstance(param, dict):
            self.config = ParamConfig(**param, required_fields=required_fields)
            self.config.validate()  # Validate all required fields in the dictionary

        # Case 2: param is already a ParamConfig
        elif isinstance(param, ParamConfig):
            self.config = param  # Use the existing ParamConfig
            self.config.validate_for_class(required_fields)  # Validate only the fields needed for this class

        # Invalid type
        else:
            raise TypeError("Parameter 'param' must be a dictionary or a ParamConfig object.")

        # Extract commonly used attributes
        self.network_number = self.config.network_number
        self.db_nptm_schema = self.config.db_nptm_schema
        self.uri = self.config.uri

        # Define table name for Network
        self.table_name = f'{self._type}_{self.network_number}_{self.db_nptm_schema}'

        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo

        # Initialize placeholders for Stations, Links and Table
        self.stations = None
        self.links = None
        self.table = None


    def _log(self, message: str):
        if self.main_print:
            print(message)


    def to_sql(self, if_exists='fail') -> None:
        """
        Writes the network table to the database.
        Ensures that the associated Links and Stations tables are written if not already present.

        Parameters
        ----------
        if_exists : str, optional
            Determines behavior if the table already exists (default is 'fail').
            Options:
                - 'fail' : Raises an error if the table exists.
                - 'replace' : Drops and recreates the table.
                - 'append' : Adds data to the existing table (not allowed here).

        Raises
        ------
        ValueError
            If 'if_exists' is set to 'append', as it is not allowed to avoid data duplication.

        Returns
        -------
        None
        """
        from sqlalchemy import create_engine
        from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, INTEGER, SMALLINT, VARCHAR
        from transnetmap.utils.sql import define_schema, schema_exists, execute_sql_script, execute_primary_key_script, table_exists
        
        # Prohibit "append" to avoid data duplication issues
        if if_exists == 'append':
            raise ValueError(
                "'append' is not allowed in this method to prevent data duplication. "
                "Use 'fail' or 'replace' instead."
            )
        
        if self.table.empty:
            raise ValueError("The table is empty. Ensure data is loaded before writing to the database.")
        
        # Define schema name
        schema = 'network'
        
        # Ensure schema exists in the database
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            define_schema(self.uri, schema)
        
        # Ensure Links and Stations tables are written
        if not table_exists(self.uri, self.links.table_name, print_status=self.main_print):
            self._log(f"Table '{schema}.{self.links.table_name}' does not exist. Writing Links table...")
            self.links.to_sql(if_exists='fail')  # Write Links table

        if not table_exists(self.uri, self.stations.table_name, print_status=self.main_print):
            self._log(f"Table '{schema}.{self.stations.table_name}' does not exist. Writing Stations table...")
            self.stations.to_sql(if_exists='fail')  # Write Stations table
        
        # Use self.table_name
        table_name = self.table_name
        
        # Write to the database (PostGIS for geospatial data)
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table.to_postgis(
                    table_name,
                    connection, 
                    schema=schema,
                    if_exists=if_exists, 
                    index=False,
                    dtype={
                        'id_a':SMALLINT,
                        'id_b':SMALLINT,
                        'code_a':VARCHAR,
                        'code_b':VARCHAR,
                        'level':SMALLINT,
                        'length':INTEGER,
                        'lng_a':DOUBLE_PRECISION,
                        'lat_a':DOUBLE_PRECISION,
                        'lng_b':DOUBLE_PRECISION,
                        'lat_b':DOUBLE_PRECISION
                    }
                )
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to the database: {e}")
            
        # add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["id_a","id_b"],
            schema=schema,
            include_schema_in_pk_name=False,
            print_status=self.main_print
        )
        
        script = f'''
        COMMENT ON TABLE "network"."{table_name}" IS 'In "network" length of sections in [m] (length as the crow flies, no fractal factor)';
        '''
        execute_sql_script(self.uri, script, print_status=self.main_print)
        
        self._log(f"Writing to the database is successful. Table: '{schema}.{table_name}'")


    def read_sql(self) -> "Network":
        """
        Reads the network table from the database and loads it into the instance.
    
        Returns
        -------
        Network
            The current instance with the table loaded into the 'self.table' attribute as a GeoDataFrame.
        
        Raises
        ------
        RuntimeError
            If the stations table does not exist in the database.
        """
        from transnetmap.utils.sql import table_exists
        from sqlalchemy import create_engine
        import geopandas as gpd
    
        # Define schema and table name
        schema = 'network'
        table_name = self.table_name
    
        # Check if the table exists
        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )
    
        # Load table from the database
        sql_query = f'''SELECT * FROM "{schema}"."{table_name}"'''        
        dtype = {
            'id_a': 'int16',
            'id_b': 'int16',
            'level': 'int16',
            'length': 'int32',
            'code_a': 'str',
            'code_b': 'str',
            'lng_a': 'float64',
            'lat_a': 'float64',
            'lng_b': 'float64',
            'lat_b': 'float64'
        }
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table = gpd.read_postgis(sql_query, connection, crs='EPSG:4326').astype(dtype)
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")

        self._log(f"Importing from the database is successful. Table: '{schema}.{table_name}'")
    
        return self


    def create_network(self, stations: Stations, links: Links) -> "Network":
        """
        Creates a network by combining stations and links data to generate line geometries
        (as 'shapely.geometry.LineString') and additional attributes such as length
        (as-the-crow-flies distance) and endpoint identifiers (coordinates and codes).
    
        Parameters
        ----------
        stations : Stations
            An instance of the Stations class containing station data.
        links : Links
            An instance of the Links class containing link data.
    
        Returns
        -------
        self : Network
            The current instance with the 'table' attribute set as a GeoDataFrame.
        """
        import pandas as pd
        import geopandas as gpd
        from shapely.geometry import LineString
        from pyproj import Geod
        from transnetmap.utils.dct import dct_level
    
        # Validate input types
        if not isinstance(stations, Stations):
            raise TypeError("Parameter 'stations' must be an instance of the Stations class.")
        if not isinstance(links, Links):
            raise TypeError("Parameter 'links' must be an instance of the Links class.")
        if not isinstance(stations.table, gpd.GeoDataFrame):
            raise ValueError("The 'stations.table' attribute must be a valid GeoDataFrame.")
        if not isinstance(links.table, pd.DataFrame):
            raise ValueError("The 'links.table' attribute must be a valid DataFrame.")
        
        # Validate network consistency
        if self.network_number != stations.network_number:
            raise ValueError("The network number of the Stations object does not match the Network object.")
        if self.network_number != links.network_number:
            raise ValueError("The network number of the Links object does not match the Network object.")
        
        self.stations = stations
        self.links = links
        
        self._log("\nStart creating the 'network' table.\n")
    
        def create_line(level):
            """
            Creates a DataFrame for a specific link level by joining station data.
    
            Parameters
            ----------
            level : str
                The level of the link (e.g., 'lower', 'main', 'higher').
    
            Returns
            -------
            pd.DataFrame
                A DataFrame containing joined data for the specified link level.
            """
            links_level = links.table[[f"{level}_a", f"{level}_b"]].rename(
                columns={f"{level}_a": "a", f"{level}_b": "b"}
            )
            links_level = links_level.merge(
                stations.table[["code", "lng", "lat"]],
                how="inner", left_on="a", right_on="code"
            ).rename(columns={"lng": "lng_a", "lat": "lat_a"}).drop(columns="code")
            links_level = links_level.merge(
                stations.table[["code", "lng", "lat"]],
                how="inner", left_on="b", right_on="code"
            ).rename(columns={"lng": "lng_b", "lat": "lat_b"}).drop(columns="code")
            links_level["level"] = dct_level[level]
            return links_level
    
        # Create a geodetic calculator based on WGS84
        geod = Geod(ellps="WGS84")
        
        def calculate_length(geometry):
            """Calculate geodetic length of a LineString in meters."""
            return geod.geometry_length(geometry)
    
        # Create lines for all levels
        lower_line = create_line("lower")
        self._log("Lower lines have been created.")
        main_line = create_line("main")
        self._log("Main lines have been created.")
        higher_line = create_line("higher")
        self._log("Higher lines have been created.\nStart creating line geometries.")
    
        # Combine lines and create geometries
        all_lines = pd.concat([lower_line, main_line, higher_line], ignore_index=True)
        all_lines["geom"] = all_lines.apply(
            lambda row: LineString([(row.lng_a, row.lat_a), (row.lng_b, row.lat_b)]), axis=1
        )
        all_lines = gpd.GeoDataFrame(all_lines, geometry="geom", crs="EPSG:4326")
        all_lines = all_lines.rename(columns={"a": "code_a", "b": "code_b"})
        
        # Calculate the length of each geometry in meters
        all_lines["length"] = all_lines["geom"].apply(calculate_length).round().astype("int32")
        
        self._log("Line geometries have been created.\nStart identifying zones according to NPTM.")
    
        # Add zone identifiers
        station_ids = stations.table[["id", "code"]]
        all_lines = all_lines.merge(
            station_ids, left_on="code_a", right_on="code"
        ).rename(columns={"id": "id_a"}).drop(columns="code")
        all_lines = all_lines.merge(
            station_ids, left_on="code_b", right_on="code"
        ).rename(columns={"id": "id_b"}).drop(columns="code")
    
        # Final formatting for database export
        # - Ensures consistent column ordering
        # - Sets explicit data types to match database schema
        dtype = {
            'id_a': 'int16',
            'id_b': 'int16',
            'level': 'int16',
            'length': 'int32',
            'code_a': 'str',
            'code_b': 'str',
            'lng_a': 'float64',
            'lat_a': 'float64',
            'lng_b': 'float64',
            'lat_b': 'float64'
        }
        self.table = all_lines.reindex(columns=[
            "id_a",
            "id_b",
            "geom",
            "level",
            "length",
            "code_a",
            "code_b",
            "lng_a",
            "lat_a",
            "lng_b",
            "lat_b"
        ]).sort_values(by=["level", "code_a", "code_b"]
                       ).reset_index(drop=True).astype(dtype)
                       
        self._log("\nThe 'network' table has been successfully created.")

        return self


    def _add_stations(self) -> "Network":
        """
        Private method to add the Stations attribute to the Network object.
        This method is intended to be used internally by the class to support
        operations like displaying the network (e.g., 'show' methods).
        
        Returns
        -------
        self : Network
            The updated Network instance with the 'stations' attribute added if it not defined.
        """
        # Check if stations already exist
        if hasattr(self, "stations") and self.stations is not None:
            self._log("The 'stations' attribute already exists. No action taken.")
            return self  # Simply return the current object if stations already exist
        
        # Log that stations are being added
        self._log("Adding 'stations' attribute to the Network object.")
        
        # Load the stations table
        self.stations = Stations(self.config).read_sql()
        
        return self


    def show(
        self,
        return_layers=False,
        file_name="network_map",
        save_to_desktop=False,
        custom_path=None,
        location=None,
        zoom_start=None
    ):
        """
        Displays a map of the network using Folium, showing different link levels with associated circles around stations.
        Optionally, returns the Folium FeatureGroups instead of displaying the map.
    
        Parameters
        ----------
        return_layers : bool, optional
            If True, returns a dictionary of Folium FeatureGroups for each link level and the station circles. Default is False.
        file_name : str, optional
            Base name of the file to save the map as (without extension). Default is "network_map".
        save_to_desktop : bool, optional
            If True, saves the map to the user's desktop. Default is False.
        custom_path : str, optional
            If provided, saves the map to the specified directory. Overrides save_to_desktop.
        location : tuple(float, float), optional
            Custom (latitude, longitude) coordinates for centering the map.
            If provided, `zoom_start` must also be specified.
        zoom_start : int, optional
            Custom zoom level. If None, the map will automatically fit the bounds.
    
        Returns
        -------
        dict or None
            If `return_layers` is True, returns a dictionary with Folium FeatureGroups for each link level and the station circles.
            Otherwise, the map is displayed and saved.
        
        Raises
        ------
        AttributeError
            If the network table is not loaded or does not contain a valid geometry column.
        ValueError
            If the dataset is empty or has no valid geometry column for mapping.
        """
        from folium import FeatureGroup, PolyLine, Circle, LayerControl
        from transnetmap.utils.map_utils import show_map, auto_fit_map
        from transnetmap.utils.dct import stations_areas_radius
    
        # 🔹 Step 1: Validate input data
        if self.table is None:
            raise AttributeError("❌ The network table is not loaded. Use 'create_network()' or 'read_sql()' first.")
    
        if self.table.empty:
            raise ValueError("⚠️ The network dataset is empty. Ensure the data is correctly loaded.")
    
        if "level" not in self.table.columns or "geom" not in self.table.columns:
            raise ValueError("⚠️ The dataset must contain 'level' and 'geom' columns for visualization.")
    
        # 🔹 Step 2: Create FeatureGroups for link levels and station areas
        feature_groups = {}
        fg_stations = FeatureGroup(name="Stations areas", show=False, control=True)  # Hidden by default
    
        def create_level_fg(level, data, fg_circles):
            """
            Creates a specific link level layer and its station circles.
            Colours and styles are fixed by levels.
            """
            fg_links = FeatureGroup(name=f"NTS level {level}", show=True, control=True)
            visited_locations = set()  # Track visited locations to avoid duplicate circles
            radius = stations_areas_radius[level]
    
            for _, row in data.iterrows():
                # Polyline for each link
                line_coords = [(lat, lon) for lon, lat in row["geom"].coords]
                PolyLine(
                    locations=line_coords,
                    color={1: "green", 2: "blue", 3: "red"}[level],
                    weight=2.5,
                    opacity=0.8,
                    popup=f"<b>Length:</b> {row['length'] / 1000:.1f} km"
                ).add_to(fg_links)
    
                # Create start and end circles
                for loc in [line_coords[0], line_coords[-1]]:
                    if loc not in visited_locations:
                        visited_locations.add(loc)
                        Circle(
                            location=loc,
                            radius=radius,
                            color={1: "green", 2: "blue", 3: "red"}[level],
                            fill=False,
                            weight=2,
                            opacity=0.5,
                            popup=f"<b>Radius:</b> {radius / 1000:.1f} km"
                        ).add_to(fg_circles)
    
            return fg_circles, fg_links
    
        # 🔹 Step 3: Process each link level
        for level in [1, 2, 3]:
            data = self.table[self.table["level"] == level]
            if not data.empty:
                _, feature_groups[f"level_{level}"] = create_level_fg(level, data, fg_stations)
    
        feature_groups["stations_areas"] = fg_stations
    
        # 🔹 Step 4: Return layers if requested
        if return_layers:
            return feature_groups
    
        # 🔹 Step 5: Create the map
        m = auto_fit_map(self.table, location=location, zoom_start=zoom_start)
    
        # 🔹 Step 6: Add all layers to the map
        for layer in feature_groups.values():
            layer.add_to(m)
    
        # 🔹 Step 7: Add layer control
        LayerControl().add_to(m)
    
        # 🔹 Step 8: Save and display the map
        show_map(m, file_name=file_name, save_to_desktop=save_to_desktop, custom_path=custom_path)
    
        return None  # ✅ The map is displayed, no need to return anything


    def show_side_by_side(
        self,
        file_name="side_by_side_map",
        save_to_desktop=False,
        custom_path=None,
        location=None,
        zoom_start=None
    ):
        """
        Displays the network and station maps side by side using Folium and Branca.
        
        Parameters
        ----------
        file_name : str, optional
            Base name of the file to save the map as (without extension). Default is "side_by_side_map".
        save_to_desktop : bool, optional
            If True, saves the map to the user's desktop. Default is False.
        custom_path : str, optional
            If provided, saves the map to the specified directory. Overrides save_to_desktop.
        location : tuple(float, float), optional
            Custom (latitude, longitude) coordinates for centering the map.
            If provided, `zoom_start` must also be specified.
        zoom_start : int, optional
            Custom zoom level. If None, the map will automatically fit the bounds.

        Raises
        ------
        AttributeError
            If the network or stations table is not loaded.
        
        Returns
        -------
        None
            The method saves the map to an HTML file and opens it in a browser.
        """
        import branca
        from folium import LayerControl
        from transnetmap.utils.map_utils import show_map, auto_fit_map
        from transnetmap.utils.sql import table_exists

        # 🔹 Step 1: Ensure Network table exists in DB or is loaded
        if not table_exists(self.uri, self.table_name, print_status=self.main_print):
            self._log("⚠️ Network table does not exist in DB. Checking local attribute...")
        else:
            self.read_sql()

        if self.table is None:
            raise AttributeError("❌ The network table is missing. Use `create_network()` or `read_sql()` first.")

        # 🔹 Step 2: Ensure Stations data is available
        self._add_stations()

        if self.stations.table is None:
            raise AttributeError("❌ The stations table is missing. Use `read_sql()` or `read_csv()` first.")

        # 🔹 Step 3: Create base maps (auto-fit bounds if no manual `location`)
        network_map = auto_fit_map(self.table, location=location, zoom_start=zoom_start)
        station_map = auto_fit_map(self.table, location=location, zoom_start=zoom_start)

        # 🔹 Step 4: Add stations layer to station map
        self.stations.show(return_layers=True).add_to(station_map)

        # 🔹 Step 5: Add all network layers to network map
        for layer in self.show(return_layers=True).values():
            layer.add_to(network_map)
            
        # 🔹 Step 6: Add layer control to network map
        LayerControl().add_to(network_map)

        # 🔹 Step 7: Create a Branca figure for side-by-side maps
        fig = branca.element.Figure()
        subplot1 = fig.add_subplot(1, 2, 1)  # Left: Stations
        subplot2 = fig.add_subplot(1, 2, 2)  # Right: Network
        subplot1.add_child(station_map)
        subplot2.add_child(network_map)

        # # 🔹 Step 7: Add layer control to network map
        # LayerControl().add_to(network_map)

        # 🔹 Step 8: Save and display the combined figure
        show_map(fig, file_name=file_name, save_to_desktop=save_to_desktop, custom_path=custom_path)

        return None  # ✅ No return needed, map is displayed


    def show_all(
        self,
        file_name="network_all_map",
        save_to_desktop=False,
        custom_path=None,
        location=None,
        zoom_start=None
    ):
        """
        Displays a comprehensive map of the network, including zones from the NPTM,
        stations, and network links.
    
        Parameters
        ----------
        file_name : str, optional
            Base name of the file to save the map as (without extension). Default is "network_all_map".
        save_to_desktop : bool, optional
            If True, saves the map to the user's desktop. Default is False.
        custom_path : str, optional
            If provided, saves the map to the specified directory. Overrides save_to_desktop.
        location : tuple(float, float), optional
            Custom (latitude, longitude) coordinates for centering the map.
            If provided, `zoom_start` must also be specified.
        zoom_start : int, optional
            Custom zoom level. If None, the map will automatically fit the bounds.

        Raises
        ------
        AttributeError
            If the network or stations table is not loaded.
        ValueError
            If the NPTM zones data is missing or invalid.

        Returns
        -------
        None
            The method saves the map to an HTML file and opens it in a browser.
        """
        from folium import GeoJson, GeoJsonPopup, LayerControl
        from transnetmap.utils.map_utils import show_map, auto_fit_map
        from transnetmap.pre.nptm import NPTM
        from transnetmap.utils.sql import table_exists

        # 🔹 Step 1: Ensure Network table exists in DB or is loaded
        if not table_exists(self.uri, self.table_name, print_status=self.main_print):
            self._log("⚠️ Network table does not exist in DB. Checking local attribute...")
        else:
            self.read_sql()

        if self.table is None:
            raise AttributeError("❌ The network table is missing. Use `create_network()` or `read_sql()` first.")

        # 🔹 Step 2: Ensure Stations data is available
        self._add_stations()

        if self.stations.table is None:
            raise AttributeError("❌ The stations table is missing. Use `read_sql()` or `read_csv()` first.")

        # 🔹 Step 3: Load zones data from DB
        required_fields = ["db_zones_table"]
        zones = NPTM(self.config, required_fields).read_sql(self.config.db_zones_table, columns=["id", "geom"])

        if zones is None or zones.empty:
            raise ValueError("⚠️ The NPTM zones data is missing or invalid. Cannot generate the map.")

        # 🔹 Step 4: Define styling functions for zones
        def style_function(feature):
            return {
                'fillColor': feature["properties"].get("color", "#cdcdcd"),  # Default or custom fill color
                'color': '#a8a8a8',  # Border color
                'weight': 1,         # Border thickness
                'fillOpacity': 0.0   # Fill opacity
            }
        
        def highlight_function(feature):
            return {
                'fillColor': '#d9d919',  # Highlight fill color
                'color': '#e47833',      # Highlight border color
                'weight': 3,             # Highlight border thickness
                'fillOpacity': 0.25      # Highlight fill opacity
            }
        
        # 🔹 Step 5: Retrieve network and stations layers
        network_layers = self.show(return_layers=True)
        stations_layer = self.stations.show(return_layers=True)
        
        # 🔹 Step 6: Create the GeoJson layer for zones
        zones_layer = GeoJson(
            zones,
            name="Zones",
            style_function=style_function,
            highlight_function=highlight_function,
            popup=GeoJsonPopup(fields=['id'], aliases=['Zone id:'], labels=True),
            popup_keep_highlighted=True
        )
        
        # 🔹 Step 7: Create the base map (auto-fit bounds if no manual location)
        m = auto_fit_map(zones, location=location, zoom_start=zoom_start)
        
        # 🔹 Step 8: Add layers to the map
        stations_layer.add_to(m)  # Add stations
        
        for layer in network_layers.values():  # Add network layers
            layer.add_to(m)
        
        zones_layer.add_to(m)  # Add zones
        
        # 🔹 Step 9: Add layer control
        LayerControl().add_to(m)

        # 🔹 Step 10: Save and display the map
        show_map(m, file_name=file_name, save_to_desktop=save_to_desktop, custom_path=custom_path)

        return None  # ✅ The map is displayed, no need to return anything


# ===========================
if __name__ == "__main__":
    
    # Complete dictionary of creation and calculation parameters
    dct_param = {
        "network_number": 4,
        "physical_values_set_number": 1,
        "main_print": True,
        "sql_echo": False,
        "db_nptm_schema": "nptm17",
        "db_zones_table": "zones17",
        "db_imt_table": "imt22",
        "db_pt_table": "pt22",
        "uri": "postgresql://username:password@host:port/database",
    }  

    
    stations = Stations(dct_param).read_sql()
    links = Links(dct_param).read_sql()
    
    network = Network(dct_param).create_network(stations, links)
    network.show()
    network.to_sql(if_exists='fail')
    
    network_2 = Network(dct_param).read_sql()
    network_2.show()
    network_2.show_side_by_side()
    network_2.show_all()
    network_2_fg = network_2.show(return_layers=True)
    
    
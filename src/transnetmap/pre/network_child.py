# -*- coding: utf-8 -*-
"""
Child network entities for transnetmap: **Stations** and **Links**.

This module contains two small classes that help prepare, validate, and persist
stations and links related to a network. They focus on:
- configuration validation via the dataclass `transnetmap.utils.config.ParamConfig`,
- CSV → dataframe/GeoDataFrame ingestion with semantic checks,
- simple SQL persistence and reading using PostgreSQL/PostGIS.
"""

from __future__ import annotations

from typing import Optional, Union, TYPE_CHECKING

from transnetmap.utils.config import ParamConfig

if TYPE_CHECKING:  # noqa: F401
    import folium
    from pathlib import Path

__all__ = ["Stations", "Links"]


# -----------------------------------------------------------------------------
# Class: Stations
# -----------------------------------------------------------------------------
class Stations:
    """
    Represents the Stations object in the network, handling station-specific
    attributes and operations such as database interactions and CSV imports.

    This class is part of a larger network modeling framework where stations
    serve as nodes connected by links. Each station is uniquely identified
    within a specific network and National Passenger Traffic Model (NPTM) schema.
    
    Constants
    ---------
    _type : str
    
        The type of object, always set to 'stations'.

    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters
    network_number : int
        The unique identification number of the network this station set belongs to.
    table : geopandas.GeoDataFrame
        Table with data relating to the Stations class (None at initialisation).
    db_nptm_schema : str
        Name of the schema containing the National Passenger Traffic Model (NPTM) data.
        This schema acts as both a namespace and an identifier for dependent tables.
    table_name : str
        The name of the database table for stations, constructed as
        'stations_<network_number>_<db_nptm_schema>'.
    uri : str
        PostgreSQL database connection string for reading and writing data.
    main_print : bool
        Indicates whether execution information should be printed to the console.
    sql_echo : bool
        Indicates whether SQL query logs should be displayed.

    Notes
    -----
    - This class is responsible for managing station data, including validation,
      importing from CSV files, and interaction with the database.
    - The `Stations` class is designed to integrate seamlessly with other components
      of the network framework, such as `Links` and `Network`.
    - Parameters passed during initialization are validated for completeness and
      type conformity, ensuring consistency across all instances.
    """

    _type = 'stations'  # Object type

    def __init__(self, param: Union[dict, ParamConfig], *, required_fields: Optional[list] = None) -> None:
        """
        Initializes the Stations instance with specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
                
            - `"network_number"` : int  
                Unique identification number for the network instance.
            - `"db_nptm_schema"` : str  
                Name of the schema containing the National Passenger Traffic Model data.
            - `"db_zones_table"` : str  
                Name of the database table containing zone data.
            - `"uri"` : str  
                PostgreSQL database connection string.

            Optional keys:
                
            - `"main_print"` : bool  
                Enables console output for execution status. Default is False.
            - `"sql_echo"` : bool  
                Enables SQL query logging. Default is False.

        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided,
            defaults to `["network_number", "db_nptm_schema", "db_zones_table", "uri"]`.

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `Stations`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        # Define required fields for Stations (default)
        default_required_fields = ["network_number", "db_nptm_schema", "db_zones_table", "uri"]
    
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
        
        # Define table name for Stations
        self.table_name = f'{self._type}_{self.network_number}_{self.db_nptm_schema}'

        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo
        
        # Initialize placeholders for table
        self.table = None


    def create_network(self):
        """
        Raises an exception to indicate that this method is not applicable for the Stations class.

        Raises
        ------
        AttributeError
            Always raised because the method is not available for Stations.
        """
        raise AttributeError("This method is not available in Stations class.")


    def to_sql(self, *, if_exists='fail') -> None:
        """
        Writes the stations table to the database.

        Parameters
        ----------
        if_exists : str, optional
            Determines behavior if the table already exists (default is `'fail'`).  
            Options:  
                - `'fail'` : Raises an error if the table exists.  
                - `'replace'` : Drops and recreates the table.  
                - `'append'` : Adds data to the existing table (not allowed here).  

        Raises
        ------
        ValueError
            If `'if_exists'` is set to `'append'`, as it is not allowed to avoid data duplication.

        Returns
        -------
        None
        """
        from sqlalchemy import create_engine
        from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, SMALLINT, VARCHAR
        from transnetmap.utils.sql import define_schema, schema_exists, execute_primary_key_script
        
        # Prohibit "append" to avoid data duplication issues
        if if_exists == 'append':
            raise ValueError(
                "'append' is not allowed in this method to prevent data duplication. "
                "Use 'fail' or 'replace' instead."
            )
        
        if self.table.empty:
            raise ValueError("The table is empty. Ensure data is loaded before writing to the database.")
        
        # Define schema name
        schema = "network"
        
        # Ensure schema exists in the database
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            define_schema(self.uri, schema)
        
        # Use self.table_name
        table_name = self.table_name
        
        # Write table to database (PostGIS for geospatial data)
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table.to_postgis(
                    table_name,
                    connection, 
                    schema=schema,
                    if_exists=if_exists, 
                    index=False,
                    dtype={
                        'code': VARCHAR,
                        'name': VARCHAR,
                        'lng': DOUBLE_PRECISION,
                        'lat': DOUBLE_PRECISION,
                        'id': SMALLINT
                    }
                )
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to the database: {e}")
        
        # add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["id"],
            schema=schema,
            include_schema_in_pk_name=False,
            print_status=self.main_print
        )
        
        if self.main_print:
            print(f"Writing to the database is successful. Table: '{schema}.{table_name}'")


    def read_sql(self) -> Stations:
        """
        Reads the stations table from the database and loads it into the instance.
    
        Returns
        -------
        Stations
            The current instance with the table loaded into the 'self.table' attribute as a GeoDataFrame.
        
        Raises
        ------
        RuntimeError
            If the links table does not exist in the database.
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
            'code':'str', 
             'name':'str', 
             'id': 'int16', 
             'lat': 'float64', 
             'lng': 'float64'
        }
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table = gpd.read_postgis(sql_query, connection, crs='EPSG:4326').astype(dtype)
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")

        if self.main_print:
            print(f"Importing from the database is successful. Table: '{schema}.{table_name}'")
    
        return self


    def read_csv(self, file: Union[str, Path]) -> Stations:
        """
        Reads a CSV file containing station data and validates its content.
    
        The table created includes station geometries as 'shapely.geometry.Point'
        objects (EPSG:4326) and associates stations with their respective zones
        based on NPTM data. The method performs several validations, including
        checks for missing columns, duplicate station codes or names, and
        overlapping geometries within a 500-meter buffer.
        
        Expected format
        ---------------
        1) Columns: 
        
            ['CODE', 'NAME', 'LAT', 'LNG']  
        
           - Column separator: ';'  
           - Decimal separator: '.'  
           - Latitude and longitude coordinates: WGS 84 (EPSG:4326)  
    
        Parameters
        ----------
        file : str or pathlib.Path
            Path to the CSV file.
    
        Returns
        -------
        self.table : geopandas.GeoDataFrame
            A GeoDataFrame containing the validated station data, including geometries
            in EPSG:4326 and zone associations.
    
        Raises
        ------
        ValueError
            If the file name does not match the expected format ('stations_[number].csv').
        KeyError
            If required configuration parameters for NPTM zones are missing.
        RuntimeError
            If required columns are missing from the CSV.  
            If duplicate station codes, names, or coordinates are detected.  
            If geometries overlap within a 500-meter buffer.  
        """
        import pandas as pd
        import geopandas as gpd
        from numpy import cos, radians
        from shapely.geometry import Point
        from transnetmap.pre.nptm import NPTM
        from transnetmap.utils.sql import table_exists
        from transnetmap.utils.utils import validate_input_file_name
        
        # Validate file name format
        file_str_valid = f'{self._type}_{self.network_number}.csv'
        validate_input_file_name(file, file_str_valid)
        
        if self.main_print:
            print('\nStarting creation of the "stations" table.')
    
        # Validate the existence of NPTM zones table in the database
        schema = self.db_nptm_schema
        table_name = self.config.db_zones_table
        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined using NPTM.setup_data and written using NPTM.to_sql.\n'
                f'(Schema: "{schema}")'
            )
        # Load CSV, import and validate station data: Coordinates in WGS 84 (EPSG:4326)
        try:
            data = pd.read_csv(file, sep=';', decimal='.', header=0, dtype={
                'CODE': 'str',
                'NAME': 'str',
                'LAT': 'float64',
                'LNG': 'float64'
                }
            )
        except Exception as e:
           raise ValueError(f"Error reading CSV file: {str(e)}")
    
        # Validate required columns
        required_columns = ['CODE', 'NAME', 'LAT', 'LNG']
        if not all(column in data.columns for column in required_columns):
            missing_columns = [col for col in required_columns if col not in data.columns]
            raise RuntimeError(f"Missing required columns in the CSV file: {', '.join(missing_columns)}")
    
        # Check for duplicates in station codes and names
        if data['CODE'].duplicated().any():
            raise RuntimeError("Duplicate station codes found.")
        if data['NAME'].duplicated().any():
            raise RuntimeError("Duplicate station names found.")
        
        # Checking the data types of geographical columns
        if not pd.api.types.is_numeric_dtype(data['LAT']) or not pd.api.types.is_numeric_dtype(data['LNG']):
            raise RuntimeError("Columns 'LAT' and 'LNG' must contain numeric values.")
    
        # Create 'Point' geometries
        data['geom'] = data.apply(lambda row: Point(row['LNG'], row['LAT']), axis=1)
    
        # Data formatting
        gdf_stations = (
            gpd.GeoDataFrame(data, geometry='geom', crs="EPSG:4326")
            .rename(columns={'CODE': 'code', 'NAME': 'name', 'LNG': 'lng', 'LAT': 'lat'})
            .reindex(columns=['code', 'geom', 'name', 'lng', 'lat'])
            .sort_values(by='code')
            .reset_index(drop=True)
        )
    
        # Radius in meters to buffer
        buffer_meters = 500
        
        # Compute buffer in degrees based on latitude
        gdf_stations['buffer_deg'] = buffer_meters / (111320 * cos(radians(gdf_stations['lat'])))
        
        # Add a temporary buffer for spatial queries to check duplicate geometries
        gdf_stations['geom_buffer'] = gdf_stations.apply(
            lambda row: row['geom'].buffer(row['buffer_deg']),
            axis=1
        )
        
        # Query spatial index for duplicates using the new `query()` method
        possible_duplicates = gdf_stations.sindex.query(
            gdf_stations['geom_buffer'], predicate='intersects'
        )
        
        # Check if the number of intersections exceeds the number of stations
        if len(possible_duplicates) > len(gdf_stations):
            raise RuntimeError("Duplicate station geometries found within 500 meters.")
    
        # Remove the temporary buffer columns
        gdf_stations = gdf_stations.drop(columns=['geom_buffer', 'buffer_deg'])
    
        # Load NPTM zones data
        nptm_zones = NPTM(self.config).read_sql(table_name, columns=['geom', 'id'])
    
        # Spatial join to identify zones
        gdf_inside = gdf_stations.sjoin(nptm_zones, how='left', predicate='intersects').drop(columns=['index_right'])
    
        # Count stations without a match
        num_no_match = gdf_inside['id'].isna().sum()
        
        # Print only if main_print is True
        if self.main_print and num_no_match > 0:
            print(f"{num_no_match} stations do not intersect any zone in NPTM zones data.")
        
        # Assign IDs to stations without a match
        if num_no_match > 0:
            next_id_start = 10001  # Starting ID for unmatched stations
            gdf_inside['id'] = gdf_inside['id'].fillna(
                pd.Series(
                    range(next_id_start, next_id_start + num_no_match), 
                    index=gdf_inside.index[gdf_inside['id'].isna()]
                )
            ).astype('int16')
        
        # Finalize the table
        self.table = (
            gdf_inside.reindex(columns=['code', 'geom', 'name', 'id', 'lng', 'lat'])
            .sort_values(by='code')
            .reset_index(drop=True)
        )
    
        if self.main_print:
            print('\nThe "stations" table has been successfully created.')
    
        return self           


    def show(
        self,
        *,
        return_layers: bool = False,
        file_name: str = "stations_map",
        save_to_desktop: bool = False,
        custom_path: Optional[str] = None,
        location: Optional[tuple[float, float]] = None,
        zoom_start: Optional[int] = None
    ) -> Optional[folium.GeoJson | folium.Map]:
        """
        Displays a map of the stations using Folium or returns the GeoJson object.
    
        This method visualizes the loaded station locations, automatically fitting the map
        to the dataset's bounds unless `location` and `zoom_start` are provided.
        Optionally, returns the GeoJson layer instead of displaying the map.
    
        Parameters
        ----------
        return_layers : bool, optional
            If True, returns the Folium GeoJson object for further use. Default is False.
        file_name : str, optional
            Base name of the file to save the map as (without extension). Default is "stations_map".
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
        folium.GeoJson or None
            If `return_layers` is True, returns the Folium GeoJson object.  
            Otherwise, the map is displayed and saved.  
            *The `folium.Map` object is returned only for internal use.*
    
        Raises
        ------
        AttributeError
            If the stations table is not loaded or does not contain a valid geometry column.
        ValueError
            If the dataset is empty or has no valid geometry column for mapping.
        """
        # Step 1: Validate input data
        from folium import GeoJson, GeoJsonTooltip, GeoJsonPopup
        from transnetmap.utils.map import show_map, auto_fit_map
    
        if self.table is None:
            raise AttributeError("The stations table is not loaded. Use 'read_sql()' or 'read_csv()' first.")
            
        if self.table.empty:
            raise ValueError("The stations dataset is empty. Ensure the data is correctly loaded.")
    
        if "geom" not in self.table.columns:
            raise ValueError("No valid geometry column found. The dataset is not valid for mapping.")
    
        # Step 2: Configure tooltips and popups
        tooltip = GeoJsonTooltip(fields=['name'], aliases=['Name:'], labels=True)
        popup = GeoJsonPopup(
            fields=['name', 'lng', 'lat', 'id'],
            aliases=['Name:', 'Longitude (x):', 'Latitude (y):', 'Zone id:'],
            labels=True
        )
    
        # Step 3: Create the GeoJson layer
        stations_layer = GeoJson(
            self.table,
            name="Stations",
            show=True,
            control=True,
            tooltip=tooltip,
            popup=popup
        )
    
        # Step 4: Return the layer if requested
        if return_layers:
            return stations_layer
    
        # Step 5: Create the map, auto-fit bounds or use custom location/zoom
        m = auto_fit_map(self.table, location=location, zoom_start=zoom_start)
    
        # Step 6: Add the stations layer to the map
        stations_layer.add_to(m)
    
        # Step 7: Save and display the map
        show_map(m, file_name=file_name, save_to_desktop=save_to_desktop, custom_path=custom_path)
        
        return m  # The map is displayed for internal use only.



# -----------------------------------------------------------------------------
# Class: Links
# -----------------------------------------------------------------------------
class Links:
    """
    Represents the Links object in the network, handling link-specific attributes 
    and operations such as database interactions and CSV imports.

    This class models the relationships between stations, defining the connections
    at various levels (e.g., lower, main, higher) within a network.
    
    Constants
    ---------
    _type : str
    
        The type of object, always set to 'links'.
        
    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters.
    network_number : int
        The unique identification number of the network this link set belongs to.
    table : pandas.DataFrame
        Table with data relating to the Links class (None at initialisation).
    db_nptm_schema : str
        Name of the schema containing the National Passenger Traffic Model (NPTM) data.
        This schema acts as both a namespace and an identifier for dependent tables.
    table_name : str
        The name of the database table for links, constructed as 'links_<network_number>_<db_nptm_schema>'.
    uri : str
        PostgreSQL database connection string for reading and writing data.
    main_print : bool
        Indicates whether execution information should be printed to the console.
    sql_echo : bool
        Indicates whether SQL query logs should be displayed.

    Notes
    -----
    - The `Links` class is responsible for managing link data, including validation,
      importing from CSV files, and interaction with the database.
    - Links define the relationships between stations within a network and are critical 
      for constructing the network geometry and performing connectivity analyses.
    - Parameters passed during initialization are validated for completeness and type conformity.
    """
    
    _type = 'links'  # Object type

    def __init__(self, param: Union[dict, ParamConfig], *, required_fields: Optional[list] = None) -> None:
        """
        Initializes the Links instance with specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
                
            - `"network_number"` : int  
                Unique identification number for the network instance.
            - `"db_nptm_schema"` : str  
                Name of the schema containing the National Passenger Traffic Model data.
            - `"uri"` : str  
                PostgreSQL database connection string.

            Optional keys:
                
            - `"main_print"` : bool  
                Enables console output for execution status. Default is False.
            - `"sql_echo"` : bool  
                Enables SQL query logging. Default is False.

        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided,
            defaults to `["network_number", "uri"]`.

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `Links`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        # Define required fields for Links (default)
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

        # Define table name for Links
        self.table_name = f'{self._type}_{self.network_number}_{self.db_nptm_schema}'

        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo
        
        # Initialize placeholders for table
        self.table = None


    def create_network(self):
        """
        Raises an exception to indicate that this method is not applicable for the Links class.

        Raises
        ------
        AttributeError
            Always raised because the method is not available for Links.
        """
        raise AttributeError("This method is not available in Links class.")


    def to_sql(self, *, if_exists='fail') -> None:
        """
        Writes the links table to the database.

        Parameters
        ----------
        if_exists : str, optional
            Determines behavior if the table already exists (default is `'fail'`).  
            Options:  
                - `'fail'` : Raises an error if the table exists.  
                - `'replace'` : Drops and recreates the table.  
                - `'append'` : Adds data to the existing table (not allowed here).  

        Raises
        ------
        ValueError
            If `'if_exists'` is set to `'append'`, as it is not allowed to avoid data duplication.

        Returns
        -------
        None
        """
        from sqlalchemy import create_engine
        from sqlalchemy.dialects.postgresql import VARCHAR
        from transnetmap.utils.sql import define_schema, schema_exists
        
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
        
        # Use self.table_name
        table_name = self.table_name
        
        # Write table to database
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table.to_sql(
                    table_name,
                    connection, 
                    schema=schema,
                    if_exists=if_exists, 
                    index=False,
                    dtype={
                        'lower_a': VARCHAR,
                        'lower_b': VARCHAR,
                        'main_a': VARCHAR,
                        'main_b': VARCHAR,
                        'higher_a': VARCHAR,
                        'higher_b': VARCHAR
                    }
                )
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to the database: {e}")
        
        # add primary key to table
        ### The format of the links table does not allow it to contain a primary key ###
        
        if self.main_print:
            print(f"Writing to the database is successful. Table: '{schema}.{table_name}'")


    def read_sql(self) -> Links:
        """
        Reads the links table from the database and loads it into the instance.
    
        Returns
        -------
        Links
            The current instance with the table loaded into the 'self.table' attribute as a DataFrame.
        
        Raises
        ------
        RuntimeError
            If the links table does not exist in the database.
        """
        from transnetmap.utils.sql import table_exists
        from sqlalchemy import create_engine
        import pandas as pd
    
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
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table = pd.read_sql_query(sql_query, connection)
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")
    
        if self.main_print:
            print(f"Importing from the database is successful. Table: '{schema}.{table_name}'")
    
        return self  


    def read_csv(self, file: Union[str, Path]) -> Links:
        """
        Reads a CSV file containing link data and validates its content.
    
        The table created lists the associations of station codes for three different
        link levels: lower (1), main (2), and higher (3). The method performs
        several validations, including checks for missing columns, incomplete
        links, and duplicate links (both direct and reverse).
        
        Expected format
        ---------------
        1) Columns: 
        
            ['LOWER_A', 'LOWER_B', 'MAIN_A', 'MAIN_B', 'HIGHER_A', 'HIGHER_B']  
        
           - Column separator: ';'  
           - Decimal separator: '.'  
           - Latitude and longitude coordinates: WGS 84 (EPSG:4326)  
           - Column pairs may be empty (level not present in the network). They are ignored.
        
        Parameters
        ----------
        file : str or pathlib.Path
            Path to the CSV file.
    
        Returns
        -------
        self.table : pandas.DataFrame
            A DataFrame containing the validated and sorted link data.
            Columns: ['lower_a', 'lower_b', 'main_a', 'main_b', 'higher_a', 'higher_b']
    
        Raises
        ------
        ValueError
            If the file name does not match the expected format ('links_[number].csv').
        RuntimeError
            If required columns are missing from the CSV.  
            If there are incomplete links (e.g., one station code is missing in a pair).  
            If duplicate links are detected (including reverse duplicates).  
        """
        import pandas as pd
        from transnetmap.utils.utils import validate_input_file_name
        
        # Validate file name format
        file_str_valid = f'{self._type}_{self.network_number}.csv'
        validate_input_file_name(file, file_str_valid)
        
        if self.main_print:
            print('\nStarting creation of the "links" table.')
            
        # Load CSV, import Link Data
        try:
            data = pd.read_csv(file, sep=';', header=0, dtype={
                'LOWER_A': 'str', 'LOWER_B': 'str',
                'MAIN_A': 'str', 'MAIN_B': 'str',
                'HIGHER_A': 'str', 'HIGHER_B': 'str'
                }
            )
        except Exception as e:
           raise ValueError(f"Error reading CSV file: {str(e)}")
    
        # Validate required columns
        required_columns = ['LOWER_A', 'LOWER_B', 'MAIN_A', 'MAIN_B', 'HIGHER_A', 'HIGHER_B']
        if not all(col in data.columns for col in required_columns):
            raise RuntimeError(f"Missing required columns. Expected: {required_columns}")
    
        # Sorting and cleaning for each link level
        def process_links(df, col_a, col_b, level_name):
            """Sort, clean, and validate links for a specific level."""
            # Sort links alphabetically
            sorted_links = df[[col_a, col_b]].sort_values(by=[col_a, col_b]).reset_index(drop=True)
    
            # Check for incomplete links
            if any(sorted_links[col_a].isna() ^ sorted_links[col_b].isna()):
                raise RuntimeError(
                    f"{level_name.capitalize()} links are incomplete.\n"
                    f"Ensure these are valid pairs of station codes."
                )
    
            # Drop rows with missing values
            sorted_links.dropna(inplace=True)
    
            # Check for duplicates
            if sorted_links.duplicated().any():
                raise RuntimeError(f"{level_name.capitalize()} links are duplicated.")
            if sorted_links.apply(lambda row: sorted([row[col_a], row[col_b]]), axis=1).duplicated().any():
                raise RuntimeError(
                    f"{level_name.capitalize()} links are duplicated.\n"
                    f"Be careful not to include returns."
                )
    
            return sorted_links
    
        # Process links for each level
        data_lower = process_links(data, 'LOWER_A', 'LOWER_B', 'lower')
        data_main = process_links(data, 'MAIN_A', 'MAIN_B', 'main')
        data_higher = process_links(data, 'HIGHER_A', 'HIGHER_B', 'higher')
    
        # Combine all levels into a single table
        table_links = pd.concat(
            [data_lower, data_main, data_higher], axis=1
        ).rename(columns={
            'LOWER_A': 'lower_a', 'LOWER_B': 'lower_b',
            'MAIN_A': 'main_a', 'MAIN_B': 'main_b',
            'HIGHER_A': 'higher_a', 'HIGHER_B': 'higher_b'
        })
    
        # Reorder columns for consistency
        self.table = table_links.reindex(columns=['lower_a', 'lower_b', 'main_a', 'main_b', 'higher_a', 'higher_b'])
    
        if self.main_print:
            print('\nThe "links" table has been successfully created.')
    
        return self


# -----------------------------------------------------------------------------
# Main (example-only)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    
    # Complete dictionary of creation and calculation parameters
    dct_param = {
        "network_number": 4,
        "physical_values_set_number": None,
        "main_print": False,
        "sql_echo": False,
        "db_nptm_schema": "nptm17",
        "db_zones_table": "zones17",
        "db_imt_table": "imt22",
        "db_pt_table": "pt22",
        "uri": "postgresql://username:password@host:port/database",
    }  

    
    file_links = rf"C:\...\test\datasets\links_{dct_param["network_number"]}.csv"
    file_stations = rf"C:\...\test\datasets\stations_{dct_param["network_number"]}.csv"
    
    links = Links(dct_param).read_csv(file_links)
    stations = Stations(dct_param).read_csv(file_stations)
    
    stations.show()

    links.to_sql(if_exists='fail')
    stations.to_sql(if_exists='fail')
    
    links_2 = Links(dct_param).read_sql()
    stations_2 = Stations(dct_param).read_sql()
    
    stations_2.show()
    stations_2_fg = stations_2.show(return_layers=True)
    
    
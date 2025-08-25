# -*- coding: utf-8 -*-

from typing import Union
from transnetmap.utils.config import ParamConfig

class NPTM:
    """
    Represents the National Passenger Traffic Model (NPTM) for Switzerland. This class is
    designed to format and analyze travel time and distance data, but can also be adapted
    to work with other transport models that have similar datasets.
    
    Constants
    ----------
    _type : str
        The type of object, always set to 'national_passenger_traffic_model'.

    Attributes
    ----------
    uri : str
        PostgreSQL database connection string.

    Notes
    -----
    The NPTM class interacts with travel time and distance datasets stored in a PostgreSQL
    database. It provides methods for formatting, analyzing, and storing transportation
    data. While initially designed for the Swiss NPTM, it can be adapted to other models
    with compatible data formats.
    """

    _type = 'national_passenger_traffic_model'  # Object type

    def __init__(self, param: Union[dict, ParamConfig], required_fields=None):
        """
        Initializes the NPTM instance with the specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            Dictionary containing configuration parameters for NPTM,
            or an already validated ParamConfig object.
            
            Required keys:
            - "db_nptm_schema" : str
                Schema name for the NPTM in the PostgreSQL database.
            - "db_zones_table" : str
                Name of the table containing traffic zones data.
            - "db_imt_table" : str
                Name of the table containing travel time and distance data for individual motorized transport (IMT).
            - "db_pt_table" : str
                Name of the table containing travel time and distance data for public transport (PT).
            - "uri" : str
                PostgreSQL database connection string.

            Optional keys:
            - "main_print" : bool
                If True, enables console output for execution status (default is False).
            - "sql_echo" : bool
                If True, enables SQL query logging (default is False).
                
        required_fields : list, optional
            A list of fields that are required for this specific instance.
            If not provided, defaults to all required fields for the class:
            ["db_nptm_schema", "db_zones_table", "db_imt_table", "db_pt_table", "uri"]

        Raises
        ------
        ValueError
            If any required parameter is missing.
        TypeError
            If a parameter has an incorrect type.

        Notes
        -----
        This method validates all required parameters and adjusts optional ones
        based on the execution context. It ensures compatibility with the
        transportation model's data and database schema.
        """ 
        # Define required fields for NPTM (default)
        default_required_fields = ["db_nptm_schema", "db_zones_table", "db_imt_table", "db_pt_table", "uri"]
    
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
        self.uri = self.config.uri

        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo
        
        # Initialize placeholders for all tables
        self.imt_mtx = None
        self.pt_mtx = None
        self.zones_gdf = None


    def setup_data(self, gdf_zones, df_imt_time, df_imt_length, df_pt_time, df_pt_length) -> "NPTM":
        """
        Formats and concatenates NPTM travel time and distance data.
        The geographic coordinate system is defined as WGS 84 (EPSG:4326)

        Parameters
        ----------
        gdf_zones : geopandas.GeoDataFrame
            GeoDataFrame containing NPTM zones data with required columns:
                - 'id' : int
                    New unique IDs for zones, numbered from 1.
                - 'nptmid' : Original zone IDs from the NPTM.

        df_imt_time : polars.DataFrame
            DataFrame with travel times for individual motorized transport.
            Required columns:
                - 'from' : Original NPTM zone IDs (route origin).
                - 'to' : Original NPTM zone IDs (route destination).
                - 'value' : Travel time (minutes). Null for no connection.

        df_imt_length : polars.DataFrame
            DataFrame with travel distances for individual motorized transport.
            Required columns:
                - 'from' : Original NPTM zone IDs (route origin).
                - 'to' : Original NPTM zone IDs (route destination).
                - 'value' : Travel distance (kilometers). Null for no connection.

        df_pt_time : polars.DataFrame
            DataFrame with travel times for public transport.
            Required columns:
                - 'from' : Original NPTM zone IDs (route origin).
                - 'to' : Original NPTM zone IDs (route destination).
                - 'value' : Travel time (minutes). Null for no connection.

        df_pt_length : polars.DataFrame
            DataFrame with travel distances for public transport.
            Required columns:
                - 'from' : Original NPTM zone IDs (route origin).
                - 'to' : Original NPTM zone IDs (route destination).
                - 'value' : Travel distance (kilometers). Null for no connection.

        Returns
        -------
        self.imt_mtx : polars.DataFrame
            Formatted displacement matrix for individual motorized transport.
        self.pt_mtx : polars.DataFrame
            Formatted displacement matrix for public transport.
        self.zones_gdf : geopandas.GeoDataFrame
            GeoDataFrame with NPTM zones data.
        """
        import polars as pl
        from transnetmap.utils.dct import dct_type
        from transnetmap.utils.utils import convert_to_pg_array
        
        # Prepare zone IDs
        df_id = (
            pl.DataFrame(gdf_zones[['id', 'nptmid']])
            .with_columns(pl.col('id').cast(pl.Int16))
        )
        
        # Format individual motorized transport data
        df_imt_time = (
            df_imt_time.join(df_id, how='left', left_on='from', right_on='nptmid')
            .drop('from')
            .rename({'id': 'from'})
            .with_columns(pl.col('from').cast(pl.Int16))
            .join(df_id, how='left', left_on='to', right_on='nptmid')
            .drop('to')
            .rename({'id': 'to', 'value': 'time'})
            .with_columns([
                pl.col('to').cast(pl.Int16),
                pl.col('time').cast(pl.Float32)
            ])
        )
        df_imt_length = (
            df_imt_length.join(df_id, how='left', left_on='from', right_on='nptmid')
            .drop('from')
            .rename({'id': 'from'})
            .with_columns(pl.col('from').cast(pl.Int16))
            .join(df_id, how='left', left_on='to', right_on='nptmid')
            .drop('to')
            .rename({'id': 'to', 'value': 'length'})
            .with_columns([
                pl.col('to').cast(pl.Int16),
                pl.col('length').cast(pl.Float32)
            ])
        )

        # Format public transport data
        df_pt_time = (
            df_pt_time.join(df_id, how='left', left_on='from', right_on='nptmid')
            .drop('from')
            .rename({'id': 'from'})
            .with_columns(pl.col('from').cast(pl.Int16))
            .join(df_id, how='left', left_on='to', right_on='nptmid')
            .drop('to')
            .rename({'id': 'to', 'value': 'time'})
            .with_columns([
                pl.col('to').cast(pl.Int16),
                pl.col('time').cast(pl.Float32)
            ])
        )
        df_pt_length = (
            df_pt_length.join(df_id, how='left', left_on='from', right_on='nptmid')
            .drop('from')
            .rename({'id': 'from'})
            .with_columns(pl.col('from').cast(pl.Int16))
            .join(df_id, how='left', left_on='to', right_on='nptmid')
            .drop('to')
            .rename({'id': 'to', 'value': 'length'})
            .with_columns([
                pl.col('to').cast(pl.Int16),
                pl.col('length').cast(pl.Float32)
            ])
        )

        # Combine time and length data for IMT
        imt_mtx = (
            df_imt_time.join(df_imt_length, on=['from', 'to'])
            .select(['from', 'to', 'time', 'length'])
            .sort(['from', 'to'])
            .with_columns(
                    pl.concat_list([pl.col('from'), pl.col('to')])
                    .alias('path')
                    .cast(pl.List(pl.Int16))
                )
            )
        # Combine time and length data for PT
        pt_mtx = (
            df_pt_time.join(df_pt_length, on=['from', 'to'])
            .select(['from', 'to', 'time', 'length'])
            .sort(['from', 'to'])
            .with_columns(
                    pl.concat_list([pl.col('from'), pl.col('to')])
                    .alias('path')
                    .cast(pl.List(pl.Int16))
                )
            )
        # Convert paths to PostgreSQL arrays
        for mtx in [imt_mtx, pt_mtx]:
            path = mtx['path'].to_pandas().apply(lambda x: convert_to_pg_array(x))
            mtx.replace_column(-1, pl.Series('path', pl.from_pandas(path)))

        # Assign types
        imt_mtx = imt_mtx.with_columns(
            pl.when((pl.col('time').is_null()) | (pl.col('length').is_null()))
            .then(dct_type['withoutIMT'])
            .otherwise(dct_type['IMT'])
            .alias('type')
            .cast(pl.Int8)
        )
        
        pt_mtx = pt_mtx.with_columns(
            pl.when((pl.col('time').is_null()) | (pl.col('length').is_null()))
            .then(dct_type['withoutPT'])
            .otherwise(dct_type['PT'])
            .alias('type')
            .cast(pl.Int8)
        )

        # Final matrices
        self.imt_mtx = imt_mtx.select(['from', 'to', 'type', 'time', 'length', 'path'])
        self.pt_mtx = pt_mtx.select(['from', 'to', 'type', 'time', 'length', 'path'])
        self.zones_gdf = gdf_zones.to_crs(4326) # Transformation into WGS 84 geographic coordinates

        if self.main_print:
            print("Setup data complete.")
        return self


    def to_sql(self, comments: dict, if_exists='fail') -> None:
        """
        Writes the formatted NPTM data (zones, IMT, PT) to the PostgreSQL database.
    
        Parameters
        ----------
        comments : dict
            Dictionary of comments for the schema and tables. Expected keys:
                - 'schema' : str
                    Comment for the NPTM schema.
                - 'zones' : str
                    Comment for the traffic zones table.
                - 'IMT' : str
                    Comment for the individual motorized transport table.
                - 'PT' : str
                    Comment for the public transport table.
        if_exists : str, optional
            Determines behavior if the tables already exist in the database (default is 'fail').
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
        from sqlalchemy.dialects.postgresql import SMALLINT
        from transnetmap.utils.sql import define_schema, schema_exists, execute_sql_script
        import time
    
        # ===============================
        # === Helper Functions ===
        # ===============================
        
        def write_db_pl_df(table, table_name, comment, if_exists='fail'):
            """
            Writes a polars DataFrame (IMT/PT) to the database.
            """
            # Define schema name
            schema = self.config.db_nptm_schema
            
            # Measure time for database write
            start_time = time.time()
            
            # Write table using ADBC engine
            try:
                table.write_database(
                    table_name=f"{schema}.{table_name}",
                    connection=self.uri,
                    engine="adbc",
                    if_table_exists=if_exists
                )
            except Exception as e:
                raise RuntimeError(f"An error occurred while writing to the database: {e}")
            
            # Format path as SMALLINT[], set primary key and add comment
            script = f'''
            ALTER TABLE "{schema}"."{table_name}"
            ALTER COLUMN path TYPE SMALLINT[]
            USING string_to_array(trim(both '{{}}' from path), ',')::SMALLINT[];
            
            ALTER TABLE "{schema}"."{table_name}"
            ADD CONSTRAINT {table_name}_pkey PRIMARY KEY ("from", "to");
            
            COMMENT ON TABLE "{schema}"."{table_name}" IS '{comment}';
            '''
            execute_sql_script(self.uri, script, print_status=self.main_print)
            
            elapsed_time = round(time.time() - start_time)
            print(f"Writing to the database is successful.\n"
                  f"Table: '{table_name}'\n"
                  f"Time taken: {elapsed_time} seconds.\n")
        
        def write_db_gdf_zones(table, table_name, comment, if_exists='fail'):
            """
            Writes a GeoDataFrame (zones) to the database.
            """
            # Define schema name
            schema = self.config.db_nptm_schema
            
            # Measure time for database write
            start_time = time.time()
            
            # Write table to database (PostGIS for geospatial data)
            try:
                with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                    table.to_postgis(
                        table_name,
                        connection, 
                        schema=schema,
                        if_exists=if_exists, 
                        index=False,
                        dtype={'id': SMALLINT}
                    )
            except Exception as e:
                raise RuntimeError(f"An error occurred while writing to the database: {e}")
            
            # Set primary key and add comment            
            script = f'''
            ALTER TABLE "{schema}"."{table_name}"
            ADD CONSTRAINT {table_name}_pkey PRIMARY KEY ("id");
            
            COMMENT ON TABLE "{schema}"."{table_name}" IS '{comment}';
            '''
            execute_sql_script(self.uri, script, print_status=self.main_print)
            
            elapsed_time = round(time.time() - start_time)
            print(f"Writing to the database is successful.\n"
                  f"Table: '{table_name}'\n"
                  f"Time taken: {elapsed_time} seconds.\n")
    
        # ===============================
        # === Setup ===
        # ===============================
        
        # Prohibit "append" to avoid data duplication issues
        if if_exists == 'append':
            raise ValueError(
                "'append' is not allowed in this method to prevent data duplication. "
                "Use 'fail' or 'replace' instead."
            )
    
        # Ensure schema exists in the database
        schema_name = self.config.db_nptm_schema
        if not schema_exists(self.uri, schema_name, print_status=self.main_print):
            define_schema(self.uri, schema_name, comments['schema'])
    
        # Extract comments for tables
        com_imt = comments['IMT']
        com_pt = comments['PT']
        com_zones = comments['zones']
    
        # ===============================
        # === Step 1: Write Zones ===
        # ===============================
        
        zones_table_name = self.config.db_zones_table
        write_db_gdf_zones(self.zones_gdf, zones_table_name, com_zones, if_exists=if_exists)
    
        # ===============================
        # === Step 2: Write IMT ===
        # ===============================
    
        imt_table_name = self.config.db_imt_table
        write_db_pl_df(self.imt_mtx, imt_table_name, com_imt, if_exists=if_exists)
    
        # ===============================
        # === Step 3: Write PT ===
        # ===============================
    
        pt_table_name = self.config.db_pt_table
        write_db_pl_df(self.pt_mtx, pt_table_name, com_pt, if_exists=if_exists)
        
        return None


    def read_sql(self, table_name, columns=None, where_condition=None):
        """
        Imports NPTM data from the database, with optional column selection and where condition.
    
        Parameters
        ----------
        table_name : str
            Name of the table (e.g., IMT, PT, or zones).
        columns : list of str, optional
            List of column names to select. If None, selects all columns (*).
            Default is None.
        where_condition : str, optional
            SQL WHERE clause to filter the rows. If None, no filtering is applied.
            Default is None.
    
        Returns
        -------
        polars.DataFrame or geopandas.GeoDataFrame
            - polars.DataFrame for IMT or PT tables.
            - geopandas.GeoDataFrame for zones table.
    
        Raises
        ------
        ValueError
            If the table name is invalid or unsupported.
        """
        if table_name == self.config.db_zones_table:
            table = self.__read_sql_zones(table_name, columns=columns, where_condition=where_condition)
        elif table_name in [self.config.db_imt_table, self.config.db_pt_table]:
                table = self.__read_sql_data(table_name, columns=columns, where_condition=where_condition)
        else:
            raise ValueError('Invalid or unsupported table name.')
    
        return table


    def __read_sql_data(self, table_name, columns=None, where_condition=None):
        """
        Imports travel data (IMT/PT) from the database, with optional column selection and where condition.
    
        Parameters
        ----------
        table_name : str
            Name of the table to read from (e.g., IMT or PT).
        columns : list of str, optional
            List of column names to select. If None, selects all columns (*).
            Default is None.
        where_condition : str, optional
            SQL WHERE clause to filter the rows. If None, no filtering is applied.
            Default is None.
    
        Returns
        -------
        polars.DataFrame
            DataFrame containing the travel data.
    
        Raises
        ------
        RuntimeError
            If the specified table does not exist in the database.
        """
        from transnetmap.utils.sql import table_exists, validate_columns
        import polars as pl
        import time
        
        # Define the schema and available column types
        schema = self.config.db_nptm_schema
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
            'path': pl.List(pl.Int16)
        }
    
        if self.main_print:
            print(f'Checking if the "{table_name}" table exists in the database.')
     
        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )
        
        if columns:
            # Perform the column check
            columns_part = validate_columns(self.uri, columns, table_name, schema, print_status=self.main_print)
            if self.main_print:
                print("All required columns exist. Proceeding with the query.")
        else:
            columns_part = "*"  # Default to all columns
            
        # Build the full query
        sql_query = f'SELECT {columns_part} FROM "{schema}"."{table_name}"'
        if where_condition:
            sql_query += f'\n{where_condition}'
            
        # Execute the query and load data into a Polars DataFrame
        start_time = time.time()
        try:
            table = pl.read_database_uri(sql_query, self.uri, engine='adbc')
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")
    
        # Dynamically cast columns based on the selection
        if columns:
            selected_types = {col: column_types[col] for col in columns if col in column_types}
        else:
            selected_types = column_types
    
        table = table.with_columns([pl.col(col).cast(dtype) for col, dtype in selected_types.items()])

        if self.main_print:
            print(f'Loading from the database was successful.\n'
                  f'Table: "{schema}"."{table_name}"\n'
                  f'Time to load: {round(time.time() - start_time, 3)} seconds.\n'
                  f'Size in polars.DataFrame format: {round(table.estimated_size("mb"))} MB.')
    
        return table


    def __read_sql_zones(self, table_name, columns=None, where_condition=None):
        """
        Imports geographical zones data from the database, with optional column selection and where condition.
    
        Parameters
        ----------
        table_name : str
            Name of the zones table.
        columns : list of str, optional
            List of column names to select. If None, selects all columns (*).
            Default is None.
        where_condition : str, optional
            SQL WHERE clause to filter the rows. If None, no filtering is applied.
            Default is None.
    
        Returns
        -------
        geopandas.GeoDataFrame
            GeoDataFrame containing the zones data.
    
        Raises
        ------
        RuntimeError
            If the specified table does not exist in the database.
        """
        import geopandas as gpd
        from sqlalchemy import create_engine
        from transnetmap.utils.sql import table_exists, validate_columns
        import time
        
        # Define the schema
        schema = self.config.db_nptm_schema
    
        if self.main_print:
            print(f'Checking if the "{table_name}" table exists in the database.')
    
        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )
            
        if columns:
            # Perform the column check
            columns_part = validate_columns(self.uri, columns, table_name, schema, print_status=self.main_print)
            if self.main_print:
                print("All required columns exist. Proceeding with the query.")
        else:
            columns_part = "*"  # Default to all columns
                
        # Build the full query
        sql_query = f'SELECT {columns_part} FROM "{schema}"."{table_name}"'
        if where_condition:
            sql_query += f'\n{where_condition}'
    
        start_time = time.time()
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                table = gpd.read_postgis(sql_query, connection, crs='EPSG:4326')
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")
        
        if "id" in table.columns:
            table = table.astype({'id': 'int16'})
        
        if self.main_print:
            print(f'Loading from the database was successful.\n'
                  f'Table: "{schema}"."{table_name}"\n'
                  f'Time to load: {round(time.time() - start_time, 3)} seconds.')

        return table


# ===========================
if __name__ == "__main__":
    
    #%% Definition of general parameters
    
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
    
    
    #%% Importing geographical data 
    
    " Expected data for formatting "
    '''
        GeoDataFrame with NPTM zones data : goepandas.GeoDataFrame
        Required columns :
            'id' : int
                New identification ids for NPTM base zones.
                Numbering from 1 to reduce the size of data tables by imposing an int16 format. 
            'nptmid' : undefined format
                Former NPTM base zone identification ids.
    '''
    
    # import geopandas as gpd
    # from numpy import arange
    
    # ===============================
    # === Fonctions ===
    # ===============================
    
    # def show_gdf(gdf):
        
        # import folium
        # import webbrowser
        # import tempfile
        
        # m = folium.Map(location=[47.03743, 8.35966], zoom_start=8) # Création de la carte Folium
            
        # folium.GeoJson(gdf).add_to(m) # Ajout des données du GeoDataFrame à la carte
        
    #     temp_html = tempfile.NamedTemporaryFile(suffix=".html").name # Enregistrement de la carte dans un fichier HTML temporaire
    #     m.save(temp_html)
    #     webbrowser.open("file://" + temp_html) # Ouvrir le fichier HTML dans le navigateur par défaut

    # ===============================
    # === Import ===
    # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\Verkehrszonen_Schweiz_NPVM_2017\Verkehrszonen_Schweiz_NPVM_2017_shp.zip"
    # name_base_id =  'ID' # The name of the 'ID' column depends on the base data
    
    # zones_2017 = gpd.read_file(file).sort_values(by=name_base_id).reset_index(drop=True)
    
    # del file
    
    # zones_2017.insert(0, 'id', arange(1, len(zones_2017) + 1))
    # zones_2017.insert(1, 'geometry', zones_2017.pop('geometry'))
    
    # zones_2017.rename(columns={name_base_id:'nptmid'}, inplace=True)
    
    # del name_base_id
    
    # ===============================
    # === Control ===
    # ===============================
    
    # show_gdf(zones_2017)




    #%% Importing displacement matrix
    
    " Expected data for formatting "
    '''
        DataFame with NPTM travel data for imt or pt : polars.DataFrame
            The unit of travel time data is minutes.
            The unit of travel distance data is kilometers.
            Required columns :
                'from' : undefined format
                    Former NPTM base zone identification ids.
                    Route origin. 
                'to' : undefined format
                    Former NPTM base zone identification ids.
                    Route destination.
                'value' : float32
                    Travel time or distance between origin and destination.
    
    In the transport (Individual Motorised Transport or Public Transport) matrixes at traffic zone level, 
    relations without links have the value Null.
    
    '''

    # import polars as pl

    # ===============================
    # === Fonctions ===
    # ===============================

    # def read_mtx(file, comment_line_top=None, comment_line_rear=None):
        
    #     with open(file, 'r', encoding='cp1252') as file:
    #         lines = file.readlines()
            
    #         if not comment_line_rear and comment_line_top:
    #             data_lines = lines[:comment_line_top]
    #         elif not comment_line_top and comment_line_rear:
    #             data_lines = lines[-comment_line_rear:]
    #         elif comment_line_top and comment_line_rear:
    #             data_lines = lines[comment_line_top:-comment_line_rear]
    #         else: pass
            
    #         # Initialize an empty dictionary for the final data
    #         data_dict = {"from": [], "to": [], "value": []}
    #         for line in data_lines:
    #             # Split each line into columns
    #             columns = line.split()
                
    #             # Append each column to the appropriate key in the dictionary
    #             data_dict["from"].append(columns[0])
    #             data_dict["to"].append(columns[1])
    #             data_dict["value"].append(columns[2])
                
    #     return data_dict

    # ===============================
    # === Step 1 : IMT travel time ===
    # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_Strasse_Reisezeit_Distanz_CH\DWV_2017_Strasse_Reisezeit_CH.mtx"
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # imt_time = pl.DataFrame(
    #     read_mtx(file, 
    #              comment_line_top=comment_line_top, 
    #              comment_line_rear=comment_line_rear
    #              )
    # )
    # imt_time = imt_time.with_columns(
    #         pl.col("from").cast(pl.Int64),
    #         pl.col("to").cast(pl.Int64),
    #         pl.col("value").cast(pl.Float32)
    # )
    
    # ===============================
    # === Step 2 : IMT travel distance ===
    # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_Strasse_Reisezeit_Distanz_CH\DWV_2017_Strasse_Distanz_CH.mtx"
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # imt_length = pl.DataFrame(
    #     read_mtx(file, 
    #              comment_line_top=comment_line_top, 
    #              comment_line_rear=comment_line_rear
    #             )
    # )
    # imt_length = imt_length.with_columns(
    #         pl.col("from").cast(pl.Int64),
    #         pl.col("to").cast(pl.Int64),
    #         pl.col("value").cast(pl.Float32)
    # )
    
    # ===============================
    # === Step 3 : PT travel time ===
    # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_OeV_Reisezeit_Distanz_CH\DWV_2017_ÖV_Reisezeit_CH.mtx"

    # no_relations_value = 999999
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # pt_time = pl.DataFrame(
    #     read_mtx(file, 
    #              comment_line_top=comment_line_top, 
    #              comment_line_rear=comment_line_rear
    #              )
    #     )
    # pt_time = pt_time.with_columns(
    #                     pl.col("from").cast(pl.Int64),
    #                     pl.col("to").cast(pl.Int64),
    #                     pl.col("value").cast(pl.Float32)
    #                     )
    # pt_time = pt_time.with_columns( # Remplacement des valeurs spécifiques par Null
    #                     pl.when(pl.col("value") == no_relations_value).then(None).otherwise(pl.col("value")).alias("value")
    #                     )
    
    # ===============================
    # === Step 4 : PT travel distance ===
    # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_OeV_Reisezeit_Distanz_CH\DWV_2017_ÖV_Distanz_CH.mtx"
    
    # no_relations_value = 999999
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # pt_length = pl.DataFrame(
    #     read_mtx(file, 
    #              comment_line_top=comment_line_top, 
    #              comment_line_rear=comment_line_rear
    #              )
    #     )
    # pt_length = pt_length.with_columns(
    #                     pl.col("from").cast(pl.Int64),
    #                     pl.col("to").cast(pl.Int64),
    #                     pl.col("value").cast(pl.Float32)
    #                     )
    # pt_length = pt_length.with_columns( # Remplacement des valeurs spécifiques par Null
    #                     pl.when(pl.col("value") == no_relations_value).then(None).otherwise(pl.col("value")).alias("value")
    #                     )
    
    # ===============================
    # === Step 5 : Clear variables ===
    # ===============================

    # del file; del comment_line_top; del comment_line_rear; del no_relations_value
    
    
    
    #%%Create NPTM object
    
    # nptm = NPTM(dct_param)
    
    
    
    #%% Create tables NPTM

    # nptm.setup_data(zones_2017, imt_time, imt_length, pt_time, pt_length)
    
    # del zones_2017; del imt_time; del imt_length; del pt_time; del pt_length


    #%% Creating the schema for NPTM and write tables
    

    # comment_schema = '''Sources : Swiss Confederation
    #     Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
    #     according to 2017 status.
    #     Data relating to travel times and distances are dated 20 April 2022 in the displacements matrixs.
    # '''
    
    # comment_imt = '''Matrix of travel times and distances in Switzerland, according to the zones of the 2017 national passenger traffic model.
    # Data relating to individual motorised transport, units: time in minutes and distances (lenght) in kilometers.
    # Sources : Swiss Confederation
    #     Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
    #     according to 2017 status.
    #     Data relating to travel times and distances are dated 20 April 2022.
    # '''

    # comment_pt = '''Matrix of travel times and distances in Switzerland, according to the zones of the 2017 national passenger traffic model.
    # Data relating to public transport, units: time in minutes and distances (lenght) in kilometers.
    # Relations without links have the value Null.
    # Sources : Swiss Confederation
    #     Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
    #     according to 2017 status.
    #     Data relating to travel times and distances are dated 20 April 2022.
    # '''

    # comment_zones = '''Zone structure in Switzerland according to the 2017 national passenger traffic model.
    # Sources : Swiss Confederation
    #     Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
    #     according to 2017 status.
    # '''
    
    
    
    
    # nptm.to_sql({
    #     'IMT': comment_imt, 
    #     'PT': comment_pt, 
    #     'zones': comment_zones,
    #     'schema': comment_schema})
    

    # imt = NPTM(dct_param).read_sql(dct_param["db_imt_table"])
    
    
    # condition_zones = '''WHERE "id" = 3;'''
    # condition_data = '''WHERE "from" = 3;'''
    
    # columns_zones = ["id", "geom"]
    # columns_data = ["from", "to", "type"]
    
    # zones = NPTM(dct_param).read_sql(dct_param["db_zones_table"])
    
    # zones = NPTM(dct_param).read_sql(dct_param["db_zones_table"], 
    #                                 columns=columns_zones, where_condition=condition_zones)
    
    # imt = NPTM(dct_param).read_sql(dct_param["db_imt_table"], 
    #                                columns=columns_data, where_condition=condition_data)
    
    # pt = NPTM(dct_param).read_sql(dct_param["db_pt_table"], 
    #                               columns=columns_data, where_condition=condition_data)
    
    # condition = '''WHERE "type" = -2;'''
    # pt = NPTM(dct_param).read_sql(dct_param["db_pt_table"], where_condition=condition)
    
    # condition = '''WHERE "type" = -2 AND "from" = 32;'''
    # pt32 = NPTM(dct_param).read_sql(dct_param["db_pt_table"], where_condition=condition)

    
    #%% Check comments lines displacement matrix
    
    # # ===============================
    # # === Fonctions ===
    # # ===============================
    
    # def cut_mtx(lines, comment_line_top=None, comment_line_rear=None):
             
    #     if not comment_line_rear and comment_line_top:
    #         data_lines = lines[:comment_line_top]
    #     elif not comment_line_top and comment_line_rear:
    #         data_lines = lines[-comment_line_rear:]
    #     elif comment_line_top and comment_line_rear:
    #         data_lines = lines[comment_line_top:-comment_line_rear]
    #     else: data_lines = None
        
    #     return data_lines

    
    # # ===============================
    # # === Step 1 : IMT travel time ===
    # # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_Strasse_Reisezeit_Distanz_CH\DWV_2017_Strasse_Reisezeit_CH.mtx"
    
    # with open(file, 'r', encoding='cp1252') as file:
    #     lines = file.readlines()
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # top = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=None)
    # rear = cut_mtx(lines, comment_line_top=None, comment_line_rear=comment_line_rear)
    
    # data = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=comment_line_rear)
    
    
    # # ===============================
    # # === Step 2 : IMT travel distance ===
    # # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_Strasse_Reisezeit_Distanz_CH\DWV_2017_Strasse_Distanz_CH.mtx"
    
    # with open(file, 'r', encoding='cp1252') as file:
    #     lines = file.readlines()
    
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # top = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=None)
    # rear = cut_mtx(lines, comment_line_top=None, comment_line_rear=comment_line_rear)
    
    # data = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=comment_line_rear)
    
    
    # # ===============================
    # # === Step 3 : PT travel time ===
    # # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_OeV_Reisezeit_Distanz_CH\DWV_2017_ÖV_Reisezeit_CH.mtx"
    
    # with open(file, 'r', encoding='cp1252') as file:
    #     lines = file.readlines()
    
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # top = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=None)
    # rear = cut_mtx(lines, comment_line_top=None, comment_line_rear=comment_line_rear)
    
    # data = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=comment_line_rear)

    
    # # ===============================
    # # === Step 4 : PT travel distance ===
    # # ===============================
    
    # file = r"C:\Users\valentin.pasche1\Downloads\DWV_2017_OeV_Reisezeit_Distanz_CH\DWV_2017_ÖV_Distanz_CH.mtx"
    
    # with open(file, 'r', encoding='cp1252') as file:
    #     lines = file.readlines()
    
    
    # comment_line_top=8
    # comment_line_rear=7980
    
    # top = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=None)
    # rear = cut_mtx(lines, comment_line_top=None, comment_line_rear=comment_line_rear)
    
    # data = cut_mtx(lines, comment_line_top=comment_line_top, comment_line_rear=comment_line_rear)

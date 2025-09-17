# -*- coding: utf-8 -*-
"""
Edge list construction and I/O for network analysis (transnetmap).

This module defines the class `EdgeList`, which builds a consolidated list of edges by
combining (1) the new network geometry and (2) base NPTM travel data, then filters
duplicates by the optimisation metric (time).

It also provides helpers to display irrelevant edges and to persist/read the edgelist to/from PostgreSQL.

Notes
-----
- The optimisation metric is fixed to ``'time'`` (see `EdgeList.OPTIMISATION_METRIC`).
"""

from __future__ import annotations

from typing import Optional, Union, Any

import polars as pl

from transnetmap.utils.config import ParamConfig

__all__ = ["EdgeList"]


# -----------------------------------------------------------------------------
# Class: EdgeList
# -----------------------------------------------------------------------------
class EdgeList: 
    """
    EdgeList is a core class for network analysis, designed to construct and manage the list of edges 
    required for routing and optimisation tasks. The class integrates data from a base network and 
    additional travel information, ensuring efficient handling of large datasets.

    Constants
    ----------
    OPTIMISATION_METRIC : str
    
        The metric used for optimisation, always set to 'time'.

    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters.
    network_number : int
        The identification number of the network instance.
    name_pvs : str
        The name of the physical value set (e.g., 'pvs2').
    network_extension_type : str
        Specifies the transport type used for extending the network ('IMT' or 'PT').
    db_nptm_schema : str
        Name of the schema containing National Passenger Traffic Model (NPTM) data.
    table_name_edgelist : str
        Name of the database table for the EdgeList, fixed to the constant `edgelist`.
    db_results_schema : str
        Schema name for analysis results, dynamically constructed based on network and extension parameters.
    nptm_table_name_extend : str
        Table name containing travel time and distance data for IMT or PT networks.
    uri : str
        Database connection string for PostgreSQL.
    main_print : bool
        Indicates whether to display progress and status messages during execution.
    sql_echo : bool
        Indicates whether to log SQL queries to the console.
    edgelist : polars.DataFrame, optional
        Stores the constructed edge list. Set after `create_edgelist` is called. Also set after ``read_sql_edgelist()``.
    irrelevant : bool or pandas.DataFrame, optional
        Indicates whether there are irrelevant edges in the new network:
        - False if all edges are relevant.
        - A Pandas DataFrame containing details of irrelevant edges if any exist.
    
    Notes
    -----
    - The EdgeList always optimises based on travel time (the fixed metric).
    - The NPTM base data is combined with the new network to construct the edge list.
    - The design ensures consistency across network analyses while minimising redundant computations.
    """

    OPTIMISATION_METRIC = 'time'  # Fixed optimisation metric for analysis (always based on time)

    def __init__(self, param: Union[dict, ParamConfig], *, required_fields: Optional[list] = None) -> None:
        """
        Initializes the EdgeList instance with specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
                
            - `"network_number"` : int  
                Identification number for the network instance.
            - `"physical_values_set_number"` : int  
                Identification number for the physical value set.
            - `"network_extension_type"`: str  
                Transport type identifier for network extension.
            - `"db_nptm_schema"` : str  
                Name of the schema containing the National Passenger Traffic Model (NPTM) data.
            - `"uri"` : str  
                PostgreSQL database connection string.

            Optional keys:
                
            - `"db_imt_table"` : str  
                Name of the table containing travel time and distance data for individual motorized transport (IMT).
            - `"db_pt_table"` : str  
                Name of the table containing travel time and distance data for public transport (PT).
            - `"main_print"` : bool  
                Enables console output for execution status. Default is False.
            - `"sql_echo"` : bool  
                Enables SQL query logging. Default is False.

        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided, defaults to
            `["network_number", "physical_values_set_number", "network_extension_type", "db_nptm_schema", "uri"]`.

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration or `network_extension_type` has an invalid value.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `EdgeList`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        # Define required fields for EdgeList (default)
        default_required_fields = ["network_number", "physical_values_set_number", 
                                   "network_extension_type", "db_nptm_schema", "uri"]

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
        self.name_pvs = f'pvs{self.config.physical_values_set_number}'
        self.network_extension_type = self.config.network_extension_type
        self.db_nptm_schema = self.config.db_nptm_schema
        self.uri = self.config.uri

        # Table and schema name
        self.table_name_edgelist = "edgelist" # Fixed table name for edgelist
        self.db_results_schema = (
            f'results_{self.network_number}_{self.name_pvs}_{self.network_extension_type.lower()}_{self.db_nptm_schema}'
            )

        # Define table name for network extension (as a function of the network_extension_type parameter). 
        if self.network_extension_type == "IMT":
            self.config.validate_for_class(["db_imt_table"])
            self.nptm_table_name_extend = self.config.db_imt_table
        elif self.network_extension_type == "PT":
            self.config.validate_for_class(["db_pt_table"])
            self.nptm_table_name_extend = self.config.db_pt_table
        else:
            raise ValueError(
                f"Invalid 'network_extension_type': {self.network_extension_type}. Expected 'IMT' or 'PT'."
            )
    
        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo

        # Initialize placeholders for tables
        self.edgelist = None
        self.irrelevant = None
            

    def __getattr__(self, name: str) -> Any:
        if name == "table":
            # EdgeList has an 'edgelist' table; guide the user explicitly.
            raise AttributeError(
                "EdgeList has no attribute 'table'.\n"
                "• Use 'edgelist' to access the edge list table."
            )
        # Standard Python message for other missing attributes
        raise AttributeError(f"{type(self).__name__} object has no attribute {name!r}")


    def _log(self, message: str) -> None:
        if self.main_print:
            print(message)
    

    def create_edgelist(self) -> EdgeList:
        """
        Creates the list of edges for the analysis graph using Pandas and Polars for efficiency.
    
        This method combines edges from the new network and additional data from the NPTM database
        to construct a comprehensive edge list for analysis. The resulting edge list is sorted
        by time and excludes duplicate edges, keeping the fastest connection for each pair of nodes.
        
        Returns
        -------
        self : EdgeList
            The updated EdgeList instance with the following attributes:
                
            - `self.edgelist` (polars.DataFrame): A Polars DataFrame containing the edge list.
              Sorted by 'from', 'to', and the optimisation metric ('time').
              
              Columns:
                  
                - 'from', 'to': int16 (node identifiers)
                - 'type': int8 (edge type, mapped from level)
                - 'time': float32 (travel time in minutes)
                - 'length': float32 (edge length in kilometers)
                
            - `self.irrelevant` (bool or pandas.DataFrame):
                
                - If False, all edges in the new network are relevant compared to the NPTM edges.
                - If a DataFrame, it contains edges from the new network that are longer in travel time
                  than corresponding edges from the NPTM database.
                  
                Columns:
                    
                  - 'from', 'to': int16 (node identifiers)
                  - 'network_type', 'nptm_type': str (edge type, mapped from transport type)
                  - 'network_time', 'nptm_time': float32 (travel time in minutes)
                  - 'network_length', 'nptm_length': float32 (edge length in kilometers)
    
        Raises
        ------
        ValueError
            If the resulting edge list is empty after filtering.
    
        Notes
        -----
        - The method processes the network data and maps physical values (e.g., speed, acceleration)
          based on the edge level (e.g., 'lower', 'main', 'higher') and PVS parameters.
        - Additional edges from the NPTM are filtered based on specific conditions:
            - Edges must connect nodes within the new network ('from' or 'to' is in the network).
            - Self-loops ('from' = 'to') are excluded.
            - Only edges of the specified transport type (IMT or PT) are included.
        - After combining network and NPTM edges, duplicates are resolved by keeping the shortest
          travel time for each pair of nodes. The optimisation metric is always set to 'time'.
    
        Filters Applied
        ---------------
        1. **Network edges**:
            - Physical values (e.g., speed, acceleration) are mapped based on edge level.
            - Edge length is adjusted using a fractal factor from PVS.
            - Travel time is calculated using a user-defined function.
    
        2. **NPTM edges**:
            - Edges must connect nodes in the new network.
            - Self-loops are excluded.
            - Only edges with the specified transport type ('IMT' or 'PT') are included.
    
        3. **Combined edge list**:
            - Duplicates (edges with the same 'from' and 'to') are resolved by keeping the fastest connection.
    
        Examples
        --------
        >>> edge_list = EdgeList(config).create_edgelist()
        >>> print(edge_list.table)  # Polars DataFrame with the edge list
        >>> print(edge_list.irrelevant)  # False or Pandas DataFrame of irrelevant edges
        """
        import pandas as pd
        from transnetmap.pre.nptm import NPTM
        from transnetmap.pre.network import Network
        from transnetmap.pre.pvs import PVS_TravelTime
        from transnetmap.utils.constant import DCT_LEVEL, DCT_TYPE, generate_level_to_type_mapping
        from transnetmap.utils.time_tools import import_time_function
        
        print("\nThe creation of the edgelist start.\n")
        
        # Step 1: Load network and PVS data
        network = Network(self.config).read_sql()
        pvs = PVS_TravelTime(self.config).read_sql()
        self.pvs = pvs.dct
        
        # Ensure all required keys are present in the PVS dictionary (double security check with PVS_TravelTime.read_sql)
        required_keys = {
            "tf_name",
            "l_ff", "m_ff", "h_ff",
            "l_a_it", "l_b_it", "m_a_it", "m_b_it", "h_a_it", "h_b_it",
            "l_aa", "l_ad", "m_aa", "m_ad", "h_aa", "h_ad",
            "l_ts", "m_ts", "h_ts"
        }
        
        missing_keys = required_keys - self.pvs.keys()
        if missing_keys:
            raise ValueError(
                f"Missing required keys in physical values set: {', '.join(sorted(missing_keys))}"
            )
        
        # Step 2: Define the edges list data format
        pd_dtypes = {
            'from': 'int16', 
            'to': 'int16', 
            'type': 'int8', 
            EdgeList.OPTIMISATION_METRIC: 'float32', 
            'length': 'float32'
        }
        pl_dtypes = {
            'from': pl.Int16, 
            'to': pl.Int16, 
            'type': pl.Int8, 
            EdgeList.OPTIMISATION_METRIC: pl.Float32, 
            'length': pl.Float32
        }
        columns = ['from', 'to', 'type', EdgeList.OPTIMISATION_METRIC, 'length']
        
        # Step 3: Generate mappings
        level_to_type_mapping = generate_level_to_type_mapping(DCT_LEVEL, DCT_TYPE)   

        # Step 4: Prepare the edges list from the network
        network_edgelist = network.table[["id_a", "id_b", "level", "length"]].copy().astype({'level': 'int8'})
        
        # Filter out edges where 'id_a' or 'id_b' are outside the NPTM study area (>= 10001 is defined in Stations class)
        network_edgelist = network_edgelist[ 
            (network_edgelist["id_a"] < 10001) & (network_edgelist["id_b"] < 10001) 
        ]

        # Step 5: Map physical values
        physical_params = ["ff", "aa", "ad", "ts", "a_it", "b_it"]  # All required parameters from PVS
        for param in physical_params:
            network_edgelist[param] = network_edgelist["level"].map({
                DCT_LEVEL["lower"]: self.pvs[f"l_{param}"]["value"],
                DCT_LEVEL["main"]: self.pvs[f"m_{param}"]["value"],
                DCT_LEVEL["higher"]: self.pvs[f"h_{param}"]["value"],
            })
        
        # Step 6: Adjust lengths and map level to type
        network_edgelist["length"] *= network_edgelist["ff"]
        network_edgelist = network_edgelist.rename(columns={"level": "type"})
        network_edgelist["type"] = network_edgelist["type"].replace(level_to_type_mapping)

        # Step 7: Calculate travel times
        time_function = import_time_function(self.pvs["tf_name"]["value"])
        network_edgelist[EdgeList.OPTIMISATION_METRIC] = network_edgelist.apply(
            lambda row: time_function(
                row["length"], 
                row["ts"], 
                row["aa"], 
                row["ad"]
            ) + row["a_it"] + row["b_it"],
            axis=1
        )
        
        # Step 8: Adjust length to kilometers
        network_edgelist["length"] /= 1000
        
        # Step 9: Intermediate edge list formatting
        network_edgelist = network_edgelist.rename(
            columns={"id_a": "from", "id_b": "to"}
        )[columns].astype(pd_dtypes)

        # Step 10: Creates the returned version of network edge list, and convert to Polars
        network_edgelist_reverse = network_edgelist.rename(columns={"from": "to", "to": "from"}).copy()
        network = pd.concat([network_edgelist, network_edgelist_reverse], ignore_index=True)
        network = pl.from_pandas(network).select(columns).cast(pl_dtypes)
        
        # Step 11: Retrieve NPTM data for network extension
        id_network = set(network_edgelist["from"]).union(set(network_edgelist["to"]))
        id_list = ",".join(map(str, id_network))
        type_value_nptm = DCT_TYPE[self.network_extension_type]
        
        condition_nptm = (
            f'WHERE ("from" IN ({id_list}) OR "to" IN ({id_list})) ' # Filter based on id
            f'AND "from" <> "to" ' # Exclusion filter
            f'AND "type" = {type_value_nptm}' # Filter based on type
        )
        
        nptm_data = NPTM(self.config).read_sql(
            self.nptm_table_name_extend, columns=columns, where_condition=condition_nptm
        )
        
        # Step 12: Combine network and NPTM data, sorted depending on the optimisation variable
        combined_df = pl.concat([network, nptm_data]).sort(["from", "to", EdgeList.OPTIMISATION_METRIC])
        
        # Step 13: Elimination of double links, network and NPTM, the edges shorter in time are kept 
        combined_df = combined_df.unique(subset=["from", "to"], keep="first", maintain_order=True)
        
        # Step 14: Definition of the self.table attribute (polars.DataFrame)
        # (polars.DataFRame is more efficient for interactions with the DB for large datasets)
        self.edgelist = combined_df
        
        # Step 15: Check for irrelevant edges, and convert to Pandas
        df_irrelevant = (
            network.join(nptm_data, on=["from", "to"], how="left", suffix=("_nptm"), coalesce=True)
            .filter(pl.col("time") > pl.col("time_nptm")) # Keep only edges with longer times in the network
            .sort(["time", "time_nptm"])
        ).to_pandas()
        
        # Step 16: Definition of the self.irrelevant attribute (False or pandas.DataFrame)
        if df_irrelevant.empty:
            self.irrelevant = False
            self._log("\nAll edges of the new network are relevant.")
        
        else:
            # Map type values back to their original keys for clarity (str)
            df_irrelevant["type"] = df_irrelevant["type"].map(dict(map(reversed, DCT_TYPE.items())))
            df_irrelevant["type_nptm"] = df_irrelevant["type_nptm"].map(dict(map(reversed, DCT_TYPE.items())))            
            
            # Select and rename columns for better readability in the final pandas DataFrame
            self.irrelevant = (
                    df_irrelevant.rename(
                        columns={
                            "type": "network_type", 
                            "type_nptm": "nptm_type", 
                            "time": "network_time", 
                            "time_nptm": "nptm_time",
                            "length": "network_length", 
                            "length_nptm": "nptm_length"
                        }
                    )
                    .reindex(
                        columns=[
                            "from", "to", 
                            "network_type", "network_time", "network_length", 
                            "nptm_type", "nptm_time", "nptm_length"
                        ]
                    )
                    .astype({
                        "from": "int16", 
                        "to": "int16",
                        "network_type": "str", 
                        "nptm_type": "str", 
                        "network_time": "float32", 
                        "nptm_time": "float32",
                        "network_length": "float32", 
                        "nptm_length": "float32"
                    })
                )

            print(f'''\nPlease note that {self.irrelevant.shape[0]} edges of the new network are not relevant.''')
            
        print("\nThe edgelist has been successfully created, and sorted by time.\n")

        return self    


    def show_irrelevant_edges(
        self,
        *,
        file_name: str = "irrelevant_edges",
        save_to_desktop: bool = False,
        custom_path: Optional[str] = None,
        location: Optional[tuple[float, float]] = None,
        zoom_start: Optional[int] = None,
    ) -> None:
        """
        Displays the edges of the new network that were removed due to irrelevance.
        
        This function visualizes edges that were excluded from the final 'edgelist' because 
        they were longer than equivalent NPTM edges in terms of travel time.
        
        Parameters
        ----------
        file_name : str, optional
            Base name of the file to save the map as (without extension). Default is "irrelevant_edges".
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
        None

        Notes
        -----
        - The irrelevant edges are displayed in red.
        - Station data and network edges are dynamically retrieved from the database.
        - The map is saved temporarily and opened in the default web browser.

        Raises
        ------
        AttributeError
            If `self.irrelevant` is None, meaning `create_edgelist()` has not been executed.
        RuntimeError
            If `self.irrelevant` is False, meaning all edges are relevant.
        """
        from folium import GeoJson, GeoJsonTooltip, GeoJsonPopup
        from transnetmap.pre.network_child import Stations
        from transnetmap.pre.network import Network
        from transnetmap.utils.map import show_map, auto_fit_map

        #  Step 1: Validate `self.irrelevant`
        if self.irrelevant is None:
            raise AttributeError(
                "No information about irrelevant edges has been detected. Run `create_edgelist()` first."
            )

        if self.irrelevant is False:
            print("All edges of the new network are relevant. No map to display.")
            return None

        #  Step 2: Extract relevant data
        edge_tuples = set(zip(self.irrelevant["from"], self.irrelevant["to"]))
        node_ids = set(self.irrelevant["from"]).union(self.irrelevant["to"])

        #  Step 3: Load station data, filtering on the "id" column
        stations = Stations(self.config).read_sql().table[["geom", "code", "name", "id"]]
        stations = stations[stations["id"].isin(node_ids)]

        #  Step 4: Load network edge data, filtering on edges
        network = Network(self.config).read_sql().table[["geom", "id_a", "id_b"]]
        network = network[network[["id_a", "id_b"]].apply(tuple, axis=1).isin(edge_tuples)]

        # Merge network data with `self.irrelevant` for additional information
        network = network.merge(self.irrelevant, left_on=["id_a", "id_b"], right_on=["from", "to"], how="inner")

        #  Step 5: Create the map with automatic bounds
        m = auto_fit_map(network, location=location, zoom_start=zoom_start)

        #  Step 6: Add station data to the map
        GeoJson(
            stations,
            tooltip=GeoJsonTooltip(fields=["name"], aliases=["Name:"]),
            popup=GeoJsonPopup(fields=["name", "code", "id"], aliases=["Name:", "Code:", "ID:"]),
            name="Stations",
        ).add_to(m)

        #  Step 7: Add network data to the map
        GeoJson(
            network,
            tooltip=GeoJsonTooltip(fields=["network_type"], aliases=["Type:"]),
            popup=GeoJsonPopup(
                fields=["network_type", "network_time", "network_length", 
                        "nptm_type", "nptm_time", "nptm_length"],
                aliases=["Network Type:", "Network Time (min):", "Network Length (km):",
                         "NPTM Type:", "NPTM Time (min):", "NPTM Length (km):"],
            ),
            style_function=lambda x: {"color": "#FF0000", "weight": 5, "opacity": 1},
            name="Irrelevant Edges",
        ).add_to(m)

        #  Step 8: Save and display map
        show_map(m, file_name=file_name, save_to_desktop=save_to_desktop, custom_path=custom_path)

        return None  # The map is displayed, no need to return anything


    def to_sql_edgelist(self, comment_schema: Optional[str] = None, *, if_exists: str = 'fail') -> None:
        """
        Writes the formatted edgelist data to the database.
        If the schema does not exist, it is created and commented on.
    
        This method ensures that the database schema and table are well-documented and 
        conform to the expected structure for downstream analysis. The schema is intended 
        to group all results from the same optimisation process, facilitating traceability 
        and consistency across multiple runs. Commenting the schema is highly recommended 
        to provide context on its contents, origin of data, and structural organisation.
    
        Parameters
        ----------
        comment_schema : str, optional
            Comment added to the schema in the database when it is created. The default is None.
            Mandatory parameter when the schema does not exist.   
            
        if_exists : str, optional
            Determines behavior if the tables already exist in the database (default is `'fail'`).  
            Options:  
                - `'fail'` : Raises an error if the table exists.  
                - `'replace'` : Drops and recreates the table.
    
        Raises
        ------
        ValueError
            If `if_exists` is set to an invalid value (must be `'fail'` or `'replace'`).
        KeyError
            If the schema does not exist and `comment_schema` is not provided.
        TypeError
            If `comment_schema` is provided but is not a string.
        RuntimeError
            If an error occurs while writing the data to the database.
    
        Returns
        -------
        None
            The function has no return value but writes the edgelist data to the database.
        
        Notes
        -----
        - The function ensures that the schema and table are created only if they do not already exist.
        - The `adbc` engine is used for optimal writing performance with Polars DataFrames.
        - Adds a composite primary key on the columns ["from", "to"].
        - The schema is structured as follows:
            - **Schema name**: Defined by the **network number (`network_number`)**, **physical values set (`physical_values_set_number`)**, 
              **transport extension type (`network_extension_type`)**, and the **source schema (`db_nptm_schema`)**.
            - **Tables inside the schema**:
                - `edgelist`: List of filtered sections for the optimisation algorithm.
                - `optimisation`: Optimisation results filtered with NPTM data.
                - `results_{id}`: Partial results, calculated with impacts, from or to a zone (`id` refers to `{db_zones_table}`).
        - **Optimisation is time-dependent**, meaning that results vary based on time-based calculations.
    
        Examples
        --------
        # Define the schema comment explaining its purpose and structure
        
        >>> com_schema_results = f'''
        The tables in this schema contain the results of optimising the new network {`network_number`}
        using the physical parameters "pvs{`physical_values_set_number`}".
        The source data comes from NPTM in Switzerland (schema {`db_nptm_schema`}),
        and the transport type used for extension is "{`network_extension_type`}".
        '''
    
        # Write the edgelist to the database
        
        >>> edgelist.to_sql_edgelist(
                comment_schema=com_schema_results, 
                if_exists='replace'
            )
        """
        from transnetmap.utils.sql import define_schema, schema_exists, execute_primary_key_script
        from transnetmap.utils.constant import IMPACTS
        import time
    
        #  Step 1: Validate parameters
        if if_exists not in ['fail', 'replace']:
            raise ValueError(f"Invalid value for `if_exists`: {if_exists}. Use 'fail' or 'replace'.")
    
        schema = self.db_results_schema
        table_name = self.table_name_edgelist
    
        #  Step 2: Ensure schema exists
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            if not comment_schema:
                raise KeyError("Schema does not exist, parameter 'comment_schema' is required.")
            if not isinstance(comment_schema, str):
                raise TypeError("Argument 'comment_schema' must be a string.\n")
                
            define_schema(self.uri, schema, text_comment=comment_schema)
            print(f"Schema was created: {schema}\n")
    
        #  Step 3: Validate the table before writing
        if self.edgelist.is_empty():
            raise RuntimeError(f"Attempted to write an empty EdgeList table: {table_name}")
    
        #  Step 4: Apply column types dynamically
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
        }
        for impact in IMPACTS:
            column_types[impact] = pl.Float32
    
        selected_types = {col: column_types[col] for col in self.edgelist.columns}
        self.edgelist = self.edgelist.with_columns(
            [pl.col(col).cast(dtype) for col, dtype in selected_types.items()]
        )
    
        #  Step 5: Measure time for database write
        start_time = time.time()
    
        #  Step 6: Write table using ADBC engine
        try:
            self.edgelist.write_database(
                table_name=f"{schema}.{table_name}",
                connection=self.uri,
                engine="adbc",
                if_table_exists=if_exists
            )
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to the database: {e}")
    
        #  Step 7: Add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["from", "to"],
            schema=schema,
            include_schema_in_pk_name=True,
            print_status=self.main_print
        )
    
        elapsed_time = round(time.time() - start_time)
    
        #  Step 8: Log success message
        num_rows = self.edgelist.shape[0]
        self._log(f"Writing to the database is successful.\n"
                  f"Table: '{table_name}'\n"
                  f"Number of rows inserted: {num_rows}\n"
                  f"Time taken: {elapsed_time} seconds.\n")
        
        return None


    def read_sql_edgelist(
        self,
        *,
        columns: Optional[list] = None,
        where_condition: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Imports edgelist data from the database, with optional column selection and where condition.
    
        Parameters
        ----------
        columns : list of str, optional
            List of column names to select. If None, selects all columns (*).
            Default is None.
        where_condition : str, optional
            SQL WHERE clause to filter the rows. If None, no filtering is applied.
            Default is None.
    
        Returns
        -------
        polars.DataFrame
            DataFrame containing the edgelist data.
                        
            - If called on an instance (``edgelist.read_sql_edgelist()``), the results are stored in ``self.edgelist``.
            - If called without assignment (``EdgeList(config).read_sql_edgelist()``), only the DataFrame is returned, 
              and the instance is not stored.
    
        Raises
        ------
        RuntimeError
            If the specified table does not exist in the database.  
            If the query returns an empty dataset.
    
        Notes
        -----
        - Dynamically casts columns to appropriate types as defined in the ``column_types`` dictionary.
        - Allows for SQL filtering through the ``where_condition`` parameter.
        - Uses the `adbc` engine for optimal database querying.
        """
        from transnetmap.utils.sql import table_exists, validate_columns, columns_exist
        from transnetmap.utils.constant import IMPACTS
    
        #  Step 1: Define the schema and table
        schema = self.db_results_schema
        table_name = self.table_name_edgelist
    
        #  Step 2: Check if the table exists
        self._log(f'Checking if the "{table_name}" table exists in the database.')
        
        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )
        
        #  Step 3: Validate requested columns
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
        }
        for impact in IMPACTS:
            column_types[impact] = pl.Float32
        
        if columns:
            columns_part = validate_columns(
                self.uri, columns, table_name, schema, print_status=self.main_print
            )
            self._log("All required columns exist. Proceeding with the query.")
        else:
            # Default to all available columns
            columns = columns_exist(
                self.uri, list(column_types.keys()), table_name, schema, print_status=self.main_print
            )
            columns = [col for col in columns if columns[col]]
            columns_part = ", ".join(f'"{col}"' for col in columns)
        
        #  Step 4: Build the SQL query
        sql_query = f'SELECT {columns_part} FROM "{schema}"."{table_name}"'
        if where_condition:
            sql_query += f'\n{where_condition}'
    
        self._log(f"Executing SQL query:\n{sql_query}\n")
    
        #  Step 5: Execute the SQL query and load data into a Polars DataFrame
        try:
            table = pl.read_database_uri(sql_query, self.uri, engine='adbc')
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}\nQuery: {sql_query}")
    
        #  Step 6: Check if the table is empty
        if table.is_empty():
            raise RuntimeError(f"Query returned an empty dataset for table: {table_name}")
    
        #  Step 7: Dynamically cast columns based on selection
        selected_types = {col: column_types[col] for col in columns}
        table = table.with_columns(
            [pl.col(col).cast(dtype) for col, dtype in selected_types.items()]
        )
    
        #  Step 8: Log query results
        num_rows, num_cols = table.shape
        if self.main_print:
            selected_cols = ", ".join(columns)
            print(f"Query executed successfully.\n"
                  f"Selected columns: {selected_cols}\n"
                  f"Rows loaded: {num_rows}\n"
                  f"Columns retrieved: {num_cols}\n")
    
        #  Step 9: Store the table in `self.edgelist`
        self.edgelist = table
    
        return table  # Return the DataFrame instead of modifying the instance directly
        

# -----------------------------------------------------------------------------
# Main (example-only)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
        
    # Complete dictionary of creation and calculation parameters
    dct_param = {
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
    }

    
    
    
    x = EdgeList(dct_param)
    
    
    # x.OPTIMISATION_METRIC
    
    x = x.create_edgelist()
    # # vars(x)
    
    # y = x.edgelist
    # z = x.irrelevant
    x.show_irrelevant_edges()
    
    
    # com_schema_results = f'''The tables in the schema are the results of optimising the new network "{dct_param["network_number"]}" with the physical parameters "pvs{dct_param["physical_values_set_number"]}".
    # The basic data comes from NPTM in Switzerland (schema {dct_param["db_nptm_schema"]}),
    # the type of transport, from NPTM, used to extend the new network to all the areas studied is {dct_param["network_extension_type"]}.               
    # Optimisation is time-dependent.

    # The tables are organised as follows:
    #     - edgelist: list of sections filtered for the optimisation algorithm
    #     - optimisation: optimisation results filtered with NPTM data
    #     - results_"id": partial results, calculated with impacts, from or to a zone (id ref. {dct_param["db_zones_table"]})


    # Sources : Swiss Confederation
    #     Data from National Passenger Traffic Model, provided by the Federal Office of Spatial Development (ARE),
    #     according to 2017 status.
    # '''

    # x.create_edgelist()
    # x.to_sql_edgelist(comment_schema=com_schema_results, if_exists='replace')
    
    # x.to_sql_edgelist(comment_schema=com_schema_results, if_exists='fail')
    
    # x1 = EdgeList(dct_param)
    # x1.read_sql_edgelist()
    
    # x2 = EdgeList(dct_param).read_sql_edgelist(columns=['from','to','type'], where_condition='''WHERE "type" = 5''')

    # x3 = x1.edgelist


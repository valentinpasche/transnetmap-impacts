# -*- coding: utf-8 -*-
"""
Graph-based optimisation using Dijkstra over the edge list.

This module defines the class `Graph`, a thin wrapper on top of class `transnetmap.analysis.edgelist.EdgeList`
that builds a directed graph (NetworkX DiGraph) from the edgelist and runs Dijkstra to produce
an optimisation table.
"""

from __future__ import annotations

from typing import Optional, Union, Any, TYPE_CHECKING
import time
import threading

import pandas as pd
import polars as pl
import networkx as nx

from transnetmap.analysis.edgelist import EdgeList
from transnetmap.utils.utils import spinner, convert_to_pg_array

if TYPE_CHECKING:  # noqa: F401
    from transnetmap.utils.config import ParamConfig

__all__ = ["Graph"]


# -----------------------------------------------------------------------------
# Class: Graph
# -----------------------------------------------------------------------------
class Graph(EdgeList):
    """
    Graph class for network optimization using the Dijkstra algorithm.
    
    This class `Graph` is a thin wrapper on top of class `transnetmap.analysis.edgelist.EdgeList`
    that builds a directed graph (NetworkX DiGraph) from the edgelist and runs Dijkstra to produce
    an optimisation table.
    
    Inherits parameters and methods from EdgeList.

    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters.
    graph : networkx.DiGraph
        Directed graph built from the edge list (DiGraph format).
    optimisation : polars.DataFrame or None
        Table containing the results of the Dijkstra optimization.
    table_name_optimisation : str
        Fixed table name for storing optimization results in the database.
    main_print_graph : bool
        Controls console output for the Graph-specific methods, determined
        by configuration parameters or execution context.

    Methods
    -------
    _create_graph(edgelist=None):
        Creates a directed graph (DiGraph) from the `edgelist` attribute or a custom subset.
    process_dijkstra(to_sql_optimisation=True):
        Runs Dijkstra's algorithm between all nodes to calculate shortest paths
        and formats the results.
    to_sql_optimisation(if_exists='fail'):
        Saves the results of the Dijkstra optimization into the database.
    read_sql_optimisation(columns=None, where_condition=None):
        Read the results of the Dijkstra optimization into the database.

    Notes
    -----
    - This class is tightly coupled with the `EdgeList` class, leveraging its methods
      for edge list creation, database interaction, and validation.
    - The `graph` attribute is built from the edge list and serves as the foundation
      for all optimization calculations.
    """

    def __init__(self, param: Union[dict, ParamConfig]) -> None:
        """
        Initializes the Graph class by inheriting EdgeList parameters and attributes.

        Parameters
        ----------
        param : dict or ParamConfig
            Configuration parameters for EdgeList and Graph.
            
            Default required fields from `edgelist.EdgeList`:  
                `["network_number", "physical_values_set_number", "network_extension_type", "db_nptm_schema", "uri"]`
        """
        super().__init__(param)  # Inherit from EdgeList
        self.graph = None  # Placeholder for the NetworkX graph
        self.optimisation = None  # Placeholder for the optimisation results table
        
        # Fixed table name for optimisation results
        self.table_name_optimisation = "optimisation"
        
        # Adjust parameters based on execution context
        self.main_print_graph = self.config.main_print or (__name__ == "__main__")
        

    def __getattr__(self, name: str) -> Any:
        if name == "table":
            # Graph exposes 'optimisation' (results) and still has 'edgelist' from the parent.
            raise AttributeError(
                "Graph has no attribute 'table'.\n"
                "• Use 'optimisation' to access the computed results table.\n"
                "• Use 'edgelist' to access the edge list table."
            )
        raise AttributeError(f"{type(self).__name__} object has no attribute {name!r}")


    def _log(self, message: str) -> None:
        if self.main_print:
            print(message)


    def _create_graph(self, edgelist: Optional[pl.DataFrame] = None) -> None:
        """
        Creates a directed graph (DiGraph) from the `edgelist` attribute or a custom subset for debugging purposes.
    
        This method constructs a NetworkX DiGraph instance using the edge list data. It supports passing
        a custom `edgelist` parameter to facilitate testing or debugging on a smaller subset of edges.
        However, using this parameter in production can lead to inconsistencies if the results are stored
        in the database.
    
        Parameters
        ----------
        edgelist : pl.DataFrame, optional
            A Polars DataFrame containing a custom subset of the edge list, with columns ['from', 'to', Graph.OPTIMISATION_METRIC'].
            If provided, this subset is used instead of the full `edgelist` attribute or the database.
            This option is intended for testing or debugging only.
    
        Returns
        -------
        None
            Updates `self.graph` with a NetworkX DiGraph instance.
    
         Raises
            ------
            RuntimeError
                If the schema in the database is not created before defining the analysis Graph.
                If the EdgeList table does not exist in the database and cannot be loaded.
            TypeError
                If the provided `edgelist` is not a Polars DataFrame.
            ValueError
                If the custom `edgelist` is missing required columns: ['from', 'to', Graph.OPTIMISATION_METRIC'].
           
        Notes
        -----
        - The graph is built using the `from`, `to`, and `OPTIMISATION_METRIC` columns.
        - Converts Polars DataFrame to Pandas for compatibility with NetworkX.
        - **Important**: Avoid using the `edgelist` parameter for production runs, as it bypasses the
          consistency checks tied to the full edge list. This can lead to incorrect results being
          saved in the database.
        - For production use, ensure the complete `edgelist` is used, either by reading it from the database
          or passing the full Polars DataFrame from `self.edgelist`.
        
        Examples
        --------
        >>> graph = Graph(config)
        >>> graph._create_graph()
        >>> print(graph.graph)  # Outputs the NetworkX graph
        
        >>> # Using a custom edge list for testing
        >>> custom_edgelist = edgelist.slice(0, 100)
        >>> graph._create_graph(edgelist=custom_edgelist)
        >>> print(graph.graph)
        """
        from transnetmap.utils.sql import schema_exists, table_exists
        
        schema = self.db_results_schema
        table_name = self.table_name_edgelist
        
        schema = self.db_results_schema
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            raise RuntimeError(
                "Schema in database does not exist. "
                "Run 'to_sql_edgelist' first."
            )
        
        if not table_exists(self.uri, table_name, print_status=self.main_print_graph):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'It must be written to the database (schema: "{schema}").\n'
                "\nRun 'to_sql_edgelist' first."
            )
        
        # Determine the edge list source
        if edgelist is not None:
            # Warn the user about using a custom edge list
            print("\n"
                  "Warning: You are using a custom edge list. This is intended for testing or debugging only."
            )
            if not isinstance(edgelist, pl.DataFrame):
                raise TypeError("The custom 'edgelist' must be a Polars DataFrame.")
            # Use the provided custom edge list
            self.table = edgelist[['from', 'to', Graph.OPTIMISATION_METRIC]]
            
        elif isinstance(self.edgelist, pl.DataFrame):
            # Use the edge list from the current instance
            self.table = self.edgelist[['from', 'to', Graph.OPTIMISATION_METRIC]]
        else:
            # Read the edge list from the database
            self.read_sql_edgelist(columns=['from', 'to', Graph.OPTIMISATION_METRIC])
            self.table = self.edgelist[['from', 'to', Graph.OPTIMISATION_METRIC]]
    
        # Validate required columns
        required_columns = {'from', 'to', Graph.OPTIMISATION_METRIC}
        missing_columns = required_columns - set(self.table.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns in edgelist: {', '.join(missing_columns)}")
    
        # Convert Polars DataFrame to Pandas for compatibility with NetworkX
        edgelist_df = self.table.to_pandas()
        
        # Build the directed graph from the edgelist
        self.graph = nx.from_pandas_edgelist(
            edgelist_df,
            source="from",
            target="to",
            edge_attr=True,
            create_using=nx.DiGraph
        )
        
        if self.main_print_graph:
            print(f"\nDirected graph created successfully with {self.graph.number_of_nodes()} nodes "
                  f"and {self.graph.number_of_edges()} edges.")


    def process_dijkstra(self, *, to_sql_optimisation: bool = True) -> Graph:
        """
        Runs Dijkstra's algorithm between all nodes to calculate shortest paths.
        If the table does not exist in the database, it is created.
    
        Parameters
        ----------
        to_sql_optimisation : bool, optional
            Use optimisation.to_sql_optimisation(if_exists='fail')
            The default is True.
    
        Returns
        -------
        self : Graph
            The updated Graph instance with the following attributes:
                
            - `self.optimisation` (polars.DataFrame): A Polars DataFrame containing the optimisation results.
              Sorted by 'from', 'to', and the optimisation metric ('time').
              
              Columns:
                  
                - 'from', 'to': int16 (node identifiers)
                - 'type': int8 (type identifier)
                - 'time': float32 (travel time in minutes)
                - 'nb_edges': int8 (number of edges in the path : len(path) - 1)
                - 'path': list(int16) (travel path, list of nodes)
    
        Raises
        ------
        RuntimeError
            If the schema in the database is not created before running Dijkstra.
    
        Notes
        -----
        - Calculations can take a long time on large graphs (several hundred thousand nodes).
        - Make sure you have enough memory for manipulations with Pandas and Polars.
        - Filtering with NPTM data is done in LazyFrame to minimise memory load.
        - The NPTM data import (full table) is initially loaded in DataFrame.
        """
        from transnetmap.pre.nptm import NPTM
        from transnetmap.utils.sql import schema_exists
        from transnetmap.utils.constant import DCT_TYPE
        from transnetmap.utils.utils import to_engineering_notation
    
        # Step 1: Verify graph existence and schema
        print("\nStep 1: Initalisation Dijkstra ...\n")

        if not hasattr(self, 'graph') or not isinstance(self.graph, nx.DiGraph):
            self._create_graph()
        if not self.graph or self.graph.number_of_edges() == 0:
            raise RuntimeError("The graph is empty or contains no edges. Ensure the edge list is valid.")
    
        schema = self.db_results_schema
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            raise RuntimeError(
                "Schema in database does not exist. "
                "Run 'to_sql_edgelist' first."
            )
    
        # Step 2: Calculate shortest paths using Dijkstra
        print("Step 2: Calculating shortest paths using Dijkstra's algorithm...\n")
        stop_event = threading.Event()
        spinner_thread = threading.Thread(target=spinner, args=("Calculating shortest paths", stop_event))
        spinner_thread.start()
    
        try:
            start_time = time.time()
            try:
                dijkstra_raw = dict(
                    nx.all_pairs_dijkstra(
                        self.graph,
                        cutoff=None,
                        weight=Graph.OPTIMISATION_METRIC
                    )
                )
                time_algorithm = round(time.time() - start_time)
            except nx.NetworkXNoPath as e:
                raise RuntimeError("No path exists between some nodes in the graph.") from e
            except Exception as e:
                raise RuntimeError("An error occurred during the Dijkstra calculation.") from e
        finally:
            stop_event.set()
            spinner_thread.join()
    
        print(
            "Dijkstra's algorithm successfully completed.\n"
            f"Calculation time: {time_algorithm} seconds.\n"
            "\nStep 3: Formatting results...\n"
        )
    
        # Step 3: Format results
        start_time = time.time()
        dfs = []
        for source, (distances, paths) in dijkstra_raw.items():
            target_keys = list(distances.keys())
            df_chunk = pd.DataFrame({
                "from": [source] * len(target_keys),
                "to": target_keys,
                Graph.OPTIMISATION_METRIC: [distances[key] for key in target_keys],
                "path": [paths[key] for key in target_keys]
            })
            dfs.append(df_chunk)
        del dijkstra_raw; del df_chunk  # Free memory
        
        df_dijkstra = pd.concat(dfs, ignore_index=True).astype({
            'from': 'int16', 'to': 'int16', Graph.OPTIMISATION_METRIC: 'float32'
        })
        del dfs  # Free memory
    
        df_dijkstra = df_dijkstra.loc[df_dijkstra["from"] != df_dijkstra["to"]].sort_values(
            by=['from', 'to'], ignore_index=True).reset_index(drop=True)
    
        time_formatting = round(time.time() - start_time, 3)
        if self.main_print_graph:
            print(f"Formatting completed in {time_formatting} seconds.\n")
            print("Step 4: Filtering with NPTM data...\n")
    
        # Step 4: Filter results with NPTM data
        columns = ['from', 'to', Graph.OPTIMISATION_METRIC, 'path']
        lf_nptm = NPTM(self.config).read_sql(
            self.nptm_table_name_extend, columns=columns
        ).lazy()
    
        lf_dijkstra = (
            pl.from_pandas(df_dijkstra)
            .with_columns([
                pl.col('from').cast(pl.Int16),
                pl.col('to').cast(pl.Int16),
                pl.col(Graph.OPTIMISATION_METRIC).cast(pl.Float32),
                pl.col('path').cast(pl.List(pl.Int16))
            ])
        ).lazy()
        del df_dijkstra # Free memory
        
        start_time = time.time()
        lf_optimisation = (
            lf_dijkstra.join(lf_nptm, on=['from', 'to'], how='left', suffix=("_nptm"))
            .filter(pl.col(Graph.OPTIMISATION_METRIC) < pl.col(f"{Graph.OPTIMISATION_METRIC}_nptm"))
            .with_columns([
                (pl.col('path').list.len() - 1).alias('nb_edges').cast(pl.Int8),  # Calculating number of edges
                pl.lit(DCT_TYPE['with-NTS']).alias('type').cast(pl.Int8)  # Add the 'type' column
            ])
            .select(
                pl.col('from'),
                pl.col('to'),
                pl.col('type'),
                pl.col(Graph.OPTIMISATION_METRIC),
                pl.col('nb_edges'),
                pl.col('path')
            )
        )
        
        optimisation = lf_optimisation.collect()
        time_filtering = round(time.time() - start_time)
    
        nptm_rows = lf_nptm.count().collect()[0, 0]
        new_rows = optimisation.count()[0, 0]
        
        del lf_optimisation; del lf_dijkstra; del lf_nptm # Free memory
        
        self.optimisation = optimisation
    
        if self.main_print_graph:
            print(f"NPTM filtering completed in {time_filtering} seconds.\n")
        else: print("Step 4: Results formatted.\n")
            
    
        print(
            f"Total improved links: {new_rows} / {nptm_rows}.\n"
            f"{to_engineering_notation(new_rows)} improved out of {to_engineering_notation(nptm_rows)} links.\n"
            f"Filtering and formatting time: {round(time.time() - start_time)} seconds."
        )
    
        # Step 5: Save results to the database (if required)
        if to_sql_optimisation:
            print("\nStep 5: Results storage.")
            try:
                self.to_sql_optimisation()
            except Exception as e:
                print(f"Error while saving results to the database: {e}")
    
        return self


    def to_sql_optimisation(self, *, if_exists: str = 'fail') -> None:
        """
        Saves the Dijkstra results to the database.
    
        Parameters
        ----------
        if_exists : str, optional
            Determines behavior if the tables already exist in the database (default is `'fail'`).  
            Options:  
                - `'fail'` : Raises an error if the table exists.  
                - `'replace'` : Drops and recreates the table.  
    
        Raises
        ------
        ValueError
            If `if_exists` is set to an invalid value (must be `'fail'` or `'replace'`).
        RuntimeError
            If the required schema does not exist. 
            Schema must be created using: `to_sql_edgelist()`.  
            If an error occurs while writing data to the database.  
    
        Returns
        -------
        None
            The function has no return value but writes the optimisation data to the database.
        
        Notes
        -----
        - The `adbc` engine is used for optimal writing performance with Polars DataFrames.
        - Paths are converted to a PostgreSQL-compatible array format before saving to the database.
        - The function modifies the data type of the `path` column in the database to:
            SMALLINT[] (array of smallint).
        - Adds a composite primary key on the columns ["from", "to"].
        """
        from transnetmap.utils.sql import schema_exists, execute_sql_script, execute_primary_key_script
        import time
    
        #  Step 1: Validate parameters
        if if_exists not in ['fail', 'replace']:
            raise ValueError(f"Invalid value for `if_exists`: {if_exists}. Use 'fail' or 'replace'.")
    
        schema = self.db_results_schema
        table_name = self.table_name_optimisation
    
        #  Step 2: Ensure schema exists
        if not schema_exists(self.uri, schema, print_status=self.config.main_print):
            raise RuntimeError("Schema does not exist.\n"
                               "Run 'to_sql_edgelist()' first.")
    
        print("Writing the optimisation results to the database begins.")
        
        #  Step 3: Validate the table before writing
        if self.optimisation.is_empty():
            raise RuntimeError(f"Attempted to write an empty optimisation table: {table_name}")
        
        #  Step 4: Apply column types dynamically
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'nb_edges': pl.Int8,
            'path': pl.List(pl.Int16),
        }
    
        self.optimisation = self.optimisation.with_columns(
            [pl.col(col).cast(dtype) for col, dtype in column_types.items()]
        )
    
        #  Step 5: Convert paths to PostgreSQL format
        path = self.optimisation['path'].to_pandas().apply(lambda x: convert_to_pg_array(x))
        path = pl.from_pandas(path)
        self.optimisation = self.optimisation.replace_column(-1, pl.Series('path', path)).sort(['from', 'to'])
    
        #  Step 6: Measure time for database write
        start_time = time.time()
    
        #  Step 7: Write table using ADBC engine
        try:
            self.optimisation.write_database(
                table_name=f"{schema}.{table_name}",
                connection=self.uri,
                engine="adbc",
                if_table_exists=if_exists
            )
        except Exception as e:
            raise RuntimeError(f"Error while writing to the database: {e}")
    
        #  Step 8: Adjust column type for PostgreSQL array
        script = f"""
        ALTER TABLE "{schema}"."{table_name}"
        ALTER COLUMN path TYPE SMALLINT[]
        USING string_to_array(trim(both '{{}}' from path), ',')::SMALLINT[];
        """
        try:
            execute_sql_script(self.uri, script, print_status=self.config.main_print)
        except Exception as e:
            raise RuntimeError(f"Error while executing SQL script for table '{table_name}': {e}")
    
        #  Step 9: Add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["from", "to"],
            schema=schema,
            include_schema_in_pk_name=True,
            print_status=self.config.main_print
        )
    
        #  Step 10: Log success message
        elapsed_time = round(time.time() - start_time)
        num_rows = self.optimisation.shape[0]
        print(f"Writing to the database is successful.\n"
              f"Table: '{table_name}'\n"
              f"Number of rows inserted: {num_rows}\n"
              f"Time taken: {elapsed_time} seconds.\n")
    
        return None


    def read_sql_optimisation(
        self,
        *,
        columns: Optional[list[str]] = None,
        where_condition: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Imports optimisation results from the database, with optional column selection and where condition.
    
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
            DataFrame containing the optimisation results.
                        
            - If called on an instance (``graph.read_sql_optimisation()``), the results are stored in ``self.optimisation``.
            - If called without assignment (`Graph(config).read_sql_optimisation()`), only the DataFrame is returned, 
              and the instance is not stored.
    
        Raises
        ------
        RuntimeError
            If the specified table does not exist in the database.  
            If the query returns an empty dataset.  
    
        Notes
        -----
        - Dynamically casts columns to appropriate types as defined in the `column_types` dictionary.
        - Allows for SQL filtering through the `where_condition` parameter.
        - Uses the `adbc` engine for optimal database querying.
        """       
        from transnetmap.utils.sql import table_exists, validate_columns, columns_exist
    
        #  Step 1: Define the schema and table
        schema = self.db_results_schema
        table_name = self.table_name_optimisation
    
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
            'nb_edges': pl.Int8,
            'path': pl.List(pl.Int16),
        }
        
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
            raise RuntimeError(f" Error reading data from database: {e}\nQuery: {sql_query}")
    
        #  Step 6: Check if the table is empty
        if table.is_empty():
            raise RuntimeError(f" Query returned an empty dataset for table: {table_name}")
    
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
    
        #  Step 9: Store the table in `self.optimisation`
        self.optimisation = table
    
        return table  #  Return the DataFrame instead of modifying the instance directly


# -----------------------------------------------------------------------------
# Main (example-only)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
        
    # Complete dictionary of creation and calculation parameters
    dct_param = {
        "network_number": 4,
        "physical_values_set_number": 2,
        "network_extension_type": "IMT",
        "main_print": False,
        "sql_echo": False,
        "db_nptm_schema": "nptm17",
        "db_zones_table": "zones17",
        "db_imt_table": "imt22",
        "db_pt_table": "pt22",
        "uri": "postgresql://username:password@host:port/database",
    }
    
    g0 = Graph(dct_param)
    # g0.create_edgelist()
    # g0.process_dijkstra()
    # g0.to_sql_edgelist()
    
    # g1 = Graph(dct_param)
    # g1.read_sql_edgelist()
    # g1._create_graph()
    
    # g2 = Graph(dct_param)
    # g2._create_graph()
    
    # g3 = Graph(dct_param)
    # g3.read_sql_edgelist()
    # g3._create_graph()
    # g3._create_graph(True)
    
    # newEdgeList = g0.edgelist[10000:30000]
    # g3._create_graph(newEdgeList)
    # g3.process_dijkstra(to_sql_optimisation=False)
    # g3.to_sql_optimisation(if_exists='fail')


    # Complet test for Graph and EdgeList classes
    
    # #### pvs 1 ###
    # dct_param = {
    #     "network_number": 4,
    #     "physical_values_set_number": 1,
    #     "network_extension_type": "IMT",
    #     "main_print": False,
    #     "sql_echo": False,
    #     "db_nptm_schema": "nptm17",
    #     "db_zones_table": "zones17",
    #     "db_imt_table": "imt22",
    #     "db_pt_table": "pt22",
    #     "uri": "postgresql://username:password@host:port/database",
    # }
    
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
    
    # g1 = Graph(dct_param).create_edgelist()
    # g1.to_sql_edgelist(comment_schema=com_schema_results)
    
    # g1.process_dijkstra()
    
    # #### pvs 2 ###
    # dct_param = {
    #     "network_number": 4,
    #     "physical_values_set_number": 2,
    #     "network_extension_type": "IMT",
    #     "main_print": False,
    #     "sql_echo": False,
    #     "db_nptm_schema": "nptm17",
    #     "db_zones_table": "zones17",
    #     "db_imt_table": "imt22",
    #     "db_pt_table": "pt22",
    #     "uri": "postgresql://username:password@host:port/database",
    # }
    
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
    
    # g2 = Graph(dct_param).create_edgelist()
    # g2.to_sql_edgelist(comment_schema=com_schema_results)
    
    # g2.process_dijkstra()
    
    # g = Graph(dct_param).read_sql_optimisation()
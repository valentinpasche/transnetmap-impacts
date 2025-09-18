# -*- coding: utf-8 -*-
"""
Post-processing utilities to compute and manage environmental/energy/financial impacts.

This module provides the class `Results`, a base class used by post-processing tools
(e.g., ``HeatMap``) to add and manage environmental/energy/financial impacts over the
network. It coordinates:
- validation of zones,
- impact-table loading and consistency checks,
- per-zone partial network computation (```results_{id_zone}```),
- SQL I/O (read/write) of results.

Features:

- Load impact factors from PVS tables and keep an internal registry.
- Detect and reconcile differences between DB state and expected impacts.
- Compute per-zone partial networks with impacts applied.
- Persist/reload results in PostgreSQL (```results_{id_zone}```).

Workflow (high level):

1. Optimise and persist the base network (``edgelist`` / ``optimisation``).
2. Load/validate impact tables and determine missing/inconsistent columns.
3. Update the DB (``edgelist``) with required impact columns.
4. Compute a per-zone partial network and save as ```results_{id_zone}```.

Tables & Schemas:

- Results schema: ``analysis_data.db_results_schema``.
- Per-zone table: ```results_{id_zone}```.
- Uses ADBC for reading/writing Polars DataFrames.
"""

from __future__ import annotations

from typing import Optional, Union

import polars as pl

from transnetmap.utils.config import ParamConfig
from transnetmap.analysis.graph import Graph

__all__ = ["Results"]


# -----------------------------------------------------------------------------
# Class Definition: Results
# -----------------------------------------------------------------------------
class Results:
    """
    **Base class for computing and managing environmental, energy and financial impacts in a transport network.**

    This class is responsible for:
        
    - Managing **impact calculations** for network analysis.
    - Handling **partial networks** and their respective impact computations.
    - Providing **database utilities** for reading and writing results.

    The class is **not meant to be used directly**,
    but serves as a **parent class** for specialized implementations such as `HeatMap()`.  

    **Main Responsibilities:**

    - **Impact Management:** Loads, updates, and applies impact factors to network edges.
    - **Partial Network Processing:** Computes impact values for a given zone (`id_zone`).
    - **Database Integration:** Reads/writes results from/to a PostgreSQL database.
    - **Consistency Checks:** Ensures data consistency between `edgelist`, `optimisation`, and ``results_{id_zone}``.

    **Typical Usage:**
    
    - `HeatMap()` uses this class to generate impact results for a given `id_zone`.
    - `replace_all_impacts_in_db()` is used for a **global refresh** of all impact tables.
    
    **Core Workflow:**

    1. **Network Optimization:** The base transport network is optimized and stored in `edgelist` & `optimisation`.
    2. **Impact Calculation:** The impacts are added to `edgelist` and later used for computing partial networks.
    3. **Partial Network Computation:** A zone-specific results table (``results_{id_zone}``) is generated.
    4. **Database Updates:** The computed impacts are written back into the database.

    **Methods Used by HeatMap():**

    - `validate_id_zone(id_zone)`: Ensures the zone ID exists in the database.
    - `prepare_partial_network(id_zone)`: Checks whether a new partial network needs to be computed.
    - `to_sql_partial_network(if_exists='fail')`: Writes the computed network to the database.

    **Methods Used for Full Impact Refresh:**

    - `replace_all_impacts_in_db()`: Deletes and recomputes all impact tables.
    - `_update_edgelist_in_db()`: Updates or replaces impact columns in `edgelist`.

    **Key Attributes:**

    *id_valid* : `bool` (read-only)  
    
    * Indicates whether the current `id_zone` has been validated.
        
    *id_zone* : `int` (read-only)  
    
    * The validated zone ID (set after calling `validate_id_zone()`).
        
    *table_name_partial_network* : `str` (read-only)  
    
    * The name of the results table corresponding to the current `id_zone`.
        
    *impacts_statut* : `dict`  
        Tracks the state of impact calculations and loaded tables:  
            
    - `"default"`: Full list of configured impacts.  
    - `"loaded"`: Impacts found in the database.  
    - `"update"`: Impacts that require calculation.  
    - `"current"`: Impacts currently present in `edgelist`.  
    - `"missing"`: Impacts that were expected but not found in the database.  
    - `"inconsistent"`: Impacts present in `edgelist` but missing from impact tables.  

    **Methods:**
    
    - `replace_all_impacts_in_db()`  
        Deletes and recomputes all impact tables, ensuring consistency across all results.
    
    - `validate_id_zone(id_zone)`  
        Validates whether the given `id_zone` exists in the database.
        
    - `prepare_partial_network(id_zone)`  
        Checks if ``results_{id_zone}`` exists and updates or recalculates it if needed.
        
    - `to_sql_partial_network(if_exists='fail')`  
        Writes the computed ``results_{id_zone}`` table to the database.
        
    - `read_sql_partial_network(id_zone, columns=None, where_condition=None)`  
        Loads an existing ``results_{id_zone}`` table from the database.

    **Examples**
    -------------
    **Using HeatMap() for a single zone**
    
    ```python
    heatmap = HeatMap(config, id_zone=123, zone_label="label", from_zome=True)
    ```
    
    - This automatically calls `validate_id_zone(123)` and `prepare_partial_network(123)`.
    
    **Replacing all impact tables (full refresh)**
    
    ```python
    results = Results(config)
    results.replace_all_impacts_in_db()
    ```
    - This resets **all impact tables** and recomputes them from scratch.

    **Checking if a partial network already exists**
    
    ```python
    results = Results(config)
    results.validate_id_zone(123)
    results.prepare_partial_network(123)  # If no update needed, nothing happens.
    ```
    """

    def __init__(self, param: Union[dict, ParamConfig], *, required_fields: Optional[list] = None) -> None:
        """
        Initializes the Results instance with specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
                
            - `"network_number"` : int  
                Identification number for the network instance.
            - `"physical_values_set_number"` : int  
                Identification number for the physical value set.
            - `"network_extension_type"` : str  
                Transport type identifier for network extension.
            - `"db_nptm_schema"` : str  
                Name of the schema containing the National Passenger Traffic Model (NPTM) data.
            - `"db_zones_table"` : str  
                Name of the table containing traffic zones data.
            - `"db_imt_table"` : str  
                Name of the table containing travel time and distance data for individual motorized transport (IMT).
            - `"db_pt_table"` : str  
                Name of the table containing travel time and distance data for public transport (PT).
            - `"uri"` : str  
                PostgreSQL database connection string.

            Optional keys:
                
            - `"main_print"` : bool  
                Enables console output for execution status. Default is False.
            - `"sql_echo"` : bool  
                Enables SQL query logging. Default is False.

        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided, defaults to
            `["network_number", "physical_values_set_number", "network_extension_type",  
             "db_nptm_schema", "db_zones_table", "db_imt_table", "db_pt_table", "uri"]`.

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration or `network_extension_type` has an invalid value.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `Results`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        #  Step 1: Define required fields for Results (default)
        default_required_fields = ["network_number", "physical_values_set_number", 
                                   "network_extension_type", "db_nptm_schema", 
                                   "db_zones_table", "db_imt_table", "db_pt_table", 
                                   "uri"]

        # Use custom required fields if provided, otherwise use the default ones
        required_fields = required_fields or default_required_fields

        #  Step 2: Validate the parameters
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

        #  Step 3: Extract commonly used attributes
        self.network_number = self.config.network_number
        self.name_pvs = f'pvs{self.config.physical_values_set_number}'
        self.network_extension_type = self.config.network_extension_type
        
        #  Step 4: Extract database-related attributes
        self.db_nptm_schema = self.config.db_nptm_schema
        self.db_zones_table = self.config.db_zones_table          
        self.db_imt_table = self.config.db_imt_table
        self.db_pt_table = self.config.db_pt_table
        self.uri = self.config.uri

        #  Step 5: Initialize the Graph instance (used for optimization parameters)
        self.analysis_data = Graph(self.config)
        self.db_results_schema = self.analysis_data.db_results_schema # Schema for results storage        
    
        #  Step 6: Execution context settings
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo
        
        #  Step 7: Initialize placeholders for processing
        self.dct_impacts_instances = None  # Stores impact data instances
        self.impacts_statut = None  # Tracks impact computation statuses
        
        # Stores results for the current `id_zone`
        self.table = None  # Holds ``results_{id_zone}`` after impact calculations
        self.optimisation_updated_table = None  # Stores processed `optimisation` table before writing
        
        # Read-only propertys for ID zone
        self._id_zone = None  # Stores the current zone ID when working on partial networks
        self._id_valid = None  # Indicates if the current `id_zone` has been validated
        self._table_name_partial_network = None  # Will be defined dynamically when processing a zone

        #  Step 8: Log successful initialization
        if self.main_print:
            print(f"Results class initialized for network_number: {self.network_number}, pvs: {self.name_pvs}")


    # -----------------------------------------------------------------------------
    # Logging Utility
    # -----------------------------------------------------------------------------
    def _log(self, message: str) -> None:
        """Handles conditional logging based on the `main_print` flag."""
        if self.main_print:
            print(message)


    # -----------------------------------------------------------------------------
    # Read-Only Properties
    # -----------------------------------------------------------------------------
    @property
    def id_valid(self) -> bool:
        """Read-only property: checks if the zone ID is valid."""
        return self._id_valid

    @property
    def id_zone(self) -> int:
        """Read-only property: returns the validated ID zone."""
        if not self._id_valid:
            raise AttributeError("`id_zone` is not validated. Use validate_id_zone() first.")
        return self._id_zone

    @property
    def table_name_partial_network(self) -> str:
        """Read-only property: returns the table name of the validated zone."""
        if not self._id_valid:
            raise AttributeError("Table_name_partial_network is not set. Use validate_id_zone() first.")
        return self._table_name_partial_network


    # -----------------------------------------------------------------------------
    # Public Methods
    # -----------------------------------------------------------------------------
    def replace_all_impacts_in_db(self) -> None:
        """
        Replace impact columns in the database for all existing tables.
    
        This method performs the following steps:  
            
        1. Replaces impact columns in the `edgelist` table by calling `_update_edgelist_in_db(force_replace=True)`.  
        2. Retrieves all zone IDs (`id_zone`) associated with ``results_{id_zone}`` tables in the database.  
        3. Replaces each ``results_{id_zone}`` table by recalculating impacts and saving the updated table.  
    
        Returns
        -------
        None
            This method does not return a value. It modifies the database by updating all impact tables.
    
        Raises
        ------
        RuntimeError
            If no existing ``results_{id_zone}`` tables are found in the database.  
            If the `Results` instance is not in a clean state before execution.  
            If the impacts update fails due to a database or consistency error.  
    
        Notes
        -----
        - This method is irreversible and will overwrite existing tables.
        - User confirmation is required before proceeding with the operation.
        - The instance of `Results` must be clean before execution.
    
        Examples
        --------
        >>> results = Results(config)
        >>> results.replace_all_impacts_in_db()
        """
        from transnetmap.utils.sql import execute_sql_script
    
        #  Step 1: Ensure the `Results` instance is in a clean state
        if self._id_valid is not None:
            raise RuntimeError(
                "The `Results` instance must not be linked to a specific `id_zone` "
                "before running this method."
            )
    
        if self.table is not None or self.optimisation_updated_table is not None:
            raise RuntimeError(
                "The `Results` instance must be clean. Ensure that `self.table` and "
                "`self.optimisation_updated_table` are `None` before execution."
            )
    
        #  Step 2: Confirm user intention
        confirmation = input(
            "This operation will replace impact columns in the database for all tables, "
            "including edgelist and results_{id_zone}.\nAre you sure you want to proceed? (y/n): "
        )
        if confirmation.lower() != "y":
            print("Operation canceled.")
            return
    
        #  Step 3: Replace impacts in the `edgelist` table
        print("Replacing impacts in the `edgelist` table...")
        self._update_edgelist_in_db(force_replace=True)
        print("Impacts replaced successfully in the `edgelist` table.\n")
    
        #  Step 4: Retrieve all existing ``results_{id_zone}`` tables
        print("Retrieving existing ``results_{id_zone}`` tables...")
        schema = self.db_results_schema
        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name LIKE 'results_%';
        """
        result = execute_sql_script(self.uri, query, fetch_all=True)
        
        if not result:
            raise RuntimeError("No existing ``results_{id_zone}`` tables found in the database.")
    
        #  Step 5: Extract id_zones from table names safely
        id_zones = []
        for row in result:
            table_name = row[0]
            if table_name.startswith("results_"):
                try:
                    id_zone = int(table_name.split('_')[1])
                    id_zones.append(id_zone)
                except ValueError:
                    print(f"Skipping invalid table name: {table_name}")
    
        if not id_zones:
            raise RuntimeError("No valid ``results_{id_zone}`` tables found in the database.")
    
        print(f"Found {len(id_zones)} tables: {', '.join([f'results_{id}' for id in id_zones])}\n")
    
        #  Step 6: Extract tables and parameters
        print("Loading edgelist table from the database...")
        current_impacts = self.impacts_statut["loaded"]  #  Use only impacts that exist in the database
        edgelist_columns = ["from", "to", "length"] + current_impacts
        edgelist_table = Graph(self.config).read_sql_edgelist(columns=edgelist_columns)
    
        # Validate edgelist table
        if edgelist_table.is_empty():
            raise RuntimeError(
                "The edgelist table is empty. Ensure that impacts have been calculated correctly."
            )
    
        check_invalid = edgelist_table.null_count()
        for col in check_invalid.columns:
            if not check_invalid.select(col).filter(pl.col(col) > 0).is_empty():
                raise RuntimeError("The edgelist table contains inconsistent values (polars.Null).")
    
        print(f"Edgelist table loaded successfully. Shape: {edgelist_table.shape}\n")
    
        #  Step 7: Replace each ``results_{id_zone}`` table
        for id_zone in id_zones:
            print(f"Processing table for zone ID: {id_zone}...")
    
            # Compute the new results table
            self._log(f"Recomputing impacts for zone {id_zone}...")
            self._process_partial_network(id_zone, edgelist_table.clone(), current_impacts)
    
            #  Temporarily set `_id_valid` to allow `table_name_partial_network` access
            self._id_zone = id_zone
            self._id_valid = True
            self._table_name_partial_network = f"results_{id_zone}"
    
            # Save the updated table to the database
            self.to_sql_partial_network(if_exists='replace')
            print(f"Table ``results_{id_zone}`` updated successfully.\n")
    
        #  Step 8: Clean up instance variables after execution
        self._id_valid = None  # Reset validation
        self._id_zone = None  # Reset the zone ID
        self._table_name_partial_network = None  # Ensure the property is reset
        self.table = None
        self.optimisation_updated_table = None
    
        #  Step 9: Summary log
        print("All impact tables have been replaced successfully.\n")
        print(f"Tables recreated in schema `{schema}`:")
        print("    - edgelist")
        for id_zone in id_zones:
            print(f"    - results_{id_zone}")
    
        print("\nThe ``edgelist`` table was updated.")
        print(f"A total of {len(id_zones)} zone tables were updated.\n")


    def validate_id_zone(self, id_zone: int) -> bool:
        """
        Validates the existence of a zone identifier in the NPTM zones table.
        
        Ensures that the provided `id_zone` exists in the database, checking against the
        schema and table defined in the configuration. If the schema, table, or `id_zone`
        is invalid, appropriate errors are raised.
        
        Parameters
        ----------
        id_zone : int
            The zone identifier to validate.
        
        Returns
        -------
        bool
            True if the `id_zone` exists in the database, otherwise raises an error.
        
        Raises
        ------
        ValueError
            If the schema does not exist in the database.  
            If the table does not exist in the specified schema.  
            If the `id_zone` is not found in the table.  
        RuntimeError
            If an error occurs during the SQL query execution.
        
        Notes
        -----
        - This method assumes the schema and table names are stored as attributes 
          `self.db_nptm_schema` and `self.db_zones_table` respectively.
        - The method uses parameterized SQL queries to prevent SQL injection.
        - If `self._id_valid` is already `True` and `self._id_zone == id_zone`, 
          no validation is performed to prevent unnecessary database queries.
        """
        from transnetmap.utils.sql import execute_sql_script, schema_exists, table_exists
        
        #  Step 1: Avoid unnecessary validation
        if self._id_valid and self.id_zone == id_zone:
            self._log(f"ID Zone {id_zone} is already validated.")
            return True  
        
        #  Step 2: Validate schema and table existence
        schema = self.db_nptm_schema
        table = self.db_zones_table
    
        if not schema_exists(self.uri, schema):
            raise ValueError(f"Schema '{schema}' does not exist in the database.")
        if not table_exists(self.uri, table):
            raise ValueError(f"Table '{table}' does not exist in schema '{schema}'.")
        
        #  Step 3: Check if the id_zone exists in the table
        script_id = f'''SELECT EXISTS (SELECT 1 FROM "{schema}"."{table}" WHERE id = %s);'''
        result = execute_sql_script(self.uri, script_id, params=(id_zone,))
        
        #  Step 4: Process result and update attributes
        if result and result[0]:
            self._id_valid = True
            self._id_zone = id_zone
            self._table_name_partial_network = f"results_{self.id_zone}"
            self._log(f"The ID Zone {id_zone} is valid and set.")
            return True  #  Returns True if all is OK
            
        #  If the ID is not found, raise an error
        self._id_valid = False
        raise ValueError(
            f"Invalid NPTM zones ID: {id_zone}\n"
            f"Use `Network().show_all()` to check the whole network."
        )
        
        return self._id_valid


    def prepare_partial_network(self, id_zone: int) -> None:
        """
        Prepares the partial network for the given `id_zone`.
    
        This function:
            
        1. **Validates the zone ID** (`validate_id_zone()`).  
        2. **Determines if `edgelist` needs updates** (`_update_edgelist_in_db()`),
           and updates only if necessary.  
        3. **Checks if the ``results_{id_zone}`` table is already complete**.  
               - If complete, no further processing is needed.  
               - Otherwise, the table is recalculated and saved.  
    
        Parameters
        ----------
        id_zone : int
            The zone ID for which to prepare the partial network.
    
        Returns
        -------
        None
            This method does not return a value. If execution completes, ``results_{id_zone}`` is ready.
    
        Raises
        ------
        ValueError
            If `id_zone` is invalid or missing.
        RuntimeError
            If required inputs (edgelist/optimisation) are missing or a DB error occurs.
        """
        self._log(f"Preparing partial network for zone ID: {id_zone}")
    
        #  Step 1: Validate the ID zone
        self.validate_id_zone(id_zone)
    
        #  Step 2: Determine if `edgelist` needs updates and update if necessary
        self._update_edgelist_in_db()
    
        #  Step 3: Check if ``results_{id_zone}`` is already complete
        table_exists, columns_complete = self._validate_existing_partial_network(
            self.impacts_statut["current"]
        )
        
        if table_exists and columns_complete:
            self._log(f"``results_{id_zone}`` is already up-to-date. No recalculation needed.")
            return None  # Early exit
        
        #  Step 4: Extract required data
        current_impacts = self.impacts_statut["current"]
        edgelist_columns = ["from", "to", "length"] + current_impacts
        edgelist_table = Graph(self.config).read_sql_edgelist(columns=edgelist_columns)
    
        #  Step 5: Compute the partial network
        self._log(f"Computing ``results_{id_zone}``...")
        self._process_partial_network(id_zone, edgelist_table, current_impacts)
    
        #  Step 6: Determine the appropriate `if_exists` behavior
        if table_exists and not columns_complete:
            if_exists_mode = "replace"
            self._log(f"The existing table ``results_{id_zone}`` has missing impact columns. "
                      "Switching `if_exists`: from 'fail' to 'replace'.")
        else:
            if_exists_mode = "fail"
    
        #  Step 7: Save the updated table to the database
        self.to_sql_partial_network(if_exists=if_exists_mode)
    
        self._log(f"Partial network `{id_zone}` successfully processed and saved.")
        return None


    # -----------------------------------------------------------------------------
    # Validation Methods
    # -----------------------------------------------------------------------------
    def _validate_existing_partial_network(self, impact_columns: list[str]) -> tuple[bool, bool]:
        """
        Checks if the partial network table (``results_{id_zone}``) already exists 
        and whether it contains all required impact columns.
    
        Parameters
        ----------
        impact_columns : list[str]
            List of expected impact columns to verify in the table.
    
        Returns
        -------
        tuple[bool, bool]
            - First value (`table_exists`): `True` if the table exists, `False` otherwise.
            - Second value (`columns_complete`): `True` if all required columns are present, `False` otherwise.
    
        Raises
        ------
        ValueError
            If `id_zone` has not been validated before calling this method.
        """
    
        from transnetmap.utils.sql import table_exists, columns_exist
    
        #  Step 1: Ensure the zone ID is validated
        if not self._id_valid:
            raise ValueError("The zone ID must be validated before checking ``results_{id_zone}``.")
    
        #  Step 2: Check if the table exists
        schema = self.db_results_schema
        table_name = self.table_name_partial_network
    
        self._log(f"Checking if the table '{table_name}' exists in schema '{schema}'...")
        table_exists_flag = table_exists(self.uri, table_name, print_status=self.main_print)
    
        if not table_exists_flag:
            self._log(f"Table '{table_name}' does not exist. A new calculation is required.")
            return False, False  #  Table missing, needs full recalculation
    
        #  Step 3: Verify that all required columns exist
        required_columns = ["from", "to", "type", "time", "length", "nb_edges", "path"] + impact_columns
        existing_columns = columns_exist(
            self.uri, required_columns, table_name, schema, print_status=self.main_print
        )
    
        missing_columns = [col for col, exists in existing_columns.items() if not exists]
        if missing_columns:
            self._log(f"Table '{table_name}' is missing columns: {', '.join(missing_columns)}. "
                      "A recalculation is required."
            )
            return True, False  # Table exists but missing impacts
    
        self._log(f"Table '{table_name}' is up-to-date. No recalculation needed.")
        return True, True  # Everything is fine


    # -----------------------------------------------------------------------------
    # Impact Management Methods
    # -----------------------------------------------------------------------------
    def _load_impacts(self, impacts_list: Optional[list[str]] = None) -> Results:
        """
        Loads instances of PVS_Impacts for all available or specified impact types.
        
        This method verifies the existence of impact tables in the database and initializes
        instances of `PVS_Impacts` for each valid table. The loaded instances, including their 
        data, are stored in `self.dct_impacts_instances`. The loading status of impacts is 
        tracked in the `self.impacts_statut` dictionary.
        
        Parameters
        ----------
        impacts_list : list of str, optional
            A list of impact types to load (e.g., ["CO2", "EP", "TCO"]). If ``None``, all available
            impact types are loaded as defined in the global `impacts_list`.
        
        Returns
        -------
        Results
            The current instance with the attribute `self.dct_impacts_instances` populated with 
            instances of `PVS_Impacts` and the `self.impacts_statut` dictionary updated.
        
        Raises
        ------
        RuntimeError
            If no valid impact tables are found in the database or if the `impacts_list` is invalid.
        
        Notes
        -----
        - The method checks for the existence of each impact table in the database.
        - Missing impacts are logged in `self.impacts_statut['missing']`, but the method continues
          loading available ones.
        - Loaded impacts are automatically validated and processed via the `read_sql` method.
        
        Examples
        --------
        >>> results = Results(config)
        >>> results._load_impacts(impacts_list=["CO2", "EP"])
        >>> print(results.dct_impacts_instances.keys())
        ['CO2', 'EP']
        """
        from transnetmap.pre.pvs import PVS_Impacts
        from transnetmap.utils.constant import IMPACTS
        from transnetmap.utils.sql import table_exists
    
        #  Step 1: Reset impact instances and tracking
        self.dct_impacts_instances = {}
        self.impacts_statut = {"default": sorted(IMPACTS), "loaded": [], "missing": []}
    
        #  Step 2: Validate the provided impacts_list
        if impacts_list:
            invalid_impacts = [impact for impact in impacts_list if impact not in IMPACTS]
            if invalid_impacts:
                raise RuntimeError(
                    f"Invalid impacts provided: {', '.join(invalid_impacts)}.\n"
                    f"Allowed impacts: {', '.join(IMPACTS)}."
                )
        else:
            impacts_list = IMPACTS
    
        #  Step 3: Load impacts from the database
        for impact in sorted(impacts_list):
            impact_instance = PVS_Impacts(self.config, impact)
            
            if table_exists(self.uri, impact_instance.table_name, print_status=self.main_print):
                self.dct_impacts_instances[impact] = impact_instance.read_sql()
                self.impacts_statut["loaded"].append(impact)
            else:
                self.impacts_statut["missing"].append(impact)
    
        #  Step 4: Raise an error if no impacts are loaded
        if not self.dct_impacts_instances:
            raise RuntimeError(
                "No impact tables are defined in the database.\n"
                f"Missing impacts: {', '.join(self.impacts_statut['missing'])}.\n"
                'Ensure these tables are created in the schema: "physical_values".'
            )
    
        #  Step 5: Print loaded and missing impacts for debug purposes
        if self.main_print:
            print(f"Loaded impact instances: {', '.join(self.impacts_statut['loaded'])}")
            if self.impacts_statut["missing"]:
                print(f"Missing impact tables: {', '.join(self.impacts_statut['missing'])}")
    
        return self


    def _determine_impacts_to_update(self, force_replace: bool = False) -> bool:
        """
        Determines which impact columns need to be updated in the ``edgelist`` table.
    
        This method identifies differences between:
        - The columns present in the ``edgelist`` table stored in the database.
        - The available impact tables in the database.
    
        This method does **not** modify the ``edgelist`` table; it only detects discrepancies 
        and determines whether a recalculation is necessary.
    
        Parameters
        ----------
        force_replace : bool, optional
            Defines how impact updates should be handled:
            - `False`: Only add missing impact columns without modifying existing ones (default).
            - `True`: Recalculate all impact columns and overwrite existing values.
    
        Returns
        -------
        bool
            - `True` if updates are required (some impacts need to be added/recomputed).
            - `False` if no updates are needed (all required impacts are already in the database).
    
        Raises
        ------
        RuntimeError
            If the ``optimisation`` or ``edgelist`` tables do not exist in the database.
            If inconsistencies are found in `edgelist`, requiring a full reset.
    
        Notes
        -----
        - The method first loads available impact instances via `_load_impacts()`.
        - It then verifies which impacts are already in the database and updates `self.impacts_statut["current"]`.
        - If all necessary impacts are already present, `self.impacts_statut["update"]` is set to `[]`, 
          and the function returns `False`.
        - Otherwise, `self.impacts_statut["update"]` is populated with the impacts that require processing.
        - If a column exists in `edgelist` but its corresponding impact table is missing, 
          the method raises an error to prevent inconsistencies.
        - This method does **not** modify `edgelist`, it only determines the update requirements.
    
        Examples
        --------
        >>> results = Results(config)
        >>> needs_update = results._determine_impacts_to_update(force_replace=False)
        No impacts need to be updated. Current impacts in DB: CO2, EP
        >>> print(needs_update)
        False  # No update needed
    
        >>> needs_update = results._determine_impacts_to_update(force_replace=True)
        Impacts to update: CO2, EP
        >>> print(needs_update)
        True  # Updates required
        """
        from transnetmap.utils.sql import table_exists, columns_exist
        from transnetmap.utils.constant import IMPACTS
    
        #  Step 1: Ensure `optimisation` table exists in the database
        schema = self.db_results_schema
        table_name_optimisation = self.analysis_data.table_name_optimisation
    
        self._log(f"Checking if table '{table_name_optimisation}' exists in schema '{schema}'...")
        if not table_exists(self.uri, table_name_optimisation, print_status=self.main_print):
            raise RuntimeError(
                f"Table '{schema}.{table_name_optimisation}' does not exist.\nEnsure the "
                "new network has been optimised and recorded before post-processing the results."
            )
    
        #  Step 2: Load impact tables
        self._load_impacts()
    
        if not self.impacts_statut["loaded"]:
            raise RuntimeError(
                "No impact tables were successfully loaded. Cannot proceed with update determination."
            )
    
        #  Step 3: Identify missing impact columns in `edgelist`
        table_name_edgelist = self.analysis_data.table_name_edgelist
        self._log(f"Identifying missing impact columns in '{table_name_edgelist}'...")
        
        columns_exist_in_db = columns_exist(
            self.uri, IMPACTS, table_name_edgelist, schema, print_status=self.main_print
        )
        
        # If `edgelist` has never had impact columns before, all impacts are considered missing.
        missing_columns = [col for col, exists in columns_exist_in_db.items() if not exists]
        self.impacts_statut["current"] = sorted(
            [col for col, exists in columns_exist_in_db.items() if exists]
        )
        
        #  Step 4: Identify suppressed impacts
        inconsistent_columns = [
            col for col in self.impacts_statut["current"] if col not in self.impacts_statut["loaded"]
        ]
        self.impacts_statut["inconsistent"] = inconsistent_columns
        
        if inconsistent_columns and not force_replace:
            raise RuntimeError(
                "The following impacts exist in ``edgelist`` but have no corresponding database table: "
                f"{', '.join(inconsistent_columns)}.\nExecute `replace_all_impacts_in_db()` to "
                "reset all impact tables and restore consistency.\n    -> "
                "`replace_all_impacts_in_db()` must be executed directly with a clean `Results` instance."
            )
    
        #  Step 5: Determine which impacts need updates
        if force_replace:
            columns_to_update = self.impacts_statut["loaded"]
        else:
            columns_to_update = [col for col in self.impacts_statut["loaded"] if col in missing_columns]
    
        #  Step 6: Populate `self.impacts_statut["update"]` and log results
        self.impacts_statut["update"] = sorted(columns_to_update)
    
        if not columns_to_update:
            self._log("No impacts need to be updated. Current impacts in DB: "
                      f"{', '.join(self.impacts_statut['current'])}"
            )
            return False  # No update needed
        else:
            self._log(f"Impacts to update: {', '.join(columns_to_update)}")
    
        if self.main_print and self.impacts_statut["missing"]:
            print(f"Missing impact tables: {', '.join(self.impacts_statut['missing'])}")
    
        return True  #  Return True if updates are needed


    def _update_edgelist_in_db(self, force_replace: bool = False) -> dict:
        """
        Updates the ``edgelist`` table in the database with new impact columns.
    
        This method calculates and updates impact values (e.g., CO2, EP, TCO) for the ``edgelist`` table.
        It ensures the consistency of the database table with the current configuration by either 
        recalculating all impacts (`force_replace=True`) or only adding missing impacts (`force_replace=False`).
    
        The `edgelist` table is **entirely replaced** in all cases where an update is needed.
        This approach is used for performance reasons, as replacing the table is **faster**
        than executing an SQL query to add missing columns.
    
        Parameters
        ----------
        force_replace : bool, optional
            - `False` (default): Calculate only missing impact columns and skip existing ones.
            - `True`: Recalculate all impact columns, replacing existing values.
    
        Returns
        -------
        dict
            Summarizes the status of the operation with the following keys:
            - `"default"`: Full list of impacts defined in the configuration.
            - `"loaded"`: Impacts successfully loaded from the database.
            - `"update"`: Impacts that were calculated and added/updated.
            - `"missing"`: Impacts that could not be calculated due to missing tables.
            - `"current"`: Final list of impact columns now present in `edgelist` after the update.
    
        Raises
        ------
        RuntimeError
            If the ``edgelist`` table contains inconsistent values (NULLs) before calculations.
    
        Notes
        -----
        - This method calls `_add_impacts_on_edge()` to calculate impact values.
        - If `force_replace=False`, only missing impact columns are added, while existing ones remain unchanged.
        - If `force_replace=True`, all impact columns are recalculated and **the `edgelist` table is completely replaced**.
        - If no new impacts are required, the method terminates early without modifying the database.
        - Missing impact tables are logged and reported but do not prevent processing of available impacts.
    
        Examples
        --------
        Recalculate all impact columns and update the ``edgelist`` table:
        >>> results = Results(config)
        >>> impacts_status = results._update_edgelist_in_db(force_replace=True)
        >>> print("Updated impacts:", impacts_status["update"])
    
        Add only missing impact columns without modifying existing ones:
        >>> results = Results(config)
        >>> impacts_status = results._update_edgelist_in_db(force_replace=False)
        >>> print("Current impacts in DB:", impacts_status["current"])
        """
        #  Step 1: Determine which impact columns need updates
        self._log("Determining which impact columns need updates...")
        needs_update = self._determine_impacts_to_update(force_replace=force_replace)
        
        if not needs_update:
            self._log("No updates needed. All impact columns are already in place.")
            return self.impacts_statut
    
        columns_to_update = self.impacts_statut["update"]
        self._log(f"Updating the following impact columns in ``edgelist``: {', '.join(columns_to_update)}")
    
        #  Step 2: Load the edgelist table with only valid impact columns
        valid_columns = [
            "from", "to", "type", "time", "length"
        ] + [
            impact for impact in self.impacts_statut["current"] if impact not in self.impacts_statut["inconsistent"]
        ]
        
        self._log(f"Loading ``edgelist`` with valid columns: {valid_columns}")
        edgelist = Graph(self.config).read_sql_edgelist(columns=valid_columns)
    
        #  Step 3: Validate that no NULL values exist in the table before calculations
        check_invalid = edgelist.null_count()
        for col in check_invalid.columns:
            if not check_invalid.select(col).filter(pl.col(col) > 0).is_empty():
                raise RuntimeError(
                    "The table used to calculate impacts contains inconsistent values (polars.Null)."
                )
    
        #  Step 4: Calculate missing impact values (returns only new impact columns)
        self._log("Calculating missing impact values...")
        updated_columns = self._add_impacts_on_edge(edgelist, columns_to_update)
    
        #  Step 5: Merge new impact columns with existing edgelist
        updated_edgelist = edgelist.join(
            updated_columns,
            on=['from', 'to'],
            validate="1:1"
        ).sort(['from', 'to'])
    
        #  Step 6: Define impact columns and sort them
        if force_replace:
            current_impacts = sorted(self.impacts_statut["update"])  # Takes only updated columns
        else:
            current_impacts = sorted(list(set(self.impacts_statut["current"]) | set(columns_to_update)))  # Normal case
    
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
        }
        for impact in current_impacts:
            column_types[impact] = pl.Float32
    
        updated_edgelist = updated_edgelist.select(
            ["from", "to", "type", "time", "length", *current_impacts]).with_columns(
            [pl.col(col).cast(dtype) for col, dtype in column_types.items() if col in updated_edgelist.columns]
        ).sort(['from', 'to'])
    
        #  Step 7: Save the updated table to the database
        self._log("Saving updated ``edgelist`` to the database...")
        self.analysis_data.edgelist = updated_edgelist
        self.analysis_data.to_sql_edgelist(if_exists='replace')
    
        #  Step 8: Update `self.impacts_statut["current"]` to reflect the new state
        self.impacts_statut["current"] = current_impacts
        self._log(f"Successfully updated ``edgelist`` with impacts: {', '.join(columns_to_update)}")
    
        return self.impacts_statut


    # -----------------------------------------------------------------------------
    # Impact Calculation Methods
    # -----------------------------------------------------------------------------
    def _add_impacts_on_edge(self, table: pl.DataFrame, columns_to_update: list[str]) -> pl.DataFrame:
        """
        Adds calculated impact values (e.g., CO2, EP, TCO) to a given edgelist table (e.g., edgelist, NPTM tables).
        
        This method computes environmental and energy impacts for each relation in 
        the provided table based on preloaded impact tables (`self.dct_impacts_instances`). 
        Each impact type is processed independently, applying impact values as new columns.
    
        The returned table only contains the relations (`from`, `to`) along with the newly computed 
        impact columns. Other columns present in the input table are **not included** in the output.
    
        Parameters
        ----------
        table : polars.DataFrame
            The input table containing the relations to which impact values will be added.
            Must include the columns ["from", "to", "type", "length"].
        columns_to_update : list[str]
            List of impact types to be processed and added to the table.
    
        Returns
        -------
        polars.DataFrame
            A table containing only the relations (`from`, `to`) and the newly computed impact columns,
            (e.g., columns ["from", "to", "CO2", "EP"]).
    
        Raises
        ------
        ValueError
            If required columns are missing from the input table.
        RuntimeError
            If null values are detected in the final result table.
    
        Notes
        -----
        - This method assumes that `self.dct_impacts_instances` contains validated impact data.
        - Each impact type is computed by matching rows based on `type` and `length` (joined to `max_distance`).
        - The method respects `columns_to_update` to determine which impacts to process.
        - It ensures that all calculated impacts are included and free of inconsistencies.
        
        Examples
        --------
        >>> table = pl.DataFrame({
        ...     "from": [1, 2],
        ...     "to": [3, 4],
        ...     "type": [1, 2],
        ...     "time": [30.4, 53.1],
        ...     "length": [10.5, 20.0]
        ... })
        >>> results._add_impacts_on_edge(table, columns_to_update=["CO2", "EP"])
        >>> print(table)
        shape: (2, 4)
        ┌──────┬──────┬────────┬────────┐
        │ from │ to   │ CO2    │ EP     │
        │ ---  │ ---  │ ---    │ ---    │
        │ i64  │ i64  │ f32    │ f32    │
        ╞══════╪══════╪════════╪════════╡
        │ 1    │ 3    │ 15.75  │ 8.5    │
        │ 2    │ 4    │ 16.0   │ 9.2    │
        └──────┴──────┴────────┴────────┘
        """
        from transnetmap.utils.constant import DCT_TYPE
        
        #  Step 1: Validate required columns in the input table
        required_columns = ["from", "to", "type", "length"]
        missing_columns = set(required_columns) - set(table.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}.")
        
        # Ensure correct data types for consistency
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'length': pl.Float32,
        }
        working_table = table.select(required_columns).with_columns(
            [pl.col(col).cast(dtype) for col, dtype in column_types.items()]
        ).clone()
    
        #  Step 2: Process each impact type individually
        for impact_name in columns_to_update:
            self._log(f"Processing impact type: {impact_name}")
            
            impact_instance = self.dct_impacts_instances[impact_name]
            required_impact_columns = ["type", "max_distance", "impact_value"]
    
            # Extract and verify impact data (Pandas to Polars conversion)
            impact_table = impact_instance.table[required_impact_columns].copy()
            impact_table["type"] = impact_table["type"].map(DCT_TYPE)
            impact_pl = pl.from_pandas(impact_table).sort(["type", "max_distance"], nulls_last=True)
    
            # Store results per impact type
            results = []
    
            #  Step 3: Iterate through each unique type in the table
            for _type in working_table["type"].unique():
                # Filtering ensures type-specific data is correctly selected
                table_filtered = working_table.filter(pl.col("type") == _type).sort("length")
                impact_filtered = impact_pl.filter(
                    pl.col("type") == _type).sort("max_distance", nulls_last=True
                )
                
                # Sorting must be done last to satisfy `join_asof()` requirements in Polars
                table_with_impact = table_filtered.join_asof(
                    impact_filtered,
                    left_on="length",
                    right_on="max_distance",
                    strategy="forward"
                ).with_columns(
                    pl.when(pl.col("impact_value").is_null())
                    .then(impact_filtered.filter(pl.col("max_distance").is_null()
                                                 ).select("impact_value").item(0, 0))
                    .otherwise(pl.col("impact_value"))
                    .alias("impact_value")
                )
    
                results.append(table_with_impact)
    
            # Merge results for this impact type into the working table
            final_result = pl.concat(results)
            working_table = working_table.join(
                final_result.with_columns(
                    (pl.col("impact_value") * pl.col("length"))
                    .alias(f"{impact_name}")
                    .cast(pl.Float32)
                ).select(["from", "to", f"{impact_name}"]),
                on=['from', 'to'],
                validate="1:1"
            )
    
        #  Step 4: Final cleanup and validation
        table = working_table.select(['from', 'to'] + [col for col in columns_to_update])
    
        # Ensure all calculated impacts are present in the final table
        missing_impacts = [impact for impact in columns_to_update if impact not in table.columns]
        if missing_impacts:
            raise RuntimeError(
                f"The following impact columns are missing in the final table: {', '.join(missing_impacts)}"
            )
    
        # Validate that no null values exist in the final table
        check_invalid = table.null_count()
        for col in check_invalid.columns:
            if not check_invalid.select(col).filter(pl.col(col) > 0).is_empty():
                raise RuntimeError(
                    "The table resulting from the impact calculation contains inconsistent values (polars.Null)."
                )
    
        self._log(f"Impacts successfully added: {', '.join(columns_to_update)}")
        return table


    def _add_impacts_on_optimisation(
        self,
        optimisation_table: pl.DataFrame,
        edgelist_table: pl.DataFrame,
        current_impacts: list[str],
    ) -> pl.DataFrame:
        """
        Calculates impacts and length for the partial ``optimisation`` table.
    
        This method computes impact values (e.g., CO2, EP, TCO) and length for the ``optimisation`` table 
        by associating segments of the 'path' column with the corresponding edges in the ``edgelist`` table. 
        The calculated impacts and length are added as new columns to the ``optimisation`` table.
    
        Parameters
        ----------
        optimisation_table : polars.DataFrame
            The input table containing the optimisation results, with all column.
            Must include the columns ["from", "to", "type", "time", "nb_edges", "path"].
        edgelist_table : polars.DataFrame
            The edgelist table containing impact values.
            Must include the columns ["from", "to", "length"] + current_impacts.
        current_impacts: list[str]
            List of impact columns present in the edgelist table.
    
        Returns
        -------
        polars.DataFrame
            The updated ``optimisation`` table enriched with additional columns for `length` and each 
            impact type (e.g., CO2, EP, TCO).
    
        Raises
        ------
        RuntimeError
            - If the ``edgelist`` or ``optimisation`` tables contain null or inconsistent values.
            - If any issues occur during the impact calculation process.
    
        Notes
        -----
        - The impacts are calculated based on the provided `edgelist_table`.
        - The ``optimisation`` table must contain the column 'path', which is exploded and joined with 
          the `edgelist_table` to calculate impacts for each segment of the path.
        - After calculation, the updated table is sorted by ["from", "to"] and includes all 
          necessary impact columns and metadata.
    
        Examples
        --------
        >>> results = Results(config)
        >>> edgelist_table = results.analysis_data.read_sql_edgelist(columns=["from", "to", "length", "CO2", "EP"])
        >>> optimisation_table = results.analysis_data.read_sql_optimisation(where_condition='WHERE "from" = 123 OR "to" = 123')
        >>> updated_optimisation = results._add_impacts_on_optimisation(optimisation_table, edgelist_table)
        >>> print(updated_optimisation)
        shape: (N, M)
        ┌──────┬──────┬───────┬────────┬────────┬──────┬────────┬────────┬──────┐
        │ from │ to   │ type  │ time   │ length │ CO2  │ EP     │ nb_edges │ path │
        │ ---  │ ---  │ ---   │ ---    │ ---    │ ---  │ ---    │ ---    │ ---  │
        │ i16  │ i16  │ i8    │ f32    │ f32    │ f32  │ f32    │ i8    │ list │
        ╞══════╪══════╪═══════╪════════╪════════╪══════╪════════╪════════╪══════╡
        │ 1    │ 3    │ 1     │ 10.5   │ 15.75  │ 20.3 │ 16.8   │ 2      │ ...  │
        │ 2    │ 4    │ 2     │ 20.0   │ 25.0   │ 10.2 │ 8.9    │ 3      │ ...  │
        └──────┴──────┴───────┴────────┴────────┴──────┴────────┴────────┴──────┘
        """
        self._log("Processing impacts for the `optimisation` table...")
    
        #  Step 1: Validate required columns
        required_columns = ["from", "to", "type", "time", "nb_edges", "path"]
        missing_columns = [col for col in required_columns if col not in optimisation_table.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in optimisation table: {', '.join(missing_columns)}.")
        
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
            'nb_edges': pl.Int8,
            'path': pl.List(pl.Int16),
        }
        for impact in current_impacts:
            column_types[impact] = pl.Float32  
            
        #  Step 2: Explode the `path` column to create individual edges
        exploded_table = optimisation_table.select(["from", "to", "nb_edges", "path"]).clone().with_columns(
            pl.col("path").list.slice(0, pl.col("nb_edges")).alias("path_from"),
            pl.col("path").list.slice(1).alias("path_to"),
        ).explode(["path_from", "path_to"]).select(["from", "to", "path_from", "path_to"])
    
        #  Step 3: Join with `edgelist_table` to get distances and times
        joined_table = exploded_table.join(
            edgelist_table,
            left_on=["path_from", "path_to"],
            right_on=["from", "to"],
            how="inner"
        )
    
        #  Step 4: Aggregate impact values per `from` - `to` pair
        aggregations = [
            pl.sum("length").alias("length"),
        ]
        aggregations.extend([pl.sum(impact).alias(f"{impact}") for impact in current_impacts])
    
        aggregated_table = joined_table.group_by(["from", "to"]).agg(aggregations)
        
        optimisation_updated_table = optimisation_table.join(
            aggregated_table,
            on=["from", "to"],
            how="inner",
            validate="1:1",
        ).select(["from", "to", "type", "time", "length", *current_impacts, "nb_edges", "path"]
        ).with_columns(
            [pl.col(col).cast(dtype) for col, dtype in column_types.items()]
        ).sort(["from", "to"])
    
        #  Step 5: Validate consistency and check for null values
        check_invalid = optimisation_updated_table.null_count()
        for col in check_invalid.columns:
            if not check_invalid.select(col).filter(pl.col(col) > 0).is_empty():
                raise RuntimeError(
                    "The final optimisation table contains inconsistent values (polars.Null)."
                )
    
        self._log("Impacts successfully added to the `optimisation` table.")
    
        return optimisation_updated_table


    def _process_partial_network(
        self,
        id_zone: int,
        edgelist_table: pl.DataFrame,
        current_impacts: list[str],
    ) -> pl.DataFrame:
        """
        Core method to compute impacts for the partial network of a given `id_zone`.
        
        This method calculates the complete partial network by merging:
        1. **Optimised routes (optimisation table), `type` = "with-NTS"**.
        2. **NPTM routes (IMT or PT) which extend the optimised network, `type` = "extend-NTS"**.
        3. **NPTM base routes (IMT and PT), `type` = "IMT" and "PT"**.
    
        Parameters
        ----------
        id_zone : int
            The zone ID for which to compute the partial network.
        edgelist_table : polars.DataFrame
            The edgelist table containing impact values.
            Must include the columns ["from", "to", "length"] + current_impacts.
        current_impacts: list[str]
            List of impact columns present in the edgelist table.
    
        Returns
        -------
        polars.DataFrame
            The computed network table (``results_{id_zone}``) containing all routes 
            with environmental impacts applied.
    
        Raises
        ------
        RuntimeError
            If any inconsistencies are found in the calculated impacts (NULL values or duplicates).
    
        Notes
        -----
        - Uses `_add_impacts_on_optimisation()` to process the `optimisation` table.
        - Uses `_add_impacts_on_edge()` to apply impact values to the `IMT` and `PT` NPTM tables.
        - Ensures no duplicate routes exist (`is_duplicated()`) before saving the results.
        - Verifies that all values are consistent before returning the final DataFrame.
        """
        from transnetmap.pre.nptm import NPTM
        from transnetmap.utils.constant import DCT_TYPE
    
        self._log(f"Processing partial network for zone ID: {id_zone}")
    
        #  Step 1: Process NPTM tables (IMT & PT)
        def __process_nptm_table(_type: str, id_zone: int, columns_to_add: list) -> pl.DataFrame:
            """Processes IMT or PT tables for the given `id_zone`."""
            if _type == "IMT":
                table_name = self.db_imt_table
            elif _type == "PT":
                table_name = self.db_pt_table
            else:
                self._log(f"Invalid NPTM transport type: {_type}")
                raise ValueError(" The type of NPTM transport system is inconsistent.")
    
            where = (
                f'''WHERE ("from" = {id_zone} OR "to" = {id_zone})\n'''
                f'''AND "type" = {DCT_TYPE[_type]};'''
            )
            table = NPTM(self.config).read_sql(table_name, where_condition=where
                ).with_columns(pl.lit(1, pl.Int8).alias('nb_edges')
            )
            table_with_impacts = self._add_impacts_on_edge(table.clone(), columns_to_add)
    
            processed_table = table.join(
                table_with_impacts, on=['from', 'to'], validate="1:1"
            ).sort(['from', 'to'])
    
            # Validate consistency
            check_invalid = processed_table.null_count()
            for col in check_invalid.columns:
                if not check_invalid.select(col).filter(pl.col(col) > 0).is_empty():
                    raise RuntimeError(
                        "NPTM impact calculation resulted in inconsistent values (polars.Null)."
                    )
    
            return processed_table
    
        #  Step 2: Define expected columns and types
        columns = ["from", "to", "type", "time", "length", *current_impacts, "nb_edges", "path"]
        column_types = {
            'from': pl.Int16, 'to': pl.Int16, 'type': pl.Int8,
            'time': pl.Float32, 'length': pl.Float32, 'nb_edges': pl.Int8, 'path': pl.List(pl.Int16),
        }
        for impact in current_impacts:
            column_types[impact] = pl.Float32
    
        #  Step 3: Compute `optimisation` impacts
        self._log(" Computing `optimisation` table impacts...")
        
        where = f'''WHERE "from" = {id_zone} OR "to" = {id_zone}'''
        optimisation_columns = ["from", "to", "type", "time", "nb_edges", "path"]
        optimisation_table = Graph(self.config).read_sql_optimisation(
            where_condition=where,
            columns=optimisation_columns
        )
    
        optimisation_table = self._add_impacts_on_optimisation(
            optimisation_table, edgelist_table, current_impacts
        )
    
        #  Step 4: Check if `optimisation_table` is empty
        if optimisation_table.is_empty():
            raise RuntimeError("Calculation of the impact on the optimisation table failed.")
    
        optimisation_table = optimisation_table.select(columns)
    
        #  Step 5: Compute NPTM data impacts (IMT & PT)
        self._log("Computing `IMT` and `PT` NPTM tables...")
        dct_tables = {
            "IMT": __process_nptm_table("IMT", id_zone, current_impacts).select(columns),
            "PT": __process_nptm_table("PT", id_zone, current_impacts).select(columns),
        }
    
        #  Step 6: Extend `optimisation` table with additional NPTM data (avoid duplicates)
        nptm_additional = dct_tables[self.network_extension_type].join(
            optimisation_table, on=["from", "to"], how="anti"
        ).with_columns(pl.lit(DCT_TYPE['extend-NTS'], dtype=pl.Int8).alias("type"))
    
        #  Step 7: Finalize the table
        final_table = pl.concat(
            [optimisation_table, nptm_additional, dct_tables["IMT"], dct_tables["PT"]]
        ).with_columns(
            [pl.col(col).cast(dtype) for col, dtype in column_types.items()]
        ).select(columns).sort(["from", "to", "type"])
    
        self._log(f"Partial network processing complete. Shape: {final_table.shape}")
    
        #  Step 8: Validate no duplicates
        self._log("Validating duplicate entries...")
        valid_table = final_table.select(['from', 'to', 'type']).filter(
            final_table.is_duplicated()).is_empty()
        if not valid_table:
            raise RuntimeError("The final table contains duplicate values!")
    
        #  Step 9: Validate consistency values
        self._log(" Checking for NULL values in the final table...")
        check_invalid = final_table.null_count()
        for col in check_invalid.columns:
            if not check_invalid.select(col).filter(pl.col(col) > 0).is_empty():
                raise RuntimeError("The final table contains inconsistent values (polars.Null).")
    
        self._log(f"Partial network for zone {id_zone} successfully processed.")
    
        self.table = final_table
        return self.table


    # -----------------------------------------------------------------------------
    # Database Interaction Methods (SQL)
    # -----------------------------------------------------------------------------  
    def to_sql_partial_network(self, *, if_exists='fail') -> None:
        """
        Saves the partial network with impacts calculated to the database.
        The results of the partial network are determined by a zone (both target and source),
        according to NPTM zones.
    
        Parameters
        ----------
        if_exists : str, optional
            Determines behavior if the tables already exist in the database (default is `'fail'`).  
            Options:  
                - `'fail'`: Raises an error if the table exists.  
                - `'replace'`: Drops and recreates the table.  
    
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
            The function writes the results data to the database but does not return any value.
    
        Notes
        -----
        - The `adbc` engine is used for optimal writing performance with Polars DataFrames.
        - Paths are converted to a PostgreSQL-compatible array format before saving to the database.
        - The function modifies the data type of the `path` column in the database to:
            SMALLINT[] (array of smallint).
        - Adds a composite primary key on the columns ["from", "to", "type"].
        """
        from transnetmap.utils.sql import schema_exists, execute_sql_script, execute_primary_key_script
        from transnetmap.utils.constant import IMPACTS
        from transnetmap.utils.utils import convert_to_pg_array
    
        #  Step 1: Validate parameters
        if if_exists not in ['fail', 'replace']:
            raise ValueError(f"Invalid value for `if_exists`: {if_exists}. Use 'fail' or 'replace'.")
    
        schema = self.db_results_schema
        table_name = self.table_name_partial_network
    
        #  Step 2: Ensure schema exists
        if not schema_exists(self.uri, schema, print_status=self.config.main_print):
            raise RuntimeError("Schema does not exist.\n"
                               "Run 'to_sql_edgelist()' first.")
    
        #  Step 3: Validate the table before writing
        if self.table.is_empty():
            raise RuntimeError(f"Attempted to write an empty partial network table: {table_name}")
    
        #  Step 4: Apply column types dynamically
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
            'nb_edges': pl.Int8,
            'path': pl.List(pl.Int16),
        }
        for impact in IMPACTS:
            column_types[impact] = pl.Float32
    
        selected_types = {col: column_types[col] for col in self.table.columns}
        self.table = self.table.with_columns(
            [pl.col(col).cast(dtype) for col, dtype in selected_types.items()]
        )
    
        #  Step 5: Convert paths to PostgreSQL format
        path = self.table['path'].to_pandas().apply(lambda x: convert_to_pg_array(x))
        path = pl.from_pandas(path)
        self.table = self.table.replace_column(-1, pl.Series('path', path)).sort(['from', 'to', 'type'])
    
        #  Step 6: Write table using ADBC engine
        try:
            self.table.write_database(
                table_name=f"{schema}.{table_name}",
                connection=self.uri,
                engine="adbc",
                if_table_exists=if_exists
            )
        except Exception as e:
            raise RuntimeError(f"Error while writing to the database: {e}")
    
        #  Step 7: Adjust column type for PostgreSQL array
        script = f"""
        ALTER TABLE "{schema}"."{table_name}"
        ALTER COLUMN path TYPE SMALLINT[]
        USING string_to_array(trim(both '{{}}' from path), ',')::SMALLINT[];
        """
        try:
            execute_sql_script(self.uri, script, print_status=self.config.main_print)
        except Exception as e:
            raise RuntimeError(f"Error while executing SQL script for table '{table_name}': {e}")
    
        #  Step 8: Add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["from", "to", "type"],
            schema=schema,
            include_schema_in_pk_name=True,
            print_status=self.config.main_print
        )
    
        #  Step 9: Log success message
        num_rows = self.table.shape[0]
        self._log(f"Writing to the database is successful.\n"
                  f"Table: '{table_name}'\n"
                  f"Number of rows inserted: {num_rows}\n")
    
        return None


    def read_sql_partial_network(
        self,
        *,
        id_zone: int = None,
        columns: Optional[list[str]] = None,
        where_condition: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Imports partial network results with impacts from the database, with optional column selection
        and where condition. The results of the partial network are determined by a zone (both target 
        and source), according to NPTM zones.
    
        Parameters
        ----------
        id_zone : int, optional
            The zone ID for which to retrieve results. If None, uses `self.id_zone` (default behavior).
        columns : list of str, optional
            List of column names to select. If None, selects all columns (*).
            Default is None.
        where_condition : str, optional
            SQL WHERE clause to filter the rows. If None, no filtering is applied.
            Default is None.
    
        Returns
        -------
        polars.DataFrame
            DataFrame containing the partial network results.
            
            - If called on an instance (`results.read_sql_partial_network()`), the results are stored in `self.table`.
            - If called without assignment (`Results(config).read_sql_partial_network()`), only the DataFrame is returned, 
              and the instance is not stored.
    
        Raises
        ------
        ValueError
            If no valid `id_zone` is provided and the instance does not have a validated `id_zone`.
        RuntimeError
            If the specified table does not exist in the database.  
            If the query returns an empty dataset.
    
        Notes
        -----
        - If `id_zone` is provided, validates it and updates the table accordingly.
        - Dynamically casts columns to appropriate types as defined in the `column_types` dictionary.
        - Allows for SQL filtering through the `where_condition` parameter.
        - Uses the `adbc` engine for optimal database querying.
        """
        
        from transnetmap.utils.sql import table_exists, validate_columns, columns_exist
        from transnetmap.utils.constant import IMPACTS
        import polars as pl
    
        #  Step 1: Validate or define `id_zone`
        if id_zone is not None:
            self.validate_id_zone(id_zone)
        elif not self._id_valid:
            raise ValueError(
                "No `id_zone` provided and no validated `id_zone` in the instance. "
                "Provide a valid `id_zone` or ensure it is validated beforehand."
            )
    
        #  Step 2: Define schema and table name
        schema = self.db_results_schema
        table_name = self.table_name_partial_network
    
        #  Step 3: Check if the table exists
        self._log(f' Checking if the table "{table_name}" exists in the schema "{schema}".')
        
        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )
    
        #  Step 4: Validate requested columns
        column_types = {
            'from': pl.Int16,
            'to': pl.Int16,
            'type': pl.Int8,
            'time': pl.Float32,
            'length': pl.Float32,
            'nb_edges': pl.Int8,
            'path': pl.List(pl.Int16),
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
    
        #  Step 5: Build the SQL query
        sql_query = f'SELECT {columns_part} FROM "{schema}"."{table_name}"'
        if where_condition:
            sql_query += f'\n{where_condition}'
    
        self._log(f"Executing SQL query:\n{sql_query}\n")
    
        #  Step 6: Execute the SQL query and load data into a Polars DataFrame
        try:
            table = pl.read_database_uri(sql_query, self.uri, engine='adbc')
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}\nQuery: {sql_query}")
    
        #  Step 7: Check if the table is empty
        if table.is_empty():
            raise RuntimeError(f"Query returned an empty dataset for table: {table_name}")
    
        #  Step 8: Dynamically cast columns based on selection
        selected_types = {col: column_types[col] for col in columns}
        table = table.with_columns(
            [pl.col(col).cast(dtype) for col, dtype in selected_types.items()]
        )
    
        #  Step 9: Log query results
        num_rows, num_cols = table.shape
        if self.main_print:
            selected_cols = ", ".join(columns)
            print(f"Query executed successfully.\n"
                  f"Selected columns: {selected_cols}\n"
                  f"Rows loaded: {num_rows}\n"
                  f"Columns retrieved: {num_cols}\n")
    
        #  Step 10: Store the table in `self.table`
        self.table = table
    
        return table  # Return the DataFrame instead of modifying the instance directly


# -----------------------------------------------------------------------------
# Main Execution (Testing)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    
    # Complete dictionary of creation and calculation parameters
    dct_param = {
        "network_number": 4,
        "physical_values_set_number": 1,
        "network_extension_type": "IMT",
        "main_print": True,
        "sql_echo": False,
        "db_nptm_schema": "nptm17",
        "db_zones_table": "zones17",
        "db_imt_table": "imt22",
        "db_pt_table": "pt22",
        "uri": "postgresql://username:password@host:port/database",
    }

    " Choice of heat map origin or destination. " " Partial calculation depending on the zone selected. "
    from transnetmap.pre.network import Network 
    Network(dct_param).read_sql().show_all()
    
    
    results = Results(dct_param)
    results.prepare_partial_network(52) # Zone ou il manque PT partiel
    
    dct_param["physical_values_set_number"] = 2
    results_2 = Results(dct_param)
    results_2.replace_all_impacts_in_db()
    
    
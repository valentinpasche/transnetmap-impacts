# -*- coding: utf-8 -*-
"""
Physical Value Sets (PVS) management for travel time and impacts.

This module defines two small classes used to **load, validate, and persist** physical
values used by transnetmap:
- class `PVS_TravelTime` – parameters for travel time computation,
- class `PVS_Impacts` – parameters for environmental/energy/cost impacts (e.g., CO2, EP, TCO).

Notes
-----
- CSV/SQL schemas are documented in each class docstring.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

import numpy as np
import pandas as pd

from transnetmap.utils.config import ParamConfig

if TYPE_CHECKING:  # noqa: F401
    from pathlib import Path

__all__ = ["PVS_TravelTime", "PVS_Impacts"]


# -----------------------------------------------------------------------------
# Class: PVS_TravelTime
# -----------------------------------------------------------------------------
class PVS_TravelTime:
    """
    Represents a Physical Value Set of Travel Time used in the network analysis process.
    This class manages specific physical parameters or values related to travel time,
    allowing them to be retrieved, validated, updated, or stored in a PostgreSQL database.
    
    Guide
    -----
    These tables are network-agnostic: to enable reuse across different geographic configurations, 
    they must define a complete set of rows for all transport types and for all three NTS levels. 
    Pipelines may ignore unused level rows at runtime.

    Constants
    ---------
    _type : str
    
        The type of object, always set to 'travel_time'.

    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters.
    physical_values_set_number : int
        The identification number for the physical value set.
    table : str
        Logical table name derived from the loaded set (e.g., ``travel_time_set_[number]``);
        placeholder at ``__init__``, populated after ``read_csv()`` / ``read_sql()`` / ``to_sql()``.
    dct : dict
        Internal metadata/lookup populated during validation/processing of the loaded dataset.
    uri : str
        PostgreSQL database connection string for reading and writing data.
    schema : str
        The schema name for storing the physical value set in the database.
    table_name : str
        The name of the database table where the physical values are stored.
    main_print : bool
        Indicates whether execution information should be printed to the console.
    sql_echo : bool
        Indicates whether SQL query logs should be displayed.
    table : pandas.DataFrame or None
        Stores the physical value set as a DataFrame after reading from CSV or database.
    dct : dict or None
        Stores the physical value set as a dictionary for easy access to parameter values.

    Methods
    -------
    __init__(param, required_fields=None):
        Initializes the PVS_TravelTime instance with specified and validated parameters.
    read_csv(file):
        Reads a physical value set from a CSV file, validates its format, and stores it as a DataFrame and dictionary.
    to_sql(if_exists='fail'):
        Writes the physical value set to the PostgreSQL database.
    read_sql():
        Reads the physical value set from the PostgreSQL database and stores it in the instance.
    _validate_and_process_table(data):
        Validates and processes the input table for required structure and types (internal method).

    Notes
    -----
    - Datasets are network-agnostic and must include parameters for all three NTS levels (low/main/high) to remain reusable 
    across configurations; unused levels may be present with placeholder values and an explanatory `'comments'` entry.
    - The class ensures that each physical value set is uniquely identified by its `physical_values_set_number`.
    - The `to_sql` and `read_sql` methods handle database interactions, while `read_csv` processes data from CSV files.
    - Validation ensures data integrity and adherence to the expected structure, whether loaded from CSV or SQL.
    - Parameters passed during initialization are validated for completeness and type conformity.

    Examples
    --------
    >>> param = {"physical_values_set_number": 1, "uri": "postgresql://username:password@host:port/database"}
    >>> pvs = PVS_TravelTime(param)
    >>> pvs.read_csv("physical_values_travel_time_1.csv")
    >>> pvs.to_sql(if_exists="replace")
    >>> pvs.read_sql()
    >>> print(pvs.dct["l_ts"]["value"])  # Access a specific parameter
    """

    _type = 'travel_time'  # Object type

    def __init__(self, param: Union[dict, ParamConfig], *, required_fields: Optional[list] = None) -> None:
        """
        Initializes the PVS_TravelTime instance with specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
                
            - `"physical_values_set_number"` : int  
                Identification number for the physical value set.
            - `"uri"` : str  
                PostgreSQL database connection string.

            Optional keys:
                
            - `"main_print"` : bool  
                Enables console output for execution status. Default is False.
            - `"sql_echo"` : bool  
                Enables SQL query logging. Default is False.

        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided,
            defaults to `["physical_values_set_number", "uri"]`.

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `PVS_TravelTime`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        # Define required fields for PVS_TravelTime (default)
        default_required_fields = ["physical_values_set_number", "uri"]

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
        self.physical_values_set_number = self.config.physical_values_set_number
        self.uri = self.config.uri
        
        # Define table name and schema for PVS_TravelTime
        self.schema = 'physical_values'
        self.table_name = f'{self._type}_set_{self.physical_values_set_number}'

        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo
        
        # Initialize placeholders for table and dct
        self.table = None
        self.dct = None


    def _log(self, message: str) -> None:
        if self.main_print:
            print(message)


    def _validate_and_process_table(self, data: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Validates and processes the input table for required structure and types.
    
        Parameters
        ----------
        data : pandas.DataFrame
            The table to validate and process.
    
        Returns
        -------
        pandas.DataFrame
            Validated and processed table.
        dict
            Dictionary representation of the table.
    
        Raises
        ------
        RuntimeError
            If required columns or names are missing, or if data types are invalid.
        """
        def process_entry_value(self, name, value):
            """
            Processes the value of a physical value entry based on its name.
            
            Parameters
            ----------
            name : str
                The name of the physical value entry.
            value : any
                The raw value from the CSV or database.
            
            Returns
            -------
            int, str, or float
                The processed value with the correct type.
            
            Raises
            ------
            ValueError
                If the value cannot be converted to the expected type.
            """
            try:
                if name == 'tf_name':
                    return str(value)
                return float(value)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Error processing value for '{name}': {value}") from e
        
        # Convert 'value' to appropriate type
        def safe_cast(value, target_type):
            try:
                return target_type(value)
            except (ValueError, TypeError):
                return np.nan
        
        # Required structure
        required_columns = set(['name', 'value', 'unit', 'description', 'comments'])
        required_names = [
            "tf_name", "l_ff", "m_ff", "h_ff",
            "l_a_it", "l_b_it", "m_a_it", "m_b_it", "h_a_it", "h_b_it",
            "l_aa", "l_ad", "m_aa", "m_ad", "h_aa", "h_ad",
            "l_ts", "m_ts", "h_ts"
        ]
        
        # Validate column structure
        missing_columns = required_columns - set(data.columns)
        extra_columns = [col for col in data.columns if col not in required_columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        if extra_columns:
            raise ValueError(f"Unexpected columns: {', '.join(extra_columns)}")
    
        # Validate required names in 'name' column
        missing_names = [name for name in required_names if name not in data['name'].tolist()]
        if missing_names:
            raise ValueError(f"Missing required names in the 'name' column: {', '.join(missing_names)}")
    
        # Validate 'tf_name'
        tf_name_row = data.loc[data['name'] == 'tf_name']
        if not tf_name_row['value'].apply(lambda x: isinstance(x, str)).all():
            raise ValueError("Invalid 'tf_name': must be a string.")
    
        data['value'] = data.apply(
            lambda row: safe_cast(row['value'], float) if row['name'] != 'tf_name' else row['value'],
            axis=1
        )
    
        # Validate numeric values in 'value'
        invalid_values = data.loc[
            (data['name'] != 'tf_name') &
            (~data['value'].apply(lambda x: isinstance(x, (int, float)) and np.isfinite(x)))
        ]
        if not invalid_values.empty:
            raise ValueError(
                f"Invalid numeric values in the 'value' column for names: {', '.join(invalid_values['name'].tolist())}"
            )
        
        # Convert to dictionary format
        dct = data.set_index('name').to_dict(orient='index')
    
        # Ensure dictionary values match expected types
        for key, entry in dct.items():
            entry['value'] = process_entry_value(self, key, entry['value'])
            entry['unit'] = str(entry['unit'])
            entry['description'] = str(entry['description'])
            entry['comments'] = str(entry['comments']) if pd.notna(entry['comments']) else ""
    
        return data, dct


    def to_sql(self, *, if_exists: str = 'fail') -> None:
        """
        Writes the physical value set to the database.
        
        This method ensures the physical values set (PVS) is properly formatted and stored in the database.
        The table is created in the ``physical_values`` schema, with a unique table name based on the physical
        values set number (``travel_time_set_[number]``).
        
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
            
        Notes
        -----
        - The 'name' column is used as the primary key.
        - The table is created in the 'physical_values' schema.
        - The method uses SQLAlchemy for database interaction and supports PostgreSQL.
        - Each row in the table corresponds to a specific physical parameter required for travel time calculations.
        
        Returns
        -------
        None
        
        Examples
        --------
        >>> pvs_travel_time = PVS_TravelTime(param)
        >>> pvs_travel_time.read_csv("physical_values_travel_time_1.csv")
        >>> pvs_travel_time.to_sql(if_exists='replace')
        """
        from sqlalchemy import create_engine
        from sqlalchemy.dialects.postgresql import VARCHAR, TEXT
        from transnetmap.utils.sql import define_schema, schema_exists, execute_primary_key_script
        
        # Prohibit "append" to avoid data duplication issues
        if if_exists == 'append':
            raise ValueError(
                "'append' is not allowed in this method to prevent data duplication. "
                "Use 'fail' or 'replace' instead."
            )
        
        if self.table.empty:
            raise ValueError("The table is empty. Ensure data is loaded before writing to the database.")
        
        # Define schema and table name
        schema = self.schema
        table_name = self.table_name
        
        # Ensure schema exists in the database
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            define_schema(self.uri, schema)
        
        # Write to the database
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table.to_sql(
                    table_name,
                    connection,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    dtype={
                        'name': VARCHAR,
                        'value': VARCHAR,
                        'unit': TEXT,
                        'description': TEXT,
                        'comments': TEXT
                    }
                )
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to the database: {e}")
        
        # add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["name"],
            schema=schema,
            include_schema_in_pk_name=False,
            print_status=self.main_print
        )
        
        self._log(f"Writing to the database is successful. Table: '{schema}.{table_name}'")


    def read_sql(self) -> PVS_TravelTime:
        """
        Reads the physical value set from the database and loads it into the instance.
        
        Returns
        -------
        PVS_TravelTime
            The current instance with the table loaded into the 'self.table' attribute as a DataFrame.
            The table is also validated and stored as a dictionary into the 'self.dct' attribute.

        Raises
        ------
        RuntimeError
            If the table does not exist in the database.
        ValueError
            If the data format is invalid.
        """
        from transnetmap.utils.sql import table_exists
        from sqlalchemy import create_engine

        # Define schema and table name
        schema = self.schema
        table_name = self.table_name

        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )

        sql_query = f'SELECT * FROM "{schema}"."{table_name}"'
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                data = pd.read_sql_query(sql_query, connection)
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")
        
        # Validate and process the table
        self.table, self.dct = self._validate_and_process_table(data)

        self._log(f"Import from database successful. Table: '{schema}.{table_name}'")
        
        return self


    def read_csv(self, file: Union[str, Path]) -> PVS_TravelTime:
        """
        Reads a physical value set (PVS) from a CSV file and validates its format.
        
        The CSV file must follow the specified structure:
        
        Expected format
        ---------------
        1) Columns : 
        
            ['name', 'value', 'unit', 'description', 'comments']  
        
           - `'comments' is optional; others are mandatory.  
           - `'value'` must be a valid numeric value, except for the `'tf_name'` row.  
           - `'unit'`, `'description'` and `'name'` must be non-empty strings.  
           - The `'name'` column must include specific predefined keys (see below).  
        
        2) Row-specific rules :  
    
           - Row with `'name'` = `'tf_name'`:  
               * `'value'` must be a string representing the name of a time function.  
           - Rows with other `'name'` values:  
               * `'value'` must be numeric (`float` or `int`).  
               * Corresponding `'unit'` and `'description'` fields must describe the value appropriately.  
        
        3) Required keys in `'name'` column :  
    
           - `"tf_name"` : Name of the time function.  
           - `"l_ff"`, `"m_ff"`, `"h_ff"` : Fractal factors for different network levels (lower, main, higher).  
           - `"l_a_it"`, `"l_b_it"`, `"m_a_it"`, `"m_b_it"`, `"h_a_it"`, `"h_b_it"` : Interface times (start and end) for network levels.  
           - `"l_aa"`, `"l_ad"`, `"m_aa"`, `"m_ad"`, `"h_aa"`, `"h_ad"` : Average acceleration and deceleration for network levels.  
           - `"l_ts"`, `"m_ts"`, `"h_ts"` : Top speeds for network levels.  
             
               *All low/main/high keys are mandatory even if the target network uses fewer levels; 
               unused level values can be placeholders and should be documented via `'comments'`.*

        
        Parameters
        ----------
        file : str or pathlib.Path
            Path to the CSV file.
        
        Returns
        -------
        PVS_TravelTime
            The updated instance with attributes:  
            - `self.table`: Pandas DataFrame containing the validated CSV data.  
            - `self.dct`: Dictionary representation of the data, keyed by `'name'`.  
        
        Raises
        ------
        ValueError
            If the file name, format, or data content is invalid.
        
        Notes
        -----
        - The method ensures that the CSV file adheres to strict structural and content requirements.
        - The validated data is stored in the `self.table` attribute for further operations.
        - The `self.dct` attribute provides a dictionary version of the data for quick lookups.
        
        Examples
        --------
        >>> pvs = PVS_TravelTime(config)
        >>> pvs.read_csv("physical_values_travel_time_1.csv")
        >>> print(pvs.table.head())
                 name value unit description                        comments
        0     tf_name suarm    -     Time function Symmetrical Uniform Rectilinear...
        1        l_ff  1.15    -  Lower fractal factor                        
        2        m_ff  1.15    -   Main fractal factor                        
        3        h_ff  1.15    - Higher fractal factor                        
        4      l_a_it     3  min     Start interface time for lower network level    
        
        >>> print(pvs.dct["tf_name"])
        {'value': 'suarm', 'unit': '-', 'description': 'Time function', 'comments': 'Symmetrical Uniform Rectilinear Motion'}
        """
        from transnetmap.utils.utils import validate_input_file_name
        
        # Validate file name format
        file_str_valid = f'{self.schema}_{self._type}_{self.physical_values_set_number}.csv'
        validate_input_file_name(file, file_str_valid)
            
        # Load CSV
        try:
            data = pd.read_csv(
                file, sep=';', dtype={
                    'name': str,
                    'value': str,
                    'unit': str,
                    'description': str,
                    'comments': str,
                }
            )
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {e}")
        
        # Validate and process the table
        self.table, self.dct = self._validate_and_process_table(data)
            
        return self



# -----------------------------------------------------------------------------
# Class: PVS_Impacts
# -----------------------------------------------------------------------------
class PVS_Impacts:
    """
    Represents a Physical Value Set of Impacts (e.g., CO2 and Primary Energy and Total Cost of Ownership).
    This class manages specific physical parameters related to environmental, energy and financial impacts,
    allowing them to be retrieved, validated, updated, or stored in a PostgreSQL database.
    
    Guide
    -----
    These tables are network-agnostic: to enable reuse across different geographic configurations, 
    they must define a complete set of rows for all transport types and for all three NTS levels. 
    Pipelines may ignore unused level rows at runtime.

    Constants
    ---------
    _type : str
    
        The type of object, always set to 'impacts'.

    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters.
    impact_type : str
        The name identifying the type of impact, defined as ('CO2', 'EP' or 'TCO').
    physical_values_set_number : int
        The identification number for the physical value set.
    table : str
        Logical table name derived from the impact type and set number (e.g., ``impacts_[impact_type]_[number]``);
        placeholder at ``__init__``, populated after ``read_csv()`` / ``read_sql()`` / ``to_sql()``.
    uri : str
        PostgreSQL database connection string for reading and writing data.
    schema : str
        The schema name for storing the physical value set in the database.
    table_name : str
        The name of the database table where the physical values are stored.
    main_print : bool
        Indicates whether execution information should be printed to the console.
    sql_echo : bool
        Indicates whether SQL query logs should be displayed.
    table : pandas.DataFrame or None
        Stores the physical value set as a DataFrame after reading from CSV or database.
    dct : dict or None
        Stores the physical value set as a dictionary for easy access to parameter values.

    Methods
    -------
    __init__(param, CO2, required_fields=None):
        Initializes the PVS_Impacts instance with specified and validated parameters.
    read_csv(file):
        Reads a physical value set from a CSV file, validates its format, and stores it as a DataFrame and dictionary.
    to_sql(if_exists='fail'):
        Writes the physical value set to the PostgreSQL database.
    read_sql():
        Reads the physical value set from the PostgreSQL database and stores it in the instance.
    _validate_and_process_table(data):
        Validates and processes the input table for required structure and types (internal method).

    Notes
    -----
    - Datasets are network-agnostic and must include parameters for all three NTS levels (low/main/high) to remain reusable 
    across configurations; unused levels may be present with placeholder values and an explanatory `'comments'` entry.
    - The class ensures that each physical value set is uniquely identified by its `physical_values_set_number`.
    - The `to_sql` and `read_sql` methods handle database interactions, while `read_csv` processes data from CSV files.
    - Impact types (e.g., CO2, EP, TCO) are distinguished through the `impact_type` column.
    - Validation ensures data integrity and adherence to the expected structure, whether loaded from CSV or SQL.
    - Parameters passed during initialization are validated for completeness and type conformity.
    - The impact taxonomy and level/type mapping follow ``transnetmap.utils.constant.DCT_TYPE``.
      Ensure your impact names and columns align with these keys when loading or exporting tables.

    Examples
    --------
    >>> param = {"physical_values_set_number": 1, "uri": "postgresql://username:password@host:port/database"}
    >>> pvs = PVS_Impacts(param, 'CO2')
    >>> pvs.read_csv("physical_values_impacts_1.csv")
    >>> pvs.to_sql(if_exists="replace")
    >>> pvs.read_sql()
    """

    _type = 'impacts'  # Object type

    def __init__(
        self,
        param: Union[dict, ParamConfig],
        impact_type: str,
        *,
        required_fields: Optional[list] = None
    ) -> None:
        """
        Initializes the PVS_Impacts instance with specified parameters and impact type.

        Parameters
        ----------
        param : dict or ParamConfig
            A dictionary of configuration parameters or an already validated ParamConfig object.

            Required keys (for the default configuration):
                
            - `"physical_values_set_number"` : int  
                Identification number for the physical value set.
            - `"uri"` : str  
                PostgreSQL database connection string.

            Optional keys:
                
            - `"main_print"` : bool  
                Enables console output for execution status. Default is False.
            - `"sql_echo"` : bool  
                Enables SQL query logging. Default is False.
        
        impact_type : str
            Type of impact represented by the instance. Must be one of `['CO2', 'EP', 'TCO']`.
            
        required_fields : list, optional
            A custom list of fields required for this specific instance. If not provided,
            defaults to `["physical_values_set_number", "uri"]`.

        Raises
        ------
        ValueError
            Raised if any required parameter is missing from the configuration.  
            If the provided `impact_type` is invalid.
        TypeError
            Raised if a parameter has an incorrect type.

        Notes
        -----
        - If `param` is a dictionary, it is validated for all required fields.
        - If `param` is a `ParamConfig` object, only the fields relevant to the `PVS_TravelTime`
          class are validated.
        - This method ensures that all mandatory parameters are present and that optional
          parameters are set to default values if not provided.
        """
        from transnetmap.utils.constant import IMPACTS
        
        # Validate the impact type
        if impact_type not in IMPACTS:
            raise ValueError(f"Invalid impact type: '{impact_type}'. Must be one of {IMPACTS}. "
                             f"Ensure you have correctly defined 'IMPACTS' in your configuration.")
        self.impact_type = impact_type  # Set the impact type
        
        # Define required fields for PVS_Impacts (default)
        default_required_fields = ["physical_values_set_number", "uri"]

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
        self.physical_values_set_number = self.config.physical_values_set_number
        self.uri = self.config.uri
        
        # Define table name and schema for PVS_TravelTime
        self.schema = 'physical_values'
        self.table_name = f'{self._type}_{self.impact_type}_{self.physical_values_set_number}'

        # Extract and adjust parameters based on execution context
        self.main_print = self.config.main_print or (__name__ == "__main__")
        self.sql_echo = self.config.sql_echo
        
        # Initialize placeholders for the table
        self.table = None


    def _log(self, message: str) -> None:
        if self.main_print:
            print(message)


    def _validate_and_process_table(self, data: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Validates and processes the input table for required structure and types.
        
        This method performs the following validations:
        1. Ensures the table has the required columns.
        2. Converts specific columns ('max_distance', 'impact_value', 'load_percent') to `float32`.
        3. Validates the presence and consistency of mandatory columns:
           - 'impact_value' must not contain null values.
           - 'impact_unit' must have a single unique value.
           - 'motorization', 'description', and 'sources' must not contain null values.
        4. Checks the relationship between 'type' and 'max_distance':
           - For types with a single row, 'max_distance' must be NaN.
           - For types with multiple rows, 'max_distance' must have exactly one NaN, and all other values must be unique.
        5. Ensures each pair of ['type', 'impact_value'] is unique.
        6. Verifies that all required keys from the `DCT_TYPE` dictionary are present in the 'type' column, 
           excluding keys starting with "without" or "with".
        
        Parameters
        ----------
        data : pandas.DataFrame
            The table to validate and process.
        
        Returns
        -------
        pandas.DataFrame
            The validated and processed table.
        
        Raises
        ------
        ValueError
            Raised in the following cases:
            - Missing required columns: If any of the required columns are absent.
            - Null values in 'impact_value': If the 'impact_value' column contains null values.
            - Invalid 'type' values: If the 'type' column contains values that are not defined in the `DCT_TYPE` dictionary.
            - Missing required keys: If any required keys from the `DCT_TYPE` dictionary are missing in the 'type' column, 
              excluding keys starting with "without" or "with".
            - Invalid 'impact_type': If the 'impact_type' column contains values that do not match the specified `self.impact_type`.
            - Inconsistent 'impact_unit': If the 'impact_unit' column contains multiple unique values.
            - Null values in mandatory columns: If the 'motorization', 'description', or 'sources' columns contain null values.
            - Invalid 'max_distance' relationship:
                - For types with a single row, 'max_distance' must be NaN.
                - For types with multiple rows, 'max_distance' must have exactly one NaN, and all other values must be unique.
            - Duplicate ['type', 'impact_value'] pairs: If any duplicate pairs exist in the table.
        """
        from transnetmap.utils.constant import DCT_TYPE
        
        # Expected column structure
        required_columns = [
            "type", "max_distance", "impact_type", "impact_value", "impact_unit",
            "motorization", "load_percent", "description", "comments", "sources"
        ]
        
        # Validate columns
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Process data types
        data["max_distance"] = pd.to_numeric(data["max_distance"], errors="coerce").astype("float32")
        data["impact_value"] = pd.to_numeric(data["impact_value"], errors="coerce").astype("float32")     
        data["load_percent"] = pd.to_numeric(data["load_percent"], errors="coerce").astype("float32")
        
        # Validate mandatory columns
        if data["impact_value"].isnull().any():
            raise ValueError("The 'impact_value' column contains null values, which are not allowed.")
        if not data["type"].isin(list(DCT_TYPE.keys())).all():
            raise ValueError("The 'type' column contains type values, which are not allowed.\n"
                             "Types are defined in the `DCT_TYPE` dictionary."
                             )
        if not data["impact_type"].isin([self.impact_type]).all():
            raise ValueError("The 'impact_type' column contains type values, which are not allowed.\n"
                             f"All values must correspond to the type of impact defined: '{self.impact_type}'"
                             )
        if not data["impact_unit"].nunique() == 1:
            raise ValueError("The 'impact_unit' column contains different values, they must be identical.")
        
        # Ensure all keys in DCT_TYPE (excluding 'without' and 'with' keys) are present in the 'type' column
        required_keys = [key for key in DCT_TYPE.keys() if not key.startswith("extend") and not key.startswith("with")] # ("with" operate "without" to)
        missing_keys = [key for key in required_keys if key not in data["type"].tolist()]
        if missing_keys:
            raise ValueError(f"The following keys from 'utils.constant.DCT_TYPE' are missing in the 'type' column: {', '.join(missing_keys)}")

        # Ensure mandatory fields are filled
        for col in ["motorization", "description", "sources"]:
            if data[col].isnull().any():
                raise ValueError(f"The '{col}' column contains null values, which are not allowed.")

        # Validate 'type' and 'max_distance' relationship
        grouped = data.groupby("type")["max_distance"]
        
        for type_name, group in grouped:
            nan_count = group.isna().sum()
            unique_distances = group.dropna().nunique()
            
            if len(group) == 1:
                if nan_count != 1:
                    raise ValueError(f"For type '{type_name}', there should be exactly one row with NaN in 'max_distance'.")
            else:
                if nan_count != 1:
                    raise ValueError(f"For type '{type_name}', there must be exactly one NaN value in 'max_distance' across multiple rows.")
                if unique_distances != len(group) - 1:
                    raise ValueError(f"For type '{type_name}', all non-NaN 'max_distance' values must be unique.")
        
        # Validate uniqueness of ["type", "impact_value"]
        duplicate_pairs = data.duplicated(subset=["type", "impact_value"], keep=False)
        if duplicate_pairs.any():
            duplicate_rows = data.loc[duplicate_pairs, ["type", "impact_value"]]
            raise ValueError(
                f"Duplicate entries found for the following 'type' and 'impact_value' pairs:\n"
                f"{duplicate_rows.to_string(index=False)}"
            )
        
        # Sort by type and max_distance, placing NaN last
        data = data.sort_values(by=['type','max_distance'], na_position='last')
        
        return data


    def to_sql(self, *, if_exists: str = 'fail') -> None:
        """
        Writes the physical value set to the database.
        
        This method ensures the physical values set (PVS) is properly formatted and stored in the database.
        The table is created in the ``physical_values`` schema, with a unique table name based on the impact type
        and the physical values set number (``impacts_[impact type]_[number]``).
        
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
            
        Notes
        -----
        - The 'name' column is used as the primary key.
        - The table is created in the 'physical_values' schema.
        - The method uses SQLAlchemy for database interaction and supports PostgreSQL.
        - Each row in the table corresponds to a specific physical parameter for impacts calculations.
        
        Returns
        -------
        None
        
        Examples
        --------
        >>> pvs_impacts_co2 = PVS_Impacts(param, 'CO2')
        >>> pvs_impacts_co2.read_csv("physical_values_impacts_CO2_1.csv")
        >>> pvs_impacts_co2.to_sql(if_exists='replace')
        """
        from sqlalchemy import create_engine
        from sqlalchemy.dialects.postgresql import VARCHAR, TEXT, REAL
        from transnetmap.utils.sql import define_schema, schema_exists, execute_primary_key_script
        
        # Prohibit "append" to avoid data duplication issues
        if if_exists == 'append':
            raise ValueError(
                "'append' is not allowed in this method to prevent data duplication. "
                "Use 'fail' or 'replace' instead."
            )
            
        if self.table.empty:
            raise ValueError("The table is empty. Ensure data is loaded before writing to the database.")
 
        # Define schema and table name
        schema = self.schema
        table_name = self.table_name
        
        # Ensure schema exists in the database
        if not schema_exists(self.uri, schema, print_status=self.main_print):
            define_schema(self.uri, schema)
        
        # Write to the database
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                self.table.to_sql(
                    table_name,
                    connection,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    dtype={
                        'type': VARCHAR,
                        'max_distance': REAL,
                        'impact_type': VARCHAR,
                        'impact_value': REAL,
                        'impact_unit': VARCHAR,
                        'motorization': VARCHAR,
                        'load_percent': REAL,
                        'description': TEXT,
                        'comments': TEXT,
                        'sources': TEXT
                    }
                )
        except Exception as e:
            raise RuntimeError(f"An error occurred while writing to the database: {e}")
        
        # add primary key to table
        execute_primary_key_script(
            uri=self.uri,
            table=table_name,
            list_columns=["type", "impact_value"],
            schema=schema,
            include_schema_in_pk_name=False,
            print_status=self.main_print
        )

        self._log(f"Writing to the database is successful. Table: '{schema}.{table_name}'")


    def read_sql(self) -> PVS_Impacts:
        """
        Reads the physical value set from the database and loads it into the instance.
        
        Returns
        -------
        PVS_Impacts
            The current instance with the table loaded into the 'self.table' attribute as a DataFrame.
            The table is also validated and processed.

        Raises
        ------
        RuntimeError
            If the table does not exist in the database.
        ValueError
            If the data format is invalid or the validation fails.
        """
        from transnetmap.utils.sql import table_exists
        from sqlalchemy import create_engine

        # Define schema and table name
        schema = self.schema
        table_name = self.table_name

        if not table_exists(self.uri, table_name, print_status=self.main_print):
            raise RuntimeError(
                f'Table "{table_name}" does not exist in the database.\n'
                f'Ensure it is defined and written to the database (schema: "{schema}").'
            )
            
        sql_query = f'SELECT * FROM "{schema}"."{table_name}"'
        try:
            with create_engine(self.uri, echo=self.sql_echo).connect() as connection:
                data = pd.read_sql_query(sql_query, connection)
        except Exception as e:
            raise RuntimeError(f"Error reading data from database: {e}")
        
        # Validate and process the table
        self.table = self._validate_and_process_table(data)

        self._log(f"Import from database successful. Table: '{schema}.{table_name}'")
        
        return self


    def read_csv(self, file: Union[str, Path]) -> PVS_Impacts:
        """
        Reads a physical value set (PVS) from a CSV file and validates its format.
        
        This method ensures that the CSV adheres to the expected structure and content rules.
        
        Expected format
        ---------------
        1) Columns : 
        
            ['type', 'max_distance', 'impact_type', 'impact_value', 'impact_unit',
             'motorization', 'load_percent', 'description', 'comments', 'sources']  
        
           - `'comments'` is optional; others are mandatory.  
           - `'max_distance'` and `'load_percent'` can contain missing values, represented as `'-'`.  
           - `'impact_value'` must be a numeric value (`float` or `int`).  
           - `'impact_type'` must match the `impact_type` parameter passed to the method.  
           - `'impact_unit'`, `'motorization'`, `'description'`, and `'sources'` must have consistent values.  
        
        2) Row-specific rules :
                     
           - `'type'`: Must match the keys defined in the ``DCT_TYPE`` dictionary.  
           *Network-level coverage: even if the target network uses only one or two levels, 
           the `'type'` column MUST include all three NTS levels (`'NTS-lower'`, `'NTS-main'`, `'NTS-higher'`) in addition to the `'IMT'`/`'PT'` keys. 
           Unused levels may carry placeholder numeric values and should be annotated in `'comments'`.*  
           - `'max_distance'`: For each `'type'`, the following rules apply:  
               * If a `'type'` is present only once, `'max_distance'` must be `NaN`.  
               * If a `'type'` is present multiple times, exactly one row must have `'max_distance' = NaN`,  
                 and all other rows must have unique numeric values.  
           - `'impact_type'`: Must match the `impact_type` parameter passed to the method.  
           - `'impact_unit'`: All rows must have the same value.  
           - `'impact_value'`: Must be a numeric value and cannot be null.  
        
        Parameters
        ----------
        file : str or pathlib.Path
            Path to the CSV file.
        
        Returns
        -------
        PVS_Impacts
            The current instance with attributes:  
            - `self.table`: Pandas DataFrame containing the validated CSV data.  
        
        Raises
        ------
        ValueError
            If the file name, format, or data content is invalid.
        
        Notes
        -----
        - The method ensures that the CSV file adheres to strict structural and content requirements.
        - Missing values for `'max_distance'` and `'load_percent'` are automatically converted to `NaN` for processing.
        - The validated data is stored in the `self.table` attribute for further operations.
        
        Examples
        --------
        >>> impacts = PVS_Impacts(config, 'CO2')
        >>> impacts.read_csv("physical_values_impacts_CO2_1.csv")
        >>> print(impacts.table.head())
              type  max_distance impact_type  impact_value      impact_unit motorization  ...
        0      IMT           NaN         CO2         1.25  kg / seat-km     average      ...
        1       PT           4.0         CO2         0.55  kg / seat-km     average      ...
        2  NTS-main           NaN         CO2         0.34  kg / seat-km     electric     ...
        """
        from transnetmap.utils.utils import validate_input_file_name
        
        # Validate file name format
        file_str_valid = f'{self.schema}_{self._type}_{self.impact_type}_{self.physical_values_set_number}.csv'
        validate_input_file_name(file, file_str_valid)
        
        # Load CSV
        try:
           data = pd.read_csv(
               file, sep=';', dtype=str
           )
        except Exception as e:
           raise ValueError(f"Error reading CSV file: {str(e)}")

        # Validate and process the table
        self.table = self._validate_and_process_table(data)
            
        return self


# -----------------------------------------------------------------------------
# Main (example-only)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    
    # Complete dictionary of creation and calculation parameters
    dct_param = {
        "physical_values_set_number": 1,
        "main_print": False,
        "sql_echo": False,
        "uri": "postgresql://username:password@host:port/database",
    }
    
    
    file_tt_1 = r"C:\...\test\datasets\physical_values_travel_time_1.csv"

    pvs_tt = PVS_TravelTime(dct_param).read_csv(file_tt_1)
    pvs_tt_dct = pvs_tt.dct
    pvs_tt_table = pvs_tt.table
    
    pvs_tt.to_sql(if_exists='fail')

    pvs_tt_2 = PVS_TravelTime(dct_param).read_sql()

# ===========================
    
    file_co2_1 = r"C:\...\test\datasets\physical_values_impacts_CO2_1.csv"
    file_ep_1 = r"C:\...\test\datasets\physical_values_impacts_EP_1.csv"
    
    pvs_co2_1 = PVS_Impacts(dct_param, 'CO2').read_csv(file_co2_1)
    pvs_ep_1 = PVS_Impacts(dct_param, 'EP').read_csv(file_ep_1)
    
    pvs_co2_1_table = pvs_co2_1.table
    pvs_ep_1_table = pvs_ep_1.table
    
    pvs_co2_1.to_sql(if_exists='fail')
    pvs_ep_1.to_sql(if_exists='fail')
    
    pvs_co2_1_table_2 = PVS_Impacts(dct_param, 'CO2').read_sql().table
    pvs_ep_1_table_2 = PVS_Impacts(dct_param, 'EP').read_sql().table   
    
    
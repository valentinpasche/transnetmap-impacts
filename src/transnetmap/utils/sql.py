# -*- coding: utf-8 -*-

def execute_sql_script(
    uri: str, 
    script: str, 
    params=None, 
    fetch_all=False, 
    print_status=True, 
    raise_on_error=True
) -> list:
    """
    Executes an SQL script directly in the database and returns the results if applicable.

    Parameters
    ----------
    uri : str
        PostgreSQL DB connection string.
    script : str
        SQL script to execute.
    params : list or tuple, optional
        Parameters to securely pass into the SQL script. Default is None.
    fetch_all : bool, optional
        If True, fetches all rows from the result. Default is False (fetch one row).
    print_status : bool, optional
        If True, displays status messages (default is True).
    raise_on_error : bool, optional
        Whether to raise an exception on error. Default is True.

    Returns
    -------
    list or None
        The results of the SQL query as a list (e.g., for SELECT statements), 
        or None if the query does not return results (e.g., for INSERT, UPDATE, DELETE).
    
    Notes
    -----
    - For SELECT statements or any script returning results, only the first row is returned.
    - Commits are automatically performed for write operations.
    
    Raises
    ------
    Exception
        Any error encountered during script execution is printed to the console.
    RuntimeError
        If an error occurs during SQL execution and `raise_on_error` is True.
    """
    import psycopg2

    conn = None
    error_occurred = False  # Tracks if an error occurred during execution

    try:
        # Validate params
        if params and not isinstance(params, (list, tuple)):
            raise ValueError("The `params` argument must be a list or tuple of parameters.")
        
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(uri)
        with conn.cursor() as cur:
            # Execute the SQL script with parameters
            cur.execute(script, params)

            # Check if the query returns results (e.g., a SELECT statement)
            if cur.description:
                if fetch_all:
                    result = cur.fetchall()  # Fetch all rows
                    if print_status:
                        print(f"SQL script executed successfully with {len(result)} rows.")
                    return result
                else:
                    result = cur.fetchone()  # Fetch the first row of the result (useful for EXISTS or SELECT)
                    if print_status:
                        print("SQL script executed successfully with one result.")
                    return result
            else:
                # If the query does not return rows (e.g., an INSERT/UPDATE/DELETE statement)
                conn.commit()
                if print_status:
                    operation = script.split()[0].upper()
                    print(f"SQL script executed successfully: {operation} operation completed.")
                return None

    except Exception as error:
        # Mark that an error occurred and log the error message
        error_occurred = True
        if raise_on_error:
            error_message = f"Error executing SQL script: {error}\nScript:\n{script}"
            raise RuntimeError(error_message) from error
        print(f"Error executing SQL script: {error}")
        return None

    finally:
        # Close the database connection
        if conn is not None:
            conn.close()
            if print_status or error_occurred:
                print("Database connection closed.")


def execute_primary_key_script(
    uri: str, 
    table: str, 
    list_columns: list, 
    schema: str, 
    include_schema_in_pk_name=False, 
    print_status=True
) -> None:
    """
    Adds a primary key constraint to a specified table in the PostgreSQL database,
    with optional schema-prefixed constraint name.
    
    Parameters
    ----------
    uri : str
        PostgreSQL database connection string.
    table : str
        Name of the target table.
    list_columns : list of str
        List of columns to include in the primary key constraint.
    schema : str
        Name of the schema containing the target table.
    include_schema_in_pk_name : bool, optional
        If True, includes the schema name in the primary key constraint name. Default is False.
    print_status : bool, optional
        If True, prints status messages to the console. Default is True.

    Returns
    -------
    None
        This function performs an operation on the database but does not return a value.
    
    Raises
    ------
    ValueError
        If the schema or table does not exist, or if any of the specified columns are missing.
    RuntimeError
        If an error occurs during the execution of the SQL script.

    Notes
    -----
    - The primary key constraint name defaults to `{table_name}_pkey`.
    - If `include_schema_in_pk_name` is True, the constraint name becomes `{schema}_{table_name}_pkey`.
    - The function validates the schema, table, and column existence before attempting to add the constraint.
    - This function uses `execute_sql_script` for executing the SQL command.
    - The `param` argument in `execute_sql_script` is explicitly set to `None` as this method does not require dynamic parameters.

    Examples
    --------
    Adding a primary key to the table "my_table" in the schema "my_schema":
    >>> execute_primary_key_script(
    ...     uri="postgresql://user:password@localhost/mydb",
    ...     table="my_table",
    ...     list_columns=["id", "name"],
    ...     schema="my_schema",
    ...     include_schema_in_pk_name=True,
    ...     print_status=True
    ... )
    """
    # Step 1: Validate the schema
    if not schema_exists(uri, schema, print_status=print_status):
        raise ValueError(f"Schema '{schema}' does not exist in the database.")
    
    # Step 2: Validate the table
    if not table_exists(uri, table, print_status=print_status):
        raise ValueError(f"Table '{schema}.{table}' does not exist in the database.")
        
    # Step 3: Validate the columns
    if not list_columns or not isinstance(list_columns, list):
        raise ValueError("'list_columns' must be a non-empty list of column names.")
    
    # Check if columns exist in the specified table
    column_check = columns_exist(uri, list_columns, table, schema, print_status=print_status)
    missing_columns = [col for col, exists in column_check.items() if not exists]
    if missing_columns:
        raise ValueError(f"The following columns do not exist in table '{schema}.{table}': {', '.join(missing_columns)}")
    
    # Step 4: Construct the composite primary key string
    pk_columns = ", ".join([f'"{col}"' for col in list_columns])
    pk_name = f"{schema}_{table}_pkey" if include_schema_in_pk_name else f"{table}_pkey"
    table_full_name = f'"{schema}"."{table}"'
    
    # Step 5: Construct and execute the SQL script
    script = f"""
    ALTER TABLE {table_full_name}
    ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_columns});
    """
    
    # Execute the script with optional parameters (explicit param=None for clarity)
    execute_sql_script(uri, script, params=None, print_status=print_status, raise_on_error=True)
    
    if print_status:
        print(f"Primary key '{pk_name}' added successfully to table '{schema}.{table}'.")
    
    return None


def define_schema(uri: str, name_schema: str, text_comment=None, print_status=True) -> None:
    """
    Creates a schema in the PostgreSQL database and optionally adds a comment to it.

    Parameters
    ----------
    uri : str
        PostgreSQL DB connection string.
    name_schema : str
        Name of the schema to create.
    text_comment : str, optional
        A comment to add to the schema after its creation. Default is None.
    print_status : bool, optional
        If True, displays status messages during the execution (default is True).

    Returns
    -------
    None
        This function performs an operation on the database but does not return a value.

    Raises
    ------
    ValueError
        If the schema already exists.
    RuntimeError
        If an error occurs during execution.

    Notes
    -----
    - Uses `execute_sql_script` for executing SQL commands.
    - Ensures the schema is created only if it does not exist.
    """
    # Validate the schema name (SQL injection protection)
    if schema_exists(uri, name_schema, print_status=print_status):
        raise ValueError(f"Schema '{name_schema}' already exists in the database.")
    
    # Construct the SQL script to create the schema and optionally add a comment
    if text_comment:
        script = f'''
        CREATE SCHEMA {name_schema};
        COMMENT ON SCHEMA {name_schema} IS %s;
        '''
        params = (text_comment,)
    else:
        script = f'''
        CREATE SCHEMA {name_schema};
        '''
        params = None
    
    # Execute the SQL script using the provided utility function
    execute_sql_script(uri, script, params=params, print_status=print_status)

    return None


def schema_exists(uri: str, schema: str, print_status=True) -> bool:
    """
    Checks if a schema exists in the PostgreSQL database.

    Parameters
    ----------
    uri : str
        PostgreSQL DB connection string.
    schema : str
        Name of the schema to check for existence.
        Ensure the schema name is properly quoted in the SQL query.
    print_status : bool, optional
        If True, displays status messages during the execution (default is True).

    Returns
    -------
    bool
        True if the schema exists, False otherwise.

    Notes
    -----
    - This function uses `execute_sql_script` for executing the SQL command.
    - This function is safe to use even if the schema does not exist.
    """
    # SQL script to check schema existence
    script = '''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = %s
        ) AS schema_existence;
    '''
    
    # Execute the SQL script and retrieve the result
    result = execute_sql_script(uri, script, params=(schema,), print_status=print_status)
    
    # Return True if schema exists, otherwise False
    if result is not None:
        return result[0]  # The first column contains the result of EXISTS
    else:
        return False


def table_exists(uri: str, table: str, print_status=True) -> bool:
    """
    Checks if a table exists in the PostgreSQL database.

    Parameters
    ----------
    uri : str
        PostgreSQL DB connection string.
    table : str
        Name of the table to check for existence.
        Ensure the table name is properly quoted in the SQL query.
    print_status : bool, optional
        If True, displays status messages during the execution (default is True).

    Returns
    -------
    bool
        True if the table exists, False otherwise.

    Notes
    -----
    - This function uses `execute_sql_script` for executing the SQL command.
    - This function works regardless of the schema, provided the table name is correct.
    """
    # SQL script to check table existence
    script = '''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        ) AS table_existence;
    '''
    
    # Execute the SQL script and retrieve the result
    result = execute_sql_script(uri, script, params=(table,), print_status=print_status)
    
    # Return True if the table exists, otherwise False
    if result is not None:
        return result[0]  # The first column contains the result of EXISTS
    else:
        return False


def columns_exist(uri: str, columns: list, table: str, schema: str, print_status=True) -> dict:
    """
    Checks if specified columns exist in a given table in the PostgreSQL database.

    Parameters
    ----------
    uri : str
        PostgreSQL DB connection string.
    columns : list of str
        List of column names to check for existence.
    table : str
        Name of the table to check.
    schema : str
        Name of the schema containing the table.
    print_status : bool, optional
        If True, displays status messages during the execution (default is True).

    Returns
    -------
    dict
        A dictionary with column names as keys and their existence status (True/False) as values.

    Notes
    -----
    - This function uses `execute_sql_script` for executing the SQL command.
    - This function queries the information_schema.columns view to check for column existence.
    - Ensure that the schema and table names are properly quoted in the SQL query.
    """
    # SQL script to check column existence
    script = '''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        AND table_schema = %s
        AND column_name IN %s;
    '''
    columns_tuple = tuple(columns) # Convert list to tuple for SQL
    params = (table, schema, columns_tuple)
    
    # Execute the SQL script and retrieve the result
    result = execute_sql_script(uri, script, params=params, fetch_all=True, print_status=print_status)
    
    # Debugging: Print the raw result
    if print_status:
        print(f"Raw result from execute_sql_script: {result}")
    
    # Parse the result to determine column existence
    if result is not None:
        existing_columns = {row[0] for row in result}  # Extract existing column names from the result
        return {col: (col in existing_columns) for col in columns}
    else:
        return {col: False for col in columns}


def validate_columns(uri: str, columns: list, table: str, schema: str, print_status=True) -> str:
    """
    Validates the existence of specified columns in a database table and formats them for SQL queries.

    Parameters
    ----------
    uri : str
        PostgreSQL database connection string.
    columns : list of str
        List of column names to validate.
    table : str
        Name of the table containing the columns.
    schema : str
        Name of the schema containing the table.
    print_status : bool, optional
        If True, prints status messages to the console. Default is True.

    Returns
    -------
    str
        A comma-separated string of quoted column names for use in SQL queries.

    Raises
    ------
    ValueError
        If any specified column does not exist in the table.

    Notes
    -----
    - This function ensures all requested columns exist before constructing the SQL query part.
    - The column names are properly quoted for SQL syntax compatibility.

    Examples
    --------
    >>> validate_columns(uri, ['from', 'to', 'time'], 'imt22', 'nptm17')
    '"from", "to", "time"'
    """
    column_check = columns_exist(uri, columns, table, schema, print_status=print_status)
    missing = [col for col, exists in column_check.items() if not exists]
    
    if missing:
        raise ValueError(f"Missing columns in the table: {', '.join(missing)}")
        
    columns_part = ", ".join(f'"{col}"' for col in columns)  # Properly quote column names
    return columns_part


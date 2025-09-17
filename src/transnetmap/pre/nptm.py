# -*- coding: utf-8 -*-
"""
NPTM (National Passenger Traffic Model) utilities for Switzerland-oriented workflows.

This module defines the class`NPTM`, a small helper to format and persist NPTM inputs
(zones + IMT/PT matrices) into PostgreSQL/PostGIS. While tailored to the Swiss NPTM,
it can work with any dataset exposing the same structure.

Notes
-----
- The long Swiss tutorial that used to live in this module's ``if __main__`` has been
  moved into a separate script: ``nptm_test.py`` (in progress, example-only).
"""

from __future__ import annotations

from typing import Optional, Union, TYPE_CHECKING

import polars as pl

from transnetmap.utils.config import ParamConfig

if TYPE_CHECKING:  # noqa: F401
    import geopandas as gpd

__all__ = ["NPTM"]


class NPTMUserWarning(UserWarning):
    """Non-fatal data-quality issues detected during NPTM setup."""
    pass

class NPTM:
    """
    Represents a National Passenger Traffic Model (NPTM).
    
    This class formats and analyzes travel-time and travel-distance data at traffic-zone level.
    While initially designed for the Swiss NPTM, it can be adapted to other transport models
    with compatible datasets and schema conventions.
    
    The NPTM class interacts with travel time and distance datasets stored in a PostgreSQL database.
    
    Constants
    ---------
    _type : str
    
        The type of object, always set to 'national_passenger_traffic_model'.
    
    Attributes
    ----------
    config : ParamConfig
        Dataclass with validated configuration parameters (database URIs, schemas, table names).
    uri : str
        PostgreSQL database connection string.
    imt_mtx : polars.DataFrame or None
        Formatted OD matrix for Individual Motorised Transport (IMT); populated after
        method `setup_data`.  
        Columns: ``from``, ``to``, ``type``, ``time``, ``length``, ``path``.
    pt_mtx : polars.DataFrame or None
        Formatted OD matrix for Public Transport (PT); populated after method `setup_data`.  
        Columns: ``from``, ``to``, ``type``, ``time``, ``length``, ``path``.
    zones_gdf : geopandas.GeoDataFrame or None
        Zones geometry in WGS 84 (EPSG:4326); populated after method `setup_data`.
    
    Information relating to raw input data
    --------------------------------------
    If the active geometry column is not named ``geom``, it is automatically renamed to ``geom`` with a warning.
    
    **ID constraints for zones**  
    - The ``id`` column must start at 1.  
    - IDs must be unique and must not exceed the int16 limit (32767).  
    - Non-contiguous IDs (e.g., 1, 2, 4, 5, …) are allowed but will trigger a warning.
    
    **OD matrices policy**  
    - Input matrices are **directed** (all (from,to) pairs are considered distinct).  
    - Rows whose origin/destination cannot be mapped to provided zones are **dropped** with a warning.  
    - Duplicate (from,to) pairs with identical values (within a small tolerance) are **collapsed** with a warning.  
    - Conflicting duplicates **raise** a ``ValueError``.  
    - Missing relations are **created with null values** during completion (directed graph).  
    - Optional symmetric fill (``value[from,to] := value[to,from]``) can be enabled only for metrics  
      that are guaranteed to be symmetric by construction.
    
    **The geographic CRS is WGS 84 (EPSG:4326).**
    """

    _type = 'national_passenger_traffic_model'  # Object type

    def __init__(self, param: Union[dict, ParamConfig], *, required_fields: Optional[list] = None) -> None:
        """
        Initializes the NPTM instance with the specified and validated parameters.

        Parameters
        ----------
        param : dict or ParamConfig
            Dictionary containing configuration parameters for NPTM,
            or an already validated ParamConfig object.
            
            Required keys (for the default configuration):
                
            - `"db_nptm_schema"` : str  
                Schema name for the NPTM in the PostgreSQL database.
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
                If True, enables console output for execution status (default is `False`).
            - `"sql_echo"` : bool  
                If True, enables SQL query logging (default is `False`).
                
        required_fields : list, optional
            A list of fields that are required for this specific instance.
            If not provided, defaults to all required fields for the class:  
            `["db_nptm_schema", "db_zones_table", "db_imt_table", "db_pt_table", "uri"]`

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


    def _warn(self, metric: str, summary: str, details: list[str], *, stacklevel: int = 3) -> None:
        """
        Emit a readable, multi-line warning message.
    
        Parameters
        ----------
        metric : str
            Short context label (e.g., "IMT time", "PT length", "Zones").
        summary : str
            One-line summary of the issue.
        details : list[str]
            Bullet points explaining action taken and hints.
        stacklevel : int, optional
            Passed to warnings.warn to point to the caller.
        """
        import warnings
        # Leading newline to visually separate from the "UserWarning:" header
        msg = "\n" + f"{metric} — {summary}\n" + "\n".join(f"• {line}" for line in details)
        warnings.warn(msg, NPTMUserWarning, stacklevel=stacklevel)


    def setup_data(
        self,
        *,
        zones_gdf: gpd.GeoDataFrame,
        imt_mtx_time: pl.DataFrame,
        imt_mtx_length: pl.DataFrame,
        pt_mtx_time: pl.DataFrame,
        pt_mtx_length: pl.DataFrame,
    ) -> NPTM:
        """
        Format and consolidate NPTM travel-time and distance data.
        
        This method validates zone IDs, formats OD matrices for IMT and PT (time and length),
        completes missing (from,to) pairs, and builds final matrices with type labels and a
        two-node path (``[from, to]``). The geographic coordinate system is WGS 84 (EPSG:4326).
        
        Parameters
        ----------
        zones_gdf : geopandas.GeoDataFrame  
            Zones with required columns:  
              - ``id`` : int16 (optional)  
                  New unique IDs for zones; numbering **starts at 1** and must fit in int16 (≤ 32767).  
                  Non-contiguous sequences are allowed (a warning is issued).  
              - ``nptmid`` : legacy zone IDs from the NPTM (format unchanged).  
              - ``geom`` : Column that stores the geometry objects of the zones (shapely.geometry).  
        imt_mtx_time : polars.DataFrame  
            IMT travel times with columns:  
              - ``from`` : legacy origin (``nptmid``)  
              - ``to``   : legacy destination (``nptmid``)  
              - ``value``: travel time (minutes). ``Null`` for no connection.  
        imt_mtx_length : polars.DataFrame  
            IMT travel distances with columns:  
              - ``from`` / ``to`` (legacy ``nptmid``), ``value`` in kilometers. ``null`` for no connection.  
        pt_mtx_time : polars.DataFrame  
            PT travel times (minutes) with the same structure as above.  
        pt_mtx_length : polars.DataFrame  
            PT travel distances (kilometers) with the same structure as above.  
        
        Returns
        -------
        NPTM
            The instance itself (for chaining). Populated attributes:  
              - ``self.imt_mtx`` : polars.DataFrame  
                  Columns: ``from``, ``to``, ``type``, ``time``, ``length``, ``path``.  
              - ``self.pt_mtx``  : polars.DataFrame  
                  Columns: ``from``, ``to``, ``type``, ``time``, ``length``, ``path``.  
              - ``self.zones_gdf`` : geopandas.GeoDataFrame  
                  Reprojected to EPSG:4326.
        
        Raises
        ------
        TypeError
            If the input format of the DataFrames (polars or geopandas) is invalid.  
            If ``id`` is not integer-like in ``gdf_zones``.
        ValueError
            If the GeoDataFrame’s geometry column do not exist.  
            If one or more required columns are missing in the DataFrames.  
            If ``id`` does not start at 1, contains duplicates, or exceeds int16 bounds (``32767``).  
            If conflicting duplicate (from,to) pairs are detected in any OD matrix.

                
        Notes
        -----
        - If the geometry column is not named ``geom``, it is renamed to ``geom`` prior to processing (warning).
        - Rows whose origin/destination do not map to the provided zones are **dropped**
          (warning with the exact count) before duplicate checks and matrix completion.
        - OD matrices are validated and **completed** to cover all (from,to) pairs over the provided zones:
            - duplicates with identical values (within a small tolerance) are **collapsed** (warning);
            - conflicting duplicates **raise** a ``ValueError``;
            - **missing relations** are created with **null** values (directed graph).
        - The graph (class `transnetmap.analysis.graph.Graph`) is directed; no automatic symmetry
          is applied by default. A symmetric fill (``value[from,to] := value[to,from]``) can be enabled
          only if the metric is guaranteed to be symmetric by construction.
        """
        from transnetmap.utils.constant import DCT_TYPE
        from transnetmap.utils.utils import convert_to_pg_array
        
        self._validate_zone_ids(zones_gdf)            
        
        # Prepare zone IDs
        df_id = (
            pl.DataFrame(zones_gdf[['id', 'nptmid']])
            .with_columns(pl.col('id').cast(pl.Int16))
        )
        ids_ref = df_id["id"].unique().sort()
        
        # Format individual motorized transport data
        metric = "IMT time"
        df_imt_time = self._format_individual_OD_matrix(imt_mtx_time, df_id, metric=metric)
        df_imt_time = self._validate_and_complete_od(df_imt_time, ids_ref, metric=metric, symmetric=False)
        df_imt_time = df_imt_time.select(
            pl.col("from").cast(pl.Int16),
            pl.col("to").cast(pl.Int16),
            pl.col("value").alias("time")
        )
        
        metric="IMT length"
        df_imt_length = self._format_individual_OD_matrix(imt_mtx_length, df_id, metric=metric)
        df_imt_length = self._validate_and_complete_od(df_imt_length, ids_ref, metric=metric, symmetric=False)
        df_imt_length = df_imt_length.select(
            pl.col("from").cast(pl.Int16),
            pl.col("to").cast(pl.Int16),
            pl.col("value").alias("length")
        )

        # Format public transport data
        metric="PT time"
        df_pt_time = self._format_individual_OD_matrix(pt_mtx_time, df_id, metric=metric)
        df_pt_time = self._validate_and_complete_od(df_pt_time, ids_ref, metric=metric, symmetric=False)
        df_pt_time = df_pt_time.select(
            pl.col("from").cast(pl.Int16),
            pl.col("to").cast(pl.Int16),
            pl.col("value").alias("time")
        )
        
        metric="PT length"
        df_pt_length = self._format_individual_OD_matrix(pt_mtx_length, df_id, metric=metric)
        df_pt_length = self._validate_and_complete_od(df_pt_length, ids_ref, metric=metric, symmetric=False)
        df_pt_length = df_pt_length.select(
            pl.col("from").cast(pl.Int16),
            pl.col("to").cast(pl.Int16),
            pl.col("value").alias("length")
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
            .then(DCT_TYPE['withoutIMT'])
            .otherwise(DCT_TYPE['IMT'])
            .alias('type')
            .cast(pl.Int8)
        )
        
        pt_mtx = pt_mtx.with_columns(
            pl.when((pl.col('time').is_null()) | (pl.col('length').is_null()))
            .then(DCT_TYPE['withoutPT'])
            .otherwise(DCT_TYPE['PT'])
            .alias('type')
            .cast(pl.Int8)
        )

        # Final matrices
        self.imt_mtx = imt_mtx.select(['from', 'to', 'type', 'time', 'length', 'path'])
        self.pt_mtx = pt_mtx.select(['from', 'to', 'type', 'time', 'length', 'path'])
        self.zones_gdf = zones_gdf.to_crs(4326) # Transformation into WGS 84 geographic coordinates

        print("\nSetup data complete.\n")
        return self


    def _validate_zone_ids(self, zones_gdf: gpd.GeoDataFrame) -> None:
        """Validate zone ``id`` constraints on the input GeoDataFrame.
        
        Parameters
        ----------
        zones_gdf : geopandas.GeoDataFrame
            Zones with at least the column ``id``.
        
        Conditions
        ----------
        - ``id`` starts at 1
        - ``id`` is unique (no duplicates)
        - ``max(id)`` ≤ int16 max (32767)
        - Warn if ``id`` is not contiguous (e.g., 1, 2, 4, 5, ...)
        
        Raises
        ------
        TypeError
            If the input is not a ``geopandas.GeoDataFrame``.  
            If the ``id`` column is not integer-like.
        ValueError
            If one or more required columns are missing (``"id"``, ``"nptmid"``).  
            If ``id`` does not start at 1, contains duplicates, or exceeds int16 bounds (``32767``).  
            If the GeoDataFrame’s geometry column do not exist.
        
        Notes
        -----
        - This method validates only the **format** and **range** of the ``id`` column.  
        - Semantic checks (e.g., geometry validity) are out of scope.  
        - If the GeoDataFrame’s active geometry column name differs from ``geom``, 
        it is automatically renamed to ``geom`` (warning).
        """
        import numpy as np
        import geopandas as gpd
        
        if not isinstance(zones_gdf, gpd.GeoDataFrame):
            raise TypeError("'Zones' must be a geopandas.GeoDataFrame; got type={type(gdf)}")
        
        if not hasattr(zones_gdf, "geometry") or zones_gdf.geometry is None:
            raise ValueError("GeoDataFrame has no active geometry column; use set_geometry(...) first.")
        
        col_geom = zones_gdf.geometry.name
        if col_geom != "geom":
            if "geom" in zones_gdf.columns:
                raise ValueError(
                    "'Zones': Cannot rename geometry column to 'geom' because a non-geometry column named "
                    "'geom' already exists. Rename that column first."
                )
            zones_gdf.rename_geometry("geom", inplace=True)
            self._warn(
                "Zones",
                "Non-compliant geometry column name",
                [
                    f"Active geometry column was '{col_geom}', renamed to 'geom'.",
                    "Action: the geometry column has been renamed to 'geom'.",
                    "Hint: use `gdf_zones.rename_geometry('geom', inplace=True)` to keep it consistent."
                ],
                stacklevel=4,
            )
        
        if "id" not in zones_gdf.columns:
            raise ValueError("Missing 'id' column in gdf_zones.")
        
        if "nptmid" not in zones_gdf.columns:
            raise ValueError("Missing 'nptmid' column in gdf_zones.")
        
        ids = zones_gdf["id"].to_numpy()
        if not np.issubdtype(ids.dtype, np.integer):
            raise TypeError(f"'id' must be integer-like; got dtype={ids.dtype!r}")
    
        ids64 = ids.astype(np.int64, copy=False)
        if ids64.min() != 1:
            raise ValueError(f"'id' must start at 1; got min={int(ids64.min())}.")
    
        if np.unique(ids64).size != ids64.size:
            raise ValueError("'id' contains duplicates.")
    
        int16_max = np.iinfo(np.int16).max  # 32767
        if ids64.max() > int16_max:
            raise ValueError(f"'id' must fit in int16 (<= {int16_max}); got max={int(ids64.max())}.")
    
        # Warn if non-contiguous (gaps)
        expected = np.arange(1, ids64.max() + 1, dtype=np.int64)
        if not np.array_equal(np.sort(ids64), expected):
            self._warn(
                "Zones",
                "Non-contiguous 'id' values (gaps) detected",
                [
                    "This is allowed; you may reindex for compact ids, but it is not required.",
                    "Impact: may slightly increase memory/index sizes."
                ],
                stacklevel=4,
            )
    
    
    def _format_individual_OD_matrix(
        self,
        df: pl.DataFrame,
        df_id: pl.DataFrame,
        *,
        metric: str,
    ) -> pl.DataFrame:
        """Map legacy NPTM IDs to compact int16 IDs; drop out-of-domain rows; cast types.
        
        Behavior
        --------
        1) Maps legacy IDs (``nptmid``) to compact ``id`` (int16) on both ``from`` and ``to``.  
        2) **Drops** rows whose origin/destination cannot be mapped to the provided zones (warn with count).  
        3) Casts dtypes to ``from``/``to`` as Int16 and ``value`` as Float32.  
        
        Parameters
        ----------
        df : polars.DataFrame
            Input OD data with columns: ``from``, ``to``, ``value`` (``from``/``to`` expressed as legacy ``nptmid``).
        df_id : polars.DataFrame
            Two-column mapping ``[["id", "nptmid"]]`` where ``id`` is Int16.
        metric : str
            Metric label used in the warning (e.g., ``"IMT time"``, ``"PT length"``).
        
        Returns
        -------
        polars.DataFrame
            Columns: ``from`` (Int16), ``to`` (Int16), ``value`` (Float32), restricted to known zones.
        
        Raises
        ------
        TypeError
            If ``df`` is not a ``polars.DataFrame``.  
            If enforcing a numeric dtype for ``"value"``.
        ValueError
            If one or more required columns are missing (``"from"``, ``"to"``, ``"value"``).  
            If unexpected extra columns are present (more than the three required).
                
        Notes
        -----
        This function **does not** handle duplicates or matrix completion; these tasks are performed
        by method `_validate_and_complete_od`.
        """
        if not isinstance(df, pl.DataFrame):
            raise TypeError(f"OD matrix '{metric}' must be a `polars.DataFrame`; got type={type(df)}")
        
        required = {"from", "to", "value"}
        cols = set(df.columns)
        
        missing = required - cols
        if missing:
            raise ValueError(
                f"OD matrix '{metric}' must contain columns {sorted(required)}; "
                f"missing: {sorted(missing)}. Got: {df.columns}"
            )
        if len(df.columns) != 3:
            extras = [c for c in df.columns if c not in required]
            raise ValueError(
                f"OD matrix '{metric}' must contain exactly columns {sorted(required)}; "
                f"unexpected extra columns: {extras}. Got: {df.columns}"
            )
        
        if df.schema["value"] not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32):
            raise TypeError("Column 'value' must be numeric.")
        
        df = (
            df.join(df_id, how='left', left_on='from', right_on='nptmid')
              .drop('from')
              .rename({'id': 'from'})
              .with_columns(pl.col('from').cast(pl.Int16))
              .join(df_id, how='left', left_on='to', right_on='nptmid')
              .drop('to')
              .rename({'id': 'to'})
              .with_columns([
                  pl.col('to').cast(pl.Int16),
                  pl.col('value').cast(pl.Float32)
              ])
        )
    
        # Count + remove “out-of-domain” relationships (IDs not present in zones)
        unknown_mask = pl.col('from').is_null() | pl.col('to').is_null()
        n_unknown = df.filter(unknown_mask).height
        if n_unknown:
            self._warn(
                metric,
                "Dropped rows with unknown zone IDs",
                [
                    f"Dropped {n_unknown} row(s) not present in zones (no 'nptmid' match).",
                    "Action: removed these rows before deduplication and matrix completion.",
                    "Hint: ensure legacy 'from'/'to' ids exist in gdf_zones['nptmid']."
                ],
                stacklevel=4,
            )
        df = df.filter(~unknown_mask)
    
        return df.clone()
    
    
    def _validate_and_complete_od(
        self,
        df: pl.DataFrame,
        ids: pl.Series,
        *,
        metric: str,
        symmetric: bool = False,
        tol: float = 1e-6,
    ) -> pl.DataFrame:
        """Validate and complete a directed OD matrix on (from,to) pairs over ``ids``.
        
        Behavior
        -----
        1) **Deduplicate** (from,to):  
            - consistent (max−min ≤ tol) → keep first, warn with count  
            - inconsistent → raise ``ValueError``  
            
        2) **Complete** onto ``ids × ids`` (left join):  
            - missing relations remain ``null`` (warn with the exact count)  
            - if ``symmetric=True``, try the inverse (to,from) fill before leaving ``null``
        
        Parameters
        ----------
        df : polars.DataFrame
            OD data with columns ``from`` (int16), ``to`` (int16), ``value`` (float32).
            Rows referencing unknown zones should already have been dropped upstream.
        ids : polars.Series
            Reference set of zone IDs (int16) defining the cartesian product ``ids × ids``.
        metric : str
            Metric label used in warnings/errors (e.g., ``"IMT time"``, ``"PT length"``).
        symmetric : bool, optional
            If True, attempts to fill missing ``value(from,to)`` from ``value(to,from)`` before leaving it null.
            Use **only** for metrics that are symmetric by construction. Default is False.
        tol : float, optional
            Tolerance to consider duplicate values equal (max−min ≤ tol). Default is 1e−6.
        
        Returns
        -------
        polars.DataFrame
            Columns: ``from`` (Int16), ``to`` (Int16), ``value`` (Float32).
        
        Raises
        ------
        ValueError
            If conflicting duplicate (from,to) pairs are detected.
        """
        
        # Enforce dtypes
        df = df.select(
            pl.col("from").cast(pl.Int16),
            pl.col("to").cast(pl.Int16),
            pl.col("value").cast(pl.Float32),
        )
    
        # 1) Duplicates
        dup = (
            df.group_by(["from", "to"])
              .agg(
                  n=pl.len(),
                  vmin=pl.col("value").min(),
                  vmax=pl.col("value").max(),
              )
              .filter(pl.col("n") > 1)
        )
        if dup.height:
            inconsistent = dup.filter((pl.col("vmax") - pl.col("vmin")) > tol).height
            if inconsistent:
                raise ValueError(
                    f"{metric}: {inconsistent} (from,to) pairs have conflicting values."
                )
            # Collapse duplicates (keep first)
            df = df.unique(subset=["from", "to"], keep="first")
            self._warn(
                metric,
                "Collapsed duplicate (from,to) pairs",
                [
                    f"Collapsed {dup.height} pair(s) equal within tol={tol:g}; kept first occurrence.",
                    "Action: continued with unique pairs only.",
                    "Tip: deduplicate upstream to avoid this warning."
                ],
                stacklevel=4,
            )
        
        # 2) Complete to ids × ids
        ids = pl.Series("id", ids).cast(pl.Int16)
        full = pl.DataFrame({"from": ids}).join(
            pl.DataFrame({"to": ids}), how="cross"
        ).select(pl.col("from"), pl.col("to"))
    
        out = full.join(df, on=["from", "to"], how="left")
    
        # Optional symmetric fill (use with care)
        if symmetric:
            inv = df.select(
                pl.col("to").alias("from"),
                pl.col("from").alias("to"),
                pl.col("value").alias("value_inv"),
            )
            out = (
                out.join(inv, on=["from", "to"], how="left")
                   .with_columns(
                       pl.when(pl.col("value").is_null())
                         .then(pl.col("value_inv"))
                         .otherwise(pl.col("value"))
                         .alias("value")
                   )
                   .drop("value_inv")
            )
    
        # Warn on remaining missing
        n_missing = out.select(pl.col("value").is_null().sum().alias("n")).item()
        if n_missing:
            self._warn(
                metric,
                "Completed ids×ids; missing relations remain null",
                [
                    f"{n_missing} (from,to) pair(s) have null values (directed graph: no connection).",
                    "Action: left as null; downstream logic treats them as absent links.",
                    "Note: enable symmetric fill only for truly symmetric metrics."
                ],
                stacklevel=4,
            )
    
        return out.select(
            pl.col("from").cast(pl.Int16),
            pl.col("to").cast(pl.Int16),
            pl.col("value").cast(pl.Float32),
        )


    def to_sql(self, comments: dict, *, if_exists: str = 'fail') -> None:
        """
        Writes the formatted NPTM data (zones, IMT, PT) to the PostgreSQL database.
    
        Parameters
        ----------
        comments : dict
            Dictionary of comments for the schema and tables. Expected keys:  
                - `'schema'` : str  
                    Comment for the NPTM schema.  
                - `'zones'` : str  
                    Comment for the traffic zones table.  
                - `'IMT'` : str  
                    Comment for the individual motorized transport table.  
                - `'PT'` : str  
                    Comment for the public transport table.  
        if_exists : str, optional
            Determines behavior if the tables already exist in the database (default is `'fail'`).  
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


    def read_sql(
        self,
        table_name: str,
        *,
        columns: Optional[list[str]] = None,
        where_condition: Optional[str] = None,
    ) -> pl.DataFrame | gpd.GeoDataFrame:
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
            table = self._read_sql_zones(table_name, columns=columns, where_condition=where_condition)
        elif table_name in [self.config.db_imt_table, self.config.db_pt_table]:
                table = self._read_sql_data(table_name, columns=columns, where_condition=where_condition)
        else:
            raise ValueError('Invalid or unsupported table name.')
    
        return table


    def _read_sql_data(
        self,
        table_name: str,
        columns: Optional[list[str]] = None,
        where_condition: Optional[str] = None,
    ) -> pl.DataFrame:
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


    def _read_sql_zones(
        self,
        table_name: str,
        columns: Optional[list[str]] = None,
        where_condition: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
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


# -----------------------------------------------------------------------------
# Minimal main (example-only)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print("This module provides the NPTM class. For a full Swiss example, see 'nptm_test.py'.")

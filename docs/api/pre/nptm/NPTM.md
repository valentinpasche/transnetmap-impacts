# transnetmap.pre.NPTM

> **Note**  
> This private methods of `NPTM.setup_data()` are intentionally documented, 
>so you can validate/shape your inputs before calling `NPTM.setup_data()` :  
> - `_validate_zone_ids()`  
> - `_format_individual_OD_matrix()`  
> - `_validate_and_complete_od()`  
> This makes the full behavior of `setup_data()` transparent.

::: transnetmap.pre.nptm.NPTM
    options:
      show_root_heading: true
      show_root_full_path: false
      docstring_style: numpy
      members_order: source
      show_source: true
      show_signature: true
      separate_signature: true
      show_signature_annotations: true
      merge_init_into_class: true
      show_bases: true
      inherited_members: true
      show_submodules: false
      members:
        - setup_data
        - to_sql
        - read_sql
        - _validate_zone_ids
        - _format_individual_OD_matrix
        - _validate_and_complete_od
      filters:
        - "!^_"

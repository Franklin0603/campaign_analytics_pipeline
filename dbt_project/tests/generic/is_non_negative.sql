{%- test is_non_negative(model, column_name) -%}

/*
    Test: is_non_negative
    
    Purpose: Validate that numeric column has no negative values
    
    Usage in schema.yml:
        tests:
          - is_non_negative
    
    Returns: Rows with negative values
*/

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{%- endtest -%}
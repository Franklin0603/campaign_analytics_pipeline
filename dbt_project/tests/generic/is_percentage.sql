{%- test is_percentage(model, column_name) -%}

/*
    Test: is_percentage
    
    Purpose: Validate that a percentage column is between 0 and 100
    
    Usage in schema.yml:
        tests:
          - is_percentage
    
    Returns: Rows where percentage is invalid (< 0 or > 100)
*/

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0 
   or {{ column_name }} > 100

{%- endtest -%}
{%- macro cents_to_dollars(column_name, decimal_places=2) -%}

/*
    Macro: cents_to_dollars
    
    Purpose: Convert cents to dollars (useful if source data is in cents)
    
    Usage:
        {{ cents_to_dollars('cost_cents', 2) }}
    
    Returns: Value divided by 100 and rounded
*/

round(({{ column_name }}::numeric / 100), {{ decimal_places }})

{%- endmacro -%}
{%- macro safe_divide(numerator, denominator, decimal_places=2) -%}

/*
    Macro: safe_divide
    
    Purpose: Safely divide two numbers, returning 0 when denominator is 0
    
    Usage:
        {{ safe_divide('revenue', 'cost', 2) }}
        {{ safe_divide('clicks', 'impressions', 4) }}
    
    Args:
        numerator: Column or value to divide
        denominator: Column or value to divide by
        decimal_places: Number of decimal places (default: 2)
    
    Returns: Rounded result or 0 if denominator is 0
*/

case 
    when {{ denominator }} > 0 
    then round(({{ numerator }}::numeric / {{ denominator }}::numeric), {{ decimal_places }})
    else 0 
end

{%- endmacro -%}
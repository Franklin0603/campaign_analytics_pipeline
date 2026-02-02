{%- macro date_spine(start_date, end_date) -%}

/*
    Macro: date_spine
    
    Purpose: Generate a continuous series of dates between two dates
    
    Usage:
        {{ date_spine('2024-01-01', '2024-12-31') }}
    
    Returns: Table with 'date_day' column
*/

select 
    (date '{{ start_date }}' + generate_series(0, date '{{ end_date }}' - date '{{ start_date }}'))::date as date_day

{%- endmacro -%}
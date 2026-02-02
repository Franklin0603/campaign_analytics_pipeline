{%- test conversions_lte_clicks(model, conversion_column, click_column) -%}

/*
    Test: conversions_lte_clicks
    
    Purpose: Validate business logic - conversions should never exceed clicks
    
    Usage in schema.yml (model-level test):
      tests:
        - conversions_lte_clicks:
            conversion_column: conversions
            click_column: clicks
    
    Returns: Rows where conversions > clicks (invalid data)
*/

select
    {{ conversion_column }},
    {{ click_column }}
from {{ model }}
where {{ conversion_column }} > {{ click_column }}

{%- endtest -%}
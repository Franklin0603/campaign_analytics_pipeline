{%- test clicks_lte_impressions(model, click_column, impression_column) -%}

/*
    Test: clicks_lte_impressions
    
    Purpose: Validate business logic - clicks should never exceed impressions
    
    Usage in schema.yml (model-level test):
      tests:
        - clicks_lte_impressions:
            click_column: clicks
            impression_column: impressions
    
    Returns: Rows where clicks > impressions (invalid data)
*/

select
    {{ click_column }},
    {{ impression_column }}
from {{ model }}
where {{ click_column }} > {{ impression_column }}

{%- endtest -%}
{%- test end_date_after_start_date(model, start_date_column, end_date_column) -%}

/*
    Test: end_date_after_start_date
    
    Purpose: Validate that end_date is after or equal to start_date
    
    Usage in schema.yml (model-level test):
      tests:
        - end_date_after_start_date:
            start_date_column: campaign_start_date
            end_date_column: campaign_end_date
    
    Returns: Rows where end_date < start_date
*/

select
    {{ start_date_column }},
    {{ end_date_column }}
from {{ model }}
where {{ end_date_column }} < {{ start_date_column }}

{%- endtest -%}
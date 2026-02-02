{%- test recency(model, column_name, datepart, interval) -%}

/*
    Test: recency
    
    Purpose: Ensure data is recent (not stale)
    
    Usage in schema.yml:
        tests:
          - recency:
              datepart: day
              interval: 7  # Alert if data is older than 7 days
    
    Returns: Fails if data is not recent
*/

select
    max({{ column_name }}) as most_recent_date,
    current_date - interval '{{ interval }} {{ datepart }}' as threshold_date
from {{ model }}
having max({{ column_name }}) < current_date - interval '{{ interval }} {{ datepart }}'

{%- endtest -%}
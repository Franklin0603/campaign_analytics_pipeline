{%- macro generate_schema_name(custom_schema_name, node) -%}

{#
    Macro: generate_schema_name
    
    Purpose: Override default dbt schema naming for cleaner schemas
    
    Default dbt behavior:
        - Dev: <target_schema>_<custom_schema_name>  (e.g., gold_staging)
        - Prod: <custom_schema_name>                  (e.g., staging)
    
    Our behavior:
        - Always use custom_schema_name directly
        - Ensures staging.*, intermediate.*, core.*, analytics.* in all environments
#}

{%- set default_schema = target.schema -%}

{%- if custom_schema_name is none -%}
    {{ default_schema }}
{%- else -%}
    {{ custom_schema_name | trim }}
{%- endif -%}

{%- endmacro -%}
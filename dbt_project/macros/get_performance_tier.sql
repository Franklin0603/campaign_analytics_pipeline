{%- macro get_performance_tier(roi_percent) -%}

/*
    Macro: get_performance_tier
    
    Purpose: Categorize campaigns into performance tiers based on ROI
    
    Usage:
        {{ get_performance_tier('roi_percent') }}
    
    Business Rules:
        - Excellent: ROI >= 100%
        - Good: ROI >= 50%
        - Fair: ROI >= 0%
        - Poor: ROI < 0%
    
    Returns: Performance tier as string
*/

case 
    when {{ roi_percent }} >= 100 then 'Excellent'
    when {{ roi_percent }} >= 50 then 'Good'
    when {{ roi_percent }} >= 0 then 'Fair'
    when {{ roi_percent }} < 0 then 'Poor'
    else 'Unknown'
end

{%- endmacro -%}
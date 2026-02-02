{%- macro calculate_campaign_kpis(imp_col, click_col, conv_col, cost_col, rev_col) -%}

case 
    when {{ imp_col }} > 0 
    then round(({{ click_col }}::numeric / {{ imp_col }}::numeric) * 100, 4)
    else 0 
end as ctr_percent,

case 
    when {{ click_col }} > 0 
    then round(({{ conv_col }}::numeric / {{ click_col }}::numeric) * 100, 4)
    else 0 
end as cvr_percent,

case 
    when {{ click_col }} > 0 
    then round({{ cost_col }}::numeric / {{ click_col }}::numeric, 2)
    else 0 
end as cpc_usd,

case 
    when {{ imp_col }} > 0 
    then round(({{ cost_col }}::numeric / {{ imp_col }}::numeric) * 1000, 2)
    else 0 
end as cpm_usd,

case 
    when {{ conv_col }} > 0 
    then round({{ cost_col }}::numeric / {{ conv_col }}::numeric, 2)
    else 0 
end as cpa_usd,

round(({{ rev_col }} - {{ cost_col }})::numeric, 2) as profit_usd,

case 
    when {{ cost_col }} > 0 
    then round((({{ rev_col }} - {{ cost_col }})::numeric / {{ cost_col }}::numeric) * 100, 2)
    else 0 
end as roi_percent,

case 
    when {{ rev_col }} > 0 
    then round(({{ cost_col }}::numeric / {{ rev_col }}::numeric) * 100, 2)
    else null 
end as cost_revenue_ratio_percent,

case 
    when {{ conv_col }} > 0 
    then round({{ rev_col }}::numeric / {{ conv_col }}::numeric, 2)
    else 0 
end as revenue_per_conversion_usd,

case 
    when {{ click_col }} > 0 
    then round({{ rev_col }}::numeric / {{ click_col }}::numeric, 2)
    else 0 
end as revenue_per_click_usd

{%- endmacro -%}
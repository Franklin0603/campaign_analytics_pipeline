{% snapshot advertisers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='advertiser_id',
      strategy='timestamp',
      updated_at='silver_loaded_at',
      invalidate_hard_deletes=True
    )
}}

/*
    Snapshot: Track advertiser attribute changes over time
    
    Purpose: Capture historical changes to advertiser data
    Use Case: Track when advertisers change industry, country, or other attributes
    
    Strategy: Timestamp-based (tracks changes when silver_loaded_at changes)
*/

select * from {{ ref('stg_advertisers') }}

{% endsnapshot %}
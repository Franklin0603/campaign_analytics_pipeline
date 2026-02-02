{% snapshot campaigns_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='campaign_id',
      strategy='timestamp',
      updated_at='silver_loaded_at',
      invalidate_hard_deletes=True
    )
}}

/*
    Snapshot: Track campaign status changes over time
    
    Purpose: Capture historical changes to campaign attributes
    Use Case: Answer questions like "When did this campaign's status change?"
    
    Strategy: Timestamp-based (tracks changes when silver_loaded_at changes)
*/

select * from {{ ref('stg_campaigns') }}

{% endsnapshot %}
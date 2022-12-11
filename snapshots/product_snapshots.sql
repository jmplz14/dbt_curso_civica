{% snapshot product_snapshots %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='check',
      check_cols=['price'],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('sql_server_dbo', 'products') }}

{% endsnapshot %}
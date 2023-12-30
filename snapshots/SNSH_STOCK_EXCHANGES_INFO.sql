{% snapshot SNSH_STOCK_EXCHANGES_INFO %}

{{
    config(
      unique_key= 'STOCK_EXCHANGES_HKEY',
      strategy='check',
      check_cols=['STOCK_EXCHANGES_HDIFF']
    )
}}

select * from {{ ref('STG_STOCK_EXCHANGES_INFO') }}

{% endsnapshot %}
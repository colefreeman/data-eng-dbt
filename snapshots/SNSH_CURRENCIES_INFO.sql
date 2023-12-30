{% snapshot SNSH_CURRENCIES_INFO %}

{{
    config(
      unique_key= 'CURRENCIES_HKEY',
      strategy='check',
      check_cols=['CURRENCIES_HDIFF']
    )
}}

select * from {{ ref('STG_CURRENCIES_INFO') }}

{% endsnapshot %}
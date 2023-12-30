{% snapshot SNSH_COUNTRIES_INFO %}

{{
    config(
      unique_key= 'COUNTRIES_HKEY',
      strategy='check',
      check_cols=['COUNTRIES_HDIFF']
    )
}}

select * from {{ ref('STG_COUNTRIES_INFO') }}

{% endsnapshot %}
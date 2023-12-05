{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
          country_name as COUNTRY_NAME --TEXT
        , country_code_2_letter as COUNTRY_CODE_TWO_LETTER --TEXT
        , country_code_3_letter as COUNTRY_CODE_THREE_LETTER --TEXT
        , country_code_numeric as NUMERIC_COUNTRY_CODE --NUMBER
        , iso_3166_2 as ISO_3166_2_CODE --TEXT
        , region as REGION --TEXT
        , sub_region as SUB_REGION --TEXT
        , intermediate_region as INTERMEDIATE_REGION --TEXT
        , region_code as REGION_CODE --NUMBER
        , sub_region_code as SUB_REGION_CODE --NUMBER
        , intermediate_region_code as INTERMEDIATE_REGION_CODE --NUMBER
        , LOAD_TS as LOAD_TS --TIMESTAMP_NTZ
        , 'SEED.COUNTRIES_INFO' as RECORD_SOURCE

FROM {{ source('seeds', 'COUNTRIES_INFO') }}

),

default_record as (
    SELECT
          'missing' as COUNTRY_NAME
        , 'missing' as COUNTRY_CODE_TWO_LETTER
        , 'missing' as COUNTRY_CODE_THREE_LETTER
        , '-1' as NUMERIC_COUNTRY_CODE
        , 'missing' as ISO_3166_2_CODE
        , 'missing' as REGION
        , 'missing' as SUB_REGION
        , 'missing' as INTERMEDIATE_REGION
        , '-1' as REGION_CODE
        , '-1' as SUB_REGION_CODE
        , '-1' as INTERMEDIATE_REGION_CODE
        , '2020-01-01' as LOAD_TS
        , 'system.DefaultKey' as RECORD_SOURCE
),

with_default_record as (
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_record
),

hashed as (
    SELECT
          {{ dbt_utils.surrogate_key(['COUNTRY_NAME', 'COUNTRY_CODE_TWO_LETTER']) }} as COUNTRIES_HKEY
        , {{ dbt_utils.surrogate_key(['COUNTRY_NAME', 'COUNTRY_CODE_TWO_LETTER', 'COUNTRY_CODE_THREE_LETTER', 'NUMERIC_COUNTRY_CODE', 'SUB_REGION_CODE']) }} as COUNTRIES_HDIFF
        , *
        , '{{ run_started_at }}' as LOAD_TS_UTC
    FROM src_data
)

SELECT * FROM hashed


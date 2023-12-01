{{ config(materialized='ephemeral') }}

WITH
src_data as (
    SELECT
          AlphabeticCode as ALPHABETIC_CODE --TEXT
        , NumericCode as NUMERIC_CODE --NUMBER
        , DecimalDigits as DECIMAL_PLACES --NUMBER
        , CurrencyName as CURRENCY_NAME --TEXT
        , Locations as COUNTRIES_PLACES_USED --TEXT
        , LOAD_TS as LOAD_TS ----TIMESTAMP_NTZ
        , 'SEED.CURRENCIES_INFO' as RECORD_SOURCE

FROM {{ source('seeds', 'CURRENCIES_INFO') }}

),

default_record as (
    SELECT
          'Missing' as ALPHABETIC_CODE
        , '-1' as NUMERIC_CODE
        , '-1' as DECIMAL_PLACES
        , 'Missing' as CURRENCY_NAME
        , 'Missing' as COUNTRIES_PLACES_USED
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
          concat_ws('|', ALPHABETIC_CODE, NUMERIC_CODE) as CURRENCIES_HKEY
        , concat_ws('|', ALPHABETIC_CODE, NUMERIC_CODE, CURRENCY_NAME) as CURRENCIES_HDIFF
        , * EXCLUDE LOAD_TS
        , '{{ run_started_at }}' as LOAD_TS_UTC
    FROM with_default_record
)

SELECT * FROM hashed





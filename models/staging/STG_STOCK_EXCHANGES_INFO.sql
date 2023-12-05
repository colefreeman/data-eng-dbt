{{ config(materialized='ephemeral') }}

WITH 
src_data as (
    SELECT
          Name as STOCK_EXCHANGE_NAME --TEXT
        , ID as EXCHANGE_CODE --TEXT
        , Country as COUNTRY_LOCATED --TEXT
        , City as CITY_LOCATED --TEXT
        , Zone as ZONE_LOCATED --TEXT
        , Delta as DELTA --NUMBER
        , DST_period as DST_PERIOD --TEXT
        , Open as OPENING_TIME --TEXT
        , Close as CLOSING_TIME --TEXT
        , Lunch as LUNCH_TIME --TEXT
        , Open_UTC as OPEN_TIMESTAMP --TIMESTAMP_NTZ
        , Close_UTC as CLOSE_TIMESTAMP --TIMESTAMP_NTZ
        , Lunch_UTC as LUNCH_TIMESTAMP--TIMESTAMP_NTZ
        , LOAD_TS as LOAD_TS --TIMESTAMP_NTZ
        , 'SEED.STOCK_EXCHANGES_INFO' as RECORD_SOURCE

FROM {{ source('seeds', 'STOCK_EXCHANGES_INFO') }}

),

default_record as (
    SELECT
      'Missing' as STOCK_EXCHANGE_NAME
    , 'Missing' as EXCHANGE_CODE
    , 'Missing' as COUNTRY_LOCATED
    , 'Missing' as CITY_LOCATED
    , 'Missing' as ZONE_LOCATED
    , '-1' as DELTA
    , 'Missing' as DST_PERIOD
    , 'Missing' as OPENING_TIME
    , 'Missing' as CLOSING_TIME
    , 'Missing' as LUNCH_TIME
    , '2020-01-01' as OPEN_TIMESTAMP
    , '2020-01-01' as CLOSE_TIMESTAMP
    , '2020-01-01' as LUNCH_TIMESTAMP
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
          {{ dbt_utils.surrogate_key(['STOCK_EXCHANGE_NAME', 'EXCHANGE_CODE']) }} as STOCK_EXCHANGES_HKEY
        , {{ dbt_utils.surrogate_key(['STOCK_EXCHANGE_NAME', 'EXCHANGE_CODE', 'COUNTRY_LOCATED', 'CITY_LOCATED']) }} as STOCK_EXCHANGES_HDIFF
        , *
        , '{{ run_started_at }}' as LOAD_TS_UTC
    FROM src_data
)

SELECT * FROM hashed
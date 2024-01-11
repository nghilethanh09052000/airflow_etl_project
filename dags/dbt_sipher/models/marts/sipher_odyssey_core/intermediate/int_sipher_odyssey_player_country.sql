{{- config(
  materialized='incremental',
  unique_key='device_sk'
) -}}

WITH raw AS (
  SELECT
    *,
    {{ get_string_value_from_user_properties(key="ather_id") }} AS ather_id,
  FROM {{ ref('stg_firebase__sipher_odyssey_events_all_time') }}
)
,
geo AS (
  SELECT 
    
  FROM raw
)
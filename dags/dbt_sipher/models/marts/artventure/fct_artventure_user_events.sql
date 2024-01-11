{{- config(
    materialized='table'
) -}}

WITH raw AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date,
        TIMESTAMP_MICROS(event_timestamp) AS timestamp,
        user_id,
        user_pseudo_id,
        event_name,
        event_params
    FROM {{ ref('stg_firebase__artventure_events_all_time') }}
    WHERE event_name NOT IN (
        'task_registered',
        'task_executing',
        'task_executed'
    )
)
,user_events AS (
    SELECT
        *,
        CASE WHEN 
            {{ get_string_value_from_event_params(key="page_location") }} like '%alpha%'
            OR REGEXP_CONTAINS({{ get_string_value_from_event_params(key="page_location") }}, 
                r"https://artventure.ai/ai-recipes/[^/]+")
            THEN 'alpha' ELSE 'internal'
        END AS version
    FROM raw
)

SELECT * FROM user_events
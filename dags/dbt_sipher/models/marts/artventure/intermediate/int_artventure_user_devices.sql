{{- config(
  materialized='incremental',
  unique_key='device_sk',
  merge_update_columns = [
    'user_pseudo_ids',
    'last_used_at',
    'operating_system_version',
    'browser',
    'browser_version',
    'web_info_browser',
    'web_info_browser_version',
    'web_info_hostname'
  ],
) -}}

WITH raw AS (
    SELECT
        *,
        {{ get_string_value_from_event_params(key="user_email") }} AS user_email,
    FROM {{ ref('stg_firebase__artventure_events_all_time') }}
    WHERE event_name = 'click'
        AND {{ get_string_value_from_event_params(key="event_label") }} = 'sign_in_with_google'
)

,device_properties AS (
    SELECT
        user_pseudo_id,
        {{ dbt_utils.generate_surrogate_key([
            'device.category',
            'device.operating_system'
        ]) }} AS _device_model_key,
        MAX(TIMESTAMP_MICROS(event_timestamp)) AS last_used_at,
        MAX(device.mobile_brand_name) AS mobile_brand_name,
        MAX(device.category) AS category,
        MAX(device.mobile_model_name) AS mobile_model_name,
        MAX(device.mobile_marketing_name) AS mobile_marketing_name,
        MAX(device.mobile_os_hardware_model) AS mobile_os_hardware_model,
        MAX(device.operating_system) AS operating_system,
        MAX(device.operating_system_version) AS operating_system_version,
        MAX(device.browser) AS browser,
        MAX(device.browser_version) AS browser_version,
        MAX(device.web_info.browser) AS web_info_browser,
        MAX(device.web_info.browser_version) AS web_info_browser_version,
        MAX(device.web_info.hostname) AS web_info_hostname,
    FROM raw
    GROUP BY user_pseudo_id, _device_model_key
)

,map_device_to_user_id AS (
    SELECT
        user_id,
        user_email,
        ARRAY_AGG(DISTINCT user_pseudo_id) AS user_pseudo_ids
    FROM raw
    WHERE user_id IS NOT NULL
    GROUP BY user_id, user_email
)

,final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'user_id',
            'user_email',
            '_device_model_key'
        ]) }} AS device_sk,
        user_id,
        user_email,
        _device_model_key,
        ARRAY_AGG(DISTINCT user_pseudo_id IGNORE NULLS) AS user_pseudo_ids,
        MAX(last_used_at) AS last_used_at,
        MAX(category) AS category,
        MAX(mobile_brand_name) AS mobile_brand_name,
        MAX(mobile_model_name) AS mobile_model_name,
        MAX(mobile_marketing_name) AS mobile_marketing_name,
        MAX(mobile_os_hardware_model) AS mobile_os_hardware_model,
        MAX(operating_system) AS operating_system,
        MAX(operating_system_version) AS operating_system_version,
        MAX(browser) AS browser,
        MAX(browser_version) AS browser_version,
        MAX(web_info_browser) AS web_info_browser,
        MAX(web_info_browser_version) AS web_info_browser_version,
        MAX(web_info_hostname) AS web_info_hostname
    FROM map_device_to_user_id, UNNEST(user_pseudo_ids) AS user_pseudo_id_map
    LEFT JOIN device_properties ON device_properties.user_pseudo_id = user_pseudo_id_map
    GROUP BY user_id, user_email, _device_model_key
)


{% if is_incremental() -%}
,current_ids_and_new_ids AS (
    SELECT
        device_sk,
        user_pseudo_id
    FROM {{ this }}
    LEFT JOIN UNNEST(user_pseudo_ids) AS user_pseudo_id

    UNION DISTINCT

    SELECT
        device_sk,
        user_pseudo_id
    FROM final
    LEFT JOIN UNNEST(user_pseudo_ids) AS user_pseudo_id
)

,combined_id AS (
    SELECT
        device_sk,
        ARRAY_AGG(DISTINCT user_pseudo_id IGNORE NULLS) AS user_pseudo_ids,
    FROM current_ids_and_new_ids
    GROUP BY device_sk
)

SELECT
    final.*
    EXCEPT(_device_model_key)
    REPLACE (
        combined_id.user_pseudo_ids AS user_pseudo_ids
    ),
    {{ load_metadata(sources=[ref('stg_firebase__artventure_events_all_time')]) }} AS load_metadata
FROM final
LEFT JOIN combined_id USING(device_sk)

{%- else -%}

SELECT
    * EXCEPT(_device_model_key),
    {{ load_metadata(sources=[ref('stg_firebase__artventure_events_all_time')]) }} AS load_metadata
FROM final

{%- endif -%}
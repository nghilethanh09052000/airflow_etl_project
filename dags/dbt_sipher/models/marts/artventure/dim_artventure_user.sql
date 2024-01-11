{{- config(
    materialized='table',
) -}}

WITH devices AS (
    SELECT user_id,
        user_email,
        ARRAY_AGG(
            STRUCT(
                last_used_at,
                user_pseudo_ids,
                category,
                mobile_brand_name,
                mobile_model_name,
                mobile_marketing_name,
                mobile_os_hardware_model,
                operating_system,
                operating_system_version,
                browser,
                browser_version,
                web_info_browser,
                web_info_browser_version,
                web_info_hostname
            )
        ) AS device
    FROM {{ ref('int_artventure_user_devices') }}
    GROUP BY user_id, user_email
)
SELECT *
FROM devices
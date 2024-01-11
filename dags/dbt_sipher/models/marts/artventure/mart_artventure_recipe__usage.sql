{{- config(
    alias='mart_artventure_recipe__usage',
    materialized='table',
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
)-}}

WITH raw_generate AS (
    SELECT
        COALESCE(
            {{ get_string_value_from_event_params(key="recipe_name") }},
            {{ get_string_value_from_event_params(key="recipe_id") }}
        ) as recipe,
        {{ get_string_value_from_event_params(key="template") }} AS template,
        {{ get_string_value_from_event_params(key="gender") }} AS gender,
        {{ get_string_value_from_event_params(key="age") }} AS age,
        {{ get_string_value_from_event_params(key="theme") }} AS theme,
        {{ get_string_value_from_event_params(key="style") }} AS style,
        {{ get_string_value_from_event_params(key="backgroundColor") }} AS background_color,
        {{ get_int_value_from_event_params(key="shapePrecisionLevel") }} AS shape_precision_level,
        COALESCE(
            {{ get_int_value_from_event_params(key="creative_intensity") }},
            {{ get_double_value_from_event_params(key="creative_intensity") }},
            {{ get_int_value_from_event_params(key="creativeIntensity") }},
            {{ get_double_value_from_event_params(key="creativeIntensity") }}
        ) AS creative_intensity,
        COALESCE(
            {{ get_int_value_from_event_params(key="similarity") }},
            {{ get_double_value_from_event_params(key="similarity") }}
        ) AS similarity,
        {{ get_string_value_from_event_params(key="aspectRatio") }} AS aspect_ratio,
        {{ get_int_value_from_event_params(key="upscaleRatio") }} AS upscale_ratio,
        {{ get_int_value_from_event_params(key="denoising_strength") }} AS denoising_strength,
        {{ get_string_value_from_event_params(key="shapeControl") }} AS shape_control,
        {{ get_string_value_from_event_params(key="modelHash") }} AS model_hash,
        {{ get_string_value_from_event_params(key="pose") }} AS pose,
        {{ get_int_value_from_event_params(key="duration") }} AS duration,
        version,
        date,
        CASE WHEN user_id = 'anonymous' THEN user_pseudo_id ELSE user_id END AS user_id,
    FROM {{ ref("fct_artventure_user_events") }}
    WHERE event_name = 'click'
        AND {{ get_string_value_from_event_params(key="event_label") }} 
            IN ('recipe_alpha_generate', 'recipe-generate')
)
,reporting AS (
    SELECT
        *
    FROM raw_generate
    WHERE recipe IS NOT NULL
)

SELECT * FROM reporting

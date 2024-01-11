{{- config(
  materialized ='table',
  cluster_by = ['user_id', 'item_category']
) -}}

WITH character AS(
  SELECT 
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    int.item_sub_type AS item_type,
    dim.item_name AS item_sub_type,
    dim.race AS race,
    int.rarity AS rarity,
    int.tier AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_character` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_character` dim
  ON dim.item_id = int.item_sub_type AND dim.item_category = int.item_type
  WHERE int.item_type = 'Character'
  ORDER BY item_sub_type, instance_id
)

, blueprint AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_name AS item_type,
    dim.item_type AS item_sub_type,
    CAST(NULL AS STRING) AS race,
    COALESCE(int.rarity, dim.rarity) AS rarity,
    COALESCE(int.tier, CAST(dim.tier AS INT64)) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_blueprint` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_blueprint` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('Blueprint', 'Currency')
  ORDER BY item_type DESC
)

, em_shards AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    CAST(NULL AS STRING) AS item_sub_type,
    dim.race AS race,
    CAST(NULL AS STRING) AS rarity,
    int.tier AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_em_shards` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_em_shards` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('EM Shards', 'Currency')
  ORDER BY item_type DESC
)

, em_cores AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    CAST(NULL AS STRING) AS item_sub_type,
    dim.race AS race,
    CAST(NULL AS STRING) AS rarity,
    int.tier AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_em_cores` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_em_cores` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('EM Cores', 'Currency')
  ORDER BY item_type DESC
)

, character_exp AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    CAST(NULL AS STRING) AS item_sub_type,
    CAST(NULL AS STRING) AS race,
    CAST(NULL AS STRING) AS rarity,
    CAST(NULL AS INT64) AS tier,
    int.level,
    CAST(NULL AS INT64) AS ps,
    CAST(NULL AS STRING) AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_character_exp` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_character_exp` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('CharacterExp', 'Currency')
  ORDER BY item_type DESC
)

, rarity_core AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    CAST(NULL AS STRING) AS item_sub_type,
    CAST(NULL AS STRING)  AS race,
    COALESCE(dim.rarity, int.rarity) AS rarity,
    COALESCE(CAST(dim.tier AS INT64), int.tier) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_rarity_core` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_rarity_core` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('RarityCore', 'Currency')
  ORDER BY item_type DESC
)

, currency AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    CAST(NULL AS STRING) AS item_sub_type,
    CAST(NULL AS STRING)  AS race,
    CAST(NULL AS STRING) AS rarity,
    CAST(NULL AS INT64) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_currency` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_currency` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('Currency', 'ConsumableItem')
  ORDER BY item_type DESC
)

, capsule AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    int.item_sub_type AS item_sub_type,
    CAST(NULL AS STRING)  AS race,
    CAST(NULL AS STRING) AS rarity,
    CAST(NULL AS INT64) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_capsule` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_capsule` dim
  ON dim.item_id = int.instance_id 
  WHERE int.item_type IN ('Currency', 'Capsule')
  ORDER BY item_type DESC
)

-- , banner_capsule AS(
--   SELECT
--     int.user_id,
--     int.instance_id,
--     int.item_code,
--     int.item_type AS item_category,
--     dim.item_category AS item_type,
--     int.item_sub_type AS item_sub_type,
--     CAST(NULL AS STRING)  AS race,
--     CAST(NULL AS STRING) AS rarity,
--     CAST(NULL AS INT64) AS tier,
    -- int.level,
--     int.ps AS ps,
--     int.boost AS boost,
--     int.updated_balance,
--     int.updated_balance_date,
--     int.updated_balance_timestamp,
--   FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
--   LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_banner_capsule` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_meta_banner_capsule` dim
--   ON dim.item_id = int.instance_id 
--   WHERE int.item_type IN ('Currency', 'Capsule')
--   ORDER BY item_type DESC
-- )

, software AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    int.item_sub_type AS item_sub_type,
    CAST(NULL AS STRING)  AS race,
    COALESCE(dim.rarity, int.rarity) AS rarity,
    CAST(NULL AS INT64) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_software` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_software` dim
  ON dim.item_id = int.item_code 
  WHERE int.item_type IN ('Software')
  ORDER BY item_type DESC
)

, weapon AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.weapon_type AS item_type,
    dim.weapon_subtype AS item_sub_type,
    CAST(NULL AS STRING)  AS race,
    COALESCE(dim.rarity, int.rarity) AS rarity,
    COALESCE(CAST(dim.tier AS INT64), int.tier) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_weapon` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_weapon` dim
  ON dim.item_id = int.item_code 
  WHERE int.item_type IN ('Weapon')
  ORDER BY item_type DESC
)

, weapon_part AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.weapon_type AS item_type,
    COALESCE(int.item_sub_type, dim.part_type) AS item_sub_type,
    CAST(NULL AS STRING)  AS race,
    COALESCE(dim.rarity, int.rarity) AS rarity,
    COALESCE(CAST(dim.tier AS INT64), int.tier) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_weapon_part` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_weapon_part` dim
  ON dim.item_id = int.item_code 
  WHERE int.item_type IN ('Part')
  ORDER BY item_type DESC
)

, gear AS(
  SELECT
    int.user_id,
    int.instance_id,
    int.item_code,
    int.item_type AS item_category,
    dim.item_category AS item_type,
    COALESCE(int.item_sub_type, dim.gear_type) AS item_sub_type,
    dim.race AS race,
    COALESCE(dim.rarity, int.rarity) AS rarity,
    COALESCE(CAST(dim.tier AS INT64), int.tier) AS tier,
    int.level,
    int.ps AS ps,
    int.boost AS boost,
    int.updated_balance,
    int.updated_balance_date,
    int.updated_balance_timestamp,
  FROM {{ ref('int_user_inventory_balance_latest_update_by_instance') }} int
  LEFT JOIN `sipher-data-platform.reporting_game_meta.dim_sipher_meta_gear` dim
--   LEFT JOIN `sipher-data-testing.reporting_game_meta.dim_sipher_meta_gear` dim
  ON dim.item_id = int.item_code 
  WHERE int.item_type IN ('Gear')
  ORDER BY item_type DESC
)

, union_data AS(
  SELECT * FROM character
  UNION ALL
  SELECT * FROM blueprint WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM em_shards WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM em_cores WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM character_exp WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM rarity_core WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM currency WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM capsule WHERE item_type IS NOT NULL
  UNION ALL
  SELECT * FROM software
  UNION ALL
  SELECT * FROM weapon
  UNION ALL
  SELECT * FROM weapon_part
  UNION ALL
  SELECT * FROM gear
)


SELECT *,
DATE('{{ var("ds")}}') AS snapshot_date
-- '{{ dbt_airflow_macros.ds(timezone="UTC") }}' AS snapshot_date
FROM union_data
{{- config(
    materialized='incremental',
	merge_update_columns = [
    'user_id',
    'session_id'
  ],
	partition_by={
		'field': 'day0_date_tzutc',
		'data_type': 'DATE',
	},
	cluster_by=['day_diff']
  )-}}

WITH raw AS
(
	SELECT  *
	FROM {{ ref('stg_firebase__sipher_odyssey_events_14d') }}
) 

, cohort_user AS
(
	SELECT DISTINCT 
		ather_id
		,email
		,user_id
		,user_name
		,MIN(day0_date_tzutc) AS day0_date_tzutc
	FROM {{ ref('int_sipher_odyssey_player_day0_version') }}
	GROUP BY  1,2,3,4
) 

, login_start_raw AS
(
	SELECT DISTINCT 
		user_id
		,event_name
		,{{ get_string_value_from_event_params(key="build_number") }} AS build_number 
		,app_info.version AS app_version
		,MIN (event_timestamp) AS current_build_timestamp
	FROM raw
	WHERE event_name = 'login_start'
	GROUP BY  user_id
	         ,event_name
	         ,build_number
	         ,app_version
) 

, login_start AS
(
	SELECT DISTINCT 
		login_start_raw.user_id
		,ather_id
		,cohort.day0_date_tzutc                                                                                       AS day0_date_tzutc
		,event_name
		,build_number
		,app_version
		,email
		,user_name
		,current_build_timestamp
		,LEAD(current_build_timestamp,1) OVER (PARTITION BY login_start_raw.user_id ORDER BY current_build_timestamp) AS next_build_timestamp
	FROM login_start_raw
	JOIN cohort_user cohort
	ON (login_start_raw.user_id = cohort.user_id)
) 

, gameplay_start AS
(
	SELECT DISTINCT 
		event_date
		,event_timestamp
		,COALESCE(raw.user_pseudo_id)                                          AS user_pseudo_id
		,raw.user_id                                                           AS user_id
		,cohort.day0_date_tzutc                                                AS day0_date_tzutc
		,date_diff(PARSE_DATE('%Y%m%d',event_date),cohort.day0_date_tzutc,day) AS day_diff
		,event_name
		,{{ get_string_value_from_event_params(key="session_id") }} AS session_id
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.dungeon_id' )) AS dungeon_id
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="run_info")}}, '$.room_id' )) AS room_id
		,UPPER({{ get_string_value_from_event_params(key="mode") }}) AS mode
		,UPPER({{ get_string_value_from_event_params(key="difficulty") }}) AS difficulty
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.race' )) AS race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.sub_race' )) AS sub_race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.level' )) AS character_level
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.PS' )) AS character_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="armor")}}, '$.id' )) AS armor
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="armor")}}, '$.PS' )) AS armor_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="head")}}, '$.id' )) AS head
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="head")}}, '$.PS' )) AS head_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="shoes")}}, '$.id' )) AS shoes
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="shoes")}}, '$.PS' )) AS shoes_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="legs")}}, '$.id' )) AS legs
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="legs")}}, '$.PS' )) AS legs_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gloves")}}, '$.id' )) AS gloves
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gloves")}}, '$.PS' )) AS gloves_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_1")}}, '$.id' )) AS weapon1
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_1")}}, '$.PS' )) AS weapon1_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_2")}}, '$.id' )) AS weapon2
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="weapon_2")}}, '$.PS' )) AS weapon2_PS
	FROM raw , UNNEST
	(event_params
	) AS ep
	JOIN cohort_user cohort
	ON raw.user_id = cohort.user_id
	WHERE event_name IN ('gameplay_start')
	ORDER BY event_date DESC, user_pseudo_id, event_timestamp 
) 

, gameplay_end AS(
	SELECT  DISTINCT 
		event_date
		,event_timestamp
		,COALESCE(raw.user_pseudo_id)                                          AS user_pseudo_id
		,raw.user_id                                                           AS user_id
		,cohort.day0_date_tzutc                                                AS day0_date_tzutc
		,date_diff(PARSE_DATE('%Y%m%d',event_date),cohort.day0_date_tzutc,day) AS day_diff
		,event_name
		,{{ get_string_value_from_event_params(key="session_id") }} AS session_id
		,UPPER({{ get_string_value_from_event_params(key="mode") }}) AS mode
		,{{ get_double_value_from_event_params(key="player_amount") }} AS player_amount
		,({{ get_double_value_from_event_params(key="player_remain") }}) AS player_remain
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.race' )) AS race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.sub_race' )) AS sub_race
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.level' )) AS character_level
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="player_character")}}, '$.PS' )) AS character_PS
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.status' )) AS status
		,SPLIT(UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.level_count' )), '/')[OFFSET(0)] AS level_count
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="gameplay_result")}}, '$.time_played' )) AS time_played
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}}, '$.get_hit' )) AS get_hit
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}}, '$.skill' )) AS skill_count
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}}, '$.dash' )) AS dash_count
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="action_count")}}, '$.changeweapon' )) AS changeweapon_count
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}}, '$.skill' )) AS skill
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}}, '$.ranged' )) AS ranged
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}}, '$.meele' )) AS meele
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="damage_dealt")}}, '$.status_effect' )) AS status_effect
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}}, '$.enemy' )) AS enemy
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}}, '$.down' )) AS down
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}}, '$.revive' )) AS revive
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}}, '$.being_revived' )) AS being_revived
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}}, '$.hp_loss' )) AS hp_loss
		,UPPER(JSON_EXTRACT_SCALAR ({{ get_string_value_from_event_params(key="meta_count")}}, '$.hp_pct' )) AS hp_pct
	FROM raw
	JOIN cohort_user cohort
	ON raw.user_id = cohort.user_id
	WHERE event_name IN ('gameplay_end')
	ORDER BY event_date DESC, user_pseudo_id, event_timestamp 
) 

, gameplay_final AS
(
	SELECT  DISTINCT 
		gameplay_start.event_date                                                   		  AS gameplay_start_event_date
		,gameplay_start.event_timestamp                                                       AS gameplay_start_event_timestamp
		,login_start.build_number                                                             AS build_number
		,login_start.app_version
		,login_start.email
		,user_name

		,gameplay_start.user_pseudo_id                                                        AS user_pseudo_id
		,COALESCE(gameplay_start.user_id,gameplay_end.user_id)                                AS user_id
		,COALESCE(gameplay_start.day0_date_tzutc,gameplay_end.day0_date_tzutc)                AS day0_date_tzutc
		,COALESCE(gameplay_start.day_diff,gameplay_end.day_diff)                              AS day_diff
		,gameplay_start.event_name                                                            AS gameplay_start_event_name
		,gameplay_start.session_id                                                            AS session_id
		,CASE 
			WHEN CAST(login_start.build_number AS INT64) >= CAST(dim_dungeon.build_number AS INT64)
				AND login_start.app_version >= dim_dungeon.app_version 
			THEN UPPER(dim_dungeon.rename_dungeon_id)  
			ELSE gameplay_start.dungeon_id 
		END 																				  AS dungeon_id
		,gameplay_start.room_id                                                               AS room_id
		,COALESCE(gameplay_start.mode,gameplay_end.mode)                                      AS mode
		,gameplay_start.difficulty                                                            AS difficulty
		,COALESCE(gameplay_start.race,gameplay_end.race)                                      AS race
		,COALESCE(gameplay_start.sub_race,gameplay_end.sub_race)                              AS sub_race
		,CAST(COALESCE(gameplay_start.character_level,gameplay_end.character_level) AS INT64) AS character_level
		,CAST(COALESCE(gameplay_start.character_PS,gameplay_end.character_PS) AS INT64) - CAST(gameplay_start.armor_PS AS INT64) - CAST(gameplay_start.head_PS AS INT64) - CAST(gameplay_start.shoes_PS AS INT64) - CAST(gameplay_start.legs_PS AS INT64) - CAST(gameplay_start.gloves_PS AS INT64) - CAST(gameplay_start.weapon1_PS AS INT64) - CAST(gameplay_start.weapon2_PS AS INT64) AS character_PS
		,gameplay_start.armor                                                                 AS armor
		,CAST(gameplay_start.armor_PS AS INT64)                                               AS armor_PS
		,gameplay_start.head                                                                  AS head
		,CAST(gameplay_start.head_PS AS INT64)                                                AS head_PS
		,gameplay_start.shoes                                                                 AS shoes
		,CAST(gameplay_start.shoes_PS AS INT64)                                               AS shoes_PS
		,gameplay_start.legs                                                                  AS legs
		,CAST(gameplay_start.legs_PS AS INT64)                                                AS legs_PS
		,gameplay_start.gloves                                                                AS gloves
		,CAST(gameplay_start.gloves_PS AS INT64)                                              AS gloves_PS
		,gameplay_start.weapon1                                                               AS weapon1
		,CAST(gameplay_start.weapon1_PS AS INT64)                                             AS weapon1_PS
		,gameplay_start.weapon2                                                               AS weapon2
		,CAST(gameplay_start.weapon2_PS AS INT64)                                             AS weapon2_PS
		,gameplay_end.event_date                                                              AS gameplay_end_event_date
		,gameplay_end.event_timestamp                                                         AS gameplay_end_event_timestamp
		,gameplay_end.event_name                                                              AS gameplay_end_event_name
		,CAST(gameplay_end.player_amount AS INT64)                                            AS gameplay_player_amount
		,CAST(gameplay_end.player_remain AS INT64)                                            AS gameplay_player_remain
		,COALESCE(gameplay_end.status,'UNDETECTED')                                           AS gameplay_status
		,CAST(COALESCE(NULLIF(gameplay_end.level_count,''),NULL) AS INT64)                     AS gameplay_level_count 
		,CAST(gameplay_end.time_played AS INT64)                                              AS gameplay_time_played
		,CAST(gameplay_end.get_hit AS FLOAT64)                                              AS gameplay_get_hit
		,CAST(gameplay_end.skill_count AS INT64)                                              AS gameplay_skill_count
		,CAST(gameplay_end.dash_count AS INT64)                                               AS gameplay_dash_count
		,CAST(gameplay_end.changeweapon_count AS INT64)                                       AS gameplay_changeweapon_count
		,CAST(gameplay_end.skill AS INT64)                                                    AS gameplay_skill
		,CAST(gameplay_end.ranged AS INT64)                                                   AS gameplay_ranged
		,CAST(gameplay_end.meele AS INT64)                                                    AS gameplay_meele
		,gameplay_end.status_effect                                                           AS gameplay_status_effect
		,CAST(gameplay_end.enemy AS INT64)                                                    AS gameplay_enemy
		,CAST(gameplay_end.down AS INT64)                                                     AS gameplay_down
		,CAST(gameplay_end.revive AS INT64)                                                   AS gameplay_revive
		,CAST(gameplay_end.being_revived AS INT64)                                            AS gameplay_being_revived
		,CAST(gameplay_end.hp_loss AS FLOAT64)                                                AS gameplay_hp_loss
		,CAST(gameplay_end.hp_pct AS FLOAT64)                                                 AS gameplay_hp_pct
	FROM gameplay_start
	LEFT JOIN gameplay_end
	ON gameplay_start.session_id = gameplay_end.session_id AND gameplay_start.user_pseudo_id = gameplay_end.user_pseudo_id
	LEFT JOIN login_start
	ON gameplay_start.user_id = login_start.user_id AND gameplay_start.event_timestamp > login_start.current_build_timestamp AND gameplay_start.event_timestamp < COALESCE(login_start.next_build_timestamp, 7258118400000000)
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon` dim_dungeon
	ON UPPER(gameplay_start.dungeon_id) = UPPER(dim_dungeon.event_dungeon_id)
	WHERE gameplay_start.session_id IS NOT NULL 
)


SELECT  *
FROM gameplay_final
where build_number <> '1307281126'
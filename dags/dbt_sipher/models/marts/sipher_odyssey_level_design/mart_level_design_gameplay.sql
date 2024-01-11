{{- config(
    materialized='table',
)-}}


WITH max_lvl_reach AS
(
	SELECT  
		user_id
		,session_id
		,MAX(level_start_level_count) level_start_level_count
		,SUM(time_played)time_played
	FROM (
		SELECT DISTINCT 
			user_id
	       ,session_id
	       ,level_start_level_count
		   ,time_played
		FROM  {{ ref('fct_level_design_lvl') }}
	)
	WHERE true
	GROUP BY  1,2
) 

, raw AS
(
	SELECT DISTINCT 
		a.* EXCEPT(gameplay_level_count,gameplay_time_played)
	    ,COALESCE(gameplay_level_count,level_start_level_count )gameplay_level_count
		,COALESCE(gameplay_time_played,time_played )gameplay_time_played
		,CAST(character_PS AS INT64) + CAST(armor_PS AS INT64) + CAST(head_PS AS INT64) + CAST(shoes_PS AS INT64)+ CAST(gloves_PS AS INT64) + CAST(weapon1_PS AS INT64) + CAST(weapon2_PS AS INT64) + CAST(legs_PS AS INT64) AS totalPS
		,UPPER(CONCAT(
		CASE 
			WHEN  a.difficulty  LIKE CONCAT('%',dim_difficulty.original_name,'%') THEN dim_difficulty.rename
			ELSE a.difficulty
		END 
		,'_'
		,
		CASE  
			WHEN a.dungeon_id  LIKE '%ENDLESS%' THEN 'ENDLESS'
			WHEN a.dungeon_id  LIKE CONCAT('%', dim_dungeon.original_name,'%') THEN CONCAT(dim_dungeon.rename,REPLACE(a.dungeon_id,CONCAT('DUNGEON_', dim_dungeon.original_name),''))
			ELSE a.dungeon_id
		END 
		)) AS dungeon_id_difficulty

	FROM {{ ref('fct_level_design_gameplay') }} a
	LEFT JOIN max_lvl_reach ml
	ON a.user_id = ml.user_id AND a.user_id = ml.user_id AND a.session_id = ml.session_id
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_dungeon
	ON a.dungeon_id  LIKE CONCAT('%', dim_dungeon.original_name,'%')
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_difficulty
	ON a.difficulty  LIKE CONCAT('%',dim_difficulty.original_name,'%')
) 

, max_min_ps_date_diff AS
(
	SELECT DISTINCT 
		user_id
		,day_diff
		,MAX(totalPS) maxtotalPS
		,MAX(gameplay_start_event_timestamp)max_gameplay_start_event_timestamp
		,MIN(totalPS) min_totalPS
		,MIN(gameplay_start_event_timestamp)min_gameplay_start_event_timestamp
	FROM raw
	GROUP BY 1,2
)

,final AS 
(
	SELECT DISTINCT 
		raw.*
       ,m.maxtotalPS
	   ,min_.min_totalPS
       ,DENSE_RANK() OVER (PARTITION BY raw.user_id,mode,difficulty ,dungeon_id ORDER BY gameplay_start_event_timestamp)                 AS dungeon_start_cnt
       ,DENSE_RANK() OVER (PARTITION BY raw.user_id,mode,difficulty ,dungeon_id,gameplay_status ORDER BY gameplay_start_event_timestamp) AS dungeon_win_cnt

	FROM raw
	LEFT JOIN max_min_ps_date_diff m ON raw.user_id = m.user_id AND raw.day_diff = m.day_diff AND raw.gameplay_start_event_timestamp = m.max_gameplay_start_event_timestamp
	LEFT JOIN max_min_ps_date_diff min_ ON raw.user_id = min_.user_id AND raw.day_diff = min_.day_diff AND raw.gameplay_start_event_timestamp = min_.min_gameplay_start_event_timestamp
)

,only1buildnumber AS 
(
	SELECT DISTINCT 
		user_id 
	FROM {{ this }}
	GROUP BY 1
	HAVING COUNT(DISTINCT build_number ) = 1
)

,user_old_bn AS 
(
	SELECT DISTINCT 
		a.user_id
	FROM {{ this }} a 
	LEFT JOIN (
		SELECT DISTINCT 
			user_id  
		FROM  {{ this }}
		WHERE build_number IN ('1309211827','1309202141')
		) b 
		ON a.user_id = b.user_id
	WHERE b.user_id IS NULL
)


SELECT DISTINCT 
	f.*,
	bnc.pack_name,
	PARSE_DATE('%m/%d/%Y', ew.date_added) AS date_added,
	ew.group AS group_,
	CASE 
		WHEN day0_date_tzutc >= DATE('2023-08-01') AND bn.user_id IS NOT NULL AND f.build_number IN ('1309211827','1309202141') THEN 'new' 
		WHEN day0_date_tzutc >= DATE('2023-08-01') AND obn.user_id IS NOT NULL THEN 'old' 
	END AS build

FROM final f 
LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_build_number_classification` bnc ON  f.build_number = bnc.build_number AND f.app_version = bnc.app_version
LEFT JOIN  `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_closed_alpha_whitelist_email` ew ON f.email = ew.email
LEFT JOIN  only1buildnumber bn ON f.user_id = bn.user_id
LEFT JOIN  user_old_bn obn ON f.user_id = obn.user_id
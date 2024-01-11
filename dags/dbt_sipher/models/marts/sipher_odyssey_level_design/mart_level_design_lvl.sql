{{- config(
    materialized='table',
)-}}


WITH raw AS
(
	SELECT DISTINCT 
		a.*
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

	FROM {{ ref('fct_level_design_lvl') }} a
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_dungeon ON a.dungeon_id  LIKE CONCAT('%', dim_dungeon.original_name,'%')
	LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_dungeon_difficulty` dim_difficulty ON a.difficulty  LIKE CONCAT('%',dim_difficulty.original_name,'%')

	WHERE true 
) 

, rank_ AS
(
	SELECT  
		*
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id ORDER BY gameplay_start_event_timestamp) AS dungeon_start_cnt
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id, gameplay_status ORDER BY gameplay_start_event_timestamp) AS dungeon_win_cnt
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id,level_start_level_count ORDER BY level_start_event_timestamp) AS dungeon_lvl_start_cnt
		,DENSE_RANK() OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id,level_start_level_count,level_status ORDER BY level_start_event_timestamp) AS dungeon_lvl_win_cnt
		,LAG(time_played) OVER (PARTITION BY user_id,mode,difficulty ,dungeon_id, session_id ORDER BY level_start_event_timestamp) AS lag_time_played

	FROM raw
)
 
,final AS 
(
	SELECT  
		f.*except(time_played,lag_time_played ),
		f.time_played as cumulative_time_played,
		COALESCE( f.time_played - lag_time_played,f.time_played)  as time_played,
	FROM rank_ f
	WHERE build_number <> '1307281126'
)

,buildtb AS 
(
	SELECT DISTINCT
		build,
		user_id  
	FROM {{ ref('mart_level_design_gameplay')}} 
)


SELECT DISTINCT 
	f.*,
	bnc.pack_name, 
	PARSE_DATE('%m/%d/%Y', ew.date_added) AS date_added,
	ew.group AS group_,
	bn.build

FROM final f 
LEFT JOIN `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_build_number_classification` bnc ON  f.build_number = bnc.build_number AND f.app_version = bnc.app_version
LEFT JOIN  `sipher-data-platform.sipher_odyssey_core.dim_sipher_odyssey_closed_alpha_whitelist_email` ew ON f.email = ew.email
LEFT JOIN  buildtb bn ON f.user_id = bn.user_id
ORDER BY user_id, level_start_event_timestamp
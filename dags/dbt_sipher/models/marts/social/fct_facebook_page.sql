{{- config(
  materialized='table',
  partition_by={
      "field": "end_time_insights",
      "data_type": "date",
      "granularity": "day"
},
) -}}

{%- set metrics = [
    'page_actions_post_reactions_like_total',
    "page_actions_post_reactions_love_total",
    "page_actions_post_reactions_wow_total",
    "page_actions_post_reactions_haha_total",
    "page_actions_post_reactions_sorry_total",
    "page_actions_post_reactions_anger_total",
    "page_consumptions",
    "page_consumptions_unique",
    "page_consumptions_by_other_clicks",
    "page_consumptions_by_other_clicks_unique",
    "page_consumptions_by_photo_view",
    "page_consumptions_by_photo_view_unique",
    "page_content_activity",
    "page_content_activity_by_fan",
    "page_content_activity_by_fan_unique",
    "page_content_activity_by_page_post",
    "page_content_activity_by_page_post_unique",
    "page_content_activity_by_other",
    "page_content_activity_by_other_unique",
    "page_engaged_users",
    "page_fans",
    "page_fan_adds_total",
    "page_fan_adds_paid",
    "page_fan_adds_unpaid",
    "page_fan_removes",
    "page_fan_removes_unique",
    "page_impressions",
    "page_impressions_by_page_post",
    "page_impressions_by_page_post_unique",
    "page_impressions_by_checkin",
    "page_impressions_by_checkin_unique",
    "page_impressions_nonviral",
    "page_impressions_nonviral_unique",
    "page_impressions_organic_unique_v2",
    "page_impressions_organic_v2",
    "page_impressions_paid",
    "page_impressions_paid_unique",
    "page_impressions_unique",
    "page_impressions_viral",
    "page_impressions_viral_unique",
    "page_negative_feedback",
    "page_places_checkin_total",
    "page_places_checkin_total_unique",
    "page_places_checkin_mobile",
    "page_places_checkin_mobile_unique",
    "page_positive_feedback_by_link",
    "page_positive_feedback_by_link_unique",
    "page_positive_feedback_by_like",
    "page_positive_feedback_by_like_unique",
    "page_positive_feedback_by_comment",
    "page_positive_feedback_by_comment_unique",
    "page_positive_feedback_by_other",
    "page_positive_feedback_by_other_unique",
    "page_post_engagements",
    "page_posts_impressions",
    "page_posts_impressions_nonviral",
    "page_posts_impressions_nonviral_unique",
    "page_posts_impressions_organic",
    "page_posts_impressions_organic_unique",
    "page_posts_impressions_paid",
    "page_posts_impressions_paid_unique",
    "page_posts_impressions_unique",
    "page_posts_impressions_viral",
    "page_posts_impressions_viral_unique",
    "page_posts_served_impressions_organic_unique",
    "page_video_complete_views_30s",
    "page_video_complete_views_30s_autoplayed",
    "page_video_complete_views_30s_click_to_play",
    "page_video_complete_views_30s_organic",
    "page_video_complete_views_30s_paid",
    "page_video_complete_views_30s_repeat_views",
    "page_video_complete_views_30s_unique",
    "page_video_repeat_views",
    "page_video_view_time",
    "page_video_views",
    "page_video_views_10s",
    "page_video_views_10s_autoplayed",
    "page_video_views_10s_click_to_play",
    "page_video_views_10s_organic",
    "page_video_views_10s_paid",
    "page_video_views_10s_repeat",
    "page_video_views_10s_unique",
    "page_video_views_autoplayed",
    "page_video_views_by_page_uploaded",
    "page_video_views_by_page_uploaded_from_crossposts",
    "page_video_views_by_page_uploaded_from_shares",
    "page_video_views_by_page_hosted_crosspost",
    "page_video_views_by_page_hosted_share",
    "page_video_views_by_page_owned",
    "page_video_views_click_to_play",
    "page_video_views_organic",
    "page_video_views_paid",
    "page_video_views_unique",
    "page_views_logged_in_total",
    "page_views_logged_in_unique",
    "page_views_total",
    "page_views_by_internal_referer_logged_in_unique_other",
    "page_views_by_internal_referer_logged_in_unique_none",
    "page_views_by_internal_referer_logged_in_unique_search",
    "page_views_by_profile_tab_logged_in_unique_photos",
    "page_views_by_profile_tab_logged_in_unique_home",
    "page_views_by_profile_tab_logged_in_unique_about",
    "page_views_by_profile_tab_total_photos",
    "page_views_by_profile_tab_total_home",
    "page_views_by_profile_tab_total_about",
    "page_views_by_site_logged_in_unique_other",
    "page_views_by_site_logged_in_unique_mobile",
    "page_views_by_site_logged_in_unique_www",
] -%}

WITH pg_over AS (
    SELECT
        page_id
        , page_name
        , username
        , followers_count
        , category
        , link
        , snapshot_date
    FROM
       {{ ref('stg_facebook_page_overall') }} 
)
,page_ins AS (
    SELECT
        CAST(end_time AS DATE) AS end_time_insights
        , end_time
        , page_id
        , period
        , {% for metric in metrics %}
            COALESCE({{ metric }}, 0) AS {{ metric }},
        {% endfor -%}
        snapshot_date
    FROM
        {{ ref('stg_facebook_page_insights') }} 
)
, final AS (
    SELECT
        pi.end_time_insights
        , po.page_id
        , po.page_name
        , po.username
        , po.followers_count
        , po.category
        , po.link
        , {% for metric in metrics %}
            pi.{{ metric }},
        {% endfor -%}
    FROM
        page_ins pi
    LEFT JOIN pg_over po ON pi.end_time_insights = po.snapshot_date AND pi.page_id = po.page_id
)

SELECT * FROM final


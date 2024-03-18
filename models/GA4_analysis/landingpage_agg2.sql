# --***merge landing page counts to new user count 
select
    t1.landing_page_path,
    t1.session_partition_date,
    t1.session_count,
    t1.session_partition_sum_engagement_time_msec,
    t1.session_partition_sum_event_value_in_usd,
    t1.client_key_count,
    t2.new_user_count
from {{ ref("landingpage_agg1") }} t1
left join
    {{ ref("new_user_count") }} t2
    on t1.landing_page_path = t2.page_path
    and t1.session_partition_date = t2.event_date_dt

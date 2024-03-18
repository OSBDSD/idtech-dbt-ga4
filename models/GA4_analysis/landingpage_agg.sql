# ---merge with conversion events the landing page agg table (ouput table userd in looker report)
select
    t1.*,
    t2.session_count as sessions,
    t2.session_partition_sum_engagement_time_msec,
    t2.session_partition_sum_event_value_in_usd as revenue,
    t2.client_key_count as users,
    t2.new_user_count as new_users
from {{ ref("conversions") }} t1
left join
    {{ ref("landingpage_agg2") }} t2
    on t1.landing_page_path = t2.landing_page_path
    and t1.session_partition_date = t2.session_partition_date

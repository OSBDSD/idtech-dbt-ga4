# ---Base table 4:count unique "session key" for the landing pages, use table dim_ga4__sessions_daily
select
    t1.landing_page_path,
    t1.session_partition_date,
    sum(
        t2.session_partition_sum_engagement_time_msec
    ) as session_partition_sum_engagement_time_msec,
    sum(
        t2.session_partition_sum_event_value_in_usd
    ) as session_partition_sum_event_value_in_usd,
    count(distinct t2.client_key) as client_key_count,
    count(distinct t1.session_key) as session_count
from {{ ref("dim_ga4__sessions_daily") }} t1
left join
    {{ ref("fct_ga4__sessions_daily") }} t2
    on t1.session_key = t2.session_key
    and t1.session_partition_date = t2.session_partition_date
where t1.session_partition_date>=current_date()-365    
group by t1.landing_page_path, t1.session_partition_date

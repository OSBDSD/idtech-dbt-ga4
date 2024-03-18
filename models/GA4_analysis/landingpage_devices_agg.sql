# -----Base table 6: devices and operating system  (ouput table userd in looker report)
select
    t1.landing_page_path,
    t1.session_partition_date,
    t1.device_category,
    t1.device_web_info_browser,
    t1.device_operating_system,
    t1.device_operating_system_version,
    count(distinct t2.client_key) as client_key_count,
    count(distinct t1.session_key) as session_count,
from {{ ref("dim_ga4__sessions_daily") }} t1
left join
    {{ ref("fct_ga4__sessions_daily") }} t2
    on t1.session_key = t2.session_key
    and t1.session_partition_key = t2.session_partition_key
group by
    t1.landing_page_path,
    t1.session_partition_date,
    t1.device_category,
    t1.device_web_info_browser,
    t1.device_operating_system,
    t1.device_operating_system_version

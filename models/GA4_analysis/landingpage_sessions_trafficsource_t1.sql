# ---Base table 5: use table dim_ga4__sessions_daily and table fct_ga4_session_daily to gather session conversions and map traffic source (based on session_medium)
select
    t1.landing_page_path,
    t1.session_partition_date,
    concat(t1.session_partition_date, t1.landing_page_location) as page_key,
    t1.session_medium,
    case
        when contains_substr(t1.session_medium, 'cpc')
        then 'Search'
        when contains_substr(t1.session_medium, 'email')
        then 'Email'
        when contains_substr(t1.session_medium, 'ocpm')
        then 'Social'
        when
            t1.landing_page_path like '%blog%'
            and t1.session_medium not like '%email%'
            and t1.session_medium not like '%ocpm%'
            and t1.session_medium not like '%cpc%'
        then 'Blog'
        when
            t1.session_medium like '%organic%'
            and t1.landing_page_path not like '%blog%'
        then 'SEO'
        when contains_substr(t1.session_medium, 'none')
        then 'Direct'
        when
            not contains_substr(t1.session_medium, 'cpc')
            and not contains_substr(t1.session_medium, 'ocpm')
            and not contains_substr(t1.session_medium, 'organic')
            and not contains_substr(t1.session_medium, 'email')
        then 'Other'
        else "not_assigned"
    end as traffic_source,
    t1.session_key,
    t2.client_key,
    t2.blog_lead_gen_count,
    t2.brochure_request_completion_count,
    t2.gated_offer_leads_count,
    t2.homepage_lead_gen_count,
    t2.location_lead_gen_count,
    t2.program_lead_gen_count,
    t2.topic_lead_gen_count,
    t2.quiz_completion_count,
    t2.purchase_count,
    t2.purchase_ipc_count,
    t2.purchase_opl_count,
    t2.purchase_ota_count,
    t2.purchase_vtc_count,
    t2.session_partition_sum_engagement_time_msec,
    t2.session_partition_sum_event_value_in_usd,
from {{ ref("dim_ga4__sessions_daily") }} t1
left join
    {{ ref("fct_ga4__sessions_daily") }} t2
    on t1.session_partition_key = t2.session_partition_key
where t1.session_partition_date>=current_date()-365   

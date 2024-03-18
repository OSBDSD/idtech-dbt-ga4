#---agg by month by landing_page_path, date, traffic_group, traffic_source  (ouput table userd in looker report)
 
select 
Traffic_source_group,  Traffic_source, 
parse_DATE('%Y-%m',format_date('%Y-%m', DATE_TRUNC(session_partition_date, MONTH))) AS year_month, 
EXTRACT(YEAR FROM session_partition_date) AS Year,
EXTRACT(MONTH FROM session_partition_date) AS Month,
sum(session_partition_sum_engagement_time_msec) as engagement_time_msec,
sum(session_partition_sum_event_value_in_usd) as Revenue,
count (distinct client_key) as Users,
count (distinct session_key) as sessions,
sum(blog_lead_gen_count) as blog_lead_gen,
sum(brochure_request_completion_count) as brochure_request_completion,
sum(gated_offer_leads_count) as gated_offer_leads,
sum(homepage_lead_gen_count) as homepage_lead_gen,
sum(location_lead_gen_count) as location_lead_gen,
sum(summercamp_lead_gen_test_count_n) as summercamp_lead_gen_test,
sum(regional_lead_gen_count_n) as regional_lead_gen,
sum(program_lead_gen_count) as program_lead_gen,
sum(topic_lead_gen_count) as topic_lead_gen, 
sum(quiz_completion_count) as quiz_completion,
sum(purchase_count) as purchase,
sum(purchase_ipc_count) as purchase_ipc,
sum(purchase_opl_count) as purchase_opl,
sum(purchase_ota_count) as purchase_ota,
sum(purchase_vtc_count) as purchase_vtc,
sum(blog_lead_gen_count+brochure_request_completion_count+gated_offer_leads_count+homepage_lead_gen_count+location_lead_gen_count
+program_lead_gen_count+topic_lead_gen_count+quiz_completion_count+summercamp_lead_gen_test_count_n+regional_lead_gen_count_n) as Leads
from  {{ref("landingpage_sessions_trafficsource_t5")}}
group by all
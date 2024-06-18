#---agg by month by landing_page_path, date, traffic_group, traffic_source  (ouput table userd in looker report)
 
SELECT
  Traffic_source_group,
  Traffic_source,
  year_month,
  Year,
  Month,
  SUM(engagement_time_msec) AS engagement_time_msec,
  SUM(Revenue) AS Revenue,
  SUM(Users) AS Users,
  SUM(sessions) AS sessions,
  SUM(blog_lead_gen) AS blog_lead_gen,
  SUM(brochure_request_completion) AS brochure_request_completion,
  SUM(gated_offer_leads) AS gated_offer_leads,
  SUM(homepage_lead_gen) AS homepage_lead_gen,
  SUM(location_lead_gen) AS location_lead_gen,
  SUM(summercamp_lead_gen_test) AS summercamp_lead_gen_test,
  SUM(regional_lead_gen) AS regional_lead_gen,
  SUM(program_lead_gen) AS program_lead_gen,
  SUM(topic_lead_gen) AS topic_lead_gen,
  SUM(quiz_completion) AS quiz_completion,
  SUM(purchase) AS purchase,
  SUM(purchase_ipc) AS purchase_ipc,
  SUM(purchase_opl) AS purchase_opl,
  SUM(purchase_ota) AS purchase_ota,
  SUM(purchase_vtc) AS purchase_vtc,
  SUM(Leads) AS Leads
FROM
  {{ref('landing_page_session_agg')}}
GROUP BY
  ALL
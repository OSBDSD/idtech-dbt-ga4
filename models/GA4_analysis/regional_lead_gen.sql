## ---Base table 2: obtainb regional_lead_gen and summercamp_lead_gen_test event count per page_key
 
SELECT page_key, count(distinct event_key) as  regional_lead_gen_count  
FROM {{ref('stg_ga4__events')}} 
where event_name= "regional_lead_gen"
group by all
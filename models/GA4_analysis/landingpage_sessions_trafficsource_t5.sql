# ----convert event with null value to "0" to prevent calculation errors

select *,
case when summercamp_lead_gen_test_count is null then 0 else summercamp_lead_gen_test_count end as summercamp_lead_gen_test_count_n,
case when regional_lead_gen_count is null then 0 else regional_lead_gen_count end as regional_lead_gen_count_n,
from  {{ref("landingpage_sessions_trafficsource_t4")}}
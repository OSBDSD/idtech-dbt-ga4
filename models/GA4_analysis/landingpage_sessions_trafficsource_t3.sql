# ----merge event regional_lead_gen_count
select t1.*, t2.regional_lead_gen_count
from {{ ref("landingpage_sessions_trafficsource_t2") }} t1
left join {{ ref("regional_lead_gen") }} t2 on t1.page_key = t2.page_key

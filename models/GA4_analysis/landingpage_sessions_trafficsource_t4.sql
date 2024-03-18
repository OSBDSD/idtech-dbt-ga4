#---merge event summercamp_lead_gen_test_count

select t1.*, t2.summercamp_lead_gen_test_count
from  {{ref('landingpage_sessions_trafficsource_t3')}} t1 
left join  {{ref('summercamp_lead_gen_test')}} t2 on t1.page_key =t2.page_key
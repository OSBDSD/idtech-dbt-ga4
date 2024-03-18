# ---Base table 3: obtain summercamp_lead_gen_test count
select page_key, count(distinct event_key) as summercamp_lead_gen_test_count
from {{ ref("stg_ga4__events") }}
where event_name = "summercamp_lead_gen_test"
group by all

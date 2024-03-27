# ---Base table 1: count new user per page_path per pt date. Test Change Comment

select page_path, event_date_dt, count(distinct client_key) as new_user_count
from {{ ref("stg_ga4__event_first_visit") }}
group by page_path, event_date_dt

# -- create traffic group category
select
    t1.*,
    case
        when t1.traffic_source = 'Search'
        then 'PAID'
        when t1.traffic_source = 'Social'
        then 'PAID'
        when t1.traffic_source = 'Other'
        then 'PAID'
        else 'ORGANIC'
    end as traffic_source_group,
from {{ ref("landingpage_sessions_trafficsource_t1") }} t1

SELECT *
FROM   `ga4-bigquery-412121.dbt_idtech_GA4_analysis.UA_landingpage_session_agg`
UNION ALL
SELECT *
FROM {{ ref("landingpage_session_agg_0") }}  a
WHERE DATE(a.session_partition_date) >= DATE('2024-02-01')
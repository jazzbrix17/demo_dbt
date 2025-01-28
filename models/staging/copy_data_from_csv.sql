COPY raw_page_views (timestamp, user_id, anonymous_id)
FROM 'dbt_workspace\dbt_ikaros_assessment\sample_page_views.csv'
DELIMITER ','
CSV HEADER;
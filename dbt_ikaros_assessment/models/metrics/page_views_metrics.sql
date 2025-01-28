with sessions as (
    select
        user_id,
        session_id,
        count(*) as page_views,
        max(timestamp) - min(timestamp) as session_duration
    from {{ ref('stg_page_views') }}
    group by user_id, session_id
),

user_sessions as (
    select
        user_id,
        count(distinct session_id) as total_sessions,
        sum(page_views) as total_page_views,
        avg(page_views) as avg_page_views_per_session,
        avg(session_duration) as avg_session_duration
    from sessions
    group by user_id
)

select
    count(distinct user_id) as unique_visitors,
    sum(total_sessions) as total_sessions,
    sum(total_page_views) as total_page_views,
    avg(avg_page_views_per_session) as avg_page_views_per_visitor,
    avg(avg_session_duration) as avg_session_duration
from user_sessions

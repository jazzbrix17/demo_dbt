with raw_data as (
    select
        timestamp,
        user_id,
        anonymous_id
    from {{ source('raw_data', 'raw_page_views') }}
),

-- Assume sessions are defined as consecutive page views within 30 minutes
sessions as (
    select
        *,
        lag(timestamp) over (partition by user_id order by timestamp) as prev_timestamp,
        case 
            when timestamp - lag(timestamp) over (partition by user_id order by timestamp) > interval '30 minutes' 
            then 1
            else 0
        end as new_session_flag
    from raw_data
),

-- Define session groups by using cumulative sum of the new session flag
session_groups as (
    select
        *,
        sum(new_session_flag) over (partition by user_id order by timestamp) as session_id
    from sessions
)

select
    user_id,
    anonymous_id,
    timestamp,
    session_id
from session_groups

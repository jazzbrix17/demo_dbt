with source as (
        select * from {{ source('raw_data', 'raw_page_views') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
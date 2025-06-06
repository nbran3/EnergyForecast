with 

source as (

    select * from {{ source('energy_pipeline', 'tavg') }}

),

tavg as (

    select
        cast(date as date) as Date,
        value

    from source
    order by Date

)

select * from tavg

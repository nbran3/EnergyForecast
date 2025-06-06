with 

source as (

    select * from {{ source('energy_pipeline', 'recession') }}

),

recession as (

    select
        cast(observation_date as date) as Date,
        cast(usrec as float64) as value

    from source
    order by Date

)

select * from recession

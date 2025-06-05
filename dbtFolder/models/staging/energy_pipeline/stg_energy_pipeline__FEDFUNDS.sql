with 

source as (

    select * from {{ source('energy_pipeline', 'FEDFUNDS') }}

),

FFR as (

    select
        cast(date as date) as Date,
        value as FFR,
    from source
    order by Date

)

select * from FFR

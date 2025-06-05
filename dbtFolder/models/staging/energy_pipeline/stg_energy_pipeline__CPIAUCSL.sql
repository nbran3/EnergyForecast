with 

source as (

    select * from {{ source('energy_pipeline', 'CPIAUCSL') }}

),

CPI as (

    select
        cast(date as date) as Date,
        value as CPI,
        
    from source
    order by Date

)

select * from CPI

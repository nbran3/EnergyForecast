with 

source as (

    select * from {{ source('energy_pipeline', 'MCUMFN') }}

),

MCUMFN as (

    select
        cast(date as date) as Date,
        cast(value as float64) as value

        

    from source
    order by Date

)

select * from MCUMFN

with 

source as (

    select * from {{ source('energy_pipeline', 'energy') }}

),

energy as (

    select
        cast(month as date) as Date,
        `total fossil fuels production`,
        `nuclear electric power production`,
        `total renewable energy production`,
        `total primary energy production`,
        `primary energy imports`,
        `primary energy exports`,
        `primary energy net imports`,
        `primary energy stock change and other`,
        `total fossil fuels consumption`,
        `nuclear electric power consumption`,
        `total renewable energy consumption`,
        `total primary energy consumption`

    from source
    order by Date

)

select * from energy
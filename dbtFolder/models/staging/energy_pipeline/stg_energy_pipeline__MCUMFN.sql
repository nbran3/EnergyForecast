with 

source as (

    select * from {{ source('energy_pipeline', 'MCUMFN') }}

),

renamed as (

    select
        date,
        value,
        series_id

    from source

)

select * from renamed

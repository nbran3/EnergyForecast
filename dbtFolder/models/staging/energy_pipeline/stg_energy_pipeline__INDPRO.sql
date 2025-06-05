with 

source as (

    select * from {{ source('energy_pipeline', 'INDPRO') }}

),

renamed as (

    select
        date,
        value,
        series_id

    from source

)

select * from renamed

with 

source as (

    select * from {{ source('energy_pipeline', 'UNRATE') }}

),

renamed as (

    select
        cast(date as date) as Date,
        value as UNRATE,
        

    from source
    order by Date

)

select * from renamed

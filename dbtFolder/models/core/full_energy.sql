{{
    config(
        materialized='table'
        
    )
}}

WITH energy_data AS (
    SELECT *
    FROM {{ref('stg_energy_pipeline__energy')}}
),

cpi_data as (
    SELECT * 
    FROM {{ref("stg_energy_pipeline__CPIAUCSL")}}
),

ffr_data as (
    SELECT *
    FROM {{ref("stg_energy_pipeline__FEDFUNDS")}}
),

indpro_data as (
    SELECT * 
    FROM {{ref("stg_energy_pipeline__INDPRO")}}
),

mcumfn_data as (
    SELECT * 
    FROM {{ref("stg_energy_pipeline__MCUMFN")}}
),

unrate_data as(
    SELECT *
    FROM {{ref("stg_energy_pipeline__UNRATE")}}
),
recession_data as(
    SELECT *
    FROM {{ref("stg_energy_pipeline__recession")}}
),

hdd_data as (
    SELECT * 
    FROM {{ref("stg_energy_pipeline__hdd")}}
),

cdd_data as (
    SELECT * 
    FROM {{ref("stg_energy_pipeline__cdd")}}
),

tavg_data as (
    SELECT * 
    FROM {{ref("stg_energy_pipeline__tavg")}}
)

SELECT 
    energy.*,
    CASE 
        WHEN EXTRACT(MONTH FROM energy.Date) IN (6, 7, 8) THEN 1.0
        ELSE .0
    END AS is_summer,
    CASE 
        WHEN EXTRACT(MONTH FROM energy.Date) IN (12, 1, 2) THEN 1.0
        ELSE 0.0
    END AS is_winter,
    LAG(energy.`total primary energy consumption`, 1) OVER (ORDER BY energy.Date) AS lag1_total_consumption,
    LAG(energy.`total primary energy consumption`, 2) OVER (ORDER BY energy.Date) AS lag2_total_consumption,
    AVG(energy.`total primary energy consumption`) OVER (ORDER BY energy.Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3m_avg_consumption,
    LAG(cpi.value, 1) OVER (ORDER by energy.Date) as lag_cpi,
    cpi.value as CPI,
    ffr.value as FFR,
    indpro.value as INDPRO,
    mcumfn.value as MCUMFN,
    unrate.value as UNRATE,
    recession.value as is_recession,
    hdd.value as HDD,
    cdd.value as CDD,
    tavg.value as TAVG

FROM energy_data energy
INNER JOIN cpi_data cpi
    ON energy.Date = cpi.Date
INNER JOIN ffr_data ffr
    on energy.Date = ffr.Date
INNER JOIN indpro_data indpro
    on energy.Date = indpro.Date
INNER JOIN mcumfn_data as mcumfn
    on energy.Date = mcumfn.Date
INNER JOIN unrate_data as unrate
    on energy.Date = unrate.Date
INNER JOIN recession_data as recession
    on energy.Date = recession.Date
INNER JOIN hdd_data as hdd
    on energy.Date = hdd.Date
INNER JOIN cdd_data as cdd
    on energy.Date = cdd.Date
INNER JOIN tavg_data as tavg
    on energy.Date = tavg.Date

ORDER by energy.Date
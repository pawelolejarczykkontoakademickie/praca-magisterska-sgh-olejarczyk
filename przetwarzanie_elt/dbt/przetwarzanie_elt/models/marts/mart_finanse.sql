--mart_finanse.sql

WITH trips AS (
    SELECT * FROM {{ ref('stg_taxi_trips') }}
)

SELECT
    rok,
    miesiac,
    typ_platnosci,
    COUNT(*) as liczba_transakcji,
    SUM(kwota_transackji) as laczny_przychod,
    AVG(procent_napiwku) as sredni_procent_napiwku
FROM trips
GROUP BY 1, 2, 3
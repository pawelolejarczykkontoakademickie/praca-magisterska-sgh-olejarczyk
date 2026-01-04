--mart_trasy.sql

WITH przejazdy AS (
    SELECT *
    FROM {{ ref('stg_taxi_trips') }}
),
strefy AS (
    SELECT *
    FROM {{ ref('dim_strefy_chicago') }}
)

SELECT
    strefa_odbioru.nazwa_dzielnicy AS dzielnica_odbioru,
    strefa_dowozu.nazwa_dzielnicy  AS dzielnica_dowozu,
    COUNT(t.id_przejazdu)   AS liczba_przejazdow
FROM przejazdy AS t
LEFT JOIN strefy AS strefa_odbioru
    ON t.id_dzielnica_odbioru = strefa_odbioru.id_dzielnicy
LEFT JOIN strefy AS strefa_dowozu
    ON t.id_dzielnica_dowozu = strefa_dowozu.id_dzielnicy
GROUP BY
    1, 2

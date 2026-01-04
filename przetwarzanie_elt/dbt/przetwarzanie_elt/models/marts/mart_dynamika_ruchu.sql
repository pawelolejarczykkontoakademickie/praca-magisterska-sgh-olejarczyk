--mart_dynamika_ruchu.sql

WITH przejazdy AS (
    SELECT *
    FROM {{ ref('stg_taxi_trips') }}
),
strefy AS (
    SELECT *
    FROM {{ ref('dim_strefy_chicago') }}
)

SELECT
    s.nazwa_dzielnicy AS dzielnica_odbioru,
    t.godzina,
    t.rok,
    t.miesiac,
    COUNT(t.id_przejazdu) AS liczba_przejazdow,
    AVG(t.predkosc) AS srednia_predkosc_mph
FROM przejazdy AS t
LEFT JOIN strefy AS s
    ON t.id_dzielnica_odbioru = s.id_dzielnicy
GROUP BY
    1, 2, 3, 4

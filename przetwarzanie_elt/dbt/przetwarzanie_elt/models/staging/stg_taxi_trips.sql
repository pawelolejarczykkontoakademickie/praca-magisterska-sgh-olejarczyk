--stg_taxi_trips.sql

WITH raw as (
    SELECT *
    FROM `{{ target.project }}.raw.taxi_trips_{{ var('source_size','small') }}`
)

SELECT
  unique_key as id_przejazdu,
  trip_start_timestamp as godzina_rozpoczecia_przejazdu,
  trip_seconds as czas_przejazdu,
  trip_miles as dystans,
  pickup_community_area as id_dzielnica_odbioru,
  dropoff_community_area as id_dzielnica_dowozu,
  tips as napiwek,
  trip_total as kwota_transackji,
  payment_type as typ_platnosci,
  EXTRACT(YEAR  FROM trip_start_timestamp) AS rok,
  EXTRACT(MONTH FROM trip_start_timestamp) AS miesiac,
  EXTRACT(HOUR  FROM trip_start_timestamp) AS godzina,
  trip_miles / NULLIF(trip_seconds / 3600, 0) AS predkosc,
  CASE WHEN trip_total > 0 THEN (IFNULL(tips, 0) / trip_total) * 100 ELSE 0 END AS procent_napiwku

FROM raw
WHERE trip_seconds > 0
  AND trip_miles > 0
  AND pickup_community_area IS NOT NULL
  AND dropoff_community_area IS NOT NULL
  
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY unique_key
  ORDER BY trip_start_timestamp
) = 1

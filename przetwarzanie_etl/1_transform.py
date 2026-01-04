from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, hour, month, year, when, broadcast
import sys

PROJECT_ID = "przetwarzanie-etl"  
BUCKET_NAME = "bucket__etl" 

SIZE = sys.argv[1] if len(sys.argv) > 1 else "small"

PATH_RAW_PREFIX = f"gs://{BUCKET_NAME}/data/raw/{SIZE}/"
PATH_RAW_FILES = f"{PATH_RAW_PREFIX}*.parquet"
PATH_MARTS = f"gs://{BUCKET_NAME}/data/marts/{SIZE}"

def get_spark_session():
    return (
        SparkSession.builder
        .appName(f"Przetwarzanie_ETL_Transform_{SIZE}")
        .getOrCreate()
    )

def run_transform():
    spark = get_spark_session()

    try:
        df = spark.read.parquet(PATH_RAW_FILES)
    except Exception as e:
        print(f"Błąd")
        sys.exit(1)

    df_filtered = df.filter(
        (col("trip_seconds") > 0) &
        (col("trip_miles") > 0) &
        (col("pickup_community_area").isNotNull()) &
        (col("dropoff_community_area").isNotNull())
    ).dropDuplicates(['unique_key'])

    df_stg = (
        df_filtered
        .withColumnRenamed("unique_key", "id_przejazdu")
        .withColumnRenamed("trip_start_timestamp", "godzina_rozpoczecia_przejazdu")
        .withColumnRenamed("trip_seconds", "czas_przejazdu")
        .withColumnRenamed("trip_miles", "dystans")
        .withColumnRenamed("pickup_community_area", "id_dzielnica_odbioru")
        .withColumnRenamed("dropoff_community_area", "id_dzielnica_dowozu")
        .withColumnRenamed("tips", "napiwek")
        .withColumnRenamed("trip_total", "kwota_transackji") 
        .withColumnRenamed("payment_type", "typ_platnosci")
        .withColumn("rok", year("godzina_rozpoczecia_przejazdu"))
        .withColumn("miesiac", month("godzina_rozpoczecia_przejazdu"))
        .withColumn("godzina", hour("godzina_rozpoczecia_przejazdu"))
        .withColumn("predkosc", col("dystans") / (col("czas_przejazdu") / 3600))
        .withColumn("procent_napiwku", 
                    when(col("kwota_transackji") > 0, (col("napiwek") / col("kwota_transackji")) * 100)
                    .otherwise(0))
        .fillna({'napiwek': 0.0})
    )

    df_stg.cache()

    zones_data = [
        (1, "Rogers Park"), (2, "West Ridge"), (3, "Uptown"), (4, "Lincoln Square"),
        (5, "North Center"), (6, "Lake View"), (7, "Lincoln Park"), (8, "Near North Side"),
        (9, "Edison Park"), (10, "Norwood Park"), (11, "Jefferson Park"), (12, "Forest Glen"),
        (13, "North Park"), (14, "Albany Park"), (15, "Portage Park"), (16, "Irving Park"),
        (17, "Dunning"), (18, "Montclare"), (19, "Belmont Cragin"), (20, "Hermosa"),
        (21, "Avondale"), (22, "Logan Square"), (23, "Humboldt Park"), (24, "West Town"),
        (25, "Austin"), (26, "West Garfield Park"), (27, "East Garfield Park"), (28, "Near West Side"),
        (29, "North Lawndale"), (30, "South Lawndale"), (31, "Lower West Side"), (32, "Loop"),
        (33, "Near South Side"), (34, "Armour Square"), (35, "Douglas"), (36, "Oakland"),
        (37, "Fuller Park"), (38, "Grand Boulevard"), (39, "Kenwood"), (40, "Washington Park"),
        (41, "Hyde Park"), (42, "Woodlawn"), (43, "South Shore"), (44, "Chatham"),
        (45, "Avalon Park"), (46, "South Chicago"), (47, "Burnside"), (48, "Calumet Heights"),
        (49, "Roseland"), (50, "Pullman"), (51, "South Deering"), (52, "East Side"),
        (53, "West Pullman"), (54, "Riverdale"), (55, "Hegewisch"), (56, "Garfield Ridge"),
        (57, "Archer Heights"), (58, "Brighton Park"), (59, "McKinley Park"), (60, "Bridgeport"),
        (61, "New City"), (62, "West Elsdon"), (63, "Gage Park"), (64, "Clearing"),
        (65, "West Lawn"), (66, "Chicago Lawn"), (67, "West Englewood"), (68, "Englewood"),
        (69, "Greater Grand Crossing"), (70, "Ashburn"), (71, "Auburn Gresham"), (72, "Beverly"),
        (73, "Washington Heights"), (74, "Mount Greenwood"), (75, "Morgan Park"), (76, "O'Hare"),
        (77, "Edgewater")
    ]
    df_zones = spark.createDataFrame(zones_data, ["id_dzielnicy", "nazwa_dzielnicy"])

    mart_dynamika = (
        df_stg
        .join(broadcast(df_zones), df_stg.id_dzielnica_odbioru == df_zones.id_dzielnicy, "left")
        .groupBy("nazwa_dzielnicy", "godzina", "rok", "miesiac")
        .agg(
            count("id_przejazdu").alias("liczba_przejazdow"),
            avg("predkosc").alias("srednia_predkosc_mph")
        )
        .withColumnRenamed("nazwa_dzielnicy", "dzielnica_odbioru")
    )
    mart_dynamika.write.mode("overwrite").parquet(f"{PATH_MARTS}/mart_dynamika_ruchu")

    mart_finanse = df_stg.groupBy("rok", "miesiac", "typ_platnosci").agg(
        count("*").alias("liczba_transakcji"),
        sum("kwota_transackji").alias("laczny_przychod"),
        avg("procent_napiwku").alias("sredni_procent_napiwku")
    )
    mart_finanse.write.mode("overwrite").parquet(f"{PATH_MARTS}/mart_finanse")
    
    trasy_step1 = (
        df_stg
        .join(broadcast(df_zones), df_stg.id_dzielnica_odbioru == df_zones.id_dzielnicy, "left")
        .withColumnRenamed("nazwa_dzielnicy", "dzielnica_odbioru")
        .drop("id_dzielnicy")
    )
    
    trasy_step2 = (
        trasy_step1
        .join(broadcast(df_zones), trasy_step1.id_dzielnica_dowozu == df_zones.id_dzielnicy, "left")
        .withColumnRenamed("nazwa_dzielnicy", "dzielnica_dowozu")
        .drop("id_dzielnicy")
    )
    
    mart_trasy = (
        trasy_step2
        .groupBy("dzielnica_odbioru", "dzielnica_dowozu")
        .agg(count("id_przejazdu").alias("liczba_przejazdow"))
    )

    mart_trasy.write.mode("overwrite").parquet(f"{PATH_MARTS}/mart_trasy")

    spark.stop()

if __name__ == "__main__":
    run_transform()
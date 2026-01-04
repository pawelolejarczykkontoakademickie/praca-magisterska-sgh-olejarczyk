from pyspark.sql import SparkSession
import sys

PROJECT_ID = "przetwarzanie-etl"     
BUCKET_NAME = "bucket__etl" 
DATASET_ID = "transformed_marts"           

SIZE = sys.argv[1] if len(sys.argv) > 1 else "small"

PATH_MARTS = f"gs://{BUCKET_NAME}/data/marts/{SIZE}"
PATH_TEMP_BQ = f"gs://{BUCKET_NAME}/temp_spark"

def get_spark_session():
    return SparkSession.builder \
        .appName(f"Przetwarzanie_ETL_Load_{SIZE}") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0") \
        .getOrCreate()

def run_load():
    spark = get_spark_session()

    marts = ["mart_dynamika_ruchu", "mart_finanse", "mart_trasy"]

    for mart in marts:
        try:
            df = spark.read.parquet(f"{PATH_MARTS}/{mart}")
            
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{mart}_{SIZE}"
            
            df.write.format("bigquery") \
                .option("table", table_id) \
                .option("temporaryGcsBucket", PATH_TEMP_BQ) \
                .mode("overwrite") \
                .save()
        except Exception as e:
            print(f"Błąd ładowania do BigQuery!")

    spark.stop()

if __name__ == "__main__":
    run_load()
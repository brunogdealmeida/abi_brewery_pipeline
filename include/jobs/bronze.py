import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def validate_landing_to_bronze_or_fail():

    landing_path = "include/data/landing/"
    fail_path = "include/data/fail/"
    bronze_path = "include/data/bronze/"

    # Create Spark session
    spark = SparkSession.builder.appName("bronze_validation").getOrCreate()
    
    # Define o schema esperado
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True)
    ])

    # Cria o DataFrame com schema explícito
    df = spark.read.json(landing_path, schema=schema)
    
    if df is None:
        raise ValueError("Failed to read data from landing zone")

    # Validation (example)
    df_invalid = df.filter(df.id.isNull())
    df_valid = df.subtract(df_invalid)

    if df_invalid.count() > 0:
        print("Dados inválidos encontrados!")
        df_invalid.coalesce(1).write.mode("overwrite").options(header='True', delimiter=',') \
            .csv(fail_path)
    else:
        print("Dados válidos!")

    df_valid.coalesce(1).write.mode("overwrite").parquet(bronze_path)

    # Clean up landing layer
    os.system(f"rm -rf {landing_path}*")
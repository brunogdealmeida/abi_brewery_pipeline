from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import requests

def fetch_api_to_landing():
    spark = SparkSession.builder.appName("landing").getOrCreate()

    # Requisição
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url)
    response.raise_for_status()

    json_data = response.json()
    if not isinstance(json_data, list) or len(json_data) == 0:
        raise ValueError("Resposta da API está vazia ou mal formatada.")

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

    # Create DataFrame with explicit schema
    df = spark.createDataFrame(json_data, schema=schema)

    # Write to landing zone (overwrite)
    df.coalesce(1).write.mode("overwrite").json("include/data/landing/")
    
    # Return success message instead of DataFrame
    return {"status": "success", "message": "Data successfully written to landing zone"}

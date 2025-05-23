
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, trim

def transform_to_silver():
    # Create Spark session
    spark = SparkSession.builder.appName("silver_transformation").getOrCreate()
    
    # Read data from bronze
    df = spark.read.parquet("include/data/bronze/")
    
    if df is None:
        raise ValueError("Failed to read data from bronze layer")

    transformed_df = (
        df.select("*")
        .withColumn("name", trim(upper("name")))
        .withColumn("city", trim("city"))
        .withColumn("state", upper("state"))
    )

    silver_path = "include/data/silver/"
 
    transformed_df.write.partitionBy("state").mode("overwrite").parquet(silver_path)

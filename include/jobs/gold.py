
from pyspark.sql import SparkSession

def transform_to_gold():
    # Create Spark session
    spark = SparkSession.builder.appName("gold_transformation").getOrCreate()
    
    # Read data from silver (partitioned by state)
    df = spark.read.parquet("include/data/silver/")
    
    if df is None:
        raise ValueError("Failed to read data from silver layer")

    # Show schema to verify partitioning
    print("Silver layer schema:")
    df.printSchema()

    # Group by state and brewery_type
    agg_df = df.groupBy("state", "brewery_type").count()

    gold_path = "include/data/gold/"
    
    agg_df.write.mode("overwrite").parquet(gold_path)

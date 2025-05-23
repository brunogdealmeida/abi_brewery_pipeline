from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum
from typing import Dict, Any, List
import json
from datetime import datetime
import os 

# Define valid brewery types
VALID_BREWERY_TYPES = ["micro", "brewpub", "contract", "proprietor", "closed", "large"]

def check_bronze_quality(df: Any) -> Dict[str, Any]:
    """
    Perform quality checks on bronze layer data:
    - Check for null values in all fields
    """
    results = {
        "layer": "bronze",
        "checks": {},
        "total_records": df.count()
    }
    
    # Get schema fields
    fields = ["id"] #[field.name for field in df.schema.fields]
    
    # Check nulls for each field
    null_checks = {}
    for field in fields:
        null_count = df.filter(col(field).isNull()).count()
        null_checks[field] = {
            "null_count": null_count,
            "null_percentage": (null_count / results["total_records"]) * 100 if results["total_records"] > 0 else 0
        }
    
    results["checks"]["null_checks"] = null_checks
    
    return results

def check_silver_quality(df: Any) -> Dict[str, Any]:
    """
    Perform quality checks on silver layer data:
    - Check for duplicates based on id field
    - Check if website_url starts with http or https
    """
    results = {
        "layer": "silver",
        "checks": {},
        "total_records": df.count()
    }
    
    # Check duplicates
    duplicates = df.groupBy("id").count().filter("count > 1").count()
    results["checks"]["duplicates"] = {
        "count": duplicates,
        "percentage": (duplicates / results["total_records"]) * 100 if results["total_records"] > 0 else 0
    }
    
    # Check website_url format
    valid_urls = df.filter(col("website_url").rlike("^(http|https)://.*")).count()
    invalid_urls = results["total_records"] - valid_urls
    results["checks"]["website_url_format"] = {
        "valid_count": valid_urls,
        "invalid_count": invalid_urls,
        "valid_percentage": (valid_urls / results["total_records"]) * 100 if results["total_records"] > 0 else 0
    }
    
    return results

def check_gold_quality(df: Any) -> Dict[str, Any]:
    """
    Perform quality checks on gold layer data:
    - Check if brewery_type is one of the valid values
    """
    results = {
        "layer": "gold",
        "checks": {},
        "total_records": df.count()
    }
    
    # Check brewery_type values
    invalid_brewery_types = df.filter(~col("brewery_type").isin(VALID_BREWERY_TYPES)).count()
    valid_brewery_types = results["total_records"] - invalid_brewery_types
    results["checks"]["brewery_type_validation"] = {
        "valid_count": valid_brewery_types,
        "invalid_count": invalid_brewery_types,
        "valid_percentage": (valid_brewery_types / results["total_records"]) * 100 if results["total_records"] > 0 else 0
    }
    
    return results

def save_quality_results(results: Dict[str, Any]):
    """
    Save quality check results to a file
    """
    timestamp = datetime.now().isoformat()
    results_dir = "include/data_quality_results"
    
    # Create directory if it doesn't exist
    os.makedirs(results_dir, exist_ok=True)
    
    # Add metadata
    results.update({
        "execution_date": timestamp,
        "overall_success": all(check.get("valid_percentage", 0) == 100 for layer in results.get("layer_results", {}).values() for check in layer.get("checks", {}).values())
    })
    
    # Save results
    filename = f"summary_data_quality.json"
    with open(os.path.join(results_dir, filename), "w") as f:
        json.dump(results, f, indent=4)

def run_quality_checks():
    """
    Run all quality checks across all layers
    """
    spark = SparkSession.builder.appName("data_quality").getOrCreate()
    
    # Load data from each layer
    bronze_df = spark.read.parquet("include/data/bronze/")
    silver_df = spark.read.parquet("include/data/silver/")
    gold_df = spark.read.parquet("include/data/gold/")
    
    # Run checks
    results = {
        "layer_results": {
            "bronze": check_bronze_quality(bronze_df),
            "silver": check_silver_quality(silver_df),
            "gold": check_gold_quality(gold_df)
        }
    }
    
    # Save results
    save_quality_results(results)
    
    return results

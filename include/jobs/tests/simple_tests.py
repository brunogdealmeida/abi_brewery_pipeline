from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
from datetime import datetime
import os

class SimpleSparkTest:
    def __init__(self, spark):
        self.spark = spark
        self.results = []
        self.test_id = 1

    def run_test(self, df, test_name, test_func, **kwargs):
        """Run a test and record the result"""
        try:
            result = test_func(df, **kwargs)
            self.results.append({
                'test_id': self.test_id,
                'test_name': test_name,
                'result': result,
                'passed': True,
                'timestamp': datetime.now().isoformat()
            })
            self.test_id += 1
            return True
        except Exception as e:
            self.results.append({
                'test_id': self.test_id,
                'test_name': test_name,
                'result': str(e),
                'passed': False,
                'timestamp': datetime.now().isoformat()
            })
            self.test_id += 1
            return False

    def save_results(self, output_path):
        """Save test results to JSON file"""
        results_dir = os.path.dirname(output_path)
        os.makedirs(results_dir, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump({
                'test_run': {
                    'timestamp': datetime.now().isoformat(),
                    'total_tests': len(self.results),
                    'passed': sum(1 for r in self.results if r['passed']),
                    'failed': sum(1 for r in self.results if not r['passed'])
                },
                'tests': self.results
            }, f, indent=2)

def test_landing_data(df):
    """Test landing layer data integrity"""
    # Check schema
    required_columns = ['id', 'name', 'brewery_type', 'latitude', 'longitude']
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        return f"Missing required columns: {missing_columns}"
    
    # Check data types
    type_checks = []
    type_checks.append(df.filter(df.id.isNull()).count() == 0)
    type_checks.append(df.filter(df.name.isNull()).count() == 0)
    type_checks.append(df.filter(df.brewery_type.isNull()).count() == 0)
    
    return all(type_checks)

def test_bronze_data(df):
    """Test bronze layer data quality"""
    # Check for null values in key columns
    null_checks = []
    null_checks.append(df.filter(df.id.isNull()).count() == 0)
    null_checks.append(df.filter(df.name.isNull()).count() == 0)
    null_checks.append(df.filter(df.brewery_type.isNull()).count() == 0)
    
    # Check data consistency
    consistency_checks = []
    consistency_checks.append(df.filter(df.latitude < -90).count() == 0)
    consistency_checks.append(df.filter(df.latitude > 90).count() == 0)
    consistency_checks.append(df.filter(df.longitude < -180).count() == 0)
    consistency_checks.append(df.filter(df.longitude > 180).count() == 0)
    
    return all(null_checks + consistency_checks)

def test_silver_data(df):
    """Test silver layer data quality"""
    # Check for duplicates
    duplicate_count = df.groupBy("id").count().filter("count > 1").count()
    if duplicate_count > 0:
        return False
    
    # Check data completeness
    completeness_checks = []
    completeness_checks.append(df.filter(df.id.isNull()).count() == 0)
    completeness_checks.append(df.filter(df.name.isNull()).count() == 0)
    completeness_checks.append(df.filter(df.brewery_type.isNull()).count() == 0)
    completeness_checks.append(df.filter(df.city.isNull()).count() == 0)
    
    return all(completeness_checks)

def test_gold_data(df):
    """Test gold layer data quality"""
    # Check aggregations
    if df.count() == 0:
        return False
    
    # Check metrics
    if df.filter(df.brewery_count < 0).count() > 0:
        return False
    
    return True

import requests
import json
from pyspark.sql import SparkSession
from datetime import datetime
import os
from include.jobs.landing import fetch_api_to_landing
from include.jobs.bronze import validate_landing_to_bronze_or_fail
from include.jobs.silver import transform_to_silver
from include.jobs.gold import transform_to_gold

class PipelineTest:
    def __init__(self):
        self.results = []
        self.test_id = 1
        self.spark = SparkSession.builder.appName("pipeline_tests").getOrCreate()

    def run_test(self, test_name, test_func, **kwargs):
        """Run a test and record the result"""
        try:
            result = test_func(**kwargs)
            self.results.append({
                'test_id': self.test_id,
                'test_name': test_name,
                'result': str(result),
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

    def test_api_request(self):
        """Test API request functionality"""
        # Test if we can make a successful request to the API
        url = "https://api.openbrewerydb.org/v1/breweries"
        response = requests.get(url)
        response.raise_for_status()
        
        # Test if we get valid JSON response
        data = response.json()
        assert isinstance(data, list), "API response is not a list"
        assert len(data) > 0, "API response is empty"
        
        # Test if required fields are present
        required_fields = ['id', 'name', 'brewery_type']
        for field in required_fields:
            assert all(field in item for item in data[:5]), f"Missing field: {field}"
        
        return {"status": "success", "message": "API request successful"}

    def test_landing_layer(self):
        """Test landing layer operations"""
        # Test if landing layer can process data
        result = fetch_api_to_landing()
        assert result['status'] == 'success', f"Landing layer failed: {result['message']}"
        
        # Test if data was written correctly
        landing_path = "include/data/landing/"
        df = self.spark.read.json(landing_path)
        assert df.count() > 0, "No data in landing zone"
        
        return {"status": "success", "message": "Landing layer successful"}

    def test_bronze_layer(self):
        """Test bronze layer operations"""
        # Test if bronze layer can process landing data
        validate_landing_to_bronze_or_fail()
        
        # Test if data was written correctly
        bronze_path = "include/data/bronze/"
        df = self.spark.read.parquet(bronze_path)
        assert df.count() > 0, "No data in bronze layer"
        
        return {"status": "success", "message": "Bronze layer successful"}

    def test_silver_layer(self):
        """Test silver layer operations"""
        # Test if silver layer can process bronze data
        transform_to_silver()
        
        # Test if data was written correctly
        silver_path = "include/data/silver/"
        df = self.spark.read.parquet(silver_path)
        assert df.count() > 0, "No data in silver layer"
        
        return {"status": "success", "message": "Silver layer successful"}

    def test_gold_layer(self):
        """Test gold layer operations"""
        # Test if gold layer can process silver data
        transform_to_gold()
        
        # Test if data was written correctly
        gold_path = "include/data/gold/"
        df = self.spark.read.parquet(gold_path)
        assert df.count() > 0, "No data in gold layer"
        
        return {"status": "success", "message": "Gold layer successful"}

    def run_all_tests(self):
        """Run all pipeline tests"""
        self.run_test("API Request Test", self.test_api_request)
        self.run_test("Landing Layer Test", self.test_landing_layer)
        self.run_test("Bronze Layer Test", self.test_bronze_layer)
        self.run_test("Silver Layer Test", self.test_silver_layer)
        self.run_test("Gold Layer Test", self.test_gold_layer)
        
        return self.results

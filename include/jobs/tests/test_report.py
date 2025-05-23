import streamlit as st
import json
import os
from datetime import datetime

def load_test_results():
    """Load and display test results"""
    results_path = "include/data_quality_results/test_results.json"
    
    if os.path.exists(results_path):
        with open(results_path, 'r') as f:
            results = json.load(f)
            return results
    return None

def display_test_results():
    """Display test results in Streamlit"""
    results = load_test_results()
    
    if results:
        test_run = results['test_run']
        tests = results['tests']
        
        # Summary
        st.header("Test Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Tests", test_run['total_tests'])
        col2.metric("Passed", test_run['passed'], 
                   f"{test_run['passed']/test_run['total_tests']*100:.1f}%")
        col3.metric("Failed", test_run['failed'])
        
        # Detailed Results
        st.header("Detailed Test Results")
        for test in tests:
            status = "✅" if test['passed'] else "❌"
            with st.expander(f"Test {test['test_id']}: {test['test_name']} {status}"):
                st.write(f"**Result:** {test['result']}")
                st.write(f"**Timestamp:** {test['timestamp']}")
    else:
        st.warning("No test results found. Please run the data quality tests first.")
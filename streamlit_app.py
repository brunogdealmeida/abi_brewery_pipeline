import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import glob
from include.jobs.tests.test_report import display_test_results

# Page config
st.set_page_config(
    page_title="Brewery Pipeline Dashboard",
    page_icon="🍺",
    layout="wide"
)

# Constants
RESULTS_DIR = "include/data_quality_results"
GOLD_DIR = "include/data/gold"

def load_validation_results():
    """Load and process validation results."""
    results_files = glob.glob(os.path.join(RESULTS_DIR, "summary_*.json"))
    results = []
    
    for file in sorted(results_files, reverse=True):
        with open(file, 'r') as f:
            data = json.load(f)
            results.append({
                'execution_date': datetime.fromisoformat(data['execution_date']),
                'overall_success': data['overall_success'],
                'layer_results': data['layer_results']
            })
    
    return results

def load_gold_data():
    """Load gold layer data."""
    try:
        gold_path = "include/data/gold/"
        if not os.path.exists(gold_path):
            return None
            
        # Find all parquet files in the directory and subdirectories
        files = []
        for root, dirs, filenames in os.walk(gold_path):
            for filename in filenames:
                if filename.endswith('.parquet'):
                    files.append(os.path.join(root, filename))
        
        if not files:
            return None
            
        # Read all parquet files and concatenate them
        dfs = []
        for file in files:
            df = pd.read_parquet(file)
            dfs.append(df)
        
        if not dfs:
            return None
            
        result_df = pd.concat(dfs, ignore_index=True)
        if result_df.empty:
            return None
            
        return result_df
        return df
    except Exception as e:
        st.error(f"Error loading gold data: {str(e)}")
        return None

def plot_gold_metrics(gold_data):
    """Plot metrics from gold layer data."""
    if gold_data is None:
        st.warning("No gold layer data available.")
        return None, None, None

    # Plot 1: Brewery Count by State
    fig1 = px.bar(
        gold_data,
        x="state",
        y="count",
        color="brewery_type",
        title="Brewery Count by State and Type",
        labels={"count": "Number of Breweries"}
    )

    # Plot 2: Brewery Type Distribution
    fig2 = px.pie(
        gold_data,
        values="count",
        names="brewery_type",
        title="Brewery Type Distribution",
        hole=0.3
    )

    # Plot 3: Top 10 States by Brewery Count
    top_states = gold_data.groupby('state').sum().reset_index()
    top_states = top_states.sort_values('count', ascending=False).head(10)
    fig3 = px.bar(
        top_states,
        x="state",
        y="count",
        title="Top 10 States by Brewery Count",
        labels={"count": "Number of Breweries"}
    )

    return fig1, fig2, fig3

def main():
    """Main application function"""
    st.title("Brewery Pipeline Dashboard 🍺")
    
    # Load gold data first
    gold_data = load_gold_data()
    
    # Create main tabs
    tab1, tab2, tab3 = st.tabs(["📊 Tests Results", "📊 Data Quality", "📊 Gold Layer Analytics"])

    with tab1:
        # Tests Results Tab
        st.header("Pipeline Tests Results")
        display_test_results()

    with tab2:
        # Data Quality Tab
        st.header("Data Quality Metrics")
        
        # Load validation results
        validation_results = load_validation_results()
        
        if not validation_results:
            st.warning("No validation results available")
            return
        
        latest = validation_results[0]
        
        # Create tabs for each layer
        layer_tabs = st.tabs(["Bronze", "Silver", "Gold"])
        
        # Display results for each layer
        for layer, tab in zip(latest['layer_results'].keys(), layer_tabs):
            with tab:
                layer_result = latest['layer_results'][layer]
                
                # Show layer status
                st.subheader(f"{layer.upper()} Layer")
                if layer == "bronze":
                    # For Bronze layer, show ✅ if there are no null IDs
                    null_id_count = layer_result['checks'].get('null_checks', {}).get('id', {}).get('null_count', 0)
                    st.metric(
                        "Status",
                        "✅" if null_id_count == 0 else "❌"
                    )
                else:
                    # For other layers, show ✅ if all checks are valid
                    st.metric(
                        "Status",
                        "✅" if all(check.get('valid_percentage', 0) == 100 
                                   for check in layer_result['checks'].values()) else "❌"
                    )
                
                # Show total records
                st.metric(
                    "Total Records",
                    f"{layer_result['total_records']:,}",
                    delta=None
                )
                
                # Show specific checks
                for check_name, check_result in layer_result['checks'].items():
                    st.subheader(check_name.replace('_', ' ').title())
                    
                    if isinstance(check_result, dict):
                        # Create columns for metrics
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # For null checks in Bronze layer, show only ID stats
                            if layer == "bronze" and check_name == "null_checks":
                                id_null_count = check_result.get('id', {}).get('null_count', 0)
                                st.metric(
                                    "ID Null Count",
                                    f"{id_null_count:,}",
                                    delta=None
                                )
                            else:
                                for metric_name, value in check_result.items():
                                    if metric_name.endswith('_count'):
                                        st.metric(
                                            metric_name.replace('_', ' ').title(),
                                            f"{value:,}",
                                            delta=None
                                        )
                        
                        with col2:
                            # For null checks in Bronze layer, show only ID stats
                            if layer == "bronze" and check_name == "null_checks":
                                id_null_percentage = check_result.get('id', {}).get('null_percentage', 0)
                                # Show 100% when count is 0
                                if id_null_count == 0:
                                    id_null_percentage = 100.0
                                st.metric(
                                    "ID Null %",
                                    f"{id_null_percentage:.1f}%",
                                    delta=None
                                )
                            # For Silver layer duplicates, show correct percentage
                            elif layer == "silver" and check_name == "duplicates":
                                # Calculate percentage as (count / total_records) * 100
                                duplicate_percentage = (check_result['count'] / layer_result['total_records']) * 100
                                st.metric(
                                    "Duplicates %",
                                    f"{duplicate_percentage:.1f}%",
                                    delta=None
                                )
                            else:
                                for metric_name, value in check_result.items():
                                    if metric_name.endswith('_percentage'):
                                        # Show 100% when count is 0
                                        if value == 0 and check_result.get(f'{metric_name.replace("_percentage", "_count")}', 0) == 0:
                                            value = 100.0
                                        st.metric(
                                            metric_name.replace('_', ' ').title(),
                                            f"{value:.1f}%",
                                            delta=None
                                        )
                    else:
                        st.write(check_result)
                        
    with tab3:
        # Gold Layer Analytics Tab
        st.header("Gold Layer Analytics")
        
        # Load gold data
        gold_data = load_gold_data()
        
        if gold_data is not None:
            # Create sub-tabs for plots and dataframe
            sub_tab1, sub_tab2 = st.tabs(["📊 Plots", "📊 Dataframe"])
            
            with sub_tab1:
                # Show plots
                fig1, fig2, fig3 = plot_gold_metrics(gold_data)
                st.plotly_chart(fig1, use_container_width=True)
                st.plotly_chart(fig2, use_container_width=True)
                st.plotly_chart(fig3, use_container_width=True)
            
            with sub_tab2:
                # Show raw data
                st.header("Raw Data")
                st.dataframe(gold_data)
        else:
            st.error("No data found in gold layer.")

if __name__ == "__main__":
    main() 
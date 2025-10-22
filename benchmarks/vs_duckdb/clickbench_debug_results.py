#!/usr/bin/env python3
"""
Debug ClickBench Results - Find Why Sabot Returns Different Row Counts
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import duckdb
from sabot import cyarrow as ca
from sabot_sql import create_sabot_sql_bridge
import pyarrow.compute as pc


def create_simple_data():
    """Create very simple data for debugging."""
    data = {
        'UserID': [1, 2, 3, 1, 2, 3, 1, 2, 3, 1],  # 3 unique users
        'AdvEngineID': [0, 1, 2, 0, 1, 2, 0, 1, 2, 0],  # 6 zeros, 4 non-zeros
        'ResolutionWidth': [1920, 1080, 1920, 1080, 1920, 1080, 1920, 1080, 1920, 1080],
        'Amount': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    }
    
    df = pd.DataFrame(data)
    
    print("Test Data (10 rows):")
    print(df)
    print(f"\nExpected Results:")
    print(f"  COUNT(*): {len(df)}")
    print(f"  COUNT WHERE AdvEngineID <> 0: {sum(1 for x in data['AdvEngineID'] if x != 0)}")
    print(f"  COUNT DISTINCT UserID: {len(set(data['UserID']))}")
    print(f"  AVG(ResolutionWidth): {sum(data['ResolutionWidth'])/len(data['ResolutionWidth'])}")
    print(f"  SUM(AdvEngineID): {sum(data['AdvEngineID'])}")
    
    return df


def debug_query(duckdb_conn, sabot_bridge, query, description):
    """Debug a single query to see exactly what's returned."""
    print("\n" + "=" * 80)
    print(f"Debugging: {description}")
    print("=" * 80)
    print(f"SQL: {query}")
    print()
    
    # DuckDB
    print("DuckDB Result:")
    duckdb_result = duckdb_conn.execute(query).fetchall()
    print(f"  Raw result: {duckdb_result}")
    print(f"  Type: {type(duckdb_result)}")
    print(f"  Rows: {len(duckdb_result)}")
    if duckdb_result:
        print(f"  First row: {duckdb_result[0]}")
        print(f"  First row type: {type(duckdb_result[0])}")
    
    # Sabot
    print("\nSabot Result:")
    sabot_result = sabot_bridge.execute_sql(query)
    print(f"  Raw result type: {type(sabot_result)}")
    print(f"  Num rows: {sabot_result.num_rows if hasattr(sabot_result, 'num_rows') else 'N/A'}")
    print(f"  Num columns: {sabot_result.num_columns if hasattr(sabot_result, 'num_columns') else 'N/A'}")
    
    if hasattr(sabot_result, 'schema'):
        print(f"  Schema: {sabot_result.schema}")
    
    if hasattr(sabot_result, 'to_pydict'):
        pydict = sabot_result.to_pydict()
        print(f"  As dict: {pydict}")
    
    if hasattr(sabot_result, 'column'):
        print(f"  Column names: {sabot_result.column_names if hasattr(sabot_result, 'column_names') else 'N/A'}")
        for i in range(min(3, sabot_result.num_columns)):
            col = sabot_result.column(i)
            print(f"  Column {i}: {col.to_pylist()}")
    
    # Compare
    print("\nComparison:")
    print(f"  DuckDB returns: {len(duckdb_result)} rows")
    print(f"  Sabot returns: {sabot_result.num_rows if hasattr(sabot_result, 'num_rows') else 'N/A'} rows")
    
    if duckdb_result and hasattr(sabot_result, 'column'):
        duckdb_value = duckdb_result[0][0] if isinstance(duckdb_result[0], tuple) else duckdb_result[0]
        sabot_col = sabot_result.column(0)
        sabot_value = sabot_col[0].as_py() if hasattr(sabot_col[0], 'as_py') else sabot_col.to_pylist()[0]
        
        print(f"  DuckDB value: {duckdb_value}")
        print(f"  Sabot value: {sabot_value}")
        
        if abs(duckdb_value - sabot_value) < 0.01:
            print(f"  ✓ Values match!")
        else:
            print(f"  ✗ VALUES DIFFER! Difference: {abs(duckdb_value - sabot_value)}")


def main():
    """Run debug tests."""
    print("=" * 80)
    print("ClickBench Result Debugging")
    print("=" * 80)
    print("\nInvestigating why Sabot returns different row counts...")
    
    # Create simple test data
    df = create_simple_data()
    
    # Setup DuckDB
    print("\n" + "=" * 80)
    print("Setting up DuckDB...")
    duckdb_conn = duckdb.connect()
    duckdb_conn.register('hits', df)
    print("✓ DuckDB ready")
    
    # Setup Sabot
    print("\nSetting up Sabot...")
    sabot_table = ca.table(df.to_dict('list'))
    sabot_bridge = create_sabot_sql_bridge()
    sabot_bridge.register_table("hits", sabot_table)
    print(f"✓ Sabot ready: {sabot_table.num_rows} rows")
    
    # Debug each query
    debug_query(duckdb_conn, sabot_bridge, 
                "SELECT COUNT(*) FROM hits",
                "Query 1: Simple COUNT")
    
    debug_query(duckdb_conn, sabot_bridge,
                "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
                "Query 2: COUNT with WHERE")
    
    debug_query(duckdb_conn, sabot_bridge,
                "SELECT AVG(ResolutionWidth) FROM hits",
                "Query 4: Simple AVG")
    
    debug_query(duckdb_conn, sabot_bridge,
                "SELECT COUNT(DISTINCT UserID) FROM hits",
                "Query 5: COUNT DISTINCT")
    
    debug_query(duckdb_conn, sabot_bridge,
                "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
                "Query 3: Multiple aggregations")
    
    # Cleanup
    duckdb_conn.close()
    
    print("\n" + "=" * 80)
    print("Investigation Complete")
    print("=" * 80)
    print("\nNext: Analyze output to understand result format differences")


if __name__ == '__main__':
    main()


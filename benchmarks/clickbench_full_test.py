#!/usr/bin/env python3
"""
Full ClickBench Test - All Queries
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import time
import pandas as pd
import duckdb
from sabot import cyarrow as ca
from sabot_sql import create_sabot_sql_bridge


def create_test_data(num_rows=1_000_000):
    """Create test data."""
    import random
    
    print(f"Creating test data ({num_rows:,} rows)...")
    start = time.time()
    
    data = {
        'WatchID': list(range(num_rows)),
        'JavaEnable': [random.choice([0, 1]) for _ in range(num_rows)],
        'Title': [f'Title {i % 1000}' for i in range(num_rows)],
        'GoodEvent': [1] * num_rows,
        'EventTime': [1000000000 + i * 60 for i in range(num_rows)],
        'EventDate': [18000 + i // 10000 for i in range(num_rows)],
        'CounterID': [i % 1000 for i in range(num_rows)],
        'ClientIP': [f'192.168.{(i//256)%256}.{i%256}' for i in range(num_rows)],
        'RegionID': [i % 100 for i in range(num_rows)],
        'UserID': [i % 100000 for i in range(num_rows)],
        'CounterClass': [0] * num_rows,
        'OS': [random.choice([1, 2, 3]) for _ in range(num_rows)],
        'UserAgent': [random.choice([1, 2, 3, 4, 5]) for _ in range(num_rows)],
        'URL': [f'https://example.com/page_{i % 50000}' for i in range(num_rows)],
        'Referer': [f'https://referer.com/{i % 10000}' if i % 10 > 0 else '' for i in range(num_rows)],
        'IsRefresh': [0] * num_rows,
        'RefererCategoryID': [i % 50 for i in range(num_rows)],
        'RefererRegionID': [i % 100 for i in range(num_rows)],
        'URLCategoryID': [i % 50 for i in range(num_rows)],
        'URLRegionID': [i % 100 for i in range(num_rows)],
        'ResolutionWidth': [random.choice([1920, 1366, 1280, 1024, 800]) for _ in range(num_rows)],
        'ResolutionHeight': [random.choice([1080, 768, 1024, 600]) for _ in range(num_rows)],
        'ResolutionDepth': [random.choice([24, 32]) for _ in range(num_rows)],
        'FlashMajor': [random.choice([0, 11, 10]) for _ in range(num_rows)],
        'FlashMinor': [random.choice([0, 1, 2]) for _ in range(num_rows)],
        'FlashMinor2': [0] * num_rows,
        'NetMajor': [random.choice([0, 1, 2]) for _ in range(num_rows)],
        'NetMinor': [0] * num_rows,
        'UserAgentMajor': [random.choice([1, 2, 3, 4, 5]) for _ in range(num_rows)],
        'UserAgentMinor': [f'{random.randint(0,99)}' for _ in range(num_rows)],
        'CookieEnable': [1] * num_rows,
        'JavascriptEnable': [1] * num_rows,
        'IsMobile': [random.choice([0, 1]) for _ in range(num_rows)],
        'MobilePhone': [random.choice([0, 1, 2]) for _ in range(num_rows)],
        'MobilePhoneModel': [f'model_{i % 50}' if i % 100 < 10 else '' for i in range(num_rows)],
        'Params': [''] * num_rows,
        'IPNetworkID': [i % 1000 for i in range(num_rows)],
        'TraficSourceID': [i % 10 for i in range(num_rows)],
        'SearchEngineID': [i % 5 for i in range(num_rows)],
        'SearchPhrase': [f'search_{i % 10000}' if i % 100 < 20 else '' for i in range(num_rows)],
        'AdvEngineID': [random.choice([0, 0, 0, 1, 2, 3]) for _ in range(num_rows)],
        'IsArtifical': [0] * num_rows,
        'WindowClientWidth': [random.choice([1900, 1366, 1280]) for _ in range(num_rows)],
        'WindowClientHeight': [random.choice([1000, 768, 1024]) for _ in range(num_rows)],
        'ClientTimeZone': [0] * num_rows,
        'ClientEventTime': [1000000000 + i * 60 for i in range(num_rows)],
        'SilverlightVersion1': [0] * num_rows,
        'SilverlightVersion2': [0] * num_rows,
        'SilverlightVersion3': [0] * num_rows,
        'SilverlightVersion4': [0] * num_rows,
        'PageCharset': ['utf-8'] * num_rows,
        'CodeVersion': [1] * num_rows,
        'IsLink': [0] * num_rows,
        'IsDownload': [0] * num_rows,
        'IsNotBounce': [1] * num_rows,
        'FUniqID': [i for i in range(num_rows)],
        'OriginalURL': [f'https://example.com/page_{i % 50000}' for i in range(num_rows)],
        'HID': [i for i in range(num_rows)],
        'IsOldCounter': [0] * num_rows,
        'IsEvent': [random.choice([0, 1]) for _ in range(num_rows)],
        'IsParameter': [0] * num_rows,
        'DontCountHits': [0] * num_rows,
        'WithHash': [0] * num_rows,
        'HitColor': [''] * num_rows,
        'LocalEventTime': [1000000000 + i * 60 for i in range(num_rows)],
        'Age': [random.choice([0, 18, 25, 35, 45, 55, 65]) for _ in range(num_rows)],
        'Sex': [random.choice([0, 1, 2]) for _ in range(num_rows)],
        'Income': [random.choice([0, 1, 2, 3, 4, 5]) for _ in range(num_rows)],
        'Interests': [0] * num_rows,
        'Robotness': [0] * num_rows,
        'RemoteIP': [i % 100000 for i in range(num_rows)],
        'WindowName': [0] * num_rows,
        'OpenerName': [0] * num_rows,
        'HistoryLength': [random.randint(0, 20) for _ in range(num_rows)],
        'BrowserLanguage': ['en'] * num_rows,
        'BrowserCountry': ['US'] * num_rows,
        'SocialNetwork': [''] * num_rows,
        'SocialAction': [''] * num_rows,
        'HTTPError': [0] * num_rows,
        'SendTiming': [random.randint(0, 1000) for _ in range(num_rows)],
        'DNSTiming': [random.randint(0, 100) for _ in range(num_rows)],
        'ConnectTiming': [random.randint(0, 100) for _ in range(num_rows)],
        'ResponseStartTiming': [random.randint(0, 500) for _ in range(num_rows)],
        'ResponseEndTiming': [random.randint(0, 1000) for _ in range(num_rows)],
        'FetchTiming': [random.randint(0, 1000) for _ in range(num_rows)],
        'SocialSourceNetworkID': [0] * num_rows,
        'SocialSourcePage': [''] * num_rows,
        'ParamPrice': [0] * num_rows,
        'ParamOrderID': [''] * num_rows,
        'ParamCurrency': [''] * num_rows,
        'ParamCurrencyID': [0] * num_rows,
        'OpenstatServiceName': [''] * num_rows,
        'OpenstatCampaignID': [''] * num_rows,
        'OpenstatAdID': [''] * num_rows,
        'OpenstatSourceID': [''] * num_rows,
        'UTMSource': [''] * num_rows,
        'UTMMedium': [''] * num_rows,
        'UTMCampaign': [''] * num_rows,
        'UTMContent': [''] * num_rows,
        'UTMTerm': [''] * num_rows,
        'FromTag': [''] * num_rows,
        'HasGCLID': [0] * num_rows,
        'RefererHash': [hash(f'https://referer.com/{i % 10000}') % 2**63 for i in range(num_rows)],
        'URLHash': [hash(f'https://example.com/page_{i % 50000}') % 2**63 for i in range(num_rows)],
        'CLID': [0] * num_rows,
    }
    
    df = pd.DataFrame(data)
    elapsed = time.time() - start
    print(f"✓ Created {num_rows:,} rows, {len(df.columns)} columns in {elapsed:.1f}s")
    
    return df


def run_benchmarks():
    """Run all ClickBench queries."""
    print("=" * 80)
    print("ClickBench Full Test: All Queries")
    print("=" * 80)
    
    # Load queries
    queries_file = os.path.join(os.path.dirname(__file__), 'clickbench/queries.sql')
    with open(queries_file) as f:
        queries = [line.strip() for line in f if line.strip()]
    
    print(f"Loaded {len(queries)} queries\n")
    
    # Create data
    df = create_test_data(1_000_000)
    
    # Setup systems
    print("\nSetting up DuckDB...")
    duckdb_conn = duckdb.connect()
    duckdb_conn.register('hits', df)
    
    print("Setting up Sabot...")
    sabot_table = ca.table(df.to_dict('list'))
    sabot_bridge = create_sabot_sql_bridge()
    sabot_bridge.register_table("hits", sabot_table)
    print(f"✓ Ready: {len(df):,} rows\n")
    
    results = []
    
    for idx, query in enumerate(queries, 1):  # All queries
        print(f"\n[{idx}/{len(queries)}] {query[:80]}...")
        
        try:
            # DuckDB
            start = time.perf_counter()
            duckdb_result = duckdb_conn.execute(query).fetchall()
            duckdb_time = time.perf_counter() - start
            
            # Sabot
            start = time.perf_counter()
            sabot_result = sabot_bridge.execute_sql(query)
            sabot_time = time.perf_counter() - start
            
            speedup = duckdb_time / sabot_time if sabot_time > 0 else 0
            winner = "Sabot" if speedup > 1.1 else ("DuckDB" if speedup < 0.9 else "Tie")
            
            print(f"  DuckDB: {duckdb_time*1000:.2f}ms | Sabot: {sabot_time*1000:.2f}ms | {winner} ({speedup:.2f}x)")
            
            results.append({
                'query_id': idx,
                'duckdb_time': duckdb_time,
                'sabot_time': sabot_time,
                'speedup': speedup,
                'winner': winner
            })
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            results.append({
                'query_id': idx,
                'error': str(e)
            })
    
    # Summary
    print("\n" + "=" * 80)
    print(f"SUMMARY (All {len(valid_results)} Queries)")
    print("=" * 80)
    
    valid_results = [r for r in results if 'speedup' in r]
    
    total_duckdb = sum(r['duckdb_time'] for r in valid_results)
    total_sabot = sum(r['sabot_time'] for r in valid_results)
    
    sabot_wins = sum(1 for r in valid_results if r['winner'] == 'Sabot')
    duckdb_wins = sum(1 for r in valid_results if r['winner'] == 'DuckDB')
    ties = sum(1 for r in valid_results if r['winner'] == 'Tie')
    
    print(f"\nTotal Time:")
    print(f"  DuckDB: {total_duckdb*1000:.1f}ms")
    print(f"  Sabot:  {total_sabot*1000:.1f}ms")
    
    overall = total_duckdb / total_sabot if total_sabot > 0 else 0
    if overall > 1.1:
        print(f"  Overall: Sabot {overall:.2f}x faster")
    elif overall < 0.9:
        print(f"  Overall: DuckDB {1/overall:.2f}x faster")
    else:
        print(f"  Overall: Tied")
    
    print(f"\nWins: DuckDB={duckdb_wins}, Sabot={sabot_wins}, Ties={ties}")
    
    # Best for each
    sabot_best = sorted([r for r in valid_results if r['winner'] == 'Sabot'], 
                        key=lambda x: x['speedup'], reverse=True)[:3]
    if sabot_best:
        print(f"\nTop Sabot wins:")
        for r in sabot_best:
            print(f"  Q{r['query_id']}: {r['speedup']:.2f}x faster")
    
    duckdb_best = sorted([r for r in valid_results if r['winner'] == 'DuckDB'],
                         key=lambda x: x['speedup'])[:3]
    if duckdb_best:
        print(f"\nTop DuckDB wins:")
        for r in duckdb_best:
            print(f"  Q{r['query_id']}: {1/r['speedup']:.2f}x faster")
    
    duckdb_conn.close()


if __name__ == '__main__':
    run_benchmarks()


#!/usr/bin/env python3
"""
Fintech Kernels Demo - Streaming financial analytics with Sabot.

This demo shows how to use Sabot's fintech kernels for real-time
market analytics, risk management, and execution analysis.

Features demonstrated:
1. Online statistics (EWMA, rolling z-scores)
2. Microstructure (midprice, order flow imbalance)
3. Volatility (realized variance, bipower variation)
4. Liquidity (Kyle lambda, Amihud illiquidity)
5. FX arbitrage (triangular arbitrage detection)
6. Crypto funding rates
7. Execution TCA (VWAP, implementation shortfall)
8. Risk monitoring (CUSUM change detection, VaR)

All kernels process Arrow RecordBatches for maximum performance.
"""

import numpy as np
import pyarrow as pa
from datetime import datetime

# Import Sabot fintech kernels
try:
    from sabot.fintech import (
        # Online stats
        log_returns,
        ewma,
        welford_mean_var,
        rolling_zscore,
        # Microstructure
        midprice,
        microprice,
        l1_imbalance,
        ofi,
        # Volatility
        realized_var,
        bipower_var,
        # Liquidity
        kyle_lambda,
        amihud_illiquidity,
        # FX
        triangular_arbitrage,
        carry_signal,
        # Crypto
        funding_apr,
        basis_annualized,
        # Execution
        vwap,
        implementation_shortfall,
        # Risk
        cusum,
        historical_var,
    )
    KERNELS_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Fintech kernels not available: {e}")
    print("Run 'python build.py' to compile Cython extensions")
    KERNELS_AVAILABLE = False
    exit(1)


def generate_sample_price_data(n=1000):
    """Generate synthetic price data for testing."""
    np.random.seed(42)
    
    # Geometric Brownian motion
    returns = np.random.normal(0.0001, 0.02, n)
    prices = 100.0 * np.exp(np.cumsum(returns))
    
    # Add bid/ask spread
    spread_bps = 5.0
    bid = prices * (1 - spread_bps / 20000)
    ask = prices * (1 + spread_bps / 20000)
    
    # Generate sizes
    bid_size = np.random.exponential(1000, n)
    ask_size = np.random.exponential(1000, n)
    
    # Generate volumes
    volume = np.random.exponential(500, n)
    
    return {
        'price': prices,
        'bid': bid,
        'ask': ask,
        'bid_size': bid_size,
        'ask_size': ask_size,
        'volume': volume,
    }


def demo_online_statistics():
    """Demo 1: Online statistics kernels."""
    print("\n" + "="*60)
    print("DEMO 1: Online Statistics (EWMA, Welford, Z-Scores)")
    print("="*60)
    
    # Generate price data
    data = generate_sample_price_data(1000)
    batch = pa.record_batch(data, names=list(data.keys()))
    
    print(f"Processing {batch.num_rows} price ticks...")
    
    # 1. Compute log returns
    print("\n1. Computing log returns...")
    batch = log_returns(batch, 'price')
    print(f"   ✓ Added 'log_return' column")
    
    # 2. Compute EWMA
    print("\n2. Computing EWMA (alpha=0.94, ~20-period half-life)...")
    batch = ewma(batch, alpha=0.94)
    print(f"   ✓ Added 'ewma' column")
    
    # 3. Compute rolling z-score
    print("\n3. Computing rolling z-scores (window=100)...")
    batch = rolling_zscore(batch, window=100)
    print(f"   ✓ Added 'zscore' column")
    
    # Print sample results
    print("\n📊 Sample results (last 5 rows):")
    df = batch.to_pandas().tail(5)
    print(df[['price', 'log_return', 'ewma', 'zscore']].to_string(index=False))
    
    print("\n✅ Online statistics demo complete!")
    return batch


def demo_microstructure():
    """Demo 2: Microstructure kernels."""
    print("\n" + "="*60)
    print("DEMO 2: Microstructure (Midprice, Microprice, OFI)")
    print("="*60)
    
    # Generate quote data
    data = generate_sample_price_data(500)
    batch = pa.record_batch(data, names=list(data.keys()))
    
    print(f"Processing {batch.num_rows} quote updates...")
    
    # 1. Compute midprice
    print("\n1. Computing midprice...")
    batch = midprice(batch)
    print(f"   ✓ Added 'midprice' column")
    
    # 2. Compute microprice (size-weighted)
    print("\n2. Computing microprice (size-weighted)...")
    batch = microprice(batch)
    print(f"   ✓ Added 'microprice' column")
    
    # 3. Compute L1 imbalance
    print("\n3. Computing L1 order book imbalance...")
    batch = l1_imbalance(batch)
    print(f"   ✓ Added 'l1_imbalance' column")
    
    # 4. Compute OFI
    print("\n4. Computing order flow imbalance (OFI)...")
    batch = ofi(batch)
    print(f"   ✓ Added 'ofi' column")
    
    # Print sample results
    print("\n📊 Sample results (last 5 rows):")
    df = batch.to_pandas().tail(5)
    print(df[['bid', 'ask', 'midprice', 'microprice', 'l1_imbalance', 'ofi']].to_string(index=False))
    
    print("\n✅ Microstructure demo complete!")
    return batch


def demo_volatility_and_liquidity():
    """Demo 3: Volatility and liquidity kernels."""
    print("\n" + "="*60)
    print("DEMO 3: Volatility & Liquidity (RV, Kyle Lambda)")
    print("="*60)
    
    # Generate data with returns
    data = generate_sample_price_data(500)
    batch = pa.record_batch(data, names=list(data.keys()))
    batch = log_returns(batch, 'price')
    
    # Add signed volume for Kyle lambda
    signed_vol = data['volume'] * np.random.choice([-1, 1], len(data['volume']))
    batch = batch.append_column('signed_volume', pa.array(signed_vol))
    
    print(f"Processing {batch.num_rows} observations...")
    
    # 1. Realized variance
    print("\n1. Computing realized variance (20-period window)...")
    batch = realized_var(batch, 'log_return', window=20)
    print(f"   ✓ Added 'realized_var' column")
    
    # 2. Bipower variation (jump-robust)
    print("\n2. Computing bipower variation (jump-robust estimator)...")
    batch = bipower_var(batch, 'log_return', window=20)
    print(f"   ✓ Added 'bipower_var' column")
    
    # 3. Kyle's lambda (price impact)
    print("\n3. Computing Kyle's lambda (price impact)...")
    batch = kyle_lambda(batch, 'log_return', 'signed_volume', window=20)
    print(f"   ✓ Added 'kyle_lambda' column")
    
    # 4. Amihud illiquidity
    print("\n4. Computing Amihud illiquidity...")
    batch = amihud_illiquidity(batch, 'log_return', 'volume', window=20)
    print(f"   ✓ Added 'amihud_illiquidity' column")
    
    # Print sample results
    print("\n📊 Sample results (last 5 rows):")
    df = batch.to_pandas().tail(5)
    print(df[['realized_var', 'bipower_var', 'kyle_lambda', 'amihud_illiquidity']].to_string(index=False))
    
    print("\n✅ Volatility & liquidity demo complete!")
    return batch


def demo_fx_crypto():
    """Demo 4: FX and crypto kernels."""
    print("\n" + "="*60)
    print("DEMO 4: FX & Crypto (Arbitrage, Funding Rates)")
    print("="*60)
    
    # Generate FX triangular arb data
    n = 100
    spot_ab = np.random.uniform(1.10, 1.12, n)
    spot_bc = np.random.uniform(0.85, 0.87, n)
    spot_ac = spot_ab * spot_bc * (1 + np.random.normal(0, 0.0001, n))  # Small deviation
    fees_bps = np.full(n, 2.0)  # 2 bps fees
    
    fx_data = {
        'spot_ab': spot_ab,
        'spot_bc': spot_bc,
        'spot_ac': spot_ac,
        'fees_bps': fees_bps,
    }
    fx_batch = pa.record_batch(fx_data, names=list(fx_data.keys()))
    
    print(f"\n1. Detecting triangular arbitrage opportunities...")
    fx_batch = triangular_arbitrage(fx_batch)
    
    df_fx = fx_batch.to_pandas()
    arb_opportunities = df_fx[df_fx['arb_edge_bps'] > 0]
    print(f"   ✓ Found {len(arb_opportunities)} arbitrage opportunities")
    if len(arb_opportunities) > 0:
        print(f"   📈 Best edge: {arb_opportunities['arb_edge_bps'].max():.2f} bps")
    
    # Generate crypto funding rate data
    funding_rate = np.random.normal(0.0001, 0.00005, n)  # 0.01% ± 0.005%
    crypto_data = {
        'funding_rate': funding_rate,
    }
    crypto_batch = pa.record_batch(crypto_data, names=list(crypto_data.keys()))
    
    print(f"\n2. Annualizing funding rates (8h frequency)...")
    crypto_batch = funding_apr(crypto_batch, funding_freq_hours=8.0)
    
    df_crypto = crypto_batch.to_pandas()
    print(f"   ✓ Average funding APR: {df_crypto['funding_apr'].mean()*100:.2f}%")
    
    # Generate crypto basis data
    perp_price = 50000.0 * (1 + np.cumsum(np.random.normal(0, 0.01, n)))
    spot_price = perp_price * (1 + np.random.normal(0.0002, 0.0001, n))  # Small basis
    basis_data = {
        'perp_price': perp_price,
        'spot_price': spot_price,
    }
    basis_batch = pa.record_batch(basis_data, names=list(basis_data.keys()))
    
    print(f"\n3. Computing annualized basis (perp vs spot)...")
    basis_batch = basis_annualized(basis_batch, T_days=90.0)
    
    df_basis = basis_batch.to_pandas()
    print(f"   ✓ Average basis: {df_basis['basis_apr'].mean()*100:.3f}%")
    
    print("\n✅ FX & crypto demo complete!")


def demo_execution_and_risk():
    """Demo 5: Execution and risk kernels."""
    print("\n" + "="*60)
    print("DEMO 5: Execution & Risk (VWAP, CUSUM, VaR)")
    print("="*60)
    
    # Generate execution data
    data = generate_sample_price_data(500)
    batch = pa.record_batch(data, names=list(data.keys()))
    
    print(f"Processing {batch.num_rows} trades...")
    
    # 1. Compute VWAP
    print("\n1. Computing VWAP...")
    batch = vwap(batch, 'price', 'volume')
    print(f"   ✓ Added 'vwap' column")
    
    # Add arrival price for IS calculation
    arrival_price = data['price'][0]
    batch = batch.append_column('arrival_price', pa.array([arrival_price] * len(data['price'])))
    batch = batch.append_column('exec_price', batch.column('price'))  # Alias for demo
    
    # 2. Implementation shortfall
    print("\n2. Computing implementation shortfall...")
    batch = implementation_shortfall(batch)
    print(f"   ✓ Added 'implementation_shortfall_bps' column")
    
    # 3. CUSUM change detection on returns
    batch = log_returns(batch, 'price')
    print("\n3. Running CUSUM change detection on returns...")
    batch = cusum(batch, 'log_return', k=0.0005, h=0.01)
    print(f"   ✓ Added 'cusum_stat' and 'cusum_alarm' columns")
    
    df = batch.to_pandas()
    alarms = df[df['cusum_alarm'] == 1]
    print(f"   🚨 Detected {len(alarms)} change points")
    
    # 4. Historical VaR
    # Generate P&L data
    pnl = np.cumsum(np.random.normal(100, 1000, 500))
    risk_batch = pa.record_batch({'pnl': pnl}, names=['pnl'])
    
    print("\n4. Computing historical VaR (95%)...")
    risk_batch = historical_var(risk_batch, 'pnl', alpha=0.05, window=100)
    print(f"   ✓ Added 'var' column")
    
    df_risk = risk_batch.to_pandas().tail(10)
    print(f"   📊 Latest VaR: ${df_risk['var'].iloc[-1]:.2f}")
    
    print("\n✅ Execution & risk demo complete!")


def main():
    """Run all demos."""
    print("\n" + "="*70)
    print("🚀 SABOT FINTECH KERNELS DEMO")
    print("="*70)
    print("\nHigh-performance streaming financial analytics with Arrow")
    print("All kernels process RecordBatches with O(1) amortized updates")
    
    if not KERNELS_AVAILABLE:
        print("\n❌ Cython kernels not available. Please compile first.")
        return
    
    # Run demos
    demo_online_statistics()
    demo_microstructure()
    demo_volatility_and_liquidity()
    demo_fx_crypto()
    demo_execution_and_risk()
    
    print("\n" + "="*70)
    print("✅ ALL DEMOS COMPLETE")
    print("="*70)
    print("\n💡 Next steps:")
    print("  - Integrate kernels into Sabot Stream pipelines")
    print("  - Connect to Kafka for real-time processing")
    print("  - Build custom strategies with composed kernels")
    print("  - Scale with morsel-driven parallelism")
    print("\nSee examples/ for more use cases!")


if __name__ == "__main__":
    main()


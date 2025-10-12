#!/usr/bin/env python3
"""
Tests for Sabot fintech kernels.

Tests are integration-level (not units) per project guidelines.
"""

import pytest
import numpy as np
import pyarrow as pa

# Import kernels
try:
    from sabot.fintech import (
        log_returns,
        ewma,
        welford_mean_var,
        rolling_zscore,
        midprice,
        microprice,
        l1_imbalance,
        ofi,
        realized_var,
        bipower_var,
        kyle_lambda,
        amihud_illiquidity,
        triangular_arbitrage,
        funding_apr,
        basis_annualized,
        vwap,
        implementation_shortfall,
        cusum,
        historical_var,
    )
    KERNELS_AVAILABLE = True
except ImportError:
    KERNELS_AVAILABLE = False


@pytest.mark.skipif(not KERNELS_AVAILABLE, reason="Fintech kernels not compiled")
class TestOnlineStats:
    """Test online statistics kernels."""
    
    def test_log_returns(self):
        """Test log returns computation."""
        prices = np.array([100.0, 102.0, 101.0, 103.0, 102.5])
        batch = pa.record_batch({'price': prices}, names=['price'])
        
        result = log_returns(batch, 'price')
        
        assert 'log_return' in result.schema.names
        assert result.num_rows == len(prices)
        
        returns = result.column('log_return').to_numpy()
        # First return should be NaN or 0
        # Second return: log(102/100) ≈ 0.0198
        assert np.isclose(returns[1], np.log(102.0 / 100.0), rtol=1e-6)
    
    def test_ewma(self):
        """Test EWMA kernel."""
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        batch = pa.record_batch({'value': values}, names=['value'])
        
        result = ewma(batch, alpha=0.5)
        
        assert 'ewma' in result.schema.names
        ewma_values = result.column('ewma').to_numpy()
        
        # EWMA should be smooth and increasing
        assert ewma_values[0] == 1.0  # First value
        assert ewma_values[-1] < 5.0  # Lagging
        assert ewma_values[-1] > 3.0  # But catching up
    
    def test_rolling_zscore(self):
        """Test rolling z-score kernel."""
        # Generate data with outlier
        values = np.concatenate([
            np.random.normal(0, 1, 100),
            [10.0],  # Outlier
            np.random.normal(0, 1, 50)
        ])
        batch = pa.record_batch({'value': values}, names=['value'])
        
        result = rolling_zscore(batch, window=20)
        
        assert 'zscore' in result.schema.names
        zscores = result.column('zscore').to_numpy()
        
        # Outlier should have high z-score
        assert zscores[100] > 3.0  # More than 3 std devs


@pytest.mark.skipif(not KERNELS_AVAILABLE, reason="Fintech kernels not compiled")
class TestMicrostructure:
    """Test microstructure kernels."""
    
    def test_midprice(self):
        """Test midprice computation."""
        bid = np.array([99.5, 99.6, 99.7])
        ask = np.array([100.5, 100.6, 100.7])
        batch = pa.record_batch({'bid': bid, 'ask': ask}, names=['bid', 'ask'])
        
        result = midprice(batch)
        
        assert 'midprice' in result.schema.names
        mid = result.column('midprice').to_numpy()
        
        expected_mid = (bid + ask) / 2
        np.testing.assert_array_almost_equal(mid, expected_mid)
    
    def test_microprice(self):
        """Test microprice (size-weighted)."""
        bid = np.array([99.5, 99.6])
        ask = np.array([100.5, 100.6])
        bid_size = np.array([1000.0, 2000.0])
        ask_size = np.array([1500.0, 1000.0])
        
        batch = pa.record_batch({
            'bid': bid,
            'ask': ask,
            'bid_size': bid_size,
            'ask_size': ask_size
        }, names=['bid', 'ask', 'bid_size', 'ask_size'])
        
        result = microprice(batch)
        
        assert 'microprice' in result.schema.names
        μp = result.column('microprice').to_numpy()
        
        # Microprice should be between bid and ask
        assert np.all(μp > bid)
        assert np.all(μp < ask)
    
    def test_l1_imbalance(self):
        """Test L1 imbalance."""
        bid_size = np.array([1000.0, 2000.0, 1000.0])
        ask_size = np.array([1500.0, 1000.0, 1000.0])
        
        batch = pa.record_batch({
            'bid_size': bid_size,
            'ask_size': ask_size
        }, names=['bid_size', 'ask_size'])
        
        result = l1_imbalance(batch)
        
        assert 'l1_imbalance' in result.schema.names
        imbalance = result.column('l1_imbalance').to_numpy()
        
        # Check bounds: [-1, 1]
        assert np.all(imbalance >= -1.0)
        assert np.all(imbalance <= 1.0)
        
        # When bid_size > ask_size, imbalance > 0
        assert imbalance[1] > 0  # 2000 > 1000


@pytest.mark.skipif(not KERNELS_AVAILABLE, reason="Fintech kernels not compiled")
class TestVolatilityLiquidity:
    """Test volatility and liquidity kernels."""
    
    def test_realized_var(self):
        """Test realized variance."""
        returns = np.random.normal(0, 0.01, 100)
        batch = pa.record_batch({'log_return': returns}, names=['log_return'])
        
        result = realized_var(batch, 'log_return', window=20)
        
        assert 'realized_var' in result.schema.names
        rv = result.column('realized_var').to_numpy()
        
        # RV should be positive
        assert np.all(rv[20:] > 0)
    
    def test_bipower_var(self):
        """Test bipower variation (jump-robust)."""
        returns = np.random.normal(0, 0.01, 100)
        batch = pa.record_batch({'log_return': returns}, names=['log_return'])
        
        result = bipower_var(batch, 'log_return', window=20)
        
        assert 'bipower_var' in result.schema.names
        bpv = result.column('bipower_var').to_numpy()
        
        # BPV should be positive
        assert np.all(bpv[20:] >= 0)


@pytest.mark.skipif(not KERNELS_AVAILABLE, reason="Fintech kernels not compiled")
class TestFXCrypto:
    """Test FX and crypto kernels."""
    
    def test_triangular_arbitrage(self):
        """Test triangular arbitrage detection."""
        n = 10
        spot_ab = np.full(n, 1.10)
        spot_bc = np.full(n, 0.90)
        # Introduce small arbitrage
        spot_ac = spot_ab * spot_bc * 1.01  # 1% overpriced
        fees_bps = np.full(n, 2.0)
        
        batch = pa.record_batch({
            'spot_ab': spot_ab,
            'spot_bc': spot_bc,
            'spot_ac': spot_ac,
            'fees_bps': fees_bps
        }, names=['spot_ab', 'spot_bc', 'spot_ac', 'fees_bps'])
        
        result = triangular_arbitrage(batch)
        
        assert 'arb_edge_bps' in result.schema.names
        assert 'arb_route' in result.schema.names
    
    def test_funding_apr(self):
        """Test funding rate annualization."""
        funding_rate = np.array([0.0001, -0.0001, 0.00005])  # 0.01%, -0.01%, 0.005%
        batch = pa.record_batch({'funding_rate': funding_rate}, names=['funding_rate'])
        
        result = funding_apr(batch, funding_freq_hours=8.0)
        
        assert 'funding_apr' in result.schema.names
        apr = result.column('funding_apr').to_numpy()
        
        # APR should be funding_rate × (8760 / 8)
        expected_apr = funding_rate * (8760.0 / 8.0)
        np.testing.assert_array_almost_equal(apr, expected_apr)


@pytest.mark.skipif(not KERNELS_AVAILABLE, reason="Fintech kernels not compiled")
class TestExecutionRisk:
    """Test execution and risk kernels."""
    
    def test_vwap(self):
        """Test VWAP computation."""
        price = np.array([100.0, 101.0, 102.0, 103.0])
        volume = np.array([1000.0, 2000.0, 1500.0, 1000.0])
        batch = pa.record_batch({'price': price, 'volume': volume}, names=['price', 'volume'])
        
        result = vwap(batch, 'price', 'volume')
        
        assert 'vwap' in result.schema.names
        vwap_values = result.column('vwap').to_numpy()
        
        # VWAP should be cumulative
        assert len(vwap_values) == len(price)
    
    def test_cusum(self):
        """Test CUSUM change detection."""
        # Generate data with regime change
        values = np.concatenate([
            np.random.normal(0, 1, 50),
            np.random.normal(2, 1, 50)  # Shift up by 2
        ])
        batch = pa.record_batch({'value': values}, names=['value'])
        
        result = cusum(batch, 'value', k=0.5, h=5.0)
        
        assert 'cusum_stat' in result.schema.names
        assert 'cusum_alarm' in result.schema.names
        
        alarms = result.column('cusum_alarm').to_numpy()
        # Should detect change around index 50
        assert np.any(alarms[40:60] == 1)
    
    def test_historical_var(self):
        """Test historical VaR."""
        # Generate returns with fat tails
        returns = np.concatenate([
            np.random.normal(0, 1, 200),
            [-5.0, -6.0]  # Tail events
        ])
        batch = pa.record_batch({'pnl': returns}, names=['pnl'])
        
        result = historical_var(batch, 'pnl', alpha=0.05, window=100)
        
        assert 'var' in result.schema.names
        var_values = result.column('var').to_numpy()
        
        # VaR should be positive
        assert np.all(var_values[100:] >= 0)


if __name__ == "__main__":
    pytest.main([__file__, '-v'])


# -*- coding: utf-8 -*-
"""
Sabot Fintech Kernels - High-Performance Streaming Financial Analytics

A comprehensive library of streaming Arrow kernels for fintech use cases:
- Online statistics (EWMA, Welford variance, rolling z-scores)
- Microstructure (midprice, microprice, order flow imbalance)
- Volatility (realized variance, bipower variation, jump detection)
- Liquidity (Kyle lambda, Amihud illiquidity, spread estimators)
- Cross-asset (Hayashi-Yoshida covariance for async tick data)
- FX (triangular arbitrage, CIP basis, carry signals)
- Crypto (funding rates, basis annualization, liquidation density)
- Execution (Almgren-Chriss schedules, VWAP, implementation shortfall)
- Risk (CUSUM change detection, historical VaR/CVaR, EVT)

All kernels are:
1. Composable - Work seamlessly with Sabot's Stream API
2. Numerically stable - Online algorithms with Kahan summation
3. Predictable latency - Amortized O(1) per tick where possible

Performance:
- Online stats: ~5-10ns per update (Cython SIMD)
- Hash operations: ~20-30ns per element (XXH3)
- Aggregations: 1M+ updates/sec (vectorized)

Example:
    from sabot.api import Stream
    from sabot.fintech import log_returns, ewma, rolling_zscore
    
    # Compute streaming features
    stream = (Stream.from_kafka('localhost:9092', 'trades', 'analytics-group')
        .map(lambda b: log_returns(b, 'price'))
        .map(lambda b: ewma(b, 'log_return', alpha=0.94))
        .map(lambda b: rolling_zscore(b, 'ewma', window=100)))
"""

# Try to import Cython kernels
try:
    # Online statistics
    from sabot._cython.fintech.online_stats import (
        log_returns,
        welford_mean_var,
        ewma,
        ewcov,
        rolling_zscore,
    )
    
    # Microstructure
    from sabot._cython.fintech.microstructure import (
        midprice,
        microprice,
        quoted_spread,
        effective_spread,
        l1_imbalance,
        ofi,
        vpin,
    )
    
    # Volatility
    from sabot._cython.fintech.volatility import (
        realized_var,
        bipower_var,
        medrv,
        riskmetrics_vol,
    )
    
    # Liquidity
    from sabot._cython.fintech.liquidity import (
        kyle_lambda,
        amihud_illiquidity,
        roll_spread,
        corwin_schultz_spread,
    )
    
    # FX & Crypto
    from sabot._cython.fintech.fx_crypto import (
        # FX
        triangular_arbitrage,
        forward_points,
        cip_basis,
        carry_signal,
        # Crypto
        funding_apr,
        funding_cost,
        basis_annualized,
        open_interest_delta,
        perp_fair_price,
        cash_and_carry_edge,
    )
    
    # Execution & Risk
    from sabot._cython.fintech.execution_risk import (
        # Execution
        vwap,
        implementation_shortfall,
        optimal_ac_schedule,
        arrival_price_impact,
        # Risk
        cusum,
        historical_var,
        historical_cvar,
        cornish_fisher_var,
    )
    
    # Momentum & Filters
    from sabot._cython.fintech.momentum_filters import (
        ema,
        sma,
        macd,
        rsi,
        bollinger_bands,
        kalman_1d,
        donchian_channel,
        stochastic_osc,
    )
    
    # Features & Safety
    from sabot._cython.fintech.features_safety import (
        # Feature engineering
        lag,
        diff,
        rolling_min,
        rolling_max,
        rolling_std,
        rolling_skew,
        rolling_kurt,
        autocorr,
        signed_volume,
        # Market safety
        stale_quote_detector,
        price_band_guard,
        fat_finger_guard,
        circuit_breaker,
        throttle_check,
        outlier_flag_mad,
    )
    
    # Cross-Asset
    from sabot._cython.fintech.cross_asset import (
        hayashi_yoshida_cov,
        hayashi_yoshida_corr,
        xcorr_leadlag,
        pairs_spread,
        kalman_hedge_ratio,
    )
    
    # ASOF Joins
    from sabot._cython.fintech.asof_join import (
        asof_join,
        asof_join_streaming,
        asof_join_table,
        AsofJoinKernel,
        StreamingAsofJoinKernel,
    )
    
    # Distributed Operators
    from sabot._cython.fintech.distributed_kernels import (
        SymbolKeyedOperator,
        StatelessKernelOperator,
        create_ewma_operator,
        create_ofi_operator,
        create_log_returns_operator,
        create_midprice_operator,
        create_vwap_operator,
    )
    
    CYTHON_AVAILABLE = True
    
except ImportError as e:
    import warnings
    warnings.warn(
        f"Cython fintech kernels not available: {e}. "
        "Run 'python build.py' to compile Cython extensions."
    )
    CYTHON_AVAILABLE = False
    
    # Stub implementations
    def log_returns(*args, **kwargs):
        raise NotImplementedError("Cython kernels not compiled")
    
    def welford_mean_var(*args, **kwargs):
        raise NotImplementedError("Cython kernels not compiled")
    
    # ... (other stubs)


__version__ = "0.2.0"
__all__ = [
    # Module status
    "CYTHON_AVAILABLE",
    
    # Online stats
    "log_returns",
    "welford_mean_var",
    "ewma",
    "ewcov",
    "rolling_zscore",
    
    # Microstructure
    "midprice",
    "microprice",
    "quoted_spread",
    "effective_spread",
    "l1_imbalance",
    "ofi",
    "vpin",
    
    # Volatility
    "realized_var",
    "bipower_var",
    "medrv",
    "riskmetrics_vol",
    
    # Liquidity
    "kyle_lambda",
    "amihud_illiquidity",
    "roll_spread",
    "corwin_schultz_spread",
    
    # FX
    "triangular_arbitrage",
    "forward_points",
    "cip_basis",
    "carry_signal",
    
    # Crypto
    "funding_apr",
    "funding_cost",
    "basis_annualized",
    "open_interest_delta",
    "perp_fair_price",
    "cash_and_carry_edge",
    
    # Execution
    "vwap",
    "implementation_shortfall",
    "optimal_ac_schedule",
    "arrival_price_impact",
    
    # Risk
    "cusum",
    "historical_var",
    "historical_cvar",
    "cornish_fisher_var",
    
    # Momentum & Filters
    "ema",
    "sma",
    "macd",
    "rsi",
    "bollinger_bands",
    "kalman_1d",
    "donchian_channel",
    "stochastic_osc",
    
    # Feature engineering
    "lag",
    "diff",
    "rolling_min",
    "rolling_max",
    "rolling_std",
    "rolling_skew",
    "rolling_kurt",
    "autocorr",
    "signed_volume",
    
    # Market safety
    "stale_quote_detector",
    "price_band_guard",
    "fat_finger_guard",
    "circuit_breaker",
    "throttle_check",
    "outlier_flag_mad",
    
    # Cross-asset
    "hayashi_yoshida_cov",
    "hayashi_yoshida_corr",
    "xcorr_leadlag",
    "pairs_spread",
    "kalman_hedge_ratio",
    
    # ASOF joins
    "asof_join",
    "asof_join_streaming",
    "asof_join_table",
    "AsofJoinKernel",
    "StreamingAsofJoinKernel",
    
    # Distributed operators
    "SymbolKeyedOperator",
    "StatelessKernelOperator",
    "create_ewma_operator",
    "create_ofi_operator",
    "create_log_returns_operator",
    "create_midprice_operator",
    "create_vwap_operator",
]

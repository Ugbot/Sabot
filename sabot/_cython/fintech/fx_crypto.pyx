# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
FX & Crypto Kernels - Currency and digital asset primitives.

Triangular arbitrage, funding rates, basis, CIP deviations.
"""

import cython
from libc.math cimport log, exp, fabs
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


# ============================================================================
# FX Kernels
# ============================================================================

def triangular_arbitrage(batch):
    """
    Detect triangular arbitrage opportunities.
    
    For pairs A/B, B/C, A/C, check if:
    spot_AB × spot_BC × spot_CA ≠ 1
    
    Edge (bps) = 10000 × (cross_implied / spot_AC - 1)
    
    Args:
        batch: RecordBatch with 'spot_ab', 'spot_bc', 'spot_ac', 'fees_bps'
    
    Returns:
        RecordBatch with 'arb_edge_bps' and 'arb_route' columns
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    # Extract columns
    spot_ab = batch.column('spot_ab')
    spot_bc = batch.column('spot_bc')
    spot_ac = batch.column('spot_ac')
    fees_bps = batch.column('fees_bps')
    
    # Compute cross-implied rate: AB × BC should equal AC
    cross_implied = pc.multiply(spot_ab, spot_bc)
    
    # Compute arbitrage edge in bps
    # edge = 10000 × (cross / spot - 1) - fees
    ratio = pc.divide(cross_implied, spot_ac)
    edge_gross = pc.multiply(pc.subtract(ratio, 1.0), 10000.0)
    edge_net = pc.subtract(edge_gross, fees_bps)
    
    # Determine route (simplified: just mark if profitable)
    route = pc.if_else(
        pc.greater(edge_net, 0.0),
        pa.array(['AB->BC->CA'] * batch.num_rows),
        pa.array(['no_arb'] * batch.num_rows)
    )
    
    result = batch.append_column('arb_edge_bps', edge_net)
    result = result.append_column('arb_route', route)
    return result


def forward_points(batch, double T_years=1.0):
    """
    Compute forward points (FX forward premium/discount).
    
    F = S × exp((r_dom - r_for) × T)
    Forward points = F - S
    
    Args:
        batch: RecordBatch with 'spot', 'r_dom', 'r_for'
        T_years: Time to maturity in years
    
    Returns:
        RecordBatch with 'forward_points' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    spot = batch.column('spot')
    r_dom = batch.column('r_dom')
    r_for = batch.column('r_for')
    
    # Interest rate differential
    rate_diff = pc.subtract(r_dom, r_for)
    
    # Forward = spot × exp(rate_diff × T)
    exponent = pc.multiply(rate_diff, T_years)
    # Use log-space to avoid numerical issues
    log_forward = pc.add(pc.ln(spot), exponent)
    forward = pc.exp(log_forward)
    
    # Forward points = forward - spot
    fwd_points = pc.subtract(forward, spot)
    
    return batch.append_column('forward_points', fwd_points)


def cip_basis(batch, double T_years=1.0):
    """
    Covered Interest Parity basis (CIP deviation).
    
    CIP: F/S = exp((r_dom - r_for) × T)
    
    Basis = forward_implied_rate - (r_dom - r_for)
    
    Non-zero basis indicates arbitrage or market frictions.
    
    Args:
        batch: RecordBatch with 'spot', 'forward', 'r_dom', 'r_for'
        T_years: Time to maturity
    
    Returns:
        RecordBatch with 'cip_basis' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    spot = batch.column('spot')
    forward = batch.column('forward')
    r_dom = batch.column('r_dom')
    r_for = batch.column('r_for')
    
    # Implied rate from forward: log(F/S) / T
    ratio = pc.divide(forward, spot)
    implied_rate_diff = pc.divide(pc.ln(ratio), T_years)
    
    # Actual rate differential
    actual_rate_diff = pc.subtract(r_dom, r_for)
    
    # CIP basis
    basis = pc.subtract(implied_rate_diff, actual_rate_diff)
    
    return batch.append_column('cip_basis', basis)


def carry_signal(batch, double risk_adj_factor=1.0):
    """
    Currency carry trade signal.
    
    carry = r_dom - r_for - risk_adjustment
    
    Positive carry = borrow low-yield, invest high-yield.
    
    Args:
        batch: RecordBatch with 'r_dom', 'r_for', 'volatility'
        risk_adj_factor: Risk adjustment (e.g., 1.0 × volatility)
    
    Returns:
        RecordBatch with 'carry_signal' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    r_dom = batch.column('r_dom')
    r_for = batch.column('r_for')
    
    # Raw carry
    carry = pc.subtract(r_dom, r_for)
    
    # Risk adjustment (if volatility column exists)
    if 'volatility' in batch.schema.names:
        vol = batch.column('volatility')
        risk_adj = pc.multiply(vol, risk_adj_factor)
        carry_signal = pc.subtract(carry, risk_adj)
    else:
        carry_signal = carry
    
    return batch.append_column('carry_signal', carry_signal)


# ============================================================================
# Crypto Kernels
# ============================================================================

def funding_apr(batch, double funding_freq_hours=8.0):
    """
    Annualize funding rate (perps).
    
    APR = funding_rate × (8760 / funding_freq_hours)
    
    Args:
        batch: RecordBatch with 'funding_rate' (e.g., 0.0001 = 0.01%)
        funding_freq_hours: Funding frequency (8h for most perps)
    
    Returns:
        RecordBatch with 'funding_apr' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    funding_rate = batch.column('funding_rate')
    
    # Annualize: rate × (hours_per_year / funding_freq)
    periods_per_year = 8760.0 / funding_freq_hours
    apr = pc.multiply(funding_rate, periods_per_year)
    
    return batch.append_column('funding_apr', apr)


def funding_cost(batch):
    """
    Cumulative funding cost for a position.
    
    cost = position × price × Σ funding_rates
    
    Args:
        batch: RecordBatch with 'position', 'price', 'funding_rate'
    
    Returns:
        RecordBatch with 'funding_cost' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    position = batch.column('position')
    price = batch.column('price')
    funding_rate = batch.column('funding_rate')
    
    # Per-period cost
    notional = pc.multiply(position, price)
    cost = pc.multiply(notional, funding_rate)
    
    # Cumulative sum (would need stateful kernel for true streaming)
    # For now, just return per-period cost
    return batch.append_column('funding_cost', cost)


def basis_annualized(batch, double T_days=90.0):
    """
    Annualized basis between perp and spot.
    
    Basis (annualized) = (perp / spot - 1) × (365 / T)
    
    Args:
        batch: RecordBatch with 'perp_price', 'spot_price'
        T_days: Time to expiry for futures (if applicable)
    
    Returns:
        RecordBatch with 'basis_apr' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    perp = batch.column('perp_price')
    spot = batch.column('spot_price')
    
    # Basis = (perp / spot - 1)
    ratio = pc.divide(perp, spot)
    basis_raw = pc.subtract(ratio, 1.0)
    
    # Annualize
    basis_apr = pc.multiply(basis_raw, 365.0 / T_days)
    
    return batch.append_column('basis_apr', basis_apr)


def open_interest_delta(batch):
    """
    Change in open interest (perps/futures).
    
    ΔOI_t = OI_t - OI_{t-1}
    
    Stateful kernel tracking last OI.
    
    Args:
        batch: RecordBatch with 'open_interest'
    
    Returns:
        RecordBatch with 'oi_delta' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    oi_np = batch.column('open_interest').to_numpy()
    cdef double[:] oi = oi_np
    cdef int64_t n = len(oi)
    cdef double[:] delta = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i
    
    # Compute deltas
    delta[0] = 0.0  # First value
    with nogil:
        for i in range(1, n):
            delta[i] = oi[i] - oi[i - 1]
    
    delta_array = pa.array(delta, type=pa.float64())
    return batch.append_column('oi_delta', delta_array)


def perp_fair_price(batch):
    """
    Fair price for perpetual swap.
    
    Fair = spot × (1 + expected_funding + basis)
    
    Args:
        batch: RecordBatch with 'spot', 'funding_exp', 'basis'
    
    Returns:
        RecordBatch with 'perp_fair' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    spot = batch.column('spot')
    funding_exp = batch.column('funding_exp')
    basis = batch.column('basis')
    
    # Fair = spot × (1 + funding + basis)
    adjustment = pc.add(funding_exp, basis)
    fair = pc.multiply(spot, pc.add(1.0, adjustment))
    
    return batch.append_column('perp_fair', fair)


def cash_and_carry_edge(batch):
    """
    Cash-and-carry arbitrage edge (perp vs spot).
    
    Edge = (perp - spot) / spot - funding_cost - transaction_costs
    
    Args:
        batch: RecordBatch with 'perp', 'spot', 'funding', 'fees_bps'
    
    Returns:
        RecordBatch with 'carry_edge_bps' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    perp = batch.column('perp')
    spot = batch.column('spot')
    funding = batch.column('funding')
    fees = batch.column('fees_bps')
    
    # Basis in bps
    basis_bps = pc.multiply(
        pc.divide(pc.subtract(perp, spot), spot),
        10000.0
    )
    
    # Net edge after costs
    edge = pc.subtract(
        basis_bps,
        pc.add(pc.multiply(funding, 10000.0), fees)
    )
    
    return batch.append_column('carry_edge_bps', edge)


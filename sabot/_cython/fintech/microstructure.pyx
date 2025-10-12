# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Microstructure Kernels - HFT and market making primitives.

All kernels operate on L1 quote data (bid/ask/sizes) and trade data.
"""

import cython
from libc.math cimport fabs, isnan
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


cdef class MicrostructureKernel:
    """Base class."""
    cpdef object process_batch(self, object batch):
        return batch


cdef class MidpriceKernel(MicrostructureKernel):
    """
    Midprice: (bid + ask) / 2
    
    The baseline reference price for spread and slippage calculations.
    """
    
    cpdef object process_batch(self, object batch):
        """
        Compute midprice from bid/ask.
        
        Args:
            batch: RecordBatch with 'bid' and 'ask' columns
        
        Returns:
            RecordBatch with 'midprice' column
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        # Use Arrow compute for SIMD acceleration
        bid = batch.column('bid')
        ask = batch.column('ask')
        
        # midprice = (bid + ask) / 2
        midprice = pc.divide(pc.add(bid, ask), 2.0)
        
        return batch.append_column('midprice', midprice)


cdef class MicropriceKernel(MicrostructureKernel):
    """
    Microprice: size-weighted midprice.
    
    μp = (bid × ask_size + ask × bid_size) / (bid_size + ask_size)
    
    More accurate than midprice when sizes are imbalanced.
    Stoikov (2018) shows this predicts short-term price moves.
    """
    
    cpdef object process_batch(self, object batch):
        """
        Compute microprice.
        
        Args:
            batch: RecordBatch with 'bid', 'ask', 'bid_size', 'ask_size'
        
        Returns:
            RecordBatch with 'microprice' column
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        bid = batch.column('bid')
        ask = batch.column('ask')
        bid_size = batch.column('bid_size')
        ask_size = batch.column('ask_size')
        
        # μp = (bid × ask_size + ask × bid_size) / (bid_size + ask_size)
        numerator = pc.add(
            pc.multiply(bid, ask_size),
            pc.multiply(ask, bid_size)
        )
        denominator = pc.add(bid_size, ask_size)
        
        # Handle zero denominator
        microprice = pc.divide(numerator, denominator)
        
        return batch.append_column('microprice', microprice)


cdef class L1ImbalanceKernel(MicrostructureKernel):
    """
    L1 Order book imbalance.
    
    I = (bid_size - ask_size) / (bid_size + ask_size)
    
    Range: [-1, 1]
    - I > 0: buy pressure (more bids)
    - I < 0: sell pressure (more asks)
    
    Predictive of short-term returns at 1-10ms horizons.
    """
    
    cpdef object process_batch(self, object batch):
        """
        Compute L1 imbalance.
        
        Args:
            batch: RecordBatch with 'bid_size', 'ask_size'
        
        Returns:
            RecordBatch with 'l1_imbalance' column
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        bid_size = batch.column('bid_size')
        ask_size = batch.column('ask_size')
        
        # I = (bid_size - ask_size) / (bid_size + ask_size)
        numerator = pc.subtract(bid_size, ask_size)
        denominator = pc.add(bid_size, ask_size)
        
        imbalance = pc.divide(numerator, denominator)
        
        return batch.append_column('l1_imbalance', imbalance)


cdef class OFIKernel(MicrostructureKernel):
    """
    Order Flow Imbalance (Cont, Stoikov, Talreja 2010).
    
    OFI_t = Δ(bid_size) × 1{Δbid_px >= 0} - Δ(ask_size) × 1{Δask_px <= 0}
    
    Captures how order flow changes at the bid/ask levels.
    Highly predictive of short-term price moves.
    
    Stateful kernel - tracks last bid/ask prices and sizes.
    """
    
    def __cinit__(self):
        self._last_bid_px = 0.0
        self._last_ask_px = 0.0
        self._last_bid_size = 0.0
        self._last_ask_size = 0.0
        self._has_last = False
    
    cpdef object process_batch(self, object batch):
        """
        Compute OFI for each row.
        
        Args:
            batch: RecordBatch with bid, ask, bid_size, ask_size
        
        Returns:
            RecordBatch with 'ofi' column
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        # Extract columns
        bid_px_np = batch.column('bid').to_numpy()
        ask_px_np = batch.column('ask').to_numpy()
        bid_size_np = batch.column('bid_size').to_numpy()
        ask_size_np = batch.column('ask_size').to_numpy()
        
        cdef double[:] bid_px = bid_px_np
        cdef double[:] ask_px = ask_px_np
        cdef double[:] bid_size = bid_size_np
        cdef double[:] ask_size = ask_size_np
        
        cdef int64_t n = len(bid_px)
        cdef double[:] ofi = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        cdef double delta_bid_size, delta_ask_size
        cdef double delta_bid_px, delta_ask_px
        cdef double contrib_bid, contrib_ask
        
        # Compute OFI (releases GIL!)
        with nogil:
            for i in range(n):
                if self._has_last:
                    # Compute deltas
                    delta_bid_size = bid_size[i] - self._last_bid_size
                    delta_ask_size = ask_size[i] - self._last_ask_size
                    delta_bid_px = bid_px[i] - self._last_bid_px
                    delta_ask_px = ask_px[i] - self._last_ask_px
                    
                    # Compute contributions
                    contrib_bid = delta_bid_size if delta_bid_px >= 0.0 else 0.0
                    contrib_ask = delta_ask_size if delta_ask_px <= 0.0 else 0.0
                    
                    ofi[i] = contrib_bid - contrib_ask
                else:
                    ofi[i] = 0.0
                
                # Update state
                self._last_bid_px = bid_px[i]
                self._last_ask_px = ask_px[i]
                self._last_bid_size = bid_size[i]
                self._last_ask_size = ask_size[i]
                self._has_last = True
        
        # Create Arrow array
        ofi_array = pa.array(ofi, type=pa.float64())
        return batch.append_column('ofi', ofi_array)


cdef class VPINKernel(MicrostructureKernel):
    """
    Volume-synchronized Probability of Informed Trading (VPIN).
    
    Easley, Lopez de Prado, O'Hara (2012).
    
    VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol)
    
    Computed over volume buckets (not time buckets).
    High VPIN indicates informed trading and potential volatility.
    """
    
    def __cinit__(self, double bucket_vol_target=100000.0):
        """
        Args:
            bucket_vol_target: Target volume per bucket (e.g., 100K shares)
        """
        self._buy_vol_sum = 0.0
        self._sell_vol_sum = 0.0
        self._bucket_vol_target = bucket_vol_target
    
    cpdef object process_batch(self, object batch):
        """
        Compute VPIN for trade data.
        
        Args:
            batch: RecordBatch with 'volume' and 'side' columns
                  (side: 1 = buy, -1 = sell)
        
        Returns:
            RecordBatch with 'vpin' column
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        volume_np = batch.column('volume').to_numpy()
        side_np = batch.column('side').to_numpy()
        
        cdef double[:] volume = volume_np
        cdef double[:] side = side_np
        cdef int64_t n = len(volume)
        cdef double[:] vpin = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        cdef double current_vpin
        cdef double total_vol
        
        # Compute VPIN (releases GIL!)
        with nogil:
            for i in range(n):
                # Accumulate buy/sell volume
                if side[i] > 0:
                    self._buy_vol_sum += volume[i]
                else:
                    self._sell_vol_sum += volume[i]
                
                # Compute VPIN
                total_vol = self._buy_vol_sum + self._sell_vol_sum
                if total_vol > 0.0:
                    current_vpin = fabs(self._buy_vol_sum - self._sell_vol_sum) / total_vol
                else:
                    current_vpin = 0.0
                
                vpin[i] = current_vpin
                
                # Reset bucket if target reached
                if total_vol >= self._bucket_vol_target:
                    self._buy_vol_sum = 0.0
                    self._sell_vol_sum = 0.0
        
        vpin_array = pa.array(vpin, type=pa.float64())
        return batch.append_column('vpin', vpin_array)


# ============================================================================
# Python API Functions
# ============================================================================

def midprice(batch):
    """Compute midprice: (bid + ask) / 2"""
    kernel = MidpriceKernel()
    return kernel.process_batch(batch)


def microprice(batch):
    """Compute microprice: size-weighted midprice"""
    kernel = MicropriceKernel()
    return kernel.process_batch(batch)


def l1_imbalance(batch):
    """Compute L1 order book imbalance"""
    kernel = L1ImbalanceKernel()
    return kernel.process_batch(batch)


def ofi(batch):
    """Compute order flow imbalance"""
    kernel = OFIKernel()
    return kernel.process_batch(batch)


def vpin(batch, double bucket_vol=100000.0):
    """Compute VPIN (volume-synchronized informed trading probability)"""
    kernel = VPINKernel(bucket_vol_target=bucket_vol)
    return kernel.process_batch(batch)


def quoted_spread(batch):
    """
    Quoted spread: ask - bid (in price units).
    
    Args:
        batch: RecordBatch with 'bid' and 'ask'
    
    Returns:
        RecordBatch with 'quoted_spread' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    bid = batch.column('bid')
    ask = batch.column('ask')
    spread = pc.subtract(ask, bid)
    return batch.append_column('quoted_spread', spread)


def effective_spread(batch):
    """
    Effective spread: 2 × |exec_price - midprice|.
    
    Measures actual transaction cost relative to mid.
    
    Args:
        batch: RecordBatch with 'exec_price' and 'midprice'
    
    Returns:
        RecordBatch with 'effective_spread' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    exec_price = batch.column('exec_price')
    mid = batch.column('midprice')
    
    # 2 × |exec_price - midprice|
    spread = pc.multiply(
        pc.abs(pc.subtract(exec_price, mid)),
        2.0
    )
    return batch.append_column('effective_spread', spread)


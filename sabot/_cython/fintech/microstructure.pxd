# cython: language_level=3
"""
Microstructure kernels - Market making and HFT primitives.

Price formation, order flow, and liquidity analysis.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool


cdef class MicrostructureKernel:
    """Base class for microstructure kernels."""
    cpdef object process_batch(self, object batch)


cdef class MidpriceKernel(MicrostructureKernel):
    """Midprice: (bid + ask) / 2"""
    cpdef object process_batch(self, object batch)


cdef class MicropriceKernel(MicrostructureKernel):
    """
    Microprice: weighted midprice using sizes.
    μp = (bid × ask_size + ask × bid_size) / (bid_size + ask_size)
    """
    cpdef object process_batch(self, object batch)


cdef class L1ImbalanceKernel(MicrostructureKernel):
    """
    Level-1 order book imbalance.
    I = (bid_size - ask_size) / (bid_size + ask_size)
    """
    cpdef object process_batch(self, object batch)


cdef class OFIKernel(MicrostructureKernel):
    """
    Order flow imbalance (Cont et al.).
    OFI = Δ(bid_size) × 1{Δbid_px >= 0} - Δ(ask_size) × 1{Δask_px <= 0}
    """
    cdef double _last_bid_px
    cdef double _last_ask_px
    cdef double _last_bid_size
    cdef double _last_ask_size
    cdef cbool _has_last
    cpdef object process_batch(self, object batch)


cdef class VPINKernel(MicrostructureKernel):
    """
    Volume-synchronized probability of informed trading (Easley et al.).
    """
    cdef double _buy_vol_sum
    cdef double _sell_vol_sum
    cdef double _bucket_vol_target
    cpdef object process_batch(self, object batch)


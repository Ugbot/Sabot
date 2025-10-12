"""
Stream API Extensions for Fintech Kernels.

Adds convenience methods to Stream class for fintech operations
with automatic distributed execution support.
"""

from typing import Optional


class FintechStreamMixin:
    """
    Mixin class to add fintech methods to Stream.
    
    Usage:
        # These methods can be added to Stream class:
        Stream.ewma = FintechStreamMixin.ewma
        Stream.ofi = FintechStreamMixin.ofi
        # etc.
    """
    
    def ewma(self, alpha: float = 0.94, symbol_column: str = 'symbol',
             distributed: bool = False):
        """
        Apply EWMA kernel to stream.
        
        Args:
            alpha: Smoothing parameter
            symbol_column: Column to partition by (for distributed)
            distributed: Use distributed operator (vs simple function)
        
        Returns:
            Stream with EWMA computed
        
        Examples:
            # Simple (single node, automatic morsels)
            stream.ewma(alpha=0.94)
            
            # Distributed (multi-node, network shuffle)
            stream.ewma(alpha=0.94, distributed=True)
        """
        if distributed:
            try:
                from sabot._cython.fintech.distributed_kernels import create_ewma_operator
                operator = create_ewma_operator(
                    source=self._source,
                    alpha=alpha,
                    symbol_column=symbol_column
                )
                operator = self._wrap_with_morsel_parallelism(operator)
                from sabot.api.stream import Stream
                return Stream(operator, None)
            except ImportError:
                # Fall back to simple mode
                distributed = False
        
        if not distributed:
            # Simple function mode (automatic local morsels)
            from sabot.fintech import ewma as ewma_func
            return self.map(lambda b: ewma_func(b, alpha=alpha))
    
    def ofi(self, symbol_column: str = 'symbol', distributed: bool = False):
        """
        Apply OFI (Order Flow Imbalance) kernel.
        
        Args:
            symbol_column: Column to partition by
            distributed: Use distributed operator
        
        Returns:
            Stream with OFI computed
        """
        if distributed:
            try:
                from sabot._cython.fintech.distributed_kernels import create_ofi_operator
                operator = create_ofi_operator(
                    source=self._source,
                    symbol_column=symbol_column
                )
                operator = self._wrap_with_morsel_parallelism(operator)
                from sabot.api.stream import Stream
                return Stream(operator, None)
            except ImportError:
                distributed = False
        
        if not distributed:
            from sabot.fintech import ofi as ofi_func
            return self.map(lambda b: ofi_func(b))
    
    def log_returns(self, price_column: str = 'price', symbol_column: str = 'symbol',
                    distributed: bool = False):
        """Apply log returns kernel."""
        if distributed:
            try:
                from sabot._cython.fintech.distributed_kernels import create_log_returns_operator
                operator = create_log_returns_operator(
                    source=self._source,
                    symbol_column=symbol_column
                )
                operator = self._wrap_with_morsel_parallelism(operator)
                from sabot.api.stream import Stream
                return Stream(operator, None)
            except ImportError:
                distributed = False
        
        if not distributed:
            from sabot.fintech import log_returns as lr_func
            return self.map(lambda b: lr_func(b, price_column=price_column))
    
    def midprice_op(self):
        """Apply midprice kernel (stateless - always uses local morsels)."""
        from sabot.fintech import midprice
        return self.map(lambda b: midprice(b))
    
    def vwap_op(self, price_column: str = 'price', volume_column: str = 'volume'):
        """Apply VWAP kernel."""
        from sabot.fintech import vwap
        return self.map(lambda b: vwap(b, price_column, volume_column))


def install_fintech_extensions():
    """
    Install fintech extensions to Stream class.
    
    Call this to add .ewma(), .ofi(), etc. methods to Stream.
    
    Example:
        from sabot.fintech.stream_extensions import install_fintech_extensions
        install_fintech_extensions()
        
        # Now can use
        stream.ewma(alpha=0.94)
        stream.ofi()
    """
    try:
        from sabot.api.stream import Stream
        
        # Add methods
        Stream.ewma = FintechStreamMixin.ewma
        Stream.ofi = FintechStreamMixin.ofi
        Stream.log_returns = FintechStreamMixin.log_returns
        Stream.midprice_op = FintechStreamMixin.midprice_op
        Stream.vwap_op = FintechStreamMixin.vwap_op
        
        return True
    except ImportError:
        return False


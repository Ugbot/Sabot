# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Fintech Kernel Operators - Distributed execution wrappers.

Makes fintech kernels work with Sabot's morsel parallelism and network shuffle.

Key design:
- Fintech kernels are STATEFUL (maintain running state)
- But state is KEYED BY SYMBOL (each symbol independent)
- Can be distributed by partitioning on symbol column
- Network shuffle ensures each symbol always goes to same node
"""

import cython
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string as cpp_string
from cython.operator cimport dereference as deref
import numpy as np

from sabot import cyarrow as pa
from sabot._cython.operators.shuffled_operator cimport ShuffledOperator


cdef class FintechKernelOperator(ShuffledOperator):
    """
    Base operator wrapper for fintech kernels with symbol-based partitioning.
    
    Architecture:
    - Maintains one kernel instance per symbol
    - Partitions by symbol for distributed execution
    - Each node handles a subset of symbols
    - Network shuffle ensures symbol affinity
    
    Example distributed execution:
        Node 1: handles AAPL, GOOGL (with their EWMA/OFI states)
        Node 2: handles MSFT, AMZN (with their EWMA/OFI states)
        Node 3: handles NVDA, AMD (with their EWMA/OFI states)
    """
    
    def __init__(
        self,
        source,
        kernel_func,
        str symbol_column='symbol',
        bint symbol_keyed=True,
        **kernel_kwargs
    ):
        """
        Initialize fintech kernel operator.
        
        Args:
            source: Input stream
            kernel_func: Kernel function or class to apply
            symbol_column: Column name for symbol (default: 'symbol')
            symbol_keyed: Whether to partition by symbol (default: True)
            **kernel_kwargs: Arguments to pass to kernel
        """
        # Initialize as shuffled operator if symbol-keyed
        if symbol_keyed:
            super().__init__(
                source=source,
                partition_keys=[symbol_column],  # Partition by symbol for shuffle
                num_partitions=4,  # Will be set by JobManager
                schema=None
            )
        else:
            # Not symbol-keyed - stateless, can use local morsels
            super().__init__(
                source=source,
                partition_keys=None,
                num_partitions=1,
                schema=None
            )
        
        self._kernel_func = kernel_func
        self._symbol_column = symbol_column
        self._symbol_keyed = symbol_keyed
        
        # Mark as stateful if symbol-keyed
        self._stateful = symbol_keyed
        self._key_columns = [symbol_column] if symbol_keyed else None
        
        # Store kernel kwargs for creating instances
        self._kernel_kwargs = kernel_kwargs
    
    cpdef object process_batch(self, object batch):
        """
        Process batch with per-symbol kernel instances.
        
        If symbol_keyed:
        - Splits batch by symbol
        - Applies kernel to each symbol group
        - Maintains separate state per symbol
        - Reassembles results
        
        If not symbol_keyed:
        - Applies kernel to entire batch
        - Single global state
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        if not self._symbol_keyed:
            # Single global kernel instance
            if not hasattr(self, '_global_kernel'):
                self._global_kernel = self._create_kernel()
            return self._global_kernel.process_batch(batch)
        
        # Symbol-keyed processing
        if self._symbol_column not in batch.schema.names:
            raise ValueError(f"Symbol column '{self._symbol_column}' not found in batch")
        
        # Group by symbol
        import pyarrow.compute as pc
        
        # Get unique symbols
        symbols_array = batch.column(self._symbol_column)
        symbols_np = symbols_array.to_numpy()
        unique_symbols = np.unique(symbols_np)
        
        # Process each symbol group
        result_batches = []
        
        for symbol in unique_symbols:
            # Filter batch for this symbol
            mask = pc.equal(symbols_array, symbol)
            symbol_batch = batch.filter(mask)
            
            if symbol_batch.num_rows > 0:
                # Get or create kernel for this symbol
                kernel = self._get_or_create_kernel(str(symbol))
                
                # Process this symbol's data
                result = kernel.process_batch(symbol_batch)
                if result and result.num_rows > 0:
                    result_batches.append(result)
        
        # Concatenate results
        if not result_batches:
            return None
        
        if len(result_batches) == 1:
            return result_batches[0]
        
        # Combine batches
        table = pa.Table.from_batches(result_batches)
        combined = table.combine_chunks()
        return combined.to_batches()[0]
    
    cdef object _get_or_create_kernel(self, str symbol):
        """Get or create kernel instance for symbol."""
        cdef cpp_string symbol_key = symbol.encode('utf-8')
        cdef unordered_map[cpp_string, object].iterator it
        
        it = self._symbol_states.find(symbol_key)
        
        if it != self._symbol_states.end():
            return deref(it).second
        else:
            # Create new kernel for this symbol
            kernel = self._create_kernel()
            self._symbol_states[symbol_key] = kernel
            return kernel
    
    cdef object _create_kernel(self):
        """Create a new kernel instance."""
        # If kernel_func is a class, instantiate it
        if isinstance(self._kernel_func, type):
            return self._kernel_func(**self._kernel_kwargs)
        else:
            # It's a function - return as-is
            return self._kernel_func


cdef class OnlineStatsOperator(FintechKernelOperator):
    """
    Operator wrapper for online statistics kernels.
    
    Automatically partitions by symbol for distributed execution.
    """
    
    cpdef object process_batch(self, object batch):
        """Delegate to parent with symbol-keyed processing."""
        return FintechKernelOperator.process_batch(self, batch)


cdef class MicrostructureOperator(FintechKernelOperator):
    """
    Operator wrapper for microstructure kernels.
    
    Microstructure kernels (OFI, VPIN) are stateful and need symbol partitioning.
    """
    
    cpdef object process_batch(self, object batch):
        """Delegate to parent with symbol-keyed processing."""
        return FintechKernelOperator.process_batch(self, batch)


cdef class VolatilityOperator(FintechKernelOperator):
    """
    Operator wrapper for volatility kernels.
    
    Volatility kernels maintain rolling windows per symbol.
    """
    
    cpdef object process_batch(self, object batch):
        """Delegate to parent with symbol-keyed processing."""
        return FintechKernelOperator.process_batch(self, batch)


# ============================================================================
# Python API - Factory Functions
# ============================================================================

def create_fintech_operator(kernel_func, source, str symbol_column='symbol',
                            bint symbol_keyed=True, **kernel_kwargs):
    """
    Create a fintech kernel operator with proper shuffle support.
    
    Args:
        kernel_func: Kernel function or class
        source: Input stream
        symbol_column: Column to partition by (default: 'symbol')
        symbol_keyed: Whether to partition by symbol (default: True)
        **kernel_kwargs: Arguments for kernel
    
    Returns:
        FintechKernelOperator ready for distributed execution
    
    Example:
        from sabot._cython.fintech.operators import create_fintech_operator
        from sabot._cython.fintech.online_stats import EWMAKernel
        
        # Create operator
        ewma_op = create_fintech_operator(
            EWMAKernel,
            source=stream,
            symbol_column='symbol',
            alpha=0.94
        )
        
        # Automatically partitions by symbol across nodes
        for batch in ewma_op:
            process(batch)
    """
    return FintechKernelOperator(
        source,
        kernel_func,
        symbol_column=symbol_column,
        symbol_keyed=symbol_keyed,
        **kernel_kwargs
    )


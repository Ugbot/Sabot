# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Distributed Fintech Kernel Operators.

Makes fintech kernels work seamlessly with Sabot's execution infrastructure:
- Automatic local morsels for large batches
- Network shuffle for distributed execution
- Symbol-based partitioning
- Proper operator chaining
"""

import cython
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string as cpp_string
from cython.operator cimport dereference as deref
import numpy as np

from sabot import cyarrow as pa
from sabot._cython.operators.base_operator cimport BaseOperator


cdef class SymbolKeyedOperator(BaseOperator):
    """
    Symbol-keyed operator for fintech kernels.
    
    Maintains separate kernel instances per symbol.
    Automatically enables network shuffle when deployed to cluster.
    
    Architecture:
    - Single node: All symbols in local memory, automatic morsels
    - Multi-node: Symbols partitioned across nodes, network shuffle
    """
    
    def __init__(
        self,
        source,
        kernel_class,
        str symbol_column='symbol',
        bint auto_partition=True,
        schema=None,
        **kernel_kwargs
    ):
        """
        Initialize symbol-keyed operator.
        
        Args:
            source: Upstream operator or iterable
            kernel_class: Kernel class to instantiate (e.g., EWMAKernel)
            symbol_column: Column name for symbols (default: 'symbol')
            auto_partition: Enable automatic symbol partitioning (default: True)
            schema: Output schema (optional)
            **kernel_kwargs: Arguments passed to kernel constructor
        """
        self._source = source
        self._schema = schema
        self._kernel_class = kernel_class
        self._kernel_kwargs = kernel_kwargs
        self._symbol_column = symbol_column
        self._auto_partition = auto_partition
        
        # Configure as stateful operator if auto_partition enabled
        if auto_partition:
            self._stateful = True
            self._key_columns = [symbol_column]
            self._parallelism_hint = 4  # Default parallelism
        else:
            self._stateful = False
            self._key_columns = None
            self._parallelism_hint = 1
    
    cpdef object process_batch(self, object batch):
        """
        Process batch with per-symbol kernels.
        
        Automatically handles:
        - Symbol grouping
        - Per-symbol kernel instantiation
        - State maintenance across batches
        - Result reassembly
        """
        if batch is None or batch.num_rows == 0:
            return batch
        
        # If no symbol column or auto_partition disabled, use single global kernel
        if not self._auto_partition or self._symbol_column not in batch.schema.names:
            # Single global kernel instance
            if not hasattr(self, '_global_kernel'):
                self._global_kernel = self._kernel_class(**self._kernel_kwargs)
            return self._global_kernel.process_batch(batch)
        
        # Symbol-keyed processing
        import pyarrow.compute as pc
        
        # Get symbols
        symbols_array = batch.column(self._symbol_column)
        
        # Check if we can use Arrow group_by (fastest)
        try:
            # Group by symbol and process each group
            table = pa.Table.from_batches([batch])
            grouped = table.group_by(self._symbol_column)
            
            result_batches = []
            
            for group_key, group_table in grouped:
                symbol = str(group_key[0].as_py()) if isinstance(group_key, tuple) else str(group_key.as_py())
                group_batch = group_table.to_batches()[0] if group_table.num_rows > 0 else None
                
                if group_batch and group_batch.num_rows > 0:
                    result = self._process_symbol_group(group_batch, symbol)
                    if result and result.num_rows > 0:
                        result_batches.append(result)
            
            # Concatenate results
            if not result_batches:
                return None
            
            if len(result_batches) == 1:
                return result_batches[0]
            
            combined_table = pa.Table.from_batches(result_batches)
            return combined_table.to_batches()[0]
            
        except:
            # Fallback: manual grouping
            symbols_np = symbols_array.to_numpy()
            unique_symbols = np.unique(symbols_np)
            
            result_batches = []
            
            for symbol in unique_symbols:
                # Filter for this symbol
                mask = pc.equal(symbols_array, symbol)
                symbol_batch = batch.filter(mask)
                
                if symbol_batch.num_rows > 0:
                    result = self._process_symbol_group(symbol_batch, str(symbol))
                    if result and result.num_rows > 0:
                        result_batches.append(result)
            
            # Concatenate
            if not result_batches:
                return None
            
            if len(result_batches) == 1:
                return result_batches[0]
            
            combined_table = pa.Table.from_batches(result_batches)
            return combined_table.to_batches()[0]
    
    cdef object _get_or_create_symbol_kernel(self, str symbol):
        """Get or create kernel instance for symbol."""
        cdef cpp_string symbol_key = symbol.encode('utf-8')
        cdef unordered_map[cpp_string, object].iterator it
        
        it = self._symbol_kernels.find(symbol_key)
        
        if it != self._symbol_kernels.end():
            return deref(it).second
        else:
            # Create new kernel for this symbol
            kernel = self._kernel_class(**self._kernel_kwargs)
            self._symbol_kernels[symbol_key] = kernel
            return kernel
    
    cdef object _process_symbol_group(self, object symbol_batch, str symbol):
        """Process a batch for a single symbol."""
        kernel = self._get_or_create_symbol_kernel(symbol)
        return kernel.process_batch(symbol_batch)


cdef class StatelessKernelOperator(BaseOperator):
    """
    Operator for stateless fintech kernels.
    
    No state maintenance needed - can use local morsels freely.
    """
    
    def __init__(self, source, kernel_func, schema=None, **kernel_kwargs):
        """
        Initialize stateless kernel operator.
        
        Args:
            source: Upstream operator
            kernel_func: Kernel function or class
            schema: Output schema
            **kernel_kwargs: Arguments for kernel
        """
        self._source = source
        self._schema = schema
        self._kernel_func = kernel_func
        self._kernel_kwargs = kernel_kwargs
        
        # Stateless - no shuffle needed
        self._stateful = False
        self._key_columns = None
        self._parallelism_hint = 1
        
        # Create kernel instance if it's a class
        if isinstance(kernel_func, type):
            self._kernel_instance = kernel_func(**kernel_kwargs)
        else:
            self._kernel_instance = None
    
    cpdef object process_batch(self, object batch):
        """Process batch with kernel."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        if self._kernel_instance:
            return self._kernel_instance.process_batch(batch)
        else:
            # Function-based kernel
            return self._kernel_func(batch, **self._kernel_kwargs)


# ============================================================================
# Factory Functions - Easy API
# ============================================================================

def create_ewma_operator(source, double alpha=0.94, str symbol_column='symbol'):
    """
    Create EWMA operator with symbol partitioning.
    
    Automatically distributed across nodes in cluster deployment.
    """
    from sabot._cython.fintech.online_stats import EWMAKernel
    
    return SymbolKeyedOperator(
        source=source,
        kernel_class=EWMAKernel,
        symbol_column=symbol_column,
        auto_partition=True,
        alpha=alpha
    )


def create_ofi_operator(source, str symbol_column='symbol'):
    """Create OFI operator with symbol partitioning."""
    from sabot._cython.fintech.microstructure import OFIKernel
    
    return SymbolKeyedOperator(
        source=source,
        kernel_class=OFIKernel,
        symbol_column=symbol_column,
        auto_partition=True
    )


def create_log_returns_operator(source, str symbol_column='symbol'):
    """Create log returns operator with symbol partitioning."""
    from sabot._cython.fintech.online_stats import LogReturnsKernel
    
    return SymbolKeyedOperator(
        source=source,
        kernel_class=LogReturnsKernel,
        symbol_column=symbol_column,
        auto_partition=True
    )


def create_midprice_operator(source):
    """Create midprice operator (stateless - uses local morsels)."""
    from sabot._cython.fintech.microstructure import MidpriceKernel
    
    return StatelessKernelOperator(
        source=source,
        kernel_func=MidpriceKernel
    )


def create_vwap_operator(source, str price_col='price', str volume_col='volume'):
    """Create VWAP operator."""
    from sabot._cython.fintech.execution_risk import vwap
    
    return StatelessKernelOperator(
        source=source,
        kernel_func=vwap,
        price_column=price_col,
        volume_column=volume_col
    )


# ============================================================================
# Stream API Integration
# ============================================================================

def integrate_with_stream():
    """
    Helper to show how to integrate operators with Stream API.
    
    Returns dict of methods to add to Stream class.
    """
    return {
        'ewma': lambda self, alpha=0.94, symbol_column='symbol': self._create_symbol_keyed_op(
            create_ewma_operator, alpha=alpha, symbol_column=symbol_column
        ),
        'ofi': lambda self, symbol_column='symbol': self._create_symbol_keyed_op(
            create_ofi_operator, symbol_column=symbol_column
        ),
        'log_returns': lambda self, symbol_column='symbol': self._create_symbol_keyed_op(
            create_log_returns_operator, symbol_column=symbol_column
        ),
    }


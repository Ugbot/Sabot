"""
Sabot Arrow Flight Operator Pipeline

Morsel-driven operator chain for financial data enrichment.
Each operator communicates via Arrow Flight IPC.
"""

from .csv_source import CSVSourceOperator

__all__ = ['CSVSourceOperator']

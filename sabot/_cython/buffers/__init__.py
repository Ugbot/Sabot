# -*- coding: utf-8 -*-
"""Spillable buffer implementations for bigger-than-memory streaming."""

try:
    from .spillable_buffer import SpillableBuffer
    SPILLABLE_BUFFER_AVAILABLE = True
except ImportError:
    SPILLABLE_BUFFER_AVAILABLE = False
    SpillableBuffer = None

__all__ = ['SpillableBuffer', 'SPILLABLE_BUFFER_AVAILABLE']

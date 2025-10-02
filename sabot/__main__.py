#!/usr/bin/env python3
"""
Sabot CLI - Command Line Interface for Sabot Streaming Engine

This module allows Sabot to be run as:
    python -m sabot

Or installed and run as:
    sabot
    sabot-worker
    sabot-agents
    etc.
"""

from .cli import main

if __name__ == "__main__":
    main()

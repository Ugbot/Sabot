# -*- coding: utf-8 -*-
"""
Database initialization for Sabot control plane.

Supports both SQLite (local mode) and PostgreSQL (distributed mode).
"""

import os
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def init_local_database(db_path: str = "sabot_local.db"):
    """
    Initialize SQLite database for local mode.

    Args:
        db_path: Path to SQLite database file
    """
    logger.info(f"Initializing local SQLite database: {db_path}")

    # Create directory if it doesn't exist
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    # Connect to database (creates file if it doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Read and execute schema
    schema_path = Path(__file__).parent / "dbos_schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    # Split into individual statements and execute
    statements = schema_sql.split(';')
    for statement in statements:
        statement = statement.strip()
        if statement:  # Skip empty statements
            try:
                cursor.execute(statement)
            except sqlite3.Error as e:
                logger.warning(f"Skipping statement (may be PostgreSQL-specific): {e}")
                continue

    conn.commit()
    conn.close()

    logger.info(f"Local database initialized successfully: {db_path}")


def init_database(db_url: str = None):
    """
    Initialize database based on URL.

    Args:
        db_url: Database URL. If None, uses local SQLite.
                 If starts with 'postgresql://', uses PostgreSQL.
                 Otherwise assumes SQLite file path.
    """
    if db_url is None or db_url.startswith('sqlite:///'):
        # Local mode - SQLite
        db_path = db_url.replace('sqlite:///', '') if db_url else 'sabot_local.db'
        init_local_database(db_path)
    elif db_url.startswith('postgresql://'):
        # Distributed mode - PostgreSQL
        logger.info("PostgreSQL database initialization not implemented yet")
        logger.info("Please run the schema manually or use a migration tool")
        # TODO: Implement PostgreSQL initialization
    else:
        raise ValueError(f"Unsupported database URL: {db_url}")


if __name__ == "__main__":
    # Initialize local database when run directly
    init_local_database()

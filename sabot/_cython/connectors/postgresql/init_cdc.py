"""
PostgreSQL CDC Initialization.

This module contains the SQL initialization script for PostgreSQL CDC setup.
It can be executed programmatically or saved to a file for manual execution.
"""

INIT_CDC_SQL = """-- PostgreSQL CDC Initialization Script
--
-- This script sets up logical replication for Change Data Capture.
-- It creates the wal2json extension, publication, and replication slot.

-- Enable wal2json extension (if not already enabled)
CREATE EXTENSION IF NOT EXISTS wal2json;

-- Create publication for all tables
-- This tells PostgreSQL which tables to replicate
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'sabot_cdc') THEN
        CREATE PUBLICATION sabot_cdc FOR ALL TABLES;
        RAISE NOTICE 'Created publication: sabot_cdc';
    ELSE
        RAISE NOTICE 'Publication sabot_cdc already exists';
    END IF;
END $$;

-- Create replication slot using wal2json
-- This is the "cursor" that tracks the replication position
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sabot_cdc_slot') THEN
        PERFORM pg_create_logical_replication_slot('sabot_cdc_slot', 'wal2json');
        RAISE NOTICE 'Created replication slot: sabot_cdc_slot';
    ELSE
        RAISE NOTICE 'Replication slot sabot_cdc_slot already exists';
    END IF;
END $$;

-- Create replication slot for orders example
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sabot_orders_slot') THEN
        PERFORM pg_create_logical_replication_slot('sabot_orders_slot', 'wal2json');
        RAISE NOTICE 'Created replication slot: sabot_orders_slot';
    ELSE
        RAISE NOTICE 'Replication slot sabot_orders_slot already exists';
    END IF;
END $$;

-- Show configuration
SELECT 'wal_level' as setting, setting as value FROM pg_settings WHERE name = 'wal_level'
UNION ALL
SELECT 'max_replication_slots', setting FROM pg_settings WHERE name = 'max_replication_slots'
UNION ALL
SELECT 'max_wal_senders', setting FROM pg_settings WHERE name = 'max_wal_senders';

-- Show created objects
SELECT * FROM pg_publication;
SELECT slot_name, plugin, slot_type, active, restart_lsn FROM pg_replication_slots;
"""


def execute_init_sql(connection):
    """
    Execute the CDC initialization SQL on a PostgreSQL connection.

    Args:
        connection: A psycopg2 or psycopg connection object

    Example:
        import psycopg2
        from sabot._cython.connectors.postgresql.init_cdc import execute_init_sql

        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            user="sabot",
            password="sabot",
            database="sabot"
        )
        execute_init_sql(conn)
        conn.close()
    """
    cursor = connection.cursor()
    try:
        cursor.execute(INIT_CDC_SQL)
        connection.commit()
        print("✅ PostgreSQL CDC initialized successfully")
    except Exception as e:
        connection.rollback()
        print(f"❌ Failed to initialize CDC: {e}")
        raise
    finally:
        cursor.close()


def save_init_sql_to_file(filepath):
    """
    Save the init SQL to a file for manual execution.

    Args:
        filepath: Path where to save the SQL file

    Example:
        from sabot._cython.connectors.postgresql.init_cdc import save_init_sql_to_file
        save_init_sql_to_file("/tmp/01_init_cdc.sql")
    """
    with open(filepath, 'w') as f:
        f.write(INIT_CDC_SQL)
    print(f"✅ Init SQL saved to: {filepath}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Save to file if path provided
        save_init_sql_to_file(sys.argv[1])
    else:
        # Print SQL to stdout
        print(INIT_CDC_SQL)

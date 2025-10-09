-- PostgreSQL CDC Initialization Script
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

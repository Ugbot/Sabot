#!/usr/bin/env python3
"""
Wal2json Parser for Sabot PostgreSQL CDC Connector.

Parses JSON output from wal2json logical decoding plugin into structured CDC events.
Supports both format version 1 (transaction-based) and version 2 (tuple-based).

Based on wal2json documentation and examples.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================================
# CDC Event Types
# ============================================================================

@dataclass
class CDCColumn:
    """Column information in a CDC event."""

    name: str
    type: str
    value: Any
    type_oid: Optional[int] = None
    typmod: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCColumn':
        """Create from wal2json column dictionary."""
        return cls(
            name=data.get('name', ''),
            type=data.get('type', ''),
            value=data.get('value'),
            type_oid=data.get('typeoid'),
            typmod=data.get('typmod')
        )


@dataclass
class CDCIdentity:
    """Identity (key) information for UPDATE/DELETE events."""

    names: List[str]
    types: List[str]
    values: List[Any]
    type_oids: Optional[List[int]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCIdentity':
        """Create from wal2json identity dictionary."""
        return cls(
            names=data.get('keynames', []),
            types=data.get('keytypes', []),
            values=data.get('keyvalues', []),
            type_oids=data.get('keytypeoids')
        )


@dataclass
class CDCEvent:
    """Change Data Capture event."""

    # Event metadata
    action: str  # 'I' (insert), 'U' (update), 'D' (delete), 'T' (truncate), 'M' (message)
    schema: str
    table: str
    lsn: Optional[str] = None
    timestamp: Optional[datetime] = None
    xid: Optional[int] = None
    origin: Optional[str] = None

    # Action-specific data
    columns: Optional[List[CDCColumn]] = None  # INSERT/UPDATE
    old_columns: Optional[List[CDCColumn]] = None  # UPDATE (old values)
    identity: Optional[CDCIdentity] = None  # UPDATE/DELETE (key values)
    truncate_cascade: Optional[bool] = None  # TRUNCATE
    truncate_restart_identity: Optional[bool] = None  # TRUNCATE

    # Message-specific data
    transactional: Optional[bool] = None  # MESSAGE
    prefix: Optional[str] = None  # MESSAGE
    content: Optional[str] = None  # MESSAGE

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCEvent':
        """Create CDC event from wal2json dictionary."""
        # Parse timestamp if present
        timestamp = None
        if 'timestamp' in data:
            try:
                # wal2json timestamp format: "2019-12-29 04:58:34.806671+00"
                timestamp_str = data['timestamp']
                if timestamp_str and timestamp_str != 'null':
                    timestamp = datetime.fromisoformat(timestamp_str.replace(' ', 'T'))
            except (ValueError, TypeError):
                logger.warning(f"Failed to parse timestamp: {data.get('timestamp')}")

        # Create event based on action type
        action = data.get('action', data.get('kind', ''))

        if action == 'I' or data.get('kind') == 'insert':
            # INSERT event
            return cls(
                action='I',
                schema=data.get('schema', ''),
                table=data.get('table', ''),
                lsn=data.get('lsn'),
                timestamp=timestamp,
                xid=data.get('xid'),
                origin=data.get('origin'),
                columns=[CDCColumn.from_dict(col) for col in data.get('columns', [])]
            )

        elif action == 'U' or data.get('kind') == 'update':
            # UPDATE event
            event = cls(
                action='U',
                schema=data.get('schema', ''),
                table=data.get('table', ''),
                lsn=data.get('lsn'),
                timestamp=timestamp,
                xid=data.get('xid'),
                origin=data.get('origin'),
                columns=[CDCColumn.from_dict(col) for col in data.get('columns', [])]
            )

            # Add old values if present (for UPDATE)
            if 'oldkeys' in data:
                event.identity = CDCIdentity.from_dict(data['oldkeys'])
            elif 'identity' in data:
                event.identity = CDCIdentity.from_dict(data['identity'])

            return event

        elif action == 'D' or data.get('kind') == 'delete':
            # DELETE event
            event = cls(
                action='D',
                schema=data.get('schema', ''),
                table=data.get('table', ''),
                lsn=data.get('lsn'),
                timestamp=timestamp,
                xid=data.get('xid'),
                origin=data.get('origin')
            )

            # Add identity (key values)
            if 'oldkeys' in data:
                event.identity = CDCIdentity.from_dict(data['oldkeys'])
            elif 'identity' in data:
                event.identity = CDCIdentity.from_dict(data['identity'])

            return event

        elif action == 'T' or data.get('kind') == 'truncate':
            # TRUNCATE event
            return cls(
                action='T',
                schema=data.get('schema', ''),
                table=data.get('table', ''),
                lsn=data.get('lsn'),
                timestamp=timestamp,
                xid=data.get('xid'),
                origin=data.get('origin'),
                truncate_cascade=data.get('cascade'),
                truncate_restart_identity=data.get('restart_identity')
            )

        elif action == 'M' or data.get('kind') == 'message':
            # MESSAGE event
            return cls(
                action='M',
                transactional=data.get('transactional', True),
                prefix=data.get('prefix', ''),
                content=data.get('content', ''),
                lsn=data.get('lsn')
            )

        elif action == 'B':
            # BEGIN transaction marker
            return cls(
                action='B',
                xid=data.get('xid'),
                lsn=data.get('lsn'),
                timestamp=timestamp
            )

        elif action == 'C':
            # COMMIT transaction marker
            return cls(
                action='C',
                xid=data.get('xid'),
                lsn=data.get('lsn'),
                timestamp=timestamp
            )

        else:
            # Unknown action type
            logger.warning(f"Unknown CDC action type: {action}")
            return cls(
                action=action,
                schema=data.get('schema', ''),
                table=data.get('table', ''),
                lsn=data.get('lsn'),
                timestamp=timestamp,
                xid=data.get('xid'),
                origin=data.get('origin')
            )


@dataclass
class CDCTransaction:
    """CDC transaction containing multiple events."""

    xid: Optional[int]
    lsn: Optional[str]
    timestamp: Optional[datetime]
    origin: Optional[str]
    changes: List[CDCEvent]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCTransaction':
        """Create CDC transaction from wal2json dictionary (format v1)."""
        # Parse timestamp
        timestamp = None
        if 'timestamp' in data:
            try:
                timestamp_str = data['timestamp']
                if timestamp_str and timestamp_str != 'null':
                    timestamp = datetime.fromisoformat(timestamp_str.replace(' ', 'T'))
            except (ValueError, TypeError):
                logger.warning(f"Failed to parse transaction timestamp: {data.get('timestamp')}")

        # Parse changes
        changes = []
        for change_data in data.get('change', []):
            try:
                change = CDCEvent.from_dict(change_data)
                changes.append(change)
            except Exception as e:
                logger.error(f"Failed to parse CDC change: {e}, data: {change_data}")

        return cls(
            xid=data.get('xid'),
            lsn=data.get('lsn'),
            timestamp=timestamp,
            origin=data.get('origin'),
            changes=changes
        )


# ============================================================================
# Wal2json Parser
# ============================================================================

class Wal2JsonParser:
    """
    Parser for wal2json logical decoding output.

    Supports both format versions:
    - v1: Transaction-based (single JSON object per transaction)
    - v2: Tuple-based (one JSON object per tuple change)
    """

    def __init__(self, format_version: int = 2):
        """
        Initialize parser.

        Args:
            format_version: wal2json format version (1 or 2)
        """
        self.format_version = format_version
        self._buffer = ""

    def parse_message(self, data: bytes) -> List[Union[CDCEvent, CDCTransaction]]:
        """
        Parse wal2json message data.

        Args:
            data: Raw bytes from replication stream

        Returns:
            List of CDC events/transactions
        """
        try:
            # Decode bytes to string
            json_str = data.decode('utf-8')

            # Handle multiple JSON objects (can be concatenated)
            json_str = json_str.strip()

            if not json_str:
                return []

            # Split on newlines for multiple messages
            messages = json_str.split('\n')
            results = []

            for msg in messages:
                msg = msg.strip()
                if not msg:
                    continue

                try:
                    parsed = json.loads(msg)

                    if self.format_version == 1:
                        # Format v1: transaction-based
                        if isinstance(parsed, dict) and 'change' in parsed:
                            transaction = CDCTransaction.from_dict(parsed)
                            results.append(transaction)
                        else:
                            logger.warning(f"Unexpected v1 format: {parsed}")

                    else:
                        # Format v2: tuple-based
                        if isinstance(parsed, dict):
                            event = CDCEvent.from_dict(parsed)
                            results.append(event)
                        else:
                            logger.warning(f"Unexpected v2 format: {parsed}")

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON: {e}, data: {msg[:200]}...")
                    continue

            return results

        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode message data: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing message: {e}")
            return []

    def parse_replication_message(self, replication_msg) -> List[Union[CDCEvent, CDCTransaction]]:
        """
        Parse from ReplicationMessage object.

        Args:
            replication_msg: ReplicationMessage from libpq

        Returns:
            List of CDC events/transactions
        """
        return self.parse_message(replication_msg.data)


# ============================================================================
# Convenience Functions
# ============================================================================

def create_parser(format_version: int = 2) -> Wal2JsonParser:
    """
    Create a wal2json parser.

    Args:
        format_version: wal2json format version (1 or 2)

    Returns:
        Configured Wal2JsonParser instance
    """
    return Wal2JsonParser(format_version=format_version)


def parse_wal2json_message(data: bytes, format_version: int = 2) -> List[Union[CDCEvent, CDCTransaction]]:
    """
    Parse wal2json message data.

    Args:
        data: Raw bytes from replication stream
        format_version: wal2json format version (1 or 2)

    Returns:
        List of parsed CDC events/transactions
    """
    parser = create_parser(format_version)
    return parser.parse_message(data)







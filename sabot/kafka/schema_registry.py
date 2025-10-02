#!/usr/bin/env python3
"""
Confluent Schema Registry Client

Pure Python implementation for Phase 1.
Phase 2: Will be optimized with Cython for lock-free schema caching.

Handles:
- Schema registration and retrieval
- Subject/version management
- In-memory schema caching
- Compatibility checking
"""

import logging
import json
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class CompatibilityMode(Enum):
    """Schema compatibility modes."""
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


@dataclass
class RegisteredSchema:
    """Registered schema metadata."""
    schema_id: int
    schema_str: str
    subject: str
    version: int
    schema_type: str = "AVRO"  # AVRO, PROTOBUF, JSON


class SchemaRegistryClient:
    """
    Client for Confluent Schema Registry.

    Pure Python implementation with HTTP requests.
    Phase 2 will add Cython optimization for schema caching.
    """

    def __init__(self, url: str, timeout: int = 30):
        """
        Initialize Schema Registry client.

        Args:
            url: Schema Registry URL (e.g., "http://localhost:8081")
            timeout: Request timeout in seconds
        """
        self.url = url.rstrip('/')
        self.timeout = timeout

        # In-memory caches (will be replaced with Cython lock-free cache in Phase 2)
        self._schemas_by_id: Dict[int, RegisteredSchema] = {}
        self._schemas_by_subject: Dict[str, Dict[int, RegisteredSchema]] = {}
        self._latest_versions: Dict[str, int] = {}

        # HTTP client (using requests or httpx)
        try:
            import httpx
            self._client = httpx.Client(timeout=timeout)
            self._async_client = httpx.AsyncClient(timeout=timeout)
            self._http_lib = 'httpx'
        except ImportError:
            import requests
            self._session = requests.Session()
            self._http_lib = 'requests'

        logger.info(f"Schema Registry client initialized: {url} (using {self._http_lib})")

    def register_schema(self, subject: str, schema_str: str, schema_type: str = "AVRO") -> int:
        """
        Register a schema for a subject.

        Args:
            subject: Subject name (typically "{topic}-value" or "{topic}-key")
            schema_str: Schema definition (JSON string for Avro, proto file for Protobuf)
            schema_type: Schema type ("AVRO", "PROTOBUF", "JSON")

        Returns:
            Schema ID

        Raises:
            Exception: If registration fails
        """
        url = f"{self.url}/subjects/{subject}/versions"

        payload = {
            "schema": schema_str,
            "schemaType": schema_type
        }

        if self._http_lib == 'httpx':
            response = self._client.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
        else:
            response = self._session.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()

        schema_id = result['id']

        # Cache the schema
        registered_schema = RegisteredSchema(
            schema_id=schema_id,
            schema_str=schema_str,
            subject=subject,
            version=result.get('version', 0),
            schema_type=schema_type
        )

        self._schemas_by_id[schema_id] = registered_schema

        if subject not in self._schemas_by_subject:
            self._schemas_by_subject[subject] = {}
        self._schemas_by_subject[subject][schema_id] = registered_schema

        logger.info(f"Registered schema for subject '{subject}': ID={schema_id}")

        return schema_id

    def get_schema_by_id(self, schema_id: int) -> Optional[RegisteredSchema]:
        """
        Get schema by ID.

        Args:
            schema_id: Schema ID

        Returns:
            RegisteredSchema or None if not found
        """
        # Check cache first
        if schema_id in self._schemas_by_id:
            return self._schemas_by_id[schema_id]

        # Fetch from registry
        url = f"{self.url}/schemas/ids/{schema_id}"

        try:
            if self._http_lib == 'httpx':
                response = self._client.get(url)
                response.raise_for_status()
                result = response.json()
            else:
                response = self._session.get(url, timeout=self.timeout)
                response.raise_for_status()
                result = response.json()

            # Parse response
            registered_schema = RegisteredSchema(
                schema_id=schema_id,
                schema_str=result['schema'],
                subject=result.get('subject', f'schema-{schema_id}'),
                version=result.get('version', 0),
                schema_type=result.get('schemaType', 'AVRO')
            )

            # Cache it
            self._schemas_by_id[schema_id] = registered_schema

            return registered_schema

        except Exception as e:
            logger.error(f"Failed to fetch schema ID {schema_id}: {e}")
            return None

    def get_latest_schema(self, subject: str) -> Optional[RegisteredSchema]:
        """
        Get latest schema version for a subject.

        Args:
            subject: Subject name

        Returns:
            RegisteredSchema or None if not found
        """
        url = f"{self.url}/subjects/{subject}/versions/latest"

        try:
            if self._http_lib == 'httpx':
                response = self._client.get(url)
                response.raise_for_status()
                result = response.json()
            else:
                response = self._session.get(url, timeout=self.timeout)
                response.raise_for_status()
                result = response.json()

            schema_id = result['id']
            version = result['version']

            registered_schema = RegisteredSchema(
                schema_id=schema_id,
                schema_str=result['schema'],
                subject=subject,
                version=version,
                schema_type=result.get('schemaType', 'AVRO')
            )

            # Cache it
            self._schemas_by_id[schema_id] = registered_schema
            if subject not in self._schemas_by_subject:
                self._schemas_by_subject[subject] = {}
            self._schemas_by_subject[subject][schema_id] = registered_schema
            self._latest_versions[subject] = version

            return registered_schema

        except Exception as e:
            logger.error(f"Failed to fetch latest schema for subject '{subject}': {e}")
            return None

    def get_subjects(self) -> List[str]:
        """
        Get all registered subjects.

        Returns:
            List of subject names
        """
        url = f"{self.url}/subjects"

        try:
            if self._http_lib == 'httpx':
                response = self._client.get(url)
                response.raise_for_status()
                return response.json()
            else:
                response = self._session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()

        except Exception as e:
            logger.error(f"Failed to fetch subjects: {e}")
            return []

    def check_compatibility(self, subject: str, schema_str: str, version: str = "latest") -> bool:
        """
        Check if a schema is compatible with a subject's version.

        Args:
            subject: Subject name
            schema_str: Schema to check
            version: Version to check against (default: "latest")

        Returns:
            True if compatible, False otherwise
        """
        url = f"{self.url}/compatibility/subjects/{subject}/versions/{version}"

        payload = {"schema": schema_str}

        try:
            if self._http_lib == 'httpx':
                response = self._client.post(url, json=payload)
                response.raise_for_status()
                result = response.json()
            else:
                response = self._session.post(url, json=payload, timeout=self.timeout)
                response.raise_for_status()
                result = response.json()

            return result.get('is_compatible', False)

        except Exception as e:
            logger.error(f"Failed to check compatibility for subject '{subject}': {e}")
            return False

    def set_compatibility(self, subject: str, compatibility: CompatibilityMode) -> bool:
        """
        Set compatibility mode for a subject.

        Args:
            subject: Subject name
            compatibility: Compatibility mode

        Returns:
            True if successful
        """
        url = f"{self.url}/config/{subject}"

        payload = {"compatibility": compatibility.value}

        try:
            if self._http_lib == 'httpx':
                response = self._client.put(url, json=payload)
                response.raise_for_status()
            else:
                response = self._session.put(url, json=payload, timeout=self.timeout)
                response.raise_for_status()

            logger.info(f"Set compatibility for subject '{subject}': {compatibility.value}")
            return True

        except Exception as e:
            logger.error(f"Failed to set compatibility for subject '{subject}': {e}")
            return False

    def close(self):
        """Close HTTP client."""
        if self._http_lib == 'httpx':
            self._client.close()
            if hasattr(self, '_async_client'):
                import asyncio
                try:
                    asyncio.get_event_loop().run_until_complete(self._async_client.aclose())
                except:
                    pass
        else:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

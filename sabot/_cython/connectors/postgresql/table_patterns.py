"""
Table pattern matching for PostgreSQL CDC (Flink CDC compatible).

Supports patterns like:
- public.users           # Single table
- public.*               # All tables in schema
- public.user_*          # Wildcard match
- *.orders               # Table in any schema
- public.user_[0-9]+     # Regex pattern
"""

import re
from typing import List, Pattern, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class TablePattern:
    """
    Table pattern for matching PostgreSQL tables.

    Matches schema.table patterns with SQL wildcards (*) and regex.
    """

    schema: str
    table_pattern: str
    schema_regex: Pattern
    table_regex: Pattern

    @classmethod
    def parse(cls, pattern: str) -> 'TablePattern':
        """
        Parse table pattern string.

        Args:
            pattern: Pattern like 'public.users', 'public.*', 'app_db_*.orders'

        Returns:
            TablePattern instance

        Examples:
            >>> TablePattern.parse('public.users')
            TablePattern(schema='public', table_pattern='users')

            >>> TablePattern.parse('public.*')
            TablePattern(schema='public', table_pattern='.*')

            >>> TablePattern.parse('*.user_[0-9]+')
            TablePattern(schema='*', table_pattern='user_[0-9]+')
        """
        # Split schema.table
        if '.' in pattern:
            schema, table = pattern.split('.', 1)
        else:
            # Default to public schema
            schema = 'public'
            table = pattern

        # Convert SQL wildcards to regex
        # * → .* (match any characters)
        # ? → . (match single character)
        schema_regex_str = schema.replace('*', '.*').replace('?', '.')
        table_regex_str = table.replace('*', '.*').replace('?', '.')

        return cls(
            schema=schema,
            table_pattern=table,
            schema_regex=re.compile(f'^{schema_regex_str}$', re.IGNORECASE),
            table_regex=re.compile(f'^{table_regex_str}$', re.IGNORECASE)
        )

    def matches(self, schema: str, table: str) -> bool:
        """
        Check if schema.table matches this pattern.

        Args:
            schema: PostgreSQL schema name
            table: Table name

        Returns:
            True if matches
        """
        # Match schema
        if not self.schema_regex.match(schema):
            return False

        # Match table
        if not self.table_regex.match(table):
            return False

        return True

    def __repr__(self):
        return f"TablePattern('{self.schema}.{self.table_pattern}')"


class TablePatternMatcher:
    """
    Matches PostgreSQL tables against Flink CDC-style patterns.

    Example:
        >>> matcher = TablePatternMatcher(['public.*', 'analytics.events_*'])
        >>> tables = [
        ...     ('public', 'users'),
        ...     ('public', 'orders'),
        ...     ('analytics', 'events_click'),
        ...     ('other', 'data')
        ... ]
        >>> matched = matcher.match_tables(tables)
        >>> # Returns: [('public', 'users'), ('public', 'orders'), ('analytics', 'events_click')]
    """

    def __init__(self, patterns: List[str]):
        """
        Initialize matcher with patterns.

        Args:
            patterns: List of pattern strings
        """
        self.patterns = [TablePattern.parse(p) for p in patterns]
        logger.info(f"Created matcher with {len(self.patterns)} patterns: {patterns}")

    def match_tables(self, available_tables: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """
        Match available tables against patterns.

        Args:
            available_tables: List of (schema, table) tuples

        Returns:
            List of matched (schema, table) tuples (deduplicated)

        Example:
            >>> matcher = TablePatternMatcher(['public.*'])
            >>> tables = [('public', 'users'), ('other', 'data')]
            >>> matcher.match_tables(tables)
            [('public', 'users')]
        """
        matched = set()  # Use set to deduplicate

        for schema, table in available_tables:
            for pattern in self.patterns:
                if pattern.matches(schema, table):
                    matched.add((schema, table))
                    logger.debug(f"Pattern {pattern} matched {schema}.{table}")
                    break  # Move to next table

        result = sorted(list(matched))  # Sort for consistency
        logger.info(
            f"Matched {len(result)}/{len(available_tables)} tables: "
            f"{result if len(result) <= 10 else f'{result[:10]}...'}"
        )

        return result

    def matches_table(self, schema: str, table: str) -> bool:
        """
        Check if single table matches any pattern.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            True if matches
        """
        for pattern in self.patterns:
            if pattern.matches(schema, table):
                return True
        return False


def validate_pattern(pattern: str) -> Optional[str]:
    """
    Validate pattern syntax.

    Args:
        pattern: Pattern to validate

    Returns:
        Error message if invalid, None if valid

    Example:
        >>> validate_pattern('public.users')
        None

        >>> validate_pattern('invalid..pattern')
        'Invalid pattern: multiple dots'
    """
    # Check for multiple dots
    if pattern.count('.') > 1:
        return "Invalid pattern: multiple dots (should be schema.table)"

    # Check for empty parts
    if '..' in pattern:
        return "Invalid pattern: empty schema or table"

    # Try to compile regex
    try:
        TablePattern.parse(pattern)
    except Exception as e:
        return f"Invalid regex pattern: {e}"

    return None


def expand_patterns(
    patterns: List[str],
    available_tables: List[Tuple[str, str]]
) -> List[Tuple[str, str]]:
    """
    Convenience function to expand patterns against available tables.

    Args:
        patterns: List of patterns
        available_tables: Available (schema, table) tuples

    Returns:
        Matched tables

    Example:
        >>> expand_patterns(['public.*'], [('public', 'users'), ('other', 'data')])
        [('public', 'users')]
    """
    matcher = TablePatternMatcher(patterns)
    return matcher.match_tables(available_tables)

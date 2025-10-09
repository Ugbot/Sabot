"""
Table pattern matching for MySQL CDC (Flink CDC compatible).

Supports patterns like:
- ecommerce.users          # Single table
- ecommerce.*              # All tables in database
- ecommerce.user_*         # Wildcard match
- *.orders                 # Table in any database
- ecommerce.user_[0-9]+    # Regex pattern
"""

import re
from typing import List, Pattern, Tuple, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class TablePattern:
    """
    Table pattern for matching MySQL tables.

    MySQL uses database.table (vs PostgreSQL schema.table).
    Case sensitivity depends on OS (case-sensitive on Linux, case-insensitive on Windows/Mac).
    """

    database: str
    table_pattern: str
    database_regex: Pattern
    table_regex: Pattern
    case_sensitive: bool = True

    @classmethod
    def parse(cls, pattern: str, case_sensitive: bool = True) -> 'TablePattern':
        """
        Parse table pattern string.

        Args:
            pattern: Pattern like 'db.users', 'db.*', 'app_db_*.orders'
            case_sensitive: Case-sensitive matching (default True for Linux compatibility)

        Returns:
            TablePattern instance

        Examples:
            >>> TablePattern.parse('ecommerce.users')
            TablePattern(database='ecommerce', table_pattern='users')

            >>> TablePattern.parse('ecommerce.*')
            TablePattern(database='ecommerce', table_pattern='.*')

            >>> TablePattern.parse('*.user_[0-9]+')
            TablePattern(database='*', table_pattern='user_[0-9]+')
        """
        # Split database.table
        if '.' in pattern:
            database, table = pattern.split('.', 1)
        else:
            # Default to all databases
            database = '*'
            table = pattern

        # Convert SQL wildcards to regex
        # * → .* (match any characters)
        # ? → . (match single character)
        db_regex_str = database.replace('*', '.*').replace('?', '.')
        tbl_regex_str = table.replace('*', '.*').replace('?', '.')

        # Compile regex with appropriate case sensitivity
        flags = 0 if case_sensitive else re.IGNORECASE

        return cls(
            database=database,
            table_pattern=table,
            database_regex=re.compile(f'^{db_regex_str}$', flags),
            table_regex=re.compile(f'^{tbl_regex_str}$', flags),
            case_sensitive=case_sensitive
        )

    def matches(self, database: str, table: str) -> bool:
        """
        Check if database.table matches this pattern.

        Args:
            database: MySQL database name
            table: Table name

        Returns:
            True if matches
        """
        if not self.database_regex.match(database):
            return False
        if not self.table_regex.match(table):
            return False
        return True

    def __repr__(self):
        return f"TablePattern('{self.database}.{self.table_pattern}')"


class TablePatternMatcher:
    """
    Matches MySQL tables against Flink CDC-style patterns.

    Example:
        >>> matcher = TablePatternMatcher(['ecommerce.*', 'analytics.events_*'])
        >>> tables = [
        ...     ('ecommerce', 'users'),
        ...     ('ecommerce', 'orders'),
        ...     ('analytics', 'events_click'),
        ...     ('other', 'data')
        ... ]
        >>> matched = matcher.match_tables(tables)
        >>> # Returns: [('ecommerce', 'users'), ('ecommerce', 'orders'),
        >>> #           ('analytics', 'events_click')]
    """

    def __init__(self, patterns: List[str], case_sensitive: bool = True):
        """
        Initialize matcher with patterns.

        Args:
            patterns: List of pattern strings
            case_sensitive: Case-sensitive matching
        """
        self.patterns = [TablePattern.parse(p, case_sensitive) for p in patterns]
        self.case_sensitive = case_sensitive
        logger.info(f"Created matcher with {len(self.patterns)} patterns: {patterns}")

    def match_tables(
        self,
        available_tables: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        Match available tables against patterns.

        Args:
            available_tables: List of (database, table) tuples

        Returns:
            List of matched (database, table) tuples (deduplicated and sorted)
        """
        matched = set()

        for database, table in available_tables:
            for pattern in self.patterns:
                if pattern.matches(database, table):
                    matched.add((database, table))
                    logger.debug(f"Pattern {pattern} matched {database}.{table}")
                    break  # Move to next table

        result = sorted(list(matched))
        logger.info(
            f"Matched {len(result)}/{len(available_tables)} tables: "
            f"{result if len(result) <= 10 else f'{result[:10]}...'}"
        )

        return result

    def matches_table(self, database: str, table: str) -> bool:
        """
        Check if single table matches any pattern.

        Args:
            database: Database name
            table: Table name

        Returns:
            True if matches any pattern
        """
        for pattern in self.patterns:
            if pattern.matches(database, table):
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
        >>> validate_pattern('ecommerce.users')
        None

        >>> validate_pattern('invalid..pattern')
        'Invalid pattern: multiple consecutive dots'
    """
    # Check for multiple consecutive dots
    if '..' in pattern:
        return "Invalid pattern: multiple consecutive dots (should be database.table)"

    # Check for more than one dot
    if pattern.count('.') > 1:
        return "Invalid pattern: too many dots (should be database.table)"

    # Try to compile regex
    try:
        TablePattern.parse(pattern)
    except Exception as e:
        return f"Invalid regex pattern: {e}"

    return None


def expand_patterns(
    patterns: List[str],
    available_tables: List[Tuple[str, str]],
    case_sensitive: bool = True
) -> List[Tuple[str, str]]:
    """
    Convenience function to expand patterns against available tables.

    Args:
        patterns: List of patterns
        available_tables: Available (database, table) tuples
        case_sensitive: Case-sensitive matching

    Returns:
        Matched tables

    Example:
        >>> expand_patterns(
        ...     ['ecommerce.*'],
        ...     [('ecommerce', 'users'), ('other', 'data')]
        ... )
        [('ecommerce', 'users')]
    """
    matcher = TablePatternMatcher(patterns, case_sensitive)
    return matcher.match_tables(available_tables)

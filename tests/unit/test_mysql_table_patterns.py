"""Unit tests for MySQL table pattern matching."""

import pytest
from sabot._cython.connectors.mysql_table_patterns import (
    TablePattern,
    TablePatternMatcher,
    validate_pattern,
    expand_patterns
)


class TestTablePattern:
    """Test TablePattern parsing and matching."""

    def test_parse_simple_table(self):
        """Test parsing simple table pattern."""
        pattern = TablePattern.parse('ecommerce.users')
        assert pattern.database == 'ecommerce'
        assert pattern.table_pattern == 'users'
        assert pattern.matches('ecommerce', 'users')
        assert not pattern.matches('ecommerce', 'orders')
        assert not pattern.matches('other', 'users')

    def test_parse_wildcard_all_tables(self):
        """Test parsing wildcard for all tables in database."""
        pattern = TablePattern.parse('ecommerce.*')
        assert pattern.database == 'ecommerce'
        assert pattern.table_pattern == '*'
        assert pattern.matches('ecommerce', 'users')
        assert pattern.matches('ecommerce', 'orders')
        assert pattern.matches('ecommerce', 'anything')
        assert not pattern.matches('other', 'users')

    def test_parse_wildcard_prefix(self):
        """Test parsing wildcard prefix pattern."""
        pattern = TablePattern.parse('ecommerce.user_*')
        assert pattern.matches('ecommerce', 'user_data')
        assert pattern.matches('ecommerce', 'user_profile')
        assert not pattern.matches('ecommerce', 'users')
        assert not pattern.matches('ecommerce', 'order_user')

    def test_parse_wildcard_suffix(self):
        """Test parsing wildcard suffix pattern."""
        pattern = TablePattern.parse('ecommerce.*_log')
        assert pattern.matches('ecommerce', 'access_log')
        assert pattern.matches('ecommerce', 'error_log')
        assert not pattern.matches('ecommerce', 'log')
        assert not pattern.matches('ecommerce', 'logs')

    def test_parse_wildcard_database(self):
        """Test parsing wildcard database pattern."""
        pattern = TablePattern.parse('*.users')
        assert pattern.matches('ecommerce', 'users')
        assert pattern.matches('analytics', 'users')
        assert not pattern.matches('ecommerce', 'orders')

    def test_parse_regex_pattern(self):
        """Test parsing regex pattern."""
        pattern = TablePattern.parse('ecommerce.user_[0-9]+')
        assert pattern.matches('ecommerce', 'user_1')
        assert pattern.matches('ecommerce', 'user_123')
        assert not pattern.matches('ecommerce', 'user_abc')
        assert not pattern.matches('ecommerce', 'user_')

    def test_parse_case_insensitive(self):
        """Test case-insensitive matching."""
        pattern = TablePattern.parse('ecommerce.Users', case_sensitive=False)
        assert pattern.matches('ecommerce', 'users')
        assert pattern.matches('ecommerce', 'USERS')
        assert pattern.matches('ecommerce', 'Users')

    def test_parse_no_database(self):
        """Test parsing pattern without database."""
        pattern = TablePattern.parse('users')
        assert pattern.database == '*'
        assert pattern.table_pattern == 'users'
        assert pattern.matches('any_db', 'users')


class TestTablePatternMatcher:
    """Test TablePatternMatcher."""

    def test_single_pattern(self):
        """Test matching single pattern."""
        matcher = TablePatternMatcher(['ecommerce.users'])
        tables = [
            ('ecommerce', 'users'),
            ('ecommerce', 'orders'),
            ('other', 'users')
        ]
        matched = matcher.match_tables(tables)
        assert matched == [('ecommerce', 'users')]

    def test_wildcard_pattern(self):
        """Test matching wildcard pattern."""
        matcher = TablePatternMatcher(['ecommerce.*'])
        tables = [
            ('ecommerce', 'users'),
            ('ecommerce', 'orders'),
            ('other', 'users')
        ]
        matched = matcher.match_tables(tables)
        assert sorted(matched) == [('ecommerce', 'orders'), ('ecommerce', 'users')]

    def test_multiple_patterns(self):
        """Test matching multiple patterns."""
        matcher = TablePatternMatcher([
            'ecommerce.users',
            'analytics.events_*'
        ])
        tables = [
            ('ecommerce', 'users'),
            ('ecommerce', 'orders'),
            ('analytics', 'events_click'),
            ('analytics', 'events_view'),
            ('analytics', 'data')
        ]
        matched = matcher.match_tables(tables)
        assert sorted(matched) == [
            ('analytics', 'events_click'),
            ('analytics', 'events_view'),
            ('ecommerce', 'users')
        ]

    def test_no_matches(self):
        """Test no matching tables."""
        matcher = TablePatternMatcher(['nonexistent.*'])
        tables = [('ecommerce', 'users'), ('ecommerce', 'orders')]
        matched = matcher.match_tables(tables)
        assert matched == []

    def test_duplicate_matches(self):
        """Test deduplication of matches."""
        matcher = TablePatternMatcher([
            'ecommerce.users',
            'ecommerce.*'  # Also matches users
        ])
        tables = [('ecommerce', 'users'), ('ecommerce', 'orders')]
        matched = matcher.match_tables(tables)
        # Should deduplicate
        assert sorted(matched) == [('ecommerce', 'orders'), ('ecommerce', 'users')]

    def test_matches_table(self):
        """Test single table matching."""
        matcher = TablePatternMatcher(['ecommerce.*', 'analytics.events_*'])
        assert matcher.matches_table('ecommerce', 'users')
        assert matcher.matches_table('analytics', 'events_click')
        assert not matcher.matches_table('other', 'data')


class TestValidatePattern:
    """Test pattern validation."""

    def test_valid_patterns(self):
        """Test validation of valid patterns."""
        assert validate_pattern('ecommerce.users') is None
        assert validate_pattern('ecommerce.*') is None
        assert validate_pattern('*.users') is None
        assert validate_pattern('db.user_*') is None
        assert validate_pattern('db.user_[0-9]+') is None

    def test_invalid_multiple_dots(self):
        """Test validation of pattern with multiple dots."""
        error = validate_pattern('db..table')
        assert error is not None
        assert 'multiple consecutive dots' in error.lower()

    def test_invalid_too_many_dots(self):
        """Test validation of pattern with too many dots."""
        error = validate_pattern('db.schema.table')
        assert error is not None
        assert 'too many dots' in error.lower()


class TestExpandPatterns:
    """Test pattern expansion convenience function."""

    def test_expand_simple(self):
        """Test simple pattern expansion."""
        tables = [
            ('ecommerce', 'users'),
            ('ecommerce', 'orders'),
            ('analytics', 'events')
        ]
        matched = expand_patterns(['ecommerce.*'], tables)
        assert sorted(matched) == [('ecommerce', 'orders'), ('ecommerce', 'users')]

    def test_expand_multiple(self):
        """Test multiple pattern expansion."""
        tables = [
            ('ecommerce', 'users'),
            ('ecommerce', 'orders'),
            ('analytics', 'events_click'),
            ('analytics', 'events_view')
        ]
        matched = expand_patterns(
            ['ecommerce.users', 'analytics.events_*'],
            tables
        )
        assert sorted(matched) == [
            ('analytics', 'events_click'),
            ('analytics', 'events_view'),
            ('ecommerce', 'users')
        ]

    def test_expand_case_insensitive(self):
        """Test case-insensitive pattern expansion."""
        tables = [('ecommerce', 'Users'), ('ecommerce', 'ORDERS')]
        matched = expand_patterns(
            ['ecommerce.users'],
            tables,
            case_sensitive=False
        )
        assert matched == [('ecommerce', 'Users')]

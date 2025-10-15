#!/usr/bin/env python3
"""
SabotQL Integration Tests

Tests SabotQL integration with Sabot streaming pipelines.
"""

import unittest
import tempfile
import shutil
from pathlib import Path

import pyarrow as pa


class TestTripleLookupOperator(unittest.TestCase):
    """Test triple lookup operator."""
    
    def setUp(self):
        """Setup test fixtures."""
        self.temp_dir = tempfile.mkdtemp(prefix='sabot_ql_test_')
        self.db_path = str(Path(self.temp_dir) / 'test.db')
    
    def tearDown(self):
        """Cleanup."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def test_basic_enrichment(self):
        """Test basic triple enrichment."""
        try:
            from sabot.operators.triple_lookup import TripleLookupOperator
            from sabot_ql.bindings.python import create_triple_store
        except ImportError:
            self.skipTest("SabotQL bindings not available")
        
        # Create triple store
        kg = create_triple_store(self.db_path)
        
        # Add test data
        kg.insert_triple(
            'http://example.org/AAPL',
            'http://schema.org/name',
            '"Apple Inc."'
        )
        
        # Create test batch
        batch = pa.RecordBatch.from_pydict({
            'symbol': ['http://example.org/AAPL'],
            'price': [150.23]
        })
        
        # Enrich
        op = TripleLookupOperator(
            source=iter([batch]),
            triple_store=kg,
            lookup_key='symbol',
            predicate='http://schema.org/name',
            batch_lookups=True
        )
        
        result = next(iter(op))
        
        # Verify enrichment
        self.assertEqual(result.num_rows, 1)
        self.assertIn('name', result.schema.names)
    
    def test_batch_lookups(self):
        """Test batch lookup mode."""
        try:
            from sabot.operators.triple_lookup import TripleLookupOperator
            from sabot_ql.bindings.python import create_triple_store
        except ImportError:
            self.skipTest("SabotQL bindings not available")
        
        kg = create_triple_store(self.db_path)
        
        # Add multiple companies
        for i in range(10):
            kg.insert_triple(
                f'http://example.org/Company{i}',
                'http://schema.org/name',
                f'"Company {i} Inc."'
            )
        
        # Batch with multiple rows
        batch = pa.RecordBatch.from_pydict({
            'company': [f'http://example.org/Company{i}' for i in range(10)],
            'value': list(range(10))
        })
        
        # Enrich with batch mode
        op = TripleLookupOperator(
            source=iter([batch]),
            triple_store=kg,
            lookup_key='company',
            predicate='http://schema.org/name',
            batch_lookups=True
        )
        
        result = next(iter(op))
        self.assertEqual(result.num_rows, 10)
    
    def test_cache_effectiveness(self):
        """Test LRU cache hit rate."""
        try:
            from sabot.operators.triple_lookup import TripleLookupOperator
            from sabot_ql.bindings.python import create_triple_store
        except ImportError:
            self.skipTest("SabotQL bindings not available")
        
        kg = create_triple_store(self.db_path)
        
        # Add data
        for i in range(100):
            kg.insert_triple(
                f'http://example.org/E{i}',
                'http://schema.org/prop',
                f'"Value{i}"'
            )
        
        # Create batches with repeated keys (should hit cache)
        batches = []
        for _ in range(10):
            batch = pa.RecordBatch.from_pydict({
                'entity': [f'http://example.org/E{i % 10}' for i in range(100)],
                'data': list(range(100))
            })
            batches.append(batch)
        
        # Process with cache
        op = TripleLookupOperator(
            source=iter(batches),
            triple_store=kg,
            lookup_key='entity',
            predicate='http://schema.org/prop',
            batch_lookups=False,  # Row-by-row to test cache
            cache_size=50
        )
        
        for _ in op:
            pass
        
        stats = op.get_stats()
        hit_rate = stats['cache_hit_rate']
        
        # Should have high hit rate (repeated keys)
        self.assertGreater(hit_rate, 0.5, "Cache hit rate should be >50%")


def run_tests():
    """Run all tests."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestTripleLookupOperator)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    import sys
    sys.exit(run_tests())


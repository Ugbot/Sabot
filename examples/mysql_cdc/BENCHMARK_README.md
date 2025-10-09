# MySQL CDC Benchmark

Performance benchmark for Sabot's MySQL CDC connector.

## Setup

### 1. Start MySQL with Docker

```bash
cd /Users/bengamble/Sabot
docker compose up -d mysql
```

Wait for MySQL to be ready (about 30 seconds):

```bash
# Check MySQL is healthy
docker compose ps mysql

# Or watch logs
docker compose logs -f mysql
```

### 2. Run Quick Benchmark (1K records)

```bash
python examples/mysql_cdc_benchmark.py --quick
```

### 3. Run Full Benchmark (10K records)

```bash
python examples/mysql_cdc_benchmark.py
```

### 4. Custom Benchmark

```bash
# Benchmark with 50K records
python examples/mysql_cdc_benchmark.py --records 50000
```

## Expected Performance

Based on hardware (M1 Pro, 16GB RAM):

### Python Implementation (Current)

```
Write Performance:
  Records written: 10,000
  Duration: ~1-2s
  Throughput: ~5,000-10,000 records/sec

CDC Performance:
  Records received: 10,000
  Duration: ~2-3s
  Throughput: ~3,000-5,000 records/sec
  Time to first event: ~100-200ms

Batch Processing:
  Batches processed: 100
  Avg batch size: 100 records
  Avg batch time: 20-30ms

End-to-End Performance:
  Total duration: ~3-5s
  Throughput: ~2,000-3,000 records/sec
```

### C++/Cython Target (Future)

Expected 10-100x improvement:

```
CDC Performance:
  Throughput: 50,000-500,000 records/sec
  Time to first event: <10ms
  Batch time: <5ms
```

## Benchmark Metrics

The benchmark measures:

1. **Write Throughput**: Records written to MySQL per second
2. **CDC Throughput**: Records streamed through Sabot per second
3. **Time to First Event**: Latency from write completion to first CDC event
4. **Batch Processing Time**: Time to process each CDC batch
5. **End-to-End Throughput**: Overall pipeline throughput

## Troubleshooting

### MySQL Not Ready

If you see connection errors, MySQL may still be starting:

```bash
# Wait for MySQL to be ready
docker compose logs mysql | grep "ready for connections"

# Should see: "ready for connections" twice
```

### Port Already in Use

If port 3307 is already in use:

```bash
# Check what's using the port
lsof -i :3307

# Stop the conflicting service or change port in docker-compose.yml
```

### Performance Issues

If benchmark performance is poor:

1. Check Docker resource limits:
   - Docker Desktop → Settings → Resources
   - Allocate at least 4GB RAM

2. Check system load:
   ```bash
   top -o cpu
   ```

3. Close resource-intensive applications

## Benchmarking Different Scenarios

### Large Batches (1000 records/batch)

Edit `mysql_cdc_benchmark.py`:
```python
config = MySQLCDCConfig(
    ...
    batch_size=1000,  # Increase batch size
)
```

### High-Throughput Write

Increase write batch size in `write_benchmark_data()`:
```python
batch_size = 1000  # Up from 100
```

### Multiple Tables

Modify `setup_database()` to create multiple tables and split writes across them.

## Next Steps

1. Run baseline benchmark with Python implementation
2. Implement C++/Cython version
3. Compare performance improvements
4. Optimize based on profiling results

## See Also

- [MySQL CDC Documentation](../docs/MYSQL_CDC.md)
- [PostgreSQL CDC Benchmark](postgres_cdc_benchmark.py) - Similar benchmark
- [Performance Tuning](../docs/PERFORMANCE.md) - Optimization guide

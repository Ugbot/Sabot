#!/usr/bin/env python3
"""
Two-Node Network Enrichment Demo

Demonstrates distributed processing with Arrow Flight:
- Node 1 (Server): Serves securities data via Arrow Flight
- Node 2 (Client): Fetches securities, joins with local quotes

Usage:
    # Terminal 1: Start securities server
    python two_node_network_demo.py server --port 8815 --securities 1000000

    # Terminal 2: Start join client
    python two_node_network_demo.py client --server localhost:8815 --quotes 100000
"""

import argparse
import time
import sys
import importlib.util
from pathlib import Path
from typing import Optional

# Import pyarrow FIRST to load Arrow symbols
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight

# Direct import of ArrowIPCReader
spec = importlib.util.spec_from_file_location(
    "ipc_reader",
    "/Users/bengamble/Sabot/sabot/_c/ipc_reader.cpython-311-darwin.so"
)
ipc_reader_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ipc_reader_module)
ArrowIPCReader = ipc_reader_module.ArrowIPCReader

# Import Sabot's native hash join (SIMD-accelerated)
try:
    from sabot._c.arrow_core import hash_join_batches
    NATIVE_JOIN_AVAILABLE = True
    print("✅ Using Sabot's native hash_join_batches (SIMD-accelerated)")
except ImportError as e:
    NATIVE_JOIN_AVAILABLE = False
    print(f"⚠️  Native join not available: {e}, using Python fallback")


class SecuritiesFlightServer(flight.FlightServerBase):
    """Arrow Flight server that serves securities data."""

    def __init__(self, location, securities_table):
        super().__init__(location)
        self.securities = securities_table
        self._location = location  # Store location for list_flights
        print(f"✅ Flight server initialized with {securities_table.num_rows:,} securities")

    def do_get(self, context, ticket):
        """Serve securities data when requested."""
        # Parse ticket for column projection
        ticket_str = ticket.ticket.decode('utf-8')

        if ticket_str.startswith('columns:'):
            # Column projection requested
            columns_str = ticket_str.split(':', 1)[1]
            columns = columns_str.split(',')
            projected = self.securities.select(columns)
            print(f"📤 Serving {projected.num_rows:,} rows with column projection: {columns}")
            print(f"   Original size: {self.securities.nbytes / 1024 / 1024:.1f} MB")
            print(f"   Projected size: {projected.nbytes / 1024 / 1024:.1f} MB")
            print(f"   Reduction: {100 * (1 - projected.nbytes / self.securities.nbytes):.1f}%")
            return flight.RecordBatchStream(projected)
        else:
            # No projection, return all columns
            print(f"📤 Serving securities data ({self.securities.num_rows:,} rows, all columns)...")
            return flight.RecordBatchStream(self.securities)

    def list_flights(self, context, criteria):
        """List available data streams."""
        descriptor = flight.FlightDescriptor.for_path("securities")
        info = flight.FlightInfo(
            schema=self.securities.schema,
            descriptor=descriptor,
            endpoints=[flight.FlightEndpoint(b"securities", [self._location])],
            total_records=self.securities.num_rows,
            total_bytes=self.securities.nbytes
        )
        yield info


def load_arrow_file(filepath: Path, limit_rows: Optional[int] = None) -> pa.Table:
    """Load Arrow IPC file using streaming reader."""
    start = time.perf_counter()

    reader = ArrowIPCReader(str(filepath))
    batches = reader.read_batches(limit_rows=limit_rows if limit_rows else -1)
    table = pa.Table.from_batches(batches)

    elapsed = time.perf_counter() - start
    throughput = table.num_rows / elapsed / 1e6

    print(f"✅ Loaded {table.num_rows:,} rows in {elapsed*1000:.1f}ms ({throughput:.1f}M rows/sec)")
    return table


def run_server(port: int, limit_securities: Optional[int] = None):
    """Run Arrow Flight server with securities data."""
    print("=" * 80)
    print(f"NODE 1: Securities Flight Server (Port {port})")
    print("=" * 80)
    print()

    data_dir = Path(__file__).parent
    securities_arrow = data_dir / "master_security_10m.arrow"

    # Load securities
    print(f"📂 Loading securities from {securities_arrow.name}...")
    securities = load_arrow_file(securities_arrow, limit_rows=limit_securities)
    print(f"   Columns: {securities.num_columns}, Rows: {securities.num_rows:,}")
    print()

    # Start Flight server
    location = flight.Location.for_grpc_tcp("0.0.0.0", port)
    server = SecuritiesFlightServer(location, securities)

    print(f"🌐 Starting Arrow Flight server on port {port}...")
    print(f"   Access URL: grpc://localhost:{port}")
    print()

    server.serve()
    print("✅ Server started!")
    print("Press Ctrl+C to stop...")
    print()

    try:
        server.wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
        server.shutdown()


def run_client(server_addr: str, limit_quotes: Optional[int] = None):
    """Run client that fetches securities and performs join."""
    print("=" * 80)
    print(f"NODE 2: Join Client (Server: {server_addr})")
    print("=" * 80)
    print()

    data_dir = Path(__file__).parent
    quotes_arrow = data_dir / "synthetic_inventory.arrow"

    # Load quotes locally
    print(f"📂 Loading quotes from {quotes_arrow.name}...")
    quotes = load_arrow_file(quotes_arrow, limit_rows=limit_quotes)
    print(f"   Columns: {quotes.num_columns}, Rows: {quotes.num_rows:,}")
    print()

    # Connect to Flight server and fetch securities
    print(f"🌐 Connecting to Flight server: {server_addr}...")
    try:
        client = flight.FlightClient(f"grpc://{server_addr}")

        # List available flights
        flights = list(client.list_flights())
        if not flights:
            print("❌ No data available from server")
            return

        print(f"✅ Connected! Available flights: {len(flights)}")
        for flight_info in flights:
            print(f"   - {flight_info.total_records:,} records, {flight_info.total_bytes:,} bytes")
        print()

        # Fetch securities data with column projection (only ID for join)
        print("📥 Fetching securities from server (with column projection)...")
        print("   Requesting only: ID column")
        start = time.perf_counter()

        # Create custom ticket with column projection
        columns_needed = ['ID']
        ticket_str = f"columns:{','.join(columns_needed)}"
        custom_ticket = flight.Ticket(ticket_str.encode('utf-8'))

        reader = client.do_get(custom_ticket)

        # Read all batches
        securities_batches = []
        for chunk in reader:
            securities_batches.append(chunk.data)

        securities = pa.Table.from_batches(securities_batches)

        elapsed = time.perf_counter() - start
        throughput = securities.num_rows / elapsed / 1e6

        print(f"✅ Fetched {securities.num_rows:,} securities in {elapsed*1000:.1f}ms ({throughput:.1f}M rows/sec)")
        print(f"   Columns received: {securities.column_names}")
        print(f"   Data size: {securities.nbytes / 1024 / 1024:.1f} MB")
        print()

    except Exception as e:
        print(f"❌ Failed to connect to server: {e}")
        print("   Make sure the server is running!")
        return

    # Perform hash join
    print("🔗 Performing hash join...")
    start = time.perf_counter()

    if NATIVE_JOIN_AVAILABLE:
        # Use Sabot's native Arrow C++ hash join (SIMD-accelerated)
        print("   Using native Arrow C++ hash join...")

        # Convert tables to RecordBatches (native join expects batches)
        quotes_batch = quotes.to_batches()[0] if quotes.num_rows > 0 else quotes.to_batches(max_chunksize=1)[0]
        securities_batch = securities.to_batches()[0] if securities.num_rows > 0 else securities.to_batches(max_chunksize=1)[0]

        # Perform native hash join
        joined_batch = hash_join_batches(
            quotes_batch, securities_batch,
            'instrumentId', 'ID',
            join_type='inner'
        )

        # Convert back to table
        enriched = pa.Table.from_batches([joined_batch])

    else:
        # Fallback: Python filter (slow)
        print("   Using Python fallback filter...")
        quote_ids = quotes['instrumentId'].to_pylist()
        sec_ids = set(securities['ID'].to_pylist())
        matched_mask = [qid in sec_ids for qid in quote_ids]
        enriched = quotes.filter(pa.array(matched_mask))

    elapsed = time.perf_counter() - start
    throughput = (securities.num_rows + quotes.num_rows) / elapsed / 1e6

    print(f"✅ Join complete!")
    print(f"   Input: {securities.num_rows:,} securities + {quotes.num_rows:,} quotes")
    print(f"   Output: {enriched.num_rows:,} enriched rows")
    print(f"   Time: {elapsed*1000:.1f}ms")
    print(f"   Throughput: {throughput:.1f}M rows/sec")
    print()

    # Calculate spreads
    if enriched.num_rows > 0:
        print("📊 Calculating spreads...")
        start = time.perf_counter()

        if 'spread' in enriched.column_names:
            spreads = enriched['spread']
            spread_pct = pc.multiply(spreads, 100)
            enriched = enriched.append_column('spread_pct', spread_pct)

        elapsed = time.perf_counter() - start
        print(f"✅ Spreads calculated in {elapsed*1000:.1f}ms")
        print()

    # Show top results
    if enriched.num_rows > 0:
        print("📋 Top 10 Results:")
        top_10 = enriched.slice(0, min(10, enriched.num_rows))
        for i in range(top_10.num_rows):
            row = top_10.slice(i, 1)
            inst_id = row['instrumentId'][0].as_py()
            price = row['price'][0].as_py()
            size = row['size'][0].as_py()
            print(f"   {i+1}. InstrumentID: {inst_id} - Price: {price:.4f} @ Size: {size}")
        print()

    # Summary
    print("=" * 80)
    print("✅ Distributed enrichment complete!")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Two-Node Network Enrichment Demo")
    subparsers = parser.add_subparsers(dest='mode', help='Mode: server or client')

    # Server arguments
    server_parser = subparsers.add_parser('server', help='Run securities Flight server')
    server_parser.add_argument('--port', type=int, default=8815,
                              help='Port for Flight server (default: 8815)')
    server_parser.add_argument('--securities', type=int, default=1_000_000,
                              help='Number of securities to load (default: 1M)')

    # Client arguments
    client_parser = subparsers.add_parser('client', help='Run join client')
    client_parser.add_argument('--server', type=str, default='localhost:8815',
                              help='Server address (default: localhost:8815)')
    client_parser.add_argument('--quotes', type=int, default=100_000,
                              help='Number of quotes to load (default: 100K)')

    args = parser.parse_args()

    if args.mode == 'server':
        run_server(args.port, args.securities)
    elif args.mode == 'client':
        run_client(args.server, args.quotes)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

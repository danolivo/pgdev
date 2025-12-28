#!/usr/bin/env python3
"""
Measure and analyze local buffer flush cost for PostgreSQL.

This script runs benchmarks and calculates the appropriate value for
the local_buffer_flush_cost parameter based on your system's characteristics.

Usage:
    python3 measure_flush_cost.py --host localhost --database testdb --user postgres

Requirements:
    pip install psycopg2-binary
"""

import argparse
import sys
import time
import statistics
from typing import List, Tuple

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 not installed. Install with: pip install psycopg2-binary")
    sys.exit(1)


class FlushCostMeasurement:
    def __init__(self, conn):
        self.conn = conn
        self.results = {}

    def setup(self):
        """Configure database for testing."""
        with self.conn.cursor() as cur:
            cur.execute("SET temp_buffers = '128MB'")
            cur.execute("SET work_mem = '64MB'")
            cur.execute("SET max_parallel_workers_per_gather = 0")  # Disable parallel for now
            self.conn.commit()
            print("✓ Database configured for testing")

    def get_current_costs(self) -> dict:
        """Get current cost parameters for reference."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT name, setting::float
                FROM pg_settings
                WHERE name IN ('seq_page_cost', 'random_page_cost')
            """)
            return dict(cur.fetchall())

    def create_temp_table(self, size_mb: int = 100) -> int:
        """Create a temp table of specified size and return page count."""
        rows = int((size_mb * 1024 * 1024) / 200)  # ~200 bytes per row

        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS bench_temp")
            cur.execute(f"""
                CREATE TEMP TABLE bench_temp AS
                SELECT
                    i AS id,
                    md5(i::text) AS hash_val,
                    random() AS rand_val,
                    repeat('x', 100) AS padding
                FROM generate_series(1, {rows}) i
            """)

            # Get actual page count
            cur.execute("""
                SELECT pg_relation_size('bench_temp')::float / 8192
            """)
            pages = int(cur.fetchone()[0])
            self.conn.commit()
            print(f"✓ Created temp table: {size_mb}MB ({pages} pages)")
            return pages

    def make_dirty(self, pages: int) -> int:
        """Make all pages dirty and return dirty buffer count."""
        with self.conn.cursor() as cur:
            cur.execute("UPDATE bench_temp SET rand_val = random()")
            cur.execute("SELECT pg_dirty_local_buffers()")
            dirty = cur.fetchone()[0]
            self.conn.commit()
            print(f"✓ Table dirtied: {dirty} dirty buffers")
            return dirty

    def measure_flush(self, runs: int = 5) -> Tuple[float, int]:
        """Measure flush time over multiple runs."""
        times = []
        buffers_flushed = 0

        for i in range(runs):
            # Make dirty again
            with self.conn.cursor() as cur:
                cur.execute("UPDATE bench_temp SET rand_val = random()")
                self.conn.commit()

            # Measure flush
            start = time.perf_counter()
            with self.conn.cursor() as cur:
                cur.execute("SELECT pg_flush_local_buffers()")
                buffers_flushed = cur.fetchone()[0]
                self.conn.commit()
            elapsed = time.perf_counter() - start
            times.append(elapsed)

            print(f"  Run {i+1}/{runs}: {elapsed*1000:.2f}ms ({buffers_flushed} buffers)")

        avg_time = statistics.mean(times)
        stddev = statistics.stdev(times) if len(times) > 1 else 0

        print(f"✓ Flush time: {avg_time*1000:.2f}ms ± {stddev*1000:.2f}ms")
        return avg_time, buffers_flushed

    def measure_seq_scan(self, cold: bool = False, runs: int = 5) -> float:
        """Measure sequential scan time."""
        times = []

        for i in range(runs):
            if cold:
                # Flush to make cold
                with self.conn.cursor() as cur:
                    cur.execute("SELECT pg_flush_local_buffers()")
                    self.conn.commit()

            start = time.perf_counter()
            with self.conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), AVG(rand_val) FROM bench_temp")
                cur.fetchone()
            elapsed = time.perf_counter() - start
            times.append(elapsed)

            cache_type = "cold" if cold else "warm"
            print(f"  Run {i+1}/{runs}: {elapsed*1000:.2f}ms ({cache_type} cache)")

        avg_time = statistics.mean(times)
        stddev = statistics.stdev(times) if len(times) > 1 else 0

        cache_type = "cold" if cold else "warm"
        print(f"✓ Seq scan ({cache_type}): {avg_time*1000:.2f}ms ± {stddev*1000:.2f}ms")
        return avg_time

    def calculate_cost(self, flush_time: float, buffers: int,
                      scan_time: float, seq_page_cost: float) -> float:
        """Calculate the local_buffer_flush_cost parameter."""
        time_per_flush = flush_time / buffers
        time_per_read = scan_time / buffers

        ratio = time_per_flush / time_per_read if time_per_read > 0 else 2.0
        cost = ratio * seq_page_cost

        return cost

    def run_benchmark(self, size_mb: int = 100):
        """Run complete benchmark suite."""
        print("\n" + "="*70)
        print(f"BENCHMARK: Local Buffer Flush Cost Measurement ({size_mb}MB table)")
        print("="*70 + "\n")

        # Get current costs
        costs = self.get_current_costs()
        seq_page_cost = costs.get('seq_page_cost', 1.0)
        random_page_cost = costs.get('random_page_cost', 4.0)

        print(f"Current cost parameters:")
        print(f"  seq_page_cost = {seq_page_cost}")
        print(f"  random_page_cost = {random_page_cost}\n")

        # Setup
        self.setup()

        # Create table
        pages = self.create_temp_table(size_mb)

        # Make dirty
        dirty_buffers = self.make_dirty(pages)

        # Measure sequential scan (warm cache)
        print("\nMeasuring sequential scan (warm cache)...")
        scan_warm = self.measure_seq_scan(cold=False, runs=3)

        # Measure sequential scan (cold cache)
        print("\nMeasuring sequential scan (cold cache)...")
        scan_cold = self.measure_seq_scan(cold=True, runs=3)

        # Make dirty again for flush test
        self.make_dirty(pages)

        # Measure flush
        print("\nMeasuring flush operation...")
        flush_time, buffers_flushed = self.measure_flush(runs=5)

        # Calculate cost
        print("\n" + "="*70)
        print("ANALYSIS")
        print("="*70 + "\n")

        # Method 1: Based on cold scan (preferred)
        cost_vs_cold = self.calculate_cost(flush_time, buffers_flushed,
                                           scan_cold, seq_page_cost)

        # Method 2: Based on warm scan (alternative)
        cost_vs_warm = self.calculate_cost(flush_time, buffers_flushed,
                                           scan_warm, seq_page_cost)

        print(f"Metrics:")
        print(f"  Buffers flushed: {buffers_flushed}")
        print(f"  Time per flush: {flush_time/buffers_flushed*1000:.4f}ms")
        print(f"  Time per read (cold): {scan_cold/buffers_flushed*1000:.4f}ms")
        print(f"  Time per read (warm): {scan_warm/buffers_flushed*1000:.4f}ms")
        print()

        print(f"Calculated costs:")
        print(f"  vs cold scan: {cost_vs_cold:.2f} × seq_page_cost")
        print(f"  vs warm scan: {cost_vs_warm:.2f} × seq_page_cost")
        print()

        # Recommendation
        recommended = round(cost_vs_cold * 2) / 2  # Round to nearest 0.5
        recommended = max(1.0, min(5.0, recommended))  # Clamp to reasonable range

        print(f"RECOMMENDED: local_buffer_flush_cost = {recommended:.1f}")
        print()

        # Context
        print("Interpretation:")
        if recommended < 1.5:
            print("  ✓ Very fast storage (SSD/NVMe or tmpfs)")
            print("  ✓ Parallel queries on temp tables are attractive")
        elif recommended < 2.5:
            print("  ✓ Good storage (typical SSD)")
            print("  ✓ Parallel worth considering for large datasets")
        elif recommended < 4.0:
            print("  ⚠ Slower storage (HDD or network storage)")
            print("  ⚠ Parallel requires significant query speedup to justify")
        else:
            print("  ⚠ Slow storage (network storage or overloaded system)")
            print("  ⚠ Parallel on temp tables rarely beneficial")

        print("\n" + "="*70)
        print("To apply this setting:")
        print(f"  ALTER SYSTEM SET local_buffer_flush_cost = {recommended:.1f};")
        print("  SELECT pg_reload_conf();")
        print("="*70 + "\n")

        return recommended


def main():
    parser = argparse.ArgumentParser(
        description="Measure local buffer flush cost for PostgreSQL"
    )
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='postgres', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', help='Database password')
    parser.add_argument('--size-mb', type=int, default=100,
                       help='Size of test table in MB (default: 100)')

    args = parser.parse_args()

    # Connect
    try:
        conn_params = {
            'host': args.host,
            'port': args.port,
            'database': args.database,
            'user': args.user,
        }
        if args.password:
            conn_params['password'] = args.password

        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False

        print(f"Connected to {args.database} on {args.host}:{args.port}")

        # Run benchmark
        measurement = FlushCostMeasurement(conn)
        measurement.run_benchmark(args.size_mb)

        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)


if __name__ == '__main__':
    main()

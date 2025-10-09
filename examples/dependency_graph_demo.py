"""
Software Dependency Graph Analysis with Strongly Connected Components

Demonstrates using Tarjan's algorithm to analyze dependency graphs.

Use cases:
- Detect circular dependencies (SCCs with >1 node)
- Find independent modules (source SCCs)
- Identify core/foundation modules (sink SCCs)
- Determine build order (topological sort of SCC DAG)
- Measure coupling (size of largest SCC)
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import pyarrow as pa
from sabot._cython.graph.traversal.strongly_connected_components import (
    strongly_connected_components,
    scc_membership,
    largest_scc,
    is_strongly_connected,
    find_source_sccs,
    find_sink_sccs
)


def create_package_dependency_graph():
    """
    Create a software package dependency graph with circular dependencies.

    Structure:
    - Core packages (0-2): util, logging, config (foundation)
    - Middle layer (3-7): database, api, cache, auth, metrics
    - Application layer (8-11): web_app, api_server, worker, scheduler
    - Circular dependency: web_app -> api_server -> worker -> web_app (BAD)
    """
    print("Creating software package dependency graph...")

    # Package names
    package_names = [
        # Core layer
        "util", "logging", "config",
        # Middle layer
        "database", "api", "cache", "auth", "metrics",
        # Application layer
        "web_app", "api_server", "worker", "scheduler"
    ]

    # Dependencies (package A depends on package B means A->B)
    edges = [
        # Core dependencies (foundation)
        (1, 0),  # logging -> util
        (2, 0),  # config -> util

        # Middle layer depends on core
        (3, 1),  # database -> logging
        (3, 2),  # database -> config
        (4, 1),  # api -> logging
        (5, 3),  # cache -> database
        (6, 3),  # auth -> database
        (7, 1),  # metrics -> logging

        # Application layer depends on middle
        (8, 4),  # web_app -> api
        (8, 6),  # web_app -> auth
        (9, 4),  # api_server -> api
        (9, 6),  # api_server -> auth
        (10, 3), # worker -> database
        (10, 5), # worker -> cache
        (11, 3), # scheduler -> database

        # CIRCULAR DEPENDENCY (problematic!)
        (8, 9),  # web_app -> api_server
        (9, 10), # api_server -> worker
        (10, 8), # worker -> web_app (creates cycle!)

        # Another cycle in metrics/monitoring
        (7, 8),  # metrics -> web_app (to monitor it)
        (8, 7),  # web_app -> metrics (to send metrics)
    ]

    num_vertices = len(package_names)

    # Build CSR
    indptr_list = [0]
    indices_list = []

    for u in range(num_vertices):
        neighbors = [v for (src, v) in edges if src == u]
        indices_list.extend(neighbors)
        indptr_list.append(len(indices_list))

    indptr = pa.array(indptr_list, type=pa.int64())
    indices = pa.array(indices_list, type=pa.int64())

    print(f"✓ Created dependency graph with {num_vertices} packages and {len(edges)} dependencies")

    return indptr, indices, num_vertices, package_names, edges


def analyze_dependency_structure(indptr, indices, num_vertices, package_names):
    """Analyze dependency graph structure using SCCs."""

    print("\n" + "=" * 70)
    print("DEPENDENCY GRAPH ANALYSIS")
    print("=" * 70)

    # Find SCCs
    result = strongly_connected_components(indptr, indices, num_vertices)

    print(f"\n1. Strongly Connected Components (SCCs):")
    print(f"   - Total SCCs found: {result.num_components()}")
    print(f"   - Largest SCC size: {result.largest_component_size()}")

    component_ids = result.component_ids().to_pylist()
    sizes = result.component_sizes().to_pylist()

    # Group packages by SCC
    sccs = {}
    for pkg_id, scc_id in enumerate(component_ids):
        if scc_id not in sccs:
            sccs[scc_id] = []
        sccs[scc_id].append((pkg_id, package_names[pkg_id]))

    # Print each SCC
    print(f"\n2. SCC Details:")
    for scc_id in sorted(sccs.keys(), key=lambda x: -len(sccs[x])):
        packages = sccs[scc_id]
        size = len(packages)
        print(f"\n   SCC {scc_id} ({size} package{'s' if size > 1 else ''}):")
        for pkg_id, pkg_name in packages:
            print(f"     - {pkg_name} (ID: {pkg_id})")

        if size > 1:
            print(f"     ⚠️  CIRCULAR DEPENDENCY DETECTED!")

    return result, sccs


def detect_circular_dependencies(result, sccs, package_names):
    """Identify and report circular dependencies."""

    print("\n" + "=" * 70)
    print("CIRCULAR DEPENDENCY DETECTION")
    print("=" * 70)

    sizes = result.component_sizes().to_pylist()
    circular_sccs = [scc_id for scc_id, size in enumerate(sizes) if size > 1]

    if not circular_sccs:
        print("\n✅ No circular dependencies found - dependency graph is acyclic!")
        return

    print(f"\n⚠️  Found {len(circular_sccs)} circular dependency group(s):")

    for scc_id in circular_sccs:
        packages = sccs[scc_id]
        print(f"\nCircular Group {scc_id}:")
        print(f"  Packages involved: {', '.join(pkg[1] for pkg in packages)}")
        print(f"  Impact: These packages cannot be built independently")
        print(f"  Recommendation: Refactor to break circular dependencies")


def find_build_layers(result, sccs, indptr, indices, num_vertices, package_names):
    """Identify build layers (source and sink SCCs)."""

    print("\n" + "=" * 70)
    print("BUILD LAYER ANALYSIS")
    print("=" * 70)

    component_ids = result.component_ids()

    # Find source SCCs (no incoming dependencies from other SCCs)
    sources = find_source_sccs(component_ids, indptr, indices, num_vertices)
    source_sccs = sources.to_pylist()

    # Find sink SCCs (no outgoing dependencies to other SCCs)
    sinks = find_sink_sccs(component_ids, indptr, indices, num_vertices)
    sink_sccs = sinks.to_pylist()

    print(f"\n1. Foundation Packages (Source SCCs):")
    print(f"   These can be built first with no dependencies:")
    for scc_id in source_sccs:
        packages = sccs[scc_id]
        print(f"   - SCC {scc_id}: {', '.join(pkg[1] for pkg in packages)}")

    print(f"\n2. Top-Level Packages (Sink SCCs):")
    print(f"   These depend on other packages but nothing depends on them:")
    for scc_id in sink_sccs:
        packages = sccs[scc_id]
        print(f"   - SCC {scc_id}: {', '.join(pkg[1] for pkg in packages)}")

    print(f"\n3. Build Order Recommendation:")
    print(f"   1. Build foundation packages first (Source SCCs)")
    print(f"   2. Build intermediate packages")
    print(f"   3. Build top-level applications last (Sink SCCs)")


def measure_coupling(result, sccs, package_names):
    """Measure system coupling using SCC sizes."""

    print("\n" + "=" * 70)
    print("COUPLING ANALYSIS")
    print("=" * 70)

    sizes = result.component_sizes().to_pylist()
    total_packages = result.num_vertices()
    num_sccs = result.num_components()

    # Largest SCC
    largest = largest_scc(result.component_ids(), result.component_sizes())
    largest_packages = [package_names[i] for i in largest.to_pylist()]

    # Coupling metrics
    avg_scc_size = total_packages / num_sccs
    max_scc_size = max(sizes)
    coupling_ratio = max_scc_size / total_packages

    print(f"\n1. Coupling Metrics:")
    print(f"   - Average SCC size: {avg_scc_size:.2f}")
    print(f"   - Largest SCC size: {max_scc_size}")
    print(f"   - Coupling ratio: {coupling_ratio:.2%}")

    print(f"\n2. Largest Coupled Group:")
    print(f"   Size: {len(largest_packages)} packages")
    print(f"   Packages: {', '.join(largest_packages)}")

    print(f"\n3. Interpretation:")
    if coupling_ratio > 0.5:
        print(f"   ⚠️  HIGH COUPLING - Over half the packages are tightly coupled!")
        print(f"   Recommendation: Refactor to reduce interdependencies")
    elif coupling_ratio > 0.25:
        print(f"   ⚠️  MODERATE COUPLING - Significant interdependencies exist")
        print(f"   Recommendation: Consider breaking apart large modules")
    else:
        print(f"   ✅ LOW COUPLING - System is well-modularized")


def recommend_refactoring(result, sccs, package_names):
    """Suggest refactoring to break circular dependencies."""

    print("\n" + "=" * 70)
    print("REFACTORING RECOMMENDATIONS")
    print("=" * 70)

    sizes = result.component_sizes().to_pylist()
    circular_sccs = [scc_id for scc_id, size in enumerate(sizes) if size > 1]

    if not circular_sccs:
        print("\n✅ No refactoring needed - dependency graph is already acyclic!")
        return

    print("\nTo break circular dependencies:")

    for i, scc_id in enumerate(circular_sccs, 1):
        packages = sccs[scc_id]
        print(f"\n{i}. Circular Group {scc_id}:")
        print(f"   Packages: {', '.join(pkg[1] for pkg in packages)}")
        print(f"   Strategy: Extract shared interface/contract into new package")
        print(f"   Example: Create 'api_contract' package with shared types/interfaces")
        print(f"   Result: Break cycle by having both sides depend on contract")


def check_strong_connectivity(indptr, indices, num_vertices):
    """Check if entire graph is strongly connected."""

    print("\n" + "=" * 70)
    print("GRAPH CONNECTIVITY CHECK")
    print("=" * 70)

    is_connected = is_strongly_connected(indptr, indices, num_vertices)

    if is_connected:
        print("\n⚠️  WARNING: Entire dependency graph is STRONGLY CONNECTED!")
        print("   This means there are circular dependencies involving ALL packages.")
        print("   This is extremely problematic and requires major refactoring.")
    else:
        print("\n✅ Graph is NOT strongly connected")
        print("   This is good - packages can be built in layers")


def main():
    """Run dependency graph analysis demo."""

    print("=" * 70)
    print("SOFTWARE DEPENDENCY GRAPH ANALYSIS DEMO")
    print("=" * 70)
    print("\nAnalyzing package dependencies using Strongly Connected Components")
    print("Use case: Detect circular dependencies and plan build order")

    # Create dependency graph
    indptr, indices, num_vertices, package_names, edges = create_package_dependency_graph()

    # Analyze structure
    result, sccs = analyze_dependency_structure(indptr, indices, num_vertices, package_names)

    # Detect circular dependencies
    detect_circular_dependencies(result, sccs, package_names)

    # Find build layers
    find_build_layers(result, sccs, indptr, indices, num_vertices, package_names)

    # Measure coupling
    measure_coupling(result, sccs, package_names)

    # Recommend refactoring
    recommend_refactoring(result, sccs, package_names)

    # Check strong connectivity
    check_strong_connectivity(indptr, indices, num_vertices)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total packages: {num_vertices}")
    print(f"Total dependencies: {len(edges)}")
    print(f"SCCs found: {result.num_components()}")
    print(f"Largest SCC: {result.largest_component_size()} packages")
    print("\nKey insight: SCCs reveal circular dependencies that prevent")
    print("independent package building and testing. Use this analysis to:")
    print("  1. Identify problematic dependency cycles")
    print("  2. Plan refactoring to break cycles")
    print("  3. Determine optimal build order")
    print("  4. Measure and reduce system coupling")


if __name__ == '__main__':
    main()

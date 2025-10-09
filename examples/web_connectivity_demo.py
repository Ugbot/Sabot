"""
Web Graph Connectivity Analysis

Demonstrates weakly connected components for analyzing web page connectivity.

In a web graph:
- Vertices = web pages
- Edges = hyperlinks (directed: page A links to page B)

Use case: Find clusters of interlinked pages (web communities) regardless
of link direction. Useful for:
- Identifying topic clusters
- Finding isolated page groups
- Detecting dead-end page groups
- SEO analysis
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import pyarrow as pa
from sabot._cython.graph.traversal.connected_components import (
    weakly_connected_components,
    connected_components,
    largest_component,
    component_statistics
)


def create_web_graph_csr():
    """
    Create a web graph with multiple page clusters in CSR format.

    Structure:
    - Tech blog cluster (0-4): Bidirectional links, highly connected
    - News cluster (5-9): Mostly unidirectional links
    - Product pages (10-12): Isolated from main clusters
    - Dead-end pages (13-14): Only incoming links, no outgoing
    - Orphan page (15): No links at all
    """
    print("Creating web graph...")

    # Define edges (hyperlinks)
    edges = [
        # Tech blog cluster (highly interconnected, bidirectional)
        (0, 1), (1, 0),  # Python <-> Rust
        (1, 2), (2, 1),  # Rust <-> C++
        (2, 3), (3, 2),  # C++ <-> JavaScript
        (3, 4), (4, 3),  # JavaScript <-> Go
        (4, 0), (0, 4),  # Go <-> Python
        (0, 2),          # Python -> C++

        # News cluster (mostly unidirectional, chain-like)
        (5, 6),  # tech-2024 -> ai-trends
        (6, 7),  # ai-trends -> startups
        (7, 8),  # startups -> funding
        (8, 9),  # funding -> mergers
        (9, 5),  # mergers -> tech-2024 (creates cycle)

        # Product pages (isolated cluster)
        (10, 11), (11, 10),  # laptop <-> phone
        (11, 12),            # phone -> tablet
        (12, 10),            # tablet -> laptop

        # Tech blog links to news (cross-cluster)
        (1, 5),  # Rust blog -> tech news

        # Some pages link to dead-end pages
        (0, 13),  # Python blog -> old post
        (5, 14),  # News -> deprecated
        # Dead-end pages have no outgoing links
    ]

    num_vertices = 16

    # Build CSR
    indptr_list = [0]
    indices_list = []

    for u in range(num_vertices):
        neighbors = [v for (src, v) in edges if src == u]
        indices_list.extend(neighbors)
        indptr_list.append(len(indices_list))

    indptr = pa.array(indptr_list, type=pa.int64())
    indices = pa.array(indices_list, type=pa.int64())

    print(f"✓ Created web graph with {num_vertices} pages and {len(edges)} links")

    return indptr, indices, num_vertices, edges


def analyze_web_connectivity(indptr, indices, num_vertices, edges):
    """Analyze web graph connectivity using both regular and weakly connected components."""

    print("\n" + "=" * 70)
    print("WEB GRAPH CONNECTIVITY ANALYSIS")
    print("=" * 70)

    # 1. Regular Connected Components (respects link direction)
    print("\n1. Strongly Directed Analysis (respects link direction):")
    print("   Shows: Pages reachable following actual link direction")

    regular_result = connected_components(indptr, indices, num_vertices)
    regular_components = regular_result.num_components()

    print(f"   - Found {regular_components} strongly connected groups")
    print(f"   - Largest group: {regular_result.largest_component_size()} pages")

    regular_ids = regular_result.component_ids().to_pylist()
    print(f"   - Component assignments: {regular_ids}")

    # 2. Weakly Connected Components (ignores link direction)
    print("\n2. Community Analysis (ignores link direction):")
    print("   Shows: Pages that are interconnected (any path exists)")

    weakly_result = weakly_connected_components(indptr, indices, num_vertices)
    weakly_components = weakly_result.num_components()

    print(f"   - Found {weakly_components} page communities")
    print(f"   - Largest community: {weakly_result.largest_component_size()} pages")

    weakly_ids = weakly_result.component_ids().to_pylist()
    print(f"   - Community assignments: {weakly_ids}")

    # 3. Identify page communities
    print("\n3. Identified Page Communities:")

    page_urls = [
        '/tech/blog/python', '/tech/blog/rust', '/tech/blog/cpp',
        '/tech/blog/javascript', '/tech/blog/go',
        '/news/tech-2024', '/news/ai-trends', '/news/startups',
        '/news/funding', '/news/mergers',
        '/products/laptop', '/products/phone', '/products/tablet',
        '/archive/old-post', '/archive/deprecated',
        '/isolated/test-page'
    ]

    # Group pages by weakly connected component
    communities = {}
    for page_id, component_id in enumerate(weakly_ids):
        if component_id not in communities:
            communities[component_id] = []
        communities[component_id].append((page_id, page_urls[page_id]))

    for comp_id in sorted(communities.keys()):
        pages = communities[comp_id]
        print(f"\n   Community {comp_id} ({len(pages)} pages):")
        for page_id, url in pages:
            print(f"     - Page {page_id:2d}: {url}")

    # 4. Component statistics
    print("\n4. Community Statistics:")

    stats = component_statistics(
        weakly_result.component_ids(),
        weakly_result.num_components()
    )

    stats_dict = {
        'component_id': stats.column(0).to_pylist(),
        'size': stats.column(1).to_pylist(),
        'isolated': stats.column(2).to_pylist()
    }

    print(f"\n   {'Community':<12} {'Size':<8} {'Isolated':<10}")
    print(f"   {'-'*12} {'-'*8} {'-'*10}")

    for i in range(len(stats_dict['component_id'])):
        comp_id = stats_dict['component_id'][i]
        size = stats_dict['size'][i]
        isolated = stats_dict['isolated'][i]
        status = "Yes" if isolated else "No"
        print(f"   {comp_id:<12} {size:<8} {status:<10}")

    # 5. Largest community analysis
    print("\n5. Largest Community Analysis:")

    largest = largest_component(
        weakly_result.component_ids(),
        weakly_result.component_sizes()
    )

    largest_pages = largest.to_pylist()
    print(f"   - Largest community has {len(largest_pages)} pages")
    print(f"   - Page IDs: {largest_pages}")
    print(f"   - Pages:")
    for page_id in largest_pages:
        print(f"     - {page_urls[page_id]}")

    # 6. Cross-cluster link detection
    print("\n6. Cross-Community Links:")

    cross_links = []
    for source, target in edges:
        if weakly_ids[source] != weakly_ids[target]:
            cross_links.append((source, target, page_urls[source], page_urls[target]))

    if cross_links:
        print(f"   Found {len(cross_links)} links connecting different communities:")
        for source, target, source_url, target_url in cross_links:
            print(f"     - Community {weakly_ids[source]} -> Community {weakly_ids[target]}")
            print(f"       {source_url} -> {target_url}")
    else:
        print("   No cross-community links found")

    return weakly_result, communities


def detect_isolated_pages(weakly_result):
    """Identify isolated and orphan pages."""

    print("\n" + "=" * 70)
    print("ISOLATED PAGE DETECTION")
    print("=" * 70)

    sizes = weakly_result.component_sizes().to_pylist()
    ids = weakly_result.component_ids().to_pylist()

    isolated_pages = []
    for page_id, component_id in enumerate(ids):
        if sizes[component_id] == 1:
            isolated_pages.append(page_id)

    page_urls = [
        '/tech/blog/python', '/tech/blog/rust', '/tech/blog/cpp',
        '/tech/blog/javascript', '/tech/blog/go',
        '/news/tech-2024', '/news/ai-trends', '/news/startups',
        '/news/funding', '/news/mergers',
        '/products/laptop', '/products/phone', '/products/tablet',
        '/archive/old-post', '/archive/deprecated',
        '/isolated/test-page'
    ]

    if isolated_pages:
        print(f"\nFound {len(isolated_pages)} isolated pages:")
        for page_id in isolated_pages:
            print(f"  - Page {page_id}: {page_urls[page_id]}")
        print("\nRecommendation: Add bidirectional links to connect these pages to main communities")
    else:
        print("\n✓ No isolated pages found - all pages are interconnected")


def recommend_internal_links(weakly_result, communities):
    """Recommend internal links to improve connectivity."""

    print("\n" + "=" * 70)
    print("INTERNAL LINKING RECOMMENDATIONS")
    print("=" * 70)

    num_communities = len(communities)

    if num_communities > 1:
        print(f"\nFound {num_communities} separate communities")
        print("\nRecommendations to improve site connectivity:")

        # Recommend links between large communities
        sorted_comms = sorted(
            communities.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )

        if len(sorted_comms) >= 2:
            comm1_id, comm1_pages = sorted_comms[0]
            comm2_id, comm2_pages = sorted_comms[1]

            print(f"\n1. Connect Community {comm1_id} ({len(comm1_pages)} pages) with Community {comm2_id} ({len(comm2_pages)} pages):")
            print(f"   Suggested links:")
            print(f"     - {comm1_pages[0][1]} -> {comm2_pages[0][1]}")
            print(f"     - {comm2_pages[0][1]} -> {comm1_pages[0][1]}")

        # Recommend connecting isolated pages
        isolated_comms = [
            (comp_id, pages) for comp_id, pages in communities.items()
            if len(pages) == 1
        ]

        if isolated_comms:
            largest_comm_id, largest_pages = sorted_comms[0]
            print(f"\n2. Connect isolated pages to main community (Community {largest_comm_id}):")
            for comp_id, pages in isolated_comms:
                page_id, page_url = pages[0]
                target_page = largest_pages[0][1]  # Link to first page in largest community
                print(f"     - {page_url} <-> {target_page}")
    else:
        print("\n✓ All pages are in a single community - excellent connectivity!")


def main():
    """Run web connectivity analysis demo."""

    print("=" * 70)
    print("WEB GRAPH CONNECTIVITY DEMO")
    print("=" * 70)
    print("\nAnalyzing web page interconnectivity using weakly connected components")
    print("Use case: SEO, content clustering, internal linking analysis")

    # Create web graph
    indptr, indices, num_vertices, edges = create_web_graph_csr()

    # Analyze connectivity
    weakly_result, communities = analyze_web_connectivity(indptr, indices, num_vertices, edges)

    # Detect isolated pages
    detect_isolated_pages(weakly_result)

    # Recommend internal links
    recommend_internal_links(weakly_result, communities)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total pages: 16")
    print(f"Total links: {len(edges)}")
    print(f"Page communities: {weakly_result.num_components()}")
    print(f"Largest community: {weakly_result.largest_component_size()} pages")
    print("\nKey insight: Weakly connected components reveal natural page")
    print("clusters regardless of link direction, useful for:")
    print("  - Content strategy (identify topic clusters)")
    print("  - SEO optimization (improve internal linking)")
    print("  - Site navigation (connect isolated page groups)")


if __name__ == '__main__':
    main()
